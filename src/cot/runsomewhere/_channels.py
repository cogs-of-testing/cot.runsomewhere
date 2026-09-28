"""Channels and the connection that multiplexes them over one byte stream."""

from __future__ import annotations

import math
import struct
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any

import anyio
import anyio.abc
from anyio.lowlevel import cancel_shielded_checkpoint, checkpoint_if_cancelled

from ._errors import ChannelClosed, RemoteError, StateError, WorkerGone
from ._frames import PREAMBLE, Frame, FrameDecoder, FrameError, FrameType, encode_frame
from ._values import DecodeError, decode, encode

DEFAULT_WINDOW = 1024 * 1024
_CREDIT = struct.Struct(">I")

OpenHandler = Callable[["Channel", bytes], None]


class Channel:
    """An ordered, two-way stream of values with the peer."""

    _rsh_channel = True

    def __init__(self, connection: Connection, channel_id: int) -> None:
        self._connection = connection
        self.id = channel_id
        self._items: deque[tuple[Any, int]] = deque()
        self._item_arrived: anyio.Event | None = None
        self._credit = DEFAULT_WINDOW
        self._credit_arrived: anyio.Event | None = None
        self._unacknowledged = 0
        self._closed_locally = False
        self._peer_close: dict[str, Any] | None = None
        self._peer_closed = anyio.Event()
        self._failure: BaseException | None = None
        self._receivers_waiting = 0
        #: set by the worker for a service's channel; cancelled on a close the
        #: handler is not waiting for
        self.handler_scope: anyio.CancelScope | None = None

    def __repr__(self) -> str:
        return f"<Channel {self.id}>"

    # -- the API --------------------------------------------------------------

    def new(self) -> Channel:
        """A new channel on the same gateway, usable once sent to the peer."""
        return self._connection.new_channel()

    async def send(self, value: object) -> None:
        await checkpoint_if_cancelled()
        self._raise_if_unusable()
        payload = encode(value, self._connection.channel_id)
        while self._credit <= 0:
            self._credit_arrived = anyio.Event()
            await self._credit_arrived.wait()
            self._raise_if_unusable()
        self._credit -= len(payload)
        self._connection.send_frame(FrameType.DATA, self.id, payload)
        await cancel_shielded_checkpoint()

    async def receive(self) -> Any:
        # a cancellation is taken either before an item is removed or not at
        # all, so a cancelled receive never loses one
        await checkpoint_if_cancelled()
        while not self._items:
            self._raise_if_finished()
            self._item_arrived = anyio.Event()
            self._receivers_waiting += 1
            try:
                await self._item_arrived.wait()
            finally:
                self._receivers_waiting -= 1
        value, size = self._items.popleft()
        self._consumed(size)
        await cancel_shielded_checkpoint()
        return value

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration from None

    async def wait_closed(self) -> Any:
        """Wait for the peer to close; return its result or raise its error."""
        if self._peer_close is None and self._failure is None:
            if self._closed_locally:
                raise ChannelClosed(f"{self!r} was closed by this side")
            await self._peer_closed.wait()
        if self._peer_close is not None:
            return self._close_result()
        assert self._failure is not None
        raise self._failure

    async def aclose(self) -> None:
        self.close()
        await cancel_shielded_checkpoint()

    def close(
        self, *, result: object = None, error: BaseException | None = None
    ) -> None:
        """Close for both directions, telling the peer unless it closed first."""
        if self._closed_locally:
            return
        self._closed_locally = True
        if self._peer_close is None and self._failure is None:
            info: dict[str, Any] = {}
            if error is not None:
                info = _error_info(error)
            elif result is not None:
                try:
                    encode(result, self._connection.channel_id)
                except TypeError as unsendable:
                    info = _error_info(unsendable)
                else:
                    info = {"result": result}
            self._connection.send_frame(
                FrameType.CLOSE, self.id, encode(info, self._connection.channel_id)
            )
        self._connection.forget(self)
        self._wake()

    # -- driven by the connection ---------------------------------------------

    def _deliver(self, value: Any, size: int) -> None:
        if self._closed_locally:
            return
        self._items.append((value, size))
        if self._item_arrived is not None:
            self._item_arrived.set()

    def _grant(self, amount: int) -> None:
        self._credit += amount
        if self._credit_arrived is not None:
            self._credit_arrived.set()

    def _closed_by_peer(self, info: dict[str, Any]) -> None:
        self._peer_close = info
        self._peer_closed.set()
        waited_for = self._receivers_waiting > 0
        self._wake()
        # a handler waiting on its channel sees the close; one busy elsewhere
        # is cancelled
        if not waited_for and self.handler_scope is not None:
            self.handler_scope.cancel()

    def _fail(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
        self._peer_closed.set()
        self._wake()

    def _wake(self) -> None:
        if self._item_arrived is not None:
            self._item_arrived.set()
        if self._credit_arrived is not None:
            self._credit_arrived.set()

    def _consumed(self, size: int) -> None:
        if self._closed_locally or self._peer_close is not None:
            return
        self._unacknowledged += size
        if self._unacknowledged >= DEFAULT_WINDOW // 4 or not self._items:
            self._connection.send_frame(
                FrameType.CREDIT, self.id, _CREDIT.pack(self._unacknowledged)
            )
            self._unacknowledged = 0

    def _close_result(self) -> Any:
        assert self._peer_close is not None
        if "error" in self._peer_close:
            raise RemoteError(
                self._peer_close["error"],
                remote_type=self._peer_close.get("type", ""),
                remote_traceback=self._peer_close.get("traceback", ""),
            )
        return self._peer_close.get("result")

    def _raise_if_unusable(self) -> None:
        if self._closed_locally:
            raise ChannelClosed(f"{self!r} was closed by this side")
        if self._peer_close is not None:
            raise ChannelClosed(f"{self!r} was closed by the other side")
        if self._failure is not None:
            raise self._failure

    def _raise_if_finished(self) -> None:
        if self._peer_close is not None:
            self._close_result()
            raise ChannelClosed(f"{self!r} was closed by the other side")
        if self._failure is not None:
            raise self._failure
        if self._closed_locally:
            raise ChannelClosed(f"{self!r} was closed by this side")


def _error_info(error: BaseException) -> dict[str, Any]:
    import traceback

    return {
        "error": str(error),
        "type": type(error).__name__,
        "traceback": "".join(traceback.format_exception(error)),
    }


class Connection:
    """One side of the protocol over a byte stream: frames and channels."""

    def __init__(
        self,
        stream: anyio.abc.ByteStream,
        *,
        side: str,
        on_open: OpenHandler | None = None,
    ) -> None:
        self._stream = stream
        self._decoder = FrameDecoder()
        self._channels: dict[int, Channel] = {}
        self._next_id = 1 if side == "caller" else 2
        self._on_open = on_open
        self._outgoing_send, self._outgoing = anyio.create_memory_object_stream[bytes](
            math.inf
        )
        self._control_send, self.control = anyio.create_memory_object_stream[Frame](
            math.inf
        )
        self.failure: WorkerGone | None = None
        self.gone = anyio.Event()
        self._outgoing_send.send_nowait(PREAMBLE)

    # -- channels -------------------------------------------------------------

    def new_channel(self) -> Channel:
        if self.failure is not None:
            raise self.failure
        channel = Channel(self, self._next_id)
        self._next_id += 2
        self._channels[channel.id] = channel
        return channel

    def forget(self, channel: Channel) -> None:
        self._channels.pop(channel.id, None)

    def channel_id(self, channel: Any) -> int:
        # a sync handler holds its channels wrapped for its thread
        unwrapped: Channel = getattr(channel, "async_channel", channel)
        if unwrapped._connection is not self:
            raise StateError(f"{unwrapped!r} belongs to another gateway")
        return unwrapped.id

    def _channel_for(self, channel_id: int) -> Channel:
        channel = self._channels.get(channel_id)
        if channel is None:
            channel = Channel(self, channel_id)
            self._channels[channel_id] = channel
        return channel

    # -- frames ---------------------------------------------------------------

    def send_frame(self, kind: FrameType, channel: int, payload: bytes = b"") -> None:
        if self.failure is not None:
            return
        try:
            self._outgoing_send.send_nowait(encode_frame(Frame(kind, channel, payload)))
        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
            pass

    def finish_sending(self) -> None:
        """Close the stream once everything queued so far is written."""
        self._outgoing_send.close()

    async def run(self) -> None:
        """Read and write until the stream ends; never raises."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._write)
                await self._read()
                tg.cancel_scope.cancel()
        except Exception as error:  # noqa: BLE001 - reported as the failure
            self._set_gone(f"stream failed: {error}")
        finally:
            if self.failure is None:
                self._set_gone("the connection was closed")
            for stream in (self._outgoing, self.control):
                stream.close()
            with anyio.CancelScope(shield=True):
                await self._stream.aclose()

    async def _write(self) -> None:
        try:
            async for data in self._outgoing:
                await self._stream.send(data)
            await self._stream.send_eof()
        except (anyio.BrokenResourceError, anyio.ClosedResourceError, OSError):
            pass

    async def _read(self) -> None:
        while True:
            try:
                data = await self._stream.receive()
            except (anyio.EndOfStream, anyio.ClosedResourceError):
                self._set_gone("the stream ended")
                return
            except (anyio.BrokenResourceError, OSError) as error:
                self._set_gone(f"the stream broke: {error}")
                return
            try:
                for frame in self._decoder.feed(data):
                    self._dispatch(frame)
            except (FrameError, DecodeError) as error:
                self._set_gone(f"invalid frame stream: {error}")
                return

    def _dispatch(self, frame: Frame) -> None:
        kind = frame.type
        if kind in (FrameType.HELLO, FrameType.CONFIG, FrameType.GATEWAY_CLOSE):
            self._control_send.send_nowait(frame)
        elif kind == FrameType.OPEN:
            if self._on_open is None:
                raise FrameError("the caller was asked to open a channel")
            self._on_open(self._channel_for(frame.channel), frame.payload)
        elif kind == FrameType.DATA:
            channel = self._channels.get(frame.channel)
            if channel is not None:
                channel._deliver(
                    decode(frame.payload, self._channel_for), len(frame.payload)
                )
        elif kind == FrameType.CREDIT:
            channel = self._channels.get(frame.channel)
            if channel is not None:
                channel._grant(int(_CREDIT.unpack(frame.payload)[0]))
        elif kind == FrameType.CLOSE:
            channel = self._channels.pop(frame.channel, None)
            if channel is not None:
                channel._closed_by_peer(decode(frame.payload, self._channel_for))

    def _set_gone(self, reason: str) -> None:
        if self.failure is not None:
            return
        self.failure = WorkerGone(f"worker gone: {reason}")
        self.gone.set()
        self._control_send.close()
        self._outgoing_send.close()
        for channel in list(self._channels.values()):
            channel._fail(self.failure)

    async def next_control(self, kind: FrameType) -> Frame:
        try:
            frame = await self.control.receive()
        except anyio.EndOfStream:
            assert self.failure is not None
            raise self.failure from None
        if frame.type != kind:
            raise WorkerGone(f"expected a {kind.name} frame, got {frame.type.name}")
        return frame
