"""Channels and the connection that multiplexes them over one byte stream."""

from __future__ import annotations

import contextlib
import math
import struct
import traceback
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any

import anyio
import anyio.abc
from anyio.lowlevel import cancel_shielded_checkpoint, checkpoint_if_cancelled

from ._errors import ChannelClosed, ItemsDiscarded, RemoteError, StateError, WorkerGone
from ._frames import PREAMBLE, Frame, FrameDecoder, FrameError, FrameType, encode_frame
from ._values import DecodeError, decode, encode

DEFAULT_WINDOW = 1024 * 1024
_CREDIT = struct.Struct(">I")

OpenHandler = Callable[["Channel", bytes], None]


class Channel:
    """An ordered, two-way stream of values with the peer."""

    _rsh_channel = True
    #: carries a tunnelled gateway: an edge of the teardown graph
    tunnel = False

    def __init__(self, connection: Connection, channel_id: int) -> None:
        self._connection = connection
        self.id = channel_id
        self._items: deque[tuple[Any, int]] = deque()
        self._item_arrived: anyio.Event | None = None
        self._credit = DEFAULT_WINDOW
        self._credit_arrived: anyio.Event | None = None
        self._unacknowledged = 0
        #: sizes of the items sent and not yet taken, oldest first
        self._outstanding: deque[int] = deque()
        self._granted_unmatched = 0
        self._taken_by_peer = 0
        self._granted_total = 0
        self._taken_bytes = 0
        self._closed_locally = False
        self._ended_sending = False
        self._ended_receiving = False
        #: the peer's full close: its result or error
        self._peer_close: dict[str, Any] | None = None
        self._peer_ended_sending = False
        self._peer_ended_receiving = False
        self._peer_closed = anyio.Event()
        self._failure: BaseException | None = None
        self._receivers_waiting = 0
        self._stop_requested = anyio.Event()
        #: when the peer asked for this channel's work to be done, on this
        #: side's event loop clock; best effort, the peer enforces it
        self.stop_deadline: float | None = None
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
            await self._wait_for_credit()
            self._raise_if_unusable()
        self._credit -= len(payload)
        self._outstanding.append(len(payload))
        self._connection.send_frame(FrameType.DATA, self.id, payload)
        await cancel_shielded_checkpoint()

    async def drain(self) -> None:
        """Wait until the peer has taken every item sent.

        Taken is not processed: the peer's code may still fail with an item it
        took. Raises `ItemsDiscarded` when the peer stopped receiving first.
        """
        await checkpoint_if_cancelled()
        while self._outstanding:
            if self._peer_ended_receiving:
                msg = (
                    f"{self!r}: the other side took {self._taken_by_peer} items "
                    f"and discarded {len(self._outstanding)}"
                )
                raise ItemsDiscarded(
                    msg, taken=self._taken_by_peer, discarded=len(self._outstanding)
                )
            if self._failure is not None:
                raise self._failure
            await self._wait_for_credit()

    async def _wait_for_credit(self) -> None:
        # shared by a blocked send and a drain: replacing an unset event would
        # leave the other waiting on one nobody sets
        if self._credit_arrived is None or self._credit_arrived.is_set():
            self._credit_arrived = anyio.Event()
        await self._credit_arrived.wait()

    async def receive(self) -> Any:
        # a cancellation is taken either before an item is removed or not at
        # all, so a cancelled receive never loses one
        await checkpoint_if_cancelled()
        if self._ended_receiving:
            msg = f"{self!r} was closed by this side"
            raise ChannelClosed(msg)
        while not self._items:
            self._raise_if_finished()
            self._item_arrived = anyio.Event()
            self._receivers_waiting += 1
            try:
                await self._item_arrived.wait()
            finally:
                self._receivers_waiting -= 1
        value, size = self._items.popleft()
        self._taken_bytes += size
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
                msg = f"{self!r} was closed by this side"
                raise ChannelClosed(msg)
            await self._peer_closed.wait()
        if self._peer_close is not None:
            return self._close_result()
        assert self._failure is not None
        raise self._failure

    async def aclose(self) -> None:
        self.close()
        await cancel_shielded_checkpoint()

    def stop(self, deadline: float | None = None) -> None:
        """Ask the peer to finish: send what it owes and close with a result.

        Nothing is cancelled or dropped, and both directions stay open.
        ``deadline`` is in seconds from now, and only advice to the peer.
        """
        if self._closed_locally or self._peer_close is not None:
            return
        info = {} if deadline is None else {"deadline": deadline}
        self._connection.send_frame(FrameType.STOP, self.id, encode(info))

    @property
    def stopping(self) -> bool:
        """Whether the peer asked this side to finish."""
        return self._stop_requested.is_set()

    async def stop_requested(self) -> None:
        """Wait until the peer asks this side to finish."""
        await self._stop_requested.wait()

    def close_send(self) -> None:
        """End sending: the peer takes what was sent, then sees the end."""
        self._end(send=True)

    def close_receive(self) -> None:
        """End receiving: what arrived and was not taken is discarded, and the
        peer's sends fail."""
        self._end(send=False)

    def _end(self, *, send: bool) -> None:
        if self._closed_locally or (
            self._ended_sending if send else self._ended_receiving
        ):
            return
        if self._ended_receiving if send else self._ended_sending:
            self.close()
            return
        if send:
            self._ended_sending = True
        else:
            self._ended_receiving = True
            self._items.clear()
        if self._peer_close is None and self._failure is None:
            info: dict[str, Any] = {"ends": "send"}
            if not send:
                info = {"ends": "receive", "taken": self._taken_bytes}
            self._connection.send_frame(FrameType.CLOSE, self.id, encode(info))
        self._wake()

    def close(
        self, *, result: object = None, error: BaseException | None = None
    ) -> None:
        """Close both directions, telling the peer unless it closed first.

        Only this close carries a result or an error, also after `close_send`.
        """
        if self._closed_locally:
            return
        self._closed_locally = True
        self._ended_sending = self._ended_receiving = True
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
            info["taken"] = self._taken_bytes
            self._connection.send_frame(
                FrameType.CLOSE, self.id, encode(info, self._connection.channel_id)
            )
        self._connection.forget(self)
        self._wake()

    # -- driven by the connection ---------------------------------------------

    def _deliver(self, value: Any, size: int) -> None:
        if self._ended_receiving:
            return
        self._items.append((value, size))
        if self._item_arrived is not None:
            self._item_arrived.set()

    def _grant(self, amount: int) -> None:
        self._credit += amount
        self._match_taken(amount)
        if self._credit_arrived is not None:
            self._credit_arrived.set()

    def _match_taken(self, amount: int) -> None:
        # the peer takes items in order, so bytes taken map to whole items
        self._granted_total += amount
        self._granted_unmatched += amount
        while self._outstanding and self._granted_unmatched >= self._outstanding[0]:
            self._granted_unmatched -= self._outstanding.popleft()
            self._taken_by_peer += 1

    def _closed_by_peer(self, info: dict[str, Any]) -> None:
        ends = info.pop("ends", None)
        # taken but not yet granted back, since credit goes out in batches
        taken = info.pop("taken", self._granted_total)
        self._match_taken(max(0, taken - self._granted_total))
        self._peer_ended_sending |= ends in (None, "send")
        self._peer_ended_receiving |= ends in (None, "receive")
        if ends is not None:
            # a half-close carries no result and stops nothing: the handler
            # learns of it from its next channel operation
            self._wake()
            return
        self._peer_close = info
        self._peer_closed.set()
        waited_for = self._receivers_waiting > 0
        self._wake()
        # a handler waiting on its channel sees the close; one busy elsewhere
        # is cancelled
        if not waited_for and self.handler_scope is not None:
            self.handler_scope.cancel()

    def _stopped_by_peer(self, info: dict[str, Any]) -> None:
        if "deadline" in info:
            self.stop_deadline = anyio.current_time() + info["deadline"]
        self._stop_requested.set()

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
        # after the peer ends sending its credit still comes back: it is how
        # the peer learns what was taken
        if self._ended_receiving or self._peer_close is not None:
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
        if self._ended_sending:
            msg = f"{self!r} was closed by this side"
            raise ChannelClosed(msg)
        if self._peer_ended_receiving:
            msg = f"{self!r} was closed by the other side"
            raise ChannelClosed(msg)
        if self._failure is not None:
            raise self._failure

    def _raise_if_finished(self) -> None:
        if self._peer_ended_sending:
            if self._peer_close is not None:
                self._close_result()
            msg = f"{self!r} was closed by the other side"
            raise ChannelClosed(msg)
        if self._failure is not None:
            raise self._failure
        if self._closed_locally:
            msg = f"{self!r} was closed by this side"
            raise ChannelClosed(msg)


def _error_info(error: BaseException) -> dict[str, Any]:
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
        #: after a gateway stop, from either side, no channel is created
        self.stopping = False
        self._outgoing_send.send_nowait(PREAMBLE)

    # -- channels -------------------------------------------------------------

    def new_channel(self) -> Channel:
        if self.failure is not None:
            raise self.failure
        if self.stopping:
            msg = "the gateway is stopping: no new channels"
            raise StateError(msg)
        channel = Channel(self, self._next_id)
        self._next_id += 2
        self._channels[channel.id] = channel
        return channel

    def channels(self) -> list[Channel]:
        """The channels open on this connection."""
        return list(self._channels.values())

    def forget(self, channel: Channel) -> None:
        self._channels.pop(channel.id, None)

    def channel_id(self, channel: Any) -> int:
        # a sync handler holds its channels wrapped for its thread
        unwrapped: Channel = getattr(channel, "async_channel", channel)
        if unwrapped._connection is not self:
            msg = f"{unwrapped!r} belongs to another gateway"
            raise StateError(msg)
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
        with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
            self._outgoing_send.send_nowait(encode_frame(Frame(kind, channel, payload)))

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
        elif kind == FrameType.GATEWAY_STOP:
            self.stopping = True
        elif kind == FrameType.OPEN:
            if self._on_open is None:
                msg = "the caller was asked to open a channel"
                raise FrameError(msg)
            opened = self._channel_for(frame.channel)
            if self.stopping:
                opened.close(error=StateError("the gateway is stopping"))
                return
            self._on_open(opened, frame.payload)
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
        elif kind == FrameType.STOP:
            # a stop for a channel already closed found its work done
            channel = self._channels.get(frame.channel)
            if channel is not None:
                channel._stopped_by_peer(decode(frame.payload))
        elif kind == FrameType.CLOSE:
            channel = self._channels.get(frame.channel)
            if channel is not None:
                info = decode(frame.payload, self._channel_for)
                # a half-closed channel stays routed: its full close, with the
                # result or error, is still to come
                if "ends" not in info:
                    del self._channels[frame.channel]
                channel._closed_by_peer(info)

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
            msg = f"expected a {kind.name} frame, got {frame.type.name}"
            raise WorkerGone(msg)
        return frame
