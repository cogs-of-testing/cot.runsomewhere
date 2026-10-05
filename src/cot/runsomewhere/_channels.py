"""Channels and the connection that multiplexes them over one byte stream."""

from __future__ import annotations

import contextlib
import logging
import math
import traceback
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any, cast

import anyio
import anyio.abc
from anyio.lowlevel import cancel_shielded_checkpoint, checkpoint_if_cancelled

from ._control import ProtocolError, message, unknown
from ._errors import ChannelClosed, ItemsDiscarded, RemoteError, StateError, WorkerGone
from ._frames import MAX_FIELD, PREAMBLE, Frame, FrameDecoder, FrameError, encode_frame
from ._values import DecodeError, decode, decode_recording, encode
from ._version import version as __version__

log = logging.getLogger(__name__)

DEFAULT_WINDOW = 1024 * 1024
#: an item larger than this goes out in fragments
FRAGMENT = 64 * 1024
#: the largest item, encoded; bulk data belongs in the transfer service
MAX_ITEM = 64 * 1024 * 1024
#: how long credit with nothing to ride on waits for a reply to carry it
CREDIT_DELAY = 0.001

OpenHandler = Callable[["Channel", dict[str, Any]], None]

#: the value codec's extension code for a channel; the codec knows no more
CHANNEL_REFERENCE = 0


def is_channel(value: object) -> bool:
    return getattr(type(value), "_rsh_channel", False) is True


def _any_channel(value: object) -> tuple[int, int]:
    if is_channel(value):
        return CHANNEL_REFERENCE, 0
    msg = f"cannot send {type(value).__qualname__} values: {value!r}"
    raise TypeError(msg)


def can_send(value: object) -> bool:
    """Whether a value can cross a channel, checked without sending it."""
    try:
        encode(value, _any_channel)
    except TypeError:
        return False
    return True


class Channel:
    """An ordered, two-way stream of values with the peer."""

    _rsh_channel = True
    #: carries a tunnelled gateway: an edge of the teardown graph
    tunnel = False

    def __init__(self, connection: Connection, channel_id: int) -> None:
        self._connection = connection
        self.id = channel_id
        #: value, encoded size, whether it carries channels, and the containers
        #: in it holding them, innermost first, where the decoder recorded them
        self._items: deque[tuple[Any, int, bool, list[Any] | None]] = deque()
        self._item_arrived: anyio.Event | None = None
        self._credit = DEFAULT_WINDOW
        self._credit_arrived: anyio.Event | None = None
        #: bytes taken from the peer and not yet reported back as credit
        self._unreported = 0
        #: credit reported back in total, and bytes the peer sent in total:
        #: what the peer has left to send is the window, plus the one, less
        #: the other
        self._reported = 0
        self._received_bytes = 0
        #: sizes of the items sent and not yet taken, oldest first
        self._outstanding: deque[int] = deque()
        self._granted_unmatched = 0
        self._taken_by_peer = 0
        self._granted_total = 0
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
        payload = self._connection.encode_value(value)
        if len(payload) > MAX_ITEM:
            msg = f"an item of {len(payload)} bytes is over the limit of {MAX_ITEM}"
            raise StateError(msg)
        while self._credit <= 0:
            await self._wait_for_credit()
            self._raise_if_unusable()
        self._credit -= len(payload)
        self._outstanding.append(len(payload))
        self._connection.send_item(self.id, payload, self._take_unreported())
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
        value, _, _ = await self.receive_item()
        return value

    async def receive_item(self) -> tuple[Any, bool, list[Any] | None]:
        """The next item, whether channels arrived in it, and the containers
        holding them, innermost first, when the decoder recorded them."""
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
        value, size, carries_channels, carriers = self._items.popleft()
        self._consumed(size)
        await cancel_shielded_checkpoint()
        return value, carries_channels, carriers

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
        fields = {} if deadline is None else {"deadline": deadline}
        self._connection.send_control("stop", channel=self.id, **fields)

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
            # the peer learns how much was taken from the credit before it
            self._flush_credit()
            ends = "send" if send else "receive"
            self._connection.send_control("close", channel=self.id, ends=ends)
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
                    self._connection.encode_value(result)
                except TypeError as unsendable:
                    info = _error_info(unsendable)
                else:
                    info = {"result": result}
            self._flush_credit()
            self._connection.send_control("close", channel=self.id, **info)
        self._connection.forget(self)
        self._wake()

    # -- driven by the connection ---------------------------------------------

    def _deliver(
        self,
        value: Any,
        size: int,
        carries_channels: bool,
        carriers: list[Any] | None = None,
    ) -> None:
        self._received_bytes += size
        if self._ended_receiving:
            return
        self._items.append((value, size, carries_channels, carriers))
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
        self._flush_credit()
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
        self._unreported += size
        peer_left = self._reported + DEFAULT_WINDOW - self._received_bytes
        if self._unreported >= DEFAULT_WINDOW // 4 or peer_left < DEFAULT_WINDOW // 4:
            # never hold credit a sender may be blocked on
            self._flush_credit()
        elif not self._items:
            self._connection.credit_pending(self)

    def _take_unreported(self) -> int:
        taken, self._unreported = self._unreported, 0
        self._reported += taken
        return taken

    def _flush_credit(self) -> None:
        taken = self._take_unreported()
        if taken:
            self._connection.send_frame(self.id, b"", taken)

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
        self._control_send, self.control = anyio.create_memory_object_stream[
            dict[str, Any]
        ](math.inf)
        #: channels holding credit with nothing yet to carry it
        self._credit_waiting: dict[int, Channel] = {}
        #: fragments of the item in progress, by channel
        self._fragments: dict[int, list[bytes]] = {}
        #: set by decoding an item with a channel in it
        self._decoded_a_channel = False
        #: the other side, as warnings name it; set once the handshake is done
        self.peer = "the other side"
        self.failure: WorkerGone | None = None
        self.gone = anyio.Event()
        #: after a gateway stop, the worker refuses new service calls; channels
        #: are still created, since a stopping service may need them
        self.stopping = False
        self._outgoing_send.send_nowait(PREAMBLE)

    # -- channels -------------------------------------------------------------

    def new_channel(self) -> Channel:
        if self.failure is not None:
            raise self.failure
        if self._next_id > MAX_FIELD:
            msg = "this gateway has used up its channel ids"
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

    def encode_value(self, value: object) -> bytes:
        """Encode a value for this connection: channels in it become references
        the peer resolves to its end of each."""
        return encode(value, self._channel_reference)

    def decode_value(self, payload: bytes) -> Any:
        return decode(payload, self._resolve_reference)

    def _channel_reference(self, value: object) -> tuple[int, int]:
        if not is_channel(value):
            msg = f"cannot send {type(value).__qualname__} values: {value!r}"
            raise TypeError(msg)
        # a sync handler holds its channels wrapped for its thread
        unwrapped = cast("Channel", getattr(value, "async_channel", value))
        if unwrapped._connection is not self:
            msg = f"{unwrapped!r} belongs to another gateway"
            raise StateError(msg)
        return CHANNEL_REFERENCE, unwrapped.id

    def _resolve_reference(self, code: int, channel_id: Any) -> Channel:
        if code != CHANNEL_REFERENCE or type(channel_id) is not int:
            msg = f"unknown extension {code} with {channel_id!r}"
            raise DecodeError(msg)
        self._decoded_a_channel = True
        return self._channel_for(channel_id)

    def _channel_for(self, channel_id: int) -> Channel:
        channel = self._channels.get(channel_id)
        if channel is None:
            channel = Channel(self, channel_id)
            self._channels[channel_id] = channel
        return channel

    # -- frames ---------------------------------------------------------------

    def send_frame(self, channel: int, payload: bytes = b"", taken: int = 0) -> None:
        if self.failure is not None:
            return
        with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
            self._outgoing_send.send_nowait(encode_frame(channel, payload, taken))

    def send_item(self, channel: int, payload: bytes, taken: int = 0) -> None:
        """Queue one encoded item, in fragments when it is large; the first
        frame carries the credit."""
        if self.failure is not None:
            return
        frames = []
        for start in range(0, len(payload), FRAGMENT):
            end = start + FRAGMENT
            frames.append(
                encode_frame(
                    channel, payload[start:end], taken, more=end < len(payload)
                )
            )
            taken = 0
        with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
            # queued together: no other frame on this channel comes between
            self._outgoing_send.send_nowait(b"".join(frames))

    def send_control(self, op: str, **fields: Any) -> None:
        self.send_item(0, self.encode_value(message(op, **fields)))

    def credit_pending(self, channel: Channel) -> None:
        """Hold the channel's credit until a frame can carry it, or until the
        writer would otherwise go idle for `CREDIT_DELAY`."""
        if self.stopping:
            channel._flush_credit()
            return
        if not self._credit_waiting:
            # an idle writer waits for frames, not for credit: an empty chunk
            # wakes it to start the delay
            with contextlib.suppress(
                anyio.ClosedResourceError, anyio.BrokenResourceError
            ):
                self._outgoing_send.send_nowait(b"")
        self._credit_waiting[channel.id] = channel

    def _flush_waiting_credit(self) -> None:
        waiting, self._credit_waiting = self._credit_waiting, {}
        for channel in waiting.values():
            channel._flush_credit()

    def finish_sending(self) -> None:
        """Close the stream once everything queued so far is written."""
        self._flush_waiting_credit()
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
            while True:
                chunks = self._queued()
                if not chunks:
                    chunks = await self._next_chunks()
                data = b"".join(chunks)
                if data:
                    await self._stream.send(data)
        except anyio.EndOfStream:
            with contextlib.suppress(
                anyio.BrokenResourceError, anyio.ClosedResourceError, OSError
            ):
                await self._stream.send_eof()
        except (anyio.BrokenResourceError, anyio.ClosedResourceError, OSError):
            pass

    def _queued(self) -> list[bytes]:
        """Everything queued now, to go out in one write."""
        chunks: list[bytes] = []
        try:
            while True:
                chunks.append(self._outgoing.receive_nowait())
        except anyio.WouldBlock:
            return chunks
        except anyio.EndOfStream:
            if chunks:
                return chunks
            raise

    async def _next_chunks(self) -> list[bytes]:
        """Wait for something to write; credit waiting for a reply to ride on
        goes out on its own once the writer has been idle for CREDIT_DELAY."""
        while self._credit_waiting:
            with anyio.move_on_after(CREDIT_DELAY):
                return [await self._outgoing.receive()]
            self._flush_waiting_credit()
            chunks = self._queued()
            if chunks:
                return chunks
        return [await self._outgoing.receive()]

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
            except (FrameError, DecodeError, ProtocolError) as error:
                self._set_gone(f"invalid frame stream: {error}")
                return

    def _dispatch(self, frame: Frame) -> None:
        if frame.taken:
            channel = self._channels.get(frame.channel)
            if channel is not None:
                channel._grant(frame.taken)
        if frame.abort:
            self._fragments.pop(frame.channel, None)
            return
        if frame.more:
            parts = self._fragments.setdefault(frame.channel, [])
            parts.append(frame.payload)
            if sum(map(len, parts)) > MAX_ITEM:
                msg = f"an item on channel {frame.channel} is over {MAX_ITEM} bytes"
                raise FrameError(msg)
            return
        payload = frame.payload
        earlier = self._fragments.pop(frame.channel, None)
        if earlier is not None:
            payload = b"".join([*earlier, payload])
        elif not payload:
            return  # credit only
        if frame.channel == 0:
            self._on_control(self.decode_value(payload))
            return
        channel = self._channels.get(frame.channel)
        if channel is not None:
            self._decoded_a_channel = False
            value, carriers = decode_recording(payload, self._resolve_reference)
            channel._deliver(value, len(payload), self._decoded_a_channel, carriers)

    def _on_control(self, value: Any) -> None:
        not_understood = unknown(value)
        if not_understood is not None:
            self._not_understood(value, not_understood)
            return
        op = value.pop("op")
        if op in ("hello", "config", "gateway-terminate"):
            self._control_send.send_nowait({"op": op, **value})
        elif op == "gateway-stop":
            self.stopping = True
            self._flush_waiting_credit()
        elif op == "open":
            if self._on_open is None:
                msg = "the caller was asked to open a channel"
                raise ProtocolError(msg)
            opened = self._channel_for(value["channel"])
            if self.stopping:
                opened.close(
                    error=StateError("the gateway is stopping: no new service calls")
                )
                return
            self._on_open(opened, value)
        elif op == "stop":
            # a stop for a channel already closed found its work done
            channel = self._channels.get(value["channel"])
            if channel is not None:
                channel._stopped_by_peer(value)
        elif op == "close":
            channel_id = value.pop("channel")
            channel = self._channels.get(channel_id)
            if channel is not None:
                # a half-closed channel stays routed: its full close, with the
                # result or error, is still to come
                if "ends" not in value:
                    del self._channels[channel_id]
                channel._closed_by_peer(value)

    def _not_understood(self, value: dict[str, Any], what: str) -> None:
        """A newer peer may send what this side does not know: only the channel
        the message names fails, on both sides, and the gateway goes on."""
        channel_id = value.get("channel")
        named = f" on channel {channel_id}" if type(channel_id) is int else ""
        log.warning(
            "%s sent %s, which runsomewhere %s does not know%s",
            self.peer,
            what,
            __version__,
            named,
        )
        if type(channel_id) is not int:
            return
        channel = self._channels.get(channel_id)
        if channel is None:
            return
        error = StateError(f"{self.peer} sent {what}, which this side does not know")
        channel.close(error=error)
        channel._fail(error)

    def _set_gone(self, reason: str) -> None:
        if self.failure is not None:
            return
        self.failure = WorkerGone(f"worker gone: {reason}")
        self.gone.set()
        self._control_send.close()
        self._outgoing_send.close()
        for channel in list(self._channels.values()):
            channel._fail(self.failure)

    async def next_control(self, op: str) -> dict[str, Any]:
        """The next handshake or gateway-level message, which must be ``op``."""
        try:
            value = await self.control.receive()
        except anyio.EndOfStream:
            assert self.failure is not None
            raise self.failure from None
        if value["op"] != op:
            msg = f"expected {op}, got {value['op']}"
            raise WorkerGone(msg)
        return value
