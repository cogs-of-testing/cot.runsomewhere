"""Sans-IO framing: bytes in, frames out.

A frame is a tag byte, up to three big-endian fields of 0 to 3 bytes each,
and a payload::

    tag: [ more | abort | chan:2 | len:2 | taken:2 ]   then chan, len, taken

Each 2-bit code is the byte count of its field; a field of 0 takes no bytes.
"""

from __future__ import annotations

from typing import NamedTuple

PROTOCOL_VERSION = 2
MAGIC = b"\x89RSH"
PREAMBLE = MAGIC + bytes([PROTOCOL_VERSION])
#: the largest value a header field carries
MAX_FIELD = (1 << 24) - 1

_MORE = 0x80
_ABORT = 0x40
# the header length, tag byte included, for each combination of field sizes
_HEADER_SIZE = [1 + (code >> 4) + ((code >> 2) & 3) + (code & 3) for code in range(64)]


class FrameError(ValueError):
    """The byte stream is not a valid runsomewhere frame stream."""


class Frame(NamedTuple):
    #: the channel; 0 is the control channel
    channel: int
    payload: bytes = b""
    #: bytes of the other direction taken since the last report: credit
    taken: int = 0
    #: a fragment, with more of the same item to follow
    more: bool = False
    #: the item in progress on this channel is dropped
    abort: bool = False


def _size(value: int) -> int:
    if value < 0 or value > MAX_FIELD:
        msg = f"frame field out of range: {value}"
        raise FrameError(msg)
    return (value.bit_length() + 7) // 8


def encode_frame(
    channel: int,
    payload: bytes = b"",
    taken: int = 0,
    *,
    more: bool = False,
    abort: bool = False,
) -> bytes:
    length = len(payload)
    if abort and (more or length):
        msg = "an abort carries nothing and continues nothing"
        raise FrameError(msg)
    c, n, t = _size(channel), _size(length), _size(taken)
    tag = (_MORE if more else 0) | (_ABORT if abort else 0) | c << 4 | n << 2 | t
    return b"".join(
        (
            bytes((tag,)),
            channel.to_bytes(c, "big"),
            length.to_bytes(n, "big"),
            taken.to_bytes(t, "big"),
            payload,
        )
    )


class FrameDecoder:
    """Turns arbitrary chunks of a stream into frames; never reads or waits."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._seen_preamble = False
        self._error: FrameError | None = None

    def feed(self, data: bytes) -> list[Frame]:
        if self._error is not None:
            raise self._error
        self._buffer += data
        try:
            return self._drain()
        except FrameError as error:
            self._error = error
            raise

    def _drain(self) -> list[Frame]:
        buffer = self._buffer
        if not self._seen_preamble:
            # checked as soon as bytes arrive, so an unrelated program at the
            # other end fails on its first write rather than on a full frame
            head = bytes(buffer[: len(MAGIC)])
            if not MAGIC.startswith(head):
                msg = f"not a runsomewhere stream: starts with {head!r}"
                raise FrameError(msg)
            if len(buffer) < len(PREAMBLE):
                return []
            version = buffer[len(MAGIC)]
            if version != PROTOCOL_VERSION:
                msg = f"protocol version {version}, this side speaks {PROTOCOL_VERSION}"
                raise FrameError(msg)
            del buffer[: len(PREAMBLE)]
            self._seen_preamble = True

        frames = []
        pos = 0
        available = len(buffer)
        while pos < available:
            tag = buffer[pos]
            if tag & _ABORT and tag & (_MORE | 0x0C):
                # an abort carries nothing and continues nothing
                msg = f"invalid frame tag 0x{tag:02x}"
                raise FrameError(msg)
            code = tag & 0x3F
            header = _HEADER_SIZE[code]
            if available - pos < header:
                break
            c, n = code >> 4, (code >> 2) & 3
            field = pos + 1
            channel = int.from_bytes(buffer[field : field + c], "big")
            field += c
            length = int.from_bytes(buffer[field : field + n], "big")
            field += n
            taken = int.from_bytes(buffer[field : pos + header], "big")
            end = pos + header + length
            if end > available:
                break
            frames.append(
                Frame(
                    channel,
                    bytes(buffer[pos + header : end]),
                    taken,
                    bool(tag & _MORE),
                    bool(tag & _ABORT),
                )
            )
            pos = end
        del buffer[:pos]
        return frames
