"""Sans-IO framing: bytes in, frames out."""

from __future__ import annotations

import enum
import struct
from typing import NamedTuple

PROTOCOL_VERSION = 1
MAGIC = b"\x89RSH"
PREAMBLE = MAGIC + bytes([PROTOCOL_VERSION])
HEADER = struct.Struct(">BII")
HEADER_SIZE = HEADER.size
MAX_PAYLOAD = 64 * 1024 * 1024


class FrameError(ValueError):
    """The byte stream is not a valid runsomewhere frame stream."""


class FrameType(enum.IntEnum):
    HELLO = 1
    CONFIG = 2
    OPEN = 3
    DATA = 4
    CREDIT = 5
    CLOSE = 6
    GATEWAY_CLOSE = 7


class Frame(NamedTuple):
    type: FrameType
    channel: int
    payload: bytes


def encode_frame(frame: Frame) -> bytes:
    if len(frame.payload) > MAX_PAYLOAD:
        raise FrameError(
            f"payload too large: {len(frame.payload)} bytes, limit {MAX_PAYLOAD}"
        )
    return HEADER.pack(frame.type, frame.channel, len(frame.payload)) + frame.payload


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
        if not self._seen_preamble:
            # checked as soon as bytes arrive, so an unrelated program at the
            # other end fails on its first write rather than on a full frame
            head = bytes(self._buffer[: len(MAGIC)])
            if not MAGIC.startswith(head):
                raise FrameError(f"not a runsomewhere stream: starts with {head!r}")
            if len(self._buffer) < len(PREAMBLE):
                return []
            version = self._buffer[len(MAGIC)]
            if version != PROTOCOL_VERSION:
                raise FrameError(
                    f"protocol version {version}, this side speaks {PROTOCOL_VERSION}"
                )
            del self._buffer[: len(PREAMBLE)]
            self._seen_preamble = True

        frames = []
        while len(self._buffer) >= HEADER_SIZE:
            kind, channel, length = HEADER.unpack_from(self._buffer)
            try:
                frame_type = FrameType(kind)
            except ValueError:
                raise FrameError(f"unknown frame type {kind}") from None
            if length > MAX_PAYLOAD:
                raise FrameError(
                    f"frame too large: {length} bytes, limit {MAX_PAYLOAD}"
                )
            end = HEADER_SIZE + length
            if len(self._buffer) < end:
                break
            frames.append(
                Frame(frame_type, channel, bytes(self._buffer[HEADER_SIZE:end]))
            )
            del self._buffer[:end]
        return frames
