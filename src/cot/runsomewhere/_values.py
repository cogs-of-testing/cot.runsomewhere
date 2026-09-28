"""The value codec: builtin values and channels, nothing that can run code."""

from __future__ import annotations

import struct
from collections.abc import Callable
from typing import Any

# one byte per type; nothing here names a class or a module
_NONE = b"N"
_TRUE = b"T"
_FALSE = b"F"
_INT = b"I"
_FLOAT = b"D"
_COMPLEX = b"C"
_STR = b"S"
_BYTES = b"B"
_TUPLE = b"("
_LIST = b"["
_DICT = b"{"
_SET = b"<"
_FROZENSET = b">"
_CHANNEL = b"@"

_U32 = struct.Struct(">I")
_DOUBLE = struct.Struct(">d")
_COMPLEX_PAIR = struct.Struct(">dd")

_SEQUENCES: dict[type, bytes] = {
    tuple: _TUPLE,
    list: _LIST,
    set: _SET,
    frozenset: _FROZENSET,
}

ChannelToId = Callable[[Any], int]
IdToChannel = Callable[[int], Any]


class DecodeError(ValueError):
    """The bytes are not a valid encoded value."""


def is_channel(value: object) -> bool:
    return getattr(type(value), "_rsh_channel", False) is True


def encode(value: object, channels: ChannelToId | None = None) -> bytes:
    """Encode a sendable value; raise TypeError for anything else."""
    out = bytearray()
    try:
        _encode(value, out, set(), channels)
    except RecursionError:
        raise TypeError("value nests too deeply to send") from None
    return bytes(out)


def can_send(value: object) -> bool:
    """Whether a value can cross a channel, checked without sending it."""
    try:
        encode(value, channels=lambda channel: 0)
    except TypeError:
        return False
    return True


def _encode(
    value: object, out: bytearray, active: set[int], channels: ChannelToId | None
) -> None:
    kind = type(value)
    if value is None:
        out += _NONE
    elif kind is bool:
        out += _TRUE if value else _FALSE
    elif kind is int:
        assert isinstance(value, int)
        size = (value.bit_length() + 8) // 8
        out += _INT + _U32.pack(size) + value.to_bytes(size, "big", signed=True)
    elif kind is float:
        out += _FLOAT + _DOUBLE.pack(value)
    elif kind is complex:
        assert isinstance(value, complex)
        out += _COMPLEX + _COMPLEX_PAIR.pack(value.real, value.imag)
    elif kind is str:
        assert isinstance(value, str)
        data = value.encode("utf-8", "surrogatepass")
        out += _STR + _U32.pack(len(data)) + data
    elif kind is bytes:
        assert isinstance(value, bytes)
        out += _BYTES + _U32.pack(len(value)) + value
    elif kind in _SEQUENCES or kind is dict:
        if id(value) in active:
            raise TypeError("cannot send a container that contains itself")
        active.add(id(value))
        if kind is dict:
            assert isinstance(value, dict)
            out += _DICT + _U32.pack(len(value))
            for key, item in value.items():
                _encode(key, out, active, channels)
                _encode(item, out, active, channels)
        else:
            assert isinstance(value, (tuple, list, set, frozenset))
            out += _SEQUENCES[kind] + _U32.pack(len(value))
            for item in value:
                _encode(item, out, active, channels)
        active.discard(id(value))
    elif channels is not None and is_channel(value):
        out += _CHANNEL + _U32.pack(channels(value))
    else:
        raise TypeError(f"cannot send {kind.__qualname__} values: {value!r}")


def decode(data: bytes, channels: IdToChannel | None = None) -> Any:
    """Decode one value; raise DecodeError for anything malformed."""
    reader = _Reader(data, channels)
    try:
        value = reader.value()
    except RecursionError:
        raise DecodeError("value nests too deeply") from None
    if reader.position != len(data):
        raise DecodeError(f"{len(data) - reader.position} trailing bytes")
    return value


class _Reader:
    def __init__(self, data: bytes, channels: IdToChannel | None) -> None:
        self.data = data
        self.position = 0
        self.channels = channels

    def take(self, count: int) -> bytes:
        end = self.position + count
        if end > len(self.data):
            raise DecodeError("truncated value")
        chunk = self.data[self.position : end]
        self.position = end
        return chunk

    def u32(self) -> int:
        count: int = _U32.unpack(self.take(4))[0]
        return count

    def value(self) -> Any:
        tag = self.take(1)
        if tag == _NONE:
            return None
        if tag == _TRUE:
            return True
        if tag == _FALSE:
            return False
        if tag == _INT:
            return int.from_bytes(self.take(self.u32()), "big", signed=True)
        if tag == _FLOAT:
            return _DOUBLE.unpack(self.take(8))[0]
        if tag == _COMPLEX:
            return complex(*_COMPLEX_PAIR.unpack(self.take(16)))
        if tag == _STR:
            try:
                return self.take(self.u32()).decode("utf-8", "surrogatepass")
            except UnicodeDecodeError as error:
                raise DecodeError(str(error)) from None
        if tag == _BYTES:
            return self.take(self.u32())
        if tag == _TUPLE:
            return tuple(self.items())
        if tag == _LIST:
            return list(self.items())
        if tag in (_SET, _FROZENSET):
            kind = set if tag == _SET else frozenset
            try:
                return kind(self.items())
            except TypeError as error:
                raise DecodeError(f"unhashable set member: {error}") from None
        if tag == _DICT:
            count = self.u32()
            result = {}
            for _ in range(count):
                key = self.value()
                try:
                    result[key] = self.value()
                except TypeError as error:
                    raise DecodeError(f"unhashable dict key: {error}") from None
            return result
        if tag == _CHANNEL and self.channels is not None:
            return self.channels(self.u32())
        raise DecodeError(f"unknown tag {tag!r}")

    def items(self) -> list[Any]:
        return [self.value() for _ in range(self.u32())]
