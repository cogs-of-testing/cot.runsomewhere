"""The value codec: builtin values, nothing that can run code.

It knows nothing of channels. A value it has no encoding for goes to
``default(value)``, which returns ``(code, inner)``; the codec writes an
extension item, and on decode hands ``(code, inner)`` to ``ext_hook``. The
caller owns the codes and what they mean.

The format, one tag byte per item, multi-byte fields big-endian:

====================  =========================================================
``0x80 | n``          int ``n``, 0 <= n < 64
``0xC0 | n``          str of ``n`` UTF-8 bytes, n < 32
``N`` ``T`` ``F``     None, True, False
``1`` ``2`` ``4``     int in 8, 16 or 32 bits, signed
``i`` / ``I``         int as signed bytes, their count in 8 / 32 bits
``D`` ``C``           float, complex: IEEE 754 doubles
``s`` / ``S``         str, its byte count in 8 / 32 bits
``b`` / ``B``         bytes, likewise
``[`` / ``]``         list, its item count in 8 / 32 bits, then the items
``(`` / ``)``         tuple, likewise
``<`` / ``l``         set, likewise
``>`` / ``g``         frozenset, likewise
``{`` / ``}``         dict, its pair count in 8 / 32 bits, then key, value, ...
``X``                 extension: a code byte, then one item
====================  =========================================================

Fixed widths keep the tag alone enough to know what follows, so a decoder
never resumes inside a variable-length field.

This module is the reference. The optional ``cot-runsomewhere-speedups``
distribution provides the same codec in C, as ``_cot_runsomewhere_speedups``;
`encode` and `decode` use it when it is installed and its FORMAT matches,
unless ``COT_RUNSOMEWHERE_PURE`` is set in the environment.
"""

from __future__ import annotations

import os
import struct
from collections.abc import Callable
from typing import Any

#: the version of the format below; the C codec must report the same
FORMAT = 1

#: how many containers and extensions may enclose an item, on both sides:
#: the same limit for every implementation, so none accepts what another
#: refuses
MAX_DEPTH = 200

Default = Callable[[Any], tuple[int, Any]]
ExtHook = Callable[[int, Any], Any]

_I8 = struct.Struct(">b")
_I16 = struct.Struct(">h")
_I32 = struct.Struct(">i")
_U32 = struct.Struct(">I")
_DOUBLE = struct.Struct(">d")
_COMPLEX = struct.Struct(">dd")

_FIXINT = 0x80
_FIXSTR = 0xC0
_NONE, _TRUE, _FALSE = b"N", b"T", b"F"
_INT8, _INT16, _INT32 = b"1", b"2", b"4"
_FLOAT, _COMPLEX_TAG = b"D", b"C"
_EXT = 0x58  # X

# the tags with a length or count, as (8-bit form, 32-bit form)
_STR = (0x73, 0x53)  # s S
_BYTES = (0x62, 0x42)  # b B
_BIGINT = (0x69, 0x49)  # i I
_LIST = (0x5B, 0x5D)  # [ ]
_TUPLE = (0x28, 0x29)  # ( )
_SET = (0x3C, 0x6C)  # < l
_FROZENSET = (0x3E, 0x67)  # > g
_DICT = (0x7B, 0x7D)  # { }

_SEQUENCES: dict[type, tuple[int, int]] = {
    list: _LIST,
    tuple: _TUPLE,
    set: _SET,
    frozenset: _FROZENSET,
}


class DecodeError(ValueError):
    """The bytes are not a valid encoded value."""


# -- encoding -------------------------------------------------------------------


def py_encode(value: object, default: Default | None = None) -> bytes:
    """Encode a sendable value; raise TypeError for anything else."""
    out = bytearray()
    _encode(value, out, default, 0)
    return bytes(out)


def _length(out: bytearray, tags: tuple[int, int], count: int) -> None:
    if count < 256:
        out.append(tags[0])
        out.append(count)
    elif count <= 0xFFFFFFFF:
        out.append(tags[1])
        out += _U32.pack(count)
    else:
        msg = f"too large to send: {count} bytes or items"
        raise TypeError(msg)


def _encode(value: object, out: bytearray, default: Default | None, depth: int) -> None:
    if depth > MAX_DEPTH:
        msg = "value nests too deeply, or contains itself"
        raise TypeError(msg)
    kind = type(value)
    if kind is str:
        assert isinstance(value, str)
        data = value.encode("utf-8", "surrogatepass")
        if len(data) < 32:
            out.append(_FIXSTR | len(data))
        else:
            _length(out, _STR, len(data))
        out += data
    elif kind is int:
        assert isinstance(value, int)
        _encode_int(value, out)
    elif kind is dict:
        assert isinstance(value, dict)
        _length(out, _DICT, len(value))
        for key, item in value.items():
            _encode(key, out, default, depth + 1)
            _encode(item, out, default, depth + 1)
    elif kind in _SEQUENCES:
        assert isinstance(value, (list, tuple, set, frozenset))
        _length(out, _SEQUENCES[kind], len(value))
        for item in value:
            _encode(item, out, default, depth + 1)
    elif value is None:
        out += _NONE
    elif kind is bool:
        out += _TRUE if value else _FALSE
    elif kind is float:
        assert isinstance(value, float)
        out += _FLOAT + _DOUBLE.pack(value)
    elif kind is bytes:
        assert isinstance(value, bytes)
        _length(out, _BYTES, len(value))
        out += value
    elif kind is complex:
        assert isinstance(value, complex)
        out += _COMPLEX_TAG + _COMPLEX.pack(value.real, value.imag)
    elif default is not None:
        code, inner = default(value)
        out.append(_EXT)
        out.append(code)
        _encode(inner, out, default, depth + 1)
    else:
        msg = f"cannot send {kind.__qualname__} values: {value!r}"
        raise TypeError(msg)


def _encode_int(value: int, out: bytearray) -> None:
    if 0 <= value < 64:
        out.append(_FIXINT | value)
    elif -128 <= value < 128:
        out += _INT8 + _I8.pack(value)
    elif -32768 <= value < 32768:
        out += _INT16 + _I16.pack(value)
    elif -(2**31) <= value < 2**31:
        out += _INT32 + _I32.pack(value)
    else:
        size = (value.bit_length() + 8) // 8
        _length(out, _BIGINT, size)
        out += value.to_bytes(size, "big", signed=True)


# -- decoding -------------------------------------------------------------------

# each reader takes (data, position, tag, ext_hook, depth) and returns
# (value, position after it); a short buffer shows as IndexError or
# struct.error, which decode() reports as truncation
_Reader = Callable[[bytes, int, int, "ExtHook | None", int], tuple[Any, int]]
_READERS: list[_Reader | None] = [None] * 256


def py_decode(data: bytes, ext_hook: ExtHook | None = None) -> Any:
    """Decode one value; raise DecodeError for anything malformed."""
    data = bytes(data)
    try:
        value, end = _item(data, 0, ext_hook, 0)
    except (IndexError, struct.error):
        msg = "truncated value"
        raise DecodeError(msg) from None
    if end != len(data):
        msg = f"{len(data) - end} trailing bytes"
        raise DecodeError(msg)
    return value


def _item(data: bytes, pos: int, ext_hook: ExtHook | None, depth: int) -> Any:
    if depth > MAX_DEPTH:
        msg = "value nests too deeply"
        raise DecodeError(msg)
    tag = data[pos]
    # the one-byte forms are most items in practice: no call for them
    if tag & 0xC0 == _FIXINT:
        return tag & 0x3F, pos + 1
    if tag & 0xE0 == _FIXSTR:
        end = pos + 1 + (tag & 0x1F)
        if end > len(data):
            msg = "truncated value"
            raise DecodeError(msg)
        try:
            return data[pos + 1 : end].decode("utf-8", "surrogatepass"), end
        except UnicodeDecodeError as error:
            raise DecodeError(str(error)) from None
    reader = _READERS[tag]
    if reader is None:
        msg = f"unknown tag 0x{tag:02x}"
        raise DecodeError(msg)
    return reader(data, pos + 1, tag, ext_hook, depth)


def _count(data: bytes, pos: int, tag: int, tags: tuple[int, int]) -> tuple[int, int]:
    if tag == tags[0]:
        return data[pos], pos + 1
    return _U32.unpack_from(data, pos)[0], pos + 4


def _raw(data: bytes, pos: int, size: int) -> tuple[bytes, int]:
    end = pos + size
    if end > len(data):
        msg = "truncated value"
        raise DecodeError(msg)
    return data[pos:end], end


def _text(data: bytes, pos: int, size: int) -> tuple[str, int]:
    raw, end = _raw(data, pos, size)
    try:
        return raw.decode("utf-8", "surrogatepass"), end
    except UnicodeDecodeError as error:
        raise DecodeError(str(error)) from None


def _constant(value: object) -> _Reader:
    def read(_data: bytes, pos: int, *_: object) -> tuple[Any, int]:
        return value, pos

    return read


def _fixed(layout: struct.Struct) -> _Reader:
    def read(data: bytes, pos: int, *_: object) -> tuple[Any, int]:
        return layout.unpack_from(data, pos)[0], pos + layout.size

    return read


def _read_complex(data: bytes, pos: int, *_: object) -> tuple[Any, int]:
    return complex(*_COMPLEX.unpack_from(data, pos)), pos + 16


def _read_str(data: bytes, pos: int, tag: int, *_: object) -> tuple[Any, int]:
    size, pos = _count(data, pos, tag, _STR)
    return _text(data, pos, size)


def _read_bytes(data: bytes, pos: int, tag: int, *_: object) -> tuple[Any, int]:
    size, pos = _count(data, pos, tag, _BYTES)
    return _raw(data, pos, size)


def _read_bigint(data: bytes, pos: int, tag: int, *_: object) -> tuple[Any, int]:
    size, pos = _count(data, pos, tag, _BIGINT)
    raw, pos = _raw(data, pos, size)
    return int.from_bytes(raw, "big", signed=True), pos


def _items(
    data: bytes, pos: int, count: int, ext_hook: ExtHook | None, depth: int
) -> tuple[list[Any], int]:
    # every item takes a byte at least: a count beyond the data is a lie, and
    # must not make us loop or allocate for it
    if pos + count > len(data):
        msg = "truncated value"
        raise DecodeError(msg)
    items = []
    for _ in range(count):
        item, pos = _item(data, pos, ext_hook, depth)
        items.append(item)
    return items, pos


def _sequence(tags: tuple[int, int], kind: type) -> _Reader:
    def read(
        data: bytes, pos: int, tag: int, ext_hook: ExtHook | None, depth: int
    ) -> tuple[Any, int]:
        count, pos = _count(data, pos, tag, tags)
        items, pos = _items(data, pos, count, ext_hook, depth + 1)
        if kind is list:
            return items, pos
        try:
            return kind(items), pos
        except TypeError as error:
            msg = f"unhashable set member: {error}"
            raise DecodeError(msg) from None

    return read


def _read_dict(
    data: bytes, pos: int, tag: int, ext_hook: ExtHook | None, depth: int
) -> tuple[Any, int]:
    count, pos = _count(data, pos, tag, _DICT)
    flat, pos = _items(data, pos, 2 * count, ext_hook, depth + 1)
    try:
        return dict(zip(flat[::2], flat[1::2], strict=True)), pos
    except TypeError as error:
        msg = f"unhashable dict key: {error}"
        raise DecodeError(msg) from None


def _read_ext(
    data: bytes, pos: int, tag: int, ext_hook: ExtHook | None, depth: int
) -> tuple[Any, int]:
    if ext_hook is None:
        msg = f"unknown tag 0x{tag:02x}"
        raise DecodeError(msg)
    code = data[pos]
    inner, pos = _item(data, pos + 1, ext_hook, depth + 1)
    return ext_hook(code, inner), pos


def _install() -> None:
    single: dict[bytes, _Reader] = {
        _NONE: _constant(None),
        _TRUE: _constant(True),
        _FALSE: _constant(False),
        _INT8: _fixed(_I8),
        _INT16: _fixed(_I16),
        _INT32: _fixed(_I32),
        _FLOAT: _fixed(_DOUBLE),
        _COMPLEX_TAG: _read_complex,
    }
    for code, reader in single.items():
        _READERS[code[0]] = reader
    paired: dict[tuple[int, int], _Reader] = {
        _STR: _read_str,
        _BYTES: _read_bytes,
        _BIGINT: _read_bigint,
        _DICT: _read_dict,
        **{tags: _sequence(tags, kind) for kind, tags in _SEQUENCES.items()},
    }
    for tags, paired_reader in paired.items():
        for tag in tags:
            _READERS[tag] = paired_reader
    _READERS[_EXT] = _read_ext


_install()


def _load_speedups() -> Any:
    if os.environ.get("COT_RUNSOMEWHERE_PURE"):
        return None
    try:
        import _cot_runsomewhere_speedups as speedups  # noqa: PLC0415
    except ImportError:
        return None
    # a C codec for another format would garble the wire, not fail loudly
    if getattr(speedups, "FORMAT", None) != FORMAT:
        return None
    return speedups


#: the C codec in use, or None for the pure-Python one
speedups = _load_speedups()


def py_decode_recording(
    data: bytes, ext_hook: ExtHook | None = None
) -> tuple[Any, list[Any] | None]:
    """The value, and no record: the pure codec does not record carriers."""
    return py_decode(data, ext_hook), None


decode_recording = py_decode_recording

if speedups is None:
    encode = py_encode
    decode = py_decode
else:
    _c_encode = speedups.encode
    _c_decode = speedups.decode

    def encode(value: object, default: Default | None = None) -> bytes:
        """Encode a sendable value; raise TypeError for anything else."""
        result: bytes = _c_encode(value, default)
        return result

    def decode(data: bytes, ext_hook: ExtHook | None = None) -> Any:
        """Decode one value; raise DecodeError for anything malformed."""
        return _c_decode(data, ext_hook, DecodeError)

    if getattr(speedups, "RECORDS_CARRIERS", 0):

        def decode_recording(
            data: bytes, ext_hook: ExtHook | None = None
        ) -> tuple[Any, list[Any] | None]:
            """The value, and every container in it holding an extension item
            at any depth, innermost first."""
            carriers: list[Any] = []
            return _c_decode(data, ext_hook, DecodeError, carriers), carriers
