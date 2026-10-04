import contextlib
import struct

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from cot.runsomewhere._values import MAX_DEPTH, DecodeError, decode, encode

scalars = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False)
    | st.complex_numbers(allow_nan=False)
    | st.text()
    | st.binary()
)
hashable = st.recursive(
    scalars,
    lambda inner: st.tuples(inner) | st.frozensets(inner, max_size=4),
    max_leaves=10,
)


def containers(keys):
    return lambda inner: (
        st.lists(inner, max_size=6)
        | st.tuples(inner, inner)
        | st.dictionaries(keys, inner, max_size=6)
    )


sendable = st.recursive(
    hashable | st.sets(hashable, max_size=4), containers(hashable), max_leaves=30
)
# no sets where bytes are compared: a set rebuilt from its decoded items may
# iterate in another order
ordered = st.recursive(scalars, containers(scalars), max_leaves=30)


def same(a, b):
    """Equal, and of the same type all the way down."""
    if type(a) is not type(b):
        return False
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(map(same, a, b))
    if isinstance(a, (set, frozenset)):
        return {(type(x), x) for x in a} == {(type(y), y) for y in b}
    if isinstance(a, dict):
        return {(type(k), k) for k in a} == {(type(k), k) for k in b} and all(
            same(a[k], b[k]) for k in a
        )
    return a == b


@given(sendable)
def test_every_sendable_value_roundtrips_with_its_exact_type(value):
    assert same(decode(encode(value)), value)


@given(ordered)
def test_reencoding_a_decoded_value_gives_the_same_bytes(value):
    data = encode(value)
    assert encode(decode(data)) == data


@given(st.integers())
@example(63)
@example(64)
@example(-129)
@example(2**31)
def test_an_int_takes_the_smallest_form_its_range_allows(value):
    if 0 <= value < 64:
        expected = 1
    elif -(2**7) <= value < 2**7:
        expected = 2
    elif -(2**15) <= value < 2**15:
        expected = 3
    elif -(2**31) <= value < 2**31:
        expected = 5
    else:
        expected = 2 + (value.bit_length() + 8) // 8
    assert len(encode(value)) == expected


@given(st.text())
def test_a_short_str_costs_one_byte_over_its_utf8(value):
    size = len(value.encode("utf-8", "surrogatepass"))
    overhead = 1 if size < 32 else 2 if size < 256 else 5
    assert len(encode(value)) == size + overhead


@given(st.binary(max_size=64))
def test_arbitrary_bytes_decode_to_a_value_or_a_decode_error(data):
    with contextlib.suppress(DecodeError):
        decode(data)


@given(ordered, st.data())
def test_damaged_encodings_decode_to_a_value_or_a_decode_error(value, data):
    damaged = bytearray(encode(value))
    damaged = damaged[: data.draw(st.integers(0, len(damaged)))]
    for _ in range(data.draw(st.integers(0, 3))):
        if damaged:
            at = data.draw(st.integers(0, len(damaged) - 1))
            damaged[at] = data.draw(st.integers(0, 255))
    with contextlib.suppress(DecodeError):
        decode(bytes(damaged))


def test_a_count_beyond_the_data_is_refused_without_allocating_for_it():
    with pytest.raises(DecodeError, match="truncated"):
        decode(b"]" + struct.pack(">I", 2**32 - 1))


class Marker:
    def __init__(self, label):
        self.label = label

    def __eq__(self, other):
        return type(other) is Marker and other.label == self.label

    def __hash__(self):
        return hash(self.label)


def to_marker_reference(value):
    if type(value) is Marker:
        return 7, value.label
    raise TypeError(value)


def from_marker_reference(code, label):
    assert code == 7
    return Marker(label)


@given(st.recursive(st.builds(Marker, st.text()) | st.integers(), containers(scalars)))
def test_what_the_codec_cannot_encode_travels_through_the_hooks(value):
    data = encode(value, to_marker_reference)
    assert decode(data, from_marker_reference) == value


def test_an_extension_without_a_hook_is_an_unknown_tag():
    with pytest.raises(DecodeError, match="unknown tag 0x58"):
        decode(encode(Marker("x"), to_marker_reference))


def nested(depth):
    value = []
    for _ in range(depth):
        value = [value]
    return value


def test_both_sides_share_one_nesting_limit():
    assert decode(encode(nested(MAX_DEPTH))) == nested(MAX_DEPTH)
    with pytest.raises(TypeError, match="nests too deeply"):
        encode(nested(MAX_DEPTH + 1))
    too_deep = b"[\x01" * (MAX_DEPTH + 1) + b"[\x00"
    with pytest.raises(DecodeError, match="nests too deeply"):
        decode(too_deep)
