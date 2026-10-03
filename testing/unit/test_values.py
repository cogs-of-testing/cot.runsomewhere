import math

import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere._values import DecodeError, decode, encode

SENDABLE = [
    None,
    True,
    False,
    0,
    -1,
    2**200,
    -(2**200),
    1.5,
    -0.0,
    math.inf,
    3 + 4j,
    "",
    "text with ünïcode",
    b"",
    b"\x00\xff",
    (),
    (1, "a"),
    [],
    [1, [2, [3]]],
    {},
    {"key": [1, 2], 3: None, (1, 2): b"x"},
    set(),
    {1, 2},
    frozenset({"a"}),
    {"nested": ({1}, frozenset({2}), [{"deep": (None,)}])},
]


@pytest.mark.parametrize("value", SENDABLE, ids=repr)
def test_sendable_values_roundtrip_equal(value):
    assert decode(encode(value)) == value


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
    return a == b or (math.isnan(a) and math.isnan(b))


@pytest.mark.parametrize("value", SENDABLE, ids=repr)
def test_roundtrip_keeps_the_exact_type(value):
    assert same(decode(encode(value)), value)


def test_bool_does_not_decay_to_int():
    assert decode(encode(True)) is True
    assert decode(encode([False]))[0] is False


def test_negative_zero_and_nan_survive():
    assert math.copysign(1, decode(encode(-0.0))) == -1
    assert math.isnan(decode(encode(math.nan)))


@pytest.mark.parametrize("value", SENDABLE, ids=repr)
def test_can_send_accepts_sendable_values(value):
    assert rsh.can_send(value)


class Custom:
    pass


class DictSubclass(dict):
    pass


NOT_SENDABLE = [
    object(),
    Custom(),
    DictSubclass(),
    print,
    lambda: None,
    [1, object()],
    {"k": Custom()},
    {Custom()},
    range(3),
    bytearray(b"x"),
]


@pytest.mark.parametrize("value", NOT_SENDABLE, ids=repr)
def test_can_send_refuses_everything_else(value):
    assert not rsh.can_send(value)


@pytest.mark.parametrize("value", NOT_SENDABLE, ids=repr)
def test_encoding_refuses_what_can_send_refuses(value):
    with pytest.raises(TypeError):
        encode(value)


def test_self_referencing_containers_are_refused():
    loop = []
    loop.append(loop)
    assert not rsh.can_send(loop)
    with pytest.raises(TypeError):
        encode(loop)


@pytest.mark.parametrize(
    "data",
    [b"", b"\xff", encode([1, 2, 3])[:-1], encode("text") + b"trailing"],
    ids=["empty", "unknown-tag", "truncated", "trailing"],
)
def test_malformed_input_raises_decode_error(data):
    with pytest.raises(DecodeError):
        decode(data)


def test_nothing_in_the_encoding_names_a_class_or_module():
    # a payload claiming a pickle opcode, a module path and a class name is
    # just bytes to the decoder: refused, never imported or called
    for payload in [b"c__builtin__\neval\n", b"\x80\x04\x95", b"os:system"]:
        with pytest.raises(DecodeError):
            decode(payload)
