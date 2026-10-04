"""The C codec against the reference: the same bytes, the same refusals.

Run where cot-runsomewhere-speedups is installed; skipped elsewhere.
"""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cot.runsomewhere import _values
from cot.runsomewhere._values import DecodeError, py_decode, py_encode

speedups = pytest.importorskip("_cot_runsomewhere_speedups")


def c_encode(value, default=None):
    return speedups.encode(value, default)


def c_decode(data, ext_hook=None):
    return speedups.decode(data, ext_hook, DecodeError)


# no NaN where decoded values are compared: NaN hashes by identity, so a set
# holding one may iterate in another order each time it is rebuilt
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
sendable = st.recursive(
    hashable | st.sets(hashable, max_size=4),
    lambda inner: (
        st.lists(inner, max_size=6)
        | st.tuples(inner, inner)
        | st.dictionaries(hashable, inner, max_size=6)
    ),
    max_leaves=40,
)


def outcome(function, *args):
    """What a call gives: its value's repr, or its error class."""
    try:
        return "value", repr(function(*args))
    except (TypeError, DecodeError) as error:
        return "error", type(error)


def test_the_package_uses_the_c_codec_it_was_given():
    assert speedups.FORMAT == _values.FORMAT
    assert speedups.MAX_DEPTH == _values.MAX_DEPTH
    assert _values.speedups is speedups
    assert _values.encode is not py_encode


@given(sendable)
def test_both_encode_a_value_to_the_same_bytes(value):
    data = py_encode(value)
    assert c_encode(value) == data
    assert repr(c_decode(data)) == repr(py_decode(data))


@given(st.lists(st.floats() | st.complex_numbers()))
def test_both_encode_every_float_to_the_same_bytes(value):
    assert c_encode(value) == py_encode(value)


@given(st.binary(max_size=64))
def test_both_read_arbitrary_bytes_alike(data):
    assert outcome(c_decode, data) == outcome(py_decode, data)


@given(sendable, st.data())
def test_both_read_damaged_encodings_alike(value, data):
    damaged = bytearray(py_encode(value))
    damaged = damaged[: data.draw(st.integers(0, len(damaged)))]
    for _ in range(data.draw(st.integers(0, 3))):
        if damaged:
            at = data.draw(st.integers(0, len(damaged) - 1))
            damaged[at] = data.draw(st.integers(0, 255))
    assert outcome(c_decode, bytes(damaged)) == outcome(py_decode, bytes(damaged))


class Marker:
    def __init__(self, label):
        self.label = label

    def __repr__(self):
        return f"Marker({self.label!r})"


def to_reference(value):
    if type(value) is Marker:
        return 3, value.label
    raise TypeError(value)


def from_reference(code, label):
    return Marker(label) if code == 3 else (code, label)


@given(
    st.recursive(
        st.builds(Marker, st.text()) | st.integers(),
        lambda inner: st.lists(inner, max_size=4) | st.dictionaries(st.text(), inner),
    )
)
def test_both_send_what_they_cannot_encode_through_the_same_hooks(value):
    data = py_encode(value, to_reference)
    assert c_encode(value, to_reference) == data
    assert repr(c_decode(data, from_reference)) == repr(py_decode(data, from_reference))


class Unsendable:
    pass


@pytest.mark.parametrize(
    "value",
    [Unsendable(), [1, Unsendable()], {Unsendable(): 1}, bytearray(b"x"), range(2)],
    ids=repr,
)
def test_both_refuse_what_cannot_be_sent(value):
    assert outcome(c_encode, value) == outcome(py_encode, value) == ("error", TypeError)


def nested(depth):
    value = []
    for _ in range(depth):
        value = [value]
    return value


@pytest.mark.parametrize("depth", [_values.MAX_DEPTH, _values.MAX_DEPTH + 1])
def test_both_share_the_nesting_limit(depth):
    value = nested(depth)
    assert outcome(c_encode, value) == outcome(py_encode, value)
    data = b"[\x01" * depth + b"\x80"
    assert outcome(c_decode, data) == outcome(py_decode, data)


def test_a_list_containing_itself_is_refused_by_both():
    loop = []
    loop.append(loop)
    assert outcome(c_encode, loop) == outcome(py_encode, loop) == ("error", TypeError)


def test_a_subinterpreter_with_its_own_gil_can_use_it():
    interpreters = pytest.importorskip("concurrent.interpreters")
    interpreter = interpreters.create()
    try:
        interpreter.exec(
            "import _cot_runsomewhere_speedups as c\n"
            "assert c.decode(c.encode([1, 'x']), None, ValueError) == [1, 'x']\n"
        )
    finally:
        interpreter.close()
