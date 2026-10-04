import pytest
from hypothesis import given
from hypothesis import strategies as st

from cot.runsomewhere._frames import (
    MAX_FIELD,
    PREAMBLE,
    Frame,
    FrameDecoder,
    FrameError,
    encode_frame,
)

FRAMES = [
    Frame(1, b"open payload"),
    Frame(2, b"x" * 1000),
    Frame(2, b"", taken=300),
    Frame(3, b"first", more=True),
    Frame(3, b"", abort=True),
    Frame(0, b"control", taken=0),
    Frame(MAX_FIELD, b"y" * 70_000, taken=MAX_FIELD),
]


def encoded(frame):
    return encode_frame(
        frame.channel, frame.payload, frame.taken, more=frame.more, abort=frame.abort
    )


def stream(frames=FRAMES):
    return PREAMBLE + b"".join(map(encoded, frames))


@pytest.mark.parametrize(
    ("frame", "header"),
    [
        (Frame(0, b""), 1),
        (Frame(1, b"x" * 300), 4),
        (Frame(1, b"x" * 300, taken=200), 5),
        (Frame(1, b"", taken=200), 3),
        (Frame(300, b"x" * 70_000, taken=70_000), 9),
    ],
    ids=["empty-control", "item", "item-with-credit", "credit-only", "widest"],
)
def test_a_field_takes_only_the_bytes_its_value_needs(frame, header):
    assert len(encoded(frame)) == header + len(frame.payload)


def test_whole_stream_decodes_to_the_frames_sent():
    assert FrameDecoder().feed(stream()) == FRAMES


frames = st.builds(
    Frame,
    st.integers(0, MAX_FIELD),
    st.binary(max_size=300),
    st.integers(0, MAX_FIELD),
    more=st.booleans(),
    abort=st.just(False),
) | st.builds(
    Frame,
    st.integers(0, MAX_FIELD),
    payload=st.just(b""),
    taken=st.integers(0, MAX_FIELD),
    more=st.just(False),
    abort=st.just(True),
)


@given(st.lists(frames, max_size=8), st.lists(st.integers(1, 64), min_size=1))
def test_any_chunking_decodes_to_the_same_frames(sent, chunks):
    data = stream(sent)
    decoder = FrameDecoder()
    decoded = []
    start = 0
    for size in chunks * (len(data) // len(chunks) + 1):
        if start >= len(data):
            break
        decoded.extend(decoder.feed(data[start : start + size]))
        start += size
    assert decoded == sent


def test_every_two_way_split_decodes_to_the_same_frames():
    data = stream(FRAMES[:6])
    for cut in range(len(data) + 1):
        decoder = FrameDecoder()
        assert decoder.feed(data[:cut]) + decoder.feed(data[cut:]) == FRAMES[:6]


def test_a_partial_frame_yields_nothing_until_complete():
    decoder = FrameDecoder()
    frame = encoded(FRAMES[1])
    assert decoder.feed(PREAMBLE + frame[:-1]) == []
    assert decoder.feed(frame[-1:]) == [FRAMES[1]]


def test_a_stream_without_the_preamble_is_refused_on_the_first_bytes():
    with pytest.raises(FrameError, match="not a runsomewhere stream"):
        FrameDecoder().feed(b"SSH-2.0-OpenSSH_9.9\r\n")


def test_another_protocol_version_is_refused_by_the_preamble():
    other = PREAMBLE[:-1] + bytes([PREAMBLE[-1] - 1])
    with pytest.raises(FrameError, match="protocol version"):
        FrameDecoder().feed(other)


@pytest.mark.parametrize(("channel", "taken"), [(MAX_FIELD + 1, 0), (1, MAX_FIELD + 1)])
def test_a_field_above_24_bits_is_refused_when_encoding(channel, taken):
    with pytest.raises(FrameError, match="out of range"):
        encode_frame(channel, b"", taken)


def test_decoder_is_unusable_after_an_error():
    decoder = FrameDecoder()
    with pytest.raises(FrameError):
        decoder.feed(b"garbage!")
    with pytest.raises(FrameError):
        decoder.feed(PREAMBLE)


@pytest.mark.parametrize(
    "tag", [0xC0, 0x44], ids=["abort-and-more", "abort-with-payload"]
)
def test_an_abort_that_carries_or_continues_something_is_refused(tag):
    with pytest.raises(FrameError, match="invalid frame tag"):
        FrameDecoder().feed(PREAMBLE + bytes([tag, 1]))
