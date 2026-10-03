import struct

import pytest

from cot.runsomewhere._frames import (
    HEADER_SIZE,
    MAX_PAYLOAD,
    PREAMBLE,
    Frame,
    FrameDecoder,
    FrameError,
    FrameType,
    encode_frame,
)

FRAMES = [
    Frame(FrameType.OPEN, 1, b"open payload"),
    Frame(FrameType.DATA, 1, b""),
    Frame(FrameType.DATA, 2, b"x" * 1000),
    Frame(FrameType.CREDIT, 2, b"\x00\x10\x00\x00"),
    Frame(FrameType.CLOSE, 1, b"result"),
    Frame(FrameType.GATEWAY_CLOSE, 0, b""),
]


def stream():
    return PREAMBLE + b"".join(encode_frame(frame) for frame in FRAMES)


def test_header_is_type_channel_length():
    encoded = encode_frame(Frame(FrameType.DATA, 7, b"abc"))
    assert HEADER_SIZE == 9
    assert encoded[:HEADER_SIZE] == struct.pack(">BII", FrameType.DATA, 7, 3)
    assert encoded[HEADER_SIZE:] == b"abc"


def test_whole_stream_decodes_to_the_frames_sent():
    assert FrameDecoder().feed(stream()) == FRAMES


@pytest.mark.parametrize("chunk", [1, 2, 3, 8, 9, 10, 64])
def test_any_chunking_decodes_to_the_same_frames(chunk):
    data = stream()
    decoder = FrameDecoder()
    decoded = []
    for start in range(0, len(data), chunk):
        decoded.extend(decoder.feed(data[start : start + chunk]))
    assert decoded == FRAMES


def test_every_two_way_split_decodes_to_the_same_frames():
    data = stream()
    for cut in range(len(data) + 1):
        decoder = FrameDecoder()
        assert decoder.feed(data[:cut]) + decoder.feed(data[cut:]) == FRAMES


def test_a_partial_frame_yields_nothing_until_complete():
    decoder = FrameDecoder()
    encoded = encode_frame(FRAMES[2])
    assert decoder.feed(PREAMBLE + encoded[:-1]) == []
    assert decoder.feed(encoded[-1:]) == [FRAMES[2]]


def test_a_stream_without_the_preamble_is_refused_on_the_first_bytes():
    with pytest.raises(FrameError, match="not a runsomewhere stream"):
        FrameDecoder().feed(b"SSH-2.0-OpenSSH_9.9\r\n")


def test_another_protocol_version_is_refused_by_the_preamble():
    other = PREAMBLE[:-1] + bytes([PREAMBLE[-1] + 1])
    with pytest.raises(FrameError, match="protocol version"):
        FrameDecoder().feed(other)


def test_unknown_frame_type_is_refused():
    bogus = struct.pack(">BII", 0xFF, 1, 0)
    with pytest.raises(FrameError, match="frame type"):
        FrameDecoder().feed(PREAMBLE + bogus)


def test_a_length_above_the_limit_is_refused_before_buffering_it():
    header = struct.pack(">BII", FrameType.DATA, 1, MAX_PAYLOAD + 1)
    with pytest.raises(FrameError, match="too large"):
        FrameDecoder().feed(PREAMBLE + header)


def test_encoding_refuses_a_payload_above_the_limit():
    with pytest.raises(FrameError, match="too large"):
        encode_frame(Frame(FrameType.DATA, 1, bytes(MAX_PAYLOAD + 1)))


def test_decoder_is_unusable_after_an_error():
    decoder = FrameDecoder()
    with pytest.raises(FrameError):
        decoder.feed(b"garbage!")
    with pytest.raises(FrameError):
        decoder.feed(PREAMBLE)


def test_frame_types_cover_the_designed_set():
    assert {t.name for t in FrameType} == {
        "HELLO",
        "CONFIG",
        "OPEN",
        "DATA",
        "CREDIT",
        "CLOSE",
        "GATEWAY_CLOSE",
        "STOP",
        "GATEWAY_STOP",
    }


def test_channel_ids_use_the_full_unsigned_range():
    for channel in [0, 1, 2**31, 2**32 - 1]:
        frame = Frame(FrameType.DATA, channel, b"")
        assert FrameDecoder().feed(PREAMBLE + encode_frame(frame)) == [frame]
