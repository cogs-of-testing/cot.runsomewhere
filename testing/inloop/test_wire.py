import logging

import anyio
import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht
from cot.runsomewhere._channels import FRAGMENT, MAX_ITEM
from cot.runsomewhere._frames import PREAMBLE, FrameDecoder, encode_frame
from cot.runsomewhere._values import encode
from cot.runsomewhere.testing import open_inloop

pytestmark = pytest.mark.anyio


def frames(pipe, *, to):
    data = pipe.recorded(to=to)
    assert data.startswith(PREAMBLE)
    return FrameDecoder().feed(data)


def credit_only(frame):
    return frame.channel and not frame.payload and not frame.more


async def test_credit_rides_on_replies_in_request_response_traffic():
    pipe = rsht.Pipe(record=True)
    async with open_inloop(pipe=pipe) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            for number in range(50):
                await channel.send(number)
                assert await channel.receive() == number
    # the last reply has nothing to carry its credit: at most that one goes
    # out on its own, where v1 sent one credit frame per item
    for side in ("caller", "worker"):
        assert sum(map(credit_only, frames(pipe, to=side))) <= 1


async def test_credit_with_nothing_to_ride_on_goes_out_after_a_short_delay():
    taken = anyio.Event()

    async def take_one(channel):
        await channel.receive()
        taken.set()
        await anyio.sleep_forever()

    pipe = rsht.Pipe(record=True)
    async with open_inloop(pipe=pipe, services={"t.take_one": take_one}) as gateway:
        async with gateway.open("t.take_one") as channel:
            await channel.send("x")
            await taken.wait()
            with anyio.fail_after(1):
                await channel.drain()
            assert any(map(credit_only, frames(pipe, to="caller")))


async def test_an_item_larger_than_a_fragment_arrives_whole():
    big = bytes(range(256)) * (3 * FRAGMENT // 256 + 1)
    pipe = rsht.Pipe(record=True)
    async with open_inloop(pipe=pipe) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            await channel.send(big)
            assert await channel.receive() == big
    fragments = [frame for frame in frames(pipe, to="worker") if frame.more]
    assert len(fragments) == 3
    assert all(len(frame.payload) == FRAGMENT for frame in fragments)


async def test_an_item_over_the_limit_is_refused_when_sent(gateway):
    async with gateway.open("rsh_test_services.echo") as channel:
        with pytest.raises(rsh.StateError, match="over the limit"):
            await channel.send(bytes(MAX_ITEM))


def control(**fields):
    return encode_frame(0, encode(fields))


async def test_an_unknown_op_fails_only_the_channel_it_names(caplog):
    pipe = rsht.Pipe()
    async with open_inloop(pipe=pipe) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            await channel.send(1)
            assert await channel.receive() == 1
            with caplog.at_level(logging.WARNING):
                pipe.inject(
                    control(op="from-the-future", channel=channel.id), to="worker"
                )
                with (
                    anyio.fail_after(1),
                    pytest.raises(rsh.RemoteError, match="does not know"),
                ):
                    await channel.wait_closed()
        async with gateway.open("rsh_test_services.echo") as other:
            await other.send(2)
            assert await other.receive() == 2
    assert "'from-the-future'" in caplog.text
    assert f"on channel {channel.id}" in caplog.text


async def test_an_unknown_field_naming_no_channel_is_only_logged(caplog):
    pipe = rsht.Pipe()
    async with open_inloop(pipe=pipe) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            with caplog.at_level(logging.WARNING):
                pipe.inject(control(op="gateway-stop", keepalive=5), to="worker")
                await channel.send(1)
                assert await channel.receive() == 1
    assert "'keepalive'" in caplog.text


async def test_a_known_op_with_a_field_of_the_wrong_type_ends_the_gateway():
    pipe = rsht.Pipe()
    async with open_inloop(pipe=pipe) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            pipe.inject(control(op="stop", channel="one"), to="caller")
            with pytest.raises(rsh.WorkerGone, match=r"stop\.channel"):
                await channel.receive()
