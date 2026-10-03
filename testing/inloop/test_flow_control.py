import anyio
import pytest

from cot.runsomewhere._channels import DEFAULT_WINDOW
from cot.runsomewhere.testing import open_inloop

pytestmark = pytest.mark.anyio

ITEM = 64 * 1024


def counting_producer(sent):
    async def produce(channel):
        while True:
            await channel.send(bytes(ITEM))
            sent.append(ITEM)

    return produce


async def test_a_sender_stops_at_the_receivers_window():
    sent = []
    async with open_inloop(services={"t.produce": counting_producer(sent)}) as gateway:
        async with gateway.open("t.produce"):
            await anyio.wait_all_tasks_blocked()
            # one item may exceed what is left of the window, never more
            assert DEFAULT_WINDOW <= sum(sent) <= DEFAULT_WINDOW + ITEM


async def test_receiving_grants_the_sender_more_window():
    sent = []
    async with open_inloop(services={"t.produce": counting_producer(sent)}) as gateway:
        async with gateway.open("t.produce") as channel:
            await anyio.wait_all_tasks_blocked()
            before = sum(sent)
            for _ in range(8):
                await channel.receive()
            await anyio.wait_all_tasks_blocked()
            assert sum(sent) > before


async def test_an_item_larger_than_the_window_still_arrives():
    async with open_inloop() as gateway:
        async with gateway.open(
            "rsh_test_services.produce", count=1, size=DEFAULT_WINDOW * 3
        ) as channel:
            assert len(await channel.receive()) == DEFAULT_WINDOW * 3


async def test_a_slow_consumer_on_one_channel_does_not_stall_another():
    async def echo(channel):
        async for item in channel:
            await channel.send(item)

    sent = []
    services = {"t.produce": counting_producer(sent), "t.echo": echo}
    async with open_inloop(services=services) as gateway:
        async with gateway.open("t.produce"):
            await anyio.wait_all_tasks_blocked()
            async with gateway.open("t.echo") as echo:
                await echo.send("through")
                with anyio.fail_after(1):
                    assert await echo.receive() == "through"
