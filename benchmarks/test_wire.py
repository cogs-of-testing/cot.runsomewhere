"""What the protocol costs end to end, over the in-memory pipe.

The codec's share is in test_codec; this adds framing, credit and the event
loop.
"""

import anyio
import pytest

from cot.runsomewhere.testing import open_inloop

ROUND_TRIPS = 200
BULK_ITEMS = 100


async def sink(channel):
    return sum([len(item) async for item in channel])


def run(main):
    anyio.run(main, backend="asyncio")


@pytest.mark.parametrize("item", ["x", "x" * 300], ids=["1-byte", "300-bytes"])
def test_echo_round_trips(benchmark, item):
    benchmark.group = "round trips"
    benchmark.extra_info["round_trips"] = ROUND_TRIPS

    async def main():
        async with open_inloop() as gateway:
            async with gateway.open("rsh_test_services.echo") as channel:
                for _ in range(ROUND_TRIPS):
                    await channel.send(item)
                    await channel.receive()

    benchmark.pedantic(run, (main,), rounds=5)


@pytest.mark.parametrize("size", [300, 64 * 1024, 1024 * 1024])
def test_one_way_stream(benchmark, size):
    benchmark.group = "one-way stream"
    benchmark.extra_info["bytes"] = size * BULK_ITEMS
    item = b"x" * size

    async def main():
        async with open_inloop(services={"bench.sink": sink}) as gateway:
            async with gateway.open("bench.sink") as channel:
                for _ in range(BULK_ITEMS):
                    await channel.send(item)
                channel.close_send()
                assert await channel.wait_closed() == size * BULK_ITEMS

    benchmark.pedantic(run, (main,), rounds=3)
