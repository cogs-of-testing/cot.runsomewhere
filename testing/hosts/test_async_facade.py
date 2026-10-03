import threading

import anyio
import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht

pytestmark = pytest.mark.anyio


async def test_the_async_api_through_an_engine_host_is_the_same_api(engine):
    with rsh.use_engine(engine):
        async with rsh.open_group() as group:
            async with group.spawn(rsht.InLoop()) as gateway:
                async with gateway.open("rsh_test_services.echo") as channel:
                    await channel.send("hosted")
                    assert await channel.receive() == "hosted"


async def test_protocol_io_keeps_running_while_the_callers_thread_is_blocked(engine):
    with rsh.use_engine(engine):
        async with rsh.open_group() as group:
            async with group.spawn(rsht.InLoop()) as gateway:
                async with gateway.open(
                    "rsh_test_services.produce", count=10, size=1
                ) as channel:
                    # block the caller's loop outright; the host still reads
                    threading.Event().wait(0.2)
                    assert [item async for item in channel] == [
                        bytes([index]) for index in range(10)
                    ]


async def test_cancellation_crosses_into_the_host(engine):
    with rsh.use_engine(engine):
        async with rsh.open_group() as group:
            async with group.spawn(rsht.InLoop()) as gateway:
                async with gateway.open("rsh_test_services.echo") as channel:
                    with anyio.move_on_after(0.05) as scope:
                        await channel.receive()
                    assert scope.cancelled_caught
                    await channel.send("after cancel")
                    assert await channel.receive() == "after cancel"


async def test_without_an_override_the_async_api_runs_in_the_callers_loop():
    async with rsh.open_group() as group:
        assert isinstance(group, rsh.Group)


async def test_ending_sending_through_an_engine_host_leaves_the_result(engine):
    with rsh.use_engine(engine):
        async with rsh.open_group() as group:
            async with group.spawn(rsht.InLoop()) as gateway:
                async with gateway.open("rsh_test_services.add") as channel:
                    for number in [1, 2, 3]:
                        await channel.send(number)
                    channel.close_send()
                    with anyio.fail_after(5):
                        assert await channel.wait_closed() == 6
