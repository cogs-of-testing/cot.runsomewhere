"""Channels that arrive as values cross the facades as handles of the facade,
never as the engine host's own channel objects."""

import anyio
import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht

REMOTE_EXEC = {"rsh.remote_exec": True}


def double(channel, value):
    return value * 2


# async: a sync function would get the received channel unwrapped for its
# thread, which is a worker-side matter, not the facades'
async def say_via(channel):
    other = await channel.receive()
    await other.send("via")
    return "sent"


async def listen(channel):
    return await channel.receive()


@pytest.mark.anyio
async def test_remote_exec_runs_through_the_async_facade(engine):
    with rsh.use_engine(engine):
        async with rsh.open_group() as group:
            async with group.spawn(rsht.InLoop(), services=REMOTE_EXEC) as gateway:
                async with gateway.open(rsh.RemoteExec) as rx:
                    async with rx.run(double, value=2) as channel:
                        with anyio.fail_after(5):
                            assert await channel.wait_closed() == 4


@pytest.mark.anyio
async def test_a_received_channel_can_be_sent_on_through_the_async_facade(engine):
    with rsh.use_engine(engine):
        async with rsh.open_group() as group:
            async with group.spawn(rsht.InLoop(), services=REMOTE_EXEC) as gateway:
                async with gateway.open(rsh.RemoteExec) as rx:
                    async with (
                        rx.run(listen) as listener,
                        rx.run(say_via) as sender,
                    ):
                        # the worker's end of listener sends "via" back here
                        await sender.send(listener)
                        with anyio.fail_after(5):
                            assert await listener.receive() == "via"
                            await listener.send("done")
                            assert await listener.wait_closed() == "done"
                            assert await sender.wait_closed() == "sent"


def test_remote_exec_runs_through_the_sync_facade(engine):
    with rsh.use_engine(engine), rsh.sync.open_group() as group:
        with group.spawn(rsht.InLoop(), services=REMOTE_EXEC) as gateway:
            with gateway.open(rsh.RemoteExec) as rx:
                with rx.run(double, value=2) as channel:
                    assert channel.wait_closed(timeout=5) == 4


def test_a_received_channel_can_be_sent_on_through_the_sync_facade(engine):
    with rsh.use_engine(engine), rsh.sync.open_group() as group:
        with group.spawn(rsht.InLoop(), services=REMOTE_EXEC) as gateway:
            with gateway.open(rsh.RemoteExec) as rx:
                with rx.run(listen) as listener, rx.run(say_via) as sender:
                    # the worker's end of listener sends "via" back here
                    sender.send(listener)
                    assert listener.receive(timeout=5) == "via"
                    listener.send("done")
                    assert listener.wait_closed(timeout=5) == "done"
                    assert sender.wait_closed(timeout=5) == "sent"
