import re
import threading

import anyio
import pytest

from cot import runsomewhere as rsh
from rsh_test_services import Echo

from .conftest import inloop

pytestmark = pytest.mark.anyio


async def test_ad_hoc_services_are_exactly_what_the_worker_offers():
    async def echo(channel):
        async for item in channel:
            await channel.send(item)

    async with inloop(services={"t.echo": echo}) as gateway:
        assert "t.echo" in gateway.services
        assert "rsh_test_services.echo" not in gateway.services


async def test_parameters_arrive_as_keyword_arguments():
    seen = {}

    async def record(channel, *, name, count):
        seen.update(name=name, count=count)

    async with inloop(services={"t.record": record}) as gateway:
        async with gateway.open("t.record", name="x", count=3) as channel:
            await channel.wait_closed()
    assert seen == {"name": "x", "count": 3}


async def test_unsendable_parameters_are_refused_before_sending():
    async def record(channel, *, value):
        pytest.fail("the handler must not be called")

    async with inloop(services={"t.record": record}) as gateway:
        with pytest.raises(TypeError):
            async with gateway.open("t.record", value=object()):
                pass


async def test_opening_an_unknown_service_is_a_state_error(gateway):
    with pytest.raises(rsh.StateError, match=re.escape("t.missing")):
        async with gateway.open("t.missing"):
            pass


async def test_async_handlers_run_on_the_workers_loop():
    loop_thread = threading.get_ident()
    ran_on = []

    async def where(channel):
        ran_on.append(threading.get_ident())

    async with inloop(services={"t.where": where}) as gateway:
        async with gateway.open("t.where") as channel:
            await channel.wait_closed()
    assert ran_on == [loop_thread]


async def test_sync_handlers_run_on_a_worker_thread_with_the_sync_channel_api():
    loop_thread = threading.get_ident()
    ran_on = []

    def double(channel):
        ran_on.append(threading.get_ident())
        for item in channel:
            channel.send(item * 2)

    async with inloop(services={"t.double": double}) as gateway:
        async with gateway.open("t.double") as channel:
            await channel.send(4)
            assert await channel.receive() == 8
    assert ran_on
    assert ran_on[0] != loop_thread


async def test_a_sync_handler_sees_channel_closed_when_the_caller_leaves():
    saw = threading.Event()

    def wait(channel):
        try:
            channel.receive()
        except rsh.ChannelClosed:
            saw.set()

    async with inloop(services={"t.wait": wait}) as gateway:
        async with gateway.open("t.wait"):
            pass
        assert await anyio.to_thread.run_sync(saw.wait, 1)


async def test_every_open_is_its_own_handler_call():
    calls = []

    async def count(channel):
        calls.append(channel)

    async with inloop(services={"t.count": count}) as gateway:
        for _ in range(3):
            async with gateway.open("t.count") as channel:
                await channel.wait_closed()
    assert len(calls) == 3
    assert len(set(map(id, calls))) == 3


async def test_the_client_wraps_the_channel_of_its_service(gateway):
    async with gateway.open(Echo) as echo:
        assert isinstance(echo, Echo)
        assert await echo.roundtrip({"a": 1}) == {"a": 1}


async def test_a_client_for_a_service_not_offered_is_a_state_error():
    async with inloop(services={}) as gateway:
        with pytest.raises(rsh.StateError, match=re.escape("rsh_test_services.echo")):
            async with gateway.open(Echo):
                pass
