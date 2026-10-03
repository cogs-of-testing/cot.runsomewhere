import re

import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere.testing import open_inloop

from . import remote_exec_module

pytestmark = pytest.mark.anyio

ENABLED = {"rsh.remote_exec": True}


def double(channel, value):
    return value * 2


def echo_items(channel):
    for item in channel:
        channel.send(item)


async def async_double(channel, value):
    return value * 2


def raises(channel):
    msg = "missing"
    raise KeyError(msg)


async def test_remote_exec_is_refused_where_not_enabled(gateway):
    with pytest.raises(rsh.StateError, match=re.escape("rsh.remote_exec")):
        async with gateway.remote_exec("pass"):
            pass


async def test_a_source_string_runs_with_channel_bound():
    async with open_inloop(enable=ENABLED) as gateway:
        async with gateway.remote_exec(
            """
            channel.send(channel.receive() + 1)
            """
        ) as channel:
            await channel.send(41)
            assert await channel.receive() == 42


async def test_a_module_runs_its_source_with_channel_bound():
    async with open_inloop(enable=ENABLED) as gateway:
        async with gateway.remote_exec(remote_exec_module) as channel:
            assert await channel.receive() == "module ran"


async def test_a_function_gets_the_channel_and_keyword_arguments():
    async with open_inloop(enable=ENABLED) as gateway:
        async with gateway.remote_exec(double, value=21) as channel:
            assert await channel.wait_closed() == 42


async def test_a_sync_function_uses_the_sync_channel_api():
    async with open_inloop(enable=ENABLED) as gateway:
        async with gateway.remote_exec(echo_items) as channel:
            await channel.send("x")
            assert await channel.receive() == "x"


async def test_an_async_function_runs_on_the_workers_loop():
    async with open_inloop(enable=ENABLED) as gateway:
        async with gateway.remote_exec(async_double, value=2) as channel:
            assert await channel.wait_closed() == 4


async def test_errors_name_the_sent_code_in_the_remote_traceback():
    async with open_inloop(enable=ENABLED) as gateway:
        async with gateway.remote_exec(raises) as channel:
            with pytest.raises(rsh.RemoteError) as excinfo:
                await channel.wait_closed()
    assert "<remote_exec #" in excinfo.value.remote_traceback
    assert "KeyError" in excinfo.value.remote_traceback


def closure_factory():
    captured = 1

    def uses_closure(channel):
        return captured

    return uses_closure


GLOBAL = 1


def uses_global(channel):
    return GLOBAL


def wrong_first_parameter(chan):
    pass


@pytest.mark.parametrize(
    ("code", "kwargs", "error", "message"),
    [
        (lambda channel: None, {}, ValueError, "lambda"),
        (wrong_first_parameter, {}, ValueError, "channel"),
        (closure_factory(), {}, ValueError, "closure"),
        (uses_global, {}, ValueError, "GLOBAL"),
        ("channel.send(1)", {"value": 1}, TypeError, "keyword"),
        (double, {"value": object()}, TypeError, "object"),
    ],
    ids=[
        "lambda",
        "first-param",
        "closure",
        "global",
        "kwargs-to-source",
        "unsendable",
    ],
)
async def test_unrunnable_code_is_refused_before_sending(code, kwargs, error, message):
    async with open_inloop(enable=ENABLED) as gateway:
        with pytest.raises(error, match=message):
            async with gateway.remote_exec(code, **kwargs):
                pass
