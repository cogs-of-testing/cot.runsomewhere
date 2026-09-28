import platform

import anyio
import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht

from .conftest import inloop

pytestmark = pytest.mark.anyio


async def test_handshake_describes_the_worker(gateway):
    assert gateway.worker.python == platform.python_version()
    assert gateway.worker.version == rsh.__version__
    assert gateway.worker.platform
    assert isinstance(gateway.worker.pid, int)


async def test_worker_offers_declared_and_default_builtin_services(gateway):
    assert {"rsh.info", "rsh.via", "rsh.deploy", "rsh.transfer"} <= gateway.services
    assert "rsh_test_services.echo" in gateway.services


async def test_remote_exec_and_proxy_are_off_by_default(gateway):
    assert "rsh.remote_exec" not in gateway.services
    assert "rsh.proxy" not in gateway.services


async def test_caller_enables_off_by_default_services_at_spawn():
    async with inloop(enable={"rsh.remote_exec": True}) as gateway:
        assert "rsh.remote_exec" in gateway.services


async def test_caller_disables_a_declared_service_at_spawn():
    async with inloop(enable={"rsh_test_services.echo": False}) as gateway:
        assert "rsh_test_services.echo" not in gateway.services


async def test_version_skew_refuses_the_spawn_naming_both_versions():
    with pytest.raises(rsh.HandshakeRefused) as excinfo:
        async with inloop(worker_version="99.0.0"):
            pytest.fail("a skewed worker must not yield a gateway")
    assert "99.0.0" in str(excinfo.value)
    assert rsh.__version__ in str(excinfo.value)


async def test_leaving_the_spawn_block_closes_the_gateway():
    async with inloop() as gateway:
        pass
    with pytest.raises(rsh.StateError):
        async with gateway.open("rsh_test_services.echo"):
            pass


async def test_leaving_the_spawn_block_stops_running_services():
    stopped = anyio.Event()

    async def forever(channel):
        try:
            await anyio.sleep_forever()
        finally:
            stopped.set()

    async with inloop(services={"t.forever": forever}) as gateway:
        async with gateway.open("t.forever"):
            await anyio.wait_all_tasks_blocked()
    assert stopped.is_set()


async def test_a_cut_stream_fails_open_channels_with_worker_gone():
    pipe = rsht.Pipe()
    async with inloop(pipe=pipe) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            pipe.cut()
            with pytest.raises(rsh.WorkerGone):
                await channel.receive()


async def test_worker_gone_is_a_connection_error():
    assert issubclass(rsh.WorkerGone, OSError)
    assert issubclass(rsh.ChannelClosed, OSError)
    assert not issubclass(rsh.StateError, OSError)


async def test_a_gone_gateway_refuses_new_channels():
    pipe = rsht.Pipe()
    async with inloop(pipe=pipe) as gateway:
        pipe.cut()
        await anyio.wait_all_tasks_blocked()
        with pytest.raises(rsh.WorkerGone):
            async with gateway.open("rsh_test_services.echo"):
                pass


async def test_corrupt_bytes_from_the_worker_end_the_gateway():
    pipe = rsht.Pipe()
    async with inloop(pipe=pipe) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            pipe.inject(b"\xff" * 16, to="caller")
            with pytest.raises(rsh.WorkerGone, match="frame"):
                await channel.receive()


async def test_cancelling_the_group_scope_closes_open_gateways():
    stopped = anyio.Event()

    async def forever(channel):
        try:
            await anyio.sleep_forever()
        finally:
            stopped.set()

    async def run():
        async with rsh.open_group() as group:
            async with group.spawn(rsht.InLoop(services={"t.forever": forever})) as gw:
                async with gw.open("t.forever"):
                    await anyio.sleep_forever()

    async with anyio.create_task_group() as tg:
        tg.start_soon(run)
        await anyio.wait_all_tasks_blocked()
        tg.cancel_scope.cancel()
    assert stopped.is_set()


async def test_spawn_is_only_a_context_manager():
    async with rsh.open_group() as group:
        with pytest.raises(TypeError):
            await group.spawn(rsht.InLoop())


async def test_a_failing_service_does_not_take_the_worker_down(gateway):
    async with gateway.open("rsh_test_services.fail", message="boom") as channel:
        with pytest.raises(rsh.RemoteError):
            await channel.wait_closed()
    async with gateway.open("rsh_test_services.echo") as channel:
        await channel.send("still here")
        assert await channel.receive() == "still here"
