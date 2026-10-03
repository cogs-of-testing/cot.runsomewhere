import os
import signal
import sys
import time
from pathlib import Path

import anyio
import pytest

from cot import runsomewhere as rsh

pytestmark = pytest.mark.anyio


async def test_a_process_worker_runs_entry_point_services():
    async with rsh.open_group() as group:
        async with group.spawn(rsh.Process()) as gateway:
            assert gateway.worker.pid != os.getpid()
            async with gateway.open("rsh_test_services.echo") as channel:
                await channel.send({"across": "processes"})
                assert await channel.receive() == {"across": "processes"}


async def test_a_process_worker_uses_the_callers_interpreter_by_default():
    async with rsh.open_group() as group:
        async with group.spawn(rsh.Process()) as gateway:
            assert gateway.worker.executable == sys.executable


async def test_prints_in_the_worker_do_not_corrupt_the_protocol():
    async with rsh.open_group() as group:
        async with group.spawn(rsh.Process()) as gateway:
            async with gateway.open("rsh_test_services.noisy_echo") as channel:
                for index in range(20):
                    await channel.send(index)
                    assert await channel.receive() == index


async def test_environment_values_reach_the_worker_but_not_its_argv():
    secret = "rsh-secret-value-7f3a"
    async with rsh.open_group() as group:
        async with group.spawn(rsh.Process(env={"RSH_SECRET": secret})) as gateway:
            async with gateway.open("rsh_test_services.env", name="RSH_SECRET") as ch:
                assert await ch.wait_closed() == secret
            cmdline = Path(f"/proc/{gateway.worker.pid}/cmdline")
            if cmdline.exists():
                assert secret.encode() not in cmdline.read_bytes()


async def test_a_crashed_worker_surfaces_as_worker_gone():
    async with rsh.open_group() as group:
        async with group.spawn(rsh.Process()) as gateway:
            async with gateway.open("rsh_test_services.crash", code=3) as channel:
                with pytest.raises(rsh.WorkerGone):
                    await channel.receive()


async def test_leaving_the_spawn_block_kills_a_worker_that_will_not_stop():
    async with rsh.open_group() as group:
        targets = rsh.Teardown(stop=0.25, drain=0.25)
        async with group.spawn(rsh.Process(), teardown=targets) as gateway:
            pid = gateway.worker.pid
            async with gateway.open("rsh_test_services.stubborn"):
                pass
            start = time.monotonic()
    assert time.monotonic() - start < 5
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_a_missing_interpreter_fails_the_spawn_clearly():
    async with rsh.open_group() as group:
        with pytest.raises(rsh.HostNotFound, match="no-such-python"):
            async with group.spawn(rsh.Process(python="/no-such-python")):
                pass


async def test_an_interpreter_without_runsomewhere_is_not_bootstrapped_silently(
    tmp_path,
):
    venv = tmp_path / "bare"
    await anyio.run_process([sys.executable, "-m", "venv", "--without-pip", venv])
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    async with rsh.open_group() as group:
        with pytest.raises(rsh.StateError, match="bootstrap"):
            async with group.spawn(rsh.Process(python=str(python))):
                pass


async def test_a_wedged_worker_is_forced_after_one_budget_not_one_per_step():
    targets = rsh.Teardown(stop=1.0, drain=1.0)
    async with rsh.open_group() as group:
        async with group.spawn(rsh.Process(), teardown=targets) as gateway:
            pid = gateway.worker.pid
            async with gateway.open("rsh_test_services.wedge"):
                await anyio.sleep(0.2)
            start = time.monotonic()
    # one budget, then terminate (ignored), then the kill; not a budget each
    assert time.monotonic() - start < targets.total + 2.5
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_a_cancelled_scope_forces_the_worker_without_waiting_the_budget():
    with anyio.CancelScope() as scope:
        async with rsh.open_group() as group:
            targets = rsh.Teardown(stop=15, drain=15)
            async with group.spawn(rsh.Process(), teardown=targets) as gateway:
                pid = gateway.worker.pid
                async with gateway.open("rsh_test_services.wedge"):
                    await anyio.sleep(0.2)
                    start = time.monotonic()
                    scope.cancel()
                    await anyio.sleep_forever()
    assert time.monotonic() - start < 5
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def _gone(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    stat = Path(f"/proc/{pid}/stat")
    # an exited child not yet reaped by whoever inherited it
    return stat.exists() and stat.read_text().split()[2] == "Z"


async def test_terminating_a_relay_forces_a_wedged_worker_behind_it():
    # a leaf that ignores the end of its stream and SIGTERM would be orphaned
    # by a relay that just died
    async with rsh.open_group() as group:
        async with group.spawn(rsh.Process()) as relay:
            # the relay falls back on the targets sent with the tunnel
            targets = rsh.Teardown(stop=0.2, drain=0.3)
            async with relay.spawn(rsh.Process(), teardown=targets) as leaf:
                async with leaf.open("rsh_test_services.wedge"):
                    await anyio.sleep(0.2)
                    os.kill(relay.worker.pid, signal.SIGTERM)
                    with anyio.fail_after(5):
                        while not (_gone(relay.worker.pid) and _gone(leaf.worker.pid)):
                            await anyio.sleep(0.05)


LEFT_OPEN = """
import time
from cot import runsomewhere as rsh
group = rsh.sync.open_group().__enter__()
gateway = group.spawn(rsh.Process()).__enter__()
gateway.open("rsh_test_services.wedge").__enter__()
time.sleep(0.2)
print(gateway.worker.pid, flush=True)
"""


@pytest.mark.parametrize("engine", ["ThreadEngine", "SubinterpreterEngine"])
async def test_exiting_with_a_group_left_open_forces_its_wedged_worker(engine):
    if engine == "SubinterpreterEngine" and sys.version_info < (3, 14):
        pytest.skip("concurrent.interpreters needs 3.14")
    script = LEFT_OPEN.replace(
        "group = ", f"rsh.use_engine(rsh.{engine}()).__enter__()\ngroup = ", 1
    )
    start = time.monotonic()
    with anyio.fail_after(10):
        done = await anyio.run_process([sys.executable, "-c", script])
    assert time.monotonic() - start < 8
    pid = int(done.stdout)
    with anyio.fail_after(2):
        while not _gone(pid):
            await anyio.sleep(0.05)


def test_a_groups_policy_reaches_its_workers_through_the_sync_facade():
    policy = rsh.Shutdown(edge=rsh.Teardown(stop=0.5, drain=0.5))
    with rsh.sync.open_group(shutdown=policy) as group:
        with group.spawn(rsh.Process()) as gateway:
            pid = gateway.worker.pid
            with gateway.open("rsh_test_services.wedge"):
                time.sleep(0.2)
            start = time.monotonic()
    # the default targets would take five seconds before force
    assert time.monotonic() - start < policy.edge.total + 2.5
    assert _gone(pid)
