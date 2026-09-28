import os
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
        async with group.spawn(rsh.Process(), close_timeout=0.5) as gateway:
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
