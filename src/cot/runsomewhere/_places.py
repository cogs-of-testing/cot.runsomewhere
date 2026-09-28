"""Places: where a worker runs, and how it is started there."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

import anyio
import anyio.abc
from anyio.abc import SocketStream

from ._errors import HostNotFound, StateError


@dataclass
class Launched:
    """A started worker: its protocol stream, and how to make it go away."""

    stream: anyio.abc.ByteStream
    close: Callable[[float], Awaitable[None]]


class Place:
    kind: ClassVar[str]

    def to_value(self) -> dict[str, Any]:
        """The place as a value, for a relay to launch it."""
        raise TypeError(f"{type(self).__name__} places cannot be relayed yet")

    def worker_config(self) -> dict[str, Any]:
        """Configuration for the worker, sent end to end in its first frame."""
        return {}

    async def launch(self, task_group: anyio.abc.TaskGroup) -> Launched:
        raise StateError(f"{type(self).__name__} places are not implemented yet")


_PLACES: dict[str, str] = {
    "process": "cot.runsomewhere._places:Process",
    "inloop": "cot.runsomewhere.testing._inloop:InLoop",
}


def place_from_value(value: dict[str, Any]) -> Place:
    from importlib import import_module

    module, _, name = _PLACES[value["kind"]].partition(":")
    kind: type[Place] = getattr(import_module(module), name)
    fields = {key: item for key, item in value.items() if key != "kind"}
    return kind(**fields)


@dataclass
class Thread(Place):
    kind = "thread"


@dataclass
class Subinterpreter(Place):
    kind = "subinterpreter"


@dataclass
class Ssh(Place):
    kind = "ssh"
    host: str
    python: str | None = None
    user: str | None = None
    port: int | None = None
    config: str | None = None


@dataclass
class Container(Place):
    kind = "container"
    image: str | None = None
    name: str | None = None
    runtime: str = "podman"
    python: str | None = None


@dataclass
class Process(Place):
    """A worker process on this machine."""

    kind = "process"
    python: str | None = None
    env: dict[str, str] = field(default_factory=dict)

    def to_value(self) -> dict[str, Any]:
        # env stays out: it travels end to end in the configuration frame
        return {"kind": self.kind, "python": self.python}

    def worker_config(self) -> dict[str, Any]:
        return {"env": dict(self.env)}

    async def launch(self, task_group: anyio.abc.TaskGroup) -> Launched:
        python = self.python or sys.executable
        if self.python is not None:
            await _check_interpreter(python)
        if sys.platform == "win32":
            raise StateError("Process places on Windows are not implemented yet")
        ours, theirs = socket.socketpair()
        with theirs:
            process = await anyio.open_process(
                [
                    python,
                    "-m",
                    "cot.runsomewhere",
                    "worker",
                    "--fd",
                    str(theirs.fileno()),
                ],
                pass_fds=[theirs.fileno()],
                stdin=subprocess.DEVNULL,
                stdout=None,
                stderr=None,
            )
        stream = await SocketStream.from_socket(ours)

        async def close(timeout: float) -> None:
            with anyio.move_on_after(timeout):
                await process.wait()
                return
            if process.returncode is None:
                process.terminate()
                with anyio.move_on_after(timeout):
                    await process.wait()
                    return
                process.kill()
            await process.wait()

        return Launched(stream, close)


async def _check_interpreter(python: str) -> None:
    if not os.path.exists(python):
        raise HostNotFound(f"no interpreter at {python}")
    probe = await anyio.run_process(
        [python, "-c", "import cot.runsomewhere"],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if probe.returncode != 0:
        raise StateError(
            f"{python} has no cot.runsomewhere installed; bootstrapping it is "
            "not implemented yet"
        )
