"""Places: where a worker runs, and how it is started there."""

from __future__ import annotations

import socket
import subprocess
import sys
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import anyio
import anyio.abc
from anyio.abc import SocketStream

from ._errors import HostNotFound, StateError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


@dataclass
class Launched:
    """A started worker: its protocol stream, and how to make it go away."""

    stream: anyio.abc.ByteStream
    close: Callable[[float], Awaitable[None]]


class Place:
    kind: ClassVar[str]

    def to_value(self) -> dict[str, Any]:
        """The place as a value, for a relay to launch it."""
        msg = f"{type(self).__name__} places cannot be relayed yet"
        raise TypeError(msg)

    def worker_config(self) -> dict[str, Any]:
        """Configuration for the worker, sent end to end in its first frame."""
        return {}

    async def launch(self, _task_group: anyio.abc.TaskGroup) -> Launched:
        msg = f"{type(self).__name__} places are not implemented yet"
        raise StateError(msg)


PLACE_GROUP = "cot.runsomewhere.places"


def place_from_value(value: dict[str, Any]) -> Place:
    """A place sent by a caller, its kind resolved by entry-point name."""
    found = entry_points(group=PLACE_GROUP, name=value["kind"])
    if not found:
        msg = f"no place kind {value['kind']!r} is installed on this worker"
        raise LookupError(msg)
    kind: type[Place] = next(iter(found)).load()
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

    async def launch(self, _task_group: anyio.abc.TaskGroup) -> Launched:
        python = self.python or sys.executable
        if self.python is not None:
            await _check_interpreter(python)
        if sys.platform == "win32":
            msg = "Process places on Windows are not implemented yet"
            raise StateError(msg)
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
    if not Path(python).exists():
        msg = f"no interpreter at {python}"
        raise HostNotFound(msg)
    probe = await anyio.run_process(
        [python, "-c", "import cot.runsomewhere"],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if probe.returncode != 0:
        msg = (
            f"{python} has no cot.runsomewhere installed; bootstrapping it is "
            "not implemented yet"
        )
        raise StateError(msg)
