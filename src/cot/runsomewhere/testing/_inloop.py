from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import anyio
import anyio.abc

from .._gateway import Gateway, Group
from .._places import Launched, Place
from .._version import version as __version__
from .._worker import WorkerCore
from ._pipe import Pipe

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping


@dataclass
class InLoop(Place):
    """A worker run as tasks in the caller's own event loop.

    It is the ordinary worker core over an in-memory :class:`Pipe`, so a test
    at this level exercises the same protocol a process worker speaks.

    ``services`` replaces the entry-point services with handler objects, for
    tests of the service layer itself. ``worker_version`` makes the worker
    announce another runsomewhere version, for skew tests.
    """

    kind = "inloop"
    services: Mapping[str, Callable[..., Any]] | None = None
    pipe: Pipe | None = None
    worker_version: str | None = None

    def to_value(self) -> dict[str, Any]:
        if self.services is not None or self.pipe is not None:
            msg = "an InLoop place with handler objects or a pipe cannot be sent"
            raise TypeError(msg)
        return {"kind": self.kind, "worker_version": self.worker_version}

    async def launch(self, task_group: anyio.abc.TaskGroup) -> Launched:
        pipe = self.pipe or Pipe()
        worker = WorkerCore(
            pipe.worker_end,
            services=self.services,
            version=self.worker_version or __version__,
        )
        scope = anyio.CancelScope(shield=True)
        done = anyio.Event()

        async def run() -> None:
            try:
                with scope:
                    await worker.run()
            finally:
                done.set()

        task_group.start_soon(run)

        async def close(timeout: float) -> None:
            with anyio.move_on_after(timeout):
                await done.wait()
            scope.cancel()
            await done.wait()

        return Launched(pipe.caller_end, close)


@asynccontextmanager
async def open_inloop(
    *, enable: Mapping[str, bool] | None = None, **place_options: Any
) -> AsyncIterator[Gateway]:
    """A gateway to an :class:`InLoop` worker, in a group of its own.

    ``enable`` turns services on or off, as ``services=`` does on
    ``group.spawn``; ``place_options`` go to :class:`InLoop`.
    """
    async with (
        Group() as group,
        group.spawn(InLoop(**place_options), services=dict(enable or {})) as gateway,
    ):
        yield gateway
