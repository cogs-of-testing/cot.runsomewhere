"""Remote exec: code sent as text, checked here, run on the worker.

A concession for ad-hoc work; the parts of a system are declared services.
"""

from __future__ import annotations

import ast
import builtins
import inspect
import itertools
import textwrap
import types
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import anyio
import anyio.to_thread

from ._gateway import Client
from ._thread_channel import ThreadChannel

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from ._channels import Channel

Code = str | types.ModuleType | Callable[..., Any]


def prepare(code: Code, kwargs: dict[str, Any]) -> dict[str, Any]:
    """The request for rsh.remote_exec; raise if the code cannot stand alone."""
    call = None
    if isinstance(code, types.ModuleType):
        source = inspect.getsource(code)
    elif isinstance(code, types.FunctionType):
        call = code.__name__
        source = _source_of_function(code)
    elif isinstance(code, str):
        source = textwrap.dedent(code)
    else:
        msg = f"cannot remote_exec {type(code).__qualname__} objects"
        raise TypeError(msg)
    if call is None and kwargs:
        msg = "keyword arguments are only passed to functions"
        raise TypeError(msg)
    return {"source": source, "call": call, "kwargs": kwargs}


def _source_of_function(function: types.FunctionType) -> str:
    if function.__name__ == "<lambda>":
        msg = "a lambda cannot be sent; define a function"
        raise ValueError(msg)
    parameters = list(inspect.signature(function).parameters)
    if not parameters or parameters[0] != "channel":
        msg = f"{function.__name__} must take `channel` first"
        raise ValueError(msg)
    if function.__closure__ is not None:
        msg = f"{function.__name__} uses a closure, which cannot be sent"
        raise ValueError(msg)
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except OSError as error:
        msg = f"cannot find the source of {function.__name__}"
        raise ValueError(msg) from error
    local_names = set(function.__code__.co_varnames)
    used_globals = sorted(
        {
            node.id
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Name)
            and node.id not in local_names
            and node.id not in builtins.__dict__
        }
    )
    if used_globals:
        msg = (
            f"{function.__name__} uses non-builtin globals, which cannot be "
            f"sent: {', '.join(used_globals)}"
        )
        raise ValueError(msg)
    return source


class RemoteExec(Client, service="rsh.remote_exec"):
    """Runs code sent from here on a worker that has rsh.remote_exec enabled.

    ``async with gateway.open(rsh.RemoteExec) as rx``, then each
    ``async with rx.run(code, **kwargs) as channel`` runs one piece of code,
    connected to a channel of its own.
    """

    def __init__(self, channel: Any) -> None:
        super().__init__(channel)
        # a request and the channel answering it must not interleave with
        # another run's
        self._requesting = anyio.Lock()

    @asynccontextmanager
    async def run(self, code: Code, /, **kwargs: Any) -> AsyncIterator[Channel]:
        """Run a function, a module or a source string; raise before sending
        anything when it cannot stand alone."""
        request = prepare(code, kwargs)
        async with self._requesting:
            await self.channel.send(request)
            channel: Channel = await self.channel.receive()
        try:
            yield channel
        finally:
            await channel.aclose()


_counter = itertools.count(1)


async def serve(channel: Channel) -> None:
    """The rsh.remote_exec service: each request runs on a channel of its own,
    created here and sent back; closing the client's channel ends them all."""
    async with anyio.create_task_group() as runs:
        async for request in channel:
            run = channel.new()
            await channel.send(run)
            runs.start_soon(_run, run, request)
        runs.cancel_scope.cancel()


async def _run(channel: Channel, request: dict[str, Any]) -> None:
    with anyio.CancelScope() as scope:
        try:
            result = await _execute(channel, scope, **request)
        except Exception as error:  # noqa: BLE001 - it goes to the caller
            channel.close(error=error)
            return
        channel.close(result=result)
    if scope.cancelled_caught:
        channel.close()


async def _execute(
    channel: Channel,
    scope: anyio.CancelScope,
    *,
    source: str,
    call: str | None,
    kwargs: dict[str, Any],
) -> Any:
    filename = f"<remote_exec #{next(_counter)}>"
    code = compile(source, filename, "exec")
    namespace: dict[str, Any] = {"__name__": "__remote_exec__"}
    if call is None:
        namespace["channel"] = ThreadChannel(channel)
        await anyio.to_thread.run_sync(exec, code, namespace, abandon_on_cancel=True)
        return None
    exec(code, namespace)  # noqa: S102 - running sent code is this service
    function = namespace[call]
    if inspect.iscoroutinefunction(function):
        # as for services: only a task can be cancelled by a close
        channel.handler_scope = scope
        return await function(channel, **kwargs)
    return await anyio.to_thread.run_sync(
        lambda: function(ThreadChannel(channel), **kwargs), abandon_on_cancel=True
    )
