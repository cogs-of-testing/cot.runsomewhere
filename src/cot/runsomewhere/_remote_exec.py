"""Remote exec: code sent as text, checked here, run on the worker."""

from __future__ import annotations

import ast
import builtins
import inspect
import itertools
import textwrap
import types
from collections.abc import Callable
from typing import Any

import anyio
import anyio.to_thread

from ._channels import Channel
from ._thread_channel import ThreadChannel

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
        raise TypeError(f"cannot remote_exec {type(code).__qualname__} objects")
    if call is None and kwargs:
        raise TypeError("keyword arguments are only passed to functions")
    return {"source": source, "call": call, "kwargs": kwargs}


def _source_of_function(function: types.FunctionType) -> str:
    if function.__name__ == "<lambda>":
        raise ValueError("a lambda cannot be sent; define a function")
    parameters = list(inspect.signature(function).parameters)
    if not parameters or parameters[0] != "channel":
        raise ValueError(f"{function.__name__} must take `channel` first")
    if function.__closure__ is not None:
        raise ValueError(f"{function.__name__} uses a closure, which cannot be sent")
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except OSError as error:
        raise ValueError(f"cannot find the source of {function.__name__}") from error
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
        raise ValueError(
            f"{function.__name__} uses non-builtin globals, which cannot be "
            f"sent: {', '.join(used_globals)}"
        )
    return source


_counter = itertools.count(1)


async def serve(
    channel: Channel, *, source: str, call: str | None, kwargs: dict[str, Any]
) -> Any:
    """The rsh.remote_exec service."""
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
        return await function(channel, **kwargs)
    return await anyio.to_thread.run_sync(
        lambda: function(ThreadChannel(channel), **kwargs), abandon_on_cancel=True
    )
