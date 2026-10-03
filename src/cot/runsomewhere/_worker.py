"""The worker core: the far side of a gateway, and the services it runs."""

from __future__ import annotations

import contextlib
import inspect
import os
from collections.abc import Callable, Mapping
from importlib.metadata import EntryPoint, entry_points
from typing import Any

import anyio
import anyio.abc
import anyio.to_thread

from . import _remote_exec
from ._channels import Channel, Connection
from ._errors import HandshakeRefused
from ._frames import FrameType
from ._handshake import Hello, check_peer
from ._places import place_from_value
from ._thread_channel import ThreadChannel
from ._tunnel import relay
from ._values import decode, encode
from ._version import version as __version__

SERVICE_GROUP = "cot.runsomewhere.services"

Handler = Callable[..., Any]

#: built-in services and whether a worker enables them unless told otherwise
BUILTINS: dict[str, bool] = {
    "rsh.info": True,
    "rsh.via": True,
    "rsh.deploy": True,
    "rsh.transfer": True,
    "rsh.proxy": False,
    "rsh.remote_exec": False,
}


class WorkerCore:
    """Serves one gateway over one byte stream until the caller closes it."""

    def __init__(
        self,
        stream: anyio.abc.ByteStream,
        *,
        services: Mapping[str, Handler] | None = None,
        version: str = __version__,
    ) -> None:
        self._stream = stream
        self._version = version
        self._handlers: dict[str, Handler | EntryPoint]
        if services is None:
            self._handlers = {ep.name: ep for ep in entry_points(group=SERVICE_GROUP)}
        else:
            self._handlers = dict(services)
        self._enabled: frozenset[str] = frozenset()
        self._task_group: anyio.abc.TaskGroup | None = None
        self._connection = Connection(stream, side="worker", on_open=self._on_open)
        self._hello = Hello.local(
            version, frozenset(BUILTINS) | frozenset(self._handlers)
        )

    async def run(self) -> None:
        connection = self._connection
        async with anyio.create_task_group() as outer:
            outer.start_soon(connection.run)
            try:
                await self._serve(connection)
            finally:
                connection.finish_sending()

    async def _serve(self, connection: Connection) -> None:
        connection.send_frame(FrameType.HELLO, 0, encode(self._hello.to_value()))
        try:
            config = decode((await connection.next_control(FrameType.CONFIG)).payload)
        except OSError:
            return
        try:
            check_peer(local=self._hello, remote=Hello.from_value(config["hello"]))
        except HandshakeRefused as refused:
            connection.send_frame(
                FrameType.CONFIG, 0, encode({"ok": False, "error": str(refused)})
            )
            return
        os.environ.update(config.get("env") or {})
        requested: dict[str, bool] = config.get("services") or {}
        self._enabled = frozenset(
            name
            for name in self._hello.services
            if requested.get(name, BUILTINS.get(name, True))
        )
        connection.send_frame(
            FrameType.CONFIG, 0, encode({"ok": True, "services": self._enabled})
        )
        async with anyio.create_task_group() as handlers:
            self._task_group = handlers
            with contextlib.suppress(OSError):
                await connection.next_control(FrameType.GATEWAY_CLOSE)
            handlers.cancel_scope.cancel()

    # -- services -------------------------------------------------------------

    def _on_open(self, channel: Channel, payload: bytes) -> None:
        request = decode(payload, self._connection._channel_for)
        if self._task_group is None:
            channel.close(error=RuntimeError("the worker is not serving yet"))
            return
        self._task_group.start_soon(
            self._run_handler, channel, request["service"], request["params"]
        )

    async def _run_handler(
        self, channel: Channel, name: str, params: dict[str, Any]
    ) -> None:
        with anyio.CancelScope() as scope:
            try:
                result = await self._call(channel, name, params, scope)
            except Exception as error:  # noqa: BLE001 - it goes to the caller
                channel.close(error=error)
                return
            channel.close(result=result)
        if scope.cancelled_caught:
            channel.close()

    async def _call(
        self,
        channel: Channel,
        name: str,
        params: dict[str, Any],
        scope: anyio.CancelScope,
    ) -> Any:
        if name not in self._enabled:
            msg = f"this worker does not offer the service {name!r}"
            raise LookupError(msg)
        builtin = _BUILTIN_HANDLERS.get(name)
        if builtin is not None:
            return await builtin(self, channel, **params)
        handler = self._handlers[name]
        if isinstance(handler, EntryPoint):
            handler = self._handlers[name] = handler.load()
        if inspect.iscoroutinefunction(handler):
            # only a task can be cancelled; a sync handler's thread learns of
            # a close from its next channel operation
            channel.handler_scope = scope
            return await handler(channel, **params)
        return await anyio.to_thread.run_sync(
            lambda: handler(ThreadChannel(channel), **params), abandon_on_cancel=True
        )

    async def _info(self, _channel: Channel) -> Any:
        return self._hello.to_value()

    async def _remote_exec(self, channel: Channel) -> None:
        await _remote_exec.serve(channel)

    async def _via(self, channel: Channel, *, place: dict[str, Any]) -> None:
        await relay(channel, place_from_value(place))

    async def _not_yet(self, channel: Channel, **params: Any) -> None:
        msg = "this built-in service is not implemented yet"
        raise NotImplementedError(msg)


_BUILTIN_HANDLERS: dict[str, Callable[..., Any]] = {
    "rsh.info": WorkerCore._info,
    "rsh.via": WorkerCore._via,
    "rsh.remote_exec": WorkerCore._remote_exec,
    "rsh.deploy": WorkerCore._not_yet,
    "rsh.transfer": WorkerCore._not_yet,
    "rsh.proxy": WorkerCore._not_yet,
}
