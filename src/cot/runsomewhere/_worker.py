"""The worker core: the far side of a gateway, and the services it runs."""

from __future__ import annotations

import contextlib
import inspect
import os
import warnings
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
from ._shutdown import Teardown
from ._thread_channel import ThreadChannel
from ._tunnel import relay
from ._values import decode, encode
from ._version import version as __version__

SERVICE_GROUP = "cot.runsomewhere.services"

#: how much sooner than its caller's deadline a worker gives up on its
#: handlers, so it has exited before the caller forces it
HOP_MARGIN = 0.5

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
        #: sync handlers still running in their threads, by a token of each call
        self._in_threads: dict[object, str] = {}
        self._running = 0
        self._close_requested = anyio.Event()
        self._close_deadline = 0.0
        #: when this worker stops waiting for its handlers, on the loop clock
        self._closing_at: float | None = None
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
                self._warn_abandoned()

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
            deadline = max(0.0, await self._until_close(connection) - HOP_MARGIN)
            self._closing_at = anyio.current_time() + deadline
            await self._stop_handlers(deadline)
            handlers.cancel_scope.cancel()

    def request_close(self, deadline: float = 0.0) -> None:
        """Shut down as on a gateway-close carrying ``deadline``."""
        if not self._close_requested.is_set():
            self._close_deadline = deadline
            self._close_requested.set()

    async def _until_close(self, connection: Connection) -> float:
        async def gateway_close() -> None:
            deadline = 0.0
            with contextlib.suppress(OSError):
                frame = await connection.next_control(FrameType.GATEWAY_CLOSE)
                if frame.payload:
                    deadline = decode(frame.payload).get("deadline") or 0.0
            self.request_close(deadline)

        async with anyio.create_task_group() as waiting:
            waiting.start_soon(gateway_close)
            await self._close_requested.wait()
            waiting.cancel_scope.cancel()
        return self._close_deadline

    def _leaf_budget(self, leaf: Teardown) -> Callable[[], float]:
        def budget() -> float:
            if self._closing_at is None:
                # not shutting down: the leaf ended its stream, and gets a
                # moment to finish exiting
                return HOP_MARGIN
            # the caller could not drive the teardown: fall back on the
            # targets it sent with the tunnel
            return leaf.total

        return budget

    async def _stop_handlers(self, deadline: float) -> None:
        """Close every channel, and give the handlers until the deadline to
        return before they are cancelled and their threads left."""
        for channel in self._connection.channels():
            if channel.handler_scope is not None:
                channel.handler_scope.cancel()
            channel.close()
        with anyio.move_on_after(deadline):
            while self._running:
                await anyio.sleep(0.01)

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
        self._running += 1
        try:
            await self._run_handler_in_scope(channel, name, params)
        finally:
            self._running -= 1

    async def _run_handler_in_scope(
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
        token = object()
        self._in_threads[token] = name

        def run() -> Any:
            try:
                return handler(ThreadChannel(channel), **params)
            finally:
                self._in_threads.pop(token, None)

        return await anyio.to_thread.run_sync(run, abandon_on_cancel=True)

    def _warn_abandoned(self) -> None:
        # a thread cannot be stopped from outside: all a worker can do with a
        # handler that ignored its closed channel is leave it and say so
        for name in list(self._in_threads.values()):
            warnings.warn(
                f"the sync handler of {name!r} ignored its closed channel and "
                "was left running",
                ResourceWarning,
                stacklevel=1,
            )

    async def _info(self, _channel: Channel) -> Any:
        return self._hello.to_value()

    async def _remote_exec(self, channel: Channel) -> None:
        await _remote_exec.serve(channel)

    async def _via(
        self,
        channel: Channel,
        *,
        place: dict[str, Any],
        teardown: dict[str, Any] | None = None,
    ) -> None:
        leaf = Teardown() if teardown is None else Teardown.from_value(teardown)
        await relay(channel, place_from_value(place), self._leaf_budget(leaf))

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
