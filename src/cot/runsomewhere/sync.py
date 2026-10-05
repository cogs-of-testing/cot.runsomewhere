"""The sync facade: the async API without ``await``, run in an engine host.

``with rsh.sync.open_group() as group:``
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Iterator
from contextlib import AbstractAsyncContextManager, ExitStack
from typing import TYPE_CHECKING, Any

import anyio
import anyio.from_thread

from ._engine import (
    DEFAULT_ENGINE,
    AsyncHostedChannel,
    Engine,
    Host,
    HostedChannel,
    selected_engine,
)
from ._errors import ChannelClosed, StateError
from ._gateway import WorkerInfo
from ._shutdown import Shutdown, Teardown

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self

    from ._places import Place

__all__ = ["open_group"]


def _refuse_inside_event_loop() -> None:
    try:
        anyio.get_current_task()
    except RuntimeError:
        return
    msg = (
        "the sync facade would stall the running event loop; use the async "
        "API, `async with rsh.open_group()`"
    )
    raise StateError(msg)


def open_group(*, shutdown: Shutdown | None = None) -> Group:
    """The scope gateways are spawned in, torn down by ``shutdown``, run in the
    engine set with ``rsh.use_engine``, or in the default engine."""
    return Group(selected_engine() or DEFAULT_ENGINE, shutdown=shutdown)


class _Scope:
    """A sync scope over an async context manager kept entered in the host."""

    def __init__(
        self, engine: Host, enter: Callable[[ExitStack], tuple[int, Any]]
    ) -> None:
        self._engine = engine
        self._enter = enter
        self._handle: int | None = None
        #: what the value needs closed after the host has left the scope
        self._after = ExitStack()

    def __enter__(self) -> Any:
        self._handle, value = self._enter(self._after)
        return value

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        with self._after:
            self._engine.call("exit", self._handle)


class Group:
    def __init__(self, engine: Engine, *, shutdown: Shutdown | None = None) -> None:
        self._selected = engine
        self.shutdown = shutdown or Shutdown()
        self.engine: Host | None = None
        self._handle: int | None = None

    def __enter__(self) -> Self:
        _refuse_inside_event_loop()
        self.engine = self._selected.host()
        self._handle = self.engine.call("open_group", self.shutdown.to_value())
        return self

    def __exit__(self, *exc_info: object) -> None:
        assert self.engine is not None
        self.engine.call("exit", self._handle)

    def spawn(
        self,
        place: Place,
        *,
        services: dict[str, bool] | None = None,
        teardown: Teardown | None = None,
    ) -> _Scope:
        engine = self.engine
        if engine is None:
            msg = "the group is not open"
            raise StateError(msg)

        def enter(_after: ExitStack) -> tuple[int, Gateway]:
            handle, info, offered = engine.call(
                "spawn",
                self._handle,
                engine.place(place),
                services,
                None if teardown is None else teardown.to_value(),
            )
            return handle, Gateway(engine, handle, WorkerInfo(**info), offered)

        return _Scope(engine, enter)


class Gateway:
    def __init__(
        self, engine: Host, handle: int, worker: WorkerInfo, services: frozenset[str]
    ) -> None:
        self._engine = engine
        self._handle = handle
        self.worker = worker
        self.services = services

    def open(self, target: Any, /, **params: Any) -> _Scope:
        """Open a service: by name for its channel, by client class for its
        API, with every async method of the client made sync."""
        service = target if isinstance(target, str) else target.service

        def enter(after: ExitStack) -> tuple[int, Any]:
            handle = self._engine.call("open", self._handle, service, params)
            if isinstance(target, str):
                return handle, Channel(self._engine, handle)
            # the client's async code runs in an event loop of its own, which
            # outlives every call, so what one call starts the next can finish
            portal = after.enter_context(anyio.from_thread.start_blocking_portal())
            client = target(AsyncHostedChannel(self._engine, handle))
            return handle, _SyncClient(client, portal)

        return _Scope(self._engine, enter)


class Channel(HostedChannel):
    def send(self, value: object) -> None:
        self._engine.call("send", self._handle, self._engine.export(value))

    def receive(self, timeout: float | None = None) -> Any:
        data = self._engine.call("receive", self._handle, timeout)
        return self._engine.import_value(data, Channel)

    def wait_closed(self, timeout: float | None = None) -> Any:
        data = self._engine.call("wait_closed", self._handle, timeout)
        return self._engine.import_value(data, Channel)

    def drain(self, timeout: float | None = None) -> None:
        self._engine.call("drain", self._handle, timeout)

    def stop(self, deadline: float | None = None) -> None:
        self._engine.call("stop", self._handle, deadline)

    def close_send(self) -> None:
        self._engine.call("close_send", self._handle)

    def close_receive(self) -> None:
        self._engine.call("close_receive", self._handle)

    def close(self) -> None:
        """Close a channel that arrived as a value; one a block opened closes
        with its block."""
        self._engine.call("close", self._handle)

    def __iter__(self) -> Iterator[Any]:
        try:
            while True:
                yield self.receive()
        except ChannelClosed:
            return


def _as_sync(value: Any) -> Any:
    """A channel the client's async code handed out, as the sync API."""
    if isinstance(value, AsyncHostedChannel):
        return Channel(value._engine, value._handle)
    return value


async def _await(coroutine: Coroutine[Any, Any, Any]) -> Any:
    return await coroutine


class _SyncClient:
    """A client's API without ``await``: each method runs in the client's own
    event loop, and channels it returns come back with the sync API."""

    def __init__(self, client: Any, portal: anyio.from_thread.BlockingPortal) -> None:
        self._client = client
        self._portal = portal

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return _as_sync(attribute)

        def call(*args: Any, **kwargs: Any) -> Any:
            result = attribute(*args, **kwargs)
            if isinstance(result, Coroutine):
                return _as_sync(self._portal.call(_await, result))
            if isinstance(result, AbstractAsyncContextManager):
                return _SyncContext(self._portal, result)
            return _as_sync(result)

        return call


class _SyncContext:
    """A client's async context manager, entered and left in its event loop."""

    def __init__(
        self,
        portal: anyio.from_thread.BlockingPortal,
        context: AbstractAsyncContextManager[Any],
    ) -> None:
        self._portal = portal
        self._context = context

    def __enter__(self) -> Any:
        return _as_sync(self._portal.call(self._context.__aenter__))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        return self._portal.call(self._context.__aexit__, exc_type, exc, tb)
