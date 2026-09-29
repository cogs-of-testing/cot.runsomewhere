"""The sync facade: the async API without ``await``, run in an engine host.

``with rsh.sync.open_group() as group:``
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Iterator
from typing import TYPE_CHECKING, Any

import anyio

from ._engine import Host, SubinterpreterEngine, ThreadEngine, host_for
from ._errors import ChannelClosed, StateError
from ._gateway import WorkerInfo

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


def open_group(*, engine: ThreadEngine | SubinterpreterEngine | None = None) -> Group:
    return Group(engine)


class _Scope:
    """A sync scope over an async context manager kept entered in the host."""

    def __init__(self, engine: Host, enter: Callable[[], tuple[int, Any]]) -> None:
        self._engine = engine
        self._enter = enter
        self._handle: int | None = None

    def __enter__(self) -> Any:
        self._handle, value = self._enter()
        return value

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._engine.call("exit", self._handle)


class Group:
    def __init__(self, engine: ThreadEngine | SubinterpreterEngine | None) -> None:
        self._selector = engine
        self.engine: Host | None = None
        self._handle: int | None = None

    def __enter__(self) -> Self:
        _refuse_inside_event_loop()
        self.engine = host_for(self._selector)
        self._handle = self.engine.call("open_group")
        return self

    def __exit__(self, *exc_info: object) -> None:
        assert self.engine is not None
        self.engine.call("exit", self._handle)

    def spawn(
        self,
        place: Place,
        *,
        services: dict[str, bool] | None = None,
        close_timeout: float = 5.0,
    ) -> _Scope:
        engine = self.engine
        if engine is None:
            msg = "the group is not open"
            raise StateError(msg)

        def enter() -> tuple[int, Gateway]:
            handle, info, offered = engine.call(
                "spawn", self._handle, engine.place(place), services, close_timeout
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

        def enter() -> tuple[int, Any]:
            handle = self._engine.call("open", self._handle, service, params)
            channel = Channel(self._engine, handle)
            if isinstance(target, str):
                return handle, channel
            return handle, _SyncClient(target(_NonSuspending(channel)))

        return _Scope(self._engine, enter)


class Channel:
    def __init__(self, engine: Host, handle: int) -> None:
        self._engine = engine
        self._handle = handle

    def send(self, value: object) -> None:
        self._engine.call("send", self._handle, value)

    def receive(self, timeout: float | None = None) -> Any:
        return self._engine.call("receive", self._handle, timeout)

    def wait_closed(self, timeout: float | None = None) -> Any:
        return self._engine.call("wait_closed", self._handle, timeout)

    def __iter__(self) -> Iterator[Any]:
        try:
            while True:
                yield self.receive()
        except ChannelClosed:
            return


class _NonSuspending:
    """An async channel API that completes without suspending, so a client's
    coroutines can be driven to the end synchronously."""

    def __init__(self, channel: Channel) -> None:
        self._channel = channel

    async def send(self, value: object) -> None:
        self._channel.send(value)

    async def receive(self) -> Any:
        return self._channel.receive()

    async def wait_closed(self) -> Any:
        return self._channel.wait_closed()


class _SyncClient:
    def __init__(self, client: Any) -> None:
        self._client = client

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._client, name)
        if not callable(attribute):
            return attribute

        def call(*args: Any, **kwargs: Any) -> Any:
            result = attribute(*args, **kwargs)
            if isinstance(result, Coroutine):
                return _drive(result)
            return result

        return call


def _drive(coroutine: Coroutine[Any, Any, Any]) -> Any:
    try:
        coroutine.send(None)
    except StopIteration as done:
        return done.value
    coroutine.close()
    msg = (
        "a client method awaited something other than its channel, which the "
        "sync facade cannot run"
    )
    raise StateError(msg)
