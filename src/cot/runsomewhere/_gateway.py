"""The caller side: groups, gateways, and the scopes that open them."""

from __future__ import annotations

import contextlib
import sys
import warnings
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, overload

import anyio
import anyio.abc
from anyio.lowlevel import cancel_shielded_checkpoint, checkpoint_if_cancelled

from ._channels import Channel, Connection
from ._errors import HandshakeRefused, RemoteError, StateError
from ._handshake import Hello, check_peer
from ._places import Launched, Place
from ._shutdown import Shutdown, Teardown
from ._tunnel import ChannelByteStream
from ._version import version as __version__

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from types import TracebackType

    from typing_extensions import Self

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


class Client:
    """Base for the object a service's package ships to give its API."""

    service: ClassVar[str]

    def __init_subclass__(cls, *, service: str | None = None, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if service is not None:
            cls.service = service

    def __init__(self, channel: Any) -> None:
        self.channel = channel

    def stop(self, deadline: float | None = None) -> None:
        """Ask the service to finish; see `Channel.stop`."""
        self.channel.stop(deadline)


ClientT = TypeVar("ClientT", bound=Client)
OpenedT = TypeVar("OpenedT")


@dataclass(frozen=True)
class WorkerInfo:
    """What the worker reported about itself in the handshake."""

    python: str
    version: str
    platform: str
    pid: int
    executable: str

    @classmethod
    def from_hello(cls, hello: Hello) -> WorkerInfo:
        return cls(
            hello.python, hello.version, hello.platform, hello.pid, hello.executable
        )


class Group:
    def __init__(self, *, shutdown: Shutdown | None = None) -> None:
        self.shutdown = shutdown or Shutdown()
        self._task_group: anyio.abc.TaskGroup | None = None
        self._closed = False

    async def __aenter__(self) -> Self:
        self._task_group = anyio.create_task_group()
        await self._task_group.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        self._closed = True
        assert self._task_group is not None
        try:
            return await self._task_group.__aexit__(exc_type, exc, tb)
        except BaseExceptionGroup as group:
            # the task group is ours, not the caller's: an error the caller's
            # block raised comes back as itself, not wrapped
            if exc is not None and group.exceptions == (exc,):
                return False
            raise

    def spawn(
        self,
        place: Place,
        *,
        services: Mapping[str, bool] | None = None,
        deploy: Any = None,
        teardown: Teardown | None = None,
        through: Gateway | None = None,
    ) -> _Spawn:
        """Start a worker at ``place``; ``teardown`` overrides the targets the
        group's shutdown policy gives it. ``through`` names a gateway the new
        worker depends on without being tunnelled through it, such as one
        whose forwarded port it is reached by: that gateway is torn down after
        the new one."""
        if deploy is not None:
            msg = "deploying on spawn is not implemented yet"
            raise StateError(msg)
        proxy = None
        if through is not None:
            proxy = through._spawn_scope
            proxy.proxy = True
        return _Spawn(self, place, place.launch, services, teardown, proxy=proxy)

    def _tasks(self) -> anyio.abc.TaskGroup:
        if self._task_group is None or self._closed:
            msg = "the group is not open"
            raise StateError(msg)
        return self._task_group


class Gateway:
    """The link to one worker."""

    def __init__(
        self,
        connection: Connection,
        worker: WorkerInfo,
        services: frozenset[str],
        group: Group,
        spawn: _Spawn,
    ) -> None:
        self._connection = connection
        self.worker = worker
        self.services = services
        self._group = group
        self._spawn_scope = spawn
        self._closed = False

    def __repr__(self) -> str:
        return f"<Gateway to pid {self.worker.pid}>"

    @overload
    def open(self, target: str, /, **params: Any) -> _Open[Channel]: ...
    @overload
    def open(self, target: type[ClientT], /, **params: Any) -> _Open[ClientT]: ...
    def open(self, target: str | type[Client], /, **params: Any) -> _Open[Any]:
        """Open a service: by name for its channel, by client class for its API."""
        if isinstance(target, str):
            return _Open(self._opener(target, params), lambda channel: channel)
        return _Open(self._opener(target.service, params), target)

    def spawn(
        self,
        place: Place,
        *,
        services: Mapping[str, bool] | None = None,
        teardown: Teardown | None = None,
    ) -> _Spawn:
        """Spawn a worker reachable from this one, tunnelled through it; this
        worker becomes a proxy in the teardown graph."""
        leaf = teardown or self._group.shutdown.edge

        async def launch(_task_group: anyio.abc.TaskGroup) -> Launched:
            self._require("rsh.via")
            # the leaf's targets, for the relay to fall back on when the
            # caller cannot drive the teardown
            params = {"place": place.to_value(), "teardown": leaf.to_value()}
            channel = await self._open_channel("rsh.via", params)
            channel.tunnel = True

            async def exited() -> None:
                # the relay closes the tunnel once its leaf has exited
                with contextlib.suppress(OSError, RemoteError):
                    await channel.wait_closed()

            async def force() -> None:
                channel.close()

            return Launched(ChannelByteStream(channel), exited, force)

        self._spawn_scope.proxy = True
        return _Spawn(
            self._group, place, launch, services, teardown, proxy=self._spawn_scope
        )

    def _opener(
        self, service: str, params: dict[str, Any]
    ) -> Callable[[], Awaitable[Channel]]:
        return lambda: self._open_channel(service, params)

    def _require(self, service: str) -> None:
        self._raise_if_unusable()
        if service not in self.services:
            msg = f"this worker does not offer the service {service!r}"
            raise StateError(msg)

    def _raise_if_unusable(self) -> None:
        if self._closed:
            msg = f"{self!r} is closed"
            raise StateError(msg)
        if self._connection.failure is not None:
            raise self._connection.failure

    async def _open_channel(self, service: str, params: dict[str, Any]) -> Channel:
        self._require(service)
        connection = self._connection
        # refused before a channel exists for it
        connection.encode_value(params)
        channel = connection.new_channel()
        connection.send_control(
            "open", channel=channel.id, service=service, params=params
        )
        await cancel_shielded_checkpoint()
        return channel


class _Open(Generic[OpenedT]):
    """`async with gateway.open(...)`: a channel or client, closed on exit."""

    def __init__(
        self,
        opener: Callable[[], Awaitable[Channel]],
        wrap: Callable[[Channel], OpenedT],
    ) -> None:
        self._opener = opener
        self._wrap = wrap
        self._channel: Channel | None = None

    async def __aenter__(self) -> OpenedT:
        self._channel = await self._opener()
        return self._wrap(self._channel)

    async def __aexit__(self, *exc_info: object) -> None:
        assert self._channel is not None
        await self._channel.aclose()


async def _stop_left_open(connection: Connection, until: float) -> None:
    """The stop phase of a gateway's shutdown: the worker takes no new
    service calls, and every channel still open is stopped and drained."""
    connection.stopping = True
    connection.send_control("gateway-stop")
    # tunnels are edges of the teardown graph, torn down with their leaves
    left_open = [channel for channel in connection.channels() if not channel.tunnel]
    for channel in left_open:
        channel.stop(deadline=max(0.0, until - anyio.current_time()))
    outcomes = dict.fromkeys(left_open, "")

    async def finish(channel: Channel) -> None:
        try:
            await channel.drain()
        except OSError as error:
            # a drain during a shutdown records what went wrong, and the
            # shutdown goes on
            outcomes[channel] = f": {error}"
        with contextlib.suppress(OSError, RemoteError):
            await channel.wait_closed()

    with anyio.move_on_at(until):
        async with anyio.create_task_group() as finishing:
            for channel in left_open:
                finishing.start_soon(finish, channel)
    for channel, outcome in outcomes.items():
        warnings.warn(
            f"{channel!r} was still open when its gateway shut down{outcome}",
            ResourceWarning,
            stacklevel=1,
        )


def _accepted(answer: dict[str, Any]) -> dict[str, Any]:
    """The worker's answer to the configuration, unless it refused."""
    if not answer["ok"]:
        raise HandshakeRefused(answer["error"])
    return answer


class _Spawn:
    """`async with group.spawn(...)`: a gateway, closed with its worker on exit."""

    def __init__(
        self,
        group: Group,
        place: Place,
        launch: Callable[[anyio.abc.TaskGroup], Awaitable[Launched]],
        services: Mapping[str, bool] | None,
        teardown: Teardown | None,
        *,
        proxy: _Spawn | None = None,
    ) -> None:
        self._group = group
        self._place = place
        self._launch = launch
        self._services = dict(services or {})
        self._teardown = teardown
        #: whether workers were spawned through this one
        self.proxy = False
        #: the spawn of the worker this one is tunnelled through
        self._through = proxy
        #: open spawns tunnelled through this one: torn down before it
        self._dependents: set[_Spawn] = set()
        self._closing = False
        self._closed = anyio.Event()
        self._connection_scope = anyio.CancelScope(shield=True)
        self._connection_done = anyio.Event()
        self._launched: Launched | None = None
        self._gateway: Gateway | None = None
        self._connection: Connection | None = None

    async def __aenter__(self) -> Gateway:
        tasks = self._group._tasks()
        try:
            self._launched = await self._launch(tasks)
            connection = self._connection = Connection(
                self._launched.stream, side="caller"
            )
            tasks.start_soon(self._run_connection, connection)
            hello = await connection.next_control("hello")
            del hello["op"]
            remote = Hello.from_value(hello)
            connection.peer = f"the worker at pid {remote.pid} ({remote.version})"
            local = Hello.local(__version__)
            check_peer(local=local, remote=remote)
            config = {
                "hello": local.to_value(),
                "services": self._services,
                **self._place.worker_config(),
            }
            connection.send_control("config", config=config)
            answer = _accepted(await connection.next_control("config"))
        except BaseException:
            await self._close(graceful=False)
            raise
        self._gateway = Gateway(
            connection,
            WorkerInfo.from_hello(remote),
            answer["services"],
            self._group,
            self,
        )
        if self._through is not None:
            self._through._dependents.add(self)
        return self._gateway

    async def __aexit__(self, *exc_info: object) -> None:
        await self._close(graceful=True)

    async def _run_connection(self, connection: Connection) -> None:
        # shielded inside the task, so an outer cancel does not cut a graceful
        # close short; _close cancels it when it is done
        try:
            with self._connection_scope:
                await connection.run()
        finally:
            self._connection_done.set()

    async def _close(self, *, graceful: bool) -> None:
        """Tear down this worker's dependents, concurrently, then the worker.

        A dependent is closed once: its own block, leaving later, finds it
        done.
        """
        if self._closing:
            with anyio.CancelScope(shield=True):
                await self._closed.wait()
            return
        self._closing = True
        try:
            try:
                await self._close_dependents()
            finally:
                await self._close_worker(graceful=graceful)
        finally:
            self._closed.set()
            if self._through is not None:
                self._through._dependents.discard(self)

    async def _close_dependents(self) -> None:
        if not self._dependents:
            return
        async with anyio.create_task_group() as dependents:
            for dependent in list(self._dependents):
                dependents.start_soon(partial(dependent._close, graceful=True))

    async def _close_worker(self, *, graceful: bool) -> None:
        """Stop what is still open, ask the worker to exit, wait for it until
        the deadline, then have the place force it. A cancelled scope goes
        straight to force."""
        if self._gateway is not None:
            self._gateway._closed = True
        connection = self._connection
        healthy = graceful and connection is not None and connection.failure is None
        if not healthy:
            # without a gateway terminate, the end of the stream is what tells the
            # worker to go
            self._connection_scope.cancel()
        policy = self._group.shutdown
        targets = self._teardown or (policy.proxy if self.proxy else policy.edge)
        start = anyio.current_time()
        end = start + targets.total
        exited = False
        try:
            await checkpoint_if_cancelled()
            with anyio.move_on_at(end):
                if healthy:
                    assert connection is not None
                    await _stop_left_open(connection, start + targets.stop)
                    remaining = max(0.0, end - anyio.current_time())
                    connection.send_control("gateway-terminate", deadline=remaining)
                if connection is not None:
                    await connection.gone.wait()
                if self._launched is not None:
                    await self._launched.exited()
                exited = True
        finally:
            with anyio.CancelScope(shield=True):
                self._connection_scope.cancel()
                if connection is not None:
                    await self._connection_done.wait()
                if self._launched is not None and not exited:
                    await self._launched.force()
