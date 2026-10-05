"""Engine hosts: the async core run away from the caller, reached by message.

A host runs a :class:`HostServer` in its own event loop, on its own thread,
optionally in its own subinterpreter. The facades in the main interpreter send
it requests and wait for the answers. Through a thread host, messages are
Python objects; through a subinterpreter host they are encoded values, since
no object is shared between interpreters.

What a channel carries crosses as encoded values through either host, so a
channel inside one becomes a handle the facade wraps, never the host's own
channel object, which belongs to the host's event loop.
"""

from __future__ import annotations

import importlib
import itertools
import queue
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, ClassVar

import anyio
import anyio.abc
import anyio.lowlevel
import anyio.to_thread

from . import _errors
from ._channels import is_channel
from ._errors import StateError
from ._gateway import Group, WorkerInfo
from ._places import Place, place_from_value
from ._shutdown import Shutdown, Teardown
from ._values import DecodeError, decode, encode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from typing_extensions import Self

Message = tuple[Any, ...]

#: the value codec's extension code for a channel handle between a facade and
#: its host
CHANNEL_HANDLE = 1


class Engine:
    """Where the facades run the async core; its host starts on first use."""

    kind: ClassVar[str]

    def __init__(self) -> None:
        self._host: Host | None = None
        self._lock = threading.Lock()

    def host(self) -> Host:
        with self._lock:
            if self._host is None:
                self._host = Host(self.kind)
            return self._host


class ThreadEngine(Engine):
    """The thread host: an event loop on a dedicated OS thread."""

    kind = "thread"


class SubinterpreterEngine(Engine):
    """The subinterpreter host: an event loop on a thread in its own
    interpreter, with its own GIL. Python 3.14 and newer."""

    kind = "subinterpreter"


#: used by the facades when no override is set; its host starts lazily
DEFAULT_ENGINE = ThreadEngine()

_selected: ContextVar[Engine | None] = ContextVar(
    "cot.runsomewhere.engine", default=None
)


@contextmanager
def use_engine(engine: Engine) -> Iterator[Engine]:
    """Open groups in this context through ``engine``.

    The sync facade uses it instead of the default engine; the async API runs
    through it instead of in the caller's loop.
    """
    token = _selected.set(engine)
    try:
        yield engine
    finally:
        _selected.reset(token)


def selected_engine() -> Engine | None:
    """The engine set with :func:`use_engine` in this context, if any."""
    return _selected.get()


# -- the host side ------------------------------------------------------------


class _Holder:
    """Keeps an async context manager entered in a task of its own, since a
    scope must be left by the task that entered it."""

    def __init__(self) -> None:
        self.leave = anyio.Event()
        self.left = anyio.Event()
        self.value: Any = None
        self.error: Exception | None = None

    async def hold(self, cm: Any, *, task_status: anyio.abc.TaskStatus[None]) -> None:
        started = False
        try:
            async with cm as value:
                self.value = value
                started = True
                task_status.started()
                await self.leave.wait()
        except Exception as error:
            if not started:
                raise
            self.error = error
        finally:
            self.left.set()


class HostServer:
    def __init__(self) -> None:
        self._objects: dict[int, Any] = {}
        #: the handle of each channel a value carried out, by the channel's id
        self._handle_of: dict[int, int] = {}
        self._holders: dict[int, _Holder] = {}
        self._ids = itertools.count(1)
        self._scopes: dict[Any, anyio.CancelScope] = {}
        self._tasks: anyio.abc.TaskGroup | None = None

    async def serve(
        self, get: Callable[[], Message | None], put: Callable[[Message], None]
    ) -> None:
        async with anyio.create_task_group() as tasks:
            self._tasks = tasks
            while True:
                message = await anyio.to_thread.run_sync(get, abandon_on_cancel=True)
                if message is None:
                    tasks.cancel_scope.cancel()
                    return
                request_id, op, args = message
                if op == "cancel":
                    scope = self._scopes.get(args[0])
                    if scope is not None:
                        scope.cancel()
                    continue
                tasks.start_soon(self._run, request_id, op, args, put)

    async def _run(
        self,
        request_id: Any,
        op: str,
        args: tuple[Any, ...],
        put: Callable[[Message], None],
    ) -> None:
        with anyio.CancelScope() as scope:
            self._scopes[request_id] = scope
            try:
                result = await getattr(self, f"op_{op}")(*args)
            except Exception as error:  # noqa: BLE001 - it goes to the caller
                put((request_id, "error", _error_value(error)))
            else:
                put((request_id, "ok", result))
        del self._scopes[request_id]
        if scope.cancelled_caught:
            put((request_id, "cancelled", None))

    async def _hold(self, cm: Any) -> tuple[int, Any]:
        assert self._tasks is not None
        holder = _Holder()
        await self._tasks.start(holder.hold, cm)
        handle = next(self._ids)
        self._holders[handle] = holder
        self._objects[handle] = holder.value
        return handle, holder.value

    async def op_open_group(self, shutdown: dict[str, Any] | None) -> int:
        policy = None if shutdown is None else Shutdown.from_value(shutdown)
        handle, _ = await self._hold(Group(shutdown=policy))
        return handle

    async def op_spawn(
        self,
        group: int,
        place: Place | dict[str, Any],
        services: dict[str, bool] | None,
        teardown: dict[str, Any] | None,
    ) -> tuple[int, dict[str, Any], frozenset[str]]:
        if isinstance(place, dict):
            place = place_from_value(place)
        targets = None if teardown is None else Teardown.from_value(teardown)
        handle, gateway = await self._hold(
            self._objects[group].spawn(place, services=services, teardown=targets)
        )
        worker = gateway.worker
        info = {
            "python": worker.python,
            "version": worker.version,
            "platform": worker.platform,
            "pid": worker.pid,
            "executable": worker.executable,
        }
        return handle, info, frozenset(gateway.services)

    async def op_open(self, gateway: int, service: str, params: dict[str, Any]) -> int:
        handle, _ = await self._hold(self._objects[gateway].open(service, **params))
        return handle

    async def op_exit(self, handle: int) -> None:
        holder = self._holders.pop(handle)
        del self._objects[handle]
        holder.leave.set()
        await holder.left.wait()
        if holder.error is not None:
            raise holder.error

    def _export(self, value: Any) -> bytes:
        """A value for the facade: each channel in it becomes a handle."""
        return encode(value, self._channel_handle)

    def _channel_handle(self, value: Any) -> tuple[int, int]:
        if not is_channel(value):
            msg = f"cannot send {type(value).__qualname__} values: {value!r}"
            raise TypeError(msg)
        handle = self._handle_of.get(id(value))
        if handle is None:
            handle = self._handle_of[id(value)] = next(self._ids)
            self._objects[handle] = value
        return CHANNEL_HANDLE, handle

    def _import(self, data: bytes) -> Any:
        """A value from the facade: each handle in it becomes its channel."""
        return decode(data, self._channel_of)

    def _channel_of(self, code: int, handle: Any) -> Any:
        channel = self._objects.get(handle) if code == CHANNEL_HANDLE else None
        if not is_channel(channel):
            msg = f"no channel has the handle {handle!r} in this engine host"
            raise DecodeError(msg)
        return channel

    async def op_send(self, channel: int, data: bytes) -> None:
        await self._objects[channel].send(self._import(data))

    async def op_close(self, channel: int) -> None:
        """Close a channel; one that arrived as a value is forgotten too, one
        a block opened stays until its block is left."""
        if channel in self._holders:
            self._objects[channel].close()
            return
        value = self._objects.pop(channel)
        self._handle_of.pop(id(value), None)
        value.close()

    async def op_drain(self, channel: int, timeout: float | None) -> None:
        with anyio.fail_after(timeout):
            await self._objects[channel].drain()

    async def op_stop(self, channel: int, deadline: float | None) -> None:
        self._objects[channel].stop(deadline)

    async def op_close_send(self, channel: int) -> None:
        self._objects[channel].close_send()

    async def op_close_receive(self, channel: int) -> None:
        self._objects[channel].close_receive()

    async def op_receive(self, channel: int, timeout: float | None) -> bytes:
        with anyio.fail_after(timeout):
            return self._export(await self._objects[channel].receive())

    async def op_wait_closed(self, channel: int, timeout: float | None) -> bytes:
        with anyio.fail_after(timeout):
            return self._export(await self._objects[channel].wait_closed())


_ERRORS: dict[str, type[Exception]] = {
    name: getattr(_errors, name)
    for name in [
        "RemoteError",
        "ChannelClosed",
        "WorkerGone",
        "HostNotFound",
        "HandshakeRefused",
        "ItemsDiscarded",
        "StateError",
    ]
}
_ERRORS.update(
    {
        cls.__name__: cls
        for cls in [
            TimeoutError,
            TypeError,
            ValueError,
            LookupError,
            KeyError,
            NotImplementedError,
        ]
    }
)


def _error_value(error: Exception) -> tuple[str, str, str, dict[str, int]]:
    counts = {}
    if isinstance(error, _errors.ItemsDiscarded):
        counts = {"taken": error.taken, "discarded": error.discarded}
    return (
        type(error).__name__,
        str(error),
        getattr(error, "remote_traceback", ""),
        counts,
    )


def _raise_error(value: tuple[str, str, str, dict[str, int]]) -> None:
    name, message, remote_traceback, counts = value
    kind = _ERRORS.get(name, RuntimeError)
    if kind is _errors.RemoteError:
        raise _errors.RemoteError(message, remote_traceback=remote_traceback)
    if kind is _errors.ItemsDiscarded:
        raise _errors.ItemsDiscarded(message, **counts)
    raise kind(message)


def _subinterpreter_main(requests: Any, responses: Any) -> None:
    """Runs in the subinterpreter: the host loop, fed encoded messages."""

    def get() -> Message | None:
        data = requests.get()
        return None if data is None else tuple(decode(data))

    def put(message: Message) -> None:
        try:
            responses.put(encode(message))
        except TypeError as unsendable:
            responses.put(encode((message[0], "error", _error_value(unsendable))))

    anyio.run(HostServer().serve, get, put)


# -- the main-interpreter side ------------------------------------------------


class _Pending:
    def __init__(self, request_id: int) -> None:
        self.request_id = request_id
        self._done = threading.Event()
        self._status = ""
        self._payload: Any = None

    def resolve(self, status: str, payload: Any) -> None:
        self._status, self._payload = status, payload
        self._done.set()

    def result(self) -> Any:
        self._done.wait()
        if self._status == "error":
            _raise_error(self._payload)
        if self._status == "cancelled":
            msg = "the request was cancelled"
            raise StateError(msg)
        return self._payload


class Host:
    """The main interpreter's handle on one engine host."""

    def __init__(self, kind: str) -> None:
        self.kind = kind
        self._ids = itertools.count(1)
        self._pending: dict[int, _Pending] = {}
        self._lock = threading.Lock()
        self._put: Callable[[Message | None], None]
        self._get: Callable[[], Message]
        if kind == "thread":
            self._start_thread_host()
        else:
            self._start_subinterpreter_host()
        threading.Thread(
            target=self._dispatch, name=f"rsh-{kind}-replies", daemon=True
        ).start()
        threading.Thread(
            target=self._stop_after_main, name=f"rsh-{kind}-watch", daemon=True
        ).start()

    def _stop_after_main(self) -> None:
        # at interpreter shutdown the main thread counts as finished before
        # non-daemon threads are joined, so this wakes while the host, which
        # is one, still runs; groups left open close as cancelled
        threading.main_thread().join()
        self.shutdown()

    def _start_thread_host(self) -> None:
        requests: queue.Queue[Message | None] = queue.Queue()
        responses: queue.Queue[Message] = queue.Queue()
        self._put, self._get = requests.put, responses.get
        self._thread = threading.Thread(
            target=anyio.run,
            args=(HostServer().serve, requests.get, responses.put),
            name="rsh-thread-engine",
            daemon=False,
        )
        self._thread.start()

    def _start_subinterpreter_host(self) -> None:
        try:
            interpreters: Any = importlib.import_module("concurrent.interpreters")
        except ImportError:
            msg = "the subinterpreter engine needs Python 3.14 or newer"
            raise StateError(msg) from None
        requests = interpreters.create_queue()
        responses = interpreters.create_queue()
        self._interpreter = interpreters.create()

        def put(message: Message | None) -> None:
            requests.put(None if message is None else encode(message))

        self._put = put
        self._get = lambda: tuple(decode(responses.get()))
        self._thread = threading.Thread(
            target=self._run_subinterpreter,
            args=(requests, responses),
            name="rsh-subinterpreter-engine",
            daemon=False,
        )
        self._thread.start()

    def _run_subinterpreter(self, requests: Any, responses: Any) -> None:
        # closed by this thread, which interpreter shutdown waits for, not by
        # the watcher, which it does not
        try:
            self._interpreter.call(_subinterpreter_main, requests, responses)
        finally:
            self._interpreter.close()

    @property
    def thread_id(self) -> int | None:
        return self._thread.ident

    def _dispatch(self) -> None:
        while True:
            request_id, status, payload = self._get()
            with self._lock:
                pending = self._pending.pop(request_id, None)
            if pending is not None:
                pending.resolve(status, payload)

    def submit(self, op: str, *args: Any) -> _Pending:
        pending = _Pending(next(self._ids))
        with self._lock:
            self._pending[pending.request_id] = pending
        try:
            self._put((pending.request_id, op, args))
        except TypeError as unsendable:
            with self._lock:
                del self._pending[pending.request_id]
            msg = (
                f"{unsendable}; values crossing into a subinterpreter engine "
                "host must be sendable"
            )
            raise StateError(msg) from None
        return pending

    def call(self, op: str, *args: Any) -> Any:
        return self.submit(op, *args).result()

    async def acall(self, op: str, *args: Any) -> Any:
        pending = self.submit(op, *args)
        try:
            return await anyio.to_thread.run_sync(
                pending.result, abandon_on_cancel=True
            )
        except anyio.get_cancelled_exc_class():
            self._put((None, "cancel", (pending.request_id,)))
            raise

    def export(self, value: object) -> bytes:
        """A value for the host: each of this host's channels in it becomes
        its handle."""
        return encode(value, self._hosted_handle)

    def _hosted_handle(self, value: object) -> tuple[int, int]:
        if isinstance(value, HostedChannel):
            if value._engine is not self:
                msg = f"{value!r} belongs to another engine host"
                raise StateError(msg)
            return CHANNEL_HANDLE, value._handle
        msg = f"cannot send {type(value).__qualname__} values: {value!r}"
        raise TypeError(msg)

    def import_value(self, data: bytes, wrap: Callable[[Host, int], Any]) -> Any:
        """A value from the host, each channel in it wrapped by ``wrap``."""

        def channel(code: int, handle: Any) -> Any:
            if code != CHANNEL_HANDLE or type(handle) is not int:
                msg = f"unknown extension {code} from the engine host"
                raise DecodeError(msg)
            return wrap(self, handle)

        return decode(data, channel)

    def place(self, place: Place) -> Place | dict[str, Any]:
        """A place as it crosses into this host."""
        if self.kind == "thread":
            return place
        try:
            return place.to_value()
        except TypeError as error:
            msg = f"{error}: it cannot cross into a subinterpreter engine host"
            raise StateError(msg) from None

    def shutdown(self) -> None:
        """Close the groups still open, as cancelled, and stop the host."""
        if not self._thread.is_alive():
            return
        self._put(None)
        self._thread.join()


# -- the async facade ---------------------------------------------------------


class AsyncHostedGroup:
    """`rsh.open_group()` under `rsh.use_engine`: the async API, run in an
    engine host."""

    def __init__(self, engine: Engine, *, shutdown: Shutdown | None = None) -> None:
        self.engine = engine.host()
        self.shutdown = shutdown or Shutdown()
        self._handle: int | None = None

    async def __aenter__(self) -> Self:
        self._handle = await self.engine.acall("open_group", self.shutdown.to_value())
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        with anyio.CancelScope(shield=True):
            await self.engine.acall("exit", self._handle)

    def spawn(
        self,
        place: Place,
        *,
        services: dict[str, bool] | None = None,
        teardown: Teardown | None = None,
    ) -> _AsyncHostedScope:
        targets = None if teardown is None else teardown.to_value()

        async def enter() -> tuple[int, AsyncHostedGateway]:
            handle, info, offered = await self.engine.acall(
                "spawn", self._handle, self.engine.place(place), services, targets
            )
            return handle, AsyncHostedGateway(self.engine, handle, info, offered)

        return _AsyncHostedScope(self.engine, enter)


class AsyncHostedGateway:
    def __init__(
        self, engine: Host, handle: int, info: dict[str, Any], services: frozenset[str]
    ) -> None:
        self._engine = engine
        self._handle = handle
        self.worker = WorkerInfo(**info)
        self.services = services

    def open(self, target: Any, /, **params: Any) -> _AsyncHostedScope:
        service = target if isinstance(target, str) else target.service

        async def enter() -> tuple[int, Any]:
            handle = await self._engine.acall("open", self._handle, service, params)
            channel = AsyncHostedChannel(self._engine, handle)
            return handle, channel if isinstance(target, str) else target(channel)

        return _AsyncHostedScope(self._engine, enter)


class _AsyncHostedScope:
    def __init__(self, engine: Host, enter: Callable[[], Any]) -> None:
        self._engine = engine
        self._enter = enter
        self._handle: int | None = None

    async def __aenter__(self) -> Any:
        self._handle, value = await self._enter()
        return value

    async def __aexit__(self, *exc_info: object) -> None:
        with anyio.CancelScope(shield=True):
            await self._engine.acall("exit", self._handle)


class HostedChannel:
    """A channel kept in an engine host, reached by its handle."""

    def __init__(self, engine: Host, handle: int) -> None:
        self._engine = engine
        self._handle = handle

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._handle} in a {self._engine.kind} host>"


class AsyncHostedChannel(HostedChannel):
    async def send(self, value: object) -> None:
        await self._engine.acall("send", self._handle, self._engine.export(value))

    async def receive(self) -> Any:
        data = await self._engine.acall("receive", self._handle, None)
        return self._engine.import_value(data, AsyncHostedChannel)

    # sync in the async API too: queued behind everything sent before, and not
    # waited for, so the caller's loop never blocks on the host
    async def drain(self) -> None:
        await self._engine.acall("drain", self._handle, None)

    def stop(self, deadline: float | None = None) -> None:
        self._engine.submit("stop", self._handle, deadline)

    def close_send(self) -> None:
        self._engine.submit("close_send", self._handle)

    def close_receive(self) -> None:
        self._engine.submit("close_receive", self._handle)

    async def wait_closed(self) -> Any:
        data = await self._engine.acall("wait_closed", self._handle, None)
        return self._engine.import_value(data, AsyncHostedChannel)

    def close(self) -> None:
        """Close a channel that arrived as a value; one a block opened closes
        with its block."""
        self._engine.submit("close", self._handle)

    async def aclose(self) -> None:
        self.close()
        await anyio.lowlevel.checkpoint()

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.receive()
        except _errors.ChannelClosed:
            raise StopAsyncIteration from None
