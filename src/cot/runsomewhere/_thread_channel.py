from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio
import anyio.from_thread

from ._channels import Channel
from ._errors import ChannelClosed

if TYPE_CHECKING:
    from collections.abc import Iterator


class ThreadChannel:
    """The sync API of a channel, for code running in a worker thread."""

    _rsh_channel = True

    def __init__(self, channel: Channel) -> None:
        self.async_channel = channel

    def _raise_if_detached(self) -> None:
        # a thread that outlived its connection must not call into a loop that
        # may be gone: that call would never return
        failure = self.async_channel._connection.failure
        if failure is not None:
            raise failure

    def send(self, value: object) -> None:
        self._raise_if_detached()
        anyio.from_thread.run(self.async_channel.send, value)

    def receive(self, timeout: float | None = None) -> Any:
        self._raise_if_detached()

        async def receive() -> tuple[Any, bool, list[Any] | None]:
            with anyio.fail_after(timeout):
                return await self.async_channel.receive_item()

        value, carries_channels, carriers = anyio.from_thread.run(receive)
        if not carries_channels:
            return value
        if carriers is None:
            return _for_thread(value)
        return _along(value, carriers)

    def __iter__(self) -> Iterator[Any]:
        try:
            while True:
                yield self.receive()
        except ChannelClosed:
            return

    def wait_closed(self, timeout: float | None = None) -> Any:
        self._raise_if_detached()

        async def wait() -> Any:
            with anyio.fail_after(timeout):
                return await self.async_channel.wait_closed()

        return _for_thread(anyio.from_thread.run(wait))

    def drain(self, timeout: float | None = None) -> None:
        self._raise_if_detached()

        async def drain() -> None:
            with anyio.fail_after(timeout):
                await self.async_channel.drain()

        anyio.from_thread.run(drain)

    def new(self) -> ThreadChannel:
        return ThreadChannel(self.async_channel.new())

    @property
    def stopping(self) -> bool:
        return self.async_channel.stopping

    def wait_stopping(self, timeout: float | None = None) -> bool:
        """Wait until the peer asks this side to finish; False on timeout."""
        self._raise_if_detached()

        async def wait() -> bool:
            with anyio.move_on_after(timeout):
                await self.async_channel.stop_requested()
                return True
            return False

        return anyio.from_thread.run(wait)

    def stop(self, deadline: float | None = None) -> None:
        anyio.from_thread.run_sync(self.async_channel.stop, deadline)

    def close_send(self) -> None:
        anyio.from_thread.run_sync(self.async_channel.close_send)

    def close_receive(self) -> None:
        anyio.from_thread.run_sync(self.async_channel.close_receive)

    def close(self) -> None:
        anyio.from_thread.run_sync(self.async_channel.close)


def _for_thread(value: Any) -> Any:
    """``value`` with each channel in it given the sync API."""
    if type(value) is Channel:
        return ThreadChannel(value)
    if type(value) is dict:
        return {_for_thread(key): _for_thread(item) for key, item in value.items()}
    if type(value) in (list, tuple, set, frozenset):
        return type(value)(map(_for_thread, value))
    return value


def _along(value: Any, carriers: list[Any]) -> Any:
    """``value`` with each channel in it given the sync API, visiting only
    the containers the decoder recorded as holding one, innermost first.

    Lists and dicts were just decoded and are changed in place; tuples and
    sets are rebuilt, and the container holding each, a carrier too, takes
    the new one.
    """
    rebuilt: dict[int, Any] = {}

    def new(item: Any) -> Any:
        if type(item) is Channel:
            return ThreadChannel(item)
        return rebuilt.get(id(item), item)

    for container in carriers:
        kind = type(container)
        if kind is list:
            for index, item in enumerate(container):
                container[index] = new(item)
        elif kind is dict:
            for key, item in list(container.items()):
                new_key = new(key)
                if new_key is not key:
                    del container[key]
                container[new_key] = new(item)
        else:
            rebuilt[id(container)] = kind(map(new, container))
    return new(value)
