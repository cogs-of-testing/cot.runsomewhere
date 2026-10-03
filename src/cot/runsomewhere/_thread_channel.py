from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio
import anyio.from_thread

from ._errors import ChannelClosed

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ._channels import Channel


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

        async def receive() -> Any:
            with anyio.fail_after(timeout):
                return await self.async_channel.receive()

        return anyio.from_thread.run(receive)

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

        return anyio.from_thread.run(wait)

    def new(self) -> ThreadChannel:
        return ThreadChannel(self.async_channel.new())

    def close_send(self) -> None:
        anyio.from_thread.run_sync(self.async_channel.close_send)

    def close_receive(self) -> None:
        anyio.from_thread.run_sync(self.async_channel.close_receive)

    def close(self) -> None:
        anyio.from_thread.run_sync(self.async_channel.close)
