"""A byte stream carried over a channel: how via tunnels a gateway."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import anyio.abc

from ._errors import ChannelClosed, RemoteError

if TYPE_CHECKING:
    from ._channels import Channel
    from ._places import Place

#: how long a relay waits for its leaf to exit before the place forces it
RELAY_CLOSE_TIMEOUT = 5.0


class ChannelByteStream(anyio.abc.ByteStream):
    """Bytes items on a channel, seen as a byte stream."""

    def __init__(self, channel: Channel) -> None:
        self.channel = channel

    async def send(self, item: bytes) -> None:
        try:
            await self.channel.send(item)
        except ChannelClosed:
            raise anyio.ClosedResourceError from None

    async def receive(self, max_bytes: int = 65536) -> bytes:  # noqa: ARG002 - one item is one chunk
        try:
            data: bytes = await self.channel.receive()
        except ChannelClosed:
            raise anyio.EndOfStream from None
        except RemoteError as error:
            # the relay failed, e.g. to launch the place: its reason is the
            # only account of why this stream ended
            msg = f"the relay failed: {error}"
            raise anyio.BrokenResourceError(msg) from error
        return data

    async def send_eof(self) -> None:
        self.channel.close()

    async def aclose(self) -> None:
        self.channel.close()


async def relay(channel: Channel, place: Place) -> None:
    """The worker side of rsh.via: launch the place, move bytes both ways."""
    async with anyio.create_task_group() as tg:
        launched = await place.launch(tg)
        stream = launched.stream
        finished = anyio.Event()

        async def upstream() -> None:
            try:
                while True:
                    await channel.send(await stream.receive())
            except (anyio.EndOfStream, anyio.ClosedResourceError, OSError):
                pass
            finished.set()

        async def downstream() -> None:
            try:
                async for data in channel:
                    await stream.send(data)
                await stream.send_eof()
            except (anyio.ClosedResourceError, anyio.BrokenResourceError, OSError):
                pass
            finished.set()

        tg.start_soon(upstream)
        tg.start_soon(downstream)
        try:
            await finished.wait()
        finally:
            with anyio.CancelScope(shield=True):
                # the leaf waits for its stream to end before it exits
                await stream.aclose()
                exited = False
                with anyio.move_on_after(RELAY_CLOSE_TIMEOUT):
                    await launched.exited()
                    exited = True
                if not exited:
                    await launched.force()
            tg.cancel_scope.cancel()
