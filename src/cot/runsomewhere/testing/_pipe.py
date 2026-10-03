from __future__ import annotations

import math

import anyio
import anyio.abc
from anyio.lowlevel import checkpoint


class _Direction:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.eof = False
        self.changed: anyio.Event | None = None

    def wake(self) -> None:
        if self.changed is not None:
            self.changed.set()


class Pipe:
    """The in-memory byte pipe between an in-loop worker and its caller.

    It carries the full protocol, and is where a test injects faults.
    """

    def __init__(self, *, max_chunk: int | None = None) -> None:
        self.max_chunk = max_chunk
        self._to_worker = _Direction()
        self._to_caller = _Direction()
        self._held = False
        self._cut = False
        self.caller_end = _End(self, incoming=self._to_caller, outgoing=self._to_worker)
        self.worker_end = _End(self, incoming=self._to_worker, outgoing=self._to_caller)

    def hold(self) -> None:
        """Stop delivering bytes, in both directions."""
        self._held = True

    def release(self) -> None:
        """Resume delivering bytes."""
        self._held = False
        self._wake()

    def cut(self) -> None:
        """End the stream on both sides, as a dead worker or dropped link would."""
        self._cut = True
        self._wake()

    def inject(self, data: bytes, *, to: str) -> None:
        """Deliver raw bytes to one side as if the other side had written them."""
        direction = {"caller": self._to_caller, "worker": self._to_worker}[to]
        direction.buffer += data
        direction.wake()

    def _wake(self) -> None:
        self._to_worker.wake()
        self._to_caller.wake()


class _End(anyio.abc.ByteStream):
    def __init__(self, pipe: Pipe, *, incoming: _Direction, outgoing: _Direction):
        self._pipe = pipe
        self._incoming = incoming
        self._outgoing = outgoing

    async def send(self, item: bytes) -> None:
        await checkpoint()
        if self._pipe._cut or self._outgoing.eof:
            msg = "the pipe is cut"
            raise anyio.BrokenResourceError(msg)
        self._outgoing.buffer += item
        self._outgoing.wake()

    async def receive(self, max_bytes: int = 65536) -> bytes:
        incoming = self._incoming
        while True:
            if self._pipe._cut:
                raise anyio.EndOfStream
            if incoming.buffer and not self._pipe._held:
                size = min(
                    len(incoming.buffer), max_bytes, self._pipe.max_chunk or math.inf
                )
                chunk = bytes(incoming.buffer[: int(size)])
                del incoming.buffer[: int(size)]
                await checkpoint()
                return chunk
            if incoming.eof and not incoming.buffer:
                raise anyio.EndOfStream
            incoming.changed = anyio.Event()
            await incoming.changed.wait()

    async def send_eof(self) -> None:
        self._outgoing.eof = True
        self._outgoing.wake()

    async def aclose(self) -> None:
        self._outgoing.eof = True
        self._outgoing.wake()
        await checkpoint()
