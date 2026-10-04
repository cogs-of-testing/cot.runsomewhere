"""``python -m cot.runsomewhere``: the launch contract for workers."""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import socket
import sys
import sysconfig

import anyio
from anyio.abc import SocketStream

from ._version import version
from ._worker import WorkerCore


async def _serve_fd(fd: int) -> None:
    # handed over as a socket, not a bare fd, so family and type are kept
    stream = await SocketStream.from_socket(socket.socket(fileno=fd))
    worker = WorkerCore(stream)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(_close_on_sigterm, worker)
        await worker.run()
        tasks.cancel_scope.cancel()


async def _close_on_sigterm(worker: WorkerCore) -> None:
    # a terminated relay still closes what runs behind it, with no time to
    # drain, instead of leaving it orphaned
    with anyio.open_signal_receiver(signal.SIGTERM) as signals:
        async for _ in signals:
            worker.request_close(0.0)


def worker(arguments: argparse.Namespace) -> None:
    anyio.run(_serve_fd, arguments.fd)
    sys.stdout.flush()
    sys.stderr.flush()
    # service threads that ignored their closed channel must not keep the
    # process alive once the gateway is closed
    os._exit(0)


def info(_arguments: argparse.Namespace) -> None:
    sys.stdout.write(
        json.dumps(
            {
                "runsomewhere": version,
                "python": platform.python_version(),
                "executable": sys.executable,
                "platform": sysconfig.get_platform(),
            }
        )
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="runsomewhere")
    commands = parser.add_subparsers(required=True)
    worker_parser = commands.add_parser("worker", help="serve one gateway")
    worker_parser.add_argument(
        "--fd", type=int, required=True, help="inherited socket carrying the protocol"
    )
    worker_parser.set_defaults(run=worker)
    commands.add_parser("info", help="describe this install").set_defaults(run=info)
    arguments = parser.parse_args(argv)
    arguments.run(arguments)
