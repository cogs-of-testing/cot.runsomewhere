"""``python -m cot.runsomewhere``: the launch contract for workers."""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import sys
import sysconfig

from ._version import version


async def _serve_fd(fd: int) -> None:
    from anyio.abc import SocketStream

    from ._worker import WorkerCore

    # handed over as a socket, not a bare fd, so family and type are kept
    stream = await SocketStream.from_socket(socket.socket(fileno=fd))
    await WorkerCore(stream).run()


def worker(arguments: argparse.Namespace) -> None:
    import anyio

    anyio.run(_serve_fd, arguments.fd)
    sys.stdout.flush()
    sys.stderr.flush()
    # service threads that ignored their closed channel must not keep the
    # process alive once the gateway is closed
    os._exit(0)


def info(arguments: argparse.Namespace) -> None:
    print(
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
