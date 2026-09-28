from __future__ import annotations

import os
import platform
import re
import sys
import sysconfig
from dataclasses import dataclass, field
from typing import Any

from ._errors import HandshakeRefused
from ._frames import PROTOCOL_VERSION


@dataclass(frozen=True)
class Hello:
    """What each side announces about itself before anything else."""

    protocol: int
    version: str
    python: str
    platform: str
    pid: int
    services: frozenset[str] = frozenset()
    executable: str = field(default="", compare=False)

    @classmethod
    def local(cls, version: str, services: frozenset[str] = frozenset()) -> Hello:
        return cls(
            protocol=PROTOCOL_VERSION,
            version=version,
            python=platform.python_version(),
            platform=sysconfig.get_platform(),
            pid=os.getpid(),
            services=services,
            executable=sys.executable,
        )

    def to_value(self) -> dict[str, Any]:
        return {
            "protocol": self.protocol,
            "version": self.version,
            "python": self.python,
            "platform": self.platform,
            "pid": self.pid,
            "services": frozenset(self.services),
            "executable": self.executable,
        }

    @classmethod
    def from_value(cls, value: dict[str, Any]) -> Hello:
        return cls(**value)


def _major_minor(version: str) -> tuple[str, ...]:
    return tuple(re.findall(r"\d+", version)[:2])


def check_peer(*, local: Hello, remote: Hello) -> None:
    """Refuse a peer whose protocol or major.minor version differs."""
    if remote.protocol != local.protocol:
        raise HandshakeRefused(
            f"the other side speaks protocol {remote.protocol}, "
            f"this side speaks protocol {local.protocol}"
        )
    if _major_minor(remote.version) != _major_minor(local.version):
        raise HandshakeRefused(
            f"the other side runs runsomewhere {remote.version}, this side "
            f"{local.version}; major and minor versions must match"
        )
