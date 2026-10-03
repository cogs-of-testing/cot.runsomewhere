"""Read execnet spec strings into places.

``xspec.parse("ssh=buildbox//python=3.13")`` is ``rsh.Ssh("buildbox",
python="3.13")``. For command lines and configuration files written for
execnet; places themselves are values, and ``rsh`` has no string form of one.

Only keys with an exact equivalent are read: ``popen``, ``ssh=<host>``,
``python=<interpreter>`` and, for ``popen``, ``env:<NAME>=<value>``. Any other
key is refused rather than approximated. ``python=`` is read the way
runsomewhere reads it: a version request or an interpreter path.
"""

from __future__ import annotations

from .._places import Place, Process, Ssh

__all__ = ["parse"]


def parse(spec: str) -> Place:
    """The place an execnet spec string names; ``ValueError`` if it uses
    anything runsomewhere has no equivalent for."""
    transport: str | None = None
    host: str | None = None
    python: str | None = None
    env: dict[str, str] = {}
    seen: set[str] = set()
    for part in spec.split("//"):
        key, has_value, value = part.partition("=")
        if key in seen:
            msg = f"{key!r} appears twice in {spec!r}"
            raise ValueError(msg)
        seen.add(key)
        if key.startswith("env:") and has_value:
            env[key[len("env:") :]] = value
        elif key == "popen" and not has_value:
            transport = _one_transport(transport, key, spec)
        elif key == "ssh" and has_value:
            transport = _one_transport(transport, key, spec)
            if value.split() != [value]:
                msg = (
                    f"ssh options in {spec!r} are not read; give them to "
                    "rsh.Ssh or the ssh config"
                )
                raise ValueError(msg)
            host = value
        elif key == "python" and has_value:
            python = value
        else:
            msg = f"the execnet spec key {key!r} has no runsomewhere equivalent"
            raise ValueError(msg)
    if transport is None:
        msg = f"{spec!r} names no place: it needs popen or ssh=<host>"
        raise ValueError(msg)
    if transport == "ssh":
        assert host is not None
        if env:
            msg = f"env: in {spec!r} is only read for popen"
            raise ValueError(msg)
        return Ssh(host, python=python)
    return Process(python=python, env=env)


def _one_transport(current: str | None, key: str, spec: str) -> str:
    if current is not None:
        msg = f"{spec!r} names two places: {current} and {key}"
        raise ValueError(msg)
    return key
