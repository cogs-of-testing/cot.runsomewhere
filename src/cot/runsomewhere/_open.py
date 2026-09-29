from __future__ import annotations

from ._engine import AsyncHostedGroup, SubinterpreterEngine, ThreadEngine
from ._gateway import Group


def open_group(
    *, engine: ThreadEngine | SubinterpreterEngine | None = None
) -> Group | AsyncHostedGroup:
    """The scope gateways are spawned in.

    With an engine, the protocol runs in that engine host rather than in the
    caller's loop.
    """
    if engine is None:
        return Group()
    return AsyncHostedGroup(engine)
