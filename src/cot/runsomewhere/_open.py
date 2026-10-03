from __future__ import annotations

from ._engine import AsyncHostedGroup, selected_engine
from ._gateway import Group


def open_group() -> Group | AsyncHostedGroup:
    """The scope gateways are spawned in.

    The protocol runs in the caller's own loop, or in the engine host set
    with ``rsh.use_engine``.
    """
    engine = selected_engine()
    if engine is None:
        return Group()
    return AsyncHostedGroup(engine)
