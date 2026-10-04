from __future__ import annotations

from typing import TYPE_CHECKING

from ._engine import AsyncHostedGroup, selected_engine
from ._gateway import Group

if TYPE_CHECKING:
    from ._shutdown import Shutdown


def open_group(*, shutdown: Shutdown | None = None) -> Group | AsyncHostedGroup:
    """The scope gateways are spawned in, torn down by ``shutdown``.

    The protocol runs in the caller's own loop, or in the engine host set
    with ``rsh.use_engine``.
    """
    engine = selected_engine()
    if engine is None:
        return Group(shutdown=shutdown)
    return AsyncHostedGroup(engine, shutdown=shutdown)
