"""pytest plugin, registered by entry point: the ``rsh_group`` fixture."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from .._gateway import Group

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@pytest.fixture
async def rsh_group() -> AsyncIterator[Group]:
    """An open group in the test's event loop, closed at teardown."""
    async with Group() as group:
        yield group
