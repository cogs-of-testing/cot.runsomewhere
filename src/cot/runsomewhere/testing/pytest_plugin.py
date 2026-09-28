"""pytest plugin, registered by entry point: the ``rsh_group`` fixture."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from .. import _gateway


@pytest.fixture
async def rsh_group() -> AsyncIterator[_gateway.Group]:
    """An open group in the test's event loop, closed at teardown."""
    async with _gateway.open_group() as group:
        yield group
