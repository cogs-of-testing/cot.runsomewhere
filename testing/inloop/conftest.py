from contextlib import asynccontextmanager

import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht


@asynccontextmanager
async def inloop(*, enable=None, **place_options):
    """A gateway to an in-loop worker, inside a group of its own."""
    async with rsh.open_group() as group:
        async with group.spawn(
            rsht.InLoop(**place_options), services=enable or {}
        ) as gateway:
            yield gateway


@pytest.fixture
async def gateway():
    async with inloop() as gateway:
        yield gateway
