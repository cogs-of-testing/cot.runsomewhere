import pytest

from cot.runsomewhere import testing as rsht


@pytest.fixture
async def gateway():
    async with rsht.open_inloop() as gateway:
        yield gateway
