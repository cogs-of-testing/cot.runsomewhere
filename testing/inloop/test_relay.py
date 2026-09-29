import re

import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht

from .conftest import inloop

pytestmark = pytest.mark.anyio


async def test_a_worker_spawned_through_a_worker_serves_like_any_other(gateway):
    async with gateway.spawn(rsht.InLoop()) as leaf:
        async with leaf.open("rsh_test_services.echo") as channel:
            await channel.send("through the relay")
            assert await channel.receive() == "through the relay"


async def test_the_leaf_is_a_different_worker_from_its_relay(gateway):
    async with gateway.spawn(rsht.InLoop()) as leaf:
        assert leaf is not gateway
        assert leaf.worker == leaf.worker
        assert leaf.services == gateway.services


async def test_the_handshake_is_end_to_end():
    # the relay is compatible; only the leaf is skewed, and the caller sees it
    async with inloop() as relay:
        with pytest.raises(rsh.HandshakeRefused, match=re.escape("99.0.0")):
            async with relay.spawn(rsht.InLoop(worker_version="99.0.0")):
                pytest.fail("a skewed leaf must not yield a gateway")


async def test_losing_the_relay_fails_the_leaf_with_worker_gone():
    pipe = rsht.Pipe()
    async with inloop(pipe=pipe) as relay:
        async with relay.spawn(rsht.InLoop()) as leaf:
            async with leaf.open("rsh_test_services.echo") as channel:
                pipe.cut()
                with pytest.raises(rsh.WorkerGone):
                    await channel.receive()


async def test_leaving_the_relay_block_requires_leaving_the_leaf_block_first():
    async with inloop() as relay:
        async with relay.spawn(rsht.InLoop()) as leaf:
            pass
        with pytest.raises(rsh.StateError):
            async with leaf.open("rsh_test_services.echo"):
                pass


async def test_chains_compose_one_block_per_hop(gateway):
    async with gateway.spawn(rsht.InLoop()) as middle:
        async with middle.spawn(rsht.InLoop()) as leaf:
            async with leaf.open("rsh_test_services.echo") as channel:
                await channel.send(2)
                assert await channel.receive() == 2


async def test_a_worker_without_via_cannot_relay():
    async with inloop(enable={"rsh.via": False}) as gateway:
        with pytest.raises(rsh.StateError, match=re.escape("rsh.via")):
            async with gateway.spawn(rsht.InLoop()):
                pass
