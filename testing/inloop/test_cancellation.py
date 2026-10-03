import anyio
import pytest

from cot.runsomewhere import testing as rsht
from cot.runsomewhere.testing import open_inloop

pytestmark = pytest.mark.anyio

COUNT = 200


async def test_cancelled_receives_lose_no_items():
    async with open_inloop(pipe=rsht.Pipe(max_chunk=7)) as gateway:
        async with gateway.open(
            "rsh_test_services.produce", count=COUNT, size=1
        ) as channel:
            received = []
            attempt = 0
            while len(received) < COUNT:
                attempt += 1
                # every other receive is cancelled at its first checkpoint,
                # which may be after its item already arrived
                with anyio.move_on_after(0 if attempt % 2 else None):
                    received.append(await channel.receive())
            assert received == [bytes([index % 256]) for index in range(COUNT)]


async def test_a_cancelled_send_is_all_or_nothing():
    async with open_inloop(pipe=rsht.Pipe(max_chunk=3)) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            for index in range(20):
                with anyio.move_on_after(0):
                    await channel.send({"index": index, "pad": b"." * 100})
            await channel.send("end")
            items = []
            while (item := await channel.receive()) != "end":
                items.append(item)
            indexes = [item["index"] for item in items]
            assert indexes == sorted(set(indexes))
            assert all(item["pad"] == b"." * 100 for item in items)


async def test_cancelling_the_caller_cancels_an_async_handler():
    cancelled = anyio.Event()

    async def forever(channel):
        try:
            await anyio.sleep_forever()
        except anyio.get_cancelled_exc_class():
            cancelled.set()
            raise

    async with open_inloop(services={"t.forever": forever}) as gateway:

        async def hold_open():
            async with gateway.open("t.forever"):
                await anyio.sleep_forever()

        async with anyio.create_task_group() as tg:
            tg.start_soon(hold_open)
            await anyio.wait_all_tasks_blocked()
            tg.cancel_scope.cancel()
        with anyio.fail_after(1):
            await cancelled.wait()
