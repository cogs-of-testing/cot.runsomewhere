import anyio
import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht
from cot.runsomewhere._frames import Frame, FrameType, encode_frame
from cot.runsomewhere._values import encode
from cot.runsomewhere.testing import open_inloop

pytestmark = pytest.mark.anyio


async def test_items_arrive_in_order_over_a_byte_at_a_time_stream():
    items = [{"index": index, "payload": b"x" * index} for index in range(200)]
    async with open_inloop(pipe=rsht.Pipe(max_chunk=1)) as gateway:
        async with gateway.open("rsh_test_services.echo") as channel:
            async with anyio.create_task_group() as tg:

                async def send_all():
                    for item in items:
                        await channel.send(item)

                tg.start_soon(send_all)
                assert [await channel.receive() for _ in items] == items


async def test_return_value_of_the_handler_is_the_close_value(gateway):
    async with gateway.open("rsh_test_services.total") as channel:
        for number in [1, 2, 3, None]:
            await channel.send(number)
        assert await channel.wait_closed() == 6


async def test_iteration_ends_when_the_other_side_closes(gateway):
    async with gateway.open("rsh_test_services.produce", count=3, size=1) as channel:
        assert [item async for item in channel] == [b"\x00", b"\x01", b"\x02"]
        assert await channel.wait_closed() == 3


async def test_a_handler_exception_arrives_as_remote_error_with_traceback(gateway):
    async with gateway.open("rsh_test_services.fail", message="boom") as channel:
        with pytest.raises(rsh.RemoteError) as excinfo:
            await channel.wait_closed()
    assert "boom" in str(excinfo.value)
    assert "ValueError" in excinfo.value.remote_traceback
    assert "in fail" in excinfo.value.remote_traceback


async def test_receive_after_the_last_item_raises_channel_closed(gateway):
    async with gateway.open("rsh_test_services.produce", count=1, size=1) as channel:
        assert await channel.receive() == b"\x00"
        with pytest.raises(rsh.ChannelClosed):
            await channel.receive()


async def test_send_to_a_closed_channel_raises_channel_closed(gateway):
    async with gateway.open("rsh_test_services.produce", count=0, size=1) as channel:
        await channel.wait_closed()
        with pytest.raises(rsh.ChannelClosed):
            await channel.send(1)


async def test_unsendable_values_are_refused_at_the_caller(gateway):
    async with gateway.open("rsh_test_services.echo") as channel:
        with pytest.raises(TypeError):
            await channel.send(object())
        await channel.send("the channel is still usable")
        assert await channel.receive() == "the channel is still usable"


async def test_a_channel_sent_over_a_channel_is_usable_on_the_other_side():
    async def split(channel):
        side = channel.new()
        await channel.send(side)
        async for item in side:
            await side.send(item * 2)

    async with open_inloop(services={"t.split": split}) as gateway:
        async with gateway.open("t.split") as channel:
            side = await channel.receive()
            await side.send(21)
            assert await side.receive() == 42


async def test_a_channel_cannot_travel_over_another_gateway():
    async with open_inloop() as first, open_inloop() as second:
        async with first.open("rsh_test_services.echo") as channel:
            async with second.open("rsh_test_services.echo") as other:
                with pytest.raises(rsh.StateError):
                    await other.send(channel)


async def test_leaving_the_open_block_closes_the_channel_for_the_service():
    closed = anyio.Event()

    async def watch(channel):
        async for _ in channel:
            pass
        closed.set()

    async with open_inloop(services={"t.watch": watch}) as gateway:
        async with gateway.open("t.watch"):
            pass
        await closed.wait()


async def test_open_is_only_a_context_manager(gateway):
    with pytest.raises(TypeError):
        await gateway.open("rsh_test_services.echo")


def _half_close_from_caller(pipe, channel, ends):
    # no public half-close yet: the frame a caller would send, put on the wire
    frame = Frame(FrameType.CLOSE, channel.id, encode({"ends": ends}))
    pipe.inject(encode_frame(frame), to="worker")


async def test_the_result_still_arrives_after_the_caller_ends_its_sending():
    async def add(channel):
        return sum([item async for item in channel])

    pipe = rsht.Pipe()
    async with open_inloop(pipe=pipe, services={"t.add": add}) as gateway:
        async with gateway.open("t.add") as channel:
            for number in [1, 2, 3]:
                await channel.send(number)
            await anyio.wait_all_tasks_blocked()
            _half_close_from_caller(pipe, channel, "send")
            with anyio.fail_after(1):
                assert await channel.wait_closed() == 6


async def test_the_caller_ending_its_sending_does_not_cancel_a_busy_handler():
    proceed = anyio.Event()

    async def busy(channel):
        await proceed.wait()
        return "finished"

    pipe = rsht.Pipe()
    async with open_inloop(pipe=pipe, services={"t.busy": busy}) as gateway:
        async with gateway.open("t.busy") as channel:
            await anyio.wait_all_tasks_blocked()
            _half_close_from_caller(pipe, channel, "send")
            await anyio.wait_all_tasks_blocked()
            proceed.set()
            with anyio.fail_after(1):
                assert await channel.wait_closed() == "finished"


async def test_the_caller_ending_its_receiving_fails_the_handlers_sends():
    async def talk(channel):
        await channel.receive()
        try:
            await channel.send("unwanted")
        except rsh.ChannelClosed:
            return "refused"
        return "sent"

    pipe = rsht.Pipe()
    async with open_inloop(pipe=pipe, services={"t.talk": talk}) as gateway:
        async with gateway.open("t.talk") as channel:
            await anyio.wait_all_tasks_blocked()
            _half_close_from_caller(pipe, channel, "receive")
            await channel.send("go")
            with anyio.fail_after(1):
                assert await channel.wait_closed() == "refused"
