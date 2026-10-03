import threading

import anyio
import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere import testing as rsht
from rsh_test_services import Echo


def test_the_sync_facade_has_the_async_shape_without_await(engine):
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with group.spawn(rsht.InLoop()) as gateway:
                with gateway.open("rsh_test_services.echo") as channel:
                    channel.send("sync")
                    assert channel.receive() == "sync"


def test_clients_are_wrapped_for_the_sync_facade(engine):
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with group.spawn(rsht.InLoop()) as gateway:
                with gateway.open(Echo) as echo:
                    assert echo.roundtrip([1, 2]) == [1, 2]


def test_a_sync_receive_times_out_with_the_builtin_timeout_error(engine):
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with group.spawn(rsht.InLoop()) as gateway:
                with gateway.open("rsh_test_services.echo") as channel:
                    with pytest.raises(TimeoutError):
                        channel.receive(timeout=0.05)


def test_a_timed_out_receive_leaves_a_late_item_for_the_next(engine):
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with group.spawn(rsht.InLoop()) as gateway:
                with gateway.open("rsh_test_services.echo") as channel:
                    with pytest.raises(TimeoutError):
                        channel.receive(timeout=0)
                    channel.send("late")
                    assert channel.receive(timeout=5) == "late"


def test_the_sync_facade_refuses_to_run_inside_an_event_loop(engine):
    async def inside():
        with rsh.use_engine(engine):
            with pytest.raises(rsh.StateError, match="async"):
                with rsh.sync.open_group():
                    pass

    anyio.run(inside)


def test_the_protocol_does_not_run_on_the_callers_thread(engine):
    caller = threading.get_ident()
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            assert group.engine.thread_id != caller


def test_one_default_engine_serves_every_group_in_the_process():
    with rsh.sync.open_group() as first, rsh.sync.open_group() as second:
        assert first.engine is second.engine


def test_an_override_applies_only_inside_its_context(engine):
    with rsh.sync.open_group() as before:
        pass
    with rsh.use_engine(engine), rsh.sync.open_group() as inside:
        pass
    with rsh.sync.open_group() as after:
        pass
    assert before.engine is after.engine
    assert inside.engine is not before.engine


def test_ad_hoc_services_are_refused_by_a_subinterpreter_host(engine):
    if isinstance(engine, rsh.ThreadEngine):
        pytest.skip("handler objects are shareable with a thread host")

    def echo(channel):
        for item in channel:
            channel.send(item)

    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with pytest.raises(rsh.StateError, match="subinterpreter"):
                with group.spawn(rsht.InLoop(services={"t.echo": echo})):
                    pass


def test_ending_sending_through_the_sync_facade_leaves_the_result(engine):
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with group.spawn(rsht.InLoop()) as gateway:
                with gateway.open("rsh_test_services.add") as channel:
                    for number in [1, 2, 3]:
                        channel.send(number)
                    channel.close_send()
                    assert channel.wait_closed(timeout=5) == 6


def test_drain_through_the_sync_facade_carries_the_counts(engine):
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with group.spawn(rsht.InLoop()) as gateway:
                with gateway.open(
                    "rsh_test_services.take", count=1, delay=0.3
                ) as channel:
                    for number in range(3):
                        channel.send(number)
                    with pytest.raises(rsh.ItemsDiscarded) as excinfo:
                        channel.drain(timeout=5)
                    assert (excinfo.value.taken, excinfo.value.discarded) == (1, 2)


def test_stop_through_the_sync_facade(engine):
    with rsh.use_engine(engine):
        with rsh.sync.open_group() as group:
            with group.spawn(rsht.InLoop()) as gateway:
                with gateway.open("rsh_test_services.until_stopped") as channel:
                    channel.stop()
                    assert channel.receive(timeout=5) == "bye"
                    assert channel.wait_closed(timeout=5) == "stopped"
