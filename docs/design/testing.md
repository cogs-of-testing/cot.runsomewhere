# Testing

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

runsomewhere is tested at five levels of isolation. The rule is to test a
behaviour at the cheapest level that can show it, and most behaviour can be
shown with nothing but async tasks in one event loop.

| Level | Runs                                                         | Transport                           | Directory          |
| ----- | ------------------------------------------------------------ | ----------------------------------- | ------------------ |
| 0     | sans-IO pieces: frame decoder, value codec, handshake checks | none; no event loop                 | `testing/unit/`    |
| 1     | caller and worker cores as tasks in the test's own loop      | in-memory pipe with fault injection | `testing/inloop/`  |
| 2     | engine hosts and the sync facade                             | as the host provides                | `testing/hosts/`   |
| 3     | real worker processes                                        | socketpair, stdio                   | `testing/process/` |
| 4     | ssh to an in-process server, containers                      | ssh, container stdio                | `testing/remote/`  |

Every async test runs twice, under asyncio and under trio, through anyio's
pytest plugin. A behaviour that holds on one loop and not the other is a bug.

## Level 1: a worker as tasks

The harness is public, in `cot.runsomewhere.testing`, so that users test their
own services and clients the same way this suite does.

```python
from cot.runsomewhere import testing as rsht


async def test_echo():
    async with rsh.open_group() as group:
        async with group.spawn(rsht.InLoop()) as gateway:
            async with gateway.open("rsh_test_services.echo") as channel:
                await channel.send(1)
                assert await channel.receive() == 1
```

`rsht.InLoop()` is a place whose worker is the ordinary worker core, run as
tasks in the caller's own loop instead of in another interpreter. The two
sides are joined by an in-memory byte pipe and speak the full protocol over it:
handshake, configuration, frames, credits. Nothing is short-circuited, so a
test at this level exercises the same code a process worker runs.

### Services at level 1

- Tests of the **service layer itself** pass handler objects in directly:
  `rsht.InLoop(services={"t.echo": echo})`. The worker then offers exactly
  those, plus the built-ins. This is a construction parameter of the test
  place only, not runtime registration; no production place accepts it.
  Handlers passed this way share the test's memory, which lets a test observe
  what a handler did without a protocol for it.
- **Everything else** uses entry points, from the test package
  `rsh_test_services` (`testing/rsh_test_services/`), installed in the
  development environment. The same services then exist at every level,
  process and remote included.

### The pipe and its faults

`rsht.Pipe` is the byte pipe between the two sides, and the place for fault
injection:

```python
pipe = rsht.Pipe(max_chunk=1)  # deliver one byte per read
async with group.spawn(rsht.InLoop(pipe=pipe)) as gateway:
    ...
    pipe.hold()  # stop delivering, in both directions
    pipe.release()
    pipe.cut()  # EOF on both ends, as a dead worker
```

- `max_chunk` splits every write into reads of at most that size, which is
  how framing gets tested against arbitrary chunking.
- `hold()` and `release()` stop and resume delivery, which is how races
  between a frame arriving and a cancel get pinned down.
- `cut()` ends the stream on both sides, as a crashed worker or a dropped
  connection would.
- `inject(data, to="caller" | "worker")` delivers raw bytes as if the other
  side had written them, for corrupt-stream tests.

### Faking the other side's version

`rsht.InLoop(worker_version="99.0.0")` makes the in-loop worker announce a
different runsomewhere version in its handshake, so skew handling is tested
without two installs.

### pytest plugin

**Proposed:** a pytest plugin, registered by entry point, arrives with the
analysis helpers that give it something to do, such as failing a test that
leaves channels, places or tasks open. A fixture that only opens and closes a
group adds nothing over `async with Group()`, so none ships until then.

## Level 0

The frame decoder, the value codec and the handshake checks take bytes and
return values or raise. Their tests are plain functions: every chunking of a
stream decodes to the same frames, every sendable value round-trips with its
exact type, nothing that is not sendable encodes, and nothing decodes into
executable behaviour.

## Level 2

The sync facade and the engine hosts are tested with sync test functions,
against `InLoop` workers (which then run in the host's loop, not the
test's) and against process workers. Each test runs once per host: thread,
and subinterpreter where the Python has one.

Handler objects passed to `InLoop(services=...)` live in the main
interpreter, so they only work with the thread host; a subinterpreter host
refuses them with `StateError`, and its tests use entry-point services.

## Levels 3 and 4

Real processes test what only a process can show: the launch contract,
configuration kept out of argv, the protocol surviving a `print`, a crashed
worker surfacing as `WorkerGone`, and a stuck worker being killed when its
gateway closes.

Level 4 needs infrastructure: an ssh server run in-process by the suite with
committed test keys, and podman where it is installed. Tests skip when their
infrastructure is missing and say which.

## What the suite does not do

- It does not mock the protocol. A level-1 test that needs a fake worker uses
  the real worker core with test services.
- It does not sleep to wait for things. Level-1 tests settle with
  `anyio.wait_all_tasks_blocked()`; other levels wait on events with a
  deadline.
