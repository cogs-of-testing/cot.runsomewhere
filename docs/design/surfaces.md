# API surfaces and engine hosts

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review; anything
marked **to verify** is an assumption not yet checked.

## The core is async

runsomewhere's core API is async. It is written once, against anyio, and runs
in the caller's own trio or asyncio loop:

```python
async with rsh.open_group() as group:
    async with group.spawn(rsh.Process()) as gateway:
        async with gateway.open(Agent) as agent:
            print(await agent.status())
```

Here the protocol's IO (reading frames, writing frames, granting credits,
relaying tunnels) runs as tasks in the caller's loop, under the group's scope.
Cancellation is the caller's own scope; nothing else is needed.

Everything else is a **facade** over that core, for callers whose code is not
a trio or asyncio program, or who do not want the protocol sharing their loop.

## Engine hosts

A facade runs the async core in an **engine host**, away from the caller, and
carries calls and messages between the two:

```text
main interpreter                        engine host
┌────────────────────────┐              ┌───────────────────────────────┐
│ caller code            │   requests   │ event loop                    │
│   sync facade ─────────┼─────────────►│   async core: groups,         │
│   or async facade ◄────┼──────────────┤   gateways, channels, IO ─────┼──► workers
│ values decoded here    │   frames     │                               │
└────────────────────────┘              └───────────────────────────────┘
```

Two hosts:

| Host | Runs the core in | Messages cross as | Available |
|---|---|---|---|
| thread | an event loop on a dedicated OS thread | objects, through thread-safe queues | always |
| subinterpreter | an event loop in its own interpreter, on its own thread | frames as `bytes`, through interpreter queues | Python 3.14+ (`concurrent.interpreters`) |

In a subinterpreter host, the engine does the IO, framing, flow control and
relaying; the payloads of channel frames go to the main interpreter still
encoded, and are decoded there, by the facade, into the caller's objects. No
Python object is shared between interpreters, and the handoff is `bytes`,
which interpreter queues carry without copying through pickle.

**Why a subinterpreter.** A subinterpreter has its own GIL. The engine keeps
reading, granting credits and relaying for other gateways while the main
interpreter is busy with CPU-bound work or a long C call, so one slow caller
cannot stall a worker's tunnel, or every worker behind a relay. A thread host
gives the same structure without that isolation.

Verified on Python 3.14: anyio's asyncio backend runs in a
`concurrent.interpreters` subinterpreter, with threads and sockets, and the
host runs in-loop workers with entry-point services there. **To verify:** trio
in a subinterpreter, and spawning worker processes from one.

**Proposed:** the thread host is the default; the subinterpreter host is
opt-in until it has been measured, and chosen when a group is opened:

```python
with rsh.sync.open_group(engine=rsh.SubinterpreterEngine()) as group:
    ...
```

`rsh.ThreadEngine()` and `rsh.SubinterpreterEngine()` select a kind of host.
There is one host of each kind per process, started on first use and shared
by every group that selects it.

## The sync facade

The external API for code that is not async:

```python
with rsh.sync.open_group() as group:
    with group.spawn(rsh.Ssh("nas")) as gateway:
        with gateway.open(Agent) as agent:
            print(agent.status(timeout=10))
```

It has the same shape as the core, without `await`. Each call is sent to the
engine host and the calling thread parks until the answer comes back. Because
there is no cancellation scope, sync calls take `timeout=` and raise the
builtin `TimeoutError`.

A sync facade needs an engine host: the core has to run on some loop, and the
caller's thread is not one. Client classes are written once, async, and the
facade wraps them, running each method in the host: `gateway.open(Agent)` on
the sync facade yields the sync wrapper.

Calling the sync facade from inside a running event loop would stall that
loop, so it refuses, with a `StateError` that names the async API instead.
That check belongs to how the facade waits, not to global state: parking the
caller goes through one internal wait primitive, OS-thread based at first.
A greenlet-based wait would be a second implementation of that primitive, and
is the only place greenlet support would touch.

## The async facade

An async caller that does not want the protocol in its own loop uses the same
core API through the engine host:

```python
async with rsh.open_group(engine=rsh.SubinterpreterEngine()) as group:
    ...
```

The API is identical; calls cross to the host and back, and the caller's
cancellation is carried across: a cancelled call cancels its counterpart in
the host, and a receive cancelled after its item arrived hands that item to
the next receive instead of dropping it.

## Where it leaves workers

A worker is the same program on the other side: it runs the async core on its
own loop, and runs [services](services.md) as tasks (async handlers) or on
threads (sync handlers). Which facade the caller uses is invisible to the
worker; the wire is the same.
