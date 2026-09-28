# cot.runsomewhere: design

*Written by Claude Opus 5.5 via Claude Code for senior engineers deciding
whether runsomewhere fits their system; Ronny prompted it, it did the work,
Ronny read it.*

Status: design. Nothing is implemented yet. The API below is a sketch; names
may still move, the model will not.

```python
from cot import runsomewhere as rsh
```

## In one paragraph

runsomewhere runs parts of a software system somewhere else and connects them
to the rest. "Somewhere" is a thread, a subinterpreter, another process,
another machine over ssh, or a container. A part, a **component**, is code
your system already ships, with its own main loop; runsomewhere deploys the
code the component needs to where it will run when it is not already there,
starts it there, and gives both sides channels to talk over. Where a component
runs is one typed value, so moving it from a local process to a build box or
a container does not change the component or the code talking to it.

## The unit is a component

What runs elsewhere is a component: a part of the system designed to run on
its own, such as a test runner, a build agent, a plugin host or a host agent.
It has a main, an interface (the messages on its channels), and a lifetime,
failure and resources of its own.

Code never travels over the wire. A component's code arrives by deployment, as
installed packages at a version you chose, so both sides are checked, tested
and versioned like the rest of the system. The wire carries only data: plain
builtin values and channels.

## The model

**Place**: where a component runs. A typed value.

```python
rsh.Thread()
rsh.Subinterpreter()                             # Python 3.14+
rsh.Process(python="3.10")
rsh.Ssh("box", python="3.13")
rsh.Container("fedora:44", runtime="podman")     # fresh container
rsh.Container(name="db-1", runtime="podman")     # exec into a running one
```

**Environment**: what is installed where the component runs. Threads and
subinterpreters share the caller's. Everywhere else it is the caller's own
environment, one provisioned by uv from requirements, or a **Deployment**: your
project's lockfile, its wheel and extra file trees, sent diff-only on repeat.

**Worker**: a runsomewhere interpreter running in a place and environment. It
hosts one or more components.

**Component**: a part of your system, started in a worker by its main's import
path. It receives a channel and its configuration, runs as long as it needs,
and may end with a result.

```python
async def main(channel, *, root):
    async for job in channel:
        await channel.send(run(job, root))
    return {"ran": count}
```

**Channel**: an ordered, two-way stream of values. Channels can be sent over
channels, so a component can hand out further conversations, one per
subsystem or per client.

**Group**: the scope that owns workers and their components. Closing it stops
them in reverse order of creation, on success, error or cancellation alike.

```python
async with rsh.open_group() as group:
    worker = await group.spawn(rsh.Process())
    runner = await worker.start("mypkg.runner:main", root="/srv")
    await runner.channel.send({"job": 1})
    reply = await runner.channel.receive()
    ...
    await runner.channel.aclose()
    summary = await runner.wait()
```

## Use cases

### Test runner workers

The distributed-test shape: N runner components, each taking tests and
streaming reports back, with the controller feeding work as runners free up.

```python
async with rsh.open_group() as group:
    runners = [
        await (await group.spawn(rsh.Process())).start("mypkg.testing.runner:main")
        for _ in range(8)
    ]
    async with anyio.create_task_group() as tg:
        for runner in runners:
            tg.start_soon(drive, runner.channel, schedule)
```

A runner that crashes, leaks or segfaults takes its own worker with it. The
controller sees `WorkerGone` on that runner's channel and reschedules its
tests. The channel window stops a fast runner from filling the controller's
memory with reports.

### The same suite on a remote box, deployed

```python
async with rsh.open_group() as group:
    host = await group.spawn(rsh.Ssh("buildbox"))
    env = await host.deploy(rsh.Deployment(".", roots=["testing"]))
    runners = [
        await (await env.spawn(rsh.Process())).start("mypkg.testing.runner:main")
        for _ in range(16)
    ]
    remote_path = env.paths.translate("testing/test_x.py")
```

The box needs Python reachable by uv and nothing else. The deployment carries
the lockfile, the project's wheel and the test tree, and a second deploy sends
only what changed. All sixteen workers are relayed through one ssh connection.
The runner component and the controller code are the same as in the local
case.

### Across Pythons and distros

```python
places = [rsh.Process(python=v) for v in ["3.10", "3.12", "3.14"]] + [
    rsh.Container(image) for image in ["fedora:44", "debian:13", "alpine:3.22"]
]
async with rsh.open_group() as group:
    for place in places:
        worker = await group.spawn(place, deploy=rsh.Deployment("."))
        session = await worker.start("mypkg.testing.session:main")
        async for report in session.channel:
            show(place, report)
```

One component, one deployment, six places. uv provisions and caches the
interpreters. A container image with runsomewhere installed is used as is;
otherwise the cached wheel is mounted and run under uv inside the container.

### An agent on every host

A small, long-lived component on each machine of a fleet, answering queries and
applying changes. A script drives it, with no event loop in sight:

```python
with rsh.blocking.open_group() as group:
    agents = {
        name: group.spawn(rsh.Ssh(name)).start("fleet.agent:main")
        for name in ["nas", "router", "pi"]
    }
    for name, agent in agents.items():
        agent.channel.send({"op": "status"})
        print(name, agent.channel.receive(timeout=10))
```

The agent's messages are its interface; the controller parses dicts, not shell
output. The blocking surface is the async API without `await`, plus `timeout=`.

### A diagnostics component inside a running container

```python
async with rsh.open_group() as group:
    worker = await group.spawn(rsh.Container(name="app-1"))
    probe = await worker.start("app.diagnostics:main")
    await probe.channel.send({"dump": "connections"})
```

`Container(name=...)` execs into a container that is already running, without
restarting it or baking a debug port into the image.

### A plugin host, isolated

```python
async with rsh.open_group() as group:
    worker = await group.spawn(rsh.Subinterpreter())
    plugins = await worker.start("app.plugins.host:main", paths=plugin_paths)
```

The plugin subsystem gets its own modules and globals, and plugins that mutate
module state cannot reach the application's. A subinterpreter starts far
faster than a process, but needs Python 3.14 and extension modules that
support subinterpreters; without them the spawn fails with a clear error rather
than quietly using a thread. Switch the place to `rsh.Process()` when the
plugins may also crash. The isolation is for state, not hostile code.

## What you can rely on

- **Code never crosses the wire.** Only data does. Every component runs code
  installed in its own environment, at a version you deployed.
- **Same semantics in every place.** Threads get copies too, and a
  component's failure is `RemoteError` everywhere. A component that works in a
  thread behaves the same in a container.
- **Values:** `None`, `bool`, `int`, `float`, `complex`, `str`, `bytes`,
  `tuple`, `list`, `dict`, `set`, `frozenset`, and channels.
  `rsh.can_send(x)` checks without sending.
- **Nothing outlives its group.** No global state, no default group, no atexit
  hooks.
- **trio, asyncio or plain threads.** The async API detects the running loop.
  On async surfaces cancellation comes from your own scope, so there is no
  `timeout=`.
- **Cancellation loses nothing.** A cancelled receive leaves an item that
  already arrived for the next receive.
- **Bounded memory.** Each channel has a receiver-granted window; a sender
  waits when it is full.
- **Three kinds of error:** the other side failed (`RemoteError`, with the
  remote traceback); the connection is gone (`ChannelClosed`, `WorkerGone`,
  `HostNotFound`, all `OSError`); you used the API wrong (`StateError`).
  Blocking timeouts raise the builtin `TimeoutError`.
- **Skew fails first.** Mismatched runsomewhere versions refuse at handshake,
  before any component starts.
- **Secrets stay out of `ps`.** Worker configuration, including environment
  values, travels over the protocol, never in argv.

## What it costs, and what it is not

- **A copy per item, even on a thread.** That is the price of identical
  semantics. A thread fast path may later skip the encoding, still handing
  over copies.
- **Start cost follows isolation:** thread, subinterpreter, process, container,
  remote host, roughly in that order. Components are meant to be long-lived;
  if you would start one per request, you want a function call instead.
- **Not remote procedure calls.** There is no "run this function over there
  and return". If the far side is worth running elsewhere, it is worth being a
  component with an interface.
- **Not a security boundary.** Both sides are trusted.
- **Not an object proxy.** You cannot hold a reference to a remote object.
- **Not a scheduler or orchestrator.** You pick the place. There is no
  cluster, no placement, no restart policy.
- **Python 3.10 and newer,** on Linux, macOS and Windows.

## How it works

One **engine thread** per process runs all protocol IO. Your code, async or
blocking, hands work to it and waits; a slow caller never stalls the protocol.
Trio users who want the IO in their own nursery can open a group with
`inline=True`.

A **worker** is runsomewhere itself, installed in the target environment and
started as `python -m cot.runsomewhere worker`. Both sides speak one protocol
over a byte stream, whatever the place:

| Place | Stream |
|---|---|
| thread, subinterpreter | in-memory pair |
| process | socketpair (`socket.share()` on Windows) |
| ssh | dial-back socket where possible, else stdio |
| container | stdio via `podman run -i` / `podman exec -i` |

The stream is kept off stdio where the place allows, so stray prints in a
component cannot corrupt it. Frames are length-prefixed; the first exchange is
a versioned handshake, then the configuration frame. Deploying, spawning
through a worker and socket handoff are protocol operations, not code sent to
run.

## Status and open decisions

Build order, each step its own pull request: skeleton; core protocol with the
`Process` place; components and channels; the async, blocking and inline
surfaces; `Thread` and `Subinterpreter`; uv provisioning, `Ssh` and
`Deployment`; podman then docker; later kubernetes and a gevent profile.

Proposed, not yet settled:

1. **Component naming.** Started by the import path of its main, as above, or
   declared by the package under an entry-point group
   (`[project.entry-points."cot.runsomewhere"] runner = "mypkg.testing.runner:main"`)
   and started by name, so that only parts the system declares can be run
   elsewhere.
2. **Where a component's code runs inside its worker:** its own thread, the
   worker's main thread, or a task on the worker's trio or asyncio loop,
   chosen per component at start.
3. Copy semantics on threads too, with no `Thread(shared=True)`.
4. The engine written on anyio, which is what lets `inline=True` also work
   under asyncio.
5. Kubernetes (`kubectl exec -i`) in-tree after ssh and podman, with no public
   transport extension point until an outside transport asks for one.
6. No greenlet feature until someone asks; a gevent worker profile first if
   they do.
