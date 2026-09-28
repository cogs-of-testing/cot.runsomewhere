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
another machine over ssh, or a container. A part is a **service**: code your
system already ships, declared by its package, talked to over a channel.
runsomewhere deploys the code a service needs to where it will run when it is
not already there, starts a worker there, and connects the caller to the
service. Where a service runs is one typed value, so moving it from a local
process to a build box or a container changes neither the service nor the
code using it.

## The unit is a service

What runs elsewhere is a service: a part of the system designed to run on its
own, such as a test runner, a build agent, a plugin host or a host agent. Its
package declares it as a Python entry point, and ships a client that gives
callers its API:

```toml
[project.entry-points."cot.runsomewhere.services"]
"mypkg.testing.runner" = "mypkg.testing.runner:serve"
```

A service's code arrives by deployment, as installed packages at a version
you chose, so both sides are checked, tested and versioned like the rest of
the system. The wire carries data: plain builtin values and channels. Code
sent by the caller runs only on a worker where the caller enabled
[remote exec](remote-exec.md), for ad-hoc work and debugging.

## The model

**Place**: where a worker runs. A typed value.

```python
rsh.Thread()
rsh.Subinterpreter()                             # Python 3.14+
rsh.Process(python="3.10")
rsh.Ssh("box", python="3.13")
rsh.Container("fedora:44", runtime="podman")     # fresh container
rsh.Container(name="db-1", runtime="podman")     # exec into a running one
```

**Environment**: what is installed where the worker runs. Preferably an
existing install that already has runsomewhere and your services, used as it
is. Threads and subinterpreters share the caller's. Otherwise one provisioned
by uv from requirements, or a **Deployment**: your project's lockfile, its
wheel and extra file trees, sent diff-only on repeat.

**Worker**: a runsomewhere interpreter running in a place and environment. It
offers the services declared in its environment.

**Gateway**: the link to one worker, over one byte stream. You spawn a worker
and get its gateway; everything else goes through it
([gateways and channels](gateways-and-channels.md)).

**Service**: a declared part of the system. Opening it on a gateway calls its
handler with a fresh channel. Deploying, relaying and file transfer are
built-in services ([services](services.md)).

**Channel**: an ordered, two-way stream of values. Channels can be sent over
channels, so a service can hand out further conversations.

**Client**: the object a service's package provides, which owns the channel
and offers the service's API. Callers use clients; channels are the level
below.

**Group**: the scope that owns gateways. Closing it closes them in reverse
order of creation, on success, error or cancellation alike.

```python
async with rsh.open_group() as group:
    gateway = await group.spawn(rsh.Process())
    async with gateway.open(Runner, root="/srv") as runner:
        report = await runner.run("testing/test_x.py::test_one")
```

with the service and its client in the same package:

```python
async def serve(channel, *, root):
    async for item in channel:
        await channel.send(run_test(item, root))


class Runner(rsh.Client, service="mypkg.testing.runner"):
    async def run(self, test_id: str) -> dict:
        await self.channel.send(test_id)
        return await self.channel.receive()
```

## Use cases

### Test runners

The distributed-test shape: N runners, each taking tests and reporting back,
with the controller feeding work as runners free up.

```python
async with rsh.open_group() as group, AsyncExitStack() as stack:
    runners = [
        await stack.enter_async_context(
            (await group.spawn(rsh.Process())).open(Runner, root=".")
        )
        for _ in range(8)
    ]
    async with anyio.create_task_group() as tg:
        for runner in runners:
            tg.start_soon(drive, runner, schedule)
```

A runner that crashes, leaks or segfaults takes its own worker with it. The
controller sees `WorkerGone` from that runner and reschedules its tests. The
channel window stops a fast runner from filling the controller's memory with
reports.

### The same suite on a remote box, deployed

```python
async with rsh.open_group() as group, AsyncExitStack() as stack:
    host = await group.spawn(rsh.Ssh("buildbox"))
    env = await host.deploy(rsh.Deployment(".", roots=["testing"]))
    runners = [
        await stack.enter_async_context(
            (await env.spawn(rsh.Process())).open(Runner, root=env.paths.root)
        )
        for _ in range(16)
    ]
```

The box has no install of the project yet, so this is the concession path:
it needs a Python, and runsomewhere, its dependencies and uv arrive as wheels
([bootstrapping](bootstrap.md)).
The deployment carries the lockfile, the project's wheel and the test tree,
and a second deploy sends only what changed ([deployment](deployment.md)). All
sixteen workers are spawned through the build box's worker, over one ssh
connection ([relaying](relaying.md)). The runner and the controller code are
the same as in the local case.

### Across Pythons and distros

```python
places = [rsh.Process(python=v) for v in ["3.10", "3.12", "3.14"]] + [
    rsh.Container(image) for image in ["fedora:44", "debian:13", "alpine:3.22"]
]
async with rsh.open_group() as group:
    for place in places:
        gateway = await group.spawn(place, deploy=rsh.Deployment("."))
        async with gateway.open(Session) as session:
            async for report in session.reports():
                show(place, report)
```

One service, one deployment, six places. uv provisions and caches the
interpreters. A container image with runsomewhere installed is used as is;
otherwise the cached wheel is mounted and run under uv inside the container.

### An agent on every host

A long-lived service on each machine of a fleet, answering queries and
applying changes. A script drives it, with no event loop in sight:

```python
with rsh.sync.open_group() as group:
    for name in ["nas", "router", "pi"]:
        gateway = group.spawn(rsh.Ssh(name, python="/opt/fleet/bin/python"))
        with gateway.open(Agent) as agent:
            print(name, agent.status())
```

Each host has the agent installed, so the worker starts from that install and
nothing is bootstrapped. The agent's client is its interface; the script
calls methods, it does not parse shell output. The sync facade is the async
API without `await`, plus `timeout=` ([surfaces](surfaces.md)).

### Poking at a host

Not everything is worth a package. For a one-off question, remote exec sends
a function, as text, to a worker that has it enabled:

```python
def listing(channel, path):
    import os

    return sorted(os.listdir(path))


gateway = await group.spawn(rsh.Ssh("nas"), services={"rsh.remote_exec": True})
channel = await gateway.remote_exec(listing, path="/var/lib/app")
print(await channel.wait_closed())
```

### Diagnostics inside a running container

```python
async with rsh.open_group() as group:
    gateway = await group.spawn(rsh.Container(name="app-1"))
    async with gateway.open(Diagnostics) as diag:
        print(await diag.connections())
```

`Container(name=...)` execs into a container that is already running, without
restarting it or baking a debug port into the image. The image needs the
package declaring the diagnostics service; runsomewhere itself is brought
along if missing.

### A plugin host, isolated

```python
async with rsh.open_group() as group:
    gateway = await group.spawn(rsh.Subinterpreter())
    async with gateway.open(PluginHost, paths=plugin_paths) as plugins:
        await plugins.load_all()
```

The plugin subsystem gets its own modules and globals, and plugins that mutate
module state cannot reach the application's. A subinterpreter starts far
faster than a process, but needs Python 3.14 and extension modules that
support subinterpreters; without them the spawn fails with a clear error rather
than quietly using a thread. Switch the place to `rsh.Process()` when the
plugins may also crash. The isolation is for state, not hostile code.

## What you can rely on

- **Installed code, not shipped code.** Services run code installed in their
  worker's environment, at a version you deployed. Code sent by the caller
  runs only where remote exec was explicitly enabled.
- **Same semantics in every place.** Threads get copies too, and a service's
  failure is `RemoteError` everywhere. A service that works in a thread
  behaves the same in a container.
- **Values:** `None`, `bool`, `int`, `float`, `complex`, `str`, `bytes`,
  `tuple`, `list`, `dict`, `set`, `frozenset`, and channels.
  `rsh.can_send(x)` checks without sending.
- **Nothing outlives its group.** No global state, no default group, no atexit
  hooks.
- **Async core, sync facade.** The core API is async and runs in your own trio
  or asyncio loop, with cancellation from your own scope and no `timeout=`.
  Sync code uses a facade over the same core, run in a thread or
  subinterpreter host ([surfaces](surfaces.md)).
- **Cancellation loses nothing.** A cancelled receive leaves an item that
  already arrived for the next receive.
- **Bounded memory.** Each channel has a receiver-granted window; a sender
  waits when it is full.
- **Three kinds of error:** the other side failed (`RemoteError`, with the
  remote traceback); the connection is gone (`ChannelClosed`, `WorkerGone`,
  `HostNotFound`, all `OSError`); you used the API wrong (`StateError`).
  Sync-facade timeouts raise the builtin `TimeoutError`.
- **Skew fails first.** Mismatched runsomewhere versions refuse at handshake,
  before any service runs.
- **Secrets stay out of `ps`.** Worker configuration, including environment
  values, travels over the protocol, never in argv.

## What it costs, and what it is not

- **A copy per item, even on a thread.** That is the price of identical
  semantics. A thread fast path may later skip the encoding, still handing
  over copies.
- **Start cost follows isolation:** thread, subinterpreter, process, container,
  remote host, roughly in that order. Workers are meant to be long-lived;
  reuse them rather than spawning one per request.
- **Not remote procedure calls.** Parts of a system that run elsewhere are
  services with an interface. Remote exec runs sent code for ad-hoc work; it
  is off by default and not how a system is built.
- **Not a security boundary.** Both sides are trusted.
- **Not an object proxy.** You cannot hold a reference to a remote object.
- **Not a scheduler or orchestrator.** You pick the place. There is no
  cluster, no placement, no restart policy.
- **Python 3.10 and newer,** on Linux, macOS and Windows.

## The parts

| Document | Covers |
|---|---|
| [API surfaces and engine hosts](surfaces.md) | the async core, thread and subinterpreter engine hosts, the sync and async facades |
| [Gateways and channels](gateways-and-channels.md) | worker and gateway, the gateway's lifecycle and output, channels, flow control, the wire, errors |
| [Services](services.md) | declaring services, channels and clients, built-in services, where handlers run, stopping |
| [Remote exec](remote-exec.md) | running strings, modules and functions sent by the caller; off by default |
| [Relaying](relaying.md) | `via`: workers spawned through workers; `proxy`: connections from a worker's vantage point |
| [Places, interpreters and deployment](deployment.md) | referring to hosts, containers and interpreters; environments; deploying a project |
| [Bootstrapping](bootstrap.md) | using an existing install; the ladder from stdin and sockets to importable wheels when there is none |


## Status and open decisions

Build order, each step its own pull request: skeleton; core protocol with the
`Process` place; services, clients and remote exec; the engine hosts and the
sync facade; `Thread` and `Subinterpreter`; uv provisioning, `Ssh` and
`Deployment`; podman then docker; later kubernetes and a gevent profile.

Proposed, not yet settled:

1. Clients written once, async; `gateway.open(Client)` on the sync facade
   yields a sync wrapper ([services](services.md)).
2. Copy semantics on threads too, with no `Thread(shared=True)`.
3. The core written on anyio, so it runs in trio and asyncio callers alike;
   the thread engine host by default, the subinterpreter one opt-in until
   measured ([surfaces](surfaces.md)).
4. Kubernetes (`kubectl exec -i`) in-tree after ssh and podman, with no public
   transport extension point until an outside transport asks for one.
5. No greenlet feature until someone asks; a gevent worker profile first if
   they do.

Each part document marks its own proposals.
