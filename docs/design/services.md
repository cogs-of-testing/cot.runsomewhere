# Services

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

## What a service is

A service is a part of your system that runs in a worker and is talked to
over a channel: a test runner, a build agent, a host agent, a plugin host.
It is declared by the package that provides it, as a Python entry point:

```toml
[project.entry-points."cot.runsomewhere.services"]
"fleet.agent" = "fleet.agent:serve"
"mypkg.testing.runner" = "mypkg.testing.runner:serve"
```

The entry point's name is the service's name; its value is the handler. A
worker offers exactly the services declared in its own environment, plus the
built-in ones. Nothing is started by import path and nothing registers at
runtime: if a service is not declared by an installed package, it does not
exist on that worker.

Everything a gateway can do beyond carrying channels is a service, the
built-in ones included. The gateway core has no opcode per feature.

## Opening a service: the channel level

```python
async with gateway.open("fleet.agent", verbose=True) as channel:
    ...
```

sends one request, and the worker calls the handler with a fresh channel and
the parameters. Leaving the block closes the channel.

```python
async def serve(channel, *, verbose=False):
    async for request in channel:
        await channel.send(handle(request, verbose))
    return {"handled": count}
```

- Parameters are keyword-only and must be sendable values.
- Every `open` is one call of the handler, with its own channel. A service
  that should hold state across callers keeps it in its module, inside the
  worker.
- When the handler returns, the channel closes with the return value;
  `await channel.wait_closed()` gives it to the caller. When it raises, the
  caller gets `RemoteError`, with the remote traceback as text.
- The handshake lists the services a worker offers. Opening one it does not
  offer raises `StateError` before anything is sent; that is almost always an
  environment missing the package that declares it.

## Clients: the API level

A bare channel is the wire. Code using a service should not have to know its
message shapes, so the package that declares a service also ships a
**client**: an object that owns the channel and offers the service's API.

```python
class Agent(rsh.Client, service="fleet.agent"):
    async def status(self) -> dict:
        await self.channel.send({"op": "status"})
        return await self.channel.receive()

    async def apply(self, change: dict) -> None:
        await self.channel.send({"op": "apply", "change": change})
        await self.channel.receive()


async with gateway.open(Agent, verbose=True) as agent:
    print(await agent.status())
```

`gateway.open` takes either a service name, and yields the channel, or a
client class, and yields the client wrapping that channel. Leaving the block
closes it. Like `spawn`, `open` is only an async context manager, never
awaitable on its own: a gateway, channel or client always has a scope that
closes it. Code holding several opens each in the task that uses it. Service and client live in the same package, so
the messages between them are that package's private protocol, versioned and
tested together, and a caller only sees methods.

**Proposed:** clients are written once, async. On the sync facade the same
call yields a sync wrapper, which runs each method in the engine host
([surfaces](surfaces.md)):

```python
with rsh.sync.open_group() as group:
    with group.spawn(rsh.Ssh("nas")) as gateway:
        with gateway.open(Agent, verbose=True) as agent:
            print(agent.status())
```

The built-in services are reached through clients too. Those that extend
what a gateway is for appear as gateway methods: `gateway.spawn` is the
client of `rsh.via`, `gateway.deploy` of `rsh.deploy` and `rsh.transfer`,
`gateway.connect` and `gateway.forward` of `rsh.proxy`. Remote exec is a
concession, not part of how a system is built, so it gets no gateway method:
its client is opened like any other, `gateway.open(rsh.RemoteExec)`
([remote exec](remote-exec.md)).

## Built-in services

Names starting with `rsh.` are reserved.

| Service           | Does                                                                                              | Default |
| ----------------- | ------------------------------------------------------------------------------------------------- | ------- |
| `rsh.info`        | reports version, Python, platform and services                                                    | on      |
| `rsh.via`         | spawns a worker reachable from this one and tunnels its gateway ([relaying](relaying.md))         | on      |
| `rsh.deploy`      | builds an environment and installs into it ([deployment](deployment.md))                          | on      |
| `rsh.transfer`    | receives a file tree, diff-only ([deployment](deployment.md))                                     | on      |
| `rsh.proxy`       | connects to an address reachable from this worker and carries the bytes ([relaying](relaying.md)) | off     |
| `rsh.remote_exec` | runs code sent by the caller ([remote exec](remote-exec.md))                                      | off     |

The caller decides, per worker at spawn, which services are enabled:

```python
async with group.spawn(rsh.Ssh("box"), services={"rsh.remote_exec": True}) as gateway:
    ...
```

Declared application services are on unless the caller turns them off the
same way. The two that let a caller reach further than the system's own
services, running sent code and opening connections, are off until the
caller's code says it needs them.

## Where a handler runs inside its worker

- An **async** handler runs as a task on the worker's event loop. The loop is
  trio or asyncio, chosen per worker at spawn (`loop="trio"`); an async
  handler is written for that loop, or against anyio to run on either.
- A **sync** handler runs on a worker thread of its own and uses the
  sync channel API.

**Proposed:** a sync handler that must own the worker's main thread (signal
handlers, some GUI and C libraries) says so at its definition,

```python
@rsh.service(main_thread=True)
def serve(channel): ...
```

and such calls run one at a time on the main thread.

Calls are admitted in order and bounded: a worker has a budget of threads for
sync handlers, and a call over it is refused on its channel rather than
queued without limit.

## Stopping

Stopping a service asks its handler to finish and close its channel with a
result; closing the channel ends the call. A gateway's shutdown stops its
services first and closes what is left
([shutdown](gateways-and-channels.md#shutdown)). A handler learns of a stop
from `channel.stopping`; one that ignores it is closed when the caller's
deadline passes. A stopping handler may still create channels with
`channel.new()`, for instance to hand back a final report.

Workers spawned through a relay or a proxy are shut down before it. The
engine tracks that, as a property of each spawn, not of the service
([dependents](gateways-and-channels.md#dependents)). The worker tracks
nothing.

Closing the channel from the caller, closing the client, or closing the
gateway ends a call:

- an async handler waiting on its channel sees the close (its receive raises
  `ChannelClosed`, its iteration ends); one busy with anything else is
  cancelled;
- a sync handler is never cancelled: it sees `ChannelClosed` on its next
  channel operation, and is expected to return. A thread cannot be killed from
  outside.

A sync handler that ignores its closed channel keeps the worker from exiting
cleanly, and the gateway's shutdown forces the worker: terminating and then
killing a process. In-process places (thread, subinterpreter) cannot be
killed; a handler still running there is reported and abandoned, which is one
reason to put code you do not control in a process.

## Rules for service authors

- A service validates its parameters and answers a bad request by closing its
  channel with an error; it never lets an exception escape into the worker.
- A service that fails to start must still close its channel, so the caller
  sees an error instead of waiting.
- A service does not assume which place it runs in. The same service runs in
  a thread and in a container.
- Service names are dotted and start with the declaring package's name.
- A service ships its client in the same package.
