# Services

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

## What a service is

The gateway core knows only frames, the handshake and channels. Everything a
gateway can *do* is a **service**: a named handler in the worker that is
handed a channel each time a caller opens one to it.

```python
ch = await gateway.open("fleet.status", verbose=True)
```

opens a channel to the service `fleet.status` on that worker, which is called
as

```python
async def status_service(channel, *, verbose=False):
    ...
```

and talks over the channel for as long as the conversation lasts. The core
has no opcode per feature and no list of features: running code, relaying,
deploying and file transfer are services like any other, built on the same
seam applications use.

## Built-in services

Names starting with `rsh.` are reserved.

| Service | Does | Default |
|---|---|---|
| `rsh.exec` | starts components from installed code ([exec](exec.md)) | on |
| `rsh.exec.source` | starts components from source text ([exec](exec.md)) | off |
| `rsh.via` | spawns a worker reachable from this one and tunnels its gateway ([relaying](relaying.md)) | on |
| `rsh.proxy` | connects to an address reachable from this worker and carries the bytes ([relaying](relaying.md)) | off |
| `rsh.deploy` | builds an environment and installs into it ([deployment](deployment.md)) | on |
| `rsh.transfer` | receives a file tree, diff-only ([deployment](deployment.md)) | on |
| `rsh.info` | reports version, Python, platform and services | on |

The caller chooses which services a worker enables, in its configuration at
spawn:

```python
gateway = await group.spawn(rsh.Ssh("box"), services={"rsh.proxy": True})
```

The handshake lists what the worker actually offers. Opening a service the
worker does not offer raises `StateError` on the caller before anything is
sent; that is almost always an environment missing the package that
provides it.

Services that let the far side reach further (source execution, proxying)
are off by default. Both sides are trusted, but a capability a system does
not use should not be lying around in it.

## Application services

Applications add services by declaring them in their package metadata:

```toml
[project.entry-points."cot.runsomewhere.services"]
"fleet.status" = "fleet.agent:status_service"
"fleet.apply" = "fleet.agent:apply_service"
```

A worker offers every service declared in its own environment, so a service
exists wherever the package providing it is installed. It is imported the
first time a channel is opened to it, not at worker start.

A service handler is a function taking the channel and keyword parameters.
It may be async, running as a task on the worker's loop, or sync, running on
a worker thread and reaching its channel through the blocking surface. When
it returns, the channel closes with the return value as the result; when it
raises, the caller gets `RemoteError`.

**Proposed:** a running component may also offer services for as long as it
runs:

```python
async def main(channel, *, root):
    async with rsh.current_gateway().provide("jobs.submit", submit):
        await serve(channel, root)
```

This is how a long-lived part, such as a host agent, exposes an interface to
callers other than the one that started it, without a second package entry.
Names provided at runtime appear in `rsh.info`, not in the handshake.

## Services and components

The two overlap on purpose, and differ in lifetime:

| | Service | Component |
|---|---|---|
| Exists | whenever the worker offers it | once started, until it ends |
| Started by | opening a channel to its name | `gateway.start(...)`, via `rsh.exec` |
| Instances | one handler call per channel | one per start |
| Typical | a request/response or a stream on demand | a runner, an agent, a plugin host |

A component is the unit of the *application*; a service is the unit of the
*gateway's* capabilities.

## Rules for service authors

- A service validates its parameters and answers a bad request by closing its
  channel with an error; it never lets an exception escape into the worker.
- A service that fails to start must still close its channel, so the caller
  sees an error instead of waiting.
- A service does not assume which place it runs in. The same service runs in
  a thread and in a container.
- Service names are dotted and start with the providing package's name.
