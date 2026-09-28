# Running code: the exec service

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

Running code on a worker is a [service](services.md), `rsh.exec`, and not
something the gateway core does. A gateway without it can still relay,
deploy and serve application services; it just cannot be told to run
anything else.

## Starting a component

```python
runner = await gateway.start("mypkg.testing.runner:main", root="/srv")
await runner.channel.send({"job": 1})
summary = await runner.wait()
```

`gateway.start` opens a channel to `rsh.exec` with the entrypoint and the
parameters. The worker imports `mypkg.testing.runner` from its own
environment, and calls `main(channel, root="/srv")`. The code comes from
what is installed there, at the version that was deployed; nothing but the
name and the parameters crosses the wire.

A component's main:

```python
async def main(channel, *, root):
    async for job in channel:
        await channel.send(run(job, root))
    return {"ran": count}
```

- The first argument is the component's channel; parameters are keyword-only
  and must be sendable values.
- The return value becomes the result of `runner.wait()`.
- An exception becomes `RemoteError` on `runner.wait()` and on the caller's
  next channel operation, with the remote traceback as text.
- The component ends when main returns. Its channel closes then, if it has
  not already.

**Proposed:** components are named by the import path of their main, as
above. The alternative, declaring them under an entry-point group the way
[application services](services.md) are and starting them by name, would
restrict what a caller can start to what a package declares. Import paths
are simpler, and anything importable in the worker is already trusted.

## Where a component runs inside its worker

The profile chooses, per start:

| Profile | Runs on | For |
|---|---|---|
| `thread` | a worker thread of its own | sync code that blocks; the default for sync mains |
| `main` | the worker's main thread, one at a time | code that must own the main thread: signal handlers, some GUI and C libraries |
| `loop` | a task on the worker's event loop | async mains; the default for them |

```python
await gateway.start("app.gui:main", profile="main")
```

The worker's loop is trio or asyncio, chosen at spawn with
`loop="trio" | "asyncio"`; an async main must be written for that loop, or
against anyio to run on either.

Starts are admitted in order and bounded: a worker has a budget of threads
for `thread` components and sync services together, and a start over it is
refused on its channel rather than queued without limit.

## Stopping a component

Closing the component's channel from the caller, or `await runner.aclose()`,
asks it to stop:

- an async component is cancelled;
- a sync component sees `ChannelClosed` on its next channel operation, and is
  expected to return. A thread cannot be killed from outside.

Closing the gateway stops every component on it. A sync component that
ignores its closed channel keeps the worker from exiting cleanly, and the
gateway's close escalates to terminating and then killing the worker process.
In-process places (thread, subinterpreter) cannot be killed, which is one
reason to prefer a process for code you do not control.

## Output

A worker whose protocol runs on its own stream leaves stdio to its
components. Where that output goes is set in the worker's configuration:

- `inherit`: the worker's stdout and stderr are whatever the place gives it
  (the caller's terminal for a local process, ssh's streams for a remote one);
- `forward`: captured and delivered on a channel, `gateway.output`, as lines;
- `discard`.

**Proposed:** `inherit` by default for local processes, `forward` for remote
hosts and containers, where "inherit" would mean output lands somewhere the
caller cannot see.

When the protocol itself has to run on stdio (see
[bootstrapping](bootstrap.md)), the worker moves the protocol off fd 0 and 1
before running anything, so a component's `print` cannot corrupt the stream.

## Source execution, opt-in

For ad-hoc work and debugging, a component's module may arrive as source text
instead of an import path:

```python
gateway = await group.spawn(rsh.Ssh("box"), services={"rsh.exec.source": True})
probe = await gateway.start_source(
    """
    import os

    def main(channel):
        channel.send(os.listdir("/var/lib/app"))
    """
)
```

The rules keep it from becoming a way to move code into a system:

- **Off by default,** per worker, and enabled only in the caller's
  configuration at spawn. A worker never enables it for itself.
- **Text only.** A module's source, compiled in a fresh namespace on the
  worker. No functions, closures, bytecode or pickles; the source must stand
  alone against the worker's installed packages.
- **Same shape as a component.** It must define `main(channel, **params)`,
  and runs, stops, reports and returns exactly as an installed one does.
- **Named in tracebacks** as `<source #n from caller>`, so a `RemoteError`
  from it cannot be mistaken for installed code.
