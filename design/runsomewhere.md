# runsomewhere: design

*Written by Claude Opus 5.5 via Claude Code for Ronny and future runsomewhere
contributors; Ronny prompted it, it did the work, Ronny read it.*

Status: proposal. Nothing here is implemented yet.

## What it is

runsomewhere runs Python code somewhere other than the current interpreter, and
talks to it while it runs. "Somewhere" means any of these, from least to most
isolated:

| Place | Isolation | Code arrives by |
|---|---|---|
| thread | none: shared modules and objects | already imported |
| subinterpreter | its own modules, same process | same environment |
| process | its own interpreter, same machine | same environment, or provisioned |
| remote host | another machine (ssh) | provisioned with uv |
| container | another root filesystem (podman, docker; later kubernetes pods) | image, or provisioned with uv |

Every place has one programming model: start a *worker* in a place, then call
*entrypoints* on it and exchange data with them over *channels*. Code can move
from a thread to a container by changing only the place.

## Where it comes from

runsomewhere is a new package, started from an empty repository. Its prior art
is the async-core rework of execnet,
[pytest-dev/execnet#422](https://github.com/pytest-dev/execnet/pull/422), and
that PR is where the history lives: the design decisions, the ones that were made
and then unmade, and the failure modes the tests pin down. Code comes over from
there piece by piece, when a step below needs it, rewritten against this design.
It is not imported wholesale.

execnet itself stays where it is, for pytest-xdist. runsomewhere owes it **no
backward compatibility**: no shims, no forwarding modules, and no interoperation
with execnet on the wire.

Ideas worth porting from the PR:

- the single async protocol engine, running on trio or asyncio, with one engine
  thread per process;
- no source shipping: a worker runs an installed runsomewhere and is launched
  through the `worker` CLI, which is the launch contract;
- uv provisioning of foreign interpreters and remote hosts;
- `Deployment` and `transfer`: a locked environment plus the project, and trees
  sent diff-only;
- the protocol off stdio, with a socketpair, `socket.share()` or ssh dial-back;
- the error taxonomy (below);
- the typed wire format, and `can_send`.

## Concepts

**Place**: where a worker runs. It is a typed value, not a string:

```python
rs.Thread()
rs.Subinterpreter()
rs.Process(python="3.13")
rs.Ssh("box", python="3.13", config="~/.ssh/config")
rs.Container("quay.io/org/image:tag", runtime="podman")  # new container
rs.Container(name="running-one", runtime="podman")  # exec into one
```

A string form, such as `ssh=box//python=3.13`, remains only as a parser,
`rs.parse_place()`, for command lines and config files.

**Worker**: a running peer in a place. It is an async context manager, and it is
the identity: there is no `id=`.

**Environment**: what is installed where the worker runs. For threads and
subinterpreters this is always the coordinator's own. For processes, hosts and
containers it is one of:

- the current environment;
- provisioned by uv from a requirement;
- a `Deployment`: the project's lockfile, the project's wheel, and extra roots.

**Entrypoint**: code named by import path, `"pkg.module:function"`. The worker
imports it from its own environment. Nothing is sent as source unless you ask
for `exec_source`.

**Channel**: an ordered, bidirectional stream of simple builtin values, plus
channels. This is the same object model as execnet.

**Profile**: where an entrypoint runs *inside* the worker:

- `thread`: a worker thread per call;
- `main`: the worker's main thread, one call at a time;
- `trio` or `asyncio`: a task on the worker's own loop, for async entrypoints.

## API sketch

The async surface is the primary one, and works under both trio and asyncio.
It detects the running loop.

```python
import runsomewhere as rs

async with rs.open_group() as group:
    worker = await group.spawn(rs.Process())

    # one-shot: the return value comes back
    total = await worker.call("mypkg.tasks:add", a=1, b=2)

    # long-running: the entrypoint receives a channel as its first argument
    async with await worker.open_channel("mypkg.worker:serve", root="/srv") as ch:
        await ch.send({"job": 1})
        async for item in ch:
            ...
```

Entrypoints are ordinary functions:

```python
def add(a, b):
    return a + b


async def serve(channel, root):
    async for job in channel:
        await channel.send(run(job, root))
```

Hosts, environments and nested workers form a chain of objects. This replaces
execnet's `via=` spec key and the `Deployed.spec` string splicing:

```python
host = await group.spawn(rs.Ssh("box"))
env = await host.deploy(rs.Deployment(".", roots=["testing"]))
workers = [await env.spawn(rs.Process(), profile="thread") for _ in range(4)]
env.paths.translate("testing/test_x.py")  # local path -> remote path
```

`worker.spawn(place)` starts a child worker relayed through that worker. It is
the one place-independent spelling of "run it from over there".

The blocking surface has the same shape, without `await`:

```python
with rs.blocking.open_group() as group:
    worker = group.spawn(rs.Ssh("box"))
    print(worker.call("platform:node"))
```

### Surfaces

| Surface | For | Protocol IO runs on |
|---|---|---|
| `runsomewhere` | trio or asyncio callers | the shared engine thread |
| `runsomewhere.blocking` | threads, scripts | the shared engine thread |
| `open_group(inline=True)` | trio only: the gateway as tasks in your nursery | your loop |

Inline mode is the execnet PR's `raw_trio` namespace, turned into a flag. It raises
under asyncio. On the async surfaces, cancellation comes from the caller's own
scope, so there is no `timeout=`. The blocking surface keeps `timeout=`.

### Lifecycle

There is no default group, no module-level `spawn`, and no atexit cleanup.
Everything is opened with `async with` or `with`. A group owns its workers, and
closing it terminates them in reverse order of creation.

## Threads and subinterpreters speak the protocol too

The obvious design would give threads a direct function call and reserve the
protocol for processes. That would give each place different semantics: shared
mutable objects in a thread, copies everywhere else; exceptions as objects in one
place, `RemoteError` in the others. So every place speaks the same protocol over a
byte stream. In-process places use an in-memory stream pair, not a socket.

This costs a serialization round trip per item on a thread, which a thread does
not strictly need. A zero-copy fast path for threads is possible later, as long as
it keeps copy semantics. Measure first.

Subinterpreters use `concurrent.interpreters` (PEP 734, Python 3.14+). Each one
runs the worker on its own loop, and the byte stream crosses through an
interpreter queue. This place needs a newer Python than the rest, and extension
modules that support subinterpreters. Where either is missing, spawning a
`Subinterpreter` fails with a clear error. It does not fall back to a thread.

## Containers

A container is a command transport, the same shape as ssh: `podman run --rm -i
IMAGE runsomewhere worker --protocol-stdio`, or `podman exec -i NAME ...`. stdio is
the default protocol transport here, because a socketpair does not cross the
container boundary. A mounted unix socket is the later upgrade.

The image either already has runsomewhere installed, or provisioning mounts the
host's cached wheel and runs it under uv inside the container, the way the
execnet PR ships a wheel over ssh.

Kubernetes pods (`kubectl exec -i`) are the same transport again. Whether pods
belong in-tree or behind a documented transport extension point is still open;
see question 5.

## Greenlets: none at first, but a place for them

runsomewhere starts without gevent. The execnet PR retired a public wait-backend
registry, `Wakener`, in commit
[RonnyPfannschmidt/execnet@bdf980d](https://github.com/RonnyPfannschmidt/execnet/commit/bdf980d),
because nothing ever plugged into it. So the place kept for greenlets is internal, not a plugin
point. It sits in three spots:

1. **How the blocking surface waits.** It parks the caller on `OneShot` or
   `Mailbox`, through a single wait backend: OS threads. A greenlet backend
   becomes a second backend, and nothing else changes.
2. **The "no blocking call inside a running loop" guard** belongs to the wait
   backend, not to global state. In the execnet PR, monkey-patched `threading` makes
   that guard refuse every blocking call, and this layout avoids that failure by
   construction.
3. **The engine thread** is started through one function, which can later ask
   gevent for the unpatched `threading`. The engine must be a real OS thread,
   even in a monkey-patched process.

Worker profiles stay a closed set, and `gevent` joins it later.

A second, separate meaning of "greenlet support" is the greenback or SQLAlchemy
pattern: blocking-style calls from inside a running asyncio loop, bridged with
greenlets. That would lift the guard in point 2 rather than enforce it. It is out
of scope until someone needs it; see question 3.

## Wire protocol

- **A new handshake with a version byte.** execnet ≤ 3 cannot be spoken to. Major
  or minor skew between the two sides is refused, as in the execnet PR.
- **Per-channel flow control, reserved now:** a credit window, granted by the
  receiver. In the execnet PR a fast sender is buffered without limit on the other end.
  Adding the space after the first release is the expensive way round.
- **Entrypoint messages** carry an import path and keyword arguments. Source is a
  separate message type, used only by `exec_source`.
- **Infrastructure operations stay first-class messages,** as in the execnet PR: spawning
  through a worker, socket handoff, and deploy.

## Errors

The execnet PR's taxonomy carries over unchanged. Every error answers one of three questions:

- **The other side failed:** `RemoteError`, which carries the remote traceback as
  text.
- **The connection is gone:** `ChannelClosed`, `WorkerGone` (the PR's
  `GatewayGone`), `HostNotFound`. All are `OSError` subclasses. Timeouts on the
  blocking surface raise the builtin `TimeoutError`.
- **The call was wrong:** `StateError`, which is not an `OSError`.

## Dropped from execnet

- The `//` string DSL as the primary API, and the `id=` and `via=` spec keys.
- `remote_exec` with source strings, functions or modules as the main path, and
  the `__channelexec__` global injection.
- `default_group` and module-level `makegateway`.
- `Gateway`, which becomes `Worker`.
- `MultiChannel`. Fan-out uses group helpers, such as `group.gather`.
- `setcallback`, `makefile` and `receive(timeout=)` on async channels.
- `RSync`, `set_execmodel`, `execmodel=`, `remote_init_threads`, `remote_status`
  and `rinfo`.
- Every compatibility shim and forwarding module, including `execnet.dumps`.
- The gevent namespace. It comes back later, per the section above.
- A public `ProtocolEngine`.

## Build order

The repository starts with this document and nothing else. Each step is its own
pull request, green on its own, and ports what it needs from the execnet PR.

1. **Skeleton.** Packaging, CI, pre-commit, the licence, and the
   `runsomewhere worker|info` CLI stub.
2. **Core.** The message framing, serializer and error types, then the protocol
   engine on trio and asyncio, and a `Process` place over a socketpair. Nothing
   else yet.
3. **Entrypoints.** `call`, `open_channel` and `exec_source`, and their message
   types. The handshake version byte and the flow-control credits land here,
   before anything depends on the wire.
4. **Surfaces.** The async surface, `runsomewhere.blocking` with its wait backend,
   and `inline=True`.
5. **In-process places.** `Thread`, then `Subinterpreter`.
6. **Remote.** uv provisioning, `Ssh`, `worker.spawn` relaying, then `Deployment`
   and `transfer`.
7. **Containers.** Podman first, then docker.
8. **Later:** kubernetes, the gevent backend, and the thread fast path.

## Open questions

1. **Minimum Python.** The execnet PR supports 3.10, running trio only below
   3.11. Starting at 3.11 drops that split.
2. **Thread fast path.** Do we keep copy semantics everywhere, as proposed, or
   offer `Thread(shared=True)` for callers who want object sharing?
3. **Greenlets.** Which does runsomewhere eventually need: gevent processes, the
   greenback-style bridge, or both?
4. **Entrypoint shape.** Is `call` versus `open_channel` the right split, or should
   there be one call whose entrypoint decides by taking a `channel` parameter?
5. **Kubernetes.** In-tree, or behind a documented transport extension point?
6. **Import name.** `runsomewhere` is long for `import`. Should the docs use
   `import runsomewhere as rs`, or should there be a short alias package?
7. **Licence.** execnet is MIT. Code ported from the execnet PR keeps that
   notice either way; does runsomewhere as a whole stay MIT?
