# Relaying: via and proxy

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

Two services let a worker reach further on the caller's behalf. Both carry a
byte stream over one channel, and neither looks inside it.

| Service | Reaches | The caller gets |
|---|---|---|
| `rsh.via` | a new worker, started from this one | a gateway to that worker |
| `rsh.proxy` | an address reachable from this worker | a byte stream, or a local port |

## via: workers spawned through a worker

```python
async with group.spawn(rsh.Ssh("buildbox")) as host:
    async with host.spawn(rsh.Process()) as worker:
        ...
```

`gateway.spawn(place)`, an async context manager like `group.spawn`, opens a channel to the worker's `rsh.via` service,
which launches a new worker in `place` *as seen from that worker*: a process
on the build box, a container on the build box's podman, or another ssh hop
from there. The new worker's byte stream is carried over that channel, and
the caller runs a gateway over it as if the worker were directly connected.

```text
caller ── gateway A ──► buildbox worker ─┬─ socketpair ─► worker 1
    └── gateway 1 (tunnelled in a channel of A) ──────────►┘
```

The tunnel is end to end:

- **The handshake is end to end.** Version checks are between the caller and
  the new worker; the relay's own version does not vouch for anything.
- **The configuration is end to end.** The new worker's configuration,
  environment values included, travels inside the tunnel. The relay starts
  the process and moves bytes; it never decodes the frames it relays.
- **Flow control is per hop.** The tunnel channel has its own window, so a
  fast worker cannot fill the relay's memory either.
- **Failure follows the path.** If the relay's gateway goes, every gateway
  tunnelled through it fails with `WorkerGone`. If one tunnelled worker dies,
  only its gateway does.

The relay does the launching, because it is the one with access to the place.
So it also does the [bootstrapping](bootstrap.md) of the new worker: probing,
fetching uv, and installing runsomewhere there. What only the caller has, a
development wheel or a uv binary for another platform, the caller sends along
in the spawn request.

Chains compose, one block per hop:

```python
async with group.spawn(rsh.Ssh("outer")) as outer:
    async with outer.spawn(rsh.Ssh("inner")) as inner:
        async with inner.spawn(rsh.Process()) as leaf:
            ...
```

`leaf` is a worker two hops away. Each hop adds latency, and the caller's gateway
to the leaf still behaves like any other.

Groups own tunnelled gateways like any other, in reverse order of creation, so
the workers on the build box close before the gateway to the build box does.

### via or ssh ProxyJump

For reaching a host, an ssh jump host (`ProxyJump` in the ssh config) is the
usual answer, and runsomewhere honours it: the jump host needs nothing
installed. `via` is for when the hop is itself a place with work to do: a
host that runs sixteen workers, holds a deployment, or is the only machine
that can reach a container runtime.

## proxy: connections from a worker's vantage point

**Proposed:** `rsh.proxy`, off by default, connects to an address from the
worker's side and carries the bytes to the caller.

```python
async with group.spawn(rsh.Container(name="app-1"), services={"rsh.proxy": True}) as gateway:
    # a byte stream to a database only reachable inside the container's network
    async with gateway.connect("tcp:db:5432") as stream:
        await stream.send(startup_packet)

    # or a local listener, forwarding each accepted connection
    async with gateway.forward(local="tcp:127.0.0.1:15432", remote="tcp:db:5432"):
        run_migrations("postgresql://127.0.0.1:15432/app")
```

- Addresses are `tcp:host:port` or `unix:/path`.
- Each connection is one channel of `bytes` items with the usual window, so
  a slow client applies backpressure all the way to the far socket.
- Closing either end closes the other.
- The reverse direction, a listener on the worker forwarding to the caller,
  is `gateway.forward(remote=..., local=...)` with the roles swapped; it lets
  a worker reach a service that only exists on the caller's machine.

It is off by default because it turns every worker into a network pivot;
turning it on is a statement in the caller's code that the system needs it.
