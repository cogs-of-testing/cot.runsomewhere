# Gateways and channels

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

## Two nouns

A **worker** is an interpreter running runsomewhere somewhere: a thread, a
subinterpreter, a process, a process on another host or in a container. It is
the thing that has a pid, a Python version and an environment.

A **gateway** is the link to one worker: the protocol spoken over one byte
stream, with an endpoint on each side. Everything you do with a worker, you
do through its gateway: open its services, spawn further workers through
it, close it.

```python
async with rsh.open_group() as group:
    async with group.spawn(rsh.Process(python="3.12")) as gateway:
        gateway.worker.python  # "3.12.9"
        gateway.worker.pid  # 41327
        gateway.worker.platform  # "linux-x86_64"
        gateway.services  # frozenset({"rsh.info", "rsh.via", ...})
```

`group.spawn` is an async context manager (a plain one on the sync facade):
it starts the worker, yields its gateway, and closes the gateway when the
block ends. `gateway.worker` describes what is on the
far end, as reported in the handshake. The two are separate because they fail
separately: a gateway can be lost while its worker still runs (a relay in
between went away), and one worker can be reached through a gateway that
passes through others ([relaying](relaying.md)).

## Life of a gateway

```text
launch ─► handshake ─► configure ─► serve ─► close ─► closed
                │            │                  │
                └─ refused ──┴──────────────────┴─► gone
```

1. **Launch.** The place starts a worker and hands it one end of a byte
   stream: an in-memory pair, a socketpair, a dial-back socket, or stdio
   ([bootstrapping](bootstrap.md) covers how the worker gets installed first).
2. **Handshake.** The worker writes a hello frame: protocol version,
   runsomewhere version, Python, platform, pid, and the services it offers.
   The caller refuses a different protocol version or a different
   runsomewhere major.minor, and says why. The worker does the same in the
   other direction, before it touches its stdio, because after that the
   caller can only learn EOF.
3. **Configure.** The caller sends the worker's configuration as the first
   frame after the handshake: working directory, environment values, stdio
   disposition, enabled services, event loop. Configuration never travels
   in argv, where `ps` and `/proc` would show it.
4. **Serve.** Channels open and close; services run.
5. **Close.** Leaving the `spawn` block asks the worker to shut down. The
   worker stops its running services, drains and closes their channels, and
   exits. If it has not done so by the deadline, the place forces it
   ([shutdown](#shutdown)).
6. **Gone.** On EOF or a transport error at any point, every open channel
   fails with `WorkerGone`, and the gateway is closed. Nothing reconnects;
   a new worker is a new spawn.

A gateway belongs to exactly one group, and its `spawn` block always ends
inside the group's. Scopes nest, so a worker spawned through another is closed
before the one it runs through. If a group's scope is cancelled or fails, it
closes whatever gateways are still open, concurrently where the
[teardown graph](#shutdown) allows.

### Shutdown

The goal: everything that needs exiting exits safely, and nothing is left
running when its scope ends.

**Stopping a service is not closing its channel.** Stopping asks the handler
to finish: complete its work, send what it owes, and close its channel with a
result. Closing ends the communication. A shutdown stops first and closes
what is left.

A stop is sent on one channel, with `channel.stop()` or `client.stop()`. It
never cancels, drops nothing, and leaves both directions open, so the handler
can still send what it owes and close with a result. An async handler sees it
as `channel.stopping` and can wait for it with `await channel.stop_requested()`;
a sync handler polls `channel.stopping` or waits with
`channel.wait_stopping(timeout)`. Code sent with [remote exec](remote-exec.md)
gets the same. A stop carries a deadline, relative so clocks need not agree;
it is best effort: the worker passes it on, to an async handler as
`channel.stop_deadline` on its event loop's clock, and honours it where it
can, and
what enforces it is the caller, which closes the channel when it passes. A
handler that ignores its stop is closed like any other. A stop for a handler
that already returned is ignored, since that race is normal.

Either side creates channels: the worker makes them with `channel.new()` and
sends them back. A gateway's stop is therefore a frame of its own, not only
a stop per channel: after it, neither side creates channels, and creating one
raises `StateError`. Every channel open at that point is then stopped.

Every node that shuts down passes through the same phases:

1. **request**: the node is told to stop: a stop for a channel's service, a
   gateway stop for a worker, a cancellation for a task;
2. **drain**: it gets until a deadline to finish by itself, its channels
   drained ([closing](#closing));
3. **force**: what the place can do to a node that did not finish: terminate
   and then kill a process, cancel a task;
4. **abandon**: what survives force is reported with a `ResourceWarning` and
   left behind. Only in-process places (thread, subinterpreter) reach this:
   a thread cannot be killed from outside, which is one reason to put code
   you do not control in a process.

The place owns the force steps, because only it knows whether its worker can
be terminated or killed at all. The caller owns the timings, through a
shutdown policy, `rsh.Shutdown`. Each worker has targets of its own, an
`rsh.Teardown`: `stop`, how long its gateway's stop phase may take, and
`drain`, how long it gets from the gateway-close to exit. The policy holds
one for edge workers and one for proxies, and a spawn overrides them with
`teardown=`. The targets are soft deadlines: each bounds its own node's
wait, and nothing above caps a subtree. Hard deadlines, a cap on a whole
subtree, need more detail than that and are left for later. A cancelled
scope, and an engine stopped because its process exits, skip the waits and
go straight to force: a cancellation shortens the wait, it never leaves a
worker running.

**The teardown graph.** Workers that others depend on are proxies: a relay
with gateways tunnelled through it ([relaying](relaying.md)), and a worker
carrying `rsh.proxy` connections. The edges of the graph are those tunnel
channels and proxy connections; no other channel is part of it. An edge
worker has no dependents. A proxy is torn down only after its dependents,
and everything the graph does not order is torn down concurrently: sibling
dependents, separate subtrees, the groups of an engine, the channels of a
stop phase.

The caller drives the graph: it spawned every worker in it and holds every
target. Tearing down a proxy whose dependents are still open tears those
down first, through their own spawns, from the leaves up. When the caller
cannot drive, because a relay was sent SIGTERM or the link to it was lost,
the relay falls back on what it knows: it ends each tunnel, gives the leaf
the targets the caller sent when it opened the tunnel, and then has the place
force it. A worker that is sent SIGTERM shuts down with no time of its own
to drain, so a terminated relay does not orphan what runs behind it.

Three kinds of shutdown cascade differently:

- **A channel** that closes stops its handler on the far side. An async
  handler waiting on the channel sees the close; one busy elsewhere is
  cancelled. A sync handler sees it on its next channel operation.
- **A gateway** sends its gateway stop, stops every open channel, drains and
  closes them, then sends the gateway-close and has the place force the
  worker if it overruns. The tunnel channels of [relayed](relaying.md)
  gateways are internals of the gateway, not channels a caller opened: they
  follow the graph, never a channel's shutdown.
- **An engine** shuts down every group still open, then stops its event loop
  and its thread or subinterpreter. It is stopped when the managing
  process's main thread exits, without an `atexit` hook: its host thread is
  not a daemon, so interpreter shutdown waits for it, and a watcher that
  joins the main thread wakes first and stops it. Groups still open close as
  cancelled, which forces their workers.

A channel the caller left open when its gateway, group or engine shuts down
is drained as part of that shutdown, and the facades emit a `ResourceWarning`
naming it. A channel left by its own block is closed, and drained, as asked:
nothing to warn about.

Open:

- How a gateway stop races a channel created and sent just before it.
  HTTP/2's GOAWAY settles the same race by naming the last stream it will
  serve; channel ids are allocated per side, so the gateway stop could name
  the last id of each.

### Output

A worker whose protocol runs on its own stream leaves stdio to the code it
runs. Where that output goes is part of the configuration:

- `inherit`: whatever the place gives the worker (the caller's terminal for a
  local process, ssh's streams for a remote one);
- `forward`: captured and delivered to the caller on a channel,
  `gateway.output`, as lines;
- `discard`.

**Proposed:** `inherit` by default for local processes, `forward` for remote
hosts and containers, where "inherit" would put output somewhere the caller
cannot see. When the protocol itself has to run on stdio
([bootstrapping](bootstrap.md)), the worker moves it off fd 0 and 1 first, so
a stray `print` cannot corrupt the stream.

## Channels

A channel is an ordered, two-way stream of values between two endpoints on
one gateway. Values are `None`, `bool`, `int`, `float`, `complex`, `str`,
`bytes`, `tuple`, `list`, `dict`, `set`, `frozenset`, and channels.

```python
async with gateway.open("fleet.agent", verbose=True) as ch:  # a service, see services
    await ch.send({"op": "status"})
    reply = await ch.receive()
    async for item in ch:  # until the other side closes
        ...
```

### Opening

A channel is always opened *to* something on the far side: a service by
name, with parameters ([services](services.md)), or code sent with
[remote exec](remote-exec.md). There are no anonymous channels at gateway
level; a channel with no service would have nobody to talk to.

Further channels are created by the endpoints themselves and sent over an
existing channel:

```python
async def serve(channel):
    logs = channel.new()  # created here, usable once the peer receives it
    await channel.send({"logs": logs})
```

### Sending channels over channels

A channel sent as a value arrives as a usable channel on the other side. It
is a reference, not a copy: the sender keeps its end, the receiver gets the
other. A channel can only travel over its own gateway; sending it over a
different one raises `StateError`. Connecting endpoints across gateways is
[relaying](relaying.md)'s job, not the channel's.

### Closing

Either side may close either direction, and each close tells the peer:

- `close_send()` ends sending. Items already sent still arrive; the peer's
  next receive after the last item raises `ChannelClosed`.
- `close_receive()` ends receiving. Items that arrived and were not taken
  are discarded; the peer's sends, including one waiting for window, raise
  `ChannelClosed`.
- `close()` ends both.

Only the full close carries a result or an error; the two half-closes carry
nothing. The peer sees an error as `RemoteError`. When a service's handler
returns, its channel closes with the return value as the result, which the
caller reads with `await channel.wait_closed()`. A handler that ended its
sending early still closes with its result. A result on a half-close would let
a handler announce one and go on running, and an error after it would have
nowhere to go: execnet's `waitclose` returns on the end of sending, and an
error closing the channel after that is only logged. So `wait_closed()`
returns when the handler has finished, and a channel stays known to its
gateway until both directions have ended.

**Drain.** `await channel.drain()` waits until every item sent has been
taken by the peer. The window already says so: the receiver grants credit
back as its consumer takes items ([flow control](#flow-control)), so drain
needs no frame of its own. A drain also happens as part of `aclose()`, and of
a [shutdown](#shutdown), bounded by its deadline. The sync `close()` does not
wait, and does not drain. Taken is not processed: the peer's code may still
fail with an item it has taken.

**Proposed:** when the peer ends receiving with items not taken, a drain
raises `ItemsDiscarded`, a `ChannelClosed` that says how many were taken and
how many were discarded; the peer's end of receiving carries how much it had taken, so no
other frame is needed. If the peer took everything first, the drain kept its
promise and returns. The drain in `aclose()` raises it; a drain during a
shutdown records it. Every precedent tells the sender with an error, never a
quiet return: TCP with a reset, QUIC with STOP_SENDING answered by
RESET_STREAM, trio with `BrokenResourceError`.

Open, to be settled by an experiment against a process worker:

- Whether the drain behaves as proposed under a full window, a drain in
  progress, and a worker killed while draining.
- A caller that wants only the result, and ends receiving: the handler's
  next send raises, and the caller gets that back as the error. The
  alternative is to keep receiving open and let the window stall the
  handler. This may need a policy.

### Flow control

Every channel has a window granted by the receiver. A sender whose window is
exhausted waits (async) or blocks (sync facade); it never buffers on the
far side. The receiver grants more as its consumer takes items.

**Proposed:** the window is counted in bytes of encoded payload, 1 MiB by
default, settable per channel at open. One item may exceed the window when
the window is fully open, so a large item is slow rather than impossible.
Items above a hard frame limit (64 MiB, proposed) are refused with
`StateError` at send time; bulk data belongs in the transfer service.

### Cancellation

A cancelled receive does not lose an item that already arrived: it stays at
the head of the channel for the next receive. A cancelled send has either
been written entirely or not at all.

## The wire

Every frame is a fixed header and a payload:

| Field   | Size    | Meaning                                                                                                   |
| ------- | ------- | --------------------------------------------------------------------------------------------------------- |
| type    | 1 byte  | hello, config, open, data, credit, close (with the directions it ends), stop, gateway-stop, gateway-close |
| channel | 4 bytes | channel id; 0 for gateway-level frames                                                                    |
| length  | 4 bytes | payload length                                                                                            |

The decoder is sans-IO: it takes bytes and yields frames, never reads or
waits, so every transport and every event loop uses the same one.

Channel ids are allocated by the side that creates the channel: odd on the
caller, even on the worker, so both can open without coordination. Payloads
of data frames are values in a tagged binary encoding; nothing in it can name
a class or run code on decode.

Names on the wire are entry-point names: services by their service name,
places (sent to `rsh.via`) by their place name. No frame carries an import
path.

**Proposed:** the stream starts with 4 magic bytes and a protocol version
byte, before any frame, so that a worker started on the wrong stream, or an
unrelated program at the other end, fails on the first read with a clear
message instead of a garbled frame.

## Errors

| Situation                                                         | Error                                                              |
| ----------------------------------------------------------------- | ------------------------------------------------------------------ |
| The far side raised, or closed with an error                      | `RemoteError`, with the remote traceback as text                   |
| The channel was closed                                            | `ChannelClosed` (an `OSError`)                                     |
| A drain ended with items the peer never took                      | `ItemsDiscarded` (a `ChannelClosed`), with `taken` and `discarded` |
| The worker or the link is gone                                    | `WorkerGone` (an `OSError`)                                        |
| A place could not be reached at all                               | `HostNotFound` (an `OSError`)                                      |
| The other side's protocol or runsomewhere version is incompatible | `HandshakeRefused` (an `OSError`), naming both versions            |
| Wrong use: closed channel, foreign gateway, unknown service       | `StateError`                                                       |
| A sync-facade timeout                                             | builtin `TimeoutError`                                             |
