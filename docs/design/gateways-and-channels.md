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
do through its gateway: open channels, use its services, start components on
it, spawn further workers through it, close it.

```python
async with rsh.open_group() as group:
    gateway = await group.spawn(rsh.Process(python="3.12"))
    gateway.worker.python      # "3.12.9"
    gateway.worker.pid         # 41327
    gateway.worker.platform    # "linux-x86_64"
    gateway.services           # frozenset({"rsh.exec", "rsh.via", ...})
```

`group.spawn` returns the gateway. `gateway.worker` describes what is on the
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
   disposition, enabled services, profile defaults. Configuration never
   travels in argv, where `ps` and `/proc` would show it.
4. **Serve.** Channels open and close; services and components run.
5. **Close.** The group, or `await gateway.aclose()`, asks the worker to shut
   down. The worker cancels its components and services, closes their
   channels, and exits. The caller waits for EOF, then terminates the worker
   and, after a grace period, kills it.
6. **Gone.** On EOF or a transport error at any point, every open channel
   fails with `WorkerGone`, and the gateway is closed. Nothing reconnects;
   a new worker is a new spawn.

A gateway belongs to exactly one group. Closing the group closes its
gateways in reverse order of creation, so a worker spawned through another is
closed before the one it runs through.

## Channels

A channel is an ordered, two-way stream of values between two endpoints on
one gateway. Values are `None`, `bool`, `int`, `float`, `complex`, `str`,
`bytes`, `tuple`, `list`, `dict`, `set`, `frozenset`, and channels.

```python
ch = await gateway.open("fleet.status", verbose=True)   # a service, see services
await ch.send({"op": "status"})
reply = await ch.receive()
async for item in ch:                                    # until the other side closes
    ...
await ch.aclose()
```

### Opening

A channel is always opened *to* something on the far side: a service by
name, with parameters ([services](services.md)). Starting a component is
opening a channel to the exec service ([exec](exec.md)). There are no
anonymous channels at gateway level; a channel with no service would have
nobody to talk to.

Further channels are created by the endpoints themselves and sent over an
existing channel:

```python
async def main(channel):
    logs = channel.new()          # created here, usable once the peer receives it
    await channel.send({"logs": logs})
```

### Sending channels over channels

A channel sent as a value arrives as a usable channel on the other side. It
is a reference, not a copy: the sender keeps its end, the receiver gets the
other. A channel can only travel over its own gateway; sending it over a
different one raises `StateError`. Connecting endpoints across gateways is
[relaying](relaying.md)'s job, not the channel's.

### Closing

Either side may close. A close carries an optional error; the peer's next
receive after the last item raises `ChannelClosed`, or `RemoteError` when the
close carried one. Items sent before a close arrive before it. A close is
final for both directions: a channel is a conversation, and a half-open one
is a bug waiting to happen. Two directions that end independently are two
channels.

When a component's main returns, its channel closes with the return value as
the result, which the caller reads with `await handle.wait()`.

### Flow control

Every channel has a window granted by the receiver. A sender whose window is
exhausted waits (async) or blocks (blocking surface); it never buffers on the
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

| Field | Size | Meaning |
|---|---|---|
| type | 1 byte | hello, config, open, data, credit, close, gateway-close |
| channel | 4 bytes | channel id; 0 for gateway-level frames |
| length | 4 bytes | payload length |

The decoder is sans-IO: it takes bytes and yields frames, never reads or
waits, so every transport and every event loop uses the same one.

Channel ids are allocated by the side that creates the channel: odd on the
caller, even on the worker, so both can open without coordination. Payloads
of data frames are values in a tagged binary encoding; nothing in it can name
a class or run code on decode.

**Proposed:** the stream starts with 4 magic bytes and a protocol version
byte, before any frame, so that a worker started on the wrong stream, or an
unrelated program at the other end, fails on the first read with a clear
message instead of a garbled frame.

## Errors

| Situation | Error |
|---|---|
| The far side raised, or closed with an error | `RemoteError`, with the remote traceback as text |
| The channel was closed | `ChannelClosed` (an `OSError`) |
| The worker or the link is gone | `WorkerGone` (an `OSError`) |
| A place could not be reached at all | `HostNotFound` (an `OSError`) |
| Wrong use: closed channel, foreign gateway, unknown service | `StateError` |
| A blocking-surface timeout | builtin `TimeoutError` |
