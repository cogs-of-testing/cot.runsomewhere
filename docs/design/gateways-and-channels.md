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
5. **Close.** Leaving the `spawn` block shuts the worker down in two steps
   ([shutdown](#shutdown)). A gateway stop ends new service calls and stops
   the running ones. A gateway terminate then ends the communication, and the
   worker exits. If it has not done so by the deadline, the place forces it.
6. **Gone.** On EOF or a transport error at any point, every open channel
   fails with `WorkerGone`, and the gateway is closed. Nothing reconnects;
   a new worker is a new spawn.

A gateway belongs to exactly one group, and its `spawn` block always ends
inside the group's. Scopes nest, so a worker spawned through another is closed
before the one it runs through. If a group's scope is cancelled or fails, it
closes whatever gateways are still open, concurrently where their
[dependents](#dependents) allow.

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

A gateway has two messages of its own, a stop and a terminate:

- **Gateway stop** ends new service calls: an open that reaches the worker
  after it is refused with `StateError`. It does not end channel creation.
  A service that is shutting down may still need channels for that, to hand
  back logs or a final report, so `channel.new()` keeps working. The worker
  then stops its running service calls, and answers the stop
  ([dependents](#dependents)).
- **Gateway terminate** ends the communication. Nothing is sent after it in
  either direction, every channel still open fails, and the worker exits
  within the deadline the terminate carries. It cascades: a worker that is
  terminated terminates its dependents before it exits.

The names follow POSIX signals on purpose, and differ from them on purpose.
`SIGSTOP` suspends a process, cannot be caught, and is undone by `SIGCONT`.
A gateway stop suspends nothing and is not undone: it asks the services to
finish, which is closer to what `SIGTERM` asks of a process. A gateway
terminate is closer to `SIGKILL`, but only for the communication: the worker
still runs its own exit, and the place's force steps (`SIGTERM`, then
`SIGKILL`) are what remains if it does not. The words were chosen for what a
caller does, stop a service and terminate a link, not to mirror the signal
table.

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
`drain`, how long it gets from the gateway terminate to exit. The policy
holds one for edge workers and one for proxies, and a spawn overrides them
with `teardown=`. The targets are soft deadlines: each bounds its own node's
wait, and nothing above caps a subtree. Hard deadlines, a cap on a whole
subtree, need more detail than that and are left for later. A cancelled
scope, and an engine stopped because its process exits, skip the waits and
go straight to force: a cancellation shortens the wait, it never leaves a
worker running.

#### Dependents

A worker that others depend on is a proxy. A dependent is always another
worker, and the dependency is a property of its spawn:

- **Tunnelled:** a worker spawned with `gateway.spawn` runs through that
  gateway's `rsh.via` ([relaying](relaying.md)).
- **Declared:** a worker the engine cannot see the dependency of, such as
  one reached through a port that `rsh.proxy` forwards, is spawned with
  `group.spawn(place, through=gateway)`.

A proxy is stopped only after its dependents, and everything that is not
ordered that way is stopped concurrently: sibling dependents, separate
subtrees, the groups of an engine, the service calls of a stop phase.

Workers keep no map of these dependencies; they stay dumb. The engine keeps
it, as a tree of the spawns it made, and drives the shutdown from it. It
holds the tree before any shutdown starts, so a worker that no longer
answers cannot hide what runs behind it. Service calls are not part of it: a
connection `rsh.proxy` carries for someone else is not the engine's to stop,
and the call's `stop` target bounds it.

**Proposed:** once `rsh.proxy` exists, `through=` takes the object that
carries the dependency, such as the forward that `gateway.forward` yields,
rather than the whole gateway. Via and proxy then name the same thing the
tree records, and a forward closed early fails only what was spawned through
it.

A gateway's shutdown runs in the engine:

1. Its dependents are shut down first, concurrently, each through its own
   spawn, which repeats these steps. The tree is walked from the leaves up.
2. The engine sends the gateway stop. The worker refuses new service calls
   from then on, and does nothing else on its own.
3. The engine stops the service calls still open, concurrently, and waits
   for them to close, drained.
4. It sends the gateway terminate, and the worker exits. If a stop fails,
   by error or by its `stop` target passing, the terminate follows at once.

**Terminate cascades.** A terminate comes only after a stop has failed, so
it may tear down whatever is left without care for order: direct and
indirect dependents alike. A worker that is terminated terminates its
dependents before it exits. A relay ends each tunnel, which the leaf takes as
a terminate, gives the leaf the targets the caller sent when it opened the
tunnel, and has the place force it if it overruns. The leaf does the same for
its own dependents. This is also what happens when the engine cannot drive,
because a relay was sent `SIGTERM` or the link to it was lost: a worker that
is sent `SIGTERM`, or whose stream ends, takes it as a gateway terminate with
no time of its own to drain. A terminated relay therefore does not orphan
what runs behind it.

Three kinds of shutdown cascade differently:

- **A channel** that closes stops its handler on the far side. An async
  handler waiting on the channel sees the close; one busy elsewhere is
  cancelled. A sync handler sees it on its next channel operation.
- **A gateway** shuts down its dependents, sends its gateway stop, stops
  its service calls and waits for them to close, drained. Then it sends the
  gateway terminate, and has the place force the worker if it overruns. The tunnel channels of
  [relayed](relaying.md) gateways are internals of the gateway, not channels
  a caller opened: they follow the tree, never a channel's shutdown.
- **An engine** shuts down every group still open, then stops its event loop
  and its thread or subinterpreter. It is stopped when the managing
  process's main thread exits, without an `atexit` hook: its host thread is
  not a daemon, so interpreter shutdown waits for it, and a watcher that
  joins the main thread wakes first and stops it. Groups still open close as
  cancelled, which forces their workers.

A channel the caller left open when its gateway, group or engine shuts down
is drained as part of that shutdown, and the facades emit a `ResourceWarning`
naming it. A channel left by its own block is closed, and drained, as asked:
nothing to warn about. Channels a service created with `channel.new()` belong
to that service call and end with it. One still open at the gateway terminate
fails, and is reported the same way.

The stop phase only waits for service calls, which only the caller opens.
Since the caller is also the side that sends the gateway stop, no open can
race it. A channel created with `channel.new()` around the stop is allowed
either way, so it cannot race the stop either. This settles
https://github.com/cogs-of-testing/cot.runsomewhere/issues/13 without naming
the last channel id.

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

The window is counted in bytes of encoded payload, 1 MiB by default. How
credit travels, and how large items pass a small window, is part of
[the wire](#credit).

### Cancellation

A cancelled receive does not lose an item that already arrived: it stays at
the head of the channel for the next receive. A cancelled send has either
been written entirely or not at all.

## The wire

The protocol is version 2. Nothing older is deployed, so there is no
compatibility layer: a peer speaking another version is refused on the first
read.

The stream starts with 4 magic bytes and the protocol version byte, before
any frame. A worker started on the wrong stream, or an unrelated program at
the other end, fails on the first read with a clear message instead of a
garbled frame.

The decoder is sans-IO: it takes bytes and yields frames, never reads or
waits, so every transport and every event loop uses the same one.

### Frames

A frame is a header and a payload. The header is one tag byte and up to
three fields, each 0 to 3 bytes, big-endian:

```text
tag:  [ more | abort | chan:2 | len:2 | taken:2 ]   then chan, len, taken
size: 0 = the field is 0 and takes no bytes; 1, 2, 3 = that many bytes
```

- `chan` is the channel id; 0 is the control channel.
- `len` is the payload length.
- `taken` is how many bytes of the other direction this side has taken
  since it last said so: credit, riding along ([credit](#credit)).
- `more` and `abort` belong to fragments ([large items](#large-items)).

Each field is at most 24 bits; a larger value is a protocol error. One tag
byte tells the decoder the header's length, 1 to 10 bytes, so it never peeks
further.

| Frame                           | v1         | v2    |
| ------------------------------- | ---------- | ----- |
| a 300-byte item, no credit owed | 9 B        | 4 B   |
| the same, with credit owed      | 9 B + 13 B | 5 B   |
| a credit-only frame             | 13 B       | 3–5 B |

Channel ids are allocated by the side that creates the channel: odd on the
caller, even on the worker, so both can open without coordination. Ids are
never reused while late frames for a closed channel may still be routed by
id.

### Control

Channels carry only data. Everything else travels on channel 0 as an encoded
dict with an `op`:

| op                  | Fields                                              |
| ------------------- | --------------------------------------------------- |
| `hello`             | protocol, versions, Python, platform, pid, services |
| `config`            | the worker's configuration, or the answer to it     |
| `open`              | channel, service, params                            |
| `close`             | channel, `ends` (send or receive) or result / error |
| `stop`              | channel, deadline                                   |
| `gateway-stop`      |                                                     |
| `gateway-terminate` | deadline                                            |

Control messages are rare, so readability and room to grow win over size. A
table maps each op to its fields and their types, checked on decode. Ordering
is unchanged: there is one stream with one writer queue, so items sent before
a close arrive before it.

Later control features become ops or fields, not frame types. A control op
or field the receiver does not know fails only the channel it names, on both
sides, and both log it at warning level: the gateway (the peer's pid and both
runsomewhere versions), what was not understood, and the channel. Every other
channel and the gateway go on. Only bytes that cannot be parsed as frames end
the gateway.

### Credit

Every data frame reports, in `taken`, what this side has taken from the other
direction since its last report, so request/response traffic costs no credit
frames at all. Credit is granted on taking, not on arrival, so
[drain](#closing) keeps its meaning.

When nothing flows back, a credit-only frame goes out, delayed by at most
1 ms so it can ride a reply. Pending credit is per-channel state, not a
queued frame; when the writer would go idle it waits up to 1 ms for another
frame, then flushes every channel's pending credit at once. Credit goes out
at once when:

- what is taken and not reported reaches a quarter of the window;
- the sender's remaining credit is low, which the receiver computes exactly
  as granted + window − received, against a quarter of the window, because
  the sender may be blocked on it;
- this side closes or half-closes the channel;
- a stop arrives, or the gateway is stopping.

Before a close, the remaining credit is flushed in the last frame.

Why 1 ms: measured on v1 with 3000 round trips of a 10-byte echo, async
handlers reply 13–18 µs after the credit is due (median), and sync handlers
80–96 µs, all within 1 ms. TCP's delayed acknowledgement (RFC 1122, RFC 9293:
up to 500 ms; Linux 40–200 ms) and QUIC's `max_ack_delay` (RFC 9000: 25 ms)
are sized for WAN round trips. On a window-limited one-way stream, the
quarter-window bypass keeps a delay from capping throughput. The cost:
`drain()` on a channel with no reply traffic returns up to 1 ms later.

### Large items

An item larger than a fragment goes out as a chain of frames on its channel;
every frame but the last sets `more`. Fragments are 64 KiB to start, so
other channels' frames and control slip in between. A send cancelled
halfway sends an empty frame with `abort`, and the receiver drops the
partial item: a cancelled send is still received whole or not at all.

Credit for a fragmented item comes back when the whole item is taken. An
item larger than the window waits for the window to be fully open, then goes
through, so a large item is slow rather than impossible.

An item is encoded whole and decoded once all its fragments are in; items
above 64 MiB are refused with `StateError` at send time. Bulk data belongs
in the transfer service. Encoding and decoding an item in pieces, so that
neither side holds it whole, is left for later.

### Values

Payloads of data frames are values in a tagged binary encoding. Nothing in it
can name a class or run code on decode. One tag byte per item, multi-byte
fields big-endian:

| Tag                  | Item                                          |
| -------------------- | --------------------------------------------- |
| `0x80` + n           | int n, 0 ≤ n < 64                             |
| `0xC0` + n           | str of n UTF-8 bytes, n < 32                  |
| `N` `T` `F`          | None, True, False                             |
| `1` `2` `4`          | int in 8, 16 or 32 bits, signed               |
| `i` / `I`            | int as signed bytes, their count in 8/32 bits |
| `D` `C`              | float, complex: IEEE 754 doubles              |
| `s` / `S`, `b` / `B` | str, bytes: their size in 8 / 32 bits         |
| `[` / `]`, `(` / `)` | list, tuple: their count in 8 / 32 bits       |
| `<` / `l`, `>` / `g` | set, frozenset, likewise                      |
| `{` / `}`            | dict: its pair count, then key, value, ...    |
| `X`                  | extension: a code byte, then one item         |

Fixed widths, chosen over varints by measurement: the tag alone says how
many bytes follow, which keeps decoding simple in pure Python and in C. On an
xdist-style report the encoding is 425 bytes against 598 in v1, and both
directions are faster.

The codec knows nothing of channels. A value it has no encoding for goes to
a hook of its caller, which returns an extension code and an item; decoding
hands both back. The connection owns code 0, a channel, carried as its id:
it checks that the channel belongs to this gateway on the way out, and
resolves the id to its end of the channel on the way in. Containers and
extensions nest at most 200 deep, the same limit on both sides.

**Speedups.** `cot-runsomewhere-speedups`, built from `speedups/` in this
repository with its own pipeline, is the same codec in C (import name
`_cot_runsomewhere_speedups`), for Linux, macOS and Windows. It is optional:
the pure-Python codec is the reference, and the C one is used when it is
installed and speaks the same format. Both produce the same bytes, which the
tests check.

### Names

Names on the wire are entry-point names: services by their service name,
places (sent to `rsh.via`) by their place name. No frame carries an import
path.

### Considered and dropped

- **Varints for value lengths and small ints.** They save bytes over v1 but
  cost as much CPU as v1 in pure Python. Fixed widths are smaller still and
  faster.
- **Credit as its own control message.** At 35–69 bytes per credit, it costs
  more than it saves in chatty traffic.
- **Header fields encoded UTF-8 style.** Strict UTF-8 tops out at 21 bits and
  wastes 2 bits per continuation byte.
- **Reusing channel ids to keep them small.** Unsafe while late frames for a
  closed channel are routed by id.

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
