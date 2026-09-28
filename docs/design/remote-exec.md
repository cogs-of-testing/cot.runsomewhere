# Remote exec

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

The parts of a system run as declared [services](services.md), from code
deployed to the worker. Remote exec is the other way to run code: the caller
sends it. It exists for ad-hoc work, debugging, probing a host, and code too
small to be worth a package. It is the built-in service `rsh.remote_exec`,
and it is **off by default**.

```python
gateway = await group.spawn(rsh.Ssh("box"), services={"rsh.remote_exec": True})
channel = await gateway.remote_exec(source_or_module_or_function, **kwargs)
```

`remote_exec` returns a channel, connected to the code on the worker.

## What can be sent

### A source string

```python
channel = await gateway.remote_exec(
    """
    import os
    channel.send(sorted(os.listdir("/var/lib/app")))
    """
)
entries = await channel.receive()
```

The string is dedented and run as a fresh module, with `channel` bound in its
globals. It takes no keyword arguments.

### A module

```python
import mypkg.probes.disk

channel = await gateway.remote_exec(mypkg.probes.disk)
```

The module's source, read with `inspect.getsource` on the caller, is run the
same way as a string, with `channel` in its globals. What is sent is the text
of that one file: its own imports must resolve on the worker. It takes no
keyword arguments.

### A function

```python
def disk_usage(channel, path):
    import shutil

    usage = shutil.disk_usage(path)
    channel.send({"total": usage.total, "free": usage.free})


channel = await gateway.remote_exec(disk_usage, path="/")
```

The function's source is sent, and the worker defines and calls it with the
channel and the keyword arguments. Only functions that can stand alone as
text are accepted; the caller checks before sending anything and raises
`ValueError` otherwise:

- the first parameter is named `channel`;
- it is not a lambda;
- it has no closure: it uses no variables from an enclosing function;
- it uses no non-builtin globals: every other module it needs, it imports in
  its own body;
- its source can be found with `inspect.getsource`.

Keyword arguments must be sendable values.

## What is never sent

Only text crosses: a string, a module's file, a function's definition. No
bytecode, no pickled objects, no closures, and no values captured from the
caller's memory. What the code needs from the caller arrives as keyword
arguments or over the channel, as data. What it needs from the worker it
imports there, from the worker's environment.

## Running

- Sent code runs on a worker thread of its own, and uses the sync channel
  API. An `async def` function runs as a task on the worker's event loop
  instead, and uses the async channel API.
- When the code finishes, the channel closes. A function's return value
  becomes the close value, read with `await channel.wait_closed()`.
- An exception closes the channel with an error; the caller gets
  `RemoteError` with the remote traceback. Frames from sent code are named
  `<remote_exec #n>` in it, so they cannot be mistaken for installed code.
- Closing the channel from the caller stops the code the way a service call
  is stopped ([services](services.md)): cancelled if async, `ChannelClosed`
  on its next channel operation if sync.

## Why it is off by default

Services arrive by deployment, at a version the system chose, tested with
the rest of it. Sent code arrives from whichever caller has a gateway, at
whatever it happens to be. Both sides are trusted either way, but a worker
that only runs its declared services is one whose behaviour can be known from
its environment. Turning remote exec on is a statement in the caller's code
that this worker is for ad-hoc work.
