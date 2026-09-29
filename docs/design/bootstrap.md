# Bootstrapping

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review; anything
marked **to verify** is an assumption not yet checked.

## The desired mode: an existing install

A worker should start from an environment that already has runsomewhere and
the system's services installed: a virtual environment deployed with the
application, a container image built with them, a host provisioned by the
system's own configuration management. runsomewhere then uses that install as
it is, and bootstraps nothing:

```python
rsh.Ssh("app-host", python="/opt/app/.venv/bin/python")
rsh.Container("registry.example/app:1.4")  # image built with its services
rsh.Process(python="/opt/app/.venv/bin/python")
```

The caller runs `<python> -m cot.runsomewhere worker` there, over a
socketpair, a dial-back socket or stdio (see [below](#the-protocol-stream)).
The worker finds its services through the entry points of what is installed
([services](services.md)). If the install's runsomewhere differs from the
caller's in major or minor version, the handshake refuses, and says so; the
fix is to update the install, not to paper over it.

This is the mode to design a system for. Installed code is what the system
tested, versioned and deployed; everything below exists for targets where
that has not happened yet.

The local cases are the same mode:

- a local `Process` with no `python=` is the caller's own interpreter and
  environment, `sys.executable -m cot.runsomewhere worker`;
- threads and subinterpreters share the caller's environment and start
  nothing.

## When there is no install: the ladder

Bootstrapping is a concession, for targets that have a Python but no
suitable install: a fresh machine, a stock container image, a Python version
under test. The ladder climbs from what the target has to the same end state
as the desired mode: the target's Python can import runsomewhere, and the
services it should offer, from **wheels**, and the worker is started through
runsomewhere's **entry points**. Only how the wheels get there differs.

The caller takes the ladder only when asked (`bootstrap=True` on the place,
**proposed**) or when the place names no interpreter that has an install; an
interpreter named explicitly that lacks runsomewhere fails the spawn instead of
being bootstrapped behind the caller's back.

```text
  bare Python ──► stub on stdin ──► wheels importable ──► worker ──► uv environment
      ▲                                                                (when needed)
  no Python: ship uv, install one
```

## Rung 0: launch a Python

The place gives runsomewhere one command line on the target: a local
subprocess, `ssh host`, `podman exec -i`. On it runs

```sh
python3 -c "<stub>" <transport>
```

where `<transport>` says where the stub reads from: `stdin`, or an inherited
socket (`fd:3` for a local socketpair).

The **stub** is the only code ever sent as source during bootstrap. It is
small (a few dozen lines), stdlib only, runs on every supported Python,
and is fixed per runsomewhere version: it takes no configuration, so it can
be reviewed once and then recognised.

If `python3` is not there, see [no Python](#no-python-at-all).

## Rung 1: the stub fetches wheels

The stub speaks a tiny exchange on its stream, before the protocol proper:

1. It sends what it sees: Python version and implementation, platform tags,
   its cache directory, and which wheels (by sha256) the cache already holds.
2. The caller answers with the list of wheels this worker needs: runsomewhere
   and its runtime dependencies at the caller's exact versions, each with name,
   sha256 and size.
3. For every wheel the cache lacks, the caller sends exactly `size` bytes. The
   stub checks the hash, writes to a temporary name, and renames into the
   cache, so a broken transfer never leaves a half wheel under a real name.

Wheels are cached on the target by hash, so a second worker on the same
target transfers nothing.

## Rung 2: wheels become importable

**Proposed:** each wheel is unpacked once, into a directory named by its hash,
and those directories go on `sys.path`. No installer runs and no environment
is created: an unpacked wheel is already an importable tree, with its
`.dist-info` next to its packages, so `importlib.metadata` finds its entry
points.

Unpacked rather than imported from the zip, because packages that read
`__file__`, and compiled extensions, do not work from inside a zip.

The stub then loads the `worker` entry point from runsomewhere's `.dist-info`
and calls it with the stream it was reading. From here on it is the worker.

## Rung 3: the worker

The worker starts the protocol handshake on the same stream
([gateways](gateways-and-channels.md)), and from here on behaves exactly as a
worker from an existing install.

At this rung the worker offers the services of every wheel it was given. That
is enough whenever those wheels suit the target's Python and platform: the
common case for runsomewhere itself, whose runtime dependencies are pure
Python, and for services that are.

## Rung 4: a uv environment, when needed

Some targets need more than wheels on a path: a different Python than the one
found, compiled dependencies built for the target, or a
[deployment](deployment.md) whose lockfile must be applied exactly.

For those, the rung-3 worker becomes the **host worker**, and the environment
is built from it with uv:

- **uv is a wheel too.** If the target has no uv, the caller sends uv's wheel
  for the target's platform, the same way as any other; its binary is in the
  wheel.
- The host worker runs uv to get the Python (`python=`), create the
  environment and apply the lockfile, with `uv sync --frozen` or `uv pip install` of the wheels it already has.
- The workers that run services start inside that environment, spawned
  through the host worker ([relaying](relaying.md)), and again start from
  runsomewhere's `worker` entry point.

`uv run` is always given `--no-project`: without it, uv run from a directory
containing a `pyproject.toml` syncs that project into the worker's
environment.

## Where the wheels come from

On the caller:

- runsomewhere's own wheel: from the index for a released version, or built
  once from the caller's checkout for a development one, cached by content;
- its runtime dependencies and uv: resolved and downloaded for the target's
  platform tags, cached by hash;
- a service's package and its dependencies: from the deployment's lockfile.

A caller without network access uses a pre-seeded wheel cache.

## No Python at all

**Proposed:** the one rung below 0. If the target has a POSIX shell but no
usable Python, the caller streams uv's binary to it (extracted from uv's wheel
for that platform) with an exact-length read,
`head -c <size> > uv.tmp && chmod +x uv.tmp && mv uv.tmp uv`, has uv install a
Python, and starts at rung 0 with that Python. The exact-length read lets the
same stream carry the next command, and a short transfer never leaves a
truncated binary under its final name.

**To verify:** uv's musllinux wheels carry a statically linked binary that
runs on any Linux of the same architecture.

## The protocol stream

Whichever way a worker started, the handshake runs on the stream it was
given: version check, then the configuration frame. Where the place allows,
the protocol then moves off stdio:

| Place                     | Protocol stream                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------------------ |
| local process, POSIX      | the inherited socketpair, from the start                                                         |
| local process, Windows    | a socket duplicated into the child with `socket.share()`, falling back to stdio where that fails |
| ssh, POSIX                | a unix socket forwarded back with `ssh -R`, dialled by the worker                                |
| ssh to Windows, container | stdio                                                                                            |

On stdio, the worker moves the protocol off fd 0 and 1 before running
anything else, so a stray `print` cannot corrupt the stream.

## What bootstrapping never does

- It never installs anything outside runsomewhere's cache and workspace
  directories on the target, never uses `sudo`, and never touches the system
  Python's packages.
- It never sends code as source except the fixed stub.
- It never puts configuration or secrets in a command line.
- It never guesses: a target the stub cannot serve fails the spawn with what
  the stub reported.
