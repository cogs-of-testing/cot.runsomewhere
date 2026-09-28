# Bootstrapping by uv availability

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review; anything
marked **to verify** is an assumption not yet checked.

A worker is runsomewhere itself, installed on the target and started as
`python -m cot.runsomewhere worker`. Before that command can run, the target
needs an interpreter and an environment containing the caller's exact version
of runsomewhere. Bootstrapping is how that gets there. It never ships
runsomewhere's own code as a script to run: what arrives is a wheel, installed
the ordinary way.

uv is the tool that makes this cheap, so the strategy is decided by where uv
is available: already on the target, obtainable for it, or not at all.

## Steps

```text
probe ─► get uv ─► get Python ─► get runsomewhere ─► launch worker ─► handshake
```

Each step is skipped when the probe shows it is already satisfied.

### 1. Probe

One short POSIX shell script, run over the place's own command channel
(`ssh host sh -s`, `podman exec -i name sh -s`), prints one `key=value` per
line:

- `os`, `arch`, and `libc` (glibc version, or musl);
- `uv`: the first uv found on `PATH`, in `~/.local/bin`, `~/.cargo/bin` or
  runsomewhere's own cache, with its version;
- `python`: interpreters on `PATH` with their versions, and the requested one
  if `python=` named a path;
- `cache`: runsomewhere's cache directory, and whether it is writable;
- `runsomewhere`: whether the requested interpreter already has runsomewhere,
  and its version, from `python -m cot.runsomewhere info`.

The probe is the only shell script runsomewhere sends, it is read-only, and it
needs nothing beyond `sh`, `uname` and `command -v`.

### 2. Tiers

The probe decides one of five tiers; lower is cheaper.

| Tier | Target has | Launch |
|---|---|---|
| 0 | the caller's own interpreter (local `Process`, no `python=`) | `sys.executable -m cot.runsomewhere worker` |
| 1 | an interpreter with runsomewhere at the caller's version | `<python> -m cot.runsomewhere worker` |
| 2 | uv | `uv run --no-project --python <req> --with <runsomewhere> python -m cot.runsomewhere worker` |
| 3 | no uv, but a platform uv supports | ship uv, then tier 2 |
| 4 | no uv and no usable uv build; a Python ≥ 3.10 | ship wheels, `python -m venv`, then tier 1 |

Tier 1 preserves the interpreter exactly, which matters when `sys.executable`
is part of what is being tested. Tier 2 is the normal case for every remote
and container place.

`--no-project` is load-bearing in tier 2: without it, uv run from a directory
that contains a `pyproject.toml` syncs *that* project into the worker's
environment.

### 3. Getting uv onto the target (tier 3)

**Proposed:** the caller ships a uv binary, taken from uv's own wheels on
PyPI, which exist per platform and contain the binary.

1. The caller picks the uv wheel for the target's `os`, `arch` and `libc`, at
   the uv version the caller uses itself, so both ends behave alike.
2. It downloads that wheel once into its cache, keyed by version and
   platform, and extracts the binary.
3. It streams the binary to the target's cache over the place's command
   channel: `head -c <size> > uv.tmp && chmod +x uv.tmp && mv uv.tmp uv`.
   The exact-length read is load-bearing: it lets the same stream carry the
   next command afterwards, and a short transfer never leaves a truncated
   binary under the final name.
4. Later spawns to the same target find it in step 1.

For Linux targets, a musl build is statically linked and runs regardless of
the target's glibc (**to verify** that uv's musllinux wheels carry a static
binary). For a container started fresh, the binary is mounted read-only
instead of streamed: `-v <cache>/uv-<version>-<platform>/uv:/opt/rsh/uv:ro`.

A caller without network access uses a pre-seeded cache, or names a uv binary
per platform in its configuration.

### 4. Getting Python

Tiers 2 and 3 get the interpreter from uv: an installed one if it satisfies
`python=`, otherwise a uv-managed download on the target. A target without
network access gets the managed interpreter from the caller's own uv cache
instead, streamed the same way as the uv binary, when the platform matches.
**Proposed**, since it makes offline targets work but copies ~30 MB per
interpreter.

### 5. Getting runsomewhere

Always the caller's exact version, so the handshake cannot refuse on skew:

- a **released** caller asks for `cot.runsomewhere==<version>` from the
  index;
- a **development** caller builds its own wheel once, caches it by version
  and content, and ships it to the target's cache the same way as the uv
  binary; `--with <path-to-wheel>` then installs it without an index.

Through a relay ([relaying](relaying.md)), the caller hands the wheel, and
the uv binary if the leaf needs one, to the relay in the spawn request; the
relay bootstraps the new worker with them.

### 6. Launch and handshake

The launch command line names the protocol transport and nothing else; the
configuration follows as the first frame (see
[gateways](gateways-and-channels.md)). Where the transport can be kept off
stdio, it is:

| Place | Protocol stream |
|---|---|
| local process, POSIX | inherited socketpair |
| local process, Windows | socket duplicated into the child with `socket.share()`, falling back to stdio where that fails |
| ssh, POSIX | a unix socket forwarded back with `ssh -R`, dialled by the worker |
| ssh to Windows | stdio |
| container | stdio |

A worker on stdio moves the protocol off fd 0 and 1 before it runs anything
else.

## Tier 4: no uv at all

**Proposed:** tier 4 exists for platforms uv does not build for and for
targets whose policy forbids foreign binaries. It works only because of a
constraint the rest of the design has to keep: **runsomewhere's runtime
dependencies are pure Python**. The caller then downloads the wheels for
runsomewhere and its dependencies, ships them, and installs them with the
target's own `python -m venv` and `pip install --no-index`. It is slow, has
no interpreter provisioning (`python=` must name one that exists), and cannot
apply a deployment's lockfile with `uv sync`; a deployment on tier 4 installs
the lockfile's pinned wheels the same way instead.

## uv on the caller

The caller needs uv for every tier above 1: to build wheels, download uv
wheels and run locally provisioned processes. It uses the first of:

1. `uv` on `PATH`;
2. the `uv` Python package in the caller's environment, which ships the
   binary (**proposed**: an optional extra, `cot.runsomewhere[uv]`).

Without either, only tiers 0 and 1 and in-process places are available, and
spawning anything else fails with a message naming the missing uv.

## What bootstrapping never does

- It never installs anything outside runsomewhere's own cache and workspace
  directories on the target, never uses `sudo`, and never touches the
  system Python's packages.
- It never puts configuration or secrets in a command line.
- It never guesses: a target the probe cannot classify fails the spawn with
  the probe's output in the error.
