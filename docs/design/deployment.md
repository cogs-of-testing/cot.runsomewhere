# Places, interpreters and deployment

*Written by Claude Opus 5.5 via Claude Code for engineers deciding whether
runsomewhere fits their system; Ronny prompted it, it did the work, Ronny read
it.*

Status: design. Decisions marked **proposed** are open for review.

Three questions decide where a service runs: which *machine or container*,
which *interpreter* there, and which *installed code* that interpreter sees.
Each has its own value.

## Referring to a place

```python
rsh.Thread()
rsh.Subinterpreter()
rsh.Process(python="3.12")
rsh.Ssh("buildbox", python="3.13")
rsh.Ssh("10.0.0.7", user="ci", port=2222, config="~/.ssh/ci_config")
rsh.Container("fedora:44", runtime="podman", python="3.12")
rsh.Container(name="app-1", runtime="docker")
```

- **Ssh** takes a host as the ssh client would: an alias from the ssh config,
  with the config's user, port, identity and `ProxyJump` applied. Explicit
  arguments override the config. runsomewhere runs the system `ssh` binary,
  so agent forwarding, control masters and hardware keys work as they already
  do for you.
- **Container** with an image starts a fresh container (`run --rm -i`); with
  `name=` it execs into a running one. The runtime is `podman` or `docker`.
- **Places relative to a worker** are the same values handed to
  `gateway.spawn` ([relaying](relaying.md)); `rsh.Process()` spawned through
  the build box's gateway is a process on the build box.
- **Place kinds are entry points**, in the group `cot.runsomewhere.places`,
  like services. A place sent to `rsh.via` travels as its entry-point name
  and its fields, never as an import path. runsomewhere declares its own
  places; the test harness declares `inloop` the same way.

Places are values, not strings. For command lines and configuration files
written for execnet, `cot.runsomewhere.compat.xspec` reads execnet's spec
strings into the same values:

```python
from cot.runsomewhere.compat import xspec

xspec.parse("ssh=buildbox//python=3.13")  # rsh.Ssh("buildbox", python="3.13")
```

It is a compat module, outside the default API: `rsh` itself has no string
form of a place.

## Referring to an interpreter

`python=` is interpreted on the target, not on the caller:

| Value                              | Means                                                                                                                |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| omitted                            | for `Process`: the caller's own interpreter and environment. Elsewhere: whatever uv picks by default there           |
| `"3.12"`, `">=3.11"`, `"pypy3.10"` | a version request, resolved by uv on the target: an installed interpreter if one matches, a uv-managed one otherwise |
| `"/usr/bin/python3"`               | exactly that interpreter                                                                                             |

Omitted `python=` on a local `Process` is the one case that needs no uv at
all: the worker is `sys.executable -m cot.runsomewhere worker` in the
caller's environment.

## Referring to installed code: environments

What the worker's interpreter can import is its **environment**, one of four
kinds, in order of preference:

**Installed.** An environment on the target that already has runsomewhere
and the system's services, named by its interpreter
(`python="/opt/app/.venv/bin/python"`) or built into a container image. This
is the desired mode: it is used as it is, and nothing is bootstrapped
([bootstrapping](bootstrap.md)).

**Current.** The caller's own environment. Always the case for threads and
subinterpreters; the default for a local `Process` without `python=`.

**Provisioned.** An environment uv builds on the target from requirements:

```python
rsh.Process(python="3.10", env=rsh.Requirements("mypkg==2.1", "attrs"))
```

It is cached on the target by the hash of the requirements and interpreter,
so the second spawn is a lookup.

**Deployment.** Your project, as it is on your disk now:

```python
rsh.Deployment(".", roots=["testing", "conftest.py"], name="mypkg-ci")
```

- the project's **lockfile**, applied with `uv sync --frozen`, so the target
  resolves nothing and gets exactly your versions;
- the project's **wheel**, built on the caller and installed on the target,
  rather than a source tree;
- extra **roots**: what a run needs that the wheel does not contain, such as
  tests, conftest files and fixture data, transferred diff-only.

Provisioned environments and deployments are built by runsomewhere, and get
runsomewhere itself at the caller's exact version
([bootstrapping](bootstrap.md)). An installed one must already have a
version the handshake accepts: the same major and minor.

## Deploying

A deployment is a step on a gateway, done once per target, before the workers
that use it:

```python
async with group.spawn(rsh.Ssh("buildbox")) as host:
    env = await host.deploy(rsh.Deployment(".", roots=["testing"], name="mypkg-ci"))
    async with env.spawn(rsh.Process()) as worker:
        ...
```

For a single worker, `group.spawn(place, deploy=deployment)` does the three
steps in one call: it reaches the place, deploys, and starts the worker in the
deployed environment.

`host.deploy` uses the host worker's `rsh.deploy` and `rsh.transfer`
services, over the same connection: no second ssh session and no second set
of credentials, which is also what lets the same code deploy into a container.
`env.spawn` is `host.spawn` with the deployed environment filled in.

The steps, in the only order that works:

1. **Environment**, from the lockfile, with `uv sync --frozen` into the
   workspace's own virtual environment.
2. **Wheel**, built once on the caller, cached by content hash, sent, and
   installed into that environment.
3. **Roots**, transferred into the workspace.

A fan-out to several hosts builds the wheel once and deploys to each host
concurrently.

### Transfer

The caller sends one manifest of the whole tree: paths, sizes, modes and
modification times. The target answers with what it is missing, plus a digest
for every file whose size matches but whose time does not. Bodies follow in
1 MiB chunks for what differs. Re-sending an unchanged tree is one round trip.

**Proposed:** transfer mirrors: files in the workspace that are not in the
manifest are removed, so a deleted test does not keep running remotely.

### Workspaces

Deployments with the same `name` share a workspace on the target, so the
second caller to a host finds the environment the first one built. Without a
name, the workspace is keyed by the project's path and lockfile hash.

**Proposed:** workspaces live under
`${XDG_DATA_HOME:-~/.local/share}/cot.runsomewhere/workspaces/<name>`, caches
(wheels, uv binaries, interpreters) under
`${XDG_CACHE_HOME:-~/.cache}/cot.runsomewhere/`. Nothing is cleaned up
automatically; `runsomewhere prune` on the target removes workspaces unused
for a given time.

### Paths

The caller knows local paths; the deployed tree lives elsewhere.

```python
env.paths.translate(
    "testing/test_x.py"
)  # "/home/ci/.local/share/.../testing/test_x.py"
env.paths.root  # the remote workspace
```

A caller hands services remote paths, never local ones.

### A trap the deploy service handles

A worker inherits its environment variables from whatever started it, and the
caller very often runs inside a virtual environment. `uv` honours
`VIRTUAL_ENV`, so an install "into the workspace" would silently go into the
caller's environment instead. The deploy service removes `VIRTUAL_ENV`,
`UV_PROJECT_ENVIRONMENT` and `CONDA_PREFIX` from what it runs, and names the
target interpreter explicitly.
