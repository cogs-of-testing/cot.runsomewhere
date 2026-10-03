# cot.runsomewhere

Run parts of a software system somewhere else, connect them, and deploy them
where needed: threads, subinterpreters, processes, remote hosts, containers.

```python
from cot import runsomewhere as rsh
```

*Written by Claude Opus 5.5 via Claude Code for people deciding whether to try
runsomewhere; Ronny prompted it, it did the work, Ronny read it.*

Early, and not released. What works: the protocol, channels with flow control
and cancellation-safe receives, declared services and their clients, remote
exec, relaying through workers, local `Process` places on POSIX with an
interpreter that has runsomewhere installed, and the thread and subinterpreter
engines behind the sync and async facades. Not yet: the `Thread`,
`Subinterpreter`, `Ssh` and `Container` places, bootstrapping, deployment and
`rsh.proxy`.

The design is in [docs/design/index.md](docs/design/index.md).

Closest prior idea: execnet and its async-core rework,
[pytest-dev/execnet#422](https://github.com/pytest-dev/execnet/pull/422).
