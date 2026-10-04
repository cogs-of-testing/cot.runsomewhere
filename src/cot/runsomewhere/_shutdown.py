"""Shutdown policy: the soft targets each worker gets while it is torn down."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Teardown:
    """One worker's soft targets, in seconds.

    ``stop`` bounds its gateway's stop phase, in which channels left open are
    stopped and drained; ``drain`` bounds the time from the gateway
    terminate until the worker has exited. Past them, the place forces the worker.
    """

    stop: float = 2.5
    drain: float = 2.5

    @property
    def total(self) -> float:
        return self.stop + self.drain

    def to_value(self) -> dict[str, float]:
        return {"stop": self.stop, "drain": self.drain}

    @classmethod
    def from_value(cls, value: dict[str, Any]) -> Teardown:
        return cls(stop=value["stop"], drain=value["drain"])


@dataclass(frozen=True)
class Shutdown:
    """How a group tears down its workers: targets for edge workers, which
    nothing depends on, and for proxies, which are torn down after the
    workers that depend on them."""

    edge: Teardown = field(default_factory=Teardown)
    proxy: Teardown = field(default_factory=Teardown)

    def to_value(self) -> dict[str, Any]:
        return {"edge": self.edge.to_value(), "proxy": self.proxy.to_value()}

    @classmethod
    def from_value(cls, value: dict[str, Any]) -> Shutdown:
        return cls(
            edge=Teardown.from_value(value["edge"]),
            proxy=Teardown.from_value(value["proxy"]),
        )
