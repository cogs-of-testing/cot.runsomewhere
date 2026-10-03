from __future__ import annotations


class RemoteError(Exception):
    """The other side raised, or closed its channel with an error."""

    def __init__(
        self, message: str, *, remote_type: str = "", remote_traceback: str = ""
    ) -> None:
        super().__init__(f"{remote_type}: {message}" if remote_type else message)
        self.remote_type = remote_type
        self.remote_traceback = remote_traceback


class ChannelClosed(OSError):
    """The channel was closed."""


class ItemsDiscarded(ChannelClosed):
    """The peer stopped receiving before it took every item sent."""

    def __init__(self, message: str, *, taken: int, discarded: int) -> None:
        super().__init__(message)
        self.taken = taken
        self.discarded = discarded


class WorkerGone(OSError):
    """The worker, or the link to it, is gone."""


class HostNotFound(OSError):
    """A place could not be reached at all."""


class HandshakeRefused(OSError):
    """The other side speaks an incompatible protocol or runsomewhere version."""


class StateError(Exception):
    """The API was used wrong: a closed gateway, a foreign channel, a service
    the worker does not offer."""
