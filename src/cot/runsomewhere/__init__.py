"""Run parts of a software system somewhere else, and talk to them.

``from cot import runsomewhere as rsh``
"""

from . import sync
from ._channels import Channel
from ._engine import SubinterpreterEngine, ThreadEngine, use_engine
from ._errors import (
    ChannelClosed,
    HandshakeRefused,
    HostNotFound,
    ItemsDiscarded,
    RemoteError,
    StateError,
    WorkerGone,
)
from ._gateway import Client, Gateway, Group, WorkerInfo
from ._open import open_group
from ._places import Container, Place, Process, Ssh, Subinterpreter, Thread
from ._remote_exec import RemoteExec
from ._values import can_send
from ._version import version as __version__

__all__ = [
    "Channel",
    "ChannelClosed",
    "Client",
    "Container",
    "Gateway",
    "Group",
    "HandshakeRefused",
    "HostNotFound",
    "ItemsDiscarded",
    "Place",
    "Process",
    "RemoteError",
    "RemoteExec",
    "Ssh",
    "StateError",
    "Subinterpreter",
    "SubinterpreterEngine",
    "Thread",
    "ThreadEngine",
    "WorkerGone",
    "WorkerInfo",
    "__version__",
    "can_send",
    "open_group",
    "sync",
    "use_engine",
]
