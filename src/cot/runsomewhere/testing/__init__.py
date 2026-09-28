"""Test helpers: run a worker as tasks in the test's own event loop.

``from cot.runsomewhere import testing as rsht``
"""

from ._inloop import InLoop
from ._pipe import Pipe

__all__ = ["InLoop", "Pipe"]
