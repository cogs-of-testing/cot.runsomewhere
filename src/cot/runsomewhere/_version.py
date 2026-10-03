from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version

try:
    version = _distribution_version("cot.runsomewhere")
except PackageNotFoundError:  # pragma: no cover - running from a bare checkout
    version = "0.0.0"
