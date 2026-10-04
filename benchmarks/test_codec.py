"""What the value codec costs, per message shape.

Run with ``nox -s bench``; ``-- --benchmark-compare`` compares with the last
saved run.
"""

import pytest

from cot.runsomewhere._values import DecodeError, py_decode, py_encode

try:
    import _cot_runsomewhere_speedups as speedups
except ImportError:
    speedups = None

REPORT = (
    "runtest_logreport",
    {
        "data": {
            "nodeid": "testing/test_foo.py::TestBar::test_baz[param-3]",
            "location": ("testing/test_foo.py", 41, "TestBar.test_baz[param-3]"),
            "keywords": {
                "test_baz[param-3]": 1,
                "TestBar": 1,
                "test_foo.py": 1,
                "testing": 1,
                "parametrize": 1,
            },
            "outcome": "passed",
            "longrepr": None,
            "when": "call",
            "user_properties": [],
            "sections": [],
            "duration": 0.0012,
            "start": 1759.5,
            "stop": 1759.6,
            "$report_type": "TestReport",
            "item_index": 123,
            "worker_id": "gw3",
            "testrun_uid": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
        }
    },
)


class Reference:
    """Stands in for a channel: something only a hook can encode."""

    def __init__(self, id):
        self.id = id


def reference(value):
    return 0, value.id


MESSAGES = {
    "request": {"op": "status", "id": 7},
    "xdist-report": REPORT,
    "100-reports": [REPORT] * 100,
    "1000-ints": list(range(-200, 800)),
    "64KiB-bytes": b"x" * 65536,
    "report-with-3-references": {
        "logs": Reference(2),
        "results": Reference(4),
        "control": Reference(6),
        "report": REPORT,
    },
}


@pytest.fixture(params=["python", "c"])
def codec(request):
    """(encode, decode) of one implementation; C where speedups is installed."""
    if request.param == "python":
        return py_encode, py_decode
    if speedups is None:
        pytest.skip("cot-runsomewhere-speedups is not installed")
    return speedups.encode, lambda data, hook: speedups.decode(data, hook, DecodeError)


@pytest.mark.parametrize("name", MESSAGES)
def test_encode(benchmark, codec, name):
    encode, _ = codec
    benchmark.group = f"encode {name}"
    benchmark.extra_info["bytes"] = len(encode(MESSAGES[name], reference))
    benchmark(encode, MESSAGES[name], reference)


@pytest.mark.parametrize("name", MESSAGES)
def test_decode(benchmark, codec, name):
    encode, decode = codec
    benchmark.group = f"decode {name}"
    data = encode(MESSAGES[name], reference)
    benchmark(decode, data, lambda _code, id: Reference(id))
