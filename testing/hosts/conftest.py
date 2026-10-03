import sys

import pytest

from cot import runsomewhere as rsh

# one engine of each kind for the whole run: each starts its host on first use
ENGINES = {"thread": rsh.ThreadEngine(), "subinterpreter": rsh.SubinterpreterEngine()}

HOSTS = [
    pytest.param("thread", id="thread"),
    pytest.param(
        "subinterpreter",
        id="subinterpreter",
        marks=pytest.mark.skipif(
            sys.version_info < (3, 14), reason="concurrent.interpreters needs 3.14"
        ),
    ),
]


@pytest.fixture(params=HOSTS)
def engine(request):
    return ENGINES[request.param]
