import sys

import pytest

from cot import runsomewhere as rsh

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
    if request.param == "thread":
        return rsh.ThreadEngine()
    return rsh.SubinterpreterEngine()
