import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere.compat import xspec


@pytest.mark.parametrize(
    ("spec", "place"),
    [
        ("popen", rsh.Process()),
        ("popen//python=3.12", rsh.Process(python="3.12")),
        (
            "popen//env:A=1//env:B=x=y",
            rsh.Process(env={"A": "1", "B": "x=y"}),
        ),
        ("ssh=buildbox//python=3.13", rsh.Ssh("buildbox", python="3.13")),
        (
            "python=/usr/bin/python3//ssh=ci@box",
            rsh.Ssh("ci@box", python="/usr/bin/python3"),
        ),
    ],
)
def test_specs_with_an_exact_equivalent_become_places(spec, place):
    assert xspec.parse(spec) == place


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ("popen//id=gw0", "'id'"),
        ("popen//chdir=/tmp", "'chdir'"),
        ("socket=127.0.0.1:8888", "'socket'"),
        ("popen//via=gw0", "'via'"),
        ("ssh=-p 2222 box", "ssh options"),
        ("ssh=box//env:A=1", "only read for popen"),
        ("popen//ssh=box", "two places"),
        ("python=3.12", "names no place"),
        ("popen//python=3.12//python=3.13", "twice"),
    ],
)
def test_anything_without_an_equivalent_is_refused_by_name(spec, message):
    with pytest.raises(ValueError, match=message):
        xspec.parse(spec)


def test_the_default_api_has_no_string_form_of_a_place():
    assert not hasattr(rsh, "parse_place")
