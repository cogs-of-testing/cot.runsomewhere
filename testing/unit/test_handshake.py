import pytest

from cot import runsomewhere as rsh
from cot.runsomewhere._handshake import Hello, check_peer


def hello(version="1.4.2", protocol=1, **overrides):
    fields = dict(
        protocol=protocol,
        version=version,
        python="3.12.9",
        platform="linux-x86_64",
        pid=4242,
        services=frozenset({"rsh.info", "rsh.via"}),
    )
    fields.update(overrides)
    return Hello(**fields)


def test_same_version_is_accepted():
    check_peer(local=hello(), remote=hello())


def test_patch_difference_is_accepted():
    check_peer(local=hello("1.4.2"), remote=hello("1.4.7"))


@pytest.mark.parametrize("remote", ["1.5.0", "2.4.2", "0.4.2"])
def test_major_or_minor_skew_is_refused_naming_both_versions(remote):
    with pytest.raises(rsh.HandshakeRefused) as excinfo:
        check_peer(local=hello("1.4.2"), remote=hello(remote))
    assert "1.4.2" in str(excinfo.value)
    assert remote in str(excinfo.value)


def test_protocol_skew_is_refused_even_with_equal_versions():
    with pytest.raises(rsh.HandshakeRefused, match="protocol"):
        check_peer(local=hello(protocol=1), remote=hello(protocol=2))


def test_refusal_is_a_connection_error():
    assert issubclass(rsh.HandshakeRefused, OSError)


def test_hello_roundtrips_through_the_value_codec():
    original = hello()
    assert Hello.from_value(original.to_value()) == original
    assert rsh.can_send(original.to_value())
