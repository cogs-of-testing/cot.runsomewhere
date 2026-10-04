"""The control channel's messages: an encoded dict with an ``op``, checked
against a table of the fields each op may carry."""

from __future__ import annotations

from typing import Any

#: any value the codec carries
ANY = object()

_HELLO = {
    "protocol": int,
    "version": str,
    "python": str,
    "platform": str,
    "pid": int,
    "services": frozenset,
    "executable": str,
}

#: op -> field -> type; every field is optional unless named in REQUIRED
SCHEMA: dict[str, dict[str, Any]] = {
    "hello": _HELLO,
    # the caller's configuration, and the worker's answer to it
    "config": {
        "config": dict,
        "ok": bool,
        "services": frozenset,
        "error": str,
    },
    "open": {"channel": int, "service": str, "params": dict},
    "close": {
        "channel": int,
        "ends": str,
        "result": ANY,
        "error": str,
        "type": str,
        "traceback": str,
    },
    "stop": {"channel": int, "deadline": (int, float)},
    "gateway-stop": {},
    "gateway-terminate": {"deadline": (int, float)},
}

REQUIRED: dict[str, frozenset[str]] = {
    "hello": frozenset(_HELLO) - {"executable"},
    "open": frozenset({"channel", "service", "params"}),
    "close": frozenset({"channel"}),
    "stop": frozenset({"channel"}),
}


class ProtocolError(ValueError):
    """A control message breaks the schema of an op this side knows."""


def message(op: str, **fields: Any) -> dict[str, Any]:
    return {"op": op, **fields}


def unknown(value: Any) -> str | None:
    """What in a control message this side does not know, if anything: an op
    or a field a newer peer may send. A known op with a field of the wrong
    type, or a message that is not one at all, raises ProtocolError."""
    if type(value) is not dict or type(value.get("op")) is not str:
        msg = f"not a control message: {value!r}"
        raise ProtocolError(msg)
    op = value["op"]
    fields = SCHEMA.get(op)
    if fields is None:
        return f"the op {op!r}"
    missing = REQUIRED.get(op, frozenset()) - value.keys()
    if missing:
        msg = f"{op} without {', '.join(sorted(missing))}"
        raise ProtocolError(msg)
    for name, field in value.items():
        if name == "op":
            continue
        kind = fields.get(name)
        if kind is None:
            return f"the field {name!r} of {op}"
        if not _fits(field, kind):
            msg = f"{op}.{name} is {type(field).__name__}, not {kind}"
            raise ProtocolError(msg)
    return None


def _fits(field: Any, kind: Any) -> bool:
    if kind is ANY:
        return True
    # bool is an int to isinstance, never to the protocol
    if type(field) is bool:
        return kind is bool
    return isinstance(field, kind)
