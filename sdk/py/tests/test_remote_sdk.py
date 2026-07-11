from __future__ import annotations

import inspect

import pytest

from vmon._function_proto import value_from_proto, value_to_proto
from vmon.remote import FunctionCall, RemoteFunction, _invocation, function
from vmon.values import ValueCodec, decode_value, encode_value


def test_decorator_preserves_local_signature_and_result() -> None:
    @function
    def add(left: int, right: int = 1) -> int:
        return left + right

    assert inspect.signature(add) == inspect.signature(add._function)
    assert add(2, right=3) == 5
    assert add.local(4) == 5


def test_from_name_and_with_options_are_immutable() -> None:
    deployed = RemoteFunction.from_name("resize", namespace="images", revision="r1", client=object())
    changed = deployed.with_options(deployed.options)
    assert changed is not deployed
    assert (changed.namespace, changed.name, changed.revision) == ("images", "resize", "r1")


def test_function_call_id_validation() -> None:
    with pytest.raises(ValueError, match="call_id"):
        FunctionCall.from_id("", client=object())
    call = FunctionCall.from_id("call-1", client=object())
    assert call.id == "call-1"


@pytest.mark.parametrize("codec", [ValueCodec.JSON, ValueCodec.CBOR])
def test_portable_value_protobuf_round_trip(codec: ValueCodec) -> None:
    envelope = encode_value({"ok": [1, 2, 3]}, codec=codec, compress=False)
    restored = value_from_proto(value_to_proto(envelope))
    assert decode_value(restored) == {"ok": [1, 2, 3]}


def test_cloudpickle_remains_explicitly_trusted() -> None:
    envelope = encode_value({1, 2}, codec=ValueCodec.CLOUDPICKLE, compress=False)
    restored = value_from_proto(value_to_proto(envelope))
    with pytest.raises(PermissionError, match="trusted"):
        decode_value(restored)
    assert decode_value(restored, trusted=True) == {1, 2}

def test_python_invocation_abi_is_versioned_and_collision_safe() -> None:
    ordinary = {"args": [1], "kwargs": {"named": 2}}
    assert _invocation((ordinary,), {}) == {
        "__vmon_python_call__": 1,
        "args": [ordinary],
        "kwargs": {},
    }
    assert _invocation((), {}) == {
        "__vmon_python_call__": 1,
        "args": [],
        "kwargs": {},
    }
    assert _invocation((1, 2), {"named": 3}) == {
        "__vmon_python_call__": 1,
        "args": [1, 2],
        "kwargs": {"named": 3},
    }
