from __future__ import annotations

import inspect
import threading
from types import SimpleNamespace

import pytest

from vmon._function_proto import value_from_proto, value_to_proto
from vmon.remote import BatchCall, FunctionCall, RemoteFunction, _invocation, function
from vmon.v1 import api_pb2
from vmon.values import ValueCodec, decode_value, encode_value


def test_decorator_preserves_local_signature_and_result() -> None:
    @function
    def add(left: int, right: int = 1) -> int:
        return left + right

    assert inspect.signature(add) == inspect.signature(add._function)
    assert add(2, right=3) == 5
    assert add.local(4) == 5


def test_from_name_and_with_options_are_immutable() -> None:
    deployed = RemoteFunction.from_name(
        "resize", namespace="images", revision="r1", client=object()
    )
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


def test_reconstructed_call_pins_owner_when_preferred_endpoint_changes() -> None:
    class Calls:
        def __init__(self, owner: str) -> None:
            self.owner = owner
            self.cancelled = False

        def Get(self, request):
            if self.owner != "a":
                raise AssertionError("wrong endpoint")
            return api_pb2.CallRecord(ref=request, status=api_pb2.CALL_STATUS_RUNNING)

        def Cancel(self, request):
            self.cancelled = True
            return api_pb2.CallRecord(ref=request.call, status=api_pb2.CALL_STATUS_CANCELLED)

    class Driver:
        def __init__(self) -> None:
            self.preferred = "a"
            self.calls = {"a": Calls("a"), "b": Calls("b")}

        def call(self, operation, *, endpoint=None, stream=False):
            selected = endpoint or self.preferred
            return operation(SimpleNamespace(calls=self.calls[selected])), selected

    driver = Driver()
    call = FunctionCall.from_id("durable", client=SimpleNamespace(driver=driver))
    driver.preferred = "b"
    call.cancel()
    assert driver.calls["a"].cancelled
    assert not driver.calls["b"].cancelled


def test_batch_terminal_cancellation_is_not_silently_discarded() -> None:
    class Calls:
        def ListResults(self, request):
            return api_pb2.ListCallResultsResponse(next_cursor=request.cursor, end=True)

        def Get(self, request):
            return api_pb2.CallRecord(ref=request, status=api_pb2.CALL_STATUS_CANCELLED)

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=Calls())), "owner"

    batch = BatchCall.from_id("cancelled", client=SimpleNamespace(driver=Driver()))
    with pytest.raises(Exception, match="cancel"):
        list(batch.results())
    values = list(batch.results(return_exceptions=True))
    assert len(values) == 1
    assert isinstance(values[0], Exception)


def test_sync_batch_producer_failure_durably_cancels_call() -> None:
    cancelled = threading.Event()

    class Calls:
        def Create(self, request):
            return api_pb2.CallRecord(ref=api_pb2.CallRef(call_id="batch"))

        def StreamInputs(self, requests):
            def responses():
                for index, _ in enumerate(requests):
                    yield api_pb2.StreamCallInputsResponse(
                        call=api_pb2.CallRef(call_id="batch"),
                        committed_input_count=index,
                    )

            return responses()

        def Cancel(self, request):
            cancelled.set()
            return api_pb2.CallRecord(ref=request.call, status=api_pb2.CALL_STATUS_CANCELLED)

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=Calls())), "owner"

    function = RemoteFunction.from_name(
        "work", revision="r1", client=SimpleNamespace(driver=Driver())
    )
    function._registered = api_pb2.FunctionRevision(
        ref=api_pb2.RevisionRef(
            function=api_pb2.FunctionRef(namespace="default", name="work"),
            revision_id="r1",
        )
    )

    def failing():
        yield 1
        raise RuntimeError("producer failed")

    function.spawn_map(failing())
    assert cancelled.wait(1)
