from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from vmon.remote import FunctionCall, _AsyncCall
from vmon.v1 import api_pb2


class _NeverEndingWatch:
    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(60)
        raise StopAsyncIteration


class _Calls:
    def __init__(self) -> None:
        self.cancelled = asyncio.Event()

    def Watch(self, request, **kwargs):
        return _NeverEndingWatch()

    async def Cancel(self, request, **kwargs):
        self.cancelled.set()
        return api_pb2.CallRecord(ref=request.call, status=api_pb2.CALL_STATUS_CANCELLED)


class _Driver:
    def __init__(self, calls: _Calls) -> None:
        self.stubs = SimpleNamespace(calls=calls)

    def aio_endpoint(self):
        return self.stubs, (), 1.0


@pytest.mark.asyncio
async def test_native_aio_task_cancellation_is_durably_forwarded() -> None:
    calls = _Calls()
    client = SimpleNamespace(driver=_Driver(calls))
    handle = _AsyncCall(FunctionCall("call-aio", client))
    task = asyncio.create_task(handle.get())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls.cancelled.is_set()
