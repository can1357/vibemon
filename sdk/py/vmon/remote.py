"""Durable server-native function calls.

Calls are persisted before execution and are therefore at-least-once: an
attempt may run again after a worker failure.  Call IDs are stable and may be
reconstructed in another process with :meth:`FunctionCall.from_id`.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import os
import queue
import threading
import time
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator, Mapping
from dataclasses import replace
from typing import Any, Generic, ParamSpec, TypeVar, cast, overload
import grpc

from ._function_proto import (
    artifact_ref,
    digest,
    options_to_proto,
    package_to_proto,
    value_from_proto,
    value_to_proto,
)
from .options import (
    CpuArchitecture,
    FunctionOptions,
    FunctionVolumeMount,
    HighAvailabilityPolicy,
    NetworkPolicy,
    ResourcePolicy,
    RetryPolicy,
    SerializerPolicy,
    TimeoutPolicy,
    WorkerPolicy,
)
from .image import Image
from .package import PackageArtifact, SerializedCallable, package_callable
from .errors import APIError, RemoteFunctionError, TransportError
from .values import ValueCodec, ValueCompression, decode_value, encode_value
from .v1 import api_pb2 as pb

P = ParamSpec("P")
R = TypeVar("R")
Y = TypeVar("Y")
_UNSET = object()
_TERMINAL = {pb.CALL_STATUS_SUCCEEDED, pb.CALL_STATUS_FAILED, pb.CALL_STATUS_CANCELLED}


def is_remote() -> bool:
    """Return whether this process is executing inside a vmon worker."""
    return os.environ.get("VMON_REMOTE") == "1"


def _client(client: Any | None) -> Any:
    if client is not None:
        return client
    from .client import connect

    return connect()


def _call(
    client: Any,
    operation: Callable[[Any], Any],
    *,
    stream: bool = False,
    endpoint: str | None = None,
) -> Any:
    value = client.driver.call(operation, stream=stream, endpoint=endpoint)
    return value[0] if isinstance(value, tuple) and len(value) == 2 else value


def _call_owner(
    client: Any, operation: Callable[[Any], Any], *, endpoint: str | None = None
) -> tuple[Any, str | None]:
    value = client.driver.call(operation, endpoint=endpoint)
    return value if isinstance(value, tuple) and len(value) == 2 else (value, endpoint)


def _compose_function_options(
    options: FunctionOptions | None = None,
    *,
    image: Image | str | None = None,
    cpus: float | None = None,
    memory: int | None = None,
    disk: int | None = None,
    architecture: CpuArchitecture | str | None = None,
    ha: HighAvailabilityPolicy | str | None = None,
    volumes: tuple[FunctionVolumeMount, ...] | None = None,
    egress: Iterable[str] | None = None,
    network: NetworkPolicy | None = None,
    block_network: bool | None = None,
    egress_cidrs: Iterable[str] | None = None,
    egress_domains: Iterable[str] | None = None,
    inbound_cidrs: Iterable[str] | None = None,
    retries: RetryPolicy | int | None = None,
    startup_timeout: float | None = None,
    execution_timeout: float | None = None,
    queue_timeout: float | None = None,
    idle_timeout: float | None = None,
    result_ttl: float | None = None,
    min_workers: int | None = None,
    max_workers: int | None = None,
    buffer_workers: int | None = None,
    scale_down_after: float | None = None,
    max_outstanding_inputs: int | None = None,
    serializer: SerializerPolicy | ValueCodec | str | None = None,
) -> FunctionOptions:
    supplied = (
        image,
        cpus,
        memory,
        disk,
        architecture,
        ha,
        volumes,
        network,
        egress,
        block_network,
        egress_cidrs,
        egress_domains,
        inbound_cidrs,
        retries,
        startup_timeout,
        execution_timeout,
        queue_timeout,
        idle_timeout,
        result_ttl,
        min_workers,
        max_workers,
        buffer_workers,
        scale_down_after,
        max_outstanding_inputs,
        serializer,
    )
    network_parts = (egress, block_network, egress_cidrs, egress_domains, inbound_cidrs)
    if network is not None and any(value is not None for value in network_parts):
        raise TypeError("network cannot be combined with network shorthand settings")
    if egress is not None and any(value is not None for value in (egress_cidrs, egress_domains)):
        raise TypeError("egress cannot be combined with egress_cidrs/egress_domains")
    if options is not None and any(value is not None for value in supplied):
        raise TypeError("options cannot be combined with explicit function settings")
    if options is not None:
        return options
    base = FunctionOptions()
    resource = replace(
        base.resources,
        cpu_millis=base.resources.cpu_millis if cpus is None else int(cpus * 1000),
        memory_bytes=base.resources.memory_bytes if memory is None else memory,
        ephemeral_disk_bytes=base.resources.ephemeral_disk_bytes if disk is None else disk,
        architecture=base.resources.architecture
        if architecture is None
        else CpuArchitecture(architecture),
        high_availability=base.resources.high_availability
        if ha is None
        else HighAvailabilityPolicy(ha),
        network=(
            network
            if network is not None
            else NetworkPolicy(
                block_network=bool(block_network),
                egress_cidrs=(
                    tuple(egress_cidrs)
                    if egress_cidrs is not None
                    else tuple(value for value in egress or () if "/" in value)
                ),
                egress_domains=(
                    tuple(egress_domains)
                    if egress_domains is not None
                    else tuple(value for value in egress or () if "/" not in value)
                ),
                inbound_cidrs=tuple(inbound_cidrs or ()),
            )
        ),
        volume_mounts=base.resources.volume_mounts if volumes is None else volumes,
    )
    timeout = replace(
        base.timeouts,
        startup=base.timeouts.startup if startup_timeout is None else startup_timeout,
        execution=base.timeouts.execution if execution_timeout is None else execution_timeout,
        queue=base.timeouts.queue if queue_timeout is None else queue_timeout,
        result_ttl=base.timeouts.result_ttl if result_ttl is None else result_ttl,
    )
    worker_idle = scale_down_after if scale_down_after is not None else idle_timeout
    workers = replace(
        base.workers,
        idle_timeout=base.workers.idle_timeout if worker_idle is None else worker_idle,
        min_workers=base.workers.min_workers if min_workers is None else min_workers,
        max_workers=base.workers.max_workers if max_workers is None else max_workers,
        buffer_workers=base.workers.buffer_workers if buffer_workers is None else buffer_workers,
        max_outstanding_inputs=base.workers.max_outstanding_inputs
        if max_outstanding_inputs is None
        else max_outstanding_inputs,
    )
    retry = (
        base.retry
        if retries is None
        else RetryPolicy(max_attempts=retries + 1)
        if isinstance(retries, int)
        else retries
    )
    serialization = (
        base.serializer
        if serializer is None
        else serializer
        if isinstance(serializer, SerializerPolicy)
        else SerializerPolicy(
            input_serializer=ValueCodec(serializer),
            result_serializer=ValueCodec(serializer),
            allow_trusted_python=ValueCodec(serializer) is ValueCodec.CLOUDPICKLE,
        )
    )
    resolved_image = (
        base.image
        if image is None
        else image
        if isinstance(image, Image)
        else Image.from_registry(image)
    )
    return replace(
        base,
        image=resolved_image,
        resources=resource,
        retry=retry,
        timeouts=timeout,
        workers=workers,
        serializer=serialization,
    )


async def _run_blocking(function: Callable[..., R], *args: Any, **kwargs: Any) -> R:
    """Compatibility bridge for injected synchronous drivers."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(function, *args, **kwargs))


def _aio_endpoint(client: Any, endpoint: str | None = None) -> tuple[Any, Any, float] | None:
    provider = getattr(client.driver, "aio_endpoint", None)
    if provider is None:
        return None
    try:
        return provider(endpoint)
    except TypeError:
        return provider()


def _next_or_done(iterator: Iterator[Any]) -> tuple[bool, Any]:
    try:
        return True, next(iterator)
    except StopIteration:
        return False, None


def _raise(error: pb.CallError) -> None:
    exc = RemoteFunctionError(
        error.message or "remote function failed", code=error.code, remote_type=error.type
    )
    if error.frames:
        exc.add_note(
            "Remote traceback:\n"
            + "\n".join(f'  File "{f.file}", line {f.line}, in {f.function}' for f in error.frames)
        )
    raise exc


def _invocation(args: Iterable[Any], kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """Encode the versioned Python-only callable invocation ABI."""
    return {
        "__vmon_python_call__": 1,
        "args": list(args),
        "kwargs": dict(kwargs),
    }


def _result_value(client: Any, result: pb.CallResult, *, trusted: bool) -> Any:
    if result.WhichOneof("outcome") == "error":
        _raise(result.error)
    envelope = result.value
    data = None
    if envelope.WhichOneof("storage") == "artifact":
        chunks = _call(
            client,
            lambda s: s.artifacts.Get(pb.GetArtifactRequest(artifact=envelope.artifact)),
            stream=True,
        )
        data = b"".join(chunk.data for chunk in chunks)
    return decode_value(value_from_proto(envelope, data), trusted=trusted)


def _encode_input(
    value: object, codec: ValueCodec, *, trusted: bool, compression: Any = None
) -> tuple[pb.ValueEnvelope, bytes | None]:
    if codec is ValueCodec.CLOUDPICKLE and not trusted:
        raise PermissionError("cloudpickle calls require allow_trusted_python=True")
    envelope = encode_value(value, codec=codec, compression=compression)
    data = envelope.artifact.data if envelope.artifact else None
    return value_to_proto(envelope), data


def _upload(client: Any, data: bytes, sha256: str, media_type: str) -> pb.ArtifactRef:
    ref = artifact_ref(sha256, len(data))
    try:
        _call(client, lambda stubs: stubs.artifacts.Stat(ref))
        return ref
    except APIError as exc:
        if exc.code != "not_found" and exc.status != 404:
            raise

    def frames() -> Iterator[pb.PutArtifactRequest]:
        yield pb.PutArtifactRequest(
            header=pb.PutArtifactHeader(
                expected_digest=digest(sha256),
                expected_size_bytes=len(data),
                media_type=media_type,
            )
        )
        for offset in range(0, len(data), 256 * 1024):
            yield pb.PutArtifactRequest(data=data[offset : offset + 256 * 1024])

    record = _call(client, lambda stubs: stubs.artifacts.Put(frames()))
    return record.ref


def _arguments(
    client: Any,
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
    *,
    codec: ValueCodec,
    trusted: bool,
    compression: Any,
) -> pb.InvocationArguments:
    message = pb.InvocationArguments()
    for value in args:
        envelope, artifact = _encode_input(value, codec, trusted=trusted, compression=compression)
        if artifact is not None:
            ref = _upload(
                client,
                artifact,
                envelope.artifact.digest.value.hex(),
                "application/vnd.vmon.value",
            )
            envelope.artifact.CopyFrom(ref)
        message.positional.append(envelope)
    for name, value in kwargs.items():
        envelope, artifact = _encode_input(value, codec, trusted=trusted, compression=compression)
        if artifact is not None:
            ref = _upload(
                client,
                artifact,
                envelope.artifact.digest.value.hex(),
                "application/vnd.vmon.value",
            )
            envelope.artifact.CopyFrom(ref)
        message.named[name].CopyFrom(envelope)
    return message


async def _async_result_value(
    stubs: Any,
    metadata: Any,
    result: pb.CallResult,
    *,
    trusted: bool,
) -> Any:
    if result.WhichOneof("outcome") == "error":
        _raise(result.error)
    envelope = result.value
    data = None
    if envelope.WhichOneof("storage") == "artifact":
        chunks: list[bytes] = []
        async for chunk in stubs.artifacts.Get(
            pb.GetArtifactRequest(artifact=envelope.artifact),
            metadata=metadata,
        ):
            chunks.append(chunk.data)
        data = b"".join(chunks)
    return decode_value(value_from_proto(envelope, data), trusted=trusted)


async def _async_upload(
    stubs: Any,
    metadata: Any,
    deadline: float,
    data: bytes,
    sha256: str,
    media_type: str,
) -> pb.ArtifactRef:
    ref = artifact_ref(sha256)
    try:
        await stubs.artifacts.Stat(ref, metadata=metadata, timeout=deadline)
        return ref
    except grpc.aio.AioRpcError as exc:
        if exc.code() is not grpc.StatusCode.NOT_FOUND:
            raise

    async def frames() -> AsyncIterator[pb.PutArtifactRequest]:
        yield pb.PutArtifactRequest(
            header=pb.PutArtifactHeader(
                expected_digest=digest(sha256),
                expected_size_bytes=len(data),
                media_type=media_type,
            )
        )
        for offset in range(0, len(data), 256 * 1024):
            yield pb.PutArtifactRequest(data=data[offset : offset + 256 * 1024])

    record = await stubs.artifacts.Put(frames(), metadata=metadata, timeout=deadline)
    return record.ref


async def _async_arguments(
    stubs: Any,
    metadata: Any,
    deadline: float,
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
    *,
    codec: ValueCodec,
    trusted: bool,
    compression: Any,
) -> pb.InvocationArguments:
    message = pb.InvocationArguments()
    for name, value in [
        *((None, value) for value in args),
        *((key, value) for key, value in kwargs.items()),
    ]:
        envelope, artifact = _encode_input(value, codec, trusted=trusted, compression=compression)
        if artifact is not None:
            ref = await _async_upload(
                stubs,
                metadata,
                deadline,
                artifact,
                envelope.artifact.digest.value.hex(),
                "application/vnd.vmon.value",
            )
            envelope.artifact.CopyFrom(ref)
        if name is None:
            message.positional.append(envelope)
        else:
            message.named[name].CopyFrom(envelope)
    return message


class FunctionCall(Generic[R]):
    """A durable unary or generator call handle."""

    def __init__(
        self,
        call_id: str,
        client: Any,
        *,
        trusted: bool = False,
        generator: bool = False,
        endpoint: str | None = None,
    ) -> None:
        self.call_id = call_id
        self._client = client
        self._trusted = trusted
        self._generator = generator
        self._endpoint = endpoint

    @property
    def id(self) -> str:
        """Return the stable durable call identifier."""
        return self.call_id

    @classmethod
    def from_id(
        cls, call_id: str, *, client: Any | None = None, trusted: bool = False
    ) -> "FunctionCall[Any]":
        if not call_id:
            raise ValueError("call_id must be non-empty")
        bound_client = _client(client)
        if not hasattr(bound_client, "driver"):
            return cls(call_id, bound_client, trusted=trusted)
        record, endpoint = _call_owner(
            bound_client,
            lambda stubs: stubs.calls.Get(pb.CallRef(call_id=call_id)),
        )
        return cls(call_id, bound_client, trusted=trusted, endpoint=endpoint)

    @property
    def aio(self) -> "_AsyncCall[R]":
        """Return the native asynchronous facade for this durable call."""
        return _AsyncCall(self)

    def status(self) -> int:
        return self.get_record().status

    def done(self) -> bool:
        """Return whether the durable call has reached a terminal state."""
        return self.status() in _TERMINAL

    def get_record(self) -> pb.CallRecord:
        return _call(
            self._client,
            lambda stubs: stubs.calls.Get(pb.CallRef(call_id=self.call_id)),
            endpoint=self._endpoint,
        )

    def stats(self) -> pb.CallStats:
        return self.get_record().stats

    def graph(self) -> pb.CallGraph:
        return self.get_record().graph

    def cancel(self, reason: str = "cancelled by client") -> pb.CallRecord:
        return _call(
            self._client,
            lambda stubs: stubs.calls.Cancel(
                pb.CancelCallRequest(
                    call=pb.CallRef(call_id=self.call_id),
                    reason=reason,
                    request_id=str(uuid.uuid4()),
                )
            ),
            endpoint=self._endpoint,
        )

    def events(self, *, after_sequence: int = 0, follow: bool = True) -> Iterator[pb.CallEvent]:
        cursor = after_sequence
        failures = 0
        while True:
            try:
                stream = _call(
                    self._client,
                    lambda stubs: stubs.calls.Watch(
                        pb.WatchCallRequest(
                            cursor=pb.EventCursor(
                                call=pb.CallRef(call_id=self.call_id),
                                after_sequence=cursor,
                            ),
                            follow=follow,
                        )
                    ),
                    stream=True,
                    endpoint=self._endpoint,
                )
                failures = 0
                for event in stream:
                    if event.sequence <= cursor:
                        continue
                    cursor = event.sequence
                    yield event
                if not follow or self.status() in _TERMINAL:
                    return
            except TransportError:
                failures += 1
                if failures > 5:
                    raise
                time.sleep(min(0.05 * (2 ** (failures - 1)), 1.0))

    def logs(self, *, after_sequence: int = 0, follow: bool = True) -> Iterator[bytes]:
        for event in self.events(after_sequence=after_sequence, follow=follow):
            if event.WhichOneof("payload") == "log":
                yield event.log.data

    def iter_yields(self, *, after_sequence: int = 0) -> Iterator[R]:
        for event in self.events(after_sequence=after_sequence):
            if event.WhichOneof("payload") == "yield_result":
                yield cast(
                    R, _result_value(self._client, event.yield_result, trusted=self._trusted)
                )

    def get(self, timeout: float | None = None) -> R:
        """Wait locally up to ``timeout`` without changing server execution."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            record = self.get_record()
            if record.status in _TERMINAL:
                if record.HasField("error"):
                    _raise(record.error)
                if record.status != pb.CALL_STATUS_SUCCEEDED:
                    raise RemoteFunctionError(
                        f"remote call ended with status {record.status}",
                        code="cancelled",
                    )
                item = _call(
                    self._client,
                    lambda stubs: stubs.calls.GetResult(
                        pb.GetCallResultRequest(call=record.ref, index=0)
                    ),
                )
                return cast(R, _result_value(self._client, item, trusted=self._trusted))
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"call {self.call_id} did not complete within {timeout} seconds")
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            time.sleep(min(0.05, remaining) if remaining is not None else 0.05)

    @staticmethod
    def gather(*calls: "FunctionCall[Any]", return_exceptions: bool = False) -> list[Any]:
        values = []
        for call in calls:
            try:
                values.append(call.get())
            except Exception as exc:
                if not return_exceptions:
                    raise
                values.append(exc)
        return values


class BatchCall(Generic[R]):
    """Detached durable batch handle whose results are fetched lazily."""

    def __init__(
        self,
        call: FunctionCall[Any],
        input_count: int | None = None,
        *,
        feeder_stop: threading.Event | None = None,
        feeder_errors: list[BaseException] | None = None,
    ) -> None:
        self._call = call
        self.input_count = input_count
        self._feeder_stop = feeder_stop
        self._feeder_errors = feeder_errors if feeder_errors is not None else []

    @classmethod
    def from_id(
        cls, call_id: str, *, client: Any | None = None, trusted: bool = False
    ) -> "BatchCall[Any]":
        """Reconstruct a detached durable batch handle."""
        return cls(FunctionCall.from_id(call_id, client=client, trusted=trusted))

    @property
    def id(self) -> str:
        return self._call.id

    def status(self) -> int:
        return self._call.status()

    def done(self) -> bool:
        return self._call.done()

    def cancel(self, reason: str = "cancelled by client") -> pb.CallRecord:
        if self._feeder_stop is not None:
            self._feeder_stop.set()
        return self._call.cancel(reason)

    @property
    def aio(self) -> "_AsyncCall[Any]":
        """Return the native asynchronous facade for this durable batch."""
        return _AsyncCall(self._call)

    def results(
        self,
        *,
        order_outputs: bool = True,
        return_exceptions: bool = False,
    ) -> Iterator[R]:
        pending: dict[int, Any] = {}
        next_index = 0
        after_sequence = 0
        try:
            while True:
                page = _call(
                    self._call._client,
                    lambda stubs: stubs.calls.ListResults(
                        pb.ListCallResultsRequest(
                            cursor=pb.ResultCursor(
                                call=pb.CallRef(call_id=self.id),
                                after_sequence=after_sequence,
                            ),
                            page_size=128,
                        )
                    ),
                    endpoint=self._call._endpoint,
                )
                for item in page.results:
                    after_sequence = max(after_sequence, item.sequence)
                    try:
                        value = _result_value(self._call._client, item, trusted=self._call._trusted)
                    except Exception as exc:
                        if not return_exceptions:
                            raise
                        value = exc
                    if not order_outputs:
                        yield value
                        continue
                    pending[item.input_index] = value
                    while next_index in pending:
                        yield pending.pop(next_index)
                        next_index += 1
                if self._feeder_errors:
                    raise self._feeder_errors[0]
                if page.end and self.done():
                    record = self._call.get_record()
                    failure: BaseException | None = None
                    if record.HasField("error"):
                        failure = RemoteFunctionError(
                            record.error.message or "remote batch failed",
                            code=record.error.code,
                            remote_type=record.error.type,
                        )
                    elif record.status == pb.CALL_STATUS_CANCELLED:
                        failure = RemoteFunctionError(
                            "remote batch was cancelled", code="cancelled"
                        )
                    elif record.status != pb.CALL_STATUS_SUCCEEDED:
                        failure = RemoteFunctionError(
                            f"remote batch ended with status {record.status}",
                            code="remote_error",
                        )
                    if failure is not None:
                        if return_exceptions:
                            yield cast(R, failure)
                            return
                        raise failure
                    return
                time.sleep(0.02)
        except GeneratorExit:
            self.cancel("batch result consumer closed")
            raise

    def get(self, **kwargs: Any) -> Iterator[R]:
        return self.results(**kwargs)


class _AsyncCall(Generic[R]):
    def __init__(self, call: FunctionCall[R]) -> None:
        self._call = call

    async def cancel(self, reason: str = "cancelled by client") -> pb.CallRecord:
        endpoint = _aio_endpoint(self._call._client, self._call._endpoint)
        if endpoint is None:
            return await _run_blocking(self._call.cancel, reason)
        stubs, metadata, deadline = endpoint
        return await stubs.calls.Cancel(
            pb.CancelCallRequest(
                call=pb.CallRef(call_id=self._call.id),
                reason=reason,
                request_id=str(uuid.uuid4()),
            ),
            metadata=metadata,
            timeout=deadline,
        )

    async def get(self, timeout: float | None = None) -> R:
        endpoint = _aio_endpoint(self._call._client, self._call._endpoint)
        if endpoint is None:
            task = asyncio.create_task(_run_blocking(self._call.get, timeout))
            try:
                return await task
            except asyncio.CancelledError:
                await asyncio.shield(self.cancel("async task cancelled"))
                raise
        stubs, metadata, deadline = endpoint

        async def wait() -> R:
            cursor = 0
            while True:
                stream = stubs.calls.Watch(
                    pb.WatchCallRequest(
                        cursor=pb.EventCursor(
                            call=pb.CallRef(call_id=self._call.id),
                            after_sequence=cursor,
                        ),
                        follow=True,
                    ),
                    metadata=metadata,
                )
                async for event in stream:
                    cursor = max(cursor, event.sequence)
                    kind = event.WhichOneof("payload")
                    if kind == "result":
                        return cast(
                            R,
                            await _async_result_value(
                                stubs,
                                metadata,
                                event.result,
                                trusted=self._call._trusted,
                            ),
                        )
                    if kind == "error":
                        _raise(event.error)
                record = await stubs.calls.Get(
                    pb.CallRef(call_id=self._call.id),
                    metadata=metadata,
                    timeout=deadline,
                )
                if record.status in _TERMINAL:
                    if record.HasField("error"):
                        _raise(record.error)
                    result = await stubs.calls.GetResult(
                        pb.GetCallResultRequest(call=record.ref, index=0),
                        metadata=metadata,
                        timeout=deadline,
                    )
                    return cast(
                        R,
                        await _async_result_value(
                            stubs,
                            metadata,
                            result,
                            trusted=self._call._trusted,
                        ),
                    )

        try:
            if timeout is None:
                return await wait()
            async with asyncio.timeout(timeout):
                return await wait()
        except TimeoutError:
            raise TimeoutError(
                f"call {self._call.id} did not complete within {timeout} seconds"
            ) from None
        except asyncio.CancelledError:
            await asyncio.shield(self.cancel("async task cancelled"))
            raise


class _AsyncRemoteFunction(Generic[P, R]):
    def __init__(self, function: "RemoteFunction[P, R]") -> None:
        self._function = function

    async def spawn(self, *args: P.args, **kwargs: P.kwargs) -> _AsyncCall[R]:
        function = self._function
        if function._signature:
            function._signature.bind(*args, **kwargs)
        client = _client(function._client)
        function._client = client
        endpoint = _aio_endpoint(client, function._endpoint)
        if endpoint is None:
            return _AsyncCall(await _run_blocking(function.spawn, *args, **kwargs))
        stubs, metadata, deadline = endpoint
        revision = await function._resolve_async(stubs, metadata, deadline)
        trusted = function.options.serializer.allow_trusted_python
        arguments = await _async_arguments(
            stubs,
            metadata,
            deadline,
            args,
            kwargs,
            codec=function.options.serializer.input_serializer,
            trusted=trusted,
            compression=function.options.serializer.compression,
        )
        record = await stubs.calls.Create(
            pb.CreateCallRequest(
                type=pb.CALL_TYPE_GENERATOR if function._is_generator else pb.CALL_TYPE_UNARY,
                target=pb.CallTarget(function=revision.ref),
                inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
                inputs_closed=True,
                request_id=str(uuid.uuid4()),
            ),
            metadata=metadata,
            timeout=deadline,
        )
        return _AsyncCall(
            FunctionCall(
                record.ref.call_id,
                client,
                trusted=trusted,
                generator=function._is_generator,
            )
        )

    async def remote(self, *args: P.args, **kwargs: P.kwargs) -> R:
        call = await self.spawn(*args, **kwargs)
        return await call.get()

    @overload
    def remote_gen(
        self: "_AsyncRemoteFunction[P, Iterator[Y]]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> AsyncIterator[Y]: ...

    @overload
    def remote_gen(self, *args: P.args, **kwargs: P.kwargs) -> AsyncIterator[Any]: ...

    async def remote_gen(self, *args: P.args, **kwargs: P.kwargs) -> AsyncIterator[Any]:
        async_call = await self.spawn(*args, **kwargs)
        call = async_call._call
        endpoint = _aio_endpoint(call._client, call._endpoint)
        if endpoint is None:
            iterator = call.iter_yields()
            while True:
                present, value = await _run_blocking(_next_or_done, iterator)
                if not present:
                    return
                yield value
            return
        stubs, metadata, _ = endpoint
        cursor = 0
        try:
            while True:
                stream = stubs.calls.Watch(
                    pb.WatchCallRequest(
                        cursor=pb.EventCursor(
                            call=pb.CallRef(call_id=call.id),
                            after_sequence=cursor,
                        ),
                        follow=True,
                    ),
                    metadata=metadata,
                )
                async for event in stream:
                    cursor = max(cursor, event.sequence)
                    kind = event.WhichOneof("payload")
                    if kind == "yield_result":
                        yield await _async_result_value(
                            stubs,
                            metadata,
                            event.yield_result,
                            trusted=call._trusted,
                        )
                    elif kind == "error":
                        _raise(event.error)
                    elif kind == "status" and event.status.status in _TERMINAL:
                        return
        except asyncio.CancelledError:
            await asyncio.shield(async_call.cancel("async generator cancelled"))
            raise

    async def map(
        self, iterable: Iterable[Any] | AsyncIterable[Any], **kwargs: Any
    ) -> AsyncIterator[R]:
        function = self._function
        client = _client(function._client)
        function._client = client
        endpoint = _aio_endpoint(client, function._endpoint)
        if endpoint is None:
            batch = await _run_blocking(function.spawn_map, iterable, **kwargs)
            iterator = batch.results(
                order_outputs=kwargs.get("order_outputs", True),
                return_exceptions=kwargs.get("return_exceptions", False),
            )
            while True:
                present, value = await _run_blocking(_next_or_done, iterator)
                if not present:
                    return
                yield value
            return
        stubs, metadata, deadline = endpoint
        revision = await function._resolve_async(stubs, metadata, deadline)
        record = await stubs.calls.Create(
            pb.CreateCallRequest(
                type=pb.CALL_TYPE_BATCH,
                target=pb.CallTarget(function=revision.ref),
                inputs_closed=False,
                request_id=str(uuid.uuid4()),
            ),
            metadata=metadata,
            timeout=deadline,
        )
        call: _AsyncCall[Any] = _AsyncCall(
            FunctionCall(
                record.ref.call_id,
                client,
                trusted=function.options.serializer.allow_trusted_python,
            )
        )
        count = 0

        async def inputs() -> AsyncIterator[pb.StreamCallInputsRequest]:
            nonlocal count
            yield pb.StreamCallInputsRequest(call=record.ref)
            if hasattr(iterable, "__aiter__"):
                source = cast(AsyncIterable[Any], iterable)
            else:
                assert isinstance(iterable, Iterable)

                async def sync_source() -> AsyncIterator[Any]:
                    for value in iterable:
                        yield value

                source = sync_source()
            async for item in source:
                args = (item,)
                if function._signature:
                    function._signature.bind(*args)
                arguments = await _async_arguments(
                    stubs,
                    metadata,
                    deadline,
                    args,
                    {},
                    codec=function.options.serializer.input_serializer,
                    trusted=function.options.serializer.allow_trusted_python,
                    compression=function.options.serializer.compression,
                )
                yield pb.StreamCallInputsRequest(
                    input=pb.CallInput(
                        index=count,
                        input_id=str(uuid.uuid4()),
                        arguments=arguments,
                    )
                )
                count += 1

        async def feed() -> None:
            try:
                async for _ in stubs.calls.StreamInputs(
                    inputs(), metadata=metadata, timeout=deadline
                ):
                    pass
                await stubs.calls.CloseInputs(
                    pb.CloseCallInputsRequest(call=record.ref, expected_input_count=count),
                    metadata=metadata,
                    timeout=deadline,
                )
            except BaseException:
                await asyncio.shield(call.cancel("async batch input feeder failed"))
                raise

        feeder = asyncio.create_task(feed())
        cursor = 0
        pending: dict[int, Any] = {}
        next_index = 0
        ordered = kwargs.get("order_outputs", True)
        return_exceptions = kwargs.get("return_exceptions", False)
        try:
            while True:
                stream = stubs.calls.Watch(
                    pb.WatchCallRequest(
                        cursor=pb.EventCursor(call=record.ref, after_sequence=cursor),
                        follow=True,
                    ),
                    metadata=metadata,
                )
                async for event in stream:
                    cursor = max(cursor, event.sequence)
                    kind = event.WhichOneof("payload")
                    if kind == "result":
                        result = event.result
                        try:
                            value = await _async_result_value(
                                stubs,
                                metadata,
                                result,
                                trusted=function.options.serializer.allow_trusted_python,
                            )
                        except Exception as exc:
                            if not return_exceptions:
                                raise
                            value = exc
                        if not ordered:
                            yield value
                        else:
                            index = result.input_index if result.input_id else result.index
                            pending[index] = value
                            while next_index in pending:
                                yield pending.pop(next_index)
                                next_index += 1
                    elif kind == "error":
                        _raise(event.error)
                    elif kind == "status" and event.status.status in _TERMINAL:
                        await feeder
                        return
        except asyncio.CancelledError, GeneratorExit:
            feeder.cancel()
            await asyncio.shield(call.cancel("async map consumer closed"))
            raise


class RemoteFunction(Generic[P, R]):
    """Immutable typed reference to a zero-deploy or registered function."""

    def __init__(
        self,
        function: Callable[P, R] | None = None,
        *,
        client: Any | None = None,
        namespace: str = "default",
        name: str | None = None,
        revision: str | None = None,
        options: FunctionOptions | None = None,
        generator: bool = False,
        package_mode: str = "package",
        package_root: str | None = None,
        include: Iterable[str] = (),
        exclude: Iterable[str] = (),
        local_packages: Iterable[str] = (),
    ) -> None:
        if function is None and not name:
            raise ValueError("name is required for deployed function lookup")
        self._function, self._client = function, client
        self.namespace, self.name, self.revision = (
            namespace,
            name or (function.__name__ if function is not None else ""),
            revision,
        )
        attached_options = getattr(function, "__vmon_options__", None) if function else None
        self.options = options or attached_options or FunctionOptions()
        if not isinstance(self.options, FunctionOptions):
            raise TypeError("__vmon_options__ must be FunctionOptions")
        if package_mode == "cloudpickle":
            if options is not None and not self.options.serializer.allow_trusted_python:
                raise ValueError(
                    "cloudpickle package mode conflicts with allow_trusted_python=False"
                )
            self.options = replace(
                self.options,
                serializer=replace(self.options.serializer, allow_trusted_python=True),
            )
        if not isinstance(self.options, FunctionOptions):
            raise TypeError("__vmon_options__ must be FunctionOptions")
        self.package_mode, self.package_root = package_mode, package_root
        self.include, self.exclude, self.local_packages = (
            tuple(include),
            tuple(exclude),
            tuple(local_packages),
        )
        self._registered: pb.FunctionRevision | None = None
        self._endpoint: str | None = None
        self._lock = threading.Lock()
        self._signature = inspect.signature(function) if function else None
        self._is_generator = bool(
            generator
            or function
            and (inspect.isgeneratorfunction(function) or inspect.isasyncgenfunction(function))
        )
        if function:
            functools.update_wrapper(self, function)

    @classmethod
    def from_name(
        cls,
        name: str,
        *,
        namespace: str = "default",
        revision: str | None = None,
        client: Any | None = None,
        options: FunctionOptions | None = None,
        generator: bool = False,
    ) -> "RemoteFunction[..., Any]":
        return cls(
            None,
            name=name,
            namespace=namespace,
            revision=revision,
            client=client,
            options=options,
            generator=generator,
        )

    def with_options(
        self, options: FunctionOptions | None = None, **changes: Any
    ) -> "RemoteFunction[P, R]":
        updated = options or replace(self.options, **changes)
        return type(self)(
            self._function,
            client=self._client,
            namespace=self.namespace,
            name=self.name,
            revision=self.revision,
            options=updated,
            generator=self._is_generator,
            package_mode=self.package_mode,
            package_root=self.package_root,
            include=self.include,
            exclude=self.exclude,
            local_packages=self.local_packages,
        )

    def _adopt_revision_options(self, revision: pb.FunctionRevision) -> None:
        if self._function is not None:
            return
        spec = revision.spec.serializer
        codecs = {
            pb.VALUE_SERIALIZER_JSON: ValueCodec.JSON,
            pb.VALUE_SERIALIZER_CBOR: ValueCodec.CBOR,
            pb.VALUE_SERIALIZER_CLOUDPICKLE: ValueCodec.CLOUDPICKLE,
        }
        compressions = {
            pb.VALUE_COMPRESSION_NONE: ValueCompression.NONE,
            pb.VALUE_COMPRESSION_GZIP: ValueCompression.GZIP,
            pb.VALUE_COMPRESSION_ZSTD: ValueCompression.ZSTD,
        }
        self.options = replace(
            self.options,
            serializer=SerializerPolicy(
                input_serializer=codecs[spec.input_serializer],
                result_serializer=codecs[spec.result_serializer],
                compression=compressions[spec.compression],
                allow_trusted_python=spec.allow_trusted_python,
            ),
        )

    @property
    def aio(self) -> _AsyncRemoteFunction[P, R]:
        return _AsyncRemoteFunction(self)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._function is None:
            raise TypeError("deployed function references cannot run locally")
        return self._function(*args, **kwargs)

    def local(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self(*args, **kwargs)

    def _make_spec(
        self, package: PackageArtifact | SerializedCallable, source: pb.ArtifactRef
    ) -> pb.FunctionSpec:
        fields = options_to_proto(self.options)
        lifecycle_name = getattr(self, "__vmon_class_lifecycle__", None)
        metadata = getattr(self, "__vmon_lifecycle_metadata__", None)
        lifecycle = (
            pb.FUNCTION_LIFECYCLE_ACTOR
            if lifecycle_name == "actor"
            else pb.FUNCTION_LIFECYCLE_INSTANCE
            if lifecycle_name == "service"
            else pb.FUNCTION_LIFECYCLE_STATELESS
        )
        hooks = pb.LifecycleHooks()
        if metadata is not None and lifecycle_name is None:
            assert self._function is not None
            module = self._function.__module__
            owner = self._function.__qualname__

            def hook(method_name: str) -> pb.LifecycleHookRef:
                return pb.LifecycleHookRef(module=module, qualname=f"{owner}.{method_name}")

            initialize = (*metadata.snapshot_enter, *metadata.enter)
            if initialize:
                hooks.initialize.CopyFrom(hook(initialize[0]))
            if metadata.exit:
                hooks.shutdown.CopyFrom(hook(metadata.exit[0]))
            if metadata.snapshot_enter:
                hooks.snapshot.CopyFrom(hook(metadata.snapshot_enter[0]))
            if metadata.restore:
                hooks.restore.CopyFrom(hook(metadata.restore[0]))
            hooks.snapshot_after_initialize = metadata.snapshot_after_initialize
            hooks.snapshot_on_worker_retire = metadata.snapshot_on_worker_retire
        return pb.FunctionSpec(
            function=pb.FunctionRef(namespace=self.namespace, name=self.name),
            package=package_to_proto(package, source),
            lifecycle=lifecycle,
            lifecycle_hooks=hooks,
            **fields,
        )

    async def _resolve_async(
        self, stubs: Any, metadata: Any, deadline: float
    ) -> pb.FunctionRevision:
        if self._registered is not None:
            return self._registered
        if self._function is None:
            selector = (
                pb.FunctionSelector(
                    pinned=pb.RevisionRef(
                        function=pb.FunctionRef(namespace=self.namespace, name=self.name),
                        revision_id=self.revision,
                    )
                )
                if self.revision
                else pb.FunctionSelector(
                    current=pb.FunctionRef(namespace=self.namespace, name=self.name)
                )
            )
            self._registered = await stubs.functions.Get(
                pb.GetFunctionRequest(function=selector),
                metadata=metadata,
                timeout=deadline,
            )
            self._adopt_revision_options(self._registered)
            return self._registered
        package = package_callable(
            self._function,
            mode=cast(Any, self.package_mode),
            root=self.package_root,
            include=self.include,
            exclude=self.exclude,
            local_packages=self.local_packages,
        )
        source = await _async_upload(
            stubs,
            metadata,
            deadline,
            package.data,
            package.sha256,
            "application/vnd.vmon.python-package",
        )
        spec = self._make_spec(package, source)
        request_id = hashlib.sha256(spec.SerializeToString(deterministic=True)).hexdigest()
        self._registered = await stubs.functions.Register(
            pb.RegisterFunctionRequest(spec=spec, request_id=request_id),
            metadata=metadata,
            timeout=deadline,
        )
        return self._registered

    def _resolve(self) -> pb.FunctionRevision:
        if self._registered is not None:
            return self._registered
        client = _client(self._client)
        self._client = client
        if self._function is None:
            selector = (
                pb.FunctionSelector(
                    pinned=pb.RevisionRef(
                        function=pb.FunctionRef(namespace=self.namespace, name=self.name),
                        revision_id=self.revision,
                    )
                )
                if self.revision
                else pb.FunctionSelector(
                    current=pb.FunctionRef(namespace=self.namespace, name=self.name)
                )
            )
            self._registered, self._endpoint = _call_owner(
                client,
                lambda stubs: stubs.functions.Get(pb.GetFunctionRequest(function=selector)),
            )
            self._adopt_revision_options(self._registered)
            return self._registered
        with self._lock:
            if self._registered is not None:
                return self._registered
            package = package_callable(
                self._function,
                mode=cast(Any, self.package_mode),
                root=self.package_root,
                include=self.include,
                exclude=self.exclude,
                local_packages=self.local_packages,
            )
            source = _upload(
                client, package.data, package.sha256, "application/vnd.vmon.python-package"
            )
            spec = self._make_spec(package, source)
            request_id = hashlib.sha256(spec.SerializeToString(deterministic=True)).hexdigest()
            self._registered, self._endpoint = _call_owner(
                client,
                lambda stubs: stubs.functions.Register(
                    pb.RegisterFunctionRequest(spec=spec, request_id=request_id)
                ),
            )
            return self._registered

    def activate(self) -> pb.FunctionRecord:
        """Atomically make this immutable revision current."""
        revision = self._resolve()
        client = _client(self._client)
        return _call(
            client,
            lambda stubs: stubs.functions.Activate(
                pb.ActivateFunctionRequest(revision=revision.ref)
            ),
            endpoint=self._endpoint,
        )

    def spawn(self, *args: P.args, **kwargs: P.kwargs) -> FunctionCall[R]:
        if self._signature:
            self._signature.bind(*args, **kwargs)
        revision = self._resolve()
        client = _client(self._client)
        codec = self.options.serializer.input_serializer
        trusted = self.options.serializer.allow_trusted_python
        arguments = _arguments(
            client,
            args,
            kwargs,
            codec=codec,
            trusted=trusted,
            compression=self.options.serializer.compression,
        )
        call_type = pb.CALL_TYPE_GENERATOR if self._is_generator else pb.CALL_TYPE_UNARY
        request = pb.CreateCallRequest(
            type=call_type,
            target=pb.CallTarget(function=revision.ref),
            inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
            inputs_closed=True,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        return FunctionCall(
            record.ref.call_id,
            client,
            trusted=trusted,
            generator=self._is_generator,
            endpoint=endpoint,
        )

    def _spawn_actor(
        self, actor_id: str, method: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> FunctionCall[Any]:
        if not actor_id or not method:
            raise ValueError("actor_id and method must be non-empty")
        revision = self._resolve()
        client = _client(self._client)
        trusted = self.options.serializer.allow_trusted_python
        arguments = _arguments(
            client,
            args,
            kwargs,
            codec=self.options.serializer.input_serializer,
            trusted=trusted,
            compression=self.options.serializer.compression,
        )
        request = pb.CreateCallRequest(
            type=pb.CALL_TYPE_ACTOR,
            target=pb.CallTarget(
                function=revision.ref,
                actor=pb.ActorTarget(actor=pb.ActorRef(actor_id=actor_id), method=method),
            ),
            inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
            inputs_closed=True,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        return FunctionCall(record.ref.call_id, client, trusted=trusted, endpoint=endpoint)

    def _spawn_service(
        self,
        service_key: str,
        constructor_args: tuple[Any, ...],
        constructor_kwargs: dict[str, Any],
        method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> FunctionCall[Any]:
        if not service_key or not method:
            raise ValueError("service_key and method must be non-empty")
        revision = self._resolve()
        client = _client(self._client)
        trusted = self.options.serializer.allow_trusted_python
        constructor = _arguments(
            client,
            constructor_args,
            constructor_kwargs,
            codec=self.options.serializer.input_serializer,
            trusted=trusted,
            compression=self.options.serializer.compression,
        )
        arguments = _arguments(
            client,
            args,
            kwargs,
            codec=self.options.serializer.input_serializer,
            trusted=trusted,
            compression=self.options.serializer.compression,
        )
        request = pb.CreateCallRequest(
            type=pb.CALL_TYPE_UNARY,
            target=pb.CallTarget(
                function=revision.ref,
                service=pb.ServiceTarget(
                    service_key=service_key,
                    method=method,
                    constructor=constructor,
                ),
            ),
            inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
            inputs_closed=True,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        return FunctionCall(record.ref.call_id, client, trusted=trusted, endpoint=endpoint)

    def remote(self, *args: Any, timeout: float | None = None, **kwargs: Any) -> R:
        if self._is_generator:
            raise ValueError("generator functions must use remote_gen()")
        return self.spawn(*args, **kwargs).get(timeout)

    @overload
    def remote_gen(
        self: "RemoteFunction[P, Iterator[Y]]",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Iterator[Y]: ...

    @overload
    def remote_gen(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[Any]: ...

    def remote_gen(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[Any]:
        if not self._is_generator:
            raise ValueError("non-generator functions must use remote()")
        return self.spawn(*args, **kwargs).iter_yields()

    def spawn_map(
        self,
        iterable: Iterable[Any],
        *,
        kwargs: Mapping[str, Any] | None = None,
        starmap: bool = False,
        max_in_flight: int | None = None,
    ) -> BatchCall[R]:
        if max_in_flight is not None and max_in_flight < 1:
            raise ValueError("max_in_flight must be at least one")
        revision = self._resolve()
        client = _client(self._client)
        trusted = self.options.serializer.allow_trusted_python
        request = pb.CreateCallRequest(
            type=pb.CALL_TYPE_BATCH,
            target=pb.CallTarget(function=revision.ref),
            inputs_closed=False,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        stop = threading.Event()
        errors: list[BaseException] = []
        batch: BatchCall[R] = BatchCall(
            FunctionCall(record.ref.call_id, client, trusted=trusted, endpoint=endpoint),
            feeder_stop=stop,
            feeder_errors=errors,
        )

        def feed() -> None:
            count = 0
            try:

                def frames() -> Iterator[pb.StreamCallInputsRequest]:
                    nonlocal count
                    yield pb.StreamCallInputsRequest(call=record.ref)
                    for item in iterable:
                        if stop.is_set():
                            return
                        args = tuple(item) if starmap else (item,)
                        call_kwargs = dict(kwargs or {})
                        if self._signature:
                            self._signature.bind(*args, **call_kwargs)
                        arguments = _arguments(
                            client,
                            args,
                            call_kwargs,
                            codec=self.options.serializer.input_serializer,
                            trusted=trusted,
                            compression=self.options.serializer.compression,
                        )
                        yield pb.StreamCallInputsRequest(
                            input=pb.CallInput(
                                index=count,
                                input_id=str(uuid.uuid4()),
                                arguments=arguments,
                            )
                        )
                        count += 1

                responses = _call(
                    client,
                    lambda stubs: stubs.calls.StreamInputs(frames()),
                    stream=True,
                    endpoint=endpoint,
                )
                for _ in responses:
                    if stop.is_set():
                        break
                if not stop.is_set():
                    _call(
                        client,
                        lambda stubs: stubs.calls.CloseInputs(
                            pb.CloseCallInputsRequest(call=record.ref, expected_input_count=count)
                        ),
                        endpoint=endpoint,
                    )
                    batch.input_count = count
            except BaseException as exc:
                errors.append(exc)
                stop.set()
                try:
                    _call(
                        client,
                        lambda stubs: stubs.calls.Cancel(
                            pb.CancelCallRequest(
                                call=record.ref,
                                reason="batch input feeder failed",
                                request_id=str(uuid.uuid4()),
                            )
                        ),
                        endpoint=endpoint,
                    )
                except BaseException:
                    pass

        threading.Thread(
            target=feed,
            name=f"vmon-batch-feeder-{record.ref.call_id}",
            daemon=True,
        ).start()
        return batch

    def map(self, *iterables: Iterable[Any], **kwargs: Any) -> Iterator[R]:
        if not iterables:
            raise ValueError("map requires at least one iterable")
        items = iterables[0] if len(iterables) == 1 else zip(*iterables)
        return self.spawn_map(
            items, kwargs=kwargs.pop("kwargs", None), starmap=len(iterables) > 1
        ).results(
            **{k: v for k, v in kwargs.items() if k in {"order_outputs", "return_exceptions"}}
        )

    def starmap(self, iterable: Iterable[Iterable[Any]], **kwargs: Any) -> Iterator[R]:
        return self.spawn_map(iterable, kwargs=kwargs.pop("kwargs", None), starmap=True).results(
            **{k: v for k, v in kwargs.items() if k in {"order_outputs", "return_exceptions"}}
        )

    def for_each(self, *iterables: Iterable[Any], **kwargs: Any) -> None:
        for _ in self.map(*iterables, **kwargs):
            pass


@overload
def function(function: Callable[P, R], /) -> RemoteFunction[P, R]: ...
@overload
def function(
    function: None = None,
    /,
    *,
    client: Any | None = None,
    namespace: str = "default",
    name: str | None = None,
    options: FunctionOptions | None = None,
    image: Image | str | None = None,
    cpus: float | None = None,
    memory: int | None = None,
    disk: int | None = None,
    architecture: CpuArchitecture | str | None = None,
    ha: HighAvailabilityPolicy | str | None = None,
    volumes: tuple[FunctionVolumeMount, ...] | None = None,
    network: NetworkPolicy | None = None,
    egress: Iterable[str] | None = None,
    block_network: bool | None = None,
    egress_cidrs: Iterable[str] | None = None,
    egress_domains: Iterable[str] | None = None,
    inbound_cidrs: Iterable[str] | None = None,
    retries: RetryPolicy | int | None = None,
    startup_timeout: float | None = None,
    execution_timeout: float | None = None,
    queue_timeout: float | None = None,
    idle_timeout: float | None = None,
    result_ttl: float | None = None,
    min_workers: int | None = None,
    max_workers: int | None = None,
    buffer_workers: int | None = None,
    scale_down_after: float | None = None,
    max_outstanding_inputs: int | None = None,
    serializer: SerializerPolicy | ValueCodec | str | None = None,
    package_mode: str = "package",
    package_root: str | None = None,
    include: Iterable[str] = (),
    exclude: Iterable[str] = (),
    local_packages: Iterable[str] = (),
) -> Callable[[Callable[P, R]], RemoteFunction[P, R]]: ...
def function(
    function: Callable[P, R] | None = None,
    /,
    *,
    client: Any | None = None,
    namespace: str = "default",
    name: str | None = None,
    options: FunctionOptions | None = None,
    image: Image | str | None = None,
    cpus: float | None = None,
    memory: int | None = None,
    disk: int | None = None,
    architecture: CpuArchitecture | str | None = None,
    ha: HighAvailabilityPolicy | str | None = None,
    volumes: tuple[FunctionVolumeMount, ...] | None = None,
    network: NetworkPolicy | None = None,
    egress: Iterable[str] | None = None,
    block_network: bool | None = None,
    egress_cidrs: Iterable[str] | None = None,
    egress_domains: Iterable[str] | None = None,
    inbound_cidrs: Iterable[str] | None = None,
    retries: RetryPolicy | int | None = None,
    startup_timeout: float | None = None,
    execution_timeout: float | None = None,
    queue_timeout: float | None = None,
    idle_timeout: float | None = None,
    result_ttl: float | None = None,
    min_workers: int | None = None,
    max_workers: int | None = None,
    buffer_workers: int | None = None,
    scale_down_after: float | None = None,
    max_outstanding_inputs: int | None = None,
    serializer: SerializerPolicy | ValueCodec | str | None = None,
    package_mode: str = "package",
    package_root: str | None = None,
    include: Iterable[str] = (),
    exclude: Iterable[str] = (),
    local_packages: Iterable[str] = (),
) -> RemoteFunction[P, R] | Callable[[Callable[P, R]], RemoteFunction[P, R]]:
    """Decorate a Python callable as a typed, durable remote function."""
    has_explicit_options = options is not None or any(
        value is not None
        for value in (
            image,
            cpus,
            memory,
            disk,
            architecture,
            ha,
            volumes,
            network,
            egress,
            block_network,
            egress_cidrs,
            egress_domains,
            inbound_cidrs,
            retries,
            startup_timeout,
            execution_timeout,
            queue_timeout,
            idle_timeout,
            result_ttl,
            min_workers,
            max_workers,
            buffer_workers,
            scale_down_after,
            max_outstanding_inputs,
            serializer,
        )
    )
    configured = _compose_function_options(
        options,
        image=image,
        cpus=cpus,
        memory=memory,
        disk=disk,
        architecture=architecture,
        ha=ha,
        volumes=volumes,
        network=network,
        egress=egress,
        block_network=block_network,
        egress_cidrs=egress_cidrs,
        egress_domains=egress_domains,
        inbound_cidrs=inbound_cidrs,
        retries=retries,
        startup_timeout=startup_timeout,
        execution_timeout=execution_timeout,
        queue_timeout=queue_timeout,
        idle_timeout=idle_timeout,
        result_ttl=result_ttl,
        min_workers=min_workers,
        max_workers=max_workers,
        buffer_workers=buffer_workers,
        scale_down_after=scale_down_after,
        max_outstanding_inputs=max_outstanding_inputs,
        serializer=serializer,
    )

    def decorate(inner: Callable[P, R]) -> RemoteFunction[P, R]:
        return RemoteFunction(
            inner,
            client=client,
            namespace=namespace,
            name=name,
            options=configured if has_explicit_options else None,
            package_mode=package_mode,
            package_root=package_root,
            include=include,
            exclude=exclude,
            local_packages=local_packages,
        )

    return decorate(function) if function is not None else decorate
