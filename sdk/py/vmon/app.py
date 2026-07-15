"""Typed application grouping and atomic function activation."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, ParamSpec, Protocol, TypeVar, overload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .image import Image
from .options import (
    CpuArchitecture,
    FunctionOptions,
    FunctionVolumeMount,
    HighAvailabilityPolicy,
    NetworkPolicy,
    RetryPolicy,
    SerializerPolicy,
)
from .v1 import api_pb2
from .values import ValueCodec

if TYPE_CHECKING:
    from .remote import (
        AsyncGeneratorRemoteFunction,
        AsyncRemoteFunction,
        GeneratorRemoteFunction,
        RemoteFunction,
        SyncRemoteFunction,
    )

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])
Q = ParamSpec("Q")
T = TypeVar("T")


class _AppFunctionDecorator(Protocol):
    @overload
    def __call__(  # type: ignore[overload-overlap]
        self, function: Callable[Q, Iterator[T]], /
    ) -> GeneratorRemoteFunction[Q, T]: ...

    @overload
    def __call__(  # type: ignore[overload-overlap]
        self, function: Callable[Q, AsyncIterator[T]], /
    ) -> AsyncGeneratorRemoteFunction[Q, T]: ...

    @overload
    def __call__(  # type: ignore[overload-overlap]
        self, function: Callable[Q, Coroutine[Any, Any, T]], /
    ) -> AsyncRemoteFunction[Q, T]: ...

    @overload
    def __call__(self, function: Callable[Q, T], /) -> SyncRemoteFunction[Q, T]: ...


@dataclass(frozen=True, slots=True)
class AppBinding:
    """One application-local name pinned to an immutable function revision."""

    name: str
    namespace: str
    function_name: str
    revision_id: str


@dataclass(frozen=True, slots=True)
class AppManifest:
    """The complete, atomically activated function set for an application."""

    namespace: str
    name: str
    functions: tuple[AppBinding, ...]

    def __post_init__(self) -> None:
        if not self.namespace or not self.name:
            raise ValueError("app namespace and name must be non-empty")
        names = [binding.name for binding in self.functions]
        if len(names) != len(set(names)):
            raise ValueError("app binding names must be unique")


@dataclass(frozen=True, slots=True)
class AppRevision:
    """An immutable server-published application revision."""

    manifest: AppManifest
    revision_id: str
    created_at_unix_millis: int = 0
    previous_revision_id: str | None = None


@dataclass(frozen=True, slots=True)
class Cron:
    """A validated five-field cron schedule in an IANA time zone."""

    expression: str
    time_zone: str = "UTC"

    def __post_init__(self) -> None:
        if len(self.expression.split()) != 5:
            raise ValueError("cron expression must contain exactly five fields")
        try:
            ZoneInfo(self.time_zone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown IANA time zone {self.time_zone!r}") from exc


@dataclass(frozen=True, slots=True)
class Period:
    """A positive fixed interval from a stable Unix-millisecond anchor."""

    seconds: float | timedelta
    anchor_unix_millis: int = 0

    def __post_init__(self) -> None:
        seconds = (
            self.seconds.total_seconds() if isinstance(self.seconds, timedelta) else self.seconds
        )
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or seconds <= 0:
            raise ValueError("period must be a positive number of seconds")
        if isinstance(self.anchor_unix_millis, bool) or self.anchor_unix_millis < 0:
            raise ValueError("anchor_unix_millis must be non-negative")
        object.__setattr__(self, "seconds", float(seconds))


@dataclass(frozen=True, slots=True)
class Schedule:
    """One server-owned schedule pinned to an app and function revision."""

    id: str
    name: str
    app_revision_id: str
    function: AppBinding
    timing: Cron | Period
    created_at_unix_millis: int = 0
    updated_at_unix_millis: int = 0
    next_run_unix_millis: int | None = None


class App:
    """Optional grouping for zero-deploy definitions and atomic deployment.

    Functions do not require an ``App``.  Adding them here only gives them an
    application-local binding and makes :meth:`deploy` publish the complete set
    in one atomic ``ActivateApp`` request.
    """

    def __init__(self, name: str, *, namespace: str = "default", client: Any = None) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("app name must be non-empty")
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("app namespace must be non-empty")
        self.name = name
        self.namespace = namespace
        self.client = client
        self._definitions: dict[str, Any] = {}

    def add(self, definition: Any, *, name: str | None = None) -> Any:
        """Add a function or remote class under a unique local binding."""

        binding = name or getattr(definition, "name", None) or getattr(definition, "__name__", None)
        if not isinstance(binding, str) or not binding:
            raise ValueError("app definitions require a non-empty binding name")
        if binding in self._definitions:
            raise ValueError(f"duplicate app binding {binding!r}")
        self._definitions[binding] = definition
        return definition

    @overload
    def function(  # type: ignore[overload-overlap]
        self, fn: Callable[P, Iterator[R]], /
    ) -> GeneratorRemoteFunction[P, R]: ...

    @overload
    def function(  # type: ignore[overload-overlap]
        self, fn: Callable[P, AsyncIterator[R]], /
    ) -> AsyncGeneratorRemoteFunction[P, R]: ...

    @overload
    def function(  # type: ignore[overload-overlap]
        self, fn: Callable[P, Coroutine[Any, Any, R]], /
    ) -> AsyncRemoteFunction[P, R]: ...

    @overload
    def function(self, fn: Callable[P, R], /) -> SyncRemoteFunction[P, R]: ...

    @overload
    def function(
        self,
        fn: None = None,
        /,
        *,
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
    ) -> _AppFunctionDecorator: ...

    def function(
        self,
        fn: Callable[..., Any] | None = None,
        /,
        *,
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
    ) -> Any:
        """Decorate and add a function using the standard typed option surface."""

        from .remote import function

        def decorate(inner: Callable[..., Any]) -> RemoteFunction[..., Any]:
            configured = function(
                client=self.client,
                namespace=self.namespace,
                name=name or inner.__name__,
                options=options,
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
                package_mode=package_mode,
                package_root=package_root,
                include=include,
                exclude=exclude,
                local_packages=local_packages,
            )
            remote = configured(inner)
            return self.add(remote, name=name)

        return decorate(fn) if fn is not None else decorate

    def include(self, *, name: str | None = None) -> Callable[[Any], Any]:
        """Decorator adding an already decorated definition to this app."""

        return lambda definition: self.add(definition, name=name)

    def manifest(self, *, client: Any = None) -> AppManifest:
        """Register definitions and return their complete pinned manifest."""

        selected = client or self.client
        if selected is None:
            raise ValueError("app deployment requires a client")
        bindings = tuple(
            _binding(local_name, _register(definition, selected))
            for local_name, definition in sorted(self._definitions.items())
        )
        return AppManifest(self.namespace, self.name, bindings)

    def deploy(
        self,
        *,
        client: Any = None,
        expected_current: str | AppRevision | None = None,
        request_id: str | None = None,
    ) -> AppRevision:
        """Register every definition, then atomically activate the complete manifest."""

        selected = client or self.client
        if selected is None:
            raise ValueError("app deployment requires a client")
        manifest = self.manifest(client=selected)
        request = api_pb2.ActivateAppRequest(
            app=api_pb2.AppRef(namespace=self.namespace, name=self.name),
            functions=[
                api_pb2.AppFunctionBinding(
                    name=binding.name,
                    revision=api_pb2.RevisionRef(
                        function=api_pb2.FunctionRef(
                            namespace=binding.namespace, name=binding.function_name
                        ),
                        revision_id=binding.revision_id,
                    ),
                )
                for binding in manifest.functions
            ],
            request_id=request_id or str(uuid.uuid4()),
        )
        expected = (
            expected_current.revision_id
            if isinstance(expected_current, AppRevision)
            else expected_current
        )
        if expected:
            request.expected_current.CopyFrom(
                api_pb2.AppRevisionRef(app=request.app, revision_id=expected)
            )
        response, _ = selected.driver.call(lambda stubs: stubs.functions.ActivateApp(request))
        return _app_revision(response)

    def get(self, revision_id: str | None = None, *, client: Any = None) -> AppRevision:
        """Resolve the current app revision or an explicitly pinned revision."""

        selected = client or self.client
        if selected is None:
            raise ValueError("app lookup requires a client")
        app_ref = api_pb2.AppRef(namespace=self.namespace, name=self.name)
        selector = (
            api_pb2.AppSelector(pinned=api_pb2.AppRevisionRef(app=app_ref, revision_id=revision_id))
            if revision_id
            else api_pb2.AppSelector(current=app_ref)
        )
        response, _ = selected.driver.call(
            lambda stubs: stubs.functions.GetApp(api_pb2.GetAppRequest(app=selector))
        )
        return _app_revision(response)

    def rollback(
        self,
        target: str | AppRevision,
        *,
        expected_current: str | AppRevision | None = None,
        client: Any = None,
        request_id: str | None = None,
    ) -> AppRevision:
        """Atomically restore a prior immutable application revision."""

        selected = client or self.client
        if selected is None:
            raise ValueError("app rollback requires a client")
        app_ref = api_pb2.AppRef(namespace=self.namespace, name=self.name)
        target_id = target.revision_id if isinstance(target, AppRevision) else target
        if not target_id:
            raise ValueError("rollback target revision must be non-empty")
        request = api_pb2.RollbackAppRequest(
            target=api_pb2.AppRevisionRef(app=app_ref, revision_id=target_id),
            request_id=request_id or str(uuid.uuid4()),
        )
        expected_id = (
            expected_current.revision_id
            if isinstance(expected_current, AppRevision)
            else expected_current
        )
        if expected_id:
            request.expected_current.CopyFrom(
                api_pb2.AppRevisionRef(app=app_ref, revision_id=expected_id)
            )
        response, _ = selected.driver.call(lambda stubs: stubs.functions.RollbackApp(request))
        return _app_revision(response)

    def create_schedule(
        self,
        name: str,
        function: str | AppBinding,
        timing: Cron | Period,
        *,
        input: object = None,
        app_revision: AppRevision | None = None,
        schedule_id: str | None = None,
        labels: Mapping[str, str] | None = None,
        client: Any = None,
        request_id: str | None = None,
    ) -> Schedule:
        """Create or replace a server-owned schedule pinned to exact revisions."""

        selected = client or self.client
        if selected is None:
            raise ValueError("schedule registration requires a client")
        revision = app_revision or self.get(client=selected)
        binding = (
            function
            if isinstance(function, AppBinding)
            else next((item for item in revision.manifest.functions if item.name == function), None)
        )
        if binding is None:
            raise KeyError(f"app revision has no function binding {function!r}")
        spec = api_pb2.ScheduleSpec(
            name=name,
            app=api_pb2.AppRevisionRef(
                app=api_pb2.AppRef(namespace=self.namespace, name=self.name),
                revision_id=revision.revision_id,
            ),
            target=api_pb2.ScheduleTarget(
                function=api_pb2.RevisionRef(
                    function=api_pb2.FunctionRef(
                        namespace=binding.namespace, name=binding.function_name
                    ),
                    revision_id=binding.revision_id,
                ),
                input=_json_envelope(input),
            ),
            status=api_pb2.SCHEDULE_STATUS_ACTIVE,
            labels=dict(labels or {}),
        )
        if isinstance(timing, Cron):
            spec.cron.CopyFrom(
                api_pb2.CronSchedule(expression=timing.expression, time_zone=timing.time_zone)
            )
        elif isinstance(timing, Period):
            spec.period.CopyFrom(
                api_pb2.PeriodSchedule(
                    period_millis=round(
                        (
                            timing.seconds.total_seconds()
                            if isinstance(timing.seconds, timedelta)
                            else timing.seconds
                        )
                        * 1000
                    ),
                    anchor_unix_millis=timing.anchor_unix_millis,
                )
            )
        else:
            raise TypeError("timing must be Cron or Period")
        request = api_pb2.CreateScheduleRequest(
            spec=spec, request_id=request_id or str(uuid.uuid4())
        )
        if schedule_id:
            request.schedule_id = schedule_id
        record, _ = selected.driver.call(lambda stubs: stubs.functions.CreateSchedule(request))
        return _schedule(record, binding)

    def get_schedule(self, schedule: str | Schedule, *, client: Any = None) -> Schedule:
        """Resolve one durable schedule and its application-local function binding."""
        selected = client or self.client
        if selected is None:
            raise ValueError("schedule lookup requires a client")
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        if not schedule_id:
            raise ValueError("schedule id must be non-empty")
        record, _ = selected.driver.call(
            lambda stubs: stubs.functions.GetSchedule(api_pb2.ScheduleRef(schedule_id=schedule_id))
        )
        manifest = _pinned_app_manifest(selected, record.spec.app)
        return _schedule(record, _schedule_binding(record, manifest))

    def list_schedules(
        self,
        *,
        client: Any = None,
        page_size: int = 0,
        page_token: str = "",
    ) -> tuple[tuple[Schedule, ...], str]:
        """List this application's schedules and the stable continuation token."""

        selected = client or self.client
        if selected is None:
            raise ValueError("schedule listing requires a client")
        response, _ = selected.driver.call(
            lambda stubs: stubs.functions.ListSchedules(
                api_pb2.ListSchedulesRequest(
                    app=api_pb2.AppRef(namespace=self.namespace, name=self.name),
                    page_size=page_size,
                    page_token=page_token,
                )
            )
        )
        manifests: dict[tuple[str, str, str], AppManifest] = {}
        for record in response.schedules:
            app = record.spec.app
            key = (app.app.namespace, app.app.name, app.revision_id)
            if key not in manifests:
                manifests[key] = _pinned_app_manifest(selected, app)

        return (
            tuple(
                _schedule(
                    record,
                    _schedule_binding(
                        record,
                        manifests[
                            (
                                record.spec.app.app.namespace,
                                record.spec.app.app.name,
                                record.spec.app.revision_id,
                            )
                        ],
                    ),
                )
                for record in response.schedules
            ),
            response.next_page_token,
        )

    def delete_schedule(self, schedule: str | Schedule, *, client: Any = None) -> None:
        """Permanently delete a durable schedule by stable ID."""

        selected = client or self.client
        if selected is None:
            raise ValueError("schedule deletion requires a client")
        schedule_id = schedule.id if isinstance(schedule, Schedule) else schedule
        if not schedule_id:
            raise ValueError("schedule id must be non-empty")
        selected.driver.call(
            lambda stubs: stubs.functions.DeleteSchedule(
                api_pb2.ScheduleRef(schedule_id=schedule_id)
            )
        )

    @classmethod
    def from_name(cls, name: str, *, namespace: str = "default", client: Any = None) -> App:
        """Create a current-revision application lookup handle."""

        return cls(name, namespace=namespace, client=client)

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return iter(self._definitions.items())


def _register(definition: Any, client: Any) -> Any:
    for method_name in ("_register", "register"):
        method = getattr(definition, method_name, None)
        if callable(method):
            try:
                return method(client=client, activate=False)
            except TypeError:
                try:
                    return method(client=client)
                except TypeError:
                    return method(client)
    deploy = getattr(definition, "deploy", None)
    if callable(deploy):
        try:
            return deploy(client=client, activate=False)
        except TypeError:
            return deploy(client=client)
    resolve = getattr(definition, "_resolve", None)
    if callable(resolve):
        if getattr(definition, "_client", None) is None:
            definition._client = client
        return resolve()
    revision = getattr(definition, "revision", None) or getattr(definition, "revision_ref", None)
    if revision is not None:
        return revision
    raise TypeError("app definition must expose register(), deploy(), or a revision ref")


def _binding(name: str, revision: Any) -> AppBinding:
    ref = getattr(revision, "ref", revision)
    function = getattr(ref, "function", None)
    revision_id = getattr(ref, "revision_id", None)
    namespace = getattr(function, "namespace", None)
    function_name = getattr(function, "name", None)
    if not isinstance(revision_id, str) or not revision_id:
        raise TypeError("definition registration did not return a pinned RevisionRef")
    if not isinstance(namespace, str) or not namespace:
        raise TypeError("definition registration did not return a pinned RevisionRef")
    if not isinstance(function_name, str) or not function_name:
        raise TypeError("definition registration did not return a pinned RevisionRef")
    return AppBinding(name, namespace, function_name, revision_id)


def _app_revision(value: Any) -> AppRevision:
    bindings = tuple(
        AppBinding(
            item.name,
            item.revision.function.namespace,
            item.revision.function.name,
            item.revision.revision_id,
        )
        for item in value.functions
    )
    manifest = AppManifest(value.ref.app.namespace, value.ref.app.name, bindings)
    previous = value.previous.revision_id if value.HasField("previous") else None
    return AppRevision(manifest, value.ref.revision_id, value.created_at_unix_millis, previous)


def _json_envelope(value: object) -> api_pb2.ValueEnvelope:
    raw = json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return api_pb2.ValueEnvelope(
        schema_version=1,
        serializer=api_pb2.VALUE_SERIALIZER_JSON,
        compression=api_pb2.VALUE_COMPRESSION_NONE,
        checksum=api_pb2.Digest(
            algorithm=api_pb2.DIGEST_ALGORITHM_SHA256,
            value=hashlib.sha256(raw).digest(),
        ),
        uncompressed_size_bytes=len(raw),
        inline_data=raw,
        type_name=type(value).__qualname__,
    )


def _pinned_app_manifest(client: Any, ref: Any) -> AppManifest:
    response, _ = client.driver.call(
        lambda stubs: stubs.functions.GetApp(
            api_pb2.GetAppRequest(app=api_pb2.AppSelector(pinned=ref))
        )
    )
    return _app_revision(response).manifest


def _schedule_binding(value: Any, manifest: AppManifest) -> AppBinding:
    target = value.spec.target.function
    for binding in manifest.functions:
        if (
            binding.namespace == target.function.namespace
            and binding.function_name == target.function.name
            and binding.revision_id == target.revision_id
        ):
            return binding
    raise ValueError(
        f"schedule {value.ref.schedule_id!r} target is absent from pinned app revision "
        f"{value.spec.app.revision_id!r}"
    )


def _schedule(value: Any, known_binding: AppBinding | None = None) -> Schedule:
    spec = value.spec
    target = spec.target.function
    binding = known_binding or AppBinding(
        target.function.name,
        target.function.namespace,
        target.function.name,
        target.revision_id,
    )
    timing: Cron | Period
    if spec.HasField("cron"):
        timing = Cron(spec.cron.expression, spec.cron.time_zone)
    elif spec.HasField("period"):
        timing = Period(spec.period.period_millis / 1000, spec.period.anchor_unix_millis)
    else:
        raise ValueError("schedule record has no timing")
    next_run = value.next_run_unix_millis if value.HasField("next_run_unix_millis") else None
    return Schedule(
        value.ref.schedule_id,
        spec.name,
        spec.app.revision_id,
        binding,
        timing,
        value.created_at_unix_millis,
        value.updated_at_unix_millis,
        next_run,
    )
