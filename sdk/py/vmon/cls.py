"""Durable actor and horizontally-scalable class definitions."""

from __future__ import annotations

import functools
import inspect
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Generic, TypeVar, overload


from .errors import ActorLostError
from .options import ConcurrencyPolicy, FunctionOptions
from .remote import FunctionCall, RemoteFunction, _arguments
from .values import encode_value
from .v1 import api_pb2

R = TypeVar("R")


def method(fn: Callable[..., R]) -> Callable[..., R]:
    """Export one class method for remote actor or service dispatch."""

    setattr(fn, "__vmon_method__", True)
    return fn


def enter(
    fn: Callable[..., Any] | None = None, /, *, snapshot: bool = False
) -> Callable[..., Any]:
    """Declare an ordered initialization hook.

    ``snapshot=True`` marks the hook as running before a reusable worker
    snapshot; the default marks it as a normal cold-start hook.
    """

    def decorate(inner: Callable[..., Any]) -> Callable[..., Any]:
        setattr(inner, "__vmon_enter__", True)
        setattr(inner, "__vmon_snapshot_enter__", bool(snapshot))
        return inner

    return decorate(fn) if fn is not None else decorate


def exit(fn: Callable[..., Any]) -> Callable[..., Any]:  # noqa: A001
    """Declare an ordered graceful shutdown hook."""

    setattr(fn, "__vmon_exit__", True)
    return fn


def on_restore(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Declare an ordered post-snapshot-restore hook."""

    setattr(fn, "__vmon_restore__", True)
    return fn


@dataclass(frozen=True, slots=True)
class LifecycleMetadata:
    """Deterministic lifecycle metadata sent with a class definition."""

    methods: tuple[str, ...]
    enter_order: tuple[tuple[str, bool], ...]
    enter: tuple[str, ...]
    snapshot_enter: tuple[str, ...]
    restore: tuple[str, ...]
    exit: tuple[str, ...]
    snapshot_after_initialize: bool
    snapshot_on_worker_retire: bool


@dataclass(frozen=True, slots=True)
class ActorCheckpoint:
    """Stable reference to one immutable durable actor checkpoint."""

    id: str
    actor_id: str
    sequence: int
    created_at_unix_millis: int


class RemoteClass:
    """A server-backed durable actor definition produced by ``@vmon.cls``."""

    def __init__(
        self,
        user_cls: type,
        *,
        client: Any = None,
        namespace: str = "default",
        name: str | None = None,
        revision: str | None = None,
        options: FunctionOptions | None = None,
        include: Sequence[str] = (),
        service: bool = False,
        snapshot_after_initialize: bool = False,
        snapshot_on_worker_retire: bool = True,
        _function: RemoteFunction | None = None,
    ) -> None:
        if not inspect.isclass(user_cls):
            raise TypeError("vmon class decorators require a class")
        self._cls = user_cls
        self._client = client
        self.namespace = namespace
        self.name = name or user_cls.__name__
        self.lifecycle = "service" if service else "actor"
        members = tuple(vars(user_cls).items())
        self.metadata = LifecycleMetadata(
            methods=tuple(name for name, value in members if getattr(value, "__vmon_method__", False)),
            enter_order=tuple(
                (name, bool(getattr(value, "__vmon_snapshot_enter__", False)))
                for name, value in members
                if getattr(value, "__vmon_enter__", False)
            ),
            enter=tuple(
                name
                for name, value in members
                if getattr(value, "__vmon_enter__", False)
                and not getattr(value, "__vmon_snapshot_enter__", False)
            ),
            snapshot_enter=tuple(
                name
                for name, value in members
                if getattr(value, "__vmon_enter__", False)
                and getattr(value, "__vmon_snapshot_enter__", False)
            ),
            restore=tuple(name for name, value in members if getattr(value, "__vmon_restore__", False)),
            exit=tuple(name for name, value in members if getattr(value, "__vmon_exit__", False)),
            snapshot_after_initialize=bool(snapshot_after_initialize),
            snapshot_on_worker_retire=bool(snapshot_on_worker_retire),
        )
        if not self.metadata.methods:
            raise TypeError("remote class must export at least one @vmon.method")
        configured = options or FunctionOptions()
        if service:
            if configured.concurrency.serialize_actor_calls:
                raise ValueError("service classes cannot request actor-call serialization")
        elif configured.concurrency != ConcurrencyPolicy():
            if not configured.concurrency.serialize_actor_calls:
                raise ValueError("actors require serialize_actor_calls=True")
        else:
            configured = replace(
                configured,
                concurrency=ConcurrencyPolicy(max_concurrent_calls=1, serialize_actor_calls=True),
            )
        self.options = configured
        self._function = _function or RemoteFunction(
            user_cls,
            client=client,
            namespace=namespace,
            name=self.name,
            revision=revision,
            options=configured,
            include=include,
        )
        setattr(self._function, "__vmon_class_lifecycle__", self.lifecycle)
        setattr(self._function, "__vmon_lifecycle_metadata__", self.metadata)
        functools.update_wrapper(self, user_cls, updated=())

    def __call__(
        self, *args: Any, labels: Mapping[str, str] | None = None, **kwargs: Any
    ) -> "RemoteObject":
        if self.lifecycle == "service":
            return RemoteObject(self, None, service_init=(args, kwargs))
        client = self._bound_client()
        revision = self._function._resolve().ref
        request = api_pb2.CreateActorRequest(
            function=revision,
            request_id=str(uuid.uuid4()),
            labels=dict(labels or {}),
        )
        if args or kwargs:
            request.initial_arguments.CopyFrom(
                _arguments(
                    client,
                    args,
                    kwargs,
                    codec=self.options.serializer.input_serializer,
                    compression=self.options.serializer.compression,
                    trusted=self.options.serializer.allow_trusted_python,
                )
            )
        record, _ = client.driver.call(lambda stubs: stubs.actors.Create(request))
        return RemoteObject(self, record.ref.actor_id, record=record)

    def from_id(self, actor_id: str) -> "RemoteObject":
        """Reconnect to an existing durable actor without replaying construction."""

        if self.lifecycle != "actor":
            raise TypeError("service classes do not have durable actor IDs")
        if not actor_id:
            raise ValueError("actor id must be non-empty")
        obj = RemoteObject(self, actor_id)
        obj._record()
        return obj

    def with_options(self, options: FunctionOptions) -> "RemoteClass":
        """Return an immutable definition variant with validated options."""

        return type(self)(
            self._cls,
            client=self._client,
            namespace=self.namespace,
            name=self.name,
            options=options,
            service=self.lifecycle == "service",
            snapshot_after_initialize=self.metadata.snapshot_after_initialize,
            snapshot_on_worker_retire=self.metadata.snapshot_on_worker_retire,
            _function=self._function.with_options(options=options),
        )

    def _bound_client(self) -> Any:
        client = self._client or getattr(self._function, "_client", None)
        if client is None:
            raise ValueError("actor operation requires a bound client")
        return client

    def _register(self, *, client: Any = None, activate: bool = False) -> Any:
        if client is not None and client is not self._client:
            self._client = client
            self._function._client = client
        revision = self._function._resolve()
        if activate:
            activate_method = getattr(self._function, "activate", None)
            if callable(activate_method):
                activate_method()
        return revision


class RemoteObject:
    """A reconnectable durable actor handle or stateless service-class proxy."""

    def __init__(
        self,
        remote_cls: RemoteClass,
        actor_id: str | None,
        *,
        record: Any = None,
        service_init: tuple[tuple[Any, ...], Mapping[str, Any]] | None = None,
    ) -> None:
        self._remote_cls = remote_cls
        self._actor_id = actor_id
        self._cached_record = record
        self._service_init = service_init
        self._service_key = (
            encode_value(
                {"args": service_init[0], "kwargs": dict(service_init[1])},
                codec=remote_cls.options.serializer.input_serializer,
                compression="none",
            ).sha256
            if service_init is not None
            else None
        )

    @property
    def id(self) -> str:
        """Return the stable actor identifier."""

        if self._actor_id is None:
            raise TypeError("service instances do not have durable IDs")
        return self._actor_id

    def __getattr__(self, name: str) -> "_BoundRemoteMethod[Any]":
        if name in self._remote_cls.metadata.methods:
            return _BoundRemoteMethod(self, name)
        raise AttributeError(f"{name!r} is not decorated with @vmon.method")

    def checkpoint(self, *, request_id: str | None = None) -> ActorCheckpoint:
        """Capture and return an immutable actor recovery point."""

        checkpoint, _ = self._client.driver.call(
            lambda stubs: stubs.actors.Checkpoint(
                api_pb2.CheckpointActorRequest(
                    actor=api_pb2.ActorRef(actor_id=self.id),
                    request_id=request_id or str(uuid.uuid4()),
                )
            )
        )
        return ActorCheckpoint(
            checkpoint.ref.checkpoint_id,
            checkpoint.actor.actor_id,
            checkpoint.sequence,
            checkpoint.created_at_unix_millis,
        )

    def restore(self, checkpoint: str | ActorCheckpoint, *, request_id: str | None = None) -> None:
        """Atomically restore this identity; its constructor is never replayed."""

        checkpoint_id = checkpoint.id if isinstance(checkpoint, ActorCheckpoint) else checkpoint
        record, _ = self._client.driver.call(
            lambda stubs: stubs.actors.Restore(
                api_pb2.RestoreActorRequest(
                    actor=api_pb2.ActorRef(actor_id=self.id),
                    checkpoint=api_pb2.ActorCheckpointRef(checkpoint_id=checkpoint_id),
                    request_id=request_id or str(uuid.uuid4()),
                )
            )
        )
        self._cached_record = record
        self._ensure_alive(record)

    def fork(
        self,
        checkpoint: str | ActorCheckpoint | None = None,
        *,
        labels: Mapping[str, str] | None = None,
        request_id: str | None = None,
    ) -> "RemoteObject":
        """Create an independent actor identity from an immutable checkpoint."""

        if checkpoint is None:
            checkpoint = self.checkpoint()
        checkpoint_id = checkpoint.id if isinstance(checkpoint, ActorCheckpoint) else checkpoint
        record, _ = self._client.driver.call(
            lambda stubs: stubs.actors.Fork(
                api_pb2.ForkActorRequest(
                    checkpoint=api_pb2.ActorCheckpointRef(checkpoint_id=checkpoint_id),
                    request_id=request_id or str(uuid.uuid4()),
                    labels=dict(labels or {}),
                )
            )
        )
        return RemoteObject(self._remote_cls, record.ref.actor_id, record=record)

    def delete(self) -> None:
        """Permanently delete this actor identity."""

        self._client.driver.call(
            lambda stubs: stubs.actors.Delete(api_pb2.ActorRef(actor_id=self.id))
        )

    @property
    def _client(self) -> Any:
        return self._remote_cls._bound_client()

    def _record(self) -> Any:
        record, _ = self._client.driver.call(
            lambda stubs: stubs.actors.Get(api_pb2.ActorRef(actor_id=self.id))
        )
        self._cached_record = record
        self._ensure_alive(record)
        return record

    def _ensure_alive(self, record: Any) -> None:
        statuses = {
            api_pb2.ACTOR_STATUS_STOPPED: "stopped",
            api_pb2.ACTOR_STATUS_FAILED: "failed",
            api_pb2.ACTOR_STATUS_DELETED: "deleted",
        }
        status = statuses.get(record.status)
        if status is not None:
            raise ActorLostError(self.id, status)


class _BoundRemoteMethod(Generic[R]):
    """One exported method bound to a durable actor or scalable service."""

    def __init__(self, obj: RemoteObject, name: str) -> None:
        self._obj = obj
        self._name = name

    def spawn(self, *args: Any, **kwargs: Any) -> FunctionCall[R]:
        """Create a durable serialized method call."""

        function = self._obj._remote_cls._function
        if self._obj._actor_id is None:
            if self._obj._service_init is None or self._obj._service_key is None:
                raise RuntimeError("service handle has no constructor metadata")
            spawn_service = getattr(function, "_spawn_service", None)
            if not callable(spawn_service):
                raise RuntimeError("RemoteFunction does not support service call envelopes")
            constructor_args, constructor_kwargs = self._obj._service_init
            return spawn_service(
                self._obj._service_key,
                constructor_args,
                dict(constructor_kwargs),
                self._name,
                args,
                kwargs,
            )
        self._obj._record()
        spawn_actor = getattr(function, "_spawn_actor", None)
        if not callable(spawn_actor):
            raise RuntimeError("RemoteFunction does not support actor call targets")
        return spawn_actor(self._obj.id, self._name, args, kwargs)

    def remote(self, *args: Any, **kwargs: Any) -> R:
        """Create the durable method call and wait for its result."""

        return self.spawn(*args, **kwargs).get()

    def __call__(self, *args: Any, **kwargs: Any) -> R:
        return self.remote(*args, **kwargs)


@overload
def cls(cls_: type, /) -> RemoteClass: ...


@overload
def cls(cls_: None = None, /, **kwargs: Any) -> Callable[[type], RemoteClass]: ...


def cls(cls_: type | None = None, /, **kwargs: Any) -> Any:
    """Decorate a class as a strictly serialized durable actor."""

    def decorate(inner: type) -> RemoteClass:
        return RemoteClass(inner, **kwargs)

    return decorate(cls_) if cls_ is not None else decorate


@overload
def service(cls_: type, /) -> RemoteClass: ...


@overload
def service(cls_: None = None, /, **kwargs: Any) -> Callable[[type], RemoteClass]: ...


def service(cls_: type | None = None, /, **kwargs: Any) -> Any:
    """Decorate a class for horizontally scalable, non-durable instances."""

    def decorate(inner: type) -> RemoteClass:
        return RemoteClass(inner, service=True, **kwargs)

    return decorate(cls_) if cls_ is not None else decorate


