"""Stateful remote classes: ``@vmon.cls`` with lifecycle hooks, Modal-style.

A decorated class runs inside a warm microVM session: constructing it returns
a :class:`RemoteObject` whose ``@vmon.method``-tagged methods expose the full
remote surface (``remote``/``remote_gen``/``spawn``/``map``/``starmap``/
``for_each``/``local``/``aio``). The guest instance is created once per
session (constructor plus ``@vmon.enter`` hooks) and keeps in-memory state
across calls; ``terminate()`` runs ``@vmon.exit`` hooks with a grace period.
Session death (timeout, cancel, sandbox loss) drops that state and the next
call transparently re-initializes — the same semantics as a Modal container
replacement.
"""

from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, overload

from ._remote_source import bundle_source
from .remote import (
    _UNSET,
    FunctionCall,
    Retries,
    _aiter_from_thread,
    _AsyncFunctionCall,
    _normalize_retries,
    _RemoteEngine,
)

if TYPE_CHECKING:
    from .client import Client


def method(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Tag a method of a ``@vmon.cls`` class as remotely callable."""
    fn.__vmon_method__ = True  # type: ignore[attr-defined]
    return fn


def enter(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Tag a method to run once when the guest instance is created."""
    fn.__vmon_enter__ = True  # type: ignore[attr-defined]
    return fn


def exit(fn: Callable[..., Any]) -> Callable[..., Any]:  # noqa: A001
    """Tag a method to run when the instance's session shuts down."""
    fn.__vmon_exit__ = True  # type: ignore[attr-defined]
    return fn


@dataclass(frozen=True, slots=True)
class _ClsConfig:
    """Engine settings captured by ``@vmon.cls`` for each instance."""

    client: Client | None
    serializer: str
    retries: Retries | None
    timeout: float | None
    pool: int
    sandbox_kwargs: dict[str, Any]


class RemoteClass:
    """A ``@vmon.cls``-decorated class; calling it yields a :class:`RemoteObject`."""

    def __init__(
        self,
        user_cls: type,
        *,
        client: Client | None = None,
        serializer: str = "auto",
        retries: int | Retries | None = None,
        timeout: float | None = None,
        pool: int = 0,
        include: Sequence[Callable[..., Any] | type] = (),
        **sandbox_kwargs: Any,
    ) -> None:
        self._cls = user_cls
        self._config = _ClsConfig(
            client=client,
            serializer=serializer,
            retries=_normalize_retries(retries),
            timeout=timeout,
            pool=int(pool),
            sandbox_kwargs=dict(sandbox_kwargs),
        )
        self._include = tuple(include)
        self._methods = tuple(
            name
            for name, member in vars(user_cls).items()
            if getattr(member, "__vmon_method__", False)
        )
        self._enter = tuple(
            name
            for name, member in vars(user_cls).items()
            if getattr(member, "__vmon_enter__", False)
        )
        self._exit = tuple(
            name
            for name, member in vars(user_cls).items()
            if getattr(member, "__vmon_exit__", False)
        )
        functools.update_wrapper(self, user_cls, updated=())

    def __call__(self, *args: Any, **kwargs: Any) -> RemoteObject:
        return RemoteObject(self, args, kwargs)


class RemoteObject:
    """One parametrized instance of a remote class, owning its sandbox+session."""

    def __init__(
        self,
        remote_cls: RemoteClass,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> None:
        config = remote_cls._config
        user_cls = remote_cls._cls
        init_spec = {
            "name": user_cls.__name__,
            "args": args,
            "kwargs": dict(kwargs),
            "enter": list(remote_cls._enter),
            "exit": list(remote_cls._exit),
        }
        self._remote_cls = remote_cls
        self._args = args
        self._kwargs = dict(kwargs)
        self._local_instance: Any = None
        self._local_lock = threading.Lock()
        self._engine = _RemoteEngine(
            client=config.client,
            serializer=config.serializer,
            retries=config.retries,
            timeout=config.timeout,
            pool=config.pool,
            sandbox_kwargs=dict(config.sandbox_kwargs),
            bundle_factory=lambda: bundle_source(user_cls, remote_cls._include),
            init_factory=lambda: init_spec,
        )

    def __getattr__(self, name: str) -> _BoundRemoteMethod:
        # Only consulted for names missing on the instance: i.e. remote methods.
        if name in self._remote_cls._methods:
            return _BoundRemoteMethod(self, name)
        raise AttributeError(
            f"{name!r} is not a @vmon.method; only @vmon.method-decorated methods "
            "are callable remotely"
        )

    def terminate(self) -> None:
        """Run ``@vmon.exit`` hooks (10s grace), then tear down the sandbox."""
        self._engine.terminate()

    def _local_target(self, name: str) -> Callable[..., Any]:
        """The method bound to a lazily-built local instance (enter hooks ran once)."""
        with self._local_lock:
            if self._local_instance is None:
                instance = self._remote_cls._cls(*self._args, **self._kwargs)
                for hook in self._remote_cls._enter:
                    getattr(instance, hook)()
                self._local_instance = instance
        return getattr(self._local_instance, name)

    def __enter__(self) -> RemoteObject:
        return self

    def __exit__(self, *exc: object) -> None:
        self.terminate()


class _BoundRemoteMethod:
    """A ``@vmon.method`` bound to one :class:`RemoteObject`."""

    def __init__(self, obj: RemoteObject, name: str) -> None:
        self._obj = obj
        self._name = name
        self._engine = obj._engine

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.local(*args, **kwargs)

    def local(self, *args: Any, **kwargs: Any) -> Any:
        """Run on a local instance of the class (constructor + enter hooks once)."""
        return self._obj._local_target(self._name)(*args, **kwargs)

    @property
    def aio(self) -> _AsyncBoundRemoteMethod:
        """Thread-backed async facade for this bound method."""
        return _AsyncBoundRemoteMethod(self)

    def remote(self, *args: Any, timeout: float | None = _UNSET, **kwargs: Any) -> Any:
        """Call the method on the warm guest instance."""
        return self._engine.dispatch(
            self._engine.cached_worker(),
            self._name,
            args,
            kwargs,
            self_call=True,
            timeout=timeout,
        )

    def remote_gen(
        self, *args: Any, timeout: float | None = _UNSET, **kwargs: Any
    ) -> Iterator[Any]:
        """Stream a generator method's yields as they are produced."""
        return self._engine.dispatch(
            self._engine.cached_worker(),
            self._name,
            args,
            kwargs,
            self_call=True,
            mode="iter",
            timeout=timeout,
        )

    def spawn(self, *args: Any, **kwargs: Any) -> FunctionCall:
        """Start the method call on a dedicated session and return its handle."""
        return self._engine.spawn_call(self._name, args, kwargs, self_call=True)

    def map(
        self,
        *input_iterables: Iterable[Any],
        kwargs: Mapping[str, Any] | None = None,
        order_outputs: bool = True,
        return_exceptions: bool = False,
        concurrency: int | None = None,
        timeout: float | None = _UNSET,
    ) -> Iterator[Any]:
        """Lazily map the method across worker sandboxes (each gets its own instance)."""
        if not input_iterables:
            raise ValueError("map requires at least one input iterable")
        return self._engine.map_calls(
            self._name,
            zip(*input_iterables, strict=False),
            dict(kwargs or {}),
            self_call=True,
            order_outputs=order_outputs,
            return_exceptions=return_exceptions,
            concurrency=concurrency,
            timeout=timeout,
        )

    def starmap(
        self,
        input_iterable: Iterable[Iterable[Any]],
        *,
        kwargs: Mapping[str, Any] | None = None,
        order_outputs: bool = True,
        return_exceptions: bool = False,
        concurrency: int | None = None,
        timeout: float | None = _UNSET,
    ) -> Iterator[Any]:
        """Like :meth:`map` but each item is unpacked as the argument tuple."""
        return self._engine.map_calls(
            self._name,
            (tuple(item) for item in input_iterable),
            dict(kwargs or {}),
            self_call=True,
            order_outputs=order_outputs,
            return_exceptions=return_exceptions,
            concurrency=concurrency,
            timeout=timeout,
        )

    def for_each(
        self,
        *input_iterables: Iterable[Any],
        kwargs: Mapping[str, Any] | None = None,
        ignore_exceptions: bool = False,
        concurrency: int | None = None,
    ) -> None:
        """Run :meth:`map` for its side effects, discarding results."""
        for _ in self.map(
            *input_iterables,
            kwargs=kwargs,
            order_outputs=False,
            return_exceptions=ignore_exceptions,
            concurrency=concurrency,
        ):
            pass


class _AsyncBoundRemoteMethod:
    """Thread-backed async facade over :class:`_BoundRemoteMethod`."""

    def __init__(self, bound: _BoundRemoteMethod) -> None:
        self._bound = bound

    async def remote(self, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self._bound.remote, *args, **kwargs)

    def remote_gen(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return _aiter_from_thread(lambda: self._bound.remote_gen(*args, **kwargs))

    async def spawn(self, *args: Any, **kwargs: Any) -> _AsyncFunctionCall:
        return _AsyncFunctionCall(await asyncio.to_thread(self._bound.spawn, *args, **kwargs))

    def map(self, *iterables: Iterable[Any], **kwargs: Any) -> AsyncIterator[Any]:
        return _aiter_from_thread(lambda: self._bound.map(*iterables, **kwargs))

    def starmap(self, iterable: Iterable[Iterable[Any]], **kwargs: Any) -> AsyncIterator[Any]:
        return _aiter_from_thread(lambda: self._bound.starmap(iterable, **kwargs))

    async def for_each(self, *iterables: Iterable[Any], **kwargs: Any) -> None:
        await asyncio.to_thread(self._bound.for_each, *iterables, **kwargs)


@overload
def cls(cls_: type, /) -> RemoteClass: ...


@overload
def cls(
    cls_: None = None,
    /,
    *,
    client: Client | None = None,
    serializer: str = "auto",
    retries: int | Retries | None = None,
    timeout: float | None = None,
    pool: int = 0,
    include: Sequence[Callable[..., Any] | type] = (),
    **sandbox_kwargs: Any,
) -> Callable[[type], RemoteClass]: ...


def cls(
    cls_: type | None = None,
    /,
    *,
    client: Client | None = None,
    serializer: str = "auto",
    retries: int | Retries | None = None,
    timeout: float | None = None,
    pool: int = 0,
    include: Sequence[Callable[..., Any] | type] = (),
    **sandbox_kwargs: Any,
) -> RemoteClass | Callable[[type], RemoteClass]:
    """Decorate a class whose instances hold warm in-guest state across calls."""

    def decorate(inner: type) -> RemoteClass:
        return RemoteClass(
            inner,
            client=client,
            serializer=serializer,
            retries=retries,
            timeout=timeout,
            pool=pool,
            include=include,
            **sandbox_kwargs,
        )

    if cls_ is not None:
        return decorate(cls_)
    return decorate
