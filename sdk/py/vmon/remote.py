"""Source-serialized Python functions executed through a bound client.

``@vmon.function`` wraps a module-level function in a :class:`RemoteFunction`
whose calls run inside a warm microVM: one persistent in-guest runner session
per worker serves ``remote()``/``remote_gen()`` (cached sandbox), ``spawn()``
(dedicated session, shared sandbox), and ``map()``/``starmap()`` (ephemeral
worker sandboxes with greedy scale-up). Arguments and results travel as JSON
when possible and stdlib pickle otherwise; remote exceptions re-raise as their
original type with the guest traceback attached as a ``__notes__`` entry.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import os
import queue
import sys
import threading
import time
import uuid
import weakref
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, overload

from ._remote_runner import _SessionError, _WorkerSession, encode_value
from ._remote_source import bundle_source
from .errors import APIError, ProtocolError, RemoteFunctionError, TransportError

if TYPE_CHECKING:
    from ._remote_source import SourceBundle
    from .client import Client
    from .sandbox import Sandbox

_MAX_INFRA_FAILURES = 3
_CANCELLED_MESSAGE = "remote function call was cancelled"
_UNSET: Any = object()

_SERIALIZERS = ("auto", "json", "pickle")


def is_remote() -> bool:
    """True when running inside a vmon remote-function sandbox."""
    return os.environ.get("VMON_REMOTE") == "1"


def _live_output(stream: str, text: str) -> None:
    target = sys.stderr if stream == "stderr" else sys.stdout
    target.write(text)
    target.flush()


class Retries:
    """A retry policy with exponential (or fixed-interval) backoff, Modal-style."""

    def __init__(
        self,
        *,
        max_retries: int,
        backoff_coefficient: float = 2.0,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
    ) -> None:
        if max_retries < 0:
            raise ValueError(f"max_retries must be non-negative, got {max_retries}")
        if not 1.0 <= backoff_coefficient <= 10.0:
            raise ValueError(
                f"backoff_coefficient must be between 1.0 and 10.0, got {backoff_coefficient}"
            )
        if not 0.0 <= initial_delay <= 60.0:
            raise ValueError(f"initial_delay must be between 0 and 60 seconds, got {initial_delay}")
        if not 1.0 <= max_delay <= 60.0:
            raise ValueError(f"max_delay must be between 1 and 60 seconds, got {max_delay}")
        self.max_retries = int(max_retries)
        self.backoff_coefficient = float(backoff_coefficient)
        self.initial_delay = float(initial_delay)
        self.max_delay = float(max_delay)

    def delay_for(self, retry_number: int) -> float:
        """Delay in seconds before 1-based retry ``retry_number``."""
        return min(
            self.initial_delay * self.backoff_coefficient ** (retry_number - 1),
            self.max_delay,
        )


def _normalize_retries(retries: int | Retries | None) -> Retries | None:
    if retries is None or isinstance(retries, Retries):
        return retries
    if isinstance(retries, int) and not isinstance(retries, bool):
        return Retries(max_retries=retries, backoff_coefficient=1.0, initial_delay=1.0)
    raise TypeError("retries must be an int or a Retries instance")


class _Worker:
    """One sandbox+session slot with a recreate/terminate policy for failures.

    ``dedicated`` sessions belong to this slot (spawn, map); otherwise the
    engine's cached session is used. ``owns_sandbox`` slots (map workers) hold
    an ephemeral sandbox terminated on release; shared slots borrow the
    engine's cached sandbox.
    """

    def __init__(self, engine: _RemoteEngine, *, dedicated: bool, owns_sandbox: bool) -> None:
        self._engine = engine
        self._dedicated = dedicated
        self._owns_sandbox = owns_sandbox
        self._sandbox: Sandbox | None = None
        self._session: _WorkerSession | None = None
        self._lock = threading.Lock()

    def session(self) -> _WorkerSession:
        if not self._dedicated:
            return self._engine.session()
        with self._lock:
            if self._session is not None and self._session.alive:
                return self._session
            if self._session is not None:
                self._session.close()
                self._session = None
            if self._owns_sandbox:
                if self._sandbox is None:
                    self._sandbox = self._engine.create_sandbox()
                sandbox = self._sandbox
            else:
                sandbox = self._engine.ensure_sandbox()
            session = _WorkerSession(sandbox)
            session.start()
            self._session = session
            return session

    def kill_session(self) -> None:
        """Drop the current session so the next attempt starts a fresh one."""
        if not self._dedicated:
            self._engine.drop_session()
            return
        with self._lock:
            if self._session is not None:
                self._session.close()
                self._session = None

    def kill_infra(self) -> None:
        """Drop session and sandbox after an infrastructure failure."""
        self.kill_session()
        if self._owns_sandbox:
            with self._lock:
                sandbox = self._sandbox
                self._sandbox = None
            if sandbox is not None:
                with contextlib.suppress(Exception):
                    sandbox.terminate()
        else:
            # Shared sandbox (cached and spawn slots): recreate through the engine.
            self._engine.drop_sandbox()

    def interrupt(self) -> None:
        """Close the in-flight dedicated session (cancellation path)."""
        with self._lock:
            if self._session is not None:
                self._session.close()

    def release(self) -> None:
        """Tear down whatever this slot owns; shared resources stay warm."""
        with self._lock:
            session = self._session
            sandbox = self._sandbox
            self._session = None
            self._sandbox = None
        if self._dedicated and session is not None:
            session.close()
        if self._owns_sandbox and sandbox is not None:
            with contextlib.suppress(Exception):
                sandbox.terminate()


class _RemoteEngine:
    """Shared machinery behind remote functions and remote class instances.

    Owns the lazy client, the cached sandbox+session, spawn tracking, and the
    dispatch retry loop; :class:`RemoteFunction` and ``vmon.cls`` bind it to a
    function bundle or a class bundle plus init spec respectively.
    """

    def __init__(
        self,
        *,
        client: Client | None,
        serializer: str,
        retries: Retries | None,
        timeout: float | None,
        pool: int,
        sandbox_kwargs: dict[str, Any],
        bundle_factory: Callable[[], SourceBundle],
        init_factory: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        self._client = client
        self._owns_client = client is None
        self.serializer = serializer
        self.retries = retries
        self.timeout = timeout
        self._pool = int(pool)
        self._sandbox_kwargs = sandbox_kwargs
        self._bundle_factory = bundle_factory
        self._init_factory = init_factory
        self._bundle: SourceBundle | None = None
        self._sandbox: Sandbox | None = None
        self._session: _WorkerSession | None = None
        self._lock = threading.RLock()
        self._session_lock = threading.Lock()
        self._spawns: weakref.WeakSet[FunctionCall] = weakref.WeakSet()

    # -- resources ---------------------------------------------------------

    def client(self) -> Client:
        with self._lock:
            if self._client is None:
                from .client import connect

                self._client = connect()
            return self._client

    def bundle(self) -> SourceBundle:
        bundle = self._bundle
        if bundle is None:
            bundle = self._bundle_factory()
            self._bundle = bundle
        return bundle

    def create_sandbox(self) -> Sandbox:
        """Create one ephemeral worker sandbox (map workers)."""
        return self.client().sandboxes.create(**self._create_kwargs())

    def ensure_sandbox(self) -> Sandbox:
        """Return the cached sandbox, recreating it when gone or stopped."""
        with self._lock:
            if self._sandbox is not None:
                try:
                    info = self._sandbox.refresh()
                except APIError as exc:
                    if exc.code != "not_found" and exc.status != 404:
                        raise
                    self._sandbox = None
                else:
                    if info.status in {"stopped", "terminated", "failed"}:
                        self._sandbox = None
            if self._sandbox is None:
                self._sandbox = self.client().sandboxes.create(**self._create_kwargs())
            return self._sandbox

    def session(self) -> _WorkerSession:
        """Return the cached session, recreating it (and its sandbox) as needed."""
        with self._session_lock:
            if self._session is not None and self._session.alive:
                return self._session
            if self._session is not None:
                self._session.close()
                self._session = None
            session = _WorkerSession(self.ensure_sandbox())
            session.start()
            self._session = session
            return session

    def drop_session(self) -> None:
        with self._session_lock:
            if self._session is not None:
                self._session.close()
                self._session = None

    def drop_sandbox(self) -> None:
        """Terminate and forget the cached sandbox (and its session)."""
        self.drop_session()
        with self._lock:
            sandbox = self._sandbox
            self._sandbox = None
        if sandbox is not None:
            with contextlib.suppress(Exception):
                sandbox.terminate()

    def _create_kwargs(self) -> dict[str, Any]:
        default_image = f"python:{sys.version_info.major}.{sys.version_info.minor}-slim"
        kwargs = {"image": default_image, **self._sandbox_kwargs}
        if self._pool > 0:
            kwargs.setdefault("pool_size", self._pool)
        return kwargs

    # -- calls ---------------------------------------------------------------

    def cached_worker(self) -> _Worker:
        return _Worker(self, dedicated=False, owns_sandbox=False)

    def dispatch(
        self,
        worker: _Worker,
        name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        self_call: bool = False,
        mode: str = "value",
        timeout: float | None = _UNSET,
        on_output: Callable[[str, str], None] | None = None,
        abort: Callable[[], bool] | None = None,
        infra_budget: int = _MAX_INFRA_FAILURES,
    ) -> Any:
        """Run one call through the user-policy + infra retry loop."""
        self._precheck(args, kwargs)
        effective_timeout = self.timeout if timeout is _UNSET else timeout
        sink = _live_output if on_output is None else on_output
        policy = self.retries
        user_failures = 0
        infra_failures = 0
        while True:
            if abort is not None and abort():
                raise RemoteFunctionError(_CANCELLED_MESSAGE)
            try:
                session = worker.session()
                return session.call(
                    self.bundle(),
                    name,
                    args,
                    kwargs,
                    serializer=self.serializer,
                    mode=mode,
                    self_call=self_call,
                    init=None if self._init_factory is None else self._init_factory(),
                    timeout=effective_timeout,
                    on_output=sink,
                )
            except _SessionError, APIError, TransportError, ProtocolError:
                worker.kill_infra()
                infra_failures += 1
                if abort is not None and abort():
                    raise RemoteFunctionError(_CANCELLED_MESSAGE) from None
                if infra_failures > infra_budget:
                    raise
            except TimeoutError:
                # The session was killed at the deadline; timeouts follow the
                # user retry policy (Modal parity).
                worker.kill_session()
                user_failures += 1
                if policy is None or user_failures > policy.max_retries:
                    raise
                time.sleep(policy.delay_for(user_failures))
            except Exception:
                user_failures += 1
                if policy is None or user_failures > policy.max_retries:
                    raise
                time.sleep(policy.delay_for(user_failures))

    def _precheck(self, args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> None:
        """Raise serialization errors locally, before any sandbox work."""
        encode_value(list(args), serializer=self.serializer)
        encode_value(dict(kwargs), serializer=self.serializer)

    def spawn_call(
        self,
        name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        self_call: bool = False,
    ) -> FunctionCall:
        call = FunctionCall(self, name, args, kwargs, self_call=self_call)
        self._spawns.add(call)
        call._start()
        return call

    # -- map -----------------------------------------------------------------

    def map_calls(
        self,
        name: str,
        items: Iterator[tuple[Any, ...]],
        kwargs: Mapping[str, Any],
        *,
        self_call: bool = False,
        order_outputs: bool = True,
        return_exceptions: bool = False,
        concurrency: int | None = None,
        timeout: float | None = _UNSET,
    ) -> Iterator[Any]:
        cap = 8 if concurrency is None else int(concurrency)
        if cap < 1:
            raise ValueError("concurrency must be >= 1")
        self._precheck((), kwargs)
        return _MapRun(
            self,
            name,
            items,
            dict(kwargs),
            self_call=self_call,
            order_outputs=order_outputs,
            return_exceptions=return_exceptions,
            cap=cap,
            timeout=timeout,
        ).results()

    # -- lifecycle -------------------------------------------------------------

    def terminate(self, *, shutdown_grace: float = 10.0) -> None:
        """Close spawns and the cached session, then sandbox and owned client."""
        with self._lock:
            spawns = list(self._spawns)
            sandbox = self._sandbox
            self._sandbox = None
            client = self._client if self._owns_client else None
            if self._owns_client:
                self._client = None
        for call in spawns:
            if not call.done():
                call.cancel()
        with self._session_lock:
            session = self._session
            self._session = None
        try:
            if session is not None:
                if session.initialized:
                    session.shutdown(timeout=shutdown_grace)
                else:
                    session.close()
        finally:
            try:
                if sandbox is not None:
                    sandbox.terminate()
            finally:
                if client is not None:
                    client.close()


class FunctionCall:
    """Handle for one spawned remote call, backed by a dedicated session."""

    def __init__(
        self,
        engine: _RemoteEngine,
        name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        self_call: bool,
    ) -> None:
        self.call_id = uuid.uuid4().hex
        self._engine = engine
        self._name = name
        self._args = args
        self._kwargs = kwargs
        self._self_call = self_call
        self._worker = _Worker(engine, dedicated=True, owns_sandbox=False)
        self._finished = threading.Event()
        self._result: Any = None
        self._error: BaseException | None = None
        self._output: list[tuple[str, str]] = []
        self._replayed = False
        self._cancelled = False
        self._thread = threading.Thread(
            target=self._run,
            name=f"vmon-spawn-{self.call_id[:8]}",
            daemon=True,
        )

    def _start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        try:
            self._result = self._engine.dispatch(
                self._worker,
                self._name,
                self._args,
                self._kwargs,
                self_call=self._self_call,
                on_output=lambda stream, text: self._output.append((stream, text)),
                abort=lambda: self._cancelled,
            )
        except BaseException as exc:
            self._error = RemoteFunctionError(_CANCELLED_MESSAGE) if self._cancelled else exc
        finally:
            self._worker.release()
            self._finished.set()

    def done(self) -> bool:
        """True once the call has a result, error, or cancellation."""
        return self._finished.is_set()

    def get(self, timeout: float | None = None) -> Any:
        """Wait for completion, replay buffered output once, return or re-raise."""
        if not self._finished.wait(timeout):
            raise TimeoutError(f"remote function call did not finish within {timeout}s")
        if not self._replayed:
            self._replayed = True
            for stream, text in self._output:
                _live_output(stream, text)
        if self._error is not None:
            raise self._error
        return self._result

    def cancel(self) -> None:
        """Kill this call's session; a later ``get()`` raises the cancellation."""
        self._cancelled = True
        self._worker.interrupt()

    @staticmethod
    def gather(*calls: FunctionCall, return_exceptions: bool = False) -> list[Any]:
        """Wait for every call in order and collect results (or exceptions)."""
        results: list[Any] = []
        for call in calls:
            try:
                results.append(call.get())
            except Exception as exc:
                if not return_exceptions:
                    raise
                results.append(exc)
        return results


class _MapRun:
    """One map()/starmap() execution: feeder, scaling workers, ordered emit."""

    def __init__(
        self,
        engine: _RemoteEngine,
        name: str,
        items: Iterator[tuple[Any, ...]],
        kwargs: dict[str, Any],
        *,
        self_call: bool,
        order_outputs: bool,
        return_exceptions: bool,
        cap: int,
        timeout: float | None,
    ) -> None:
        self._engine = engine
        self._name = name
        self._items = items
        self._kwargs = kwargs
        self._self_call = self_call
        self._order_outputs = order_outputs
        self._return_exceptions = return_exceptions
        self._cap = cap
        self._timeout = timeout
        self._in_q: queue.Queue[tuple[int, tuple[Any, ...]]] = queue.Queue(maxsize=2 * cap)
        self._out_q: queue.Queue[tuple[int, Any, BaseException | None, list[tuple[str, str]]]] = (
            queue.Queue()
        )
        self._retry_items: deque[tuple[int, tuple[Any, ...]]] = deque()
        self._requeues: dict[int, int] = {}
        self._stop = threading.Event()
        self._feeder_done = threading.Event()
        self._feeder_error: list[BaseException] = []
        self._state_lock = threading.Lock()
        self._dispatched = 0
        self._busy = 0
        self._workers: list[threading.Thread] = []

    def results(self) -> Iterator[Any]:
        feeder = threading.Thread(target=self._feed, name="vmon-map-feeder", daemon=True)
        completed = 0
        next_emit = 0
        buffered: dict[int, tuple[Any, list[tuple[str, str]]]] = {}
        try:
            feeder.start()
            self._spawn_worker()
            while True:
                if self._feeder_error:
                    raise self._feeder_error[0]
                with self._state_lock:
                    total = self._dispatched
                if self._feeder_done.is_set() and completed >= total and not self._retry_items:
                    return
                self._scale_up()
                try:
                    index, value, error, output = self._out_q.get(timeout=0.05)
                except queue.Empty:
                    continue
                completed += 1
                if error is not None and not self._return_exceptions:
                    raise error
                entry = value if error is None else error
                if not self._order_outputs:
                    self._replay(output)
                    yield entry
                    continue
                buffered[index] = (entry, output)
                while next_emit in buffered:
                    item, chunks = buffered.pop(next_emit)
                    next_emit += 1
                    self._replay(chunks)
                    yield item
        finally:
            self._stop.set()
            feeder.join(timeout=5)
            for worker in self._workers:
                worker.join(timeout=30)

    @staticmethod
    def _replay(chunks: list[tuple[str, str]]) -> None:
        for stream, text in chunks:
            _live_output(stream, text)

    def _feed(self) -> None:
        try:
            for index, item in enumerate(self._items):
                with self._state_lock:
                    self._dispatched += 1
                while not self._stop.is_set():
                    try:
                        self._in_q.put((index, item), timeout=0.1)
                        break
                    except queue.Full:
                        continue
                if self._stop.is_set():
                    return
        except BaseException as exc:
            self._feeder_error.append(exc)
        finally:
            self._feeder_done.set()

    def _spawn_worker(self) -> None:
        worker = threading.Thread(
            target=self._work,
            name=f"vmon-map-{len(self._workers)}",
            daemon=True,
        )
        self._workers.append(worker)
        worker.start()

    def _scale_up(self) -> None:
        with self._state_lock:
            queued = bool(self._retry_items) or not self._in_q.empty()
            live = sum(1 for worker in self._workers if worker.is_alive())
            idle = live - self._busy
        if queued and idle <= 0 and live < self._cap and not self._stop.is_set():
            self._spawn_worker()

    def _next_item(self) -> tuple[int, tuple[Any, ...]] | None:
        while not self._stop.is_set():
            with self._state_lock:
                if self._retry_items:
                    return self._retry_items.popleft()
            try:
                return self._in_q.get(timeout=0.05)
            except queue.Empty:
                if self._feeder_done.is_set() and self._in_q.empty() and not self._retry_items:
                    return None
        return None

    def _work(self) -> None:
        slot = _Worker(self._engine, dedicated=True, owns_sandbox=True)
        try:
            while True:
                pending = self._next_item()
                if pending is None:
                    return
                with self._state_lock:
                    self._busy += 1
                try:
                    if not self._run_item(slot, pending):
                        return
                finally:
                    with self._state_lock:
                        self._busy -= 1
        finally:
            slot.release()

    def _run_item(self, slot: _Worker, pending: tuple[int, tuple[Any, ...]]) -> bool:
        """Run one item on ``slot``; False retires this worker."""
        index, item = pending
        output: list[tuple[str, str]] = []
        try:
            value = self._engine.dispatch(
                slot,
                self._name,
                item,
                self._kwargs,
                self_call=self._self_call,
                timeout=self._timeout,
                on_output=lambda stream, text: output.append((stream, text)),
                infra_budget=0,
            )
        except (_SessionError, APIError, TransportError, ProtocolError) as exc:
            # Worker infrastructure failed: retire this worker and requeue the
            # item (bounded) for a replacement worker.
            with self._state_lock:
                count = self._requeues.get(index, 0) + 1
                self._requeues[index] = count
            if count > _MAX_INFRA_FAILURES:
                self._out_q.put((index, None, exc, output))
            else:
                with self._state_lock:
                    self._retry_items.append((index, item))
            return False
        except BaseException as exc:
            self._out_q.put((index, None, exc, output))
            if not self._return_exceptions:
                self._stop.set()
                return False
            return True
        self._out_q.put((index, value, None, output))
        return True


class _AsyncFunctionCall:
    """Thread-backed async facade over :class:`FunctionCall`."""

    def __init__(self, call: FunctionCall) -> None:
        self._call = call

    @property
    def call_id(self) -> str:
        return self._call.call_id

    def done(self) -> bool:
        return self._call.done()

    def cancel(self) -> None:
        self._call.cancel()

    async def get(self, timeout: float | None = None) -> Any:
        return await asyncio.to_thread(self._call.get, timeout)

    @staticmethod
    async def gather(*calls: _AsyncFunctionCall, return_exceptions: bool = False) -> list[Any]:
        return await asyncio.to_thread(
            FunctionCall.gather,
            *(call._call for call in calls),
            return_exceptions=return_exceptions,
        )


def _aiter_from_thread(make_iter: Callable[[], Iterator[Any]]) -> AsyncIterator[Any]:
    """Bridge a blocking iterator factory into an async iterator."""

    async def agen() -> AsyncIterator[Any]:
        bridge: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=32)

        def pump() -> None:
            try:
                for item in make_iter():
                    bridge.put(("item", item))
            except BaseException as exc:
                bridge.put(("error", exc))
                return
            bridge.put(("done", None))

        threading.Thread(target=pump, name="vmon-aiter", daemon=True).start()
        while True:
            kind, payload = await asyncio.to_thread(bridge.get)
            if kind == "item":
                yield payload
            elif kind == "error":
                raise payload
            else:
                return

    return agen()


class _AsyncRemoteFunction:
    """Thread-backed async facade over :class:`RemoteFunction`."""

    def __init__(self, function: RemoteFunction) -> None:
        self._function = function

    async def remote(self, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self._function.remote, *args, **kwargs)

    def remote_gen(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return _aiter_from_thread(lambda: self._function.remote_gen(*args, **kwargs))

    async def spawn(self, *args: Any, **kwargs: Any) -> _AsyncFunctionCall:
        return _AsyncFunctionCall(await asyncio.to_thread(self._function.spawn, *args, **kwargs))

    def map(self, *iterables: Iterable[Any], **kwargs: Any) -> AsyncIterator[Any]:
        return _aiter_from_thread(lambda: self._function.map(*iterables, **kwargs))

    def starmap(self, iterable: Iterable[Iterable[Any]], **kwargs: Any) -> AsyncIterator[Any]:
        return _aiter_from_thread(lambda: self._function.starmap(iterable, **kwargs))

    async def for_each(self, *iterables: Iterable[Any], **kwargs: Any) -> None:
        await asyncio.to_thread(self._function.for_each, *iterables, **kwargs)


class RemoteFunction:
    """A source-available Python function bound lazily to a client and sandbox."""

    def __init__(
        self,
        function: Callable[..., Any],
        *,
        client: Client | None = None,
        serializer: str = "auto",
        retries: int | Retries | None = None,
        timeout: float | None = None,
        pool: int = 0,
        include: Sequence[Callable[..., Any] | type] = (),
        **sandbox_kwargs: Any,
    ) -> None:
        if serializer not in _SERIALIZERS:
            raise ValueError(f"serializer must be one of {_SERIALIZERS}, got {serializer!r}")
        self._function = function
        self._engine = _RemoteEngine(
            client=client,
            serializer=serializer,
            retries=_normalize_retries(retries),
            timeout=timeout,
            pool=pool,
            sandbox_kwargs=dict(sandbox_kwargs),
            bundle_factory=lambda: bundle_source(function, include),
        )
        self._is_generator = inspect.isgeneratorfunction(function) or inspect.isasyncgenfunction(
            function
        )
        functools.update_wrapper(self, function)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._function(*args, **kwargs)

    def local(self, *args: Any, **kwargs: Any) -> Any:
        """Run the wrapped function in this process (alias of calling it)."""
        return self._function(*args, **kwargs)

    @property
    def aio(self) -> _AsyncRemoteFunction:
        """Async ``remote``/``remote_gen``/``spawn``/``map``/``starmap``/``for_each``."""
        return _AsyncRemoteFunction(self)

    def remote(self, *args: Any, timeout: float | None = _UNSET, **kwargs: Any) -> Any:
        """Run the function in the cached warm sandbox and return its value."""
        self._reject_generator("called with .remote(); use .remote_gen()")
        return self._engine.dispatch(
            self._engine.cached_worker(),
            self._function.__name__,
            args,
            kwargs,
            timeout=timeout,
        )

    def remote_gen(
        self, *args: Any, timeout: float | None = _UNSET, **kwargs: Any
    ) -> Iterator[Any]:
        """Stream a remote generator's yields as they are produced."""
        if not self._is_generator:
            raise ValueError(
                "a non-generator function cannot be called with .remote_gen(); use .remote()"
            )
        return self._engine.dispatch(
            self._engine.cached_worker(),
            self._function.__name__,
            args,
            kwargs,
            mode="iter",
            timeout=timeout,
        )

    def spawn(self, *args: Any, **kwargs: Any) -> FunctionCall:
        """Start the call on a dedicated session and return its handle."""
        self._reject_generator("spawned; use .remote_gen()")
        return self._engine.spawn_call(self._function.__name__, args, kwargs)

    def map(
        self,
        *input_iterables: Iterable[Any],
        kwargs: Mapping[str, Any] | None = None,
        order_outputs: bool = True,
        return_exceptions: bool = False,
        concurrency: int | None = None,
        timeout: float | None = _UNSET,
    ) -> Iterator[Any]:
        """Lazily run one call per zipped input item across worker sandboxes."""
        self._reject_generator("mapped; use .remote_gen()")
        if not input_iterables:
            raise ValueError("map requires at least one input iterable")
        return self._engine.map_calls(
            self._function.__name__,
            zip(*input_iterables, strict=False),
            dict(kwargs or {}),
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
        self._reject_generator("mapped; use .remote_gen()")
        return self._engine.map_calls(
            self._function.__name__,
            (tuple(item) for item in input_iterable),
            dict(kwargs or {}),
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

    def terminate(self) -> None:
        """Close sessions and spawns, terminate the sandbox, close an owned client."""
        self._engine.terminate()

    def _reject_generator(self, action: str) -> None:
        if self._is_generator:
            raise ValueError(f"a generator function cannot be {action}")


@overload
def function(function: Callable[..., Any], /) -> RemoteFunction: ...


@overload
def function(
    function: None = None,
    /,
    *,
    client: Client | None = None,
    serializer: str = "auto",
    retries: int | Retries | None = None,
    timeout: float | None = None,
    pool: int = 0,
    include: Sequence[Callable[..., Any] | type] = (),
    **sandbox_kwargs: Any,
) -> Callable[[Callable[..., Any]], RemoteFunction]: ...


def function(
    function: Callable[..., Any] | None = None,
    /,
    *,
    client: Client | None = None,
    serializer: str = "auto",
    retries: int | Retries | None = None,
    timeout: float | None = None,
    pool: int = 0,
    include: Sequence[Callable[..., Any] | type] = (),
    **sandbox_kwargs: Any,
) -> RemoteFunction | Callable[[Callable[..., Any]], RemoteFunction]:
    """Decorate a function with warm ``remote``, ``spawn``, ``map``, and streaming calls."""

    def decorate(inner: Callable[..., Any]) -> RemoteFunction:
        return RemoteFunction(
            inner,
            client=client,
            serializer=serializer,
            retries=retries,
            timeout=timeout,
            pool=pool,
            include=include,
            **sandbox_kwargs,
        )

    if function is not None:
        return decorate(function)
    return decorate
