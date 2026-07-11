"""Persistent in-guest session runner and its client-side protocol driver.

One long-lived ``python3 -u /tmp/vmon-fn-runner.py`` process per worker,
driven over ``sandbox.exec``'s WebSocket with NDJSON frames on stdin/stdout.
Values travel as ``{"json": ...}``, ``{"pickle": "<b64>"}``, or — above
``_INLINE_LIMIT`` bytes — ``{"file": "/tmp/vmon-fn/<uuid>", "format": ...}``
spill files. See ``_WorkerSession`` for the client half.
"""

from __future__ import annotations

import base64
import builtins
import contextlib
import itertools
import json
import math
import pickle
import threading
import time
import uuid
import weakref
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, NoReturn

from .errors import RemoteFunctionError

if TYPE_CHECKING:
    from .process import Process
    from .sandbox import Sandbox

_INLINE_LIMIT = 512 * 1024
_RUNNER_PATH = "/tmp/vmon-fn-runner.py"
_SPILL_DIR = "/tmp/vmon-fn"
_HELLO_TIMEOUT = 30.0

_MISSING_PYTHON_HINT = (
    "remote function images must provide python3; pass image='python:3.14-slim' "
    "or another Python-capable image"
)

# The guest program. Must stay Python >= 3.8 compatible: guest images vary.
_SESSION_RUNNER = r'''
import asyncio
import base64
import inspect
import io
import json
import math
import os
import pickle
import sys
import threading
import traceback
import types
import uuid

PROTOCOL = min(5, pickle.HIGHEST_PROTOCOL)
INLINE_LIMIT = int(os.environ.get("VMON_FN_INLINE_LIMIT") or 524288)
SPILL_DIR = "/tmp/vmon-fn"

# Keep fd 1 as the private protocol channel; stray fd-level writes (user
# subprocesses, C extensions) are rerouted to the exec stderr channel.
_wire = os.fdopen(os.dup(1), "wb")
os.dup2(2, 1)
_ops = sys.stdin
sys.stdin = open(os.devnull, "r")
_wire_lock = threading.Lock()
_current = {"id": None}
_namespaces = {}
_instance = {"obj": None, "exit": []}


def _send(obj):
    data = (json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")
    with _wire_lock:
        _wire.write(data)
        _wire.flush()


class _OutProxy(io.TextIOBase):
    """Line/8KiB-buffered text stream that ships user output as protocol events."""

    def __init__(self, stream):
        self._stream = stream
        self._chunks = []
        self._size = 0

    def writable(self):
        return True

    def write(self, text):
        if not isinstance(text, str):
            text = str(text)
        if not text:
            return 0
        self._chunks.append(text)
        self._size += len(text)
        if self._size >= 8192 or "\n" in text:
            self.flush()
        return len(text)

    def flush(self):
        if self._chunks:
            data = "".join(self._chunks)
            self._chunks = []
            self._size = 0
            _send({"event": "out", "id": _current["id"], "stream": self._stream, "data": data})


def _flush_output():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass


def _json_clean(value):
    if value is None or type(value) in (str, int, bool):
        return True
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is list:
        return all(_json_clean(item) for item in value)
    if type(value) is dict:
        return all(type(key) is str and _json_clean(item) for key, item in value.items())
    return False


def _spill(data):
    os.makedirs(SPILL_DIR, exist_ok=True)
    path = os.path.join(SPILL_DIR, uuid.uuid4().hex)
    with open(path, "wb") as fh:
        fh.write(data)
    return path


def _encode_value(value, serializer):
    if serializer != "pickle" and (serializer == "json" or _json_clean(value)):
        try:
            data = json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError):
            raise TypeError("remote function result must be JSON-serializable")
        if len(data) > INLINE_LIMIT:
            return {"file": _spill(data), "format": "json"}
        return {"json": value}
    data = pickle.dumps(value, protocol=PROTOCOL)
    if len(data) > INLINE_LIMIT:
        return {"file": _spill(data), "format": "pickle"}
    return {"pickle": base64.b64encode(data).decode("ascii")}


def _decode_value(frame):
    if "json" in frame:
        return frame["json"]
    if "pickle" in frame:
        return pickle.loads(base64.b64decode(frame["pickle"]))
    if "file" in frame:
        path = frame["file"]
        with open(path, "rb") as fh:
            data = fh.read()
        try:
            os.remove(path)
        except OSError:
            pass
        if frame.get("format") == "pickle":
            return pickle.loads(data)
        return json.loads(data.decode("utf-8"))
    raise RuntimeError("malformed value frame")


def _load_namespace(op):
    key = op["hash"]
    module = _namespaces.get(key)
    if module is not None:
        return module
    source = op.get("source")
    if source is None:
        raise RuntimeError("unknown source hash %s" % key)
    module_name = op.get("module") or "__vmon_remote__"
    module = types.ModuleType(module_name)
    module.__dict__["__builtins__"] = __builtins__
    _namespaces[key] = module
    if module_name not in sys.modules:
        sys.modules[module_name] = module
    exec(compile(source, "<vmon-remote>", "exec"), module.__dict__)
    if module_name == "__main__":
        main_module = sys.modules.get("__main__")
        if main_module is not None and main_module is not module:
            for name, value in module.__dict__.items():
                if not name.startswith("__"):
                    setattr(main_module, name, value)
    return module


def _call_args(op):
    args = _decode_value(op["args"]) if "args" in op else []
    kwargs = _decode_value(op["kwargs"]) if "kwargs" in op else {}
    return list(args or []), dict(kwargs or {})


def _emit(event, op_id, value, serializer):
    _flush_output()
    frame = {"event": event, "id": op_id}
    frame.update(_encode_value(value, serializer))
    _send(frame)


def _handle_call(op):
    serializer = op.get("serializer") or "auto"
    module = _load_namespace(op)
    if op.get("self"):
        obj = _instance["obj"]
        if obj is None:
            raise RuntimeError("session has no initialized instance")
        target = getattr(obj, op["name"])
    else:
        try:
            target = module.__dict__[op["name"]]
        except KeyError:
            raise NameError("remote source does not define %r" % op["name"])
    args, kwargs = _call_args(op)
    result = target(*args, **kwargs)
    if (op.get("mode") or "value") == "iter":
        if inspect.isawaitable(result):
            result = asyncio.run(result)
        if inspect.isgenerator(result):
            for item in result:
                _emit("yield", op["id"], item, serializer)
        elif inspect.isasyncgen(result):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                while True:
                    try:
                        item = loop.run_until_complete(result.__anext__())
                    except StopAsyncIteration:
                        break
                    _emit("yield", op["id"], item, serializer)
            finally:
                asyncio.set_event_loop(None)
                loop.close()
        else:
            raise TypeError("remote function did not return a generator; use .remote()")
        _emit("result", op["id"], None, "json")
        return
    if inspect.isawaitable(result):
        result = asyncio.run(result)
    if inspect.isgenerator(result) or inspect.isasyncgen(result):
        raise TypeError("remote function returned a generator; call it with .remote_gen()")
    _emit("result", op["id"], result, serializer)


def _run_hook(obj, name):
    result = getattr(obj, name)()
    if inspect.isawaitable(result):
        asyncio.run(result)


def _handle_init(op):
    module = _load_namespace(op)
    cls = module.__dict__.get(op["name"])
    if cls is None:
        raise NameError("remote source does not define %r" % op["name"])
    args, kwargs = _call_args(op)
    obj = cls(*args, **kwargs)
    for name in list(op.get("enter") or []):
        _run_hook(obj, name)
    _instance["obj"] = obj
    _instance["exit"] = list(op.get("exit") or [])
    _emit("result", op["id"], None, "json")


def _handle_shutdown(op):
    obj = _instance["obj"]
    if obj is not None:
        for name in _instance["exit"]:
            try:
                _run_hook(obj, name)
            except BaseException:
                traceback.print_exc()
    _instance["obj"] = None
    _instance["exit"] = []
    _emit("result", op["id"], None, "json")


def _send_error(op_id, exc, serializer):
    _flush_output()
    frame = {
        "event": "error",
        "id": op_id,
        "etype": type(exc).__name__,
        "emod": type(exc).__module__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
        "pickle": None,
    }
    if serializer != "json":
        try:
            data = pickle.dumps(exc, protocol=PROTOCOL)
            if len(data) <= INLINE_LIMIT:
                frame["pickle"] = base64.b64encode(data).decode("ascii")
        except Exception:
            pass
    _send(frame)


def _main():
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
    except OSError:
        pass
    sys.stdout = _OutProxy("stdout")
    sys.stderr = _OutProxy("stderr")
    _send({"event": "hello", "py": list(sys.version_info[:3]), "pickle": pickle.HIGHEST_PROTOCOL})
    while True:
        line = _ops.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        op = json.loads(line)
        op_id = op.get("id")
        _current["id"] = op_id
        kind = op.get("op")
        try:
            if kind == "call":
                _handle_call(op)
            elif kind == "init":
                _handle_init(op)
            elif kind == "shutdown":
                _handle_shutdown(op)
                break
            else:
                raise RuntimeError("unknown op %r" % kind)
        except BaseException as exc:
            _send_error(op_id, exc, op.get("serializer") or "auto")
        finally:
            _current["id"] = None


_main()
'''


def _json_clean(value: Any) -> bool:
    """True when ``value`` survives a JSON round trip with full fidelity."""
    if value is None or type(value) in (str, int, bool):
        return True
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is list:
        return all(_json_clean(item) for item in value)
    if type(value) is dict:
        return all(type(key) is str and _json_clean(item) for key, item in value.items())
    return False


def encode_value(
    value: Any,
    *,
    serializer: str,
    pickle_protocol: int = 5,
    spill: Callable[[bytes, str], str] | None = None,
) -> dict[str, Any]:
    """Encode one protocol value frame per the ``serializer`` policy.

    ``spill(data, format) -> path`` stores payloads above ``_INLINE_LIMIT``
    out-of-band; without it every payload is inlined. ``serializer="json"``
    raises ``ValueError`` for unserializable values.
    """
    if serializer != "pickle" and (serializer == "json" or _json_clean(value)):
        try:
            data = json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("remote function arguments must be JSON-serializable") from exc
        if spill is not None and len(data) > _INLINE_LIMIT:
            return {"file": spill(data, "json"), "format": "json"}
        return {"json": value}
    data = pickle.dumps(value, protocol=pickle_protocol)
    if spill is not None and len(data) > _INLINE_LIMIT:
        return {"file": spill(data, "pickle"), "format": "pickle"}
    return {"pickle": base64.b64encode(data).decode("ascii")}


def decode_value(
    frame: Mapping[str, Any],
    *,
    fetch: Callable[[str], bytes] | None = None,
) -> Any:
    """Decode one protocol value frame; ``fetch(path)`` reads (and deletes) spill files."""
    if "json" in frame:
        return frame["json"]
    if "pickle" in frame:
        return pickle.loads(base64.b64decode(frame["pickle"]))
    if "file" in frame:
        if fetch is None:
            raise RemoteFunctionError("remote value used a spill file but no fetcher is available")
        data = fetch(str(frame["file"]))
        if frame.get("format") == "pickle":
            return pickle.loads(data)
        return json.loads(data.decode("utf-8"))
    raise RemoteFunctionError("remote value frame is malformed")


def raise_remote_error(frame: Mapping[str, Any]) -> NoReturn:
    """Re-raise a remote ``error`` event as faithfully as possible.

    Prefers the pickled original exception, then a builtin reconstructed from
    ``etype``/``message``, then ``RemoteFunctionError``. Reconstructed
    exceptions carry the remote traceback as a ``__notes__`` entry.
    """
    etype = str(frame.get("etype") or "RemoteError")
    emod = str(frame.get("emod") or "")
    message = str(frame.get("message") or "remote function failed")
    traceback_text = str(frame.get("traceback") or "")
    exc: BaseException | None = None
    encoded = frame.get("pickle")
    if isinstance(encoded, str):
        try:
            candidate = pickle.loads(base64.b64decode(encoded))
        except Exception:
            candidate = None
        if isinstance(candidate, BaseException):
            exc = candidate
    if exc is None and emod == "builtins":
        builtin = getattr(builtins, etype, None)
        if isinstance(builtin, type) and issubclass(builtin, BaseException):
            with contextlib.suppress(Exception):
                exc = builtin(message)
    if exc is None:
        raise RemoteFunctionError(message, remote_type=etype, traceback_text=traceback_text)
    if traceback_text:
        exc.add_note(f"[vmon remote traceback]\n{traceback_text}")
    raise exc


class _SessionError(RemoteFunctionError):
    """The session died or broke protocol without a result; retry on fresh infra."""


_runner_installed: weakref.WeakSet[Sandbox] = weakref.WeakSet()


class _WorkerSession:
    """Client half of one runner session: exec, hello, calls, lifecycle."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox
        self._process: Process | None = None
        self._call_lock = threading.Lock()
        self._line_buffer = bytearray()
        self._sent_hashes: set[str] = set()
        self._initialized = False
        self._dead = False
        self._pickle_protocol = 5
        self._stderr_tail: deque[bytes] = deque(maxlen=64)
        self._stderr_sink: Callable[[str], None] | None = None
        self._stderr_thread: threading.Thread | None = None
        self._ids = itertools.count(1)

    def start(self) -> None:
        """Install the runner (once per sandbox), exec it, and await its hello."""
        if self._sandbox not in _runner_installed:
            self._sandbox.files.write_text(_RUNNER_PATH, _SESSION_RUNNER)
            _runner_installed.add(self._sandbox)
        self._process = self._sandbox.exec(
            "python3",
            "-u",
            _RUNNER_PATH,
            env={"VMON_REMOTE": "1"},
            timeout=None,
        )
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr,
            args=(self._process,),
            name="vmon-fn-stderr",
            daemon=True,
        )
        self._stderr_thread.start()
        try:
            hello = self._read_event(time.monotonic() + _HELLO_TIMEOUT)
        except TimeoutError:
            self.close()
            raise _SessionError(
                f"remote function runner sent no hello within {_HELLO_TIMEOUT:.0f}s"
            ) from None
        except BaseException:
            self.close()
            raise
        if hello.get("event") != "hello":
            self.close()
            raise _SessionError("remote function runner sent an unexpected first frame")
        advertised = hello.get("pickle")
        if isinstance(advertised, int) and not isinstance(advertised, bool):
            self._pickle_protocol = min(5, advertised)

    @property
    def alive(self) -> bool:
        """True while the runner process is believed to be running."""
        return not self._dead and self._process is not None and self._process.returncode is None

    @property
    def initialized(self) -> bool:
        """True once an ``init`` op has completed on this session."""
        return self._initialized

    def call(
        self,
        bundle: Any,
        name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        serializer: str = "auto",
        mode: str = "value",
        self_call: bool = False,
        init: Mapping[str, Any] | None = None,
        timeout: float | None = None,
        on_output: Callable[[str, str], None] | None = None,
    ) -> Any:
        """Run one remote call; returns the value, or an iterator when ``mode="iter"``.

        Holds the session's single-call lock for the full duration — for
        iterator calls, until the returned generator is exhausted or closed.
        """
        self._call_lock.acquire()
        streaming = False
        try:
            deadline = None if timeout is None else time.monotonic() + timeout
            self._stderr_sink = (
                None if on_output is None else lambda text: on_output("stderr", text)
            )
            if init is not None and not self._initialized:
                self._send_init(bundle, init, serializer, deadline, timeout, on_output)
                self._initialized = True
            op = self._op("call", bundle, serializer)
            op["name"] = name
            op["args"] = self._encode(list(args), serializer)
            op["kwargs"] = self._encode(dict(kwargs), serializer)
            op["mode"] = mode
            op["self"] = bool(self_call)
            self._send(op)
            if mode == "iter":
                stream = self._stream_results(deadline, timeout, on_output)
                next(stream)  # prime: enter the try block so close()/GC releases the lock
                streaming = True
                return stream
            return self._await_result(deadline, timeout, on_output)
        finally:
            if not streaming:
                self._stderr_sink = None
                self._call_lock.release()

    def shutdown(self, timeout: float = 10.0) -> None:
        """Run remote ``exit`` hooks with a grace period, then close the session."""
        if self.alive and self._call_lock.acquire(timeout=timeout):
            try:
                op: dict[str, Any] = {"op": "shutdown", "id": next(self._ids)}
                self._send(op)
                self._await_result(time.monotonic() + timeout, timeout, None)
            except Exception:
                pass
            finally:
                self._call_lock.release()
        self.close()

    def close(self) -> None:
        """Kill the runner process; the server SIGTERMs it on WebSocket close."""
        self._dead = True
        if self._process is not None:
            with contextlib.suppress(Exception):
                self._process.close()

    def _stream_results(
        self,
        deadline: float | None,
        timeout: float | None,
        on_output: Callable[[str, str], None] | None,
    ) -> Iterator[Any]:
        finished = False
        try:
            yield None  # priming sentinel consumed by call()
            while True:
                event = self._next_event(deadline, timeout)
                kind = event.get("event")
                if kind == "out":
                    self._forward_output(event, on_output)
                elif kind == "yield":
                    yield self._decode(event)
                elif kind == "result":
                    finished = True
                    return
                elif kind == "error":
                    finished = True
                    raise_remote_error(event)
                else:
                    raise self._violation(f"unexpected {kind!r} event")
        finally:
            self._stderr_sink = None
            self._call_lock.release()
            # A generator abandoned mid-stream leaves the runner still yielding;
            # that session cannot serve another call, so kill it.
            if not finished and self.alive:
                self.close()

    def _await_result(
        self,
        deadline: float | None,
        timeout: float | None,
        on_output: Callable[[str, str], None] | None,
    ) -> Any:
        while True:
            event = self._next_event(deadline, timeout)
            kind = event.get("event")
            if kind == "out":
                self._forward_output(event, on_output)
            elif kind == "result":
                return self._decode(event)
            elif kind == "error":
                raise_remote_error(event)
            else:
                raise self._violation(f"unexpected {kind!r} event")

    def _send_init(
        self,
        bundle: Any,
        init: Mapping[str, Any],
        serializer: str,
        deadline: float | None,
        timeout: float | None,
        on_output: Callable[[str, str], None] | None,
    ) -> None:
        op = self._op("init", bundle, serializer)
        op["name"] = str(init["name"])
        op["args"] = self._encode(list(init.get("args") or ()), serializer)
        op["kwargs"] = self._encode(dict(init.get("kwargs") or {}), serializer)
        op["enter"] = list(init.get("enter") or ())
        op["exit"] = list(init.get("exit") or ())
        self._send(op)
        self._await_result(deadline, timeout, on_output)

    def _op(self, kind: str, bundle: Any, serializer: str) -> dict[str, Any]:
        op: dict[str, Any] = {
            "op": kind,
            "id": next(self._ids),
            "hash": bundle.sha256,
            "serializer": serializer,
        }
        if bundle.sha256 not in self._sent_hashes:
            op["source"] = bundle.source
            op["module"] = bundle.module
            self._sent_hashes.add(bundle.sha256)
        return op

    def _send(self, op: Mapping[str, Any]) -> None:
        assert self._process is not None
        line = json.dumps(op, separators=(",", ":")) + "\n"
        self._process.stdin.write(line)

    def _encode(self, value: Any, serializer: str) -> dict[str, Any]:
        return encode_value(
            value,
            serializer=serializer,
            pickle_protocol=self._pickle_protocol,
            spill=self._spill,
        )

    def _decode(self, frame: Mapping[str, Any]) -> Any:
        return decode_value(frame, fetch=self._fetch)

    def _spill(self, data: bytes, fmt: str) -> str:
        del fmt
        path = f"{_SPILL_DIR}/{uuid.uuid4().hex}"
        self._sandbox.files.write_bytes(path, data)
        return path

    def _fetch(self, path: str) -> bytes:
        data = self._sandbox.files.read_bytes(path)
        with contextlib.suppress(Exception):
            self._sandbox.files.delete(path)
        return data

    @staticmethod
    def _forward_output(
        event: Mapping[str, Any],
        on_output: Callable[[str, str], None] | None,
    ) -> None:
        if on_output is not None:
            stream = event.get("stream")
            on_output(
                stream if stream in ("stdout", "stderr") else "stdout",
                str(event.get("data") or ""),
            )

    def _next_event(self, deadline: float | None, timeout: float | None) -> dict[str, Any]:
        try:
            return self._read_event(deadline)
        except TimeoutError:
            self.close()
            raise TimeoutError(f"remote function call timed out after {timeout}s") from None

    def _read_event(self, deadline: float | None) -> dict[str, Any]:
        line = self._read_line(deadline)
        if line is None:
            raise self._death()
        try:
            frame = json.loads(line.decode("utf-8"))
        except ValueError, UnicodeDecodeError:
            raise self._violation("malformed frame") from None
        if not isinstance(frame, dict):
            raise self._violation("non-object frame")
        return frame

    def _read_line(self, deadline: float | None) -> bytes | None:
        assert self._process is not None
        while True:
            newline = self._line_buffer.find(b"\n")
            if newline >= 0:
                line = bytes(self._line_buffer[:newline])
                del self._line_buffer[: newline + 1]
                return line
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            chunk = self._process.stdout.read_chunk(timeout=remaining)
            if chunk is None:
                if self._line_buffer:
                    line = bytes(self._line_buffer)
                    self._line_buffer.clear()
                    return line
                return None
            self._line_buffer.extend(chunk)

    def _violation(self, detail: str) -> RemoteFunctionError:
        self.close()
        return _SessionError(f"remote function runner protocol violation: {detail}")

    def _death(self) -> RemoteFunctionError:
        self._dead = True
        exit_code: int | None = None
        error_text = ""
        assert self._process is not None
        try:
            exit_code = self._process.wait(timeout=5).code
        except Exception as exc:
            error_text = str(exc)
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=1.0)
        tail = b"".join(self._stderr_tail).decode("utf-8", "replace").strip()
        clues = f"{tail}\n{error_text}".lower()
        if exit_code == 127 or "no such file" in clues or "not found" in clues:
            # Deterministic image misconfiguration: not worth infra retries.
            return RemoteFunctionError(_MISSING_PYTHON_HINT)
        detail = tail or error_text or f"remote function runner exited with {exit_code}"
        return _SessionError(detail)

    def _drain_stderr(self, process: Process) -> None:
        for chunk in process.stderr:
            self._stderr_tail.append(chunk)
            sink = self._stderr_sink
            if sink is not None:
                with contextlib.suppress(Exception):
                    sink(chunk.decode("utf-8", "replace"))
        # stderr EOF means the runner process is gone: fail fast on the next call.
        self._dead = True
