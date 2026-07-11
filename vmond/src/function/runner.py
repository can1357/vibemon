#!/usr/bin/env python3
"""Protocol-v2 Python guest runner owned by vmond.

The wire format is newline-delimited JSON.  This module deliberately has no vmon
SDK imports: it is copied into a guest image and run as a long-lived process.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import contextvars
import hashlib
import importlib
import inspect
import io
import json
import math
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import threading
import stat
import time
import traceback
import types
import zipfile
from dataclasses import dataclass, field
from typing import Any, Optional

PROTOCOL_VERSION = 2
ENVELOPE_VERSION = 1
MAX_FRAME_BYTES = int(os.environ.get("VMON_RUNNER_MAX_FRAME_BYTES", str(16 * 1024 * 1024)))
MAX_VALUE_BYTES = int(os.environ.get("VMON_RUNNER_MAX_VALUE_BYTES", str(64 * 1024 * 1024)))
MAX_DEFINITIONS = int(os.environ.get("VMON_RUNNER_MAX_DEFINITIONS", "4096"))
MAX_ACTORS = int(os.environ.get("VMON_RUNNER_MAX_ACTORS", "10000"))
MAX_CHECKPOINT_BYTES = int(os.environ.get(
    "VMON_RUNNER_MAX_CHECKPOINT_BYTES", str(256 * 1024 * 1024)))
MAX_PACKAGE_EXTRACT_BYTES = int(os.environ.get(
    "VMON_RUNNER_MAX_PACKAGE_EXTRACT_BYTES", str(512 * 1024 * 1024)))
MAX_PACKAGE_ARCHIVE_BYTES = int(os.environ.get(
    "VMON_RUNNER_MAX_PACKAGE_ARCHIVE_BYTES", str(64 * 1024 * 1024)))
FUNCTION_ROOT = os.environ.get("VMON_RUNNER_FUNCTION_ROOT", "/opt/vmon")
MAX_PACKAGE_FILES = int(os.environ.get("VMON_RUNNER_MAX_PACKAGE_FILES", "10000"))
MAX_LOG_FRAGMENT_BYTES = int(os.environ.get("VMON_RUNNER_MAX_LOG_FRAGMENT_BYTES", "65536"))
MAX_BATCH_INPUTS = int(os.environ.get("VMON_RUNNER_MAX_BATCH_INPUTS", "1024"))
_DEFAULT_THREADS = max(1, min(32, int(os.environ.get("VMON_RUNNER_SYNC_THREADS", "8"))))
MAX_PENDING_TASKS = int(os.environ.get("VMON_RUNNER_MAX_PENDING_TASKS", "256"))
_DEFAULT_TASKS = max(1, int(os.environ.get("VMON_RUNNER_ASYNC_TASKS", "128")))
SPILL_THRESHOLD_BYTES = int(os.environ.get(
    "VMON_RUNNER_SPILL_THRESHOLD_BYTES", str(512 * 1024)))
SPILL_ROOT = os.environ.get("VMON_RUNNER_SPILL_ROOT", "/run/vmon/values")
MAX_SPILL_BYTES = int(os.environ.get(
    "VMON_RUNNER_MAX_SPILL_BYTES", str(512 * 1024 * 1024)))
MAX_SPILL_FILES = int(os.environ.get("VMON_RUNNER_MAX_SPILL_FILES", "4096"))
_CALL = contextvars.ContextVar("vmon_current_call", default=None)


@dataclass(frozen=True)
class CurrentCall:
    call_id: str
    function_id: str = ""
    definition_id: str = ""
    request_id: str = ""
    input_id: str = ""
    input_index: int = 0
    attempt: int = 1
    parent_request_id: Optional[str] = None
    parent_call_id: Optional[str] = None
    execution_mode: str = ""
    actor_id: Optional[str] = None
    deadline_unix_ms: Optional[int] = None


IJSON_MAX_INTEGER = (1 << 53) - 1


def _validate_ijson(value):
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return
    if isinstance(value, int):
        if value < -IJSON_MAX_INTEGER or value > IJSON_MAX_INTEGER:
            raise SerializationError("JSON integer exceeds interoperable safe range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SerializationError("JSON numbers must be finite")
        return
    if isinstance(value, list):
        for item in value:
            _validate_ijson(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise SerializationError("JSON object keys must be strings")
            _validate_ijson(item)
        return
    raise SerializationError("value of type %s is not JSON serializable" %
                             type(value).__name__)


def _json_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SerializationError("JSON object contains duplicate key %r" % key)
        result[key] = value
    return result


def _json_loads(data):
    try:
        value = json.loads(
            data.decode("utf-8") if isinstance(data, bytes) else data,
            object_pairs_hook=_json_pairs,
            parse_constant=lambda constant: (_ for _ in ()).throw(
                SerializationError("JSON number %s is not finite" % constant)))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SerializationError("invalid UTF-8 JSON: %s" % exc) from exc
    _validate_ijson(value)
    return value


def _canonical(value):
    _validate_ijson(value)
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                          allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SerializationError("JSON serialization failed: %s" % exc) from exc


def current_call():
    """Return SDK-compatible immutable metadata for the active call."""
    value = _CALL.get()
    if value is None:
        raise RuntimeError("current_call() is only available while executing a vmon call")
    fields = {name: value.get(name) for name in CurrentCall.__dataclass_fields__}
    fields["call_id"] = fields["call_id"] or ""
    fields["function_id"] = fields["function_id"] or ""
    fields["definition_id"] = fields["definition_id"] or ""
    fields["request_id"] = fields["request_id"] or ""
    fields["input_id"] = fields["input_id"] or ""
    fields["input_index"] = fields["input_index"] or 0
    return CurrentCall(**fields)




def _checksum(data):
    return hashlib.sha256(data).hexdigest()


def _verify_checksum(expected, data):
    actual = _checksum(data)
    if not expected:
        raise ValueError("value envelope is missing sha256")
    normalized = expected[7:] if expected.lower().startswith("sha256:") else expected
    if normalized.lower() != actual:
        raise ValueError("value envelope checksum mismatch: expected %s, computed %s" % (expected, actual))


def _python_abi():
    return "cp%d%d" % (sys.version_info[0], sys.version_info[1])


def _cloudpickle():
    try:
        import cloudpickle  # type: ignore
    except ImportError as exc:
        raise RuntimeError("cloudpickle codec requested but cloudpickle is not installed in the guest") from exc
    return cloudpickle
def _cloudpickle_version():
    version = getattr(_cloudpickle(), "__version__", None)
    if not isinstance(version, str) or not version:
        raise RuntimeError("installed cloudpickle does not expose a usable __version__")
    return version




def decode_value(envelope, trusted=False, cloudpickle_version=None):
    if not isinstance(envelope, dict) or "format" not in envelope:
        raise ValueError("value must be a versioned envelope with a format")
    version = envelope.get("version")
    if version != ENVELOPE_VERSION:
        raise ValueError("unsupported value envelope version %r; runner supports %d" % (version, ENVELOPE_VERSION))
    has_inline = "inline_data" in envelope
    has_path = "path" in envelope
    if has_inline == has_path:
        raise ValueError("value envelope must contain exactly one of inline_data or path")
    if has_inline:
        encoded = envelope["inline_data"]
        if not isinstance(encoded, str):
            raise ValueError("value envelope inline_data must be base64 text")
        try:
            stored = base64.b64decode(encoded.encode("ascii"), validate=True)
        except Exception as exc:
            raise ValueError("value envelope contains invalid base64 inline_data") from exc
    else:
        path = envelope["path"]
        if not isinstance(path, str) or not os.path.isabs(path):
            raise ValueError("value envelope path must be an absolute guest path")
        try:
            size = os.path.getsize(path)
            if size > MAX_VALUE_BYTES:
                raise ValueError("spilled value exceeds %d-byte limit" % MAX_VALUE_BYTES)
            with open(path, "rb") as source:
                stored = source.read(MAX_VALUE_BYTES + 1)
        except OSError as exc:
            raise ValueError("cannot read spilled value at %r: %s" % (path, exc)) from exc
        finally:
            if envelope.get("remove_after_read", True):
                try:
                    os.remove(path)
                except OSError:
                    pass
    expected_size = envelope.get("uncompressed_size")
    if (isinstance(expected_size, bool) or not isinstance(expected_size, int) or
            expected_size < 0):
        raise ValueError("value uncompressed_size must be a non-negative integer")
    if expected_size > MAX_VALUE_BYTES:
        raise ValueError("value uncompressed_size exceeds %d-byte limit" % MAX_VALUE_BYTES)
    compression = envelope.get("compression", "none")
    if compression in (None, "", "none"):
        data = stored
    elif compression == "gzip":
        import gzip
        with gzip.GzipFile(fileobj=io.BytesIO(stored)) as source:
            data = source.read(MAX_VALUE_BYTES + 1)
    elif compression == "zstd":
        try:
            import zstandard  # type: ignore
        except ImportError as exc:
            raise RuntimeError("zstd-compressed value requested but zstandard is not installed") from exc
        content_size = zstandard.frame_content_size(stored)
        unknown_sizes = {
            getattr(zstandard, "CONTENTSIZE_UNKNOWN", -1),
            getattr(zstandard, "CONTENTSIZE_ERROR", -2),
        }
        if content_size not in unknown_sizes and content_size > MAX_VALUE_BYTES:
            raise ValueError("zstd frame content size exceeds %d-byte limit" % MAX_VALUE_BYTES)
        decompressor = zstandard.ZstdDecompressor(max_window_size=MAX_VALUE_BYTES)
        with decompressor.stream_reader(io.BytesIO(stored)) as source:
            data = source.read(MAX_VALUE_BYTES + 1)
    else:
        raise ValueError("unsupported envelope compression %r" % compression)
    if len(data) > MAX_VALUE_BYTES:
        raise ValueError("decoded value exceeds %d-byte limit" % MAX_VALUE_BYTES)
    if len(data) != expected_size:
        raise ValueError("value uncompressed_size mismatch: expected %d, decoded %d" %
                         (expected_size, len(data)))
    _verify_checksum(envelope.get("sha256"), data)
    value_format = envelope.get("format")
    if value_format == "json":
        return _json_loads(data)
    if value_format == "cbor":
        try:
            import cbor2  # type: ignore
        except ImportError as exc:
            raise RuntimeError("CBOR codec requested but cbor2 is not installed in the guest") from exc
        return cbor2.loads(data)
    if value_format == "cloudpickle":
        if not trusted:
            raise ValueError("cloudpickle is executable data and requires top-level trusted=true")
        abi = envelope.get("python_abi")
        if abi != _python_abi():
            raise ValueError("cloudpickle Python ABI mismatch: payload requires %r, guest is %r" % (abi, _python_abi()))
        installed = _cloudpickle_version()
        declared = envelope.get("cloudpickle_version")
        if not isinstance(cloudpickle_version, str):
            raise ValueError("trusted cloudpickle frame is missing cloudpickle_version authority")
        if declared != cloudpickle_version or declared != installed:
            raise ValueError("cloudpickle version mismatch: envelope %r, authorized %r, installed %r" %
                             (declared, cloudpickle_version, installed))
        return _cloudpickle().loads(data)
    raise ValueError("unsupported value format %r; expected json, cbor, or cloudpickle" % value_format)


_SPILL_LOCK = threading.Lock()
_SPILL_PATHS = set()


def _spill_data(envelope, data):
    os.makedirs(SPILL_ROOT, mode=0o700, exist_ok=True)
    os.chmod(SPILL_ROOT, 0o700)
    with _SPILL_LOCK:
        total = 0
        for existing in list(_SPILL_PATHS):
            try:
                total += os.path.getsize(existing)
            except FileNotFoundError:
                _SPILL_PATHS.remove(existing)
        if len(_SPILL_PATHS) >= MAX_SPILL_FILES:
            raise RuntimeError("result spill file limit of %d reached" % MAX_SPILL_FILES)
        if total + len(data) > MAX_SPILL_BYTES:
            raise RuntimeError("result spill byte limit of %d reached" % MAX_SPILL_BYTES)
        fd, path = tempfile.mkstemp(prefix="value-", dir=SPILL_ROOT)
        try:
            os.fchmod(fd, 0o600)
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                view = view[written:]
        except BaseException:
            os.close(fd)
            try:
                os.remove(path)
            except OSError:
                pass
            raise
        else:
            os.close(fd)
        _SPILL_PATHS.add(path)
    result = dict(envelope)
    result.pop("inline_data", None)
    result["path"] = path
    result["remove_after_read"] = True
    return result


def _spill_envelope(envelope):
    data = base64.b64decode(envelope["inline_data"].encode("ascii"), validate=True)
    return _spill_data(envelope, data) if len(data) > SPILL_THRESHOLD_BYTES else envelope


def _cleanup_spills():
    with _SPILL_LOCK:
        paths = list(_SPILL_PATHS)
        _SPILL_PATHS.clear()
    for path in paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def encode_value(value, value_format="json", spill=True, compression="none",
                 redact_secrets=True, reject_secrets=True):
    secrets = _CALL.get().get("_secrets", set()) if _CALL.get() else set()
    value = _redact(value, secrets) if redact_secrets else value
    if value_format == "json":
        data = _canonical(value)
        extra = {}
    elif value_format == "cbor":
        try:
            import cbor2  # type: ignore
        except ImportError as exc:
            raise RuntimeError("CBOR codec requested but cbor2 is not installed in the guest") from exc
        data = cbor2.dumps(value)
        extra = {}
    elif value_format == "cloudpickle":
        data = _cloudpickle().dumps(value)
        extra = {"python_abi": _python_abi(), "cloudpickle_version": _cloudpickle_version()}
    else:
        raise ValueError("unsupported output format %r" % value_format)
    if len(data) > MAX_VALUE_BYTES:
        raise ValueError("encoded value exceeds %d-byte limit" % MAX_VALUE_BYTES)
    if reject_secrets:
        for secret in secrets:
            if secret.encode("utf-8") in data:
                raise ValueError("refusing to encode a value containing secret material")
    if compression in (None, "", "none"):
        stored = data
        compression = "none"
    elif compression == "gzip":
        import gzip
        stored = gzip.compress(data)
    elif compression == "zstd":
        try:
            import zstandard  # type: ignore
        except ImportError as exc:
            raise RuntimeError("zstd output requested but zstandard is not installed") from exc
        stored = zstandard.ZstdCompressor().compress(data)
    else:
        raise ValueError("unsupported output compression %r" % compression)
    result = {"version": ENVELOPE_VERSION, "format": value_format,
              "inline_data": base64.b64encode(stored).decode("ascii"),
              "uncompressed_size": len(data), "sha256": _checksum(data),
              "compression": compression}
    result.update(extra)
    return _spill_data(result, stored) if spill and len(stored) > SPILL_THRESHOLD_BYTES else result






class SnapshotWithSecretsUnsupported(RuntimeError):
    pass
class RunnerOverloaded(RuntimeError):
    pass
class ActorLostError(RuntimeError):
    pass


class RemoteFunctionError(RuntimeError):
    pass


class CheckpointContainsSecret(RuntimeError):
    pass



class SerializationError(ValueError):
    pass

def _decorator(name, lifecycle=None):
    def decorate(*decorator_args, **decorator_kwargs):
        def apply(target):
            metadata = dict(getattr(target, "__vmon__", {}))
            metadata.update(decorator_kwargs)
            metadata["kind"] = name
            if lifecycle:
                metadata["lifecycle"] = lifecycle
                setattr(target, "__vmon_%s__" % lifecycle, True)
            setattr(target, "__vmon__", metadata)
            return target
        if len(decorator_args) == 1 and callable(decorator_args[0]) and not decorator_kwargs:
            return apply(decorator_args[0])
        return apply
    return decorate


def _method_decorator(function):
    setattr(function, "__vmon_method__", True)
    return function


def _enter_decorator(function=None, *, snapshot=False):
    def apply(target):
        setattr(target, "__vmon_enter__", True)
        setattr(target, "__vmon_snapshot_enter__", bool(snapshot))
        return target
    return apply(function) if function is not None else apply


def _lifecycle_decorator(marker):
    def apply(function):
        setattr(function, marker, True)
        return function
    return apply


def _install_vmon_shim():
    existing = sys.modules.get("vmon")
    if existing is not None and getattr(existing, "__vmon_guest_shim__", False):
        return
    shim = types.ModuleType("vmon")
    shim.__vmon_guest_shim__ = True
    shim.function = _decorator("function")
    shim.cls = _decorator("cls")
    shim.service = _decorator("service")
    shim.method = _method_decorator
    shim.concurrent = _decorator("concurrent")
    shim.batched = _decorator("batched")
    shim.enter = _enter_decorator
    shim.exit = _lifecycle_decorator("__vmon_exit__")
    shim.on_restore = _lifecycle_decorator("__vmon_restore__")
    shim.current_call = current_call
    shim.is_remote = lambda: True
    shim.ActorLostError = ActorLostError
    shim.RemoteFunctionError = RemoteFunctionError
    sys.modules["vmon"] = shim


def _resolve_qualname(root, qualname):
    value = root
    for component in qualname.split("."):
        if component == "<locals>" or not component:
            raise ValueError("invalid import qualname %r" % qualname)
        value = getattr(value, component)
    return value


def _import_target(spec, root):
    if not isinstance(spec, str) or ":" not in spec:
        raise ValueError("package target must be 'module:qualname'")
    module_name, qualname = spec.split(":", 1)
    if root:
        root = os.path.abspath(root)
        if root not in sys.path:
            sys.path.insert(0, root)
    existing = sys.modules.get(module_name)
    existing_file = getattr(existing, "__file__", None) if existing else None
    if existing_file and root and not os.path.realpath(existing_file).startswith(
            os.path.realpath(root) + os.sep):
        for loaded_name in list(sys.modules):
            if loaded_name == module_name or loaded_name.startswith(module_name + "."):
                del sys.modules[loaded_name]
    _install_vmon_shim()
    importlib.invalidate_caches()
    module = importlib.import_module(module_name)
    return _resolve_qualname(module, qualname)


def _secret_strings(value):
    found = set()
    if isinstance(value, str):
        if value:
            found.add(value)
    elif isinstance(value, dict):
        for item in value.values():
            found.update(_secret_strings(item))
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            found.update(_secret_strings(item))
    return found


def _redact(value, secrets):
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, "[REDACTED]")
        return value
    if isinstance(value, dict):
        return {key: _redact(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact(item, secrets) for item in value)
    return value


class _ProtocolWriter:
    def __init__(self):
        self._lock = threading.Lock()
        self._seq = {}
        self._fd = os.dup(1)

    def replace_fd(self, fd):
        replacement = os.dup(fd)
        with self._lock:
            old = self._fd
            self._fd = replacement
            os.close(old)

    def reset(self, request_id):
        with self._lock:
            self._seq[request_id] = 0

    def emit(self, event, request_id=None, **fields):
        with self._lock:
            frame = {"protocol": PROTOCOL_VERSION, "type": event, "event": event}
            meta = _CALL.get()
            secrets = meta.get("_secrets", set()) if meta else set()
            if (meta and meta.get("_closed") is not None and meta["_closed"].is_set() and
                    event not in ("hello",)):
                return
            if request_id is not None:
                seq = self._seq.get(request_id, 0)
                self._seq[request_id] = seq + 1
                frame.update({"request_id": request_id, "seq": seq})
                if meta and meta.get("request_id") == request_id:
                    for key in ("call_id", "function_id", "input_id", "input_index", "attempt",
                                "parent_request_id", "parent_call_id"):
                        frame[key] = meta.get(key)
            frame.update(_redact(fields, secrets))
            payload = _canonical(frame) + b"\n"
            view = memoryview(payload)
            try:
                while view:
                    written = os.write(self._fd, view)
                    view = view[written:]
            except OSError:
                # A socket client may disconnect while a call is finishing.  State
                # remains alive and the next connection receives a fresh hello.
                pass
            if request_id is not None and event in ("result", "error", "cancelled", "status"):
                self._seq.pop(request_id, None)
                if meta and meta.get("request_id") == request_id and meta.get("_closed") is not None:
                    meta["_closed"].set()

    def close(self):
        with self._lock:
            os.close(self._fd)


_WRITER = _ProtocolWriter()
_NATIVE_CALL_LOCK = threading.Lock()
_NATIVE_CALLS = {}


def _native_call_enter(meta):
    with _NATIVE_CALL_LOCK:
        _NATIVE_CALLS[meta["request_id"]] = meta


def _native_call_exit(meta):
    with _NATIVE_CALL_LOCK:
        _NATIVE_CALLS.pop(meta["request_id"], None)


def _emit_native_line(stream, raw):
    with _NATIVE_CALL_LOCK:
        active = list(_NATIVE_CALLS.values())
    if len(active) != 1:
        return
    meta = active[0]
    token = _CALL.set(meta)
    try:
        _WRITER.emit("log", meta["request_id"], stream=stream,
                     message=raw.decode("utf-8", "replace"), partial=False)
    finally:
        _CALL.reset(token)


def _capture_native_fd(fd, stream):
    reader_fd, writer_fd = os.pipe()
    os.dup2(writer_fd, fd)
    os.close(writer_fd)

    def forward():
        buffered = b""
        dropping = False
        while True:
            try:
                chunk = os.read(reader_fd, MAX_LOG_FRAGMENT_BYTES)
            except OSError:
                return
            if not chunk:
                return
            buffered += chunk
            while b"\n" in buffered:
                line, buffered = buffered.split(b"\n", 1)
                if dropping:
                    dropping = False
                    continue
                _emit_native_line(stream, line)
            if len(buffered) > MAX_LOG_FRAGMENT_BYTES:
                buffered = b""
                dropping = True
                _emit_native_line(stream, b"[native log line dropped: too long]")

    threading.Thread(target=forward, name="vmon-native-%s" % stream,
                     daemon=True).start()


_capture_native_fd(1, "stdout")
_capture_native_fd(2, "stderr")




class _CapturedStream(io.TextIOBase):
    def __init__(self, name):
        self.name = name

    @property
    def encoding(self):
        return "utf-8"

    def writable(self):
        return True

    def write(self, text):
        if not isinstance(text, str):
            text = str(text)
        meta = _CALL.get()
        message = text.rstrip("\r\n")
        secrets = meta.get("_secrets", set()) if meta else set()
        message = _redact(message, secrets)
        if not message and text:
            return len(text)
        if meta is None:
            _WRITER.emit("log", None, stream=self.name, message=message,
                         partial=not text.endswith(("\n", "\r")))
            return len(text)
        encoded = message.encode("utf-8", "replace")
        while len(encoded) > MAX_LOG_FRAGMENT_BYTES:
            fragment = encoded[:MAX_LOG_FRAGMENT_BYTES].decode("utf-8", "ignore")
            _WRITER.emit("log", meta["request_id"], stream=self.name,
                         message=fragment, partial=True)
            encoded = encoded[MAX_LOG_FRAGMENT_BYTES:]
        if encoded:
            _WRITER.emit("log", meta["request_id"], stream=self.name,
                         message=encoded.decode("utf-8", "replace"),
                         partial=not text.endswith(("\n", "\r")))
        return len(text)

    def flush(self):
        return None


sys.stdout = _CapturedStream("stdout")
sys.stderr = _CapturedStream("stderr")


@dataclass
class Definition:
    definition_id: str
    revision: str
    target: Any
    mode: str
    root: Optional[str] = None
    cleanup_root: Optional[str] = None
    serialized_methods: bool = True
    secrets: set = field(default_factory=set)
    secret_names: set = field(default_factory=set)


@dataclass
class Actor:
    actor_id: str
    definition_id: str
    revision: str
    value: Any
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _contains_secret(value, names, secrets, seen=None):
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return False
    seen.add(identity)
    if isinstance(value, str):
        return value in names or any(secret in value for secret in secrets)
    if isinstance(value, bytes):
        return any(secret.encode("utf-8") in value for secret in secrets)
    if isinstance(value, dict):
        return any(_contains_secret(key, names, secrets, seen) or
                   _contains_secret(item, names, secrets, seen)
                   for key, item in value.items())
    if isinstance(value, (list, tuple, set)):
        return any(_contains_secret(item, names, secrets, seen) for item in value)
    return False
def _bounded_error_text(value, limit=131072, representation=False):
    try:
        text = repr(value) if representation else str(value)
    except BaseException:
        text = "<unprintable %s>" % type(value).__name__
    meta = _CALL.get()
    text = _redact(text, meta.get("_secrets", set()) if meta else set())
    encoded = text.encode("utf-8", "replace")
    if len(encoded) <= limit:
        return text
    return encoded[:limit].decode("utf-8", "ignore") + "... [truncated]"


def _exception_traceback(exc):
    try:
        rendered = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except BaseException:
        rendered = "%s: <traceback formatting failed>" % type(exc).__name__
    return _bounded_error_text(rendered)


class Runner:
    def __init__(self, max_threads=_DEFAULT_THREADS, max_tasks=_DEFAULT_TASKS):
        self.definitions = {}
        self.actors = {}
        self.checkpoints = {}
        self.services = {}
        self.package_cache = {}
        self.checkpoint_bytes = 0
        self.running = {}
        self.running_frames = {}
        self.deadline_handles = {}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads,
                                                               thread_name_prefix="vmon-call")
        self.slots = asyncio.Semaphore(max_tasks)
        self.stopping = False

    def _error_fields(self, exc, phase=None):
        fields = {
            "error": {
                "type": type(exc).__name__,
                "module": type(exc).__module__,
                "message": _bounded_error_text(exc),
                "repr": _bounded_error_text(exc, representation=True),
                "traceback": _exception_traceback(exc),
            }
        }
        if isinstance(exc, ActorLostError):
            fields["error"]["code"] = "actor_lost"
        if isinstance(exc, CheckpointContainsSecret):
            fields["error"]["code"] = "checkpoint_contains_secret"
        if isinstance(exc, SerializationError):
            fields["error"]["code"] = "serialization_error"
        if isinstance(exc, RunnerOverloaded):
            fields["error"]["code"] = "runner_overloaded"
        if isinstance(exc, SnapshotWithSecretsUnsupported):
            fields["error"]["code"] = "snapshot_with_secrets_unsupported"
        if phase:
            fields["error"]["phase"] = phase
        cause = exc.__cause__ or exc.__context__
        if cause is not None:
            fields["error"]["cause"] = {"type": type(cause).__name__,
                                        "module": type(cause).__module__,
                                        "message": _bounded_error_text(cause)}
        return fields

    def _extract_package(self, definition):
        root = definition.get("root")
        archive_path = definition.get("archive_path")
        if archive_path is None:
            if "archive" in definition:
                raise ValueError("inline package archives are unsupported; use archive_path")
            if not root or not os.path.isdir(root):
                raise ValueError("package definition root is not a directory: %r" % root)
            return os.path.abspath(root), None
        if not isinstance(archive_path, str) or not os.path.isabs(archive_path):
            raise ValueError("package archive_path must be absolute")
        if ".." in archive_path.split(os.sep):
            raise ValueError("package archive_path must not contain '..'")
        allowed_root = os.path.abspath(FUNCTION_ROOT)
        if os.path.realpath(allowed_root) != allowed_root or os.path.islink(allowed_root):
            raise ValueError("function package root must not be a symlink")
        lexical = os.path.abspath(os.path.normpath(archive_path))
        try:
            if os.path.commonpath((allowed_root, lexical)) != allowed_root:
                raise ValueError("package archive_path escapes function root")
        except ValueError:
            raise ValueError("package archive_path escapes function root")
        current = allowed_root
        relative = os.path.relpath(lexical, allowed_root)
        for component in relative.split(os.sep):
            current = os.path.join(current, component)
            try:
                entry = os.lstat(current)
            except OSError as exc:
                raise ValueError("cannot inspect package archive path %r: %s" %
                                 (archive_path, exc)) from exc
            if stat.S_ISLNK(entry.st_mode):
                raise ValueError("package archive path contains symlink %r" % current)
        if os.path.realpath(lexical) != lexical:
            raise ValueError("resolved package archive_path escapes or aliases its lexical path")
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(lexical, flags)
        except OSError as exc:
            raise ValueError("cannot securely open package archive %r: %s" %
                             (archive_path, exc)) from exc
        temporary = tempfile.mkdtemp(prefix="vmon-function-")
        try:
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode):
                raise ValueError("package archive_path is not a regular file")
            if opened.st_size > MAX_PACKAGE_ARCHIVE_BYTES:
                raise ValueError("package archive exceeds %d-byte input limit" %
                                 MAX_PACKAGE_ARCHIVE_BYTES)
            expected_size = definition.get("archive_size")
            if expected_size is not None:
                if isinstance(expected_size, bool) or not isinstance(expected_size, int):
                    raise ValueError("package archive_size must be an integer")
                if expected_size != opened.st_size:
                    raise ValueError("package archive size mismatch: expected %d, opened %d" %
                                     (expected_size, opened.st_size))
            expected = definition.get("archive_sha256")
            if (not isinstance(expected, str) or len(expected) != 64 or
                    any(character not in "0123456789abcdef" for character in expected)):
                raise ValueError("package archive_sha256 must be 64 lowercase hex characters")
            digest = hashlib.sha256()
            with os.fdopen(fd, "rb", closefd=False) as source:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                if digest.hexdigest() != expected:
                    raise ValueError("package archive SHA-256 mismatch")
                cached = self.package_cache.get(expected)
                if cached is not None:
                    shutil.rmtree(temporary, ignore_errors=True)
                    return cached, None
                source.seek(0)
                extract_root = os.path.join(temporary, "root")
                os.mkdir(extract_root)
                if zipfile.is_zipfile(source):
                    source.seek(0)
                    with zipfile.ZipFile(source) as bundle:
                        self._safe_zip_extract(bundle, extract_root)
                else:
                    source.seek(0)
                    try:
                        with tarfile.open(fileobj=source) as bundle:
                            self._safe_tar_extract(bundle, extract_root)
                    except tarfile.TarError as exc:
                        raise ValueError("package archive is neither a valid ZIP nor TAR") from exc
            self.package_cache[expected] = extract_root
            return extract_root, None
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
        finally:
            os.close(fd)

    @staticmethod
    def _safe_zip_extract(bundle, root):
        members = bundle.infolist()
        if len(members) > MAX_PACKAGE_FILES:
            raise ValueError("package archive exceeds %d-file limit" % MAX_PACKAGE_FILES)
        if sum(member.file_size for member in members) > MAX_PACKAGE_EXTRACT_BYTES:
            raise ValueError("package archive exceeds %d-byte extracted limit" %
                             MAX_PACKAGE_EXTRACT_BYTES)
        base = os.path.realpath(root) + os.sep
        for member in members:
            destination = os.path.realpath(os.path.join(root, member.filename))
            unix_type = (member.external_attr >> 16) & 0o170000
            if not destination.startswith(base) or unix_type == 0o120000:
                raise ValueError("package archive contains unsafe path %r" % member.filename)
        bundle.extractall(root)

    @staticmethod
    def _safe_tar_extract(bundle, root):
        members = []
        total = 0
        for member in bundle:
            members.append(member)
            if len(members) > MAX_PACKAGE_FILES:
                raise ValueError("package archive exceeds %d-file limit" % MAX_PACKAGE_FILES)
            total += member.size
            if total > MAX_PACKAGE_EXTRACT_BYTES:
                raise ValueError("package archive exceeds %d-byte extracted limit" %
                                 MAX_PACKAGE_EXTRACT_BYTES)
            destination = os.path.realpath(os.path.join(root, member.name))
            base = os.path.realpath(root) + os.sep
            if (not destination.startswith(base) or member.issym() or member.islnk() or
                    not (member.isfile() or member.isdir())):
                raise ValueError("package archive contains unsafe member %r" % member.name)
        bundle.extractall(root, members=members)

    @staticmethod
    def _callable_kind(target):
        if not inspect.isclass(target) and not inspect.isfunction(target):
            target = getattr(target, "__call__", target)
        if inspect.isasyncgenfunction(target):
            return "async_generator"
        if inspect.iscoroutinefunction(target):
            return "async"
        if inspect.isgeneratorfunction(target):
            return "generator"
        return "sync"

    async def define(self, frame):
        request_id = frame.get("request_id")
        definition_id = frame.get("definition_id") or frame.get("id")
        revision = frame.get("revision")
        if not definition_id or not revision:
            raise ValueError("define requires definition_id and immutable revision")
        old = self.definitions.get(definition_id)
        if old is not None:
            if old.revision != revision:
                raise ValueError("definition %r is already initialized at revision %r; refusing revision %r" %
                                 (definition_id, old.revision, revision))
            old.secrets = _secret_strings(frame.get("secrets", {}))
            old.secret_names = set(frame.get("secrets", {}))
            _WRITER.emit("status", request_id, status="already_initialized",
                         definition_id=definition_id, revision=revision,
                         callable_kind=self._callable_kind(old.target))
            return
        if len(self.definitions) >= MAX_DEFINITIONS:
            raise RuntimeError("definition limit of %d reached" % MAX_DEFINITIONS)
        spec = frame.get("definition") or frame
        mode = spec.get("mode") or spec.get("kind")
        cleanup_root = None
        if mode == "package":
            root, cleanup_root = self._extract_package(spec)
            try:
                target = _import_target(spec.get("target"), root)
            except Exception:
                if cleanup_root:
                    shutil.rmtree(cleanup_root, ignore_errors=True)
                raise
        elif mode in ("serialized", "cloudpickle"):
            payload = spec.get("value") or spec.get("payload")
            target = decode_value(payload, trusted=bool(frame.get("trusted")),
                                  cloudpickle_version=frame.get("cloudpickle_version"))
            root = None
        else:
            raise ValueError("definition mode must be package or serialized")
        if not callable(target):
            raise TypeError("loaded definition target is not callable")
        metadata = getattr(target, "__vmon__", {})
        serialized_methods = spec.get("serialized_methods")
        if serialized_methods is None:
            serialized_methods = metadata.get("kind") != "concurrent"
        definition_secrets = _secret_strings(frame.get("secrets", {}))
        definition_secret_names = set(frame.get("secrets", {}))
        self.definitions[definition_id] = Definition(
            definition_id, revision, target, mode, root, cleanup_root,
            bool(serialized_methods), definition_secrets, definition_secret_names)
        callable_kind = self._callable_kind(target)
        _WRITER.emit("status", request_id, status="initialized",
                     definition_id=definition_id, revision=revision,
                     callable_kind=callable_kind)
    def _call_meta(self, frame):
        secrets = _secret_strings(frame.get("secrets", {}))
        definition_id = frame.get("definition_id")
        if definition_id is None and frame.get("actor_id") in self.actors:
            definition_id = self.actors[frame["actor_id"]].definition_id
        definition = self.definitions.get(definition_id)
        if definition is not None:
            secrets.update(definition.secrets)
        return {
            "request_id": frame["request_id"],
            "input_id": frame.get("input_id"),
            "call_id": frame.get("call_id"),
            "function_id": frame.get("function_id", frame.get("definition_id")),
            "definition_id": frame.get("definition_id"),
            "input_index": frame.get("input_index"),
            "attempt": frame.get("attempt", 1),
            "parent_request_id": frame.get("parent_request_id"),
            "parent_call_id": frame.get("parent_call_id"),
            "execution_mode": frame.get("execution_mode", "unary"),
            "actor_id": frame.get("actor_id"),
            "deadline_unix_ms": frame.get("deadline_unix_ms"),
            "_secrets": secrets,
            "_closed": threading.Event(),
        }

    def _decode_args(self, frame):
        trusted = bool(frame.get("trusted"))
        cloudpickle_version = frame.get("cloudpickle_version")
        if "positional" in frame or "named" in frame:
            if "input" in frame or "args" in frame or "kwargs" in frame:
                raise ValueError("invocation must use exactly one input representation")
            positional = frame.get("positional", [])
            named = frame.get("named", {})
            if not isinstance(positional, list) or not isinstance(named, dict):
                raise TypeError("positional must be a list and named must be an object")
            if any(not isinstance(key, str) for key in named):
                raise TypeError("named argument keys must be strings")
            args = [decode_value(item, trusted=trusted,
                                 cloudpickle_version=cloudpickle_version)
                    for item in positional]
            kwargs = {key: decode_value(item, trusted=trusted,
                                        cloudpickle_version=cloudpickle_version)
                      for key, item in named.items()}
        elif "args" in frame:
            args = decode_value(frame["args"], trusted=trusted,
                                cloudpickle_version=cloudpickle_version)
            if not isinstance(args, (list, tuple)):
                raise TypeError("args envelope must decode to a list or tuple")
            kwargs = (decode_value(frame["kwargs"], trusted=trusted,
                                   cloudpickle_version=cloudpickle_version)
                      if "kwargs" in frame else {})
        elif "input" in frame:
            value = decode_value(frame["input"], trusted=trusted,
                                 cloudpickle_version=cloudpickle_version)
            tagged_keys = {"__vmon_python_call__", "args", "kwargs"}
            if isinstance(value, dict) and set(value) == tagged_keys:
                version = value["__vmon_python_call__"]
                if isinstance(version, bool) or version != 1:
                    raise ValueError("unsupported __vmon_python_call__ version %r" % version)
                args, kwargs = value["args"], value["kwargs"]
                if not isinstance(args, list):
                    raise TypeError("tagged Python call args must be a list")
                if not isinstance(kwargs, dict) or any(not isinstance(key, str) for key in kwargs):
                    raise TypeError("tagged Python call kwargs must be an object with string keys")
            else:
                args, kwargs = [value], {}
        else:
            args, kwargs = [], {}
        if not isinstance(kwargs, dict) or any(not isinstance(key, str) for key in kwargs):
            raise TypeError("kwargs must be an object with string keys")
        return list(args), kwargs

    async def _invoke(self, function, args, kwargs, frame):
        loop = asyncio.get_running_loop()
        if inspect.isasyncgenfunction(function):
            return function(*args, **kwargs)
        if inspect.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
            if frame.get("_cancel_reason"):
                raise asyncio.CancelledError()
            return result
        context = contextvars.copy_context()
        result = await loop.run_in_executor(self.executor, context.run,
                                            lambda: function(*args, **kwargs))
        if frame.get("_cancel_reason"):
            raise asyncio.CancelledError()
        if inspect.isawaitable(result):
            return await result
        return result

    async def _emit_result(self, value, frame):
        if frame.get("_cancel_reason"):
            raise asyncio.CancelledError()
        request_id = frame["request_id"]
        compression = frame.get("output_compression", "none")
        value_format = frame.get("output_format", frame.get("output_codec", "json"))
        if value_format == "cloudpickle" and not frame.get("trusted"):
            raise ValueError("cloudpickle output requires top-level trusted=true")
        if inspect.isasyncgen(value):
            index = 0
            async for item in value:
                if frame.get("_cancel_reason"):
                    raise asyncio.CancelledError()
                _WRITER.emit("yield", request_id, index=index,
                             value=encode_value(item, value_format, compression=compression))
                index += 1
            if frame.get("_cancel_reason"):
                raise asyncio.CancelledError()
            _WRITER.emit("result", request_id,
                         value=encode_value(None, value_format, compression=compression),
                         yield_count=index)
            return
        if inspect.isgenerator(value) or (
                frame.get("execution_mode") == "generator" and
                hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict))):
            index = 0
            iterator = iter(value)
            loop = asyncio.get_running_loop()
            sentinel = object()
            while True:
                context = contextvars.copy_context()
                item = await loop.run_in_executor(
                    self.executor, context.run, lambda: next(iterator, sentinel))
                if frame.get("_cancel_reason"):
                    raise asyncio.CancelledError()
                if item is sentinel:
                    break
                _WRITER.emit("yield", request_id, index=index,
                             value=encode_value(item, value_format, compression=compression))
                index += 1
            _WRITER.emit("result", request_id,
                         value=encode_value(None, value_format, compression=compression),
                         yield_count=index)
            return
        _WRITER.emit("result", request_id,
                     value=encode_value(value, value_format, compression=compression))

    async def _invoke_service(self, definition, frame, payload=None):
        if payload is None:
            service_key = frame.get("service_key")
            method_name = frame.get("method")
            constructor = frame.get("constructor") or {}
            if not isinstance(constructor, dict):
                raise TypeError("service constructor must be an invocation object")
            constructor_frame = {
                "positional": constructor.get("positional", []),
                "named": constructor.get("named", {}),
                "trusted": frame.get("trusted", False),
                "cloudpickle_version": frame.get("cloudpickle_version"),
            }
            constructor_args, constructor_kwargs = self._decode_args(constructor_frame)
            args, kwargs = self._decode_args(frame)
        else:
            expected_keys = {
                "kind", "service_key", "constructor_args", "constructor_kwargs",
                "method", "args", "kwargs",
            }
            if set(payload) != expected_keys or payload.get("kind") != "vmon.service.v1":
                raise ValueError("invalid vmon.service.v1 payload shape")
            service_key = payload["service_key"]
            method_name = payload["method"]
            constructor_args, constructor_kwargs = (
                payload["constructor_args"], payload["constructor_kwargs"])
            args, kwargs = payload["args"], payload["kwargs"]
        if not isinstance(service_key, str) or not service_key:
            raise TypeError("service_key must be non-empty text")
        if not isinstance(constructor_args, list) or not isinstance(args, list):
            raise TypeError("service constructor and method positional args must be lists")
        for name, value in (("constructor kwargs", constructor_kwargs),
                            ("method kwargs", kwargs)):
            if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
                raise TypeError("service %s must have string keys" % name)
        if not isinstance(method_name, str) or method_name.startswith("_"):
            raise ValueError("service method must be a public method name")
        cache_key = (definition.revision, service_key)
        instance = self.services.get(cache_key)
        if instance is None:
            instance = await self._invoke(
                definition.target, constructor_args, constructor_kwargs, frame)
            await self._run_hook(instance, "enter", frame)
            self.services[cache_key] = instance
        method_attribute = type(instance).__dict__.get(method_name)
        if not getattr(method_attribute, "__vmon_method__", False):
            raise ValueError("service method %r is not exported with @vmon.method" %
                             method_name)
        try:
            return await self._invoke(getattr(instance, method_name), args, kwargs, frame)
        except BaseException:
            self.services.pop(cache_key, None)
            try:
                await self._shutdown_actor(
                    Actor(service_key, definition.definition_id,
                          definition.revision, instance), frame)
            except BaseException:
                pass
            raise


    async def call(self, frame):
        request_id = frame.get("request_id")
        if not request_id:
            raise ValueError("call requires request_id")
        _WRITER.reset(request_id)
        meta = self._call_meta(frame)
        token = _CALL.set(meta)
        _native_call_enter(meta)
        try:
            definition_id = frame.get("definition_id") or frame.get("function_id")
            definition = self.definitions.get(definition_id)
            if definition is None:
                raise KeyError("definition %r is not initialized" % definition_id)
            frame["_callable_kind"] = self._callable_kind(definition.target)
            revision = frame.get("revision")
            if revision and revision != definition.revision:
                raise ValueError("call revision %r does not match initialized revision %r" %
                                 (revision, definition.revision))
            if frame.get("type") == "service_call":
                value = await self._invoke_service(definition, frame)
                await self._emit_result(value, frame)
                return
            args, kwargs = self._decode_args(frame)
            if (len(args) == 1 and not kwargs and isinstance(args[0], dict) and
                    args[0].get("kind") == "vmon.service.v1"):
                value = await self._invoke_service(definition, frame, args[0])
            else:
                value = await self._invoke(definition.target, args, kwargs, frame)
            await self._emit_result(value, frame)
        except asyncio.CancelledError:
            _WRITER.emit(
                "cancelled", request_id, reason=frame.get("_cancel_reason", "cancelled"),
                worker_kill_required=frame.get("_callable_kind") in ("sync", "generator"))
        except BaseException as exc:
            _WRITER.emit("error", request_id, **self._error_fields(exc, "call"))
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            _native_call_exit(meta)
            _CALL.reset(token)

    async def batch_call(self, frame):
        request_id = frame.get("request_id")
        if not request_id:
            raise ValueError("batch requires request_id")
        _WRITER.reset(request_id)
        meta = self._call_meta(frame)
        meta["execution_mode"] = "batch"
        token = _CALL.set(meta)
        try:
            inputs = frame.get("inputs")
            if not isinstance(inputs, list) or not inputs:
                raise ValueError("batch inputs must be a non-empty list")
            if len(inputs) > MAX_BATCH_INPUTS:
                raise ValueError("batch exceeds %d-input limit" % MAX_BATCH_INPUTS)
            definition_id = frame.get("definition_id") or frame.get("function_id")
            definition = self.definitions.get(definition_id)
            if definition is None:
                raise KeyError("definition %r is not initialized" % definition_id)
            frame["_callable_kind"] = self._callable_kind(definition.target)
            if frame.get("revision") not in (None, definition.revision):
                raise ValueError("batch revision does not match initialized definition")
            values = []
            item_metas = []
            for index, item in enumerate(inputs):
                if not isinstance(item, dict):
                    raise TypeError("batch input %d must be an object" % index)
                merged = dict(frame)
                merged.pop("inputs", None)
                merged.update(item)
                merged["definition_id"] = definition_id
                merged["execution_mode"] = "batch"
                item_meta = self._call_meta(merged)
                item_token = _CALL.set(item_meta)
                try:
                    args, kwargs = self._decode_args(merged)
                finally:
                    _CALL.reset(item_token)
                if len(args) != 1 or kwargs:
                    raise ValueError("batch input %d must decode to one positional value" % index)
                values.append(args[0])
                item_metas.append(item_meta)
            result = await self._invoke(definition.target, [values], {}, frame)
            if not isinstance(result, (list, tuple)) or len(result) != len(inputs):
                actual = len(result) if isinstance(result, (list, tuple)) else type(result).__name__
                raise ValueError("batch output cardinality mismatch: expected %d, got %s" %
                                 (len(inputs), actual))
            results = []
            value_format = frame.get("output_format", "json")
            compression = frame.get("output_compression", "none")
            for item, item_meta, value in zip(inputs, item_metas, result):
                item_token = _CALL.set(item_meta)
                try:
                    encoded = encode_value(value, value_format, compression=compression)
                finally:
                    _CALL.reset(item_token)
                results.append({
                    "request_id": item.get("request_id"),
                    "input_id": item.get("input_id"),
                    "input_index": item.get("input_index"),
                    "value": encoded,
                })
            if frame.get("_cancel_reason"):
                raise asyncio.CancelledError()
            _WRITER.emit("batch_result", request_id, results=results)
        except asyncio.CancelledError:
            _WRITER.emit(
                "cancelled", request_id, reason=frame.get("_cancel_reason", "cancelled"),
                worker_kill_required=frame.get("_callable_kind") in ("sync", "generator"))
        except BaseException as exc:
            _WRITER.emit("error", request_id, **self._error_fields(exc, "batch"))
        finally:
            _CALL.reset(token)


    async def _actor_operation(self, frame):
        request_id = frame.get("request_id")
        if not request_id:
            raise ValueError("actor operation requires request_id")
        _WRITER.reset(request_id)
        meta = self._call_meta(frame)
        token = _CALL.set(meta)
        try:
            operation = frame.get("operation") or frame.get("type")
            actor_id = frame.get("actor_id")
            if not actor_id:
                raise ValueError("actor operation requires stable actor_id")
            if operation in ("actor_create", "create"):
                if len(self.actors) >= MAX_ACTORS:
                    raise RuntimeError("actor limit of %d reached" % MAX_ACTORS)
                if actor_id in self.actors:
                    raise ValueError("actor %r already exists" % actor_id)
                definition_id = frame.get("definition_id")
                definition = self.definitions.get(definition_id)
                if definition is None:
                    raise KeyError("definition %r is not initialized" % definition_id)
                args, kwargs = self._decode_args(frame)
                value = await self._invoke(definition.target, args, kwargs, frame)
                actor = Actor(actor_id, definition_id, definition.revision, value)
                hooks = self._find_hooks(value, "enter")
                if hooks:
                    entered = await self._run_hook(value, "enter", frame)
                else:
                    hook = getattr(value, "__aenter__", None) or getattr(value, "__enter__", None)
                    entered = await self._invoke(hook, [], {}, frame) if hook else None
                if entered is not None:
                    actor.value = entered
                self.actors[actor_id] = actor
                _WRITER.emit("result", request_id, actor_id=actor_id,
                             value=encode_value({"actor_id": actor_id},
                                                frame.get("output_format",
                                                          frame.get("output_codec", "json"))))
                return
            if actor_id not in self.actors and operation in ("actor_restore", "restore"):
                if len(self.actors) >= MAX_ACTORS:
                    raise RuntimeError("actor limit of %d reached" % MAX_ACTORS)
                checkpoint_key = (actor_id, frame.get("checkpoint_id"))
                envelope = frame.get("state") or self.checkpoints.get(checkpoint_key)
                if envelope is None:
                    raise KeyError("checkpoint is unavailable; actor was not reinitialized")
                definition_id = frame.get("definition_id")
                definition = self.definitions.get(definition_id)
                if definition is None or not inspect.isclass(definition.target):
                    raise KeyError("explicit restore of a lost actor requires its class definition_id")
                state = decode_value(
                    envelope,
                    trusted=True if "state" not in frame else bool(frame.get("trusted")),
                    cloudpickle_version=(envelope.get("cloudpickle_version")
                                         if "state" not in frame
                                         else frame.get("cloudpickle_version")))
                value = definition.target.__new__(definition.target)
                self._restore_actor_state(value, state)
                actor = Actor(actor_id, definition_id, definition.revision, value)
                await self._run_hook(value, "restore", frame)
                self.actors[actor_id] = actor
                _WRITER.emit("result", request_id, actor_id=actor_id,
                             value=encode_value({"restored": True, "recreated": True}))
                return
            actor = self.actors.get(actor_id)
            if actor is None:
                raise ActorLostError("actor %r is unavailable; explicit restore is required" %
                                     actor_id)
            definition = self.definitions[actor.definition_id]
            lock = actor.lock if definition.serialized_methods else _NullAsyncLock()
            async with lock:
                if operation in ("actor_call", "method"):
                    method_name = frame.get("method")
                    if not method_name or method_name.startswith("_"):
                        raise ValueError("actor method must be a public method name")
                    if not getattr(type(actor.value).__dict__.get(method_name),
                                   "__vmon_method__", False):
                        raise ValueError("actor method %r is not exported with @vmon.method" %
                                         method_name)
                    method = getattr(actor.value, method_name)
                    args, kwargs = self._decode_args(frame)
                    result = await self._invoke(method, args, kwargs, frame)
                    await self._emit_result(result, frame)
                elif operation in ("actor_checkpoint", "checkpoint"):
                    candidate = _cloudpickle().loads(_cloudpickle().dumps(actor.value))
                    await self._run_hook(candidate, "snapshot_enter", frame)
                    state = self._actor_state(candidate)
                    definition = self.definitions[actor.definition_id]
                    if _contains_secret(state, definition.secret_names, definition.secrets):
                        raise CheckpointContainsSecret(
                            "actor checkpoint contains configured secret material")
                    envelope = encode_value(
                        state, "cloudpickle", spill=False,
                        redact_secrets=False, reject_secrets=False)
                    serialized = base64.b64decode(envelope["inline_data"])
                    if any(secret.encode("utf-8") in serialized
                           for secret in definition.secrets):
                        raise CheckpointContainsSecret(
                            "actor checkpoint serialization contains configured secret material")
                    checkpoint_id = frame.get("checkpoint_id") or request_id
                    checkpoint_key = (actor_id, checkpoint_id)
                    encoded_bytes = len(envelope["inline_data"])
                    if encoded_bytes > MAX_CHECKPOINT_BYTES:
                        raise ValueError("checkpoint exceeds runner checkpoint memory limit")
                    previous = self.checkpoints.pop(checkpoint_key, None)
                    if previous is not None:
                        self.checkpoint_bytes -= len(previous["inline_data"])
                    while self.checkpoints and self.checkpoint_bytes + encoded_bytes > MAX_CHECKPOINT_BYTES:
                        oldest = next(iter(self.checkpoints))
                        removed = self.checkpoints.pop(oldest)
                        self.checkpoint_bytes -= len(removed["inline_data"])
                    self.checkpoints[checkpoint_key] = envelope
                    self.checkpoint_bytes += encoded_bytes
                    _WRITER.emit("result", request_id, actor_id=actor_id,
                                 checkpoint_id=checkpoint_id,
                                 value=_spill_envelope(envelope))
                elif operation in ("actor_restore", "restore"):
                    checkpoint_key = (actor_id, frame.get("checkpoint_id"))
                    envelope = frame.get("state") or self.checkpoints.get(checkpoint_key)
                    if envelope is None:
                        raise KeyError("checkpoint is unavailable; actor was not reinitialized")
                    state = decode_value(
                        envelope,
                        trusted=True if "state" not in frame else bool(frame.get("trusted")),
                        cloudpickle_version=(envelope.get("cloudpickle_version")
                                             if "state" not in frame
                                             else frame.get("cloudpickle_version")))
                    candidate = _cloudpickle().loads(_cloudpickle().dumps(actor.value))
                    self._restore_actor_state(candidate, state)
                    await self._run_hook(candidate, "restore", frame)
                    actor.value = candidate
                    _WRITER.emit("result", request_id, actor_id=actor_id,
                                 value=encode_value({"restored": True}))
                elif operation in ("actor_fork", "fork"):
                    child_id = frame.get("child_actor_id") or frame.get("fork_actor_id")
                    if not child_id or child_id in self.actors:
                        raise ValueError("fork requires a new child_actor_id")
                    if len(self.actors) >= MAX_ACTORS:
                        raise RuntimeError("actor limit of %d reached" % MAX_ACTORS)
                    child_value = _cloudpickle().loads(_cloudpickle().dumps(actor.value))
                    child = Actor(child_id, actor.definition_id, actor.revision, child_value)
                    await self._run_hook(child.value, "restore", frame)
                    self.actors[child_id] = child
                    _WRITER.emit("result", request_id, actor_id=actor_id, child_actor_id=child_id,
                                 value=encode_value({"actor_id": child_id}))
                elif operation in ("actor_shutdown", "shutdown_actor"):
                    await self._shutdown_actor(actor, frame)
                    del self.actors[actor_id]
                    _WRITER.emit("result", request_id, actor_id=actor_id,
                                 value=encode_value({"shutdown": True}))
                else:
                    raise ValueError("unsupported actor operation %r" % operation)
        except asyncio.CancelledError:
            _WRITER.emit("cancelled", request_id, call_id=frame.get("call_id"),
                         reason=frame.get("_cancel_reason", "cancelled"))
        except BaseException as exc:
            _WRITER.emit("error", request_id, call_id=frame.get("call_id"), **self._error_fields(exc, "actor"))
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            _CALL.reset(token)

    @staticmethod
    def _actor_state(value):
        getter = getattr(value, "__getstate__", None)
        return getter() if getter else getattr(value, "__dict__", value)

    @staticmethod
    def _restore_actor_state(value, state):
        setter = getattr(value, "__setstate__", None)
        if setter:
            setter(state)
            return
        dict_state = state
        slot_state = None
        if isinstance(state, tuple) and len(state) == 2:
            dict_state, slot_state = state
        if dict_state is not None:
            if not isinstance(dict_state, dict) or not hasattr(value, "__dict__"):
                raise TypeError("actor dictionary state cannot be restored")
            value.__dict__.clear()
            value.__dict__.update(dict_state)
        if slot_state is not None:
            if not isinstance(slot_state, dict):
                raise TypeError("actor slot state must be an object")
            for name, item in slot_state.items():
                setattr(value, name, item)
        if dict_state is None and slot_state is None:
            return

    @staticmethod
    def _find_hooks(value, name):
        if name == "enter":
            marker = "__vmon_enter__"
            predicate = lambda item: not getattr(item, "__vmon_snapshot_enter__", False)
        elif name == "snapshot_enter":
            marker = "__vmon_enter__"
            predicate = lambda item: bool(getattr(item, "__vmon_snapshot_enter__", False))
        elif name == "restore":
            marker = "__vmon_restore__"
            predicate = lambda item: True
        elif name == "exit":
            marker = "__vmon_exit__"
            predicate = lambda item: True
        else:
            return []
        if getattr(value, marker, False) and predicate(value):
            return [value]
        hooks = []
        for owner in reversed(type(value).__mro__):
            for attribute_name, attribute in owner.__dict__.items():
                if getattr(attribute, marker, False) and predicate(attribute):
                    hooks.append(getattr(value, attribute_name))
        return hooks

    async def _run_hook(self, value, name, frame):
        result = None
        for hook in self._find_hooks(value, name):
            hook_result = await self._invoke(hook, [], {}, frame)
            if hook_result is not None:
                result = hook_result
        return result

    async def _shutdown_actor(self, actor, frame):
        hooks = self._find_hooks(actor.value, "exit")
        if hooks:
            for hook in hooks:
                await self._invoke(hook, [], {}, frame)
            return
        exit_hook = getattr(actor.value, "__aexit__", None) or getattr(actor.value, "__exit__", None)
        if exit_hook:
            await self._invoke(exit_hook, [None, None, None], {}, frame)

    async def lifecycle(self, frame):
        request_id = frame.get("request_id")
        if not request_id:
            raise ValueError("lifecycle frame requires request_id")
        _WRITER.reset(request_id)
        meta = self._call_meta(frame)
        for definition in self.definitions.values():
            meta["_secrets"].update(definition.secrets)
        token = _CALL.set(meta)
        operation = frame.get("type") or frame.get("operation")
        hook_name = "snapshot_enter" if operation == "before_snapshot" else "restore"
        try:
            if operation == "before_snapshot" and any(
                    definition.secrets for definition in self.definitions.values()):
                raise SnapshotWithSecretsUnsupported(
                    "VM snapshots are unsupported while function secrets are configured")
            for actor in list(self.actors.values()):
                async with actor.lock:
                    await self._run_hook(actor.value, hook_name, frame)
            for definition in self.definitions.values():
                if not inspect.isclass(definition.target):
                    await self._run_hook(definition.target, hook_name, frame)
            _WRITER.emit("status", request_id, status=operation + "_complete")
        except BaseException as exc:
            _WRITER.emit("error", request_id, **self._error_fields(exc, hook_name))
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            _CALL.reset(token)


    async def submit(self, frame):
        operation = frame.get("type") or frame.get("operation")
        if operation in ("hello", "handshake"):
            _emit_hello()
            return
        if operation == "define":
            await self._guard_control(frame, self.define)
            return
        if operation in ("before_snapshot", "after_restore"):
            await self.lifecycle(frame)
            return
        if operation in ("cancel", "deadline_cancel"):
            request_id = frame.get("target_request_id") or frame.get("request_id")
            task = self.running.get(request_id)
            if task is not None:
                self.running_frames[request_id]["_cancel_reason"] = "cancelled"
                task.cancel()
            else:
                _WRITER.emit("status", frame.get("request_id"), status="not_running",
                             target_request_id=request_id)
            return
        if operation == "shutdown":
            self.stopping = True
            for task in list(self.running.values()):
                task.cancel()
            _WRITER.emit("status", frame.get("request_id"), status="shutting_down")
            return
        if operation == "batch":
            coroutine = self.batch_call(frame)
        elif operation in ("call", "invoke", "service_call"):
            coroutine = self.call(frame)
        elif operation in ("actor_create", "actor_call", "actor_checkpoint", "actor_restore",
                           "actor_fork", "actor_shutdown", "create", "method", "checkpoint",
                           "restore", "fork", "shutdown_actor"):
            coroutine = self._actor_operation(frame)
        else:
            raise ValueError("unknown protocol-v2 frame type %r" % operation)
        request_id = frame.get("request_id")
        if not request_id:
            raise ValueError("%s requires request_id" % operation)
        if len(self.running) >= MAX_PENDING_TASKS:
            coroutine.close()
            raise RunnerOverloaded("runner pending request limit reached")
        if request_id in self.running:
            raise ValueError("request_id %r is already running" % request_id)
        deadline = frame.get("deadline_unix_ms")
        if deadline is not None and float(deadline) / 1000.0 <= time.time():
            frame["_cancel_reason"] = "deadline_exceeded"
            coroutine.close()
            self._emit_prestart_cancel(frame)
            return
        task = asyncio.create_task(self._bounded(coroutine))
        self.running[request_id] = task
        self.running_frames[request_id] = frame
        def finished(done, rid=request_id, pending=coroutine, request_frame=frame):
            self.running.pop(rid, None)
            self.running_frames.pop(rid, None)
            handle = self.deadline_handles.pop(rid, None)
            if handle is not None:
                handle.cancel()
            if done.cancelled():
                pending.close()
                self._emit_prestart_cancel(request_frame)
        task.add_done_callback(finished)
        if deadline is not None:
            delay = max(0.0, float(deadline) / 1000.0 - time.time())
            self.deadline_handles[request_id] = asyncio.get_running_loop().call_later(
                delay, self._deadline, request_id)

    def _emit_prestart_cancel(self, frame):
        request_id = frame["request_id"]
        _WRITER.reset(request_id)
        token = _CALL.set(self._call_meta(frame))
        try:
            _WRITER.emit("cancelled", request_id,
                         reason=frame.get("_cancel_reason", "cancelled"))
        finally:
            _CALL.reset(token)


    async def _guard_control(self, frame, function):
        request_id = frame.get("request_id")
        if not request_id:
            _WRITER.emit("error", None,
                         **self._error_fields(ValueError("define requires request_id"),
                                              "definition"))
            return
        _WRITER.reset(request_id)
        token = _CALL.set(self._call_meta(frame))
        try:
            await function(frame)
        except BaseException as exc:
            _WRITER.emit("error", request_id, **self._error_fields(exc, "definition"))
        finally:
            _CALL.reset(token)

    async def _bounded(self, coroutine):
        async with self.slots:
            await coroutine

    def _deadline(self, request_id):
        task = self.running.get(request_id)
        if task is not None and not task.done():
            self.running_frames[request_id]["_cancel_reason"] = "deadline_exceeded"
            task.cancel()

    async def close(self):
        tasks = list(self.running.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for actor in list(self.actors.values()):
            frame = {"request_id": "shutdown", "definition_id": actor.definition_id}
            token = _CALL.set(self._call_meta(frame))
            try:
                await self._shutdown_actor(actor, frame)
            except BaseException:
                pass
            finally:
                _CALL.reset(token)
        for (revision, service_key), instance in list(self.services.items()):
            definition = next(
                (item for item in self.definitions.values() if item.revision == revision), None)
            if definition is not None:
                frame = {"request_id": "shutdown", "definition_id": definition.definition_id}
                token = _CALL.set(self._call_meta(frame))
                try:
                    await self._shutdown_actor(
                        Actor(service_key, definition.definition_id, revision, instance), frame)
                except BaseException:
                    pass
                finally:
                    _CALL.reset(token)
        self.services.clear()
        self.actors.clear()
        self.executor.shutdown(wait=False)
        for definition in self.definitions.values():
            if definition.cleanup_root:
                shutil.rmtree(definition.cleanup_root, ignore_errors=True)
        for extract_root in self.package_cache.values():
            shutil.rmtree(os.path.dirname(extract_root), ignore_errors=True)
        self.package_cache.clear()
        for definition in self.definitions.values():
            definition.secrets.clear()
        self.definitions.clear()
        _cleanup_spills()


class _NullAsyncLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _reader_thread(loop, incoming):
    try:
        while True:
            raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
            if not raw:
                asyncio.run_coroutine_threadsafe(incoming.put(None), loop).result()
                return
            if len(raw) > MAX_FRAME_BYTES or not raw.endswith(b"\n"):
                if len(raw) > MAX_FRAME_BYTES and not raw.endswith(b"\n"):
                    while raw and not raw.endswith(b"\n"):
                        raw = sys.stdin.buffer.readline(MAX_FRAME_BYTES + 1)
                item = ValueError("input frame exceeds %d bytes or lacks newline" % MAX_FRAME_BYTES)
            else:
                try:
                    item = json.loads(raw)
                    if not isinstance(item, dict):
                        raise ValueError("input frame must be a JSON object")
                except BaseException as exc:
                    item = exc
            asyncio.run_coroutine_threadsafe(incoming.put(item), loop).result()
    except BaseException as exc:
        try:
            asyncio.run_coroutine_threadsafe(incoming.put(exc), loop).result()
            asyncio.run_coroutine_threadsafe(incoming.put(None), loop).result()
        except BaseException:
            pass


def _emit_hello():
    try:
        cloudpickle_version = _cloudpickle_version()
    except RuntimeError:
        cloudpickle_version = None
    formats = ["json", "cbor"] + (["cloudpickle"] if cloudpickle_version else [])
    _WRITER.emit("hello", None, version=PROTOCOL_VERSION, envelope_version=ENVELOPE_VERSION,
                 cloudpickle_version=cloudpickle_version, python_abi=_python_abi(),
                 python=platform.python_version(), formats=formats,
                 capabilities=["package", "serialized", "async", "generator", "batch",
                               "actors", "checkpoint", "restore", "fork", "cancel", "deadline",
                               "reconnect", "services"])


async def _process_item(runner, item):
    if isinstance(item, BaseException):
        _WRITER.emit("error", None, **runner._error_fields(item, "protocol"))
        return
    protocol = item.get("protocol", item.get("version", PROTOCOL_VERSION))
    if protocol != PROTOCOL_VERSION:
        _WRITER.emit("error", item.get("request_id"),
                     **runner._error_fields(ValueError(
                         "protocol version mismatch: peer sent %r, runner requires %d" %
                         (protocol, PROTOCOL_VERSION)), "handshake"))
        return
    try:
        await runner.submit(item)
    except BaseException as exc:
        _WRITER.emit("error", item.get("request_id"), **runner._error_fields(exc, "protocol"))


async def _run_stdio(runner, max_tasks):
    loop = asyncio.get_running_loop()
    incoming = asyncio.Queue(maxsize=max_tasks * 2)
    reader = threading.Thread(target=_reader_thread, args=(loop, incoming),
                              name="vmon-protocol-reader", daemon=True)
    reader.start()
    _emit_hello()
    while not runner.stopping:
        item = await incoming.get()
        if item is None:
            break
        await _process_item(runner, item)


async def _run_socket(runner, socket_path):
    parent = os.path.dirname(socket_path)
    if parent:
        os.makedirs(parent, mode=0o700, exist_ok=True)
    try:
        os.unlink(socket_path)
    except FileNotFoundError:
        pass
    connection_lock = asyncio.Lock()
    stopped = asyncio.Event()

    async def serve_connection(reader, writer):
        async with connection_lock:
            transport_socket = writer.get_extra_info("socket")
            _WRITER.replace_fd(transport_socket.fileno())
            _emit_hello()
            try:
                while not runner.stopping:
                    try:
                        raw = await reader.readline()
                    except ValueError:
                        await _process_item(
                            runner, ValueError("input frame exceeds %d bytes" %
                                               MAX_FRAME_BYTES))
                        continue
                    except (ConnectionError, asyncio.CancelledError):
                        break
                    if not raw:
                        break
                    if len(raw) > MAX_FRAME_BYTES or not raw.endswith(b"\n"):
                        await _process_item(
                            runner, ValueError("input frame exceeds %d bytes or lacks newline" %
                                               MAX_FRAME_BYTES))
                        continue
                    try:
                        item = json.loads(raw)
                        if not isinstance(item, dict):
                            raise ValueError("input frame must be a JSON object")
                    except BaseException as exc:
                        item = exc
                    await _process_item(runner, item)
                if runner.stopping:
                    stopped.set()
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, BrokenPipeError):
                    pass

    server = await asyncio.start_unix_server(
        lambda reader, writer: asyncio.create_task(serve_connection(reader, writer)),
        path=socket_path, limit=MAX_FRAME_BYTES + 1)
    os.chmod(socket_path, 0o600)
    try:
        await stopped.wait()
    finally:
        server.close()
        await server.wait_closed()
        try:
            os.unlink(socket_path)
        except FileNotFoundError:
            pass


async def run(max_threads=_DEFAULT_THREADS, max_tasks=_DEFAULT_TASKS, socket_path=None):
    runner = Runner(max_threads, max_tasks)
    try:
        if socket_path:
            await _run_socket(runner, socket_path)
        else:
            await _run_stdio(runner, max_tasks)
    finally:
        await runner.close()
        _WRITER.emit("status", None, status="stopped")
        _WRITER.close()


def main():
    parser = argparse.ArgumentParser(description="vmon protocol-v2 guest function runner")
    parser.add_argument("--max-sync-threads", type=int, default=_DEFAULT_THREADS)
    parser.add_argument("--max-async-tasks", type=int, default=_DEFAULT_TASKS)
    parser.add_argument("--socket", help="serve sequential NDJSON connections on this Unix socket")
    args = parser.parse_args()
    if args.max_sync_threads < 1 or args.max_async_tasks < 1:
        parser.error("concurrency limits must be positive")
    asyncio.run(run(args.max_sync_threads, args.max_async_tasks, args.socket))


if __name__ == "__main__":
    main()
