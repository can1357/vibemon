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
import os
import platform
import queue
import shutil
import signal
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import types
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROTOCOL_VERSION = 2
ENVELOPE_VERSION = 1
CLOUDPICKLE_CODEC_VERSION = 1
MAX_FRAME_BYTES = int(os.environ.get("VMON_RUNNER_MAX_FRAME_BYTES", str(16 * 1024 * 1024)))
MAX_VALUE_BYTES = int(os.environ.get("VMON_RUNNER_MAX_VALUE_BYTES", str(64 * 1024 * 1024)))
MAX_DEFINITIONS = int(os.environ.get("VMON_RUNNER_MAX_DEFINITIONS", "4096"))
MAX_ACTORS = int(os.environ.get("VMON_RUNNER_MAX_ACTORS", "10000"))
MAX_CHECKPOINT_BYTES = int(os.environ.get(
    "VMON_RUNNER_MAX_CHECKPOINT_BYTES", str(256 * 1024 * 1024)))
MAX_PACKAGE_EXTRACT_BYTES = int(os.environ.get(
    "VMON_RUNNER_MAX_PACKAGE_EXTRACT_BYTES", str(512 * 1024 * 1024)))
MAX_PACKAGE_FILES = int(os.environ.get("VMON_RUNNER_MAX_PACKAGE_FILES", "10000"))
MAX_LOG_FRAGMENT_BYTES = int(os.environ.get("VMON_RUNNER_MAX_LOG_FRAGMENT_BYTES", "65536"))
_DEFAULT_THREADS = max(1, min(32, int(os.environ.get("VMON_RUNNER_SYNC_THREADS", "8"))))
_DEFAULT_TASKS = max(1, int(os.environ.get("VMON_RUNNER_ASYNC_TASKS", "128")))
_CALL = contextvars.ContextVar("vmon_current_call", default=None)


def current_call():
    """Return immutable metadata for the active call, or None outside a call."""
    value = _CALL.get()
    if value is None:
        return None
    return {key: item for key, item in value.items() if not key.startswith("_")}


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


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


def decode_value(envelope, trusted=False):
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
        data = zstandard.ZstdDecompressor().decompress(stored, max_output_size=MAX_VALUE_BYTES)
    else:
        raise ValueError("unsupported envelope compression %r" % compression)
    if len(data) > MAX_VALUE_BYTES:
        raise ValueError("decoded value exceeds %d-byte limit" % MAX_VALUE_BYTES)
    expected_size = envelope.get("uncompressed_size")
    if expected_size is None or int(expected_size) != len(data):
        raise ValueError("value uncompressed_size mismatch: expected %r, decoded %d" %
                         (expected_size, len(data)))
    _verify_checksum(envelope.get("sha256"), data)
    value_format = envelope.get("format")
    if value_format == "json":
        try:
            return json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("JSON envelope is not valid UTF-8 JSON") from exc
    if value_format == "cbor":
        try:
            import cbor2  # type: ignore
        except ImportError as exc:
            raise RuntimeError("CBOR codec requested but cbor2 is not installed in the guest") from exc
        return cbor2.loads(data)
    if value_format == "cloudpickle":
        if not trusted or envelope.get("trusted") is False:
            raise ValueError("cloudpickle is executable data and requires trusted=true")
        abi = envelope.get("python_abi")
        if abi != _python_abi():
            raise ValueError("cloudpickle Python ABI mismatch: payload requires %r, guest is %r" % (abi, _python_abi()))
        codec_version = envelope.get("codec_version")
        if codec_version != CLOUDPICKLE_CODEC_VERSION:
            raise ValueError("unsupported cloudpickle codec_version %r; runner supports %d" % (codec_version, CLOUDPICKLE_CODEC_VERSION))
        return _cloudpickle().loads(data)
    raise ValueError("unsupported value format %r; expected json, cbor, or cloudpickle" % value_format)


def encode_value(value, value_format="json"):
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
        extra = {"python_abi": _python_abi(), "codec_version": CLOUDPICKLE_CODEC_VERSION}
    else:
        raise ValueError("unsupported output format %r" % value_format)
    if len(data) > MAX_VALUE_BYTES:
        raise ValueError("encoded value exceeds %d-byte limit" % MAX_VALUE_BYTES)
    result = {"version": ENVELOPE_VERSION, "format": value_format,
              "inline_data": base64.b64encode(data).decode("ascii"),
              "uncompressed_size": len(data), "sha256": _checksum(data),
              "compression": "none"}
    result.update(extra)
    return result


class ActorLostError(RuntimeError):
    pass


class RemoteFunctionError(RuntimeError):
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


def _install_vmon_shim():
    existing = sys.modules.get("vmon")
    if existing is not None and getattr(existing, "__vmon_guest_shim__", False):
        return
    shim = types.ModuleType("vmon")
    shim.__vmon_guest_shim__ = True
    shim.function = _decorator("function")
    shim.cls = _decorator("cls")
    shim.service = _decorator("service")
    shim.method = _decorator("method")
    shim.concurrent = _decorator("concurrent")
    shim.batched = _decorator("batched")
    shim.enter = _decorator("enter", "enter")
    shim.exit = _decorator("exit", "exit")
    shim.before_snapshot = _decorator("before_snapshot", "before_snapshot")
    shim.after_restore = _decorator("after_restore", "after_restore")
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
    added = False
    if root:
        root = os.path.abspath(root)
        if root not in sys.path:
            sys.path.insert(0, root)
            added = True
    try:
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
    finally:
        if added:
            try:
                sys.path.remove(root)
            except ValueError:
                pass


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
                    for key in ("call_id", "input_id", "attempt", "parent_call_id"):
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


class _CapturedStream(io.TextIOBase):
    def __init__(self, name):
        self.name = name
        self._buffers = contextvars.ContextVar("vmon_%s_buffer" % name, default="")

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
        if not message and text:
            return len(text)
        if meta is None:
            _WRITER.emit("log", None, stream=self.name, message=message,
                         partial=not text.endswith(("\n", "\r")))
            return len(text)
        while len(message.encode("utf-8", "replace")) > MAX_LOG_FRAGMENT_BYTES:
            fragment = message[:MAX_LOG_FRAGMENT_BYTES]
            _WRITER.emit("log", meta["request_id"], stream=self.name,
                         message=fragment, partial=True)
            message = message[MAX_LOG_FRAGMENT_BYTES:]
        if message:
            _WRITER.emit("log", meta["request_id"], stream=self.name, message=message,
                         partial=not text.endswith(("\n", "\r")))
        return len(text)

    def flush(self):
        text = self._buffers.get()
        if text:
            meta = _CALL.get()
            _WRITER.emit("log", meta.get("request_id") if meta else None,
                         call_id=meta.get("call_id") if meta else None,
                         stream=self.name, message=text)
            self._buffers.set("")


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


@dataclass
class Actor:
    actor_id: str
    definition_id: str
    revision: str
    value: Any
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class Runner:
    def __init__(self, max_threads=_DEFAULT_THREADS, max_tasks=_DEFAULT_TASKS):
        self.definitions = {}
        self.actors = {}
        self.checkpoints = {}
        self.checkpoint_bytes = 0
        self.running = {}
        self.running_frames = {}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads,
                                                               thread_name_prefix="vmon-call")
        self.slots = asyncio.Semaphore(max_tasks)
        self.stopping = False

    def _error_fields(self, exc, phase=None):
        fields = {
            "error": {
                "type": type(exc).__name__,
                "module": type(exc).__module__,
                "message": str(exc),
                "repr": repr(exc),
                "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            }
        }
        if phase:
            fields["error"]["phase"] = phase
        cause = exc.__cause__ or exc.__context__
        if cause is not None:
            fields["error"]["cause"] = {"type": type(cause).__name__,
                                          "module": type(cause).__module__,
                                          "message": str(cause)}
        return fields

    def _extract_package(self, definition):
        root = definition.get("root")
        archive = definition.get("archive")
        if archive is None:
            if not root or not os.path.isdir(root):
                raise ValueError("package definition root is not a directory: %r" % root)
            return os.path.abspath(root), None
        archive_data = decode_value(archive, trusted=False)
        if not isinstance(archive_data, str):
            raise ValueError("package archive envelope must decode to base64 archive text")
        raw = base64.b64decode(archive_data)
        temporary = tempfile.mkdtemp(prefix="vmon-function-")
        archive_path = os.path.join(temporary, "package")
        try:
            with open(archive_path, "wb") as output:
                output.write(raw)
            extract_root = os.path.join(temporary, "root")
            os.mkdir(extract_root)
            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path) as bundle:
                    self._safe_zip_extract(bundle, extract_root)
            elif tarfile.is_tarfile(archive_path):
                with tarfile.open(archive_path) as bundle:
                    self._safe_tar_extract(bundle, extract_root)
            else:
                raise ValueError("package archive is neither zip nor tar")
            os.remove(archive_path)
            return extract_root, temporary
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

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
        members = bundle.getmembers()
        if len(members) > MAX_PACKAGE_FILES:
            raise ValueError("package archive exceeds %d-file limit" % MAX_PACKAGE_FILES)
        if sum(member.size for member in members) > MAX_PACKAGE_EXTRACT_BYTES:
            raise ValueError("package archive exceeds %d-byte extracted limit" %
                             MAX_PACKAGE_EXTRACT_BYTES)
        base = os.path.realpath(root) + os.sep
        for member in members:
            destination = os.path.realpath(os.path.join(root, member.name))
            if (not destination.startswith(base) or member.issym() or member.islnk() or
                    not (member.isfile() or member.isdir())):
                raise ValueError("package archive contains unsafe member %r" % member.name)
        bundle.extractall(root)

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
            _WRITER.emit("status", request_id, status="already_initialized",
                         definition_id=definition_id, revision=revision)
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
            target = decode_value(payload, trusted=bool(spec.get("trusted")))
            root = None
        else:
            raise ValueError("definition mode must be package or serialized")
        if not callable(target):
            raise TypeError("loaded definition target is not callable")
        metadata = getattr(target, "__vmon__", {})
        serialized_methods = spec.get("serialized_methods")
        if serialized_methods is None:
            serialized_methods = metadata.get("kind") != "concurrent"
        self.definitions[definition_id] = Definition(
            definition_id, revision, target, mode, root, cleanup_root, bool(serialized_methods))
        _WRITER.emit("status", request_id, status="initialized",
                     definition_id=definition_id, revision=revision)

    def _call_meta(self, frame):
        secrets = _secret_strings(frame.get("secrets", {}))
        return {
            "request_id": frame["request_id"],
            "input_id": frame.get("input_id"),
            "call_id": frame.get("call_id"),
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
        if "args" in frame:
            args = decode_value(frame["args"], trusted=bool(frame.get("trusted")))
            if not isinstance(args, (list, tuple)):
                raise TypeError("args envelope must decode to a list or tuple")
        elif "input" in frame:
            args = [decode_value(frame["input"], trusted=bool(frame.get("trusted")))]
        else:
            args = []
        kwargs = decode_value(frame["kwargs"], trusted=bool(frame.get("trusted"))) if "kwargs" in frame else {}
        if not isinstance(kwargs, dict):
            raise TypeError("kwargs envelope must decode to an object")
        return list(args), kwargs

    async def _invoke(self, function, args, kwargs, frame):
        loop = asyncio.get_running_loop()
        if inspect.isasyncgenfunction(function):
            return function(*args, **kwargs)
        if inspect.iscoroutinefunction(function):
            return await function(*args, **kwargs)
        context = contextvars.copy_context()
        result = await loop.run_in_executor(self.executor, context.run,
                                            lambda: function(*args, **kwargs))
        if inspect.isawaitable(result):
            return await result
        return result

    async def _emit_result(self, value, frame):
        request_id = frame["request_id"]
        codec = frame.get("output_format", frame.get("output_codec", "json"))
        if inspect.isasyncgen(value):
            index = 0
            async for item in value:
                _WRITER.emit("yield", request_id, call_id=frame.get("call_id"), index=index,
                             value=encode_value(item, codec))
                index += 1
            _WRITER.emit("result", request_id, call_id=frame.get("call_id"),
                         value=encode_value(None, codec), yield_count=index)
            return
        if inspect.isgenerator(value) or (frame.get("execution_mode") == "generator" and
                                           hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict))):
            index = 0
            iterator = iter(value)
            loop = asyncio.get_running_loop()
            sentinel = object()
            while True:
                context = contextvars.copy_context()
                item = await loop.run_in_executor(self.executor, context.run,
                                                  lambda: next(iterator, sentinel))
                if item is sentinel:
                    break
                _WRITER.emit("yield", request_id, call_id=frame.get("call_id"), index=index,
                             value=encode_value(item, codec))
                index += 1
            _WRITER.emit("result", request_id, call_id=frame.get("call_id"),
                         value=encode_value(None, codec), yield_count=index)
            return
        _WRITER.emit("result", request_id, call_id=frame.get("call_id"),
                     value=encode_value(value, codec))

    async def call(self, frame):
        request_id = frame.get("request_id")
        if not request_id:
            raise ValueError("call requires request_id")
        _WRITER.reset(request_id)
        meta = self._call_meta(frame)
        token = _CALL.set(meta)
        try:
            definition_id = frame.get("definition_id")
            definition = self.definitions.get(definition_id)
            if definition is None:
                raise KeyError("definition %r is not initialized" % definition_id)
            revision = frame.get("revision")
            if revision and revision != definition.revision:
                raise ValueError("call revision %r does not match initialized revision %r" %
                                 (revision, definition.revision))
            args, kwargs = self._decode_args(frame)
            if frame.get("execution_mode") == "batch":
                if len(args) != 1 or not isinstance(args[0], list):
                    raise ValueError("batch call requires one list argument")
                expected = len(args[0])
                value = await self._invoke(definition.target, args, kwargs, frame)
                if inspect.isawaitable(value):
                    value = await value
                if not isinstance(value, (list, tuple)) or len(value) != expected:
                    actual = len(value) if isinstance(value, (list, tuple)) else type(value).__name__
                    raise ValueError("batch output cardinality mismatch: expected %d, got %s" % (expected, actual))
            else:
                value = await self._invoke(definition.target, args, kwargs, frame)
            await self._emit_result(value, frame)
        except asyncio.CancelledError:
            _WRITER.emit("cancelled", request_id, call_id=frame.get("call_id"),
                         reason=frame.get("_cancel_reason", "cancelled"))
        except BaseException as exc:
            _WRITER.emit("error", request_id, call_id=frame.get("call_id"), **self._error_fields(exc, "call"))
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
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
                self.actors[actor_id] = actor
                hook = self._find_hook(value, "enter")
                if hook:
                    entered = await self._invoke(hook, [], {}, frame)
                else:
                    hook = getattr(value, "__aenter__", None) or getattr(value, "__enter__", None)
                    entered = await self._invoke(hook, [], {}, frame) if hook else None
                if entered is not None:
                    actor.value = entered
                _WRITER.emit("result", request_id, actor_id=actor_id,
                             value=encode_value({"actor_id": actor_id},
                                                frame.get("output_format",
                                                          frame.get("output_codec", "json"))))
                return
            if actor_id not in self.actors and operation in ("actor_restore", "restore"):
                if len(self.actors) >= MAX_ACTORS:
                    raise RuntimeError("actor limit of %d reached" % MAX_ACTORS)
                envelope = frame.get("state") or self.checkpoints.get(frame.get("checkpoint_id"))
                if envelope is None:
                    raise KeyError("checkpoint is unavailable; actor was not reinitialized")
                definition_id = frame.get("definition_id")
                definition = self.definitions.get(definition_id)
                if definition is None or not inspect.isclass(definition.target):
                    raise KeyError("explicit restore of a lost actor requires its class definition_id")
                state = decode_value(envelope, trusted=True)
                value = definition.target.__new__(definition.target)
                self._restore_actor_state(value, state)
                actor = Actor(actor_id, definition_id, definition.revision, value)
                self.actors[actor_id] = actor
                await self._run_hook(value, "after_restore", frame)
                _WRITER.emit("result", request_id, actor_id=actor_id,
                             value=encode_value({"restored": True, "recreated": True}))
                return
            actor = self.actors.get(actor_id)
            if actor is None:
                raise KeyError("actor %r is unavailable; explicit restore is required" % actor_id)
            lock = actor.lock if self.definitions[actor.definition_id].serialized_methods else _NullAsyncLock()
            async with lock:
                if operation in ("actor_call", "method"):
                    method_name = frame.get("method")
                    if not method_name or method_name.startswith("_"):
                        raise ValueError("actor method must be a public method name")
                    method = getattr(actor.value, method_name)
                    args, kwargs = self._decode_args(frame)
                    result = await self._invoke(method, args, kwargs, frame)
                    await self._emit_result(result, frame)
                elif operation in ("actor_checkpoint", "checkpoint"):
                    await self._run_hook(actor.value, "before_snapshot", frame)
                    state = _redact(self._actor_state(actor.value), meta["_secrets"])
                    envelope = encode_value(state, "cloudpickle")
                    checkpoint_id = frame.get("checkpoint_id") or request_id
                    previous = self.checkpoints.pop(checkpoint_id, None)
                    if previous is not None:
                        self.checkpoint_bytes -= len(previous["inline_data"])
                    encoded_bytes = len(envelope["inline_data"])
                    while self.checkpoints and self.checkpoint_bytes + encoded_bytes > MAX_CHECKPOINT_BYTES:
                        oldest = next(iter(self.checkpoints))
                        removed = self.checkpoints.pop(oldest)
                        self.checkpoint_bytes -= len(removed["inline_data"])
                    if encoded_bytes > MAX_CHECKPOINT_BYTES:
                        raise ValueError("checkpoint exceeds runner checkpoint memory limit")
                    self.checkpoints[checkpoint_id] = envelope
                    self.checkpoint_bytes += encoded_bytes
                    _WRITER.emit("result", request_id, actor_id=actor_id, checkpoint_id=checkpoint_id,
                                 value=envelope)
                elif operation in ("actor_restore", "restore"):
                    envelope = frame.get("state") or self.checkpoints.get(frame.get("checkpoint_id"))
                    if envelope is None:
                        raise KeyError("checkpoint is unavailable; actor was not reinitialized")
                    state = decode_value(envelope, trusted=True)
                    self._restore_actor_state(actor.value, state)
                    await self._run_hook(actor.value, "after_restore", frame)
                    _WRITER.emit("result", request_id, actor_id=actor_id,
                                 value=encode_value({"restored": True}))
                elif operation in ("actor_fork", "fork"):
                    child_id = frame.get("child_actor_id") or frame.get("fork_actor_id")
                    if not child_id or child_id in self.actors:
                        raise ValueError("fork requires a new child_actor_id")
                    if len(self.actors) >= MAX_ACTORS:
                        raise RuntimeError("actor limit of %d reached" % MAX_ACTORS)
                    state = self._actor_state(actor.value)
                    child_value = _cloudpickle().loads(_cloudpickle().dumps(actor.value))
                    self._restore_actor_state(child_value, state)
                    child = Actor(child_id, actor.definition_id, actor.revision, child_value)
                    self.actors[child_id] = child
                    await self._run_hook(child.value, "after_restore", frame)
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
        elif hasattr(value, "__dict__") and isinstance(state, dict):
            value.__dict__.clear()
            value.__dict__.update(state)
        else:
            raise TypeError("actor does not support state restoration")

    @staticmethod
    def _find_hook(value, name):
        hook = getattr(value, name, None)
        if hook is not None:
            return hook
        marker = "__vmon_%s__" % name
        for owner in type(value).__mro__:
            for attribute_name, attribute in owner.__dict__.items():
                if getattr(attribute, marker, False):
                    return getattr(value, attribute_name)
        return None

    async def _run_hook(self, value, name, frame):
        hook = self._find_hook(value, name)
        if hook:
            return await self._invoke(hook, [], {}, frame)
        return None

    async def _shutdown_actor(self, actor, frame):
        hook = self._find_hook(actor.value, "exit")
        if hook:
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
        token = _CALL.set(self._call_meta(frame))
        operation = frame.get("type") or frame.get("operation")
        hook_name = "before_snapshot" if operation == "before_snapshot" else "after_restore"
        try:
            for actor in list(self.actors.values()):
                async with actor.lock:
                    await self._run_hook(actor.value, hook_name, frame)
            for definition in self.definitions.values():
                if not inspect.isclass(definition.target):
                    await self._run_hook(definition.target, hook_name, frame)
            _WRITER.emit("status", request_id, status=hook_name + "_complete")
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
        if operation in ("call", "invoke"):
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
        if request_id in self.running:
            raise ValueError("request_id %r is already running" % request_id)
        task = asyncio.create_task(self._bounded(coroutine))
        self.running[request_id] = task
        self.running_frames[request_id] = frame
        def finished(done, rid=request_id):
            self.running.pop(rid, None)
            self.running_frames.pop(rid, None)
        task.add_done_callback(finished)
        deadline = frame.get("deadline_unix_ms")
        if deadline is not None:
            delay = max(0.0, float(deadline) / 1000.0 - time.time())
            asyncio.get_running_loop().call_later(delay, self._deadline, request_id)

    async def _guard_control(self, frame, function):
        request_id = frame.get("request_id")
        try:
            await function(frame)
        except BaseException as exc:
            _WRITER.emit("error", request_id, **self._error_fields(exc, "definition"))

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
            try:
                await self._shutdown_actor(actor, {"request_id": "shutdown"})
            except BaseException:
                pass
        self.actors.clear()
        self.executor.shutdown(wait=False)
        for definition in self.definitions.values():
            if definition.cleanup_root:
                shutil.rmtree(definition.cleanup_root, ignore_errors=True)


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
    _WRITER.emit("hello", None, version=PROTOCOL_VERSION, envelope_version=ENVELOPE_VERSION,
                 cloudpickle_codec_version=CLOUDPICKLE_CODEC_VERSION, python_abi=_python_abi(),
                 python=platform.python_version(), formats=["json", "cbor", "cloudpickle"],
                 capabilities=["package", "serialized", "async", "generator", "batch",
                               "actors", "checkpoint", "restore", "fork", "cancel", "deadline",
                               "reconnect"])


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
