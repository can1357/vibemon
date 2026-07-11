from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import inspect
import io
import json
import sys
import threading
import traceback
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from vmon.wsframe import encode_frame, read_frame_sync


@dataclass
class RecordedRequest:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    json: Any
    body: bytes


class StubError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class V1StubServer:
    """Small v1 HTTP + WebSocket server for thin-SDK behavior tests."""

    def __init__(self) -> None:
        self.requests: list[RecordedRequest] = []
        self.sandboxes: dict[str, dict[str, Any]] = {}
        self.volumes: set[str] = set()
        self._lock = threading.RLock()
        self._next_id = 1
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self._server.stub = self  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> V1StubServer:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def recorded(self, method: str | None = None, path: str | None = None) -> list[RecordedRequest]:
        with self._lock:
            rows = list(self.requests)
        if method is not None:
            rows = [row for row in rows if row.method == method]
        if path is not None:
            rows = [row for row in rows if row.path == path]
        return rows

    def last(self, method: str, path: str) -> RecordedRequest:
        rows = self.recorded(method, path)
        assert rows, (
            f"no recorded {method} {path}; saw {[(r.method, r.path) for r in self.requests]}"
        )
        return rows[-1]

    def _handler_class(self) -> type[BaseHTTPRequestHandler]:
        stub_type = type(self)

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, _format: str, *_args: object) -> None:
                return

            def do_GET(self) -> None:
                if self.headers.get("Upgrade", "").lower() == "websocket":
                    self._handle_websocket()
                else:
                    self._handle_http()

            def do_POST(self) -> None:
                self._handle_http()

            def do_PUT(self) -> None:
                self._handle_http()

            def do_DELETE(self) -> None:
                self._handle_http()

            def _handle_websocket(self) -> None:
                stub: stub_type = self.server.stub  # type: ignore[attr-defined,valid-type]
                split = urlsplit(self.path)
                path = unquote(split.path)
                headers = {key.lower(): value for key, value in self.headers.items()}
                with stub._lock:
                    stub.requests.append(
                        RecordedRequest("GET", path, parse_qs(split.query), headers, None, b"")
                    )
                try:
                    key = self.headers.get("Sec-WebSocket-Key")
                    if not key:
                        raise StubError(400, "invalid", "missing websocket key")
                    accept = base64.b64encode(
                        hashlib.sha1(
                            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
                        ).digest()
                    ).decode()
                    response = (
                        "HTTP/1.1 101 Switching Protocols\r\n"
                        "Upgrade: websocket\r\n"
                        "Connection: Upgrade\r\n"
                        f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                    )
                    self.connection.sendall(response.encode("latin-1"))
                    stub._serve_exec_websocket(
                        self.connection, path, parse_qs(split.query), headers
                    )
                except StubError as exc:
                    self.send_response(exc.status)
                    self.end_headers()
                finally:
                    self.close_connection = True

            def _handle_http(self) -> None:
                stub: stub_type = self.server.stub  # type: ignore[attr-defined,valid-type]
                split = urlsplit(self.path)
                path = unquote(split.path)
                length = int(self.headers.get("Content-Length") or "0")
                body = self.rfile.read(length) if length else b""
                json_body: Any = None
                if body and "json" in self.headers.get("Content-Type", ""):
                    json_body = json.loads(body.decode("utf-8"))
                headers = {key.lower(): value for key, value in self.headers.items()}
                request = RecordedRequest(
                    self.command,
                    path,
                    parse_qs(split.query),
                    headers,
                    json_body,
                    body,
                )
                with stub._lock:
                    stub.requests.append(request)
                try:
                    status, payload, response_headers, raw = stub._dispatch(request)
                except StubError as exc:
                    status = exc.status
                    payload = {"code": exc.code, "message": exc.message}
                    response_headers = {"Content-Type": "application/json"}
                    raw = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                for key, value in response_headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                if raw:
                    self.wfile.write(raw)

        return Handler

    def _json_response(
        self, status: int, payload: Any, headers: dict[str, str] | None = None
    ) -> tuple[int, Any, dict[str, str], bytes]:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return status, payload, {"Content-Type": "application/json", **(headers or {})}, raw

    def _bytes_response(
        self, status: int, body: bytes, content_type: str = "application/octet-stream"
    ) -> tuple[int, None, dict[str, str], bytes]:
        return status, None, {"Content-Type": content_type}, body

    def _dispatch(self, request: RecordedRequest) -> tuple[int, Any, dict[str, str], bytes]:
        if request.path == "/healthz" and request.method == "GET":
            return self._json_response(200, {"ok": True})
        if request.path == "/v1/info" and request.method == "GET":
            return self._json_response(200, {"version": "test", "capabilities": {}})

        if request.path == "/v1/volumes":
            if request.method != "GET":
                raise StubError(405, "invalid", "method not allowed")
            with self._lock:
                return self._json_response(200, {"volumes": sorted(self.volumes)})
        if request.path.startswith("/v1/volumes/"):
            name = request.path.rsplit("/", 1)[-1]
            if request.method == "PUT":
                with self._lock:
                    self.volumes.add(name)
                return self._json_response(200, {"name": name})
            if request.method == "DELETE":
                with self._lock:
                    self.volumes.discard(name)
                return self._json_response(200, {"ok": True})
            raise StubError(405, "invalid", "method not allowed")

        if request.path == "/v1/sandboxes" and request.method == "POST":
            return self._create_sandbox(request.json or {})
        if request.path == "/v1/sandboxes" and request.method == "GET":
            tag_filters = request.query.get("tag", [])
            with self._lock:
                views = [dict(item["view"]) for item in self.sandboxes.values()]
            if tag_filters:
                wanted = dict(item.split("=", 1) for item in tag_filters if "=" in item)
                views = [
                    view
                    for view in views
                    if all(
                        str((view.get("tags") or {}).get(key)) == value
                        for key, value in wanted.items()
                    )
                ]
            return self._json_response(200, {"sandboxes": views})

        prefix = "/v1/sandboxes/"
        if request.path.startswith(prefix):
            rest = request.path[len(prefix) :]
            sandbox_id, _, suffix = rest.partition("/")
            return self._sandbox_route(sandbox_id, suffix, request)

        raise StubError(404, "not_found", f"unknown route {request.path}")

    def _create_sandbox(self, payload: dict[str, Any]) -> tuple[int, Any, dict[str, str], bytes]:
        with self._lock:
            sandbox_id = str(payload.get("name") or f"sb-{self._next_id}")
            self._next_id += 1
            tags = {str(k): str(v) for k, v in (payload.get("tags") or {}).items()}
            view = {
                "id": sandbox_id,
                "name": sandbox_id,
                "status": "running",
                "created_at": "2026-07-06T00:00:00Z",
                "last_active": "2026-07-06T00:00:00Z",
                "terminated_at": None,
                "error": None,
                "tags": tags,
                "returncode": None,
                "workdir": payload.get("workdir") or "/work",
            }
            secret_env: dict[str, str] = {}
            for item in payload.get("secrets") or []:
                if isinstance(item, dict):
                    values = item.get("values") if isinstance(item.get("values"), dict) else item
                    secret_env.update({str(k): str(v) for k, v in values.items()})
            self.sandboxes[sandbox_id] = {
                "view": view,
                "env": {str(k): str(v) for k, v in (payload.get("env") or {}).items()},
                "secret_env": secret_env,
                "files": {},
                "logs": "created\n",
                "volumes": payload.get("volumes") or {},
            }
        return self._json_response(201, view)

    def _sandbox_route(
        self, sandbox_id: str, suffix: str, request: RecordedRequest
    ) -> tuple[int, Any, dict[str, str], bytes]:
        with self._lock:
            sandbox = self.sandboxes.get(sandbox_id)
        if sandbox is None:
            raise StubError(404, "not_found", f"unknown sandbox {sandbox_id}")

        if suffix == "":
            if request.method == "GET":
                return self._json_response(200, dict(sandbox["view"]))
            if request.method == "DELETE":
                with self._lock:
                    self.sandboxes.pop(sandbox_id, None)
                return self._json_response(200, {"ok": True})
        if suffix in {"stop", "terminate", "pause", "resume"} and request.method == "POST":
            with self._lock:
                sandbox["view"]["status"] = (
                    "terminated" if suffix in {"stop", "terminate"} else suffix + "d"
                )
            return self._json_response(200, dict(sandbox["view"]))
        if suffix == "extend" and request.method == "POST":
            return self._json_response(200, {"deadline_unix": 1_800_000_000})
        if suffix == "metrics" and request.method == "GET":
            return self._json_response(200, {"vcpu_exits": 7})
        if suffix == "logs" and request.method == "GET":
            return self._bytes_response(
                200, sandbox["logs"].encode("utf-8"), "text/plain; charset=utf-8"
            )
        if suffix == "snapshots" and request.method == "POST":
            name = (request.json or {}).get("name") or f"snap-{sandbox_id}"
            return self._json_response(200, {"snapshot": name, "dir": f"/snapshots/{name}"})
        if suffix == "snapshots/fs" and request.method == "POST":
            name = (request.json or {}).get("name") or f"fs-{sandbox_id}"
            return self._json_response(200, {"image": name})
        if suffix == "exec" and request.method == "POST":
            stdout, stderr, code = self._execute(sandbox_id, request.json or {}, b"")
            return self._json_response(
                200,
                {
                    "exit": code,
                    "returncode": code,
                    "stdout_b64": base64.b64encode(stdout).decode("ascii"),
                    "stderr_b64": base64.b64encode(stderr).decode("ascii"),
                },
            )
        if suffix == "files":
            return self._files_route(sandbox, request)
        if suffix == "files/list" and request.method == "GET":
            root = (request.query.get("path") or [""])[0].rstrip("/")
            entries = []
            for path, body in sorted(sandbox["files"].items()):
                if not root or path.startswith(root + "/") or path == root:
                    entries.append({"path": path, "size": len(body), "type": "file"})
            return self._json_response(200, {"entries": entries})
        if suffix == "files/stat" and request.method == "GET":
            path = (request.query.get("path") or [""])[0]
            body = sandbox["files"].get(path)
            if body is None:
                raise StubError(404, "not_found", f"unknown file {path}")
            return self._json_response(200, {"path": path, "size": len(body), "type": "file"})
        raise StubError(404, "not_found", f"unknown sandbox route {suffix}")

    def _files_route(
        self, sandbox: dict[str, Any], request: RecordedRequest
    ) -> tuple[int, Any, dict[str, str], bytes]:
        path = (request.query.get("path") or [""])[0]
        if request.method == "PUT":
            sandbox["files"][path] = request.body
            return self._json_response(200, {"written": len(request.body)})
        if request.method == "GET":
            body = sandbox["files"].get(path)
            if body is None:
                raise StubError(404, "not_found", f"unknown file {path}")
            return self._bytes_response(200, body)
        if request.method == "DELETE":
            sandbox["files"].pop(path, None)
            return self._json_response(200, {"ok": True})
        raise StubError(405, "invalid", "method not allowed")

    def _serve_exec_websocket(
        self, sock: Any, path: str, query: dict[str, list[str]], headers: dict[str, str]
    ) -> None:
        sandbox_id = path.removeprefix("/v1/sandboxes/").removesuffix("/exec")
        opcode, payload = read_frame_sync(sock)
        if opcode != 0x1:
            return
        request = json.loads(payload.decode("utf-8"))
        stdin = bytearray()
        while True:
            opcode, payload = read_frame_sync(sock)
            if opcode == 0x8:
                return
            if opcode != 0x1:
                continue
            frame = json.loads(payload.decode("utf-8"))
            if "stdin_b64" in frame:
                stdin.extend(base64.b64decode(frame["stdin_b64"]))
            if frame.get("eof") is True:
                break
        with self._lock:
            self.requests.append(RecordedRequest("WS", path, query, headers, request, bytes(stdin)))
        stdout, stderr, code = self._execute(sandbox_id, request, bytes(stdin))
        if stdout:
            self._send_ws_json(
                sock, {"stream": "stdout", "b64": base64.b64encode(stdout).decode("ascii")}
            )
        if stderr:
            self._send_ws_json(
                sock, {"stream": "stderr", "b64": base64.b64encode(stderr).decode("ascii")}
            )
        self._send_ws_json(sock, {"exit": code, "signal": None})
        with contextlib.suppress(Exception):
            sock.sendall(encode_frame(0x8, b""))

    def _send_ws_json(self, sock: Any, value: dict[str, Any]) -> None:
        sock.sendall(encode_frame(0x1, json.dumps(value, separators=(",", ":")).encode("utf-8")))

    def _execute(
        self, sandbox_id: str, request: dict[str, Any], stdin: bytes
    ) -> tuple[bytes, bytes, int]:
        with self._lock:
            sandbox = self.sandboxes[sandbox_id]
            files = sandbox["files"]
            env = dict(sandbox["env"])
            env.update(sandbox["secret_env"])
        env.update({str(k): str(v) for k, v in (request.get("env") or {}).items()})
        cmd = [str(part) for part in request.get("cmd") or []]
        if cmd[:2] == ["python3", "--version"]:
            return b"Python 3.14.0\n", b"", 0
        if len(cmd) >= 3 and cmd[:2] == ["python3", "-u"]:
            return self._run_remote_function(stdin), b"", 0
        if cmd == ["env"]:
            return json.dumps(env, sort_keys=True).encode("utf-8"), b"", 0
        if cmd and cmd[0] == "cat" and len(cmd) == 2:
            return files.get(cmd[1], b""), b"", 0
        if cmd and cmd[0] == "printf" and len(cmd) >= 2:
            return cmd[1].encode("utf-8"), b"", 0
        return b"", b"", 0

    def _run_remote_function(self, stdin: bytes) -> bytes:
        payload = json.loads(stdin.decode("utf-8"))
        namespace = {"__builtins__": __builtins__, "__name__": "__vmon_stub_remote_function__"}
        captured_stdout = io.StringIO()
        real_stdout = sys.stdout
        try:
            exec(payload["source"], namespace)
            fn = namespace[payload["name"]]
            sys.stdout = captured_stdout
            result = fn(*payload.get("args", ()), **payload.get("kwargs", {}))
            if inspect.isawaitable(result):
                result = asyncio.run(result)
            sys.stdout = real_stdout
            json.dumps(result)
            response = {"ok": True, "result": result, "stdout": captured_stdout.getvalue()}
        except BaseException as exc:
            sys.stdout = real_stdout
            response = {
                "ok": False,
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
                "stdout": captured_stdout.getvalue(),
            }
        return json.dumps(response, separators=(",", ":")).encode("utf-8")


def configure_context(
    monkeypatch: Any,
    home: Path,
    server: V1StubServer,
    *,
    name: str = "prod",
    token: str = "test-token",
) -> None:
    from vmon.context import Context, ContextStore, contexts_path

    monkeypatch.setenv("VMON_HOME", str(home))
    monkeypatch.setenv("VMON_CONTEXT", name)
    monkeypatch.delenv("VMON_API_TOKEN", raising=False)
    store = ContextStore(contexts_path(home))
    store.load()
    store.put(Context(name, [server.url]))
    store.use(name)
    store.save_token(name, token)
