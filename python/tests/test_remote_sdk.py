from __future__ import annotations

import base64
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, cast

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI, HTTPException, Request, Response
from starlette.testclient import TestClient

from vmon.client import DaemonError
from vmon.context import Context, ContextStore
from vmon.remote import VmonClient, connect


class GatewayTestTransport:
    def __init__(
        self, app: FastAPI, *, endpoints: list[str] | None = None, token: str | None = None
    ):
        self.app = app
        self.endpoints = list(endpoints or ["http://gateway"])
        self.token = token
        self.client = TestClient(app)

    def call(self, method: str, **params: Any) -> dict[str, Any]:
        if method == "ps":
            response = self.client.get("/v1/sandboxes")
            response.raise_for_status()
            rows = response.json().get("sandboxes") or []
            return {"vms": rows}
        if method == "run":
            response = self.client.post("/v1/run?detach=1", json=params)
            response.raise_for_status()
            return response.json()
        if method == "inspect":
            response = self.client.get(f"/v1/sandboxes/{params['name']}")
            response.raise_for_status()
            return response.json()
        if method == "cp_write":
            raw = base64.b64decode(str(params["data"]))
            response = self.client.put(
                f"/v1/sandboxes/{params['name']}/files",
                params={"path": params["path"]},
                content=raw,
            )
            response.raise_for_status()
            return {"written": len(raw)}
        if method == "cp_read":
            response = self.client.get(
                f"/v1/sandboxes/{params['name']}/files", params={"path": params["path"]}
            )
            response.raise_for_status()
            return {"data": base64.b64encode(response.content).decode("ascii")}
        if method == "fs_list":
            response = self.client.get(
                f"/v1/sandboxes/{params['name']}/fs/list",
                params={"path": params.get("path") or "/"},
            )
            response.raise_for_status()
            return response.json()
        if method == "fs_stat":
            response = self.client.get(
                f"/v1/sandboxes/{params['name']}/fs/stat",
                params={"path": params.get("path") or "/"},
            )
            response.raise_for_status()
            return response.json()
        if method == "metrics":
            response = self.client.get(f"/v1/sandboxes/{params['name']}/metrics")
            response.raise_for_status()
            return response.json()
        if method == "extend":
            response = self.client.post(
                f"/v1/sandboxes/{params['name']}/extend", json={"secs": params["secs"]}
            )
            response.raise_for_status()
            return response.json()
        if method == "snapshot":
            response = self.client.post(
                f"/v1/sandboxes/{params['name']}/snapshot_template",
                json={"snapshot": params.get("snapshot"), "stop": params.get("stop", False)},
            )
            response.raise_for_status()
            return response.json()
        if method == "stop":
            response = self.client.post(f"/v1/sandboxes/{params['name']}/stop")
            response.raise_for_status()
            return response.json()
        if method == "rm":
            response = self.client.delete(f"/v1/sandboxes/{params['name']}/remove")
            response.raise_for_status()
            return response.json()
        raise DaemonError(f"unsupported fake method {method}", code="unsupported")

    def stream(self, method: str, on_event, stdin=None, **params: Any) -> dict[str, Any]:
        if method == "exec":
            cmd = list(params.get("cmd") or [])
            input_data = b""
            if stdin is not None and cmd[:2] == ["python3", "-u"]:
                input_data = stdin.read()
            response = self.client.post(
                f"/_fake/sandboxes/{params['name']}/exec",
                json={
                    "cmd": cmd,
                    "stdin_b64": base64.b64encode(input_data).decode("ascii"),
                    "timeout": params.get("timeout"),
                },
            )
            response.raise_for_status()
            body = response.json()
            for stream_name in ("stdout", "stderr"):
                data = body.get(stream_name)
                if data:
                    on_event(stream_name, base64.b64decode(data))
            return {"returncode": body.get("returncode", 0)}
        if method == "logs":
            response = self.client.get(
                f"/v1/sandboxes/{params['name']}/logs",
                params={"follow": bool(params.get("follow"))},
            )
            response.raise_for_status()
            on_event("console", response.content)
            return {}
        raise DaemonError(f"unsupported fake stream {method}", code="unsupported")

    def interactive(self, method: str, on_event, tty: bool = True, on_ready=None, **params: Any):
        if on_ready is not None:
            on_ready()
        return self.stream(method, on_event, **params)

    def ensure_running(self) -> dict[str, Any]:
        response = self.client.get("/healthz")
        response.raise_for_status()
        return {}

    def stop_daemon(self) -> dict[str, Any]:
        raise DaemonError("unsupported", code="unsupported")


@pytest.fixture
def fake_gateway() -> FastAPI:
    app = FastAPI()
    app.state.sandboxes = {}
    app.state.run_payloads = []
    app.state.created_names = []

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/v1/run")
    async def run_gateway(request: Request, detach: bool = False) -> dict[str, Any]:
        payload = await request.json()
        app.state.run_payloads.append(dict(payload))
        sandbox_id = str(payload.get("name") or f"sb-{len(app.state.sandboxes) + 1}")
        app.state.created_names.append(sandbox_id)
        view = {
            "id": sandbox_id,
            "name": sandbox_id,
            "status": "running",
            "image": payload.get("image"),
            "tags": dict(payload.get("tags") or {}),
            "returncode": None,
            "detached": detach,
        }
        app.state.sandboxes[sandbox_id] = {"view": view, "files": {}, "logs": b"created\n"}
        return dict(view)

    @app.get("/v1/sandboxes")
    def list_sandboxes() -> dict[str, list[dict[str, Any]]]:
        return {"sandboxes": [dict(box["view"]) for box in app.state.sandboxes.values()]}

    @app.get("/v1/sandboxes/{sandbox_id}")
    def inspect_sandbox(sandbox_id: str) -> dict[str, Any]:
        return dict(_box(app, sandbox_id)["view"])

    @app.put("/v1/sandboxes/{sandbox_id}/files")
    async def put_file(sandbox_id: str, path: str, request: Request) -> dict[str, bool]:
        _box(app, sandbox_id)["files"][path] = await request.body()
        return {"ok": True}

    @app.get("/v1/sandboxes/{sandbox_id}/files")
    def get_file(sandbox_id: str, path: str) -> Response:
        files = _box(app, sandbox_id)["files"]
        if path not in files:
            raise HTTPException(404, "missing file")
        return Response(files[path], media_type="application/octet-stream")

    @app.get("/v1/sandboxes/{sandbox_id}/fs/list")
    def list_files(sandbox_id: str, path: str) -> dict[str, list[dict[str, Any]]]:
        prefix = path.rstrip("/")
        entries = [
            {"path": name, "size": len(data), "type": "file"}
            for name, data in sorted(_box(app, sandbox_id)["files"].items())
            if name.startswith(prefix)
        ]
        return {"entries": entries}

    @app.get("/v1/sandboxes/{sandbox_id}/fs/stat")
    def stat_file(sandbox_id: str, path: str) -> dict[str, Any]:
        files = _box(app, sandbox_id)["files"]
        if path not in files:
            raise HTTPException(404, "missing file")
        return {"path": path, "size": len(files[path]), "type": "file"}

    @app.get("/v1/sandboxes/{sandbox_id}/logs")
    def logs(sandbox_id: str, follow: bool = False) -> Response:
        del follow
        return Response(_box(app, sandbox_id)["logs"], media_type="text/plain")

    @app.get("/v1/sandboxes/{sandbox_id}/metrics")
    def metrics(sandbox_id: str) -> dict[str, int]:
        _box(app, sandbox_id)
        return {"vcpu_exits": 7}

    @app.post("/v1/sandboxes/{sandbox_id}/extend")
    async def extend(sandbox_id: str, request: Request) -> dict[str, Any]:
        _box(app, sandbox_id)
        payload = await request.json()
        return {"deadline_unix": 1000 + int(payload["secs"])}

    @app.post("/v1/sandboxes/{sandbox_id}/snapshot_template")
    async def snapshot(sandbox_id: str, request: Request) -> dict[str, str]:
        _box(app, sandbox_id)
        payload = await request.json()
        return {"snapshot": str(payload.get("snapshot") or "snap")}

    @app.post("/v1/sandboxes/{sandbox_id}/stop")
    def stop(sandbox_id: str) -> dict[str, Any]:
        box = _box(app, sandbox_id)
        box["view"]["status"] = "stopped"
        box["view"]["returncode"] = 0
        return dict(box["view"])

    @app.delete("/v1/sandboxes/{sandbox_id}/remove")
    def remove(sandbox_id: str) -> dict[str, bool]:
        _box(app, sandbox_id)
        del app.state.sandboxes[sandbox_id]
        return {"ok": True}

    @app.post("/_fake/sandboxes/{sandbox_id}/exec")
    async def fake_exec(sandbox_id: str, request: Request) -> dict[str, Any]:
        payload = await request.json()
        cmd = [str(part) for part in payload.get("cmd") or []]
        stdin_data = base64.b64decode(str(payload.get("stdin_b64") or ""))
        stdout, stderr, returncode = _exec_fake(
            app, sandbox_id, cmd, stdin_data, payload.get("timeout")
        )
        return {
            "stdout": base64.b64encode(stdout).decode("ascii"),
            "stderr": base64.b64encode(stderr).decode("ascii"),
            "returncode": returncode,
        }

    return app


def _box(app: FastAPI, sandbox_id: str) -> dict[str, Any]:
    try:
        return app.state.sandboxes[sandbox_id]
    except KeyError as exc:
        raise HTTPException(404, "unknown sandbox") from exc


def _exec_fake(
    app: FastAPI, sandbox_id: str, cmd: list[str], stdin_data: bytes, timeout: float | None
) -> tuple[bytes, bytes, int]:
    files = _box(app, sandbox_id)["files"]
    if cmd[:2] == ["python3", "--version"]:
        return b"Python 3.14.0\n", b"", 0
    if len(cmd) >= 3 and cmd[:2] == ["python3", "-u"]:
        script = files.get(cmd[2])
        if script is None:
            return b"", f"missing script {cmd[2]}".encode(), 2
        with tempfile.NamedTemporaryFile("wb", suffix=".py", delete=False) as handle:
            handle.write(script)
            script_path = handle.name
        try:
            completed = subprocess.run(
                [sys.executable, "-u", script_path],
                input=stdin_data,
                capture_output=True,
                timeout=timeout or 10,
                check=False,
            )
        finally:
            Path(script_path).unlink(missing_ok=True)
        return completed.stdout, completed.stderr, int(completed.returncode)
    if cmd[:1] == ["cat"] and len(cmd) == 2:
        data = files.get(cmd[1])
        if data is None:
            return b"", f"cat: {cmd[1]}: No such file".encode(), 1
        return data, b"", 0
    if cmd[:1] == ["mkdir"]:
        return b"", b"", 0
    if cmd[:1] == ["rm"]:
        for target in cmd[1:]:
            if not target.startswith("-"):
                files.pop(target, None)
        return b"", b"", 0
    return b"", f"unsupported command: {cmd!r}".encode(), 127


def test_remote_sandbox_round_trip(fake_gateway: FastAPI) -> None:
    client = VmonClient(GatewayTestTransport(fake_gateway))

    sandbox = client.sandboxes.create(
        image="python:3.14-slim",
        name="sdk-1",
        cpus=2,
        mem=1024,
        env={"HELLO": "world"},
        tags={"suite": "remote"},
    )

    create_payload = fake_gateway.state.run_payloads[-1]
    assert create_payload["idempotency_key"]
    assert create_payload["mem"] == 1024
    assert create_payload["cmd"] == ["sh", "-c", "sleep 2147483647"]
    assert sandbox.name == "sdk-1"

    sandbox.filesystem.write_text("/tmp/hello.txt", "hello")
    assert sandbox.filesystem.read_text("/tmp/hello.txt") == "hello"
    assert sandbox.filesystem.read_bytes("/tmp/hello.txt") == b"hello"
    assert sandbox.filesystem.stat("/tmp/hello.txt")["size"] == 5
    assert sandbox.filesystem.list_files("/tmp") == [
        {"path": "/tmp/hello.txt", "size": 5, "type": "file"}
    ]

    proc = sandbox.exec("cat", "/tmp/hello.txt")
    assert proc.wait(timeout=5) == 0
    assert proc.stdout.read() == b"hello"
    assert sandbox.metrics() == {"vcpu_exits": 7}
    assert sandbox.extend(30) == {"deadline_unix": 1030}
    assert sandbox.snapshot("snap-1") == "snap-1"
    assert sandbox.logs() == "created\n"

    sandbox.terminate()
    assert "sdk-1" not in fake_gateway.state.sandboxes


def test_remote_sandbox_rejects_local_only_inputs(fake_gateway: FastAPI) -> None:
    client = VmonClient(GatewayTestTransport(fake_gateway))

    with pytest.raises(DaemonError, match="dockerfile") as dockerfile_error:
        client.sandboxes.create(image="alpine", dockerfile="Dockerfile")
    assert dockerfile_error.value.code == "unsupported"

    with pytest.raises(DaemonError, match="fs_dir") as fs_dir_error:
        client.sandboxes.create(image="alpine", fs_dir="/host")
    assert fs_dir_error.value.code == "unsupported"

    with pytest.raises(DaemonError, match="host path") as volume_error:
        client.sandboxes.create(
            image="alpine", volumes={"/data": {"name": "data", "host_path": "/tmp/data"}}
        )
    assert volume_error.value.code == "unsupported"


def test_context_resolution_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import vmon.remote as remote_mod

    class RecordingMeshTransport:
        instances: list[RecordingMeshTransport] = []

        def __init__(self, endpoints: list[str], token: str | None = None, on_roster=None):
            self.endpoints = list(endpoints)
            self.token = token
            self.on_roster = on_roster
            self.__class__.instances.append(self)

    class RecordingLocalTransport:
        pass

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    monkeypatch.delenv("VMON_API_TOKEN", raising=False)
    store = ContextStore()
    store.put(Context("prod", ["http://prod-a", "http://prod-b"]))
    store.use("prod")
    store.save_token("prod", "persisted-token")
    monkeypatch.setattr(remote_mod, "MeshTransport", RecordingMeshTransport)
    monkeypatch.setattr(remote_mod, "LocalTransport", RecordingLocalTransport)

    explicit = connect("prod", endpoints=["http://explicit"], token="arg-token")
    explicit_transport = cast(RecordingMeshTransport, explicit.transport)
    assert explicit_transport.endpoints == ["http://explicit"]
    assert explicit_transport.token == "arg-token"

    monkeypatch.setenv("VMON_API_TOKEN", "env-token")
    env_client = connect("prod")
    env_transport = cast(RecordingMeshTransport, env_client.transport)
    assert env_transport.endpoints == ["http://prod-a", "http://prod-b"]
    assert env_transport.token == "env-token"

    monkeypatch.delenv("VMON_API_TOKEN", raising=False)
    persisted_client = connect("prod")
    persisted_transport = cast(RecordingMeshTransport, persisted_client.transport)
    assert persisted_transport.token == "persisted-token"

    monkeypatch.setenv("VMON_CONTEXT", "prod")
    selected_client = connect()
    selected_transport = cast(RecordingMeshTransport, selected_client.transport)
    assert selected_transport.endpoints == ["http://prod-a", "http://prod-b"]

    monkeypatch.setenv("VMON_CONTEXT", "local")
    local_client = connect()
    assert isinstance(local_client.transport, RecordingLocalTransport)


def test_sandbox_create_routes_to_named_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_gateway: FastAPI
) -> None:
    import vmon.remote as remote_mod
    from vmon import Sandbox

    transports: dict[str, GatewayTestTransport] = {
        "http://prod": GatewayTestTransport(
            fake_gateway, endpoints=["http://prod"], token="tok"
        )
    }

    class RegisteredMeshTransport(GatewayTestTransport):
        def __init__(self, endpoints: list[str], token: str | None = None, on_roster=None):
            del on_roster
            selected = transports[endpoints[0]]
            self.__dict__.update(selected.__dict__)
            self.endpoints = list(endpoints)
            self.token = token

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setenv("VMON_API_TOKEN", "tok")
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    ContextStore().put(Context("prod", ["http://prod"]))
    monkeypatch.setattr(remote_mod, "MeshTransport", RegisteredMeshTransport)

    sandbox = Sandbox.create(image="python:3.14-slim", context="prod", name="routed")

    assert sandbox.sandbox_id == "routed"
    assert fake_gateway.state.run_payloads[-1]["name"] == "routed"


def test_sandbox_create_rejects_missing_explicit_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from vmon import Sandbox

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.delenv("VMON_CONTEXT", raising=False)

    # A typo'd context must never silently fall back to a local create.
    with pytest.raises(DaemonError) as excinfo:
        Sandbox.create(image="python:3.14-slim", context="ghost")
    assert excinfo.value.code == "invalid"


def test_function_decorator_runs_over_fake_gateway(fake_gateway: FastAPI) -> None:
    client = VmonClient(GatewayTestTransport(fake_gateway))

    @client.function(image="python:3.14-slim")
    def add(left, right):
        return left + right

    assert add.remote(2, 5) == 7
    add.terminate()


def test_function_decorator_map_runs_over_fake_gateway(fake_gateway: FastAPI) -> None:
    client = VmonClient(GatewayTestTransport(fake_gateway))

    @client.function(image="python:3.14-slim")
    def triple(value: int) -> dict[str, int]:
        return {"value": value, "triple": value * 3}

    assert triple.map([1, 2, 3, 4, 5], concurrency=3) == [
        {"value": 1, "triple": 3},
        {"value": 2, "triple": 6},
        {"value": 3, "triple": 9},
        {"value": 4, "triple": 12},
        {"value": 5, "triple": 15},
    ]
    assert len(fake_gateway.state.run_payloads) == 3
    assert len(fake_gateway.state.created_names) == 3
    assert len(set(fake_gateway.state.created_names)) == 3
    assert fake_gateway.state.sandboxes == {}
