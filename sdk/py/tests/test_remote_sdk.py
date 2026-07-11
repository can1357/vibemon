from __future__ import annotations

import pytest
from v1_stub import V1StubServer, configure_context

from vmon._transport import DaemonError
from vmon.sandbox import RemoteFunctionError, Sandbox, function


def test_remote_sandbox_round_trip_over_v1_stub(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        sandbox = Sandbox.create(
            image="python:3.14-slim",
            context="prod",
            name="sdk-1",
            cpus=2,
            memory=1024,
            env={"HELLO": "world"},
            tags={"suite": "remote"},
        )

        create_payload = server.last("POST", "/v1/sandboxes").json
        assert create_payload["cpus"] == 2
        assert create_payload["memory"] == 1024
        assert create_payload["env"] == {"HELLO": "world"}
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
        assert sandbox.extend(30) == {"deadline_unix": 1_800_000_000}
        assert sandbox.snapshot("snap-1") == "snap-1"
        assert sandbox.snapshot_filesystem("fs-1") == "fs-1"
        assert sandbox.logs() == "created\n"

        sandbox.remove()
        assert "sdk-1" not in server.sandboxes


def test_v1_error_envelope_maps_to_existing_daemon_error(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with pytest.raises(DaemonError) as excinfo:
            Sandbox.from_id("missing")

        assert excinfo.value.code == "not_found"
        assert excinfo.value.status == 404
        assert "unknown sandbox missing" in str(excinfo.value)


def test_function_decorator_runs_through_v1_exec_websocket(monkeypatch, mvm_home, capsys) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim", context="prod")
        def add(left: int, right: int) -> dict[str, int]:
            print("remote ran")
            return {"sum": left + right}

        assert add.remote(2, 5) == {"sum": 7}
        assert capsys.readouterr().out == "remote ran\n"
        add.terminate()

        create = server.last("POST", "/v1/sandboxes")
        assert create.json["image"] == "python:3.14-slim"
        assert server.last("PUT", "/v1/sandboxes/sb-1/files").query["path"] == [
            "/tmp/vmon-function-runner.py"
        ]
        runner_exec = [
            row
            for row in server.recorded("WS", "/v1/sandboxes/sb-1/exec")
            if row.json["cmd"][:2] == ["python3", "-u"]
        ][-1]
        assert runner_exec.body
        assert server.last("POST", "/v1/sandboxes/sb-1/terminate").json is None


def test_function_map_uses_ephemeral_workers_and_terminates_them(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim", context="prod")
        def triple(value: int) -> dict[str, int]:
            return {"value": value, "triple": value * 3}

        assert triple.map([1, 2, 3, 4, 5], concurrency=3) == [
            {"value": 1, "triple": 3},
            {"value": 2, "triple": 6},
            {"value": 3, "triple": 9},
            {"value": 4, "triple": 12},
            {"value": 5, "triple": 15},
        ]

        assert len(server.recorded("POST", "/v1/sandboxes")) == 3
        assert len(server.recorded("POST", "/v1/sandboxes/sb-1/terminate")) == 1
        assert len(server.recorded("POST", "/v1/sandboxes/sb-2/terminate")) == 1
        assert len(server.recorded("POST", "/v1/sandboxes/sb-3/terminate")) == 1


def test_function_decorator_surfaces_remote_runner_failure(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        @function(image="python:3.14-slim", context="prod")
        def explode() -> None:
            raise ValueError("boom")

        with pytest.raises(RemoteFunctionError) as excinfo:
            explode.remote()

        assert excinfo.value.remote_type == "ValueError"
        assert "boom" in str(excinfo.value)
        explode.terminate()
