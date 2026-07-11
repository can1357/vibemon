from __future__ import annotations

import asyncio
import inspect
import json

import pytest
from v1_stub import V1StubServer, configure_context

import vmon
from vmon import (
    DaemonError,
    Sandbox,
    daemon_metrics,
    events,
    health,
    list_snapshots,
    openapi_schema,
    pool_inventory,
    prewarm,
    server_info,
    shell,
    shutdown_all_pools,
)


def test_global_api_operations_stream_events_and_send_bearer_auth(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        server.required_token = "test-token"
        configure_context(monkeypatch, mvm_home, server)

        assert health() == {"ok": True}
        assert server_info()["version"] == "test"
        assert "vmon_test_total 1" in daemon_metrics()
        assert openapi_schema()["openapi"] == "3.1.0"
        assert list_snapshots() == []
        with events() as stream:
            assert list(stream) == [{"type": "ready", "sequence": 1}]

        checked_paths = {
            "/healthz",
            "/metrics",
            "/v1/events",
            "/v1/info",
            "/v1/openapi.json",
            "/v1/snapshots",
        }
        checked = [request for request in server.requests if request.path in checked_paths]
        assert {request.path for request in checked} == checked_paths
        assert all(request.headers["authorization"] == "Bearer test-token" for request in checked)


def test_pool_helpers_escape_references_and_own_their_lifecycle(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        pool = prewarm("python/base", count=2, memory=256)
        assert pool.stats() == {"size": 2, "ready": 2}
        assert pool_inventory() == {"python/base": 2}
        pool.shutdown()
        pool.shutdown()
        assert pool_inventory() == {}
        assert len(server.recorded("DELETE", "/v1/pools/python%2Fbase")) == 1

        prewarm("one", count=1)
        prewarm("two", count=1)
        shutdown_all_pools()
        assert pool_inventory() == {}


def test_sandbox_lifecycle_network_snapshots_and_forks(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        sandbox = Sandbox.create(
            image="python:3.14-slim",
            context="prod",
            name="box/name",
            tags={"suite": "coverage"},
        )
        create = server.last("POST", "/v1/sandboxes")
        assert create.body == json.dumps(
            create.json, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")

        assert sandbox.network_policy() == {
            "block_network": False,
            "cidr_allow": [],
            "domain_allow": [],
        }
        assert sandbox.set_network_policy(
            block_network=True,
            cidr_allow=["10.0.0.0/8"],
            domain_allow=["example.com"],
        ) == {
            "block_network": True,
            "cidr_allow": ["10.0.0.0/8"],
            "domain_allow": ["example.com"],
        }
        assert sandbox.migrate("node-b")["node"] == "node-b"
        assert sandbox.pause()["status"] == "paused"
        assert sandbox.resume()["status"] == "running"
        assert sandbox.extend(15)["deadline_unix"] == 1_800_000_000
        assert sandbox.metrics() == {"vcpu_exits": 7}

        assert sandbox.snapshot("base/snapshot") == "base/snapshot"
        assert list_snapshots() == ["base/snapshot"]
        assert sandbox.snapshot_filesystem("filesystem-image") == "filesystem-image"

        restored = Sandbox.from_snapshot("base/snapshot", context="prod", name="restored")
        assert restored.name == "restored"
        clones = Sandbox.fork("base/snapshot", count=2, context="prod")
        assert len(clones) == 2
        assert len({clone.name for clone in clones}) == 2

        assert sandbox.refresh()["name"] == "box/name"

        listed = Sandbox.list(tag={"suite": "coverage"}, context="prod")
        assert [item.name for item in listed] == ["box/name"]
        assert server.last("GET", "/v1/sandboxes/box%2Fname").headers["authorization"] == (
            "Bearer test-token"
        )

        for item in listed:
            item.close()
        for clone in clones:
            clone.remove()
        restored.terminate()
        sandbox.stop()
        assert sandbox.view["status"] == "stopped"
        sandbox.remove()


def test_exec_attach_logs_tunnels_and_proxy_transports(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)
        sandbox = Sandbox.create(context="prod", name="streams")
        server.sandboxes["streams"]["tunnels"] = {"8080": {"host": "127.0.0.1", "port": 48080}}

        captured = sandbox.exec_capture("printf", "captured")
        assert captured.returncode == 0
        assert captured.stdout == b"captured"
        assert captured.stderr == b""

        process = sandbox.exec("printf", "streamed")
        assert process.wait(timeout=5) == 0
        assert process.stdout.read() == b"streamed"

        sandbox.filesystem.write_bytes("/tmp/remove-me", b"gone")
        sandbox.filesystem.remove("/tmp/remove-me")
        with pytest.raises(DaemonError) as excinfo:
            sandbox.filesystem.read_bytes("/tmp/remove-me")
        assert excinfo.value.code == "not_found"

        with sandbox.attach_console() as console:
            assert console.read() == b"created\n"
        with sandbox.logs(follow=True) as logs:
            assert list(logs) == ["created\n"]
        assert sandbox.logs() == "created\n"

        assert sandbox.tunnels() == {8080: ("127.0.0.1", 48080)}
        assert sandbox.create_connect_token() == "token-streams"
        sandbox.connect_token = None

        methods = ["GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH"]
        for method in methods:
            response = sandbox.proxy_http(
                method,
                8080,
                "api/a b",
                params={"q": "x y"},
                headers={"Authorization": "must-not-replace-daemon-auth"},
                content=b"payload" if method in {"PUT", "POST", "PATCH"} else None,
            )
            assert response.status_code == 200
            if method != "HEAD":
                assert response.json()["method"] == method
                assert response.json()["path"] == "api/a b"

        proxy_request = server.last("PATCH", "/v1/sandboxes/streams/ports/8080/api/a%20b")
        assert proxy_request.headers["authorization"] == "Bearer test-token"
        assert proxy_request.query == {
            "q": ["x y"],
            "connect_token": ["token-streams"],
        }

        traversal = sandbox.proxy_http("GET", 8080, "../admin")
        assert traversal.json()["path"] == "../admin"
        server.last("GET", "/v1/sandboxes/streams/ports/8080/%2E%2E/admin")

        with sandbox.proxy_websocket(8080, "echo/path", params={"q": "hello world"}) as ws:
            ws.send_bytes(b"echo")
            assert ws.recv_message() == b"echo"
        websocket_request = server.last("GET", "/v1/sandboxes/streams/ports/8080/ws/echo/path")
        assert websocket_request.query == {
            "q": ["hello world"],
            "connect_token": ["token-streams"],
        }
        assert websocket_request.headers["authorization"] == "Bearer test-token"

        sandbox.remove()


def test_shell_and_instance_aio_preserve_streaming_semantics(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        process = shell("printf", "shell-output", context="prod", timeout=5)
        assert process.wait(timeout=5) == 0
        assert process.stdout.read() == b"shell-output"
        assert process.sandbox_name == "shell-stub"

        sandbox = Sandbox.create(context="prod", name="async-box")

        async def exercise() -> None:
            result = await sandbox.aio.exec_capture("printf", "async-output")
            assert result.stdout == b"async-output"
            assert (await sandbox.aio.network_policy())["block_network"] is False
            assert (await sandbox.aio.pause())["status"] == "paused"
            assert (await sandbox.aio.resume())["status"] == "running"
            await sandbox.aio.remove()

        asyncio.run(exercise())


def test_websocket_upgrade_and_stream_errors_remain_structured(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        server._create_sandbox({"name": "protected"})
        server.required_token = "correct-token"
        configure_context(monkeypatch, mvm_home, server, token="wrong-token")
        sandbox = Sandbox("protected")

        with pytest.raises(DaemonError) as excinfo:
            sandbox.attach_console()

        assert excinfo.value.status == 401
        assert excinfo.value.code == "unauthorized"
        assert "invalid bearer token" in str(excinfo.value)
        sandbox.close()

    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)
        missing = Sandbox("missing")
        stream = missing.attach_console()
        with pytest.raises(DaemonError) as excinfo:
            stream.read()
        assert excinfo.value.code == "not_found"
        stream.close()
        missing.close()


def test_all_exported_callables_have_useful_docs() -> None:
    undocumented = [
        name
        for name in vmon.__all__
        if callable(symbol := getattr(vmon, name)) and not inspect.getdoc(symbol)
    ]
    assert undocumented == []
