from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient

import vmon.server as server_mod
import vmon.server.app as server_app
import vmon.server.templates as server_templates
from vmon.mesh import Mesh, NodeCaps

AUTH_T = {"Authorization": "Bearer t"}
AUTH_FULL = {"Authorization": "Bearer full"}


class FakeEngine:
    def committed(self) -> tuple[int, int]:
        return (0, 0)

    def owned_ids(self) -> list[str]:
        return []

    def templates_present(self) -> list[str]:
        return []


class FakeTransport:
    pass


def _prepare_server_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.delenv("VMON_WARM_IMAGES", raising=False)
    monkeypatch.delenv("VMON_REPLICATE_SEC", raising=False)


def _isolated_app(monkeypatch: pytest.MonkeyPatch, tmp_path, *, token: str):
    _prepare_server_env(monkeypatch, tmp_path)
    monkeypatch.delenv("VMON_CLIENT_TOKEN", raising=False)
    return server_mod.create_app(token=token)


def test_mesh_note_event_and_status_stats(monkeypatch, tmp_path):
    import vmon.mesh as mesh_mod

    monkeypatch.setattr(mesh_mod, "probe_cpu_baseline", lambda: "arch:test")
    monkeypatch.setattr(mesh_mod, "probe_compat", lambda: ("test", "test"))
    mesh = Mesh(
        FakeEngine(),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        caps=NodeCaps(2, 1024),
        backend="test",
        arch="test",
        state_path=tmp_path / "mesh.json",
    )

    mesh.note_event("restore")
    mesh.note_event("restore")
    mesh.note_event("fence")

    assert mesh.status()["self"]["stats"] == {"replication": 0, "restore": 2, "fence": 1}


def test_mesh_status_endpoint_replicas_held(monkeypatch, tmp_path):
    app = _isolated_app(monkeypatch, tmp_path, token="t")

    with TestClient(app) as client:
        app.state.mesh.enabled = True
        app.state.replica_store.put(
            "sb1",
            digest="d1",
            source_node="n1",
            snapshot_dir=str(tmp_path / "snapshot"),
            params={"name": "sb1"},
        )

        response = client.get("/v1/mesh/status", headers=AUTH_T)

    assert response.status_code == 200
    replicas_held = response.json().get("replicas_held")
    assert isinstance(replicas_held, int)
    assert replicas_held == 1


def test_api_token_rotation(monkeypatch, tmp_path):
    app = _isolated_app(monkeypatch, tmp_path, token="old,new")

    with TestClient(app) as client:
        assert (
            client.get("/v1/sandboxes", headers={"Authorization": "Bearer old"}).status_code == 200
        )
        assert (
            client.get("/v1/sandboxes", headers={"Authorization": "Bearer new"}).status_code == 200
        )
        assert (
            client.get("/v1/sandboxes", headers={"Authorization": "Bearer bogus"}).status_code
            == 401
        )


def test_client_token_list_and_admin_gate(monkeypatch, tmp_path):
    _prepare_server_env(monkeypatch, tmp_path)
    monkeypatch.setenv("VMON_CLIENT_TOKEN", "c1,c2")
    app = server_mod.create_app(token="full")

    with TestClient(app) as client:
        for token in ("c1", "c2"):
            headers = {"Authorization": f"Bearer {token}"}
            assert client.get("/v1/sandboxes", headers=headers).status_code == 200
            assert client.get("/v1/mesh/status", headers=headers).status_code == 403
        assert client.get("/v1/mesh/status", headers=AUTH_FULL).status_code == 200


def test_replica_receive_peer_side_dedupe(monkeypatch, tmp_path):
    app = _isolated_app(monkeypatch, tmp_path, token="t")
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def fake_pull_template(*args, **kwargs):
        calls.append((args, kwargs))
        return tmp_path / "pulled"

    monkeypatch.setattr(server_templates, "pull_template", fake_pull_template)
    payload = {
        "digest": "d1",
        "source_url": "http://src",
        "source_node": "n",
        "params": {"name": "sb1"},
    }

    with TestClient(app) as client:
        app.state.mesh.enabled = True

        first = client.post("/v1/mesh/replica/receive", headers=AUTH_T, json=payload)
        second = client.post("/v1/mesh/replica/receive", headers=AUTH_T, json=payload)

    assert first.status_code == 200
    assert first.json() == {"ok": True}
    assert len(calls) == 1
    assert second.status_code == 200
    assert second.json() == {"ok": True, "skipped": "current"}
    assert len(calls) == 1


def test_serve_forwards_tls_to_uvicorn(monkeypatch, tmp_path):
    uvicorn = pytest.importorskip("uvicorn")
    import vmon.daemon as daemon_mod

    recorded: list[dict[str, object]] = []
    releases: list[int] = []

    class FakeDaemon:
        def __init__(self, *_args, **_kwargs):
            self.shutdowns = 0

        def serve_forever(self, *, ready):
            ready.set()

        def shutdown(self):
            self.shutdowns += 1

    paths = {
        "home": tmp_path,
        "lock": tmp_path / "vmond.lock",
        "sock": tmp_path / "vmond.sock",
        "pid": tmp_path / "vmond.pid",
    }
    paths["sock"].touch()
    app = SimpleNamespace(state=SimpleNamespace(supervisor=SimpleNamespace(_engine=object())))

    monkeypatch.delenv("VMON_TLS_CERT", raising=False)
    monkeypatch.delenv("VMON_TLS_KEY", raising=False)
    monkeypatch.setattr(server_app, "create_app", lambda **_kwargs: app)
    monkeypatch.setattr(daemon_mod, "daemon_paths", lambda: paths)
    monkeypatch.setattr(daemon_mod, "acquire_owner_lock", lambda _path: 7)
    monkeypatch.setattr(daemon_mod, "release_owner_lock", lambda fd: releases.append(fd))
    monkeypatch.setattr(daemon_mod, "Daemon", FakeDaemon)
    monkeypatch.setattr(uvicorn, "run", lambda _app, **kwargs: recorded.append(kwargs))

    server_mod.serve(
        host="127.0.0.1",
        port=0,
        token="t",
        idle_timeout=300.0,
        tls_cert="/tmp/c.pem",
        tls_key="/tmp/k.pem",
    )
    paths["sock"].touch()
    server_mod.serve(host="127.0.0.1", port=0, token="t", idle_timeout=300.0)

    assert recorded[0]["ssl_certfile"] == "/tmp/c.pem"
    assert recorded[0]["ssl_keyfile"] == "/tmp/k.pem"
    assert recorded[1].get("ssl_certfile") is None
    assert recorded[1].get("ssl_keyfile") is None
    assert releases == [7, 7]
