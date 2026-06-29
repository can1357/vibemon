import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient

import vmon.server as server_mod

FULL_AUTH = {"Authorization": "Bearer full"}
CLIENT_AUTH = {"Authorization": "Bearer client"}


def _create_app(monkeypatch, tmp_path, *, client_token: str | None = None):
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    if client_token is None:
        monkeypatch.delenv("VMON_CLIENT_TOKEN", raising=False)
    else:
        monkeypatch.setenv("VMON_CLIENT_TOKEN", client_token)
    return server_mod.create_app(token="full")


def test_client_token_allows_sandbox_routes_blocks_admin(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path, client_token="client")

    with TestClient(app) as client:
        assert client.get("/v1/sandboxes", headers=CLIENT_AUTH).status_code == 200
        assert client.get("/v1/mesh/status", headers=CLIENT_AUTH).status_code == 403
        assert (
            client.post("/v1/sandboxes/demo/migrate", headers=CLIENT_AUTH, json={}).status_code
            == 403
        )
        assert client.get("/v1/mesh/status", headers=FULL_AUTH).status_code == 200
        assert client.get("/v1/sandboxes", headers=FULL_AUTH).status_code == 200


def test_client_token_unset_unchanged(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path)

    with TestClient(app) as client:
        assert client.get("/v1/sandboxes", headers=FULL_AUTH).status_code == 200
        assert client.get("/v1/mesh/status", headers=FULL_AUTH).status_code == 200
        assert client.get("/v1/sandboxes", headers=CLIENT_AUTH).status_code == 401


def test_full_token_unaffected_by_client_tier(monkeypatch, tmp_path):
    app = _create_app(monkeypatch, tmp_path, client_token="client")

    with TestClient(app) as client:
        assert client.get("/v1/sandboxes", headers=FULL_AUTH).status_code == 200
        assert client.get("/v1/mesh/status", headers=FULL_AUTH).status_code == 200
