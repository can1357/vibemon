from __future__ import annotations

import json

import pytest
from v1_stub import V1StubServer

from vmon.context import (
    Context,
    ContextStore,
    context_token_path,
    contexts_path,
    roster_from_status,
)
from vmon.sandbox import Sandbox


def test_context_store_round_trip_omits_tokens_from_contexts_json(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("VMON_CONTEXT", raising=False)
    store = ContextStore(contexts_path(tmp_path))

    store.put(Context("prod", ["https://a.example", "https://b.example"], token="in-memory"))
    store.use("prod")
    store.save_token("prod", "persisted-token")

    raw = json.loads(contexts_path(tmp_path).read_text(encoding="utf-8"))
    assert raw["current"] == "prod"
    assert raw["contexts"]["prod"]["endpoints"] == ["https://a.example", "https://b.example"]
    assert "persisted-token" not in contexts_path(tmp_path).read_text(encoding="utf-8")
    assert "in-memory" not in contexts_path(tmp_path).read_text(encoding="utf-8")
    assert (
        context_token_path("prod", tmp_path).read_text(encoding="utf-8").strip()
        == "persisted-token"
    )

    reloaded = ContextStore(contexts_path(tmp_path))
    reloaded.load()
    assert reloaded.current_name() == "prod"
    assert reloaded.get("prod") == Context("prod", ["https://a.example", "https://b.example"])
    assert reloaded.resolve_token("prod") == "persisted-token"


def test_context_store_rejects_path_traversal_for_credentials(tmp_path) -> None:
    store = ContextStore(contexts_path(tmp_path))

    with pytest.raises(ValueError):
        store.put(Context("../evil", ["http://127.0.0.1"]))
    with pytest.raises(ValueError):
        store.save_token("../evil", "token")
    assert store.load_token("../evil") is None


def test_roster_from_mesh_status_keeps_order_and_deduplicates() -> None:
    status = {
        "self": {"advertise": "http://self"},
        "peers": [
            {"advertise": "http://peer"},
            {"advertise": "http://self"},
            {"advertise": ""},
        ],
    }

    assert roster_from_status(status, fallback="http://fallback") == ["http://self", "http://peer"]
    assert roster_from_status({}, fallback="http://fallback") == ["http://fallback"]


def test_selected_context_uses_saved_endpoint_and_env_token_precedence(
    monkeypatch, mvm_home
) -> None:
    with V1StubServer() as server:
        monkeypatch.setenv("VMON_HOME", str(mvm_home))
        monkeypatch.setenv("VMON_CONTEXT", "prod")
        monkeypatch.setenv("VMON_API_TOKEN", "env-token")
        store = ContextStore(contexts_path(mvm_home))
        store.put(Context("prod", [server.url]))
        store.use("prod")
        store.save_token("prod", "saved-token")

        assert Sandbox.list() == []
        assert server.last("GET", "/v1/sandboxes").headers["authorization"] == "Bearer env-token"

        monkeypatch.delenv("VMON_API_TOKEN", raising=False)
        assert Sandbox.list() == []
        assert server.last("GET", "/v1/sandboxes").headers["authorization"] == "Bearer saved-token"


def test_explicit_create_context_overrides_selected_context(monkeypatch, mvm_home) -> None:
    with V1StubServer() as prod, V1StubServer() as dev:
        monkeypatch.setenv("VMON_HOME", str(mvm_home))
        monkeypatch.setenv("VMON_CONTEXT", "prod")
        monkeypatch.delenv("VMON_API_TOKEN", raising=False)
        store = ContextStore(contexts_path(mvm_home))
        store.put(Context("prod", [prod.url]))
        store.put(Context("dev", [dev.url]))
        store.use("prod")

        sandbox = Sandbox.create(image="python:3.14-slim", context="dev", name="routed")

        assert sandbox.name == "routed"
        assert prod.recorded("POST", "/v1/sandboxes") == []
        assert dev.last("POST", "/v1/sandboxes").json["name"] == "routed"
