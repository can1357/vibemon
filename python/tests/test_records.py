from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest
from fastapi.security import HTTPBearer

from tests.test_faults import (
    _create_payload,
    _force_remote_pool,
    _key_for_coordinator,
    _mesh_apps,
    _open_clients,
    _record_count,
    _register_all,
)
from tests.test_mesh import FakeSandbox
from vmon.replica import ReplicaStore
from vmon.server.runtime import ServerRuntime

AUTH = {"Authorization": "Bearer secret"}


class CountingSandbox(FakeSandbox):
    created: list[str] = []

    @classmethod
    def create(cls, **kwargs):
        name = kwargs.get("name") or "sb"
        cls.created.append(name)
        return cls(name, kwargs.get("tags") or {})


def _runtime_for(app: Any) -> ServerRuntime:
    return ServerRuntime(
        supervisor=app.state.supervisor,
        mesh=app.state.mesh,
        replica_store=ReplicaStore(),
        record_store=app.state.record_store,
        lease_manager=app.state.lease_manager,
        expected_token="secret",
        outbound_token="secret",
        client_token=None,
        bearer=HTTPBearer(auto_error=False),
        config=app.state.config,
    )


def _force_peer_reaped(app: Any, node_id: str) -> None:
    mesh = app.state.mesh
    with mesh._lock:
        peer = mesh._peers[node_id]
        peer.last_seen = time.time() - mesh.reap_sec - 1.0
    mesh._reap_stale_peers()


def _run_reconcile_once(app: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import vmon.server.reconciler as reconciler

    class StopReconcile(Exception):
        pass

    first_sleep = True

    async def fake_sleep(_delay: float) -> None:
        nonlocal first_sleep
        if first_sleep:
            first_sleep = False
            return
        raise StopReconcile

    monkeypatch.setattr(reconciler.asyncio, "sleep", fake_sleep)

    async def run() -> None:
        try:
            await reconciler.ha_reconcile_forever(_runtime_for(app))
        except StopReconcile:
            return
        raise AssertionError("HA reconciler did not stop after one deterministic pass")

    asyncio.run(run())


def test_two_node_create_requires_record_majority_before_ack(monkeypatch, tmp_path) -> None:
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        key = _key_for_coordinator(apps["A"], "A", "majority-record")
        payload = _create_payload("majority-record", key)

        transport.partition(urls["A"], urls["B"])
        first = clients[urls["A"]].post("/v1/sandboxes", json=payload, headers=AUTH)

        assert first.status_code == 503
        detail = first.json()["detail"]
        assert detail["code"] == "record_unreplicated"
        assert "replicated to 1/2 required members" in detail["message"]
        assert _record_count(apps) == 1

        transport.heal(urls["A"], urls["B"])
        retry = clients[urls["A"]].post("/v1/sandboxes", json=payload, headers=AUTH)
        assert retry.status_code == 201
        assert retry.json()["id"] == "majority-record"
        assert _record_count(apps) == 1
        apps["B"].state.mesh.forget_owner("majority-record")
        replicated = clients[urls["B"]].get("/v1/mesh/locate/majority-record", headers=AUTH)

    assert replicated.status_code == 200
    assert replicated.json() == {"owner": "A", "epoch": 0}


def test_record_json_never_persists_secret_material(monkeypatch, tmp_path) -> None:
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        key = _key_for_coordinator(apps["A"], "A", "secret-record")
        payload = {
            **_create_payload("secret-record", key),
            "secrets": [{"TOKEN": "dont-persist-me"}],
        }

        created = clients[urls["A"]].post("/v1/sandboxes", json=payload, headers=AUTH)

    assert created.status_code == 201
    record_path = tmp_path / "records" / "secret-record.json"
    raw = record_path.read_text(encoding="utf-8")
    data = json.loads(raw)

    assert "dont-persist-me" not in raw
    assert "TOKEN" not in raw
    assert "secrets" not in data["params"]


def test_record_store_resolves_acked_remote_create_after_owner_gossip_is_lost(
    monkeypatch, tmp_path
) -> None:
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        _force_remote_pool(apps["A"], apps["B"])
        key = _key_for_coordinator(apps["A"], "A", "record-resolve")
        created = clients[urls["A"]].post(
            "/v1/sandboxes",
            json=_create_payload("record-resolve", key),
            headers=AUTH,
        )
        assert created.status_code == 201
        sid = created.json()["id"]
        before_gossip_loss = clients[urls["A"]].get(f"/v1/mesh/locate/{sid}", headers=AUTH)
        assert before_gossip_loss.status_code == 200
        assert before_gossip_loss.json()["owner"] == "B"

        apps["A"].state.mesh.forget_owner(sid)
        transport.partition(urls["A"], urls["B"])
        located = clients[urls["A"]].get(f"/v1/mesh/locate/{sid}", headers=AUTH)

    assert located.status_code == 200
    assert located.json() == {"owner": "B", "epoch": 0}


def test_rerun_tier_reexecutes_once_after_owner_loss_without_checkpoint(
    monkeypatch, tmp_path
) -> None:
    CountingSandbox.created = []
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path, node_ids=("A", "B", "C"))
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: CountingSandbox))
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        created = clients[urls["A"]].post(
            "/v1/sandboxes",
            json={
                "name": "rerun-no-checkpoint",
                "image": "img:x",
                "context": "owner-local-context",
                "block_network": True,
                "ha": "rerun",
                "idempotency_key": "rerun-no-checkpoint-key",
            },
            headers=AUTH,
        )
        assert created.status_code == 201
        sid = created.json()["id"]
        assert CountingSandbox.created == [sid]
        assert clients[urls["B"]].get(f"/v1/mesh/locate/{sid}", headers=AUTH).status_code == 200
        assert clients[urls["C"]].get(f"/v1/mesh/locate/{sid}", headers=AUTH).status_code == 200

        apps["A"].state.supervisor._engine._records.pop(sid, None)
        transport.partition(urls["A"], urls["B"])
        transport.partition(urls["A"], urls["C"])
        _force_peer_reaped(apps["B"], "A")
        _force_peer_reaped(apps["C"], "A")
        restorer_id = next(
            node_id
            for node_id in ("B", "C")
            if apps[node_id].state.mesh.restore_owner(sid, exclude={"A"}) == node_id
        )

        _run_reconcile_once(apps[restorer_id], monkeypatch)
        apps[restorer_id].state.mesh._orphans.append((sid, "A"))
        _run_reconcile_once(apps[restorer_id], monkeypatch)
        rerun_location = clients[urls[restorer_id]].get(f"/v1/mesh/locate/{sid}", headers=AUTH)

    assert CountingSandbox.created == [sid, sid]
    assert rerun_location.status_code == 200
    assert rerun_location.json() == {"owner": restorer_id, "epoch": 1}


def test_rerun_tier_does_not_reexecute_when_predecessor_is_reachable(monkeypatch, tmp_path) -> None:
    CountingSandbox.created = []
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: CountingSandbox))
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        created = clients[urls["A"]].post(
            "/v1/sandboxes",
            json={
                "name": "rerun-reachable-owner",
                "image": "img:x",
                "context": "owner-local-context",
                "block_network": True,
                "ha": "rerun",
                "idempotency_key": "rerun-reachable-owner-key",
            },
            headers=AUTH,
        )
        assert created.status_code == 201
        sid = created.json()["id"]
        assert CountingSandbox.created == [sid]

        _force_peer_reaped(apps["B"], "A")
        apps["B"].state.mesh.register(apps["A"].state.mesh.self_state())
        _run_reconcile_once(apps["B"], monkeypatch)
        owner_after_reconcile = clients[urls["B"]].get(f"/v1/mesh/locate/{sid}", headers=AUTH)

    assert CountingSandbox.created == [sid]
    assert owner_after_reconcile.status_code == 200
    assert owner_after_reconcile.json() == {"owner": "A", "epoch": 0}
