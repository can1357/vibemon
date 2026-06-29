from __future__ import annotations

import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient

import vmon.server as server_mod
from tests.test_mesh import FakeEngine, FakeTransport
from vmon.mesh import Mesh, NodeState


def _mesh(tmp_path: Path) -> Mesh:
    mesh = Mesh(
        FakeEngine(),
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        state_path=tmp_path / "mesh.json",
    )
    mesh.node_id = "self"
    mesh.enabled = True
    return mesh


def _peer(node_id: str) -> NodeState:
    return NodeState(node_id, f"http://{node_id}", last_seen=time.time())


def test_expected_members_grows_persists_shrinks(tmp_path: Path) -> None:
    mesh = _mesh(tmp_path)

    assert mesh._expected_members == 1

    mesh.register(_peer("p1"))
    mesh.register(_peer("p2"))

    assert mesh._expected_members == 3

    mesh.save()
    loaded = _mesh(tmp_path)
    loaded.load()

    assert loaded._expected_members == 3

    loaded.depart("p1")
    assert loaded._expected_members == 2

    loaded.leave()
    assert loaded._expected_members == 1


def test_expected_members_persists_via_gossip_merge(tmp_path: Path) -> None:
    # Growth learned over gossip (no explicit save() call) must still persist, so a
    # restarted non-seed never relearns a too-small quorum from disk and restores alone.
    mesh = _mesh(tmp_path)
    mesh._merge_heartbeat_response(
        {
            "state": _peer("p1").to_wire(),
            "known": [_peer("p1").to_wire(), _peer("p2").to_wire()],
        }
    )
    assert mesh._expected_members == 3
    reloaded = _mesh(tmp_path)
    reloaded.load()
    assert reloaded._expected_members == 3


def test_quorum_needed_and_met(tmp_path: Path) -> None:
    mesh = _mesh(tmp_path)

    mesh._expected_members = 3
    assert mesh.quorum_needed() == 2
    assert mesh.restore_quorum_met(2) is True
    assert mesh.restore_quorum_met(1) is False

    mesh._expected_members = 5
    assert mesh.quorum_needed() == 3


def test_is_peer_healthy_and_live_members(tmp_path: Path) -> None:
    mesh = _mesh(tmp_path)

    healthy = _peer("healthy")
    stale = _peer("stale")
    mesh.register(healthy)
    mesh.register(stale)
    mesh._peers["stale"].last_seen = time.time() - 10 * mesh.interval

    assert mesh.is_peer_healthy("self") is True
    assert mesh.is_peer_healthy("healthy") is True
    assert mesh.is_peer_healthy("ghost") is False
    assert mesh.is_peer_healthy("stale") is False

    live = set(mesh.live_member_ids())
    assert live == {"self", "healthy"}


def test_reachable_endpoint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    app = server_mod.create_app(token="t")
    app.state.mesh.enabled = True
    app.state.mesh.register(NodeState("p1", "http://p1", last_seen=time.time()))

    headers = {"Authorization": "Bearer t"}
    with TestClient(app) as client:
        response = client.get("/v1/mesh/reachable/p1", headers=headers)
        assert response.status_code == 200
        assert response.json() == {"reachable": True}

        response = client.get("/v1/mesh/reachable/ghost", headers=headers)
        assert response.status_code == 200
        assert response.json() == {"reachable": False}

        response = client.get(f"/v1/mesh/reachable/{app.state.mesh.node_id}", headers=headers)
        assert response.status_code == 200
        assert response.json() == {"reachable": True}
