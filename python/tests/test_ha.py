from __future__ import annotations

import time
from pathlib import Path

import pytest

from tests.test_mesh import FakeEngine, FakeTransport
from vmon.mesh import Mesh, NodeState, _hrw_score
from vmon.replica import ReplicaStore


@pytest.fixture
def sid() -> str:
    return "ha-vm"


def test_replica_store_round_trip_no_secrets_on_disk(tmp_path: Path, sid: str) -> None:
    root = tmp_path / "replicas"
    store = ReplicaStore(root)
    params = {"name": sid, "env": {"PUBLIC": "ok"}, "secrets": [{"K": "V"}]}

    store.put(
        sid,
        digest="sha256:digest",
        source_node="owner-a",
        snapshot_dir="/snapshots/ha-vm",
        params=params,
    )

    text = (root / f"{sid}.json").read_text(encoding="utf-8")
    assert '"V"' not in text
    assert '"secrets"' not in text

    record = store.get(sid)
    assert record is not None
    assert record.params == params
    assert record.needs_secrets is True
    assert store.secrets_ready(sid) is True


def test_replica_store_restart_loses_secrets(tmp_path: Path, sid: str) -> None:
    root = tmp_path / "replicas"
    ReplicaStore(root).put(
        sid,
        digest="sha256:digest",
        source_node="owner-a",
        snapshot_dir="/snapshots/ha-vm",
        params={"name": sid, "env": {"PUBLIC": "ok"}, "secrets": [{"K": "V"}]},
    )

    restarted = ReplicaStore(root)
    restarted.load()

    assert restarted.holds(sid) is True
    record = restarted.get(sid)
    assert record is not None
    assert "secrets" not in record.params
    assert record.needs_secrets is True
    assert restarted.secrets_ready(sid) is False


def _mesh(tmp_path: Path, *, owned: list[str] | None = None, node_id: str = "self") -> Mesh:
    mesh = Mesh(
        FakeEngine(owned=owned or []),
        advertise=f"http://{node_id}",
        token="t",
        transport=FakeTransport(),
        state_path=tmp_path / f"{node_id}.json",
    )
    mesh.node_id = node_id
    return mesh


def _healthy_peer(
    node_id: str, *, owned: list[str] | None = None, epoch: int | None = None
) -> NodeState:
    owned_epochs = dict.fromkeys(owned or [], epoch) if epoch is not None else {}
    return NodeState(
        node_id,
        f"http://{node_id}",
        owned=list(owned or []),
        owned_epochs=owned_epochs,
        last_seen=time.time(),
    )


def test_replica_targets_and_restore_owner(tmp_path: Path, sid: str) -> None:
    mesh = _mesh(tmp_path)
    for peer_id in ("p1", "p2"):
        mesh.register(_healthy_peer(peer_id))

    targets = mesh.replica_targets(sid, 1)
    assert targets == [max(("p1", "p2"), key=lambda node_id: _hrw_score(sid, node_id))]
    assert "self" not in targets
    assert mesh.replica_targets(sid, 1) == targets

    mesh.register(_healthy_peer("dead"))
    restore_sid = next(
        f"{sid}-{i}"
        for i in range(10_000)
        if max(
            ("self", "p1", "p2", "dead"),
            key=lambda node_id: _hrw_score(f"{sid}-{i}", node_id),
        )
        == "dead"
    )
    expected = max(("self", "p1", "p2"), key=lambda node_id: _hrw_score(restore_sid, node_id))

    assert mesh.restore_owner(restore_sid, exclude={"dead"}) == expected
    assert mesh.restore_owner(restore_sid, exclude={"dead"}) != "dead"
    assert mesh.restore_owner(restore_sid, exclude={"dead"}) == mesh.restore_owner(
        restore_sid, exclude={"dead"}
    )


def test_orphan_queue_on_reap(tmp_path: Path, sid: str) -> None:
    peer_id = "peer"
    mesh = _mesh(tmp_path)
    mesh.register(_healthy_peer(peer_id, owned=[sid], epoch=0))
    mesh.record_owner(sid, peer_id, 0)

    mesh.reap_sec = 0.01
    mesh._peers[peer_id].last_seen = time.time() - 1.0
    mesh.heartbeat_once()

    assert mesh.drain_orphans() == [(sid, peer_id)]
    assert mesh.drain_orphans() == []


def test_epoch_tiebreak_and_stale_rejected(tmp_path: Path, sid: str) -> None:
    mesh = _mesh(tmp_path, node_id="A")

    mesh.record_owner(sid, "A", 5)
    assert mesh.authoritative_owner(sid) == ("A", 5)

    mesh.record_owner(sid, "B", 3)
    assert mesh.authoritative_owner(sid) == ("A", 5)

    mesh.record_owner(sid, "C", 5)
    assert "C" > "A"
    assert mesh.authoritative_owner(sid) == ("C", 5)
    assert mesh.next_epoch(sid) == 6


def test_fenced_local_ids(tmp_path: Path) -> None:
    mesh = _mesh(tmp_path, owned=["x"], node_id="A")
    mesh.record_owner("x", "A", 4)
    assert mesh.local_epoch("x") == 4

    mesh.register(_healthy_peer("B", owned=["x"], epoch=9))
    mesh.owner_of("x")

    assert mesh.authoritative_owner("x") == ("B", 9)
    assert mesh.fenced_local_ids(["x"]) == ["x"]

    mesh.forget_local_owner("x")
    assert mesh.local_epoch("x") == 0
    assert mesh.authoritative_owner("x") == ("B", 9)


class DurableFakeEngine(FakeEngine):
    def __init__(self, *, owned: list[str]) -> None:
        super().__init__(owned=owned)
        self.epochs: dict[str, int] = {}

    def set_owner_epoch(self, sid: str, epoch: int) -> None:
        self.epochs[sid] = epoch

    def owned_epoch(self, sid: str) -> int:
        return self.epochs.get(sid, 0)


def test_epoch_durable_across_restart(tmp_path: Path) -> None:
    engine = DurableFakeEngine(owned=["x"])
    mesh1 = Mesh(
        engine,
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        state_path=tmp_path / "mesh1.json",
    )
    mesh1.node_id = "self"
    mesh1.record_owner("x", "self", 4)
    assert engine.owned_epoch("x") == 4

    mesh2 = Mesh(
        engine,
        advertise="http://self",
        token="t",
        transport=FakeTransport(),
        state_path=tmp_path / "mesh2.json",
    )
    mesh2.node_id = "self"

    assert mesh2.owner_of("x") == "self"
    assert mesh2.authoritative_owner("x") == ("self", 4)
