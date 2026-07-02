from __future__ import annotations

import math
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient

from tests.test_mesh import FakeSandbox, FakeTransport


def _field(row: Any, *names: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        for name in names:
            if name in row:
                return row[name]
        return default
    for name in names:
        if hasattr(row, name):
            return getattr(row, name)
    return default


def assert_no_epoch_overlap(history: Iterable[Any]) -> None:
    """Assert no two owners ever run the same sid concurrently, across ALL epochs.

    This is the split-brain invariant: a fenced predecessor must have stopped
    (its interval ended) before a successor at any epoch starts running.
    """

    intervals: dict[str, list[tuple[float, float, str, int]]] = defaultdict(list)
    for row in history:
        sid = str(_field(row, "sid", "sandbox_id"))
        epoch = int(_field(row, "epoch"))
        owner = str(_field(row, "owner", "node", "node_id"))
        start = float(_field(row, "start", "started_at", "from_ts"))
        raw_end = _field(row, "end", "ended_at", "until_ts", default=None)
        end = math.inf if raw_end is None else float(raw_end)
        assert start < end, f"invalid owner interval for {sid} epoch {epoch}: {start} >= {end}"
        intervals[sid].append((start, end, owner, epoch))

    for sid, claims in intervals.items():
        ordered = sorted(claims)
        for index, (left_start, left_end, left_owner, left_epoch) in enumerate(ordered):
            for right_start, right_end, right_owner, right_epoch in ordered[index + 1 :]:
                if right_start >= left_end:
                    break  # sorted by start: nothing later can overlap this interval
                if left_owner == right_owner:
                    continue
                raise AssertionError(
                    f"overlapping owners for {sid}: "
                    f"{left_owner}@epoch{left_epoch}[{left_start}, {left_end}) and "
                    f"{right_owner}@epoch{right_epoch}[{right_start}, {right_end})"
                )


def assert_fencing_converges(apps: Sequence[Any], sid: str) -> None:
    """Assert all meshes agree on one owner and losers would self-fence ``sid``."""

    owners: list[tuple[str, int]] = []
    for app in apps:
        owner, epoch = app.state.mesh.authoritative_owner(sid)
        if owner is None and sid in app.state.supervisor._engine.owned_ids():
            owner = app.state.mesh.node_id
            epoch = app.state.mesh.local_epoch(sid)
        assert owner is not None, f"{app.state.mesh.node_id} cannot resolve owner for {sid}"
        owners.append((owner, epoch))

    winner = max(owners, key=lambda item: (item[1], item[0]))
    assert all(owner == winner[0] and epoch == winner[1] for owner, epoch in owners), (
        f"meshes did not converge on {sid}: {owners}"
    )

    for app in apps:
        node_id = app.state.mesh.node_id
        if node_id == winner[0]:
            continue
        local_ids = app.state.supervisor._engine.owned_ids()
        if sid in local_ids:
            assert sid in app.state.mesh.fenced_local_ids(local_ids), (
                f"{node_id} still owns superseded {sid} without a fence decision"
            )


def assert_checkpoint_age_within_bound(
    records: Iterable[Any], *, now: float, cadence: float, push_bound: float
) -> None:
    """Assert every replica checkpoint is no older than cadence plus push latency."""

    max_age = cadence + push_bound
    assert max_age >= 0, "checkpoint age bound must be non-negative"
    for row in records:
        sid = str(_field(row, "sid", "sandbox_id", "id", default="<unknown>"))
        ts = _field(
            row,
            "checkpointed_at",
            "checkpoint_at",
            "pushed_at",
            "created_at",
            "ts",
            default=None,
        )
        assert ts is not None, f"checkpoint record for {sid} has no timestamp"
        age = now - float(ts)
        assert 0 <= age <= max_age, (
            f"checkpoint for {sid} is {age:.3f}s old; bound is {max_age:.3f}s"
        )


def assert_idempotent_create_same_sid(responses: Sequence[Any]) -> None:
    """Assert successful gateway create retries all return the same sandbox id."""

    assert responses, "no create responses supplied"
    sids: list[str] = []
    for response in responses:
        if hasattr(response, "status_code"):
            assert response.status_code == 201, (
                f"create returned HTTP {response.status_code}: {getattr(response, 'text', '')}"
            )
            payload = response.json()
        else:
            payload = response
        sid = str(payload.get("id") or payload.get("name") or "")
        assert sid, f"create response has no sandbox id: {payload!r}"
        sids.append(sid)
    assert len(set(sids)) == 1, f"idempotent create returned multiple sids: {sids}"


def assert_sid_resolvable_everywhere(apps: Sequence[Any], sid: str) -> None:
    """Assert every gateway can resolve ``sid`` to a live local or peer owner."""

    for app in apps:
        mesh = app.state.mesh
        if sid in app.state.supervisor._engine.owned_ids():
            continue
        owner = mesh.owner_of(sid)
        assert owner is not None, f"{mesh.node_id} has no owner for {sid}"
        assert owner == mesh.node_id or mesh.peer_url(owner), (
            f"{mesh.node_id} resolved {sid} to unreachable owner {owner}"
        )


def assert_rerun_at_least_once(history: Iterable[Any]) -> None:
    """Assert an acked no-checkpoint owner loss is followed by at least one re-run."""

    acked: set[str] = set()
    failed: set[str] = set()
    rerun: set[str] = set()
    for row in history:
        sid = str(_field(row, "sid", "sandbox_id"))
        event = str(_field(row, "event", "kind", "type"))
        if event in {"create_ack", "acked"}:
            acked.add(sid)
        elif event in {"owner_failed_without_checkpoint", "owner_lost_no_checkpoint"}:
            failed.add(sid)
        elif event in {"rerun", "rerun_started", "rerun_completed"}:
            rerun.add(sid)
    missing = sorted((acked & failed) - rerun)
    assert not missing, f"acked sandboxes were not re-run after owner loss: {missing}"


def assert_volume_lease_non_overlap(history: Iterable[Any]) -> None:
    """Assert writable volume leases never overlap across distinct holders."""

    leases: dict[str, list[tuple[float, float, str]]] = defaultdict(list)
    for row in history:
        volume = str(_field(row, "volume", "volume_id", "name"))
        holder = str(_field(row, "holder", "owner", "node", "node_id"))
        start = float(_field(row, "start", "granted_at", "from_ts"))
        raw_end = _field(row, "end", "expires_at", "until_ts", default=None)
        end = math.inf if raw_end is None else float(raw_end)
        assert start < end, f"invalid lease interval for {volume}: {start} >= {end}"
        leases[volume].append((start, end, holder))

    for volume, intervals in leases.items():
        ordered = sorted(intervals)
        for left, right in zip(ordered, ordered[1:], strict=False):
            left_start, left_end, left_holder = left
            right_start, right_end, right_holder = right
            if left_holder == right_holder:
                continue
            assert left_end <= right_start or right_end <= left_start, (
                f"overlapping writable leases for {volume}: "
                f"{left_holder}@[{left_start}, {left_end}) and "
                f"{right_holder}@[{right_start}, {right_end})"
            )


def _two_node_apps(monkeypatch: pytest.MonkeyPatch, tmp_path):
    import vmon.server as server_mod

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    app_a = server_mod.create_app(token="secret")
    app_b = server_mod.create_app(token="secret")
    transport = FakeTransport()
    app_a.state.mesh.transport = transport
    app_b.state.mesh.transport = transport
    app_a.state.mesh.node_id = "A"
    app_b.state.mesh.node_id = "B"
    app_a.state.mesh.state_path = tmp_path / "a-mesh.json"
    app_b.state.mesh.state_path = tmp_path / "b-mesh.json"
    app_a.state.mesh.setup("http://a")
    app_b.state.mesh.setup("http://b")
    return app_a, app_b, transport


def _register_peer_with_pool(app_a: Any, app_b: Any) -> None:
    state_b = app_b.state.mesh.self_state()
    state_b.pools = {"img:x": 2}
    state_b.last_seen = time.time()
    app_a.state.mesh.register(state_b)


def _key_for_coordinator(app: Any, coordinator: str, prefix: str) -> str:
    for idx in range(1_000):
        key = f"{prefix}-{idx}"
        if app.state.mesh.coordinator_for(key) == coordinator:
            return key
    raise AssertionError(f"could not find idempotency key coordinated by {coordinator}")


def test_no_epoch_overlap_accepts_sequential_failover_history() -> None:
    assert_no_epoch_overlap(
        [
            {"sid": "vm", "epoch": 1, "owner": "A", "start": 0.0, "end": 10.0},
            {"sid": "vm", "epoch": 2, "owner": "B", "start": 10.0, "end": None},
        ]
    )

    with pytest.raises(AssertionError, match="overlapping owners"):
        assert_no_epoch_overlap(
            [
                {"sid": "vm", "epoch": 3, "owner": "A", "start": 20.0, "end": 30.0},
                {"sid": "vm", "epoch": 3, "owner": "B", "start": 29.0, "end": 40.0},
            ]
        )
    # Cross-epoch overlap is still split-brain: an unfenced old owner running
    # alongside a restored successor at a higher epoch must fail.
    with pytest.raises(AssertionError, match="overlapping owners"):
        assert_no_epoch_overlap(
            [
                {"sid": "vm", "epoch": 1, "owner": "A", "start": 0.0, "end": None},
                {"sid": "vm", "epoch": 2, "owner": "B", "start": 5.0, "end": None},
            ]
        )


def test_checkpoint_age_helper_enforces_cadence_plus_push_bound() -> None:
    assert_checkpoint_age_within_bound(
        [
            {"sid": "fresh", "checkpointed_at": 94.0},
            {"sid": "edge", "checkpointed_at": 90.0},
        ],
        now=100.0,
        cadence=8.0,
        push_bound=2.0,
    )

    with pytest.raises(AssertionError, match="stale"):
        assert_checkpoint_age_within_bound(
            [{"sid": "stale", "checkpointed_at": 89.99}],
            now=100.0,
            cadence=8.0,
            push_bound=2.0,
        )


def test_idempotent_create_helper_on_fastapi_mesh_fakes(monkeypatch, tmp_path) -> None:
    app_a, app_b, transport = _two_node_apps(monkeypatch, tmp_path)
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        _register_peer_with_pool(app_a, app_b)
        payload = {
            "image": "img:x",
            "block_network": True,
            "pool_size": 1,
            "idempotency_key": "idem-invariant",
        }

        first = client_a.post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )
        second = client_a.post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )

    assert_idempotent_create_same_sid([first, second])
    assert (
        len(app_a.state.supervisor._engine._records) + len(app_b.state.supervisor._engine._records)
        == 1
    )


def test_sid_resolves_from_every_gateway_after_remote_create(monkeypatch, tmp_path) -> None:
    app_a, app_b, transport = _two_node_apps(monkeypatch, tmp_path)
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        _register_peer_with_pool(app_a, app_b)
        created = client_a.post(
            "/v1/sandboxes",
            json={"image": "img:x", "block_network": True, "pool_size": 1, "name": "remote"},
            headers={"Authorization": "Bearer secret"},
        )

    assert created.status_code == 201
    assert_sid_resolvable_everywhere([app_a, app_b], "remote")


def test_fencing_convergence_helper_finds_superseded_owner(monkeypatch, tmp_path) -> None:
    app_a, app_b, _transport = _two_node_apps(monkeypatch, tmp_path)
    sid = "fenced"
    app_a.state.supervisor._engine.create({"name": sid, "image": "img:x", "block_network": True})
    app_b.state.supervisor._engine.create({"name": sid, "image": "img:x", "block_network": True})
    app_a.state.mesh.record_owner(sid, "A", 1)
    app_b.state.mesh.record_owner(sid, "B", 7)
    state_b = app_b.state.mesh.self_state()
    state_b.owned_epochs = {sid: 7}
    state_b.last_seen = time.time()
    app_a.state.mesh.register(state_b)
    state_a = app_a.state.mesh.self_state()
    state_a.owned_epochs = {sid: 1}
    state_a.last_seen = time.time()
    app_b.state.mesh.register(state_a)
    app_b.state.mesh.record_owner(sid, "B", 7)

    assert_fencing_converges([app_a, app_b], sid)


def test_unknown_sid_after_ack_resolves_from_durable_record(monkeypatch, tmp_path) -> None:
    app_a, app_b, transport = _two_node_apps(monkeypatch, tmp_path)
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        _register_peer_with_pool(app_a, app_b)
        key = _key_for_coordinator(app_a, "A", "acked-record")
        created = client_a.post(
            "/v1/sandboxes",
            json={
                "image": "img:x",
                "block_network": True,
                "pool_size": 1,
                "name": "acked-record",
                "idempotency_key": key,
            },
            headers={"Authorization": "Bearer secret"},
        )
        assert created.status_code == 201
        sid = created.json()["id"]
        assert sid in app_b.state.supervisor._engine._records
        assert sid not in app_a.state.supervisor._engine._records

        app_a.state.mesh.forget_owner(sid)
        resolved = client_a.get(
            f"/v1/mesh/locate/{sid}", headers={"Authorization": "Bearer secret"}
        )

    assert resolved.status_code == 200
    assert resolved.json() == {"owner": "B", "epoch": 0}


def test_rerun_at_least_once_invariant_detects_missing_reexecution() -> None:
    assert_rerun_at_least_once(
        [
            {"sid": "acked", "event": "create_ack"},
            {"sid": "acked", "event": "owner_failed_without_checkpoint"},
            {"sid": "acked", "event": "rerun_started"},
        ]
    )

    with pytest.raises(AssertionError, match="not re-run"):
        assert_rerun_at_least_once(
            [
                {"sid": "lost", "event": "create_ack"},
                {"sid": "lost", "event": "owner_failed_without_checkpoint"},
            ]
        )


def test_volume_lease_non_overlap_invariant_on_mesh_lease_routes(monkeypatch, tmp_path) -> None:
    app_a, app_b, transport = _two_node_apps(monkeypatch, tmp_path)
    now = 100.0

    def clock() -> float:
        return now

    for label, app in (("a", app_a), ("b", app_b)):
        app.state.lease_manager.root = tmp_path / f"{label}-leases"
        app.state.lease_manager._clock = clock

    headers = {"Authorization": "Bearer secret"}
    first_payload = {"volume": "shared", "holder_node": "A", "epoch": 1, "ttl": 10.0}
    second_payload = {"volume": "shared", "holder_node": "B", "epoch": 2, "ttl": 10.0}
    history: list[dict[str, Any]] = []

    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        transport.clients = {"http://a": client_a, "http://b": client_b}
        for client in (client_a, client_b):
            response = client.post("/v1/mesh/lease/grant", json=first_payload, headers=headers)
            assert response.status_code == 200
            body = response.json()
            assert body["granted"] is True
            history.append(body["record"])

        now = 109.999
        early = client_b.post("/v1/mesh/lease/grant", json=second_payload, headers=headers)
        assert early.status_code == 200
        assert early.json()["granted"] is False

        now = 110.0
        for client in (client_a, client_b):
            response = client.post("/v1/mesh/lease/grant", json=second_payload, headers=headers)
            assert response.status_code == 200
            body = response.json()
            assert body["granted"] is True
            history.append(body["record"])

    assert_volume_lease_non_overlap(history)
