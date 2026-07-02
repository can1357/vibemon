from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient

from tests.test_invariants import assert_fencing_converges, assert_idempotent_create_same_sid
from tests.test_mesh import FakeSandbox, FakeTransport
from vmon.mesh import MeshError, NodeState
from vmon.replica import ReplicaStore


@dataclass
class _Rule:
    method: str
    path: str
    count: int
    source: str | None = None
    target: str | None = None

    def matches(self, method: str, path: str, source: str | None, target: str) -> bool:
        return (
            self.method == method
            and self.path == path
            and (self.source is None or self.source == source)
            and (self.target is None or self.target == target)
        )


@dataclass
class _DelayedCall:
    method: str
    source: str | None
    target: str
    path: str
    payload: dict[str, Any] | None
    timeout: float | None


class _FaultState:
    def __init__(self) -> None:
        self.partitions: set[frozenset[str]] = set()
        self.drop_rules: list[_Rule] = []
        self.ambiguous_rules: list[_Rule] = []
        self.delay_rules: list[_Rule] = []
        self.delayed: list[_DelayedCall] = []


class FaultyTransport:
    """Fault-injecting wrapper around the mesh transport seam.

    Each mesh receives a node-specific view via ``for_node(source_url)`` so faults
    can target A->B and B->A independently while sharing one wrapped transport.
    """

    def __init__(
        self,
        inner: FakeTransport | None = None,
        *,
        source_url: str | None = None,
        state: _FaultState | None = None,
    ) -> None:
        self.inner = inner or FakeTransport()
        self.source_url = source_url
        self._state = state or _FaultState()

    @property
    def clients(self) -> dict[str, TestClient]:
        return self.inner.clients

    @clients.setter
    def clients(self, value: dict[str, TestClient]) -> None:
        self.inner.clients = value

    @property
    def token(self) -> str:
        return self.inner.token

    @token.setter
    def token(self, value: str) -> None:
        self.inner.token = value

    def for_node(self, source_url: str) -> FaultyTransport:
        return FaultyTransport(self.inner, source_url=source_url, state=self._state)

    def partition(self, left: str, right: str) -> None:
        self._state.partitions.add(frozenset((left, right)))

    def heal(self, left: str, right: str) -> None:
        self._state.partitions.discard(frozenset((left, right)))

    def drop_next(
        self,
        method: str,
        path: str,
        *,
        count: int = 1,
        source: str | None = None,
        target: str | None = None,
    ) -> None:
        self._state.drop_rules.append(_Rule(method.upper(), path, count, source, target))

    def ambiguous_after_delivery(
        self,
        method: str,
        path: str,
        *,
        count: int = 1,
        source: str | None = None,
        target: str | None = None,
    ) -> None:
        self._state.ambiguous_rules.append(_Rule(method.upper(), path, count, source, target))

    def delay(
        self,
        method: str,
        path: str,
        *,
        count: int = 1,
        source: str | None = None,
        target: str | None = None,
    ) -> None:
        self._state.delay_rules.append(_Rule(method.upper(), path, count, source, target))

    def flush_delayed(self) -> list[dict[str, Any]]:
        calls = list(self._state.delayed)
        self._state.delayed.clear()
        results: list[dict[str, Any]] = []
        for call in calls:
            results.append(
                self._deliver(call.method, call.target, call.path, call.payload, call.timeout)
            )
        return results

    def post(
        self, base_url: str, path: str, payload: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        return self._request("POST", base_url, path, payload, timeout)

    def get(self, base_url: str, path: str, *, timeout: float | None = None) -> dict[str, Any]:
        return self._request("GET", base_url, path, None, timeout)

    def _request(
        self,
        method: str,
        base_url: str,
        path: str,
        payload: dict[str, Any] | None,
        timeout: float | None,
    ) -> dict[str, Any]:
        if self._partitioned(base_url):
            raise MeshError("peer unreachable", code="unreachable")
        if self._consume(self._state.drop_rules, method, path, base_url):
            raise MeshError("peer unreachable", code="unreachable")
        if self._consume(self._state.delay_rules, method, path, base_url):
            self._state.delayed.append(
                _DelayedCall(method, self.source_url, base_url, path, payload, timeout)
            )
            raise MeshError("peer unreachable", code="unreachable")
        if self._consume(self._state.ambiguous_rules, method, path, base_url):
            self._deliver(method, base_url, path, payload, timeout)
            raise MeshError("peer response ambiguous", code="ambiguous")
        return self._deliver(method, base_url, path, payload, timeout)

    def _partitioned(self, base_url: str) -> bool:
        if self.source_url is None:
            return False
        return frozenset((self.source_url, base_url)) in self._state.partitions

    def _consume(self, rules: list[_Rule], method: str, path: str, target: str) -> bool:
        for rule in list(rules):
            if not rule.matches(method, path, self.source_url, target):
                continue
            rule.count -= 1
            if rule.count <= 0:
                rules.remove(rule)
            return True
        return False

    def _deliver(
        self,
        method: str,
        base_url: str,
        path: str,
        payload: dict[str, Any] | None,
        timeout: float | None,
    ) -> dict[str, Any]:
        if method == "POST":
            assert payload is not None
            return self.inner.post(base_url, path, payload, timeout=timeout)
        return self.inner.get(base_url, path, timeout=timeout)


def _mesh_apps(monkeypatch: pytest.MonkeyPatch, tmp_path, node_ids: tuple[str, ...] = ("A", "B")):
    import vmon.server as server_mod

    monkeypatch.setenv("VMON_HOME", str(tmp_path))
    monkeypatch.setattr("vmon.core.Engine._sandbox_class", staticmethod(lambda: FakeSandbox))
    transport = FaultyTransport(FakeTransport())
    apps: dict[str, Any] = {}
    urls: dict[str, str] = {}
    for node_id in node_ids:
        url = f"http://{node_id.lower()}"
        app = server_mod.create_app(token="secret")
        app.state.mesh.transport = transport.for_node(url)
        app.state.mesh.node_id = node_id
        app.state.mesh.state_path = tmp_path / f"{node_id.lower()}-mesh.json"
        app.state.mesh.setup(url)
        apps[node_id] = app
        urls[node_id] = url
    return apps, urls, transport


def _open_clients(apps: dict[str, Any], urls: dict[str, str], transport: FaultyTransport):
    stack = ExitStack()
    clients = {urls[node_id]: stack.enter_context(TestClient(app)) for node_id, app in apps.items()}
    transport.clients = clients
    return stack, clients


def _register_all(apps: dict[str, Any]) -> None:
    for src_id, src in apps.items():
        for dst_id, dst in apps.items():
            if src_id == dst_id:
                continue
            src.state.mesh.register(dst.state.mesh.self_state())


def _force_remote_pool(src: Any, dst: Any) -> None:
    state = dst.state.mesh.self_state()
    state.pools = {"img:x": 4}
    src.state.mesh.register(state)


def _key_for_coordinator(app: Any, coordinator: str, prefix: str) -> str:
    for idx in range(1_000):
        key = f"{prefix}-{idx}"
        if app.state.mesh.coordinator_for(key) == coordinator:
            return key
    raise AssertionError(f"could not find idempotency key coordinated by {coordinator}")


def _create_payload(name: str, key: str) -> dict[str, Any]:
    return {
        "name": name,
        "image": "img:x",
        "block_network": True,
        "pool_size": 1,
        "idempotency_key": key,
    }


def _record_count(apps: dict[str, Any]) -> int:
    return sum(len(app.state.supervisor._engine._records) for app in apps.values())


def test_partition_during_idempotent_create_never_duplicates_after_heal(monkeypatch, tmp_path):
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        _force_remote_pool(apps["A"], apps["B"])
        key = _key_for_coordinator(apps["A"], "A", "partitioned")
        payload = _create_payload("partitioned", key)

        transport.partition(urls["A"], urls["B"])
        first = clients[urls["A"]].post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )
        transport.heal(urls["A"], urls["B"])
        second = clients[urls["B"]].post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )

    assert first.status_code == 503
    assert "create record" in first.json()["detail"]
    assert "replicated to 1/2 required members" in first.json()["detail"]
    assert_idempotent_create_same_sid([second])
    assert second.json()["id"] == "partitioned"
    assert _record_count(apps) == 1


def test_drop_once_owner_unavailable_retry_results_in_one_sandbox(monkeypatch, tmp_path):
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        _force_remote_pool(apps["A"], apps["B"])
        key = _key_for_coordinator(apps["A"], "A", "drop-owner")
        payload = _create_payload("drop-owner", key)
        transport.drop_next(
            "POST",
            "/v1/mesh/idem/sandboxes/work",
            source=urls["A"],
            target=urls["B"],
        )

        first = clients[urls["A"]].post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )
        retry = clients[urls["A"]].post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )

    assert_idempotent_create_same_sid([first, retry])
    assert first.json()["id"] == "drop-owner"
    assert _record_count(apps) == 1


def test_ambiguous_post_delivery_retry_replays_committed_create(monkeypatch, tmp_path):
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    stack, clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        _force_remote_pool(apps["A"], apps["B"])
        key = _key_for_coordinator(apps["A"], "A", "ambiguous")
        payload = _create_payload("ambiguous", key)
        transport.ambiguous_after_delivery(
            "POST",
            "/v1/mesh/idem/sandboxes/work",
            source=urls["A"],
            target=urls["B"],
        )

        first = clients[urls["A"]].post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )
        retry = clients[urls["A"]].post(
            "/v1/sandboxes", json=payload, headers={"Authorization": "Bearer secret"}
        )

    assert first.status_code == 504
    assert_idempotent_create_same_sid([retry])
    assert retry.json()["id"] == "ambiguous"
    assert _record_count(apps) == 1
    assert "ambiguous" in apps["B"].state.supervisor._engine._records


def test_delayed_gossip_converges_and_higher_epoch_wins(monkeypatch, tmp_path):
    apps, urls, transport = _mesh_apps(monkeypatch, tmp_path)
    stack, _clients = _open_clients(apps, urls, transport)
    with stack:
        _register_all(apps)
        sid = "epoch-race"
        apps["A"].state.supervisor._engine.create(
            {"name": sid, "image": "img:x", "block_network": True}
        )
        apps["B"].state.supervisor._engine.create(
            {"name": sid, "image": "img:x", "block_network": True}
        )
        apps["A"].state.mesh.record_owner(sid, "A", 1)
        apps["B"].state.mesh.record_owner(sid, "B", 7)
        transport.delay("POST", "/v1/mesh/heartbeat", source=urls["B"], target=urls["A"])

        apps["B"].state.mesh.heartbeat_once()
        assert apps["A"].state.mesh.authoritative_owner(sid) == ("A", 1)

        transport.flush_delayed()

    assert apps["A"].state.mesh.authoritative_owner(sid) == ("B", 7)
    assert_fencing_converges([apps["A"], apps["B"]], sid)


def test_replica_restart_with_lost_secrets_refuses_restore(tmp_path) -> None:
    sid = "secret-replica"
    root = tmp_path / "replicas"
    ReplicaStore(root).put(
        sid,
        digest="sha256:replica",
        source_node="A",
        snapshot_dir="/snapshots/secret-replica",
        params={"name": sid, "image": "img:x", "secrets": [{"TOKEN": "s3cr3t"}]},
    )

    restarted = ReplicaStore(root)
    restarted.load()

    assert restarted.holds(sid) is True
    record = restarted.get(sid)
    assert record is not None
    assert record.needs_secrets is True
    assert "secrets" not in record.params
    assert restarted.secrets_ready(sid) is False


def test_clock_skewed_heartbeat_uses_receiver_time_for_health(monkeypatch, tmp_path) -> None:
    import vmon.mesh as mesh_mod

    now = [1_000.0]
    monkeypatch.setattr(mesh_mod.time, "time", lambda: now[0])
    apps, _urls, _transport = _mesh_apps(monkeypatch, tmp_path)
    mesh_a = apps["A"].state.mesh
    mesh_a.interval = 10.0

    mesh_a.heartbeat(
        NodeState(
            node_id="B",
            advertise="http://b",
            ts=1_000_000.0,
            last_seen=1_000_000.0,
        ),
        [],
    )

    now[0] = 1_029.9
    assert mesh_a.is_peer_healthy("B") is True
    now[0] = 1_030.1
    assert mesh_a.is_peer_healthy("B") is False
