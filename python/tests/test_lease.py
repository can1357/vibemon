from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from vmon.lease import LeaseManager, LeaseRecord, LeaseUnavailable
from vmon.server import leases as server_leases


class ManualClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class LeaseCluster:
    def __init__(self, tmp_path, nodes: tuple[str, ...] = ("A", "B", "C")) -> None:
        self.clock = ManualClock(100.0)
        self.managers = {
            node: LeaseManager(tmp_path / node / "leases", clock=self.clock) for node in nodes
        }
        self.urls = {node: f"http://{node.lower()}" for node in nodes}
        self._by_url = {url: self.managers[node] for node, url in self.urls.items()}

    def post(self, url: str, path: str, payload: dict[str, object]) -> dict[str, object]:
        manager = self._by_url[url]
        if path == "/v1/mesh/lease/grant":
            decision = manager.vote_grant(
                str(payload["volume"]),
                str(payload["holder_node"]),
                int(payload["epoch"]),
                float(payload["ttl"]),
            )
        elif path == "/v1/mesh/lease/renew":
            decision = manager.vote_renew(
                str(payload["volume"]),
                str(payload["holder_node"]),
                int(payload["epoch"]),
                float(payload["ttl"]),
            )
        elif path == "/v1/mesh/lease/release":
            decision = manager.vote_release(
                str(payload["volume"]), str(payload["holder_node"]), int(payload["epoch"])
            )
        else:  # pragma: no cover - a bad route is a broken test fixture
            raise AssertionError(f"unexpected lease path {path!r}")
        return decision.to_wire()

    def grant(
        self,
        requester: str,
        *,
        volume: str = "data",
        holder: str | None = None,
        epoch: int = 1,
        ttl: float = 20.0,
        expected_members: int | None = None,
        peer_urls: dict[str, str] | None = None,
    ) -> LeaseRecord:
        return self.managers[requester].request_grant(
            volume=volume,
            holder_node=holder or requester,
            epoch=epoch,
            ttl=ttl,
            self_node=requester,
            peer_urls=self.urls if peer_urls is None else peer_urls,
            expected_members=expected_members or len(self.managers),
            post=self.post,
        )

    def renew(
        self,
        requester: str,
        *,
        volume: str = "data",
        holder: str | None = None,
        epoch: int = 1,
        ttl: float = 20.0,
        expected_members: int | None = None,
    ) -> LeaseRecord:
        return self.managers[requester].request_renew(
            volume=volume,
            holder_node=holder or requester,
            epoch=epoch,
            ttl=ttl,
            self_node=requester,
            peer_urls=self.urls,
            expected_members=expected_members or len(self.managers),
            post=self.post,
        )

    def release(
        self,
        requester: str,
        *,
        volume: str = "data",
        holder: str | None = None,
        epoch: int = 1,
    ) -> None:
        self.managers[requester].request_release(
            volume=volume,
            holder_node=holder or requester,
            epoch=epoch,
            self_node=requester,
            peer_urls=self.urls,
            post=self.post,
        )

    def holders(self, volume: str) -> dict[str, str | None]:
        return {
            node: (record.holder if (record := manager.current(volume)) is not None else None)
            for node, manager in self.managers.items()
        }


def test_grant_renew_release_round_trip_persists_majority_votes(tmp_path) -> None:
    cluster = LeaseCluster(tmp_path)

    granted = cluster.grant("A", volume="data", ttl=20.0)
    assert granted.holder == "A"
    assert granted.granted_at == 100.0
    assert granted.renew_deadline == 110.0
    assert granted.expires_at == 120.0
    assert cluster.holders("data") == {"A": "A", "B": "A", "C": "A"}

    cluster.clock.advance(4.0)
    renewed = cluster.renew("A", volume="data", ttl=20.0)
    assert renewed.holder == "A"
    assert renewed.granted_at == 104.0
    assert renewed.renew_deadline == 114.0
    assert renewed.expires_at == 124.0
    granted_at_by_node = {
        node: manager.current("data").granted_at for node, manager in cluster.managers.items()
    }
    assert granted_at_by_node == {"A": 104.0, "B": 104.0, "C": 104.0}

    cluster.release("A", volume="data")
    assert cluster.holders("data") == {"A": None, "B": None, "C": None}


def test_request_grant_uses_strict_majority_of_expected_members(tmp_path) -> None:
    cluster = LeaseCluster(tmp_path)
    only_one_peer = {"B": cluster.urls["B"]}

    granted = cluster.grant(
        "A", volume="two_votes_are_enough_for_three", expected_members=3, peer_urls=only_one_peer
    )
    assert granted.holder == "A"
    assert cluster.holders("two_votes_are_enough_for_three") == {"A": "A", "B": "A", "C": None}

    with pytest.raises(LeaseUnavailable) as excinfo:
        cluster.grant(
            "A",
            volume="two_votes_are_not_enough_for_five",
            expected_members=5,
            peer_urls=only_one_peer,
        )

    assert excinfo.value.votes == 2
    assert excinfo.value.needed == 3
    assert cluster.holders("two_votes_are_not_enough_for_five") == {
        "A": None,
        "B": None,
        "C": None,
    }


def test_conflicting_holder_cannot_receive_grant_until_full_ttl_elapsed(tmp_path) -> None:
    cluster = LeaseCluster(tmp_path)
    cluster.grant("A", volume="shared", holder="A", epoch=1, ttl=10.0)

    cluster.clock.now = 109.999
    with pytest.raises(LeaseUnavailable) as early:
        cluster.grant("B", volume="shared", holder="B", epoch=2, ttl=10.0)
    assert "conflict" in early.value.message
    assert cluster.holders("shared") == {"A": "A", "B": "A", "C": "A"}

    cluster.clock.now = 110.0
    successor = cluster.grant("B", volume="shared", holder="B", epoch=2, ttl=10.0)
    assert successor.holder == "B"
    assert successor.granted_at == 110.0
    assert cluster.holders("shared") == {"A": "B", "B": "B", "C": "B"}


def test_persisted_votes_survive_manager_restart(tmp_path) -> None:
    clock = ManualClock(50.0)
    root = tmp_path / "leases"
    first = LeaseManager(root, clock=clock)
    granted = first.vote_grant("vol", "A", 7, 30.0)
    assert granted.granted is True

    restarted = LeaseManager(root, clock=clock)
    record = restarted.current("vol")
    assert record == LeaseRecord("vol", "A", 7, 50.0, 30.0)
    assert restarted.active_votes() == [record]

    conflicting = restarted.vote_grant("vol", "B", 8, 30.0)
    assert conflicting.granted is False
    assert conflicting.reason == "conflict"
    assert restarted.current("vol") == record


def test_lower_epoch_renewal_is_fenced_after_higher_epoch_grant(tmp_path) -> None:
    cluster = LeaseCluster(tmp_path)
    cluster.grant("A", volume="epochvol", holder="A", epoch=1, ttl=10.0)
    cluster.clock.now = 110.0
    cluster.grant("B", volume="epochvol", holder="B", epoch=2, ttl=10.0)

    with pytest.raises(LeaseUnavailable) as excinfo:
        cluster.renew("A", volume="epochvol", holder="A", epoch=1, ttl=10.0)

    assert excinfo.value.votes == 0
    assert "stale_epoch" in excinfo.value.message
    assert cluster.holders("epochvol") == {"A": "B", "B": "B", "C": "B"}


class RenewalEngine:
    def __init__(self, record: SimpleNamespace, events: list[str]) -> None:
        self.record = record
        self.events = events

    def volume_lease_records(self) -> list[SimpleNamespace]:
        return [self.record]

    def writable_volume_names(self, _record: SimpleNamespace) -> list[str]:
        return ["data"]

    def stop(self, sid: str) -> dict[str, str]:
        self.events.append(f"stop:{sid}")
        return {"id": sid, "status": "stopped"}

    def clear_volume_leases(self, sid: str) -> None:
        self.events.append(f"clear:{sid}")


class RenewalSupervisor:
    def __init__(self, engine: RenewalEngine) -> None:
        self._engine = engine

    async def _run(self, func, *args):
        return func(*args)


class RenewalLeaseManager:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def request_renew(self, **_kwargs) -> LeaseRecord:
        self.events.append("renew")
        raise LeaseUnavailable("lost quorum", votes=1, needed=2)

    def request_release(self, **kwargs) -> None:
        self.events.append(f"release:{kwargs['volume']}:{kwargs['holder_node']}:{kwargs['epoch']}")


def _renewal_ctx(events: list[str]) -> SimpleNamespace:
    record = SimpleNamespace(
        name="writer",
        detail={
            "volumes": {"/data": {"name": "data", "read_only": False}},
            "volume_leases": {
                "data": {
                    "volume": "data",
                    "holder": "A",
                    "epoch": 3,
                    "granted_at": 100.0,
                    "ttl": 10.0,
                    "renew_deadline": 105.0,
                }
            },
        },
    )
    engine = RenewalEngine(record, events)
    return SimpleNamespace(
        mesh=SimpleNamespace(enabled=True, node_id="A", peers=lambda: []),
        config=SimpleNamespace(volume_lease_ttl_sec=10.0),
        lease_manager=RenewalLeaseManager(events),
        supervisor=RenewalSupervisor(engine),
    )


def test_renewal_failure_stops_writer_only_after_renew_deadline(monkeypatch) -> None:
    before_deadline: list[str] = []
    monkeypatch.setattr(server_leases.time, "time", lambda: 104.999)
    asyncio.run(server_leases.renew_writable_volume_leases_once(_renewal_ctx(before_deadline)))
    assert before_deadline == ["renew"]

    at_deadline: list[str] = []
    monkeypatch.setattr(server_leases.time, "time", lambda: 105.0)
    asyncio.run(server_leases.renew_writable_volume_leases_once(_renewal_ctx(at_deadline)))
    assert at_deadline == ["renew", "stop:writer", "release:data:A:3", "clear:writer"]
