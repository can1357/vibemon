import contextlib
import os
import platform
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Any

import pytest

from tests.cluster_harness import (
    NodeProc,
    api_json,
    drop_requests,
    eventually,
    free_port,
    heal_node,
    inject_latency,
    kill_node,
    local_sandboxes,
    mesh_join,
    mesh_setup,
    partition_node,
    pause_node,
    replica_volume_payload,
    restart_node,
    resume_node,
    sandbox_row,
    start_node,
    unwritable_replicas,
    wait_healthy,
    write_context_from_status,
)
from tests.test_invariants import (
    assert_checkpoint_age_within_bound,
    assert_idempotent_create_same_sid,
    assert_no_epoch_overlap,
    assert_volume_lease_non_overlap,
)


def _has_hypervisor() -> bool:
    """Mirror vmon.e2e/preflight's host hypervisor predicate without importing vmon."""
    system = platform.system()
    if system == "Linux":
        return Path("/dev/kvm").exists()
    if system == "Darwin":
        if platform.machine() not in ("arm64", "aarch64"):
            return False
        try:
            out = subprocess.run(
                ["sysctl", "-n", "kern.hv_support"],
                capture_output=True,
                text=True,
                check=False,
            ).stdout
        except OSError:
            return False
        return out.startswith("1")
    return False


def _skip_reason() -> str:
    if os.environ.get("VMON_CLUSTER_E2E") != "1":
        return "set VMON_CLUSTER_E2E=1 to run the real two-node cluster e2e"
    system = platform.system()
    if system not in ("Linux", "Darwin"):
        return f"needs Linux + /dev/kvm or macOS + Hypervisor.framework; this is {system}"
    if not _has_hypervisor():
        if system == "Linux":
            return "/dev/kvm not present; run on a KVM host (or a nested-KVM Lima VM)"
        return (
            "no usable hypervisor; needs an Apple-silicon Mac with "
            "Hypervisor.framework (kern.hv_support=1)"
        )
    return ""


_CLUSTER_E2E_SKIP = _skip_reason()
pytestmark = pytest.mark.skipif(bool(_CLUSTER_E2E_SKIP), reason=_CLUSTER_E2E_SKIP)


def test_two_node_failover_and_restore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from vmon.client import MeshTransport

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    home_a = Path(tempfile.mkdtemp(prefix="vce-", dir="/tmp"))
    home_b = Path(tempfile.mkdtemp(prefix="vce-", dir="/tmp"))
    gateway_a = start_node(
        home_a, free_port(), name="node-a", token=token, log_path=Path("/tmp/ce-a.log")
    )
    gateway_b = start_node(
        home_b, free_port(), name="node-b", token=token, log_path=Path("/tmp/ce-b.log")
    )
    sid = f"ce-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")

    try:
        setup = mesh_setup(gateway_a)
        join = mesh_join(gateway_b, setup["blob"])
        node_a = str(setup["node_id"])
        node_b = str(join["node_id"])
        wait_healthy([gateway_a, gateway_b])

        # Mesh setup persists membership; restart so HA reconcile/replication tasks are
        # active from lifespan startup on both real gateways.
        restart_node(gateway_b)
        restart_node(gateway_a)
        wait_healthy([gateway_a, gateway_b])

        endpoints = write_context_from_status(
            monkeypatch, tmp_path / "client-home", gateway_a.url, token
        )
        client = MeshTransport(endpoints, token)
        image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
        # Cold create (image import + first boot) can take tens of seconds; issue it
        # directly with a generous timeout instead of the short-budget client retry.
        api_json(
            "POST",
            gateway_a.url,
            "/v1/run?detach=1",
            token,
            payload={
                "image": image,
                "name": sid,
                "cmd": ["sleep", "600"],
                "block_network": True,
                "timeout": 900,
                "idempotency_key": f"{sid}-run",
            },
            timeout=300.0,
        )

        def _running_row() -> dict[str, Any] | None:
            for gw in (gateway_a, gateway_b):
                row = sandbox_row(
                    api_json("GET", gw.url, "/v1/sandboxes", token, timeout=10.0), sid
                )
                if row and row.get("status") == "running" and row.get("node"):
                    return row
            return None

        row = eventually(
            f"sandbox {sid} to be running on the cluster",
            _running_row,
            timeout=240.0,
            interval=2.0,
        )
        owner_node = str(row["node"])
        owner_gw, survivor_gw, survivor_node = (
            (gateway_a, gateway_b, node_b)
            if owner_node == node_a
            else (gateway_b, gateway_a, node_a)
        )

        # The survivor (non-owner) must hold a replica before we kill the owner.
        eventually(
            f"{survivor_node} to hold a replica for {sid}",
            lambda: (
                sid
                in api_json(
                    "GET", survivor_gw.url, "/v1/mesh/replica/list", token, timeout=5.0
                ).get("sids", [])
            ),
            timeout=90.0,
            interval=2.0,
        )

        kill_node(owner_gw)
        assert owner_gw.proc is not None and owner_gw.proc.poll() is not None, (
            "owner gateway did not stop"
        )

        # The client (both endpoints in its roster) fails over to the survivor.
        failed_over = client.call("ps")
        assert client.endpoints[0] == survivor_gw.url, (
            "MeshTransport did not promote the survivor after the owner died; "
            f"endpoints are {client.endpoints!r}, ps returned {failed_over!r}"
        )
        assert isinstance(failed_over.get("vms"), list), f"failover list returned {failed_over!r}"

        # The orphaned sandbox is restored on the survivor and takes ownership.
        restored = eventually(
            f"orphaned sandbox {sid} to restore on {survivor_node}",
            lambda: (
                r
                if (
                    r := sandbox_row(
                        api_json("GET", survivor_gw.url, "/v1/sandboxes", token, timeout=5.0),
                        sid,
                    )
                )
                and r.get("node") == survivor_node
                and r.get("status") == "running"
                else None
            ),
            timeout=120.0,
            interval=2.0,
        )
        assert restored.get("node") == survivor_node, (
            f"restored sandbox not owned by survivor: {restored!r}"
        )
    finally:
        for gateway in (gateway_b, gateway_a):
            if gateway.proc is not None and gateway.proc.poll() is None:
                with contextlib.suppress(Exception):
                    api_json(
                        "DELETE",
                        gateway.url,
                        f"/v1/sandboxes/{quoted_sid}/remove",
                        token,
                        timeout=10.0,
                    )
        gateway_a.stop()
        gateway_b.stop()
        shutil.rmtree(home_a, ignore_errors=True)
        shutil.rmtree(home_b, ignore_errors=True)


def _skip_if_no_virtiofs(exc: BaseException) -> None:
    """Mirror the SDK e2e: a guest kernel without virtio-fs registers no such
    filesystem type (ENODEV / os error 19); skip rather than hard-fail."""
    msg = str(exc).lower()
    if "os error 19" in msg or "no such device" in msg:
        pytest.skip("guest kernel lacks virtio-fs; set VMON_KERNEL to a virtio-fs-capable kernel")


def test_two_node_volume_ha(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A read-only named volume's data replicates with the checkpoint and is
    materialized + readable on the survivor after the owner dies."""
    from vmon.client import MeshTransport

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    home_a = Path(tempfile.mkdtemp(prefix="vce-", dir="/tmp"))
    home_b = Path(tempfile.mkdtemp(prefix="vce-", dir="/tmp"))
    gateway_a = start_node(
        home_a, free_port(), name="vol-a", token=token, log_path=Path("/tmp/cev-a.log")
    )
    gateway_b = start_node(
        home_b, free_port(), name="vol-b", token=token, log_path=Path("/tmp/cev-b.log")
    )
    sid = f"cev-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")
    vol_name = f"havol{os.getpid()}"
    sentinel = "HA_VOLUME_OK"

    try:
        # Seed the volume host-side on node A, then mount it read-only: the data is
        # quiescent and divergence-free across replication (no restore quorum needed).
        seeded = home_a / "volumes" / vol_name
        seeded.mkdir(parents=True)
        (seeded / "sentinel.txt").write_text(sentinel, encoding="utf-8")

        setup = mesh_setup(gateway_a)
        join = mesh_join(gateway_b, setup["blob"])
        node_a = str(setup["node_id"])
        node_b = str(join["node_id"])
        wait_healthy([gateway_a, gateway_b])
        restart_node(gateway_b)
        restart_node(gateway_a)
        wait_healthy([gateway_a, gateway_b])

        endpoints = write_context_from_status(
            monkeypatch, tmp_path / "client-home", gateway_a.url, token
        )
        client = MeshTransport(endpoints, token)
        image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
        # A volume-backed create pins local (mesh.pinned_local), so it lands on the
        # seeding node A. Cold create can take tens of seconds; use a long timeout.
        try:
            api_json(
                "POST",
                gateway_a.url,
                "/v1/sandboxes",
                token,
                payload={
                    "image": image,
                    "name": sid,
                    "block_network": True,
                    "timeout_secs": 900,
                    "volumes": {"/data": {"name": vol_name, "read_only": True}},
                    "idempotency_key": f"{sid}-create",
                },
                timeout=300.0,
            )
        except AssertionError as exc:
            _skip_if_no_virtiofs(exc)
            raise

        owner_row = eventually(
            f"volume sandbox {sid} running on node A",
            lambda: sandbox_row(
                api_json("GET", gateway_a.url, "/v1/sandboxes", token, timeout=10.0), sid
            ),
            timeout=240.0,
            interval=2.0,
        )
        assert owner_row.get("node") == node_a, f"volume sandbox not pinned to A: {owner_row!r}"

        # B must hold a replica (carrying the volume payload) before we kill A.
        eventually(
            f"{node_b} to hold a replica for {sid}",
            lambda: (
                sid
                in api_json("GET", gateway_b.url, "/v1/mesh/replica/list", token, timeout=5.0).get(
                    "sids", []
                )
            ),
            timeout=90.0,
            interval=2.0,
        )

        kill_node(gateway_a)
        assert gateway_a.proc is not None and gateway_a.proc.poll() is not None
        client.call("ps")  # promote the survivor in the client roster

        restored = eventually(
            f"orphaned sandbox {sid} to restore on {node_b}",
            lambda: (
                r
                if (
                    r := sandbox_row(
                        api_json("GET", gateway_b.url, "/v1/sandboxes", token, timeout=5.0), sid
                    )
                )
                and r.get("node") == node_b
                and r.get("status") == "running"
                else None
            ),
            timeout=120.0,
            interval=2.0,
        )
        assert restored.get("node") == node_b

        # Proof 1: the volume DATA travelled A -> checkpoint -> B and materialized on
        # the survivor's host (the new replicate/capture/materialize path).
        materialized = home_b / "volumes" / vol_name / "sentinel.txt"
        got = eventually(
            "volume data materialized on the survivor host",
            lambda: materialized.read_text(encoding="utf-8") if materialized.is_file() else None,
            timeout=30.0,
            interval=1.0,
        )
        assert got == sentinel, f"materialized volume mismatch: {got!r}"

        # Proof 2: the restored guest mounts the read-only volume and reads it back.
        chunks: list[bytes] = []
        try:
            client.stream(
                "exec",
                lambda _stream, data: chunks.append(data),
                name=sid,
                cmd=["cat", "/data/sentinel.txt"],
                timeout=30,
            )
        except Exception as exc:
            _skip_if_no_virtiofs(exc)
            raise
        guest_out = b"".join(chunks).decode("utf-8", errors="replace")
        assert sentinel in guest_out, f"restored guest did not read volume data: {guest_out!r}"
    finally:
        for gateway in (gateway_b, gateway_a):
            if gateway.proc is not None and gateway.proc.poll() is None:
                with contextlib.suppress(Exception):
                    api_json(
                        "DELETE",
                        gateway.url,
                        f"/v1/sandboxes/{quoted_sid}/remove",
                        token,
                        timeout=10.0,
                    )
        gateway_a.stop()
        gateway_b.stop()
        shutil.rmtree(home_a, ignore_errors=True)
        shutil.rmtree(home_b, ignore_errors=True)


def test_three_node_writable_volume_quorum_ha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With VMON_RESTORE_QUORUM on a 3-node cluster, a guest's writes to a *writable*
    named volume survive the owner's death: a surviving majority confirms the owner
    gone, restores the sandbox, materializes the writable volume, and the restored
    guest reads back its own written data."""
    from vmon.client import MeshTransport

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    homes = [Path(tempfile.mkdtemp(prefix="vce-", dir="/tmp")) for _ in range(3)]
    quorum_env = {"VMON_RESTORE_QUORUM": "1", "VMON_REPLICAS": "2"}
    gws = [
        start_node(
            homes[i],
            free_port(),
            quorum_env,
            name=f"q-{label}",
            token=token,
            log_path=Path(f"/tmp/ceq-{label}.log"),
        )
        for i, label in enumerate("abc")
    ]
    sid = f"ceq-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")
    vol_name = f"rwvol{os.getpid()}"
    sentinel = "HA_RW_OK"

    try:
        setup = mesh_setup(gws[0])
        node_ids = [str(setup["node_id"])]
        for gw in gws[1:]:
            join = mesh_join(gw, setup["blob"])
            node_ids.append(str(join["node_id"]))
        wait_healthy(gws)
        for gw in gws:
            restart_node(gw)
        wait_healthy(gws)

        endpoints = write_context_from_status(
            monkeypatch, tmp_path / "client-home", gws[0].url, token
        )
        client = MeshTransport(endpoints, token)
        image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
        # Writable volume, created on node A (volume-backed create pins local).
        try:
            api_json(
                "POST",
                gws[0].url,
                "/v1/sandboxes",
                token,
                payload={
                    "image": image,
                    "name": sid,
                    "block_network": True,
                    "timeout_secs": 900,
                    "volumes": {"/data": {"name": vol_name, "read_only": False}},
                    "idempotency_key": f"{sid}-create",
                },
                timeout=300.0,
            )
        except AssertionError as exc:
            _skip_if_no_virtiofs(exc)
            raise

        eventually(
            f"writable volume sandbox {sid} running on node A",
            lambda: sandbox_row(
                api_json("GET", gws[0].url, "/v1/sandboxes", token, timeout=10.0), sid
            ),
            timeout=240.0,
            interval=2.0,
        )

        # The guest writes into the writable volume; sync so the host dir is durable
        # before the next replication round captures it.
        try:
            client.stream(
                "exec",
                lambda _stream, _data: None,
                name=sid,
                cmd=["sh", "-c", f"echo {sentinel} > /data/written.txt && sync"],
                timeout=30,
            )
        except Exception as exc:
            _skip_if_no_virtiofs(exc)
            raise

        # Wait until BOTH survivors hold a replica whose checkpoint carries the
        # post-write volume payload (deterministic: proves the updated checkpoint
        # landed on each holder before we kill the owner).
        survivors = [(gws[i], node_ids[i], homes[i]) for i in (1, 2)]
        for _gw, nid, home in survivors:

            def replica_has_payload(home: Path = home) -> bool:
                return replica_volume_payload(home, sid, vol_name, "written.txt") == sentinel

            eventually(
                f"{nid}'s replica of {sid} to carry the post-write volume payload",
                replica_has_payload,
                timeout=120.0,
                interval=2.0,
            )

        kill_node(gws[0])
        assert gws[0].proc is not None and gws[0].proc.poll() is not None
        client.call("ps")  # promote a survivor in the client roster

        # A surviving majority (2 of 3) confirms A gone and restores the sandbox.
        restored = eventually(
            f"orphaned sandbox {sid} to restore on a survivor",
            lambda: next(
                (
                    (gw, nid, r)
                    for gw, nid, _home in survivors
                    if (
                        r := sandbox_row(
                            api_json("GET", gw.url, "/v1/sandboxes", token, timeout=5.0), sid
                        )
                    )
                    and r.get("node") == nid
                    and r.get("status") == "running"
                ),
                None,
            ),
            timeout=150.0,
            interval=2.0,
        )
        restorer_gw, restorer_id, _ = restored
        restorer_home = homes[node_ids.index(restorer_id)]

        # Proof 1: the writable volume materialized on the restorer's host with the
        # data the guest wrote before the crash.
        materialized = restorer_home / "volumes" / vol_name / "written.txt"
        got = eventually(
            "written volume data materialized on the restorer host",
            lambda: (
                materialized.read_text(encoding="utf-8").strip() if materialized.is_file() else None
            ),
            timeout=30.0,
            interval=1.0,
        )
        assert got == sentinel, f"materialized writable volume mismatch: {got!r}"

        # Proof 2: the restored guest re-mounts the writable volume and reads its
        # own pre-crash write back.
        chunks: list[bytes] = []
        client.stream(
            "exec",
            lambda _stream, data: chunks.append(data),
            name=sid,
            cmd=["cat", "/data/written.txt"],
            timeout=30,
        )
        guest_out = b"".join(chunks).decode("utf-8", errors="replace")
        assert sentinel in guest_out, f"restored guest did not read written data: {guest_out!r}"
    finally:
        for gw in reversed(gws):
            if gw.proc is not None and gw.proc.poll() is None:
                with contextlib.suppress(Exception):
                    api_json(
                        "DELETE",
                        gw.url,
                        f"/v1/sandboxes/{quoted_sid}/remove",
                        token,
                        timeout=10.0,
                    )
        for gw in gws:
            gw.stop()
        for home in homes:
            shutil.rmtree(home, ignore_errors=True)


def _start_fault_pair(
    token: str,
    env_overrides: dict[str, str] | None = None,
    *,
    prefix: str,
) -> tuple[list[Path], list[NodeProc], list[str]]:
    homes = [Path(tempfile.mkdtemp(prefix="vcf-", dir="/tmp")) for _ in range(2)]
    gateways = [
        start_node(
            homes[i],
            free_port(),
            env_overrides,
            name=f"{prefix}-{label}",
            token=token,
            log_path=Path(f"/tmp/{prefix}-{label}.log"),
            fault_proxy=True,
        )
        for i, label in enumerate("ab")
    ]
    setup = mesh_setup(gateways[0])
    join = mesh_join(gateways[1], setup["blob"])
    node_ids = [str(setup["node_id"]), str(join["node_id"])]
    wait_healthy(gateways)
    for gateway in reversed(gateways):
        restart_node(gateway)
    wait_healthy(gateways)
    return homes, gateways, node_ids


def test_fault_proxy_partition_diskfull_and_invariants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Process-level proxy faults exercise the real gateway/engine path.

    This covers drop-N-then-heal at client ingress, delayed heartbeat relaying,
    disk-full replica receive, and a full-node partition where the owner process
    stays alive but peers/clients cannot route to it.
    """

    from vmon.client import MeshTransport

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    homes, gateways, node_ids = _start_fault_pair(
        token,
        {"VMON_REPLICATE_SEC": "1"},
        prefix="fault-proxy",
    )
    gateway_a, gateway_b = gateways
    home_a = homes[0]
    home_b = homes[1]
    node_a, node_b = node_ids
    vol_name = f"faultvol{os.getpid()}"
    seeded = home_a / "volumes" / vol_name
    seeded.mkdir(parents=True)
    (seeded / "sentinel.txt").write_text("FAULT_OK", encoding="utf-8")
    sid = f"fault-{os.getpid()}"
    disk_sid = f"fault-disk-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")
    quoted_disk_sid = urllib.parse.quote(disk_sid, safe="")
    image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
    payload = {
        "image": image,
        "name": sid,
        "block_network": True,
        "timeout_secs": 900,
        "volumes": {"/data": {"name": vol_name, "read_only": True}},
    }
    disk_payload = {
        **payload,
        "name": disk_sid,
        "idempotency_key": f"{disk_sid}-create",
    }

    try:
        inject_latency(gateway_a, 0.1)
        inject_latency(gateway_b, 0.1)
        wait_healthy(gateways)
        inject_latency(gateway_a, 0.0)
        inject_latency(gateway_b, 0.0)

        drop_requests(gateway_a, paths=["/v1/sandboxes"])
        with pytest.raises(AssertionError):
            api_json(
                "POST", gateway_a.url, "/v1/sandboxes", token, payload=disk_payload, timeout=30.0
            )

        with unwritable_replicas(gateway_b):
            disk_created = api_json(
                "POST",
                gateway_a.url,
                "/v1/sandboxes",
                token,
                payload=disk_payload,
                timeout=300.0,
            )
            assert_idempotent_create_same_sid([disk_created])
            time.sleep(2.5)
            assert disk_sid not in api_json(
                "GET", gateway_b.url, "/v1/mesh/replica/list", token, timeout=5.0
            ).get("sids", [])
        with contextlib.suppress(Exception):
            api_json(
                "DELETE",
                gateway_a.url,
                f"/v1/sandboxes/{quoted_disk_sid}/remove",
                token,
                timeout=10.0,
            )

        create_started = time.time()
        created = api_json(
            "POST",
            gateway_a.url,
            "/v1/sandboxes",
            token,
            payload=payload,
            timeout=300.0,
        )
        assert_idempotent_create_same_sid([created])
        eventually(
            f"{node_b} to hold a replica for {sid}",
            lambda: (
                sid
                in api_json("GET", gateway_b.url, "/v1/mesh/replica/list", token, timeout=5.0).get(
                    "sids", []
                )
            ),
            timeout=90.0,
            interval=1.0,
        )
        meta_path = home_b / "replicas" / f"{sid}.json"
        assert_checkpoint_age_within_bound(
            [{"sid": sid, "checkpointed_at": meta_path.stat().st_mtime}],
            now=time.time(),
            cadence=1.0,
            push_bound=90.0,
        )

        endpoints = write_context_from_status(
            monkeypatch, tmp_path / "fault-client-home", gateway_b.url, token
        )
        client = MeshTransport(endpoints, token)
        assert client.call("ps").get("vms") is not None

        partition_node(gateway_a, gateways)
        pause_node(gateway_a)
        restored = eventually(
            f"partitioned owner {node_a} to be restored on {node_b}",
            lambda: (
                row
                if (
                    row := sandbox_row(
                        api_json("GET", gateway_b.backend_url, "/v1/sandboxes", token, timeout=5.0),
                        sid,
                    )
                )
                and row.get("node") == node_b
                and row.get("status") == "running"
                else None
            ),
            timeout=150.0,
            interval=2.0,
        )
        restored_at = time.time()
        resume_node(gateway_a)
        heal_node(gateway_a, gateways)
        wait_healthy(gateways)
        eventually(
            f"superseded owner {node_a} to fence {sid} after rejoin",
            lambda: sandbox_row(local_sandboxes(gateway_a, token), sid) is None,
            timeout=60.0,
            interval=1.0,
        )
        status = api_json("GET", gateway_a.url, "/v1/mesh/status", token, timeout=5.0)
        fence_count = int(((status.get("self") or {}).get("stats") or {}).get("fence") or 0)
        assert fence_count >= 1, f"expected a fence event on {node_a}, got {status!r}"
        assert restored.get("node") == node_b
        assert_no_epoch_overlap(
            [
                {
                    "sid": sid,
                    "epoch": 0,
                    "owner": node_a,
                    "start": create_started,
                    "end": restored_at,
                },
                {"sid": sid, "epoch": 1, "owner": node_b, "start": restored_at, "end": time.time()},
            ]
        )
    finally:
        with contextlib.suppress(Exception):
            resume_node(gateway_a)
        heal_node(gateway_a, gateways)
        for gateway in reversed(gateways):
            if gateway.proc is not None and gateway.proc.poll() is None:
                for quoted in (quoted_sid, quoted_disk_sid):
                    with contextlib.suppress(Exception):
                        api_json(
                            "DELETE",
                            gateway.url,
                            f"/v1/sandboxes/{quoted}/remove",
                            token,
                            timeout=10.0,
                        )
        for gateway in gateways:
            gateway.stop()
        for home in homes:
            shutil.rmtree(home, ignore_errors=True)


def test_fault_proxy_sigkill_owner_mid_create_retries_once() -> None:
    """A gateway killed during create leaves one live sandbox after retry."""

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    homes, gateways, _node_ids = _start_fault_pair(token, prefix="fault-kill")
    gateway_a, gateway_b = gateways
    sid = f"kill-create-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")
    payload = {
        "image": os.environ.get("VMON_E2E_IMAGE", "alpine:latest"),
        "name": sid,
        "block_network": True,
        "timeout_secs": 900,
        "idempotency_key": f"{sid}-create",
    }

    assert gateway_a.proxy is not None
    gateway_a.proxy.on_forward_once(
        lambda: kill_node(gateway_a),
        prefixes=["/v1/sandboxes"],
    )

    try:
        first: dict[str, Any] | None = None
        with contextlib.suppress(AssertionError):
            first = api_json(
                "POST",
                gateway_a.url,
                "/v1/sandboxes",
                token,
                payload=payload,
                timeout=300.0,
            )
        eventually(
            "owner gateway to die during create",
            lambda: gateway_a.proc is not None and gateway_a.proc.poll() is not None,
            timeout=30.0,
            interval=0.25,
        )

        retry = api_json(
            "POST",
            gateway_b.url,
            "/v1/sandboxes",
            token,
            payload=payload,
            timeout=300.0,
        )
        assert_idempotent_create_same_sid([response for response in (first, retry) if response])
        listing = api_json("GET", gateway_b.url, "/v1/sandboxes", token, timeout=10.0)
        rows = [
            row
            for row in listing.get("sandboxes", [])
            if isinstance(row, dict) and (row.get("id") == sid or row.get("name") == sid)
        ]
        assert len(rows) == 1, f"expected exactly one retried sandbox, got {rows!r}"
    finally:
        for gateway in reversed(gateways):
            if gateway.proc is not None and gateway.proc.poll() is None:
                with contextlib.suppress(Exception):
                    api_json(
                        "DELETE",
                        gateway.url,
                        f"/v1/sandboxes/{quoted_sid}/remove",
                        token,
                        timeout=10.0,
                    )
        for gateway in gateways:
            gateway.stop()
        for home in homes:
            shutil.rmtree(home, ignore_errors=True)


def test_macos_user_net_failover_restores_gateway_reachability() -> None:
    """A default macOS user-net sandbox remains gateway-reachable after HA restore."""

    if platform.system() != "Darwin":
        pytest.skip("macOS user-net failover only runs on Darwin")

    import json
    import urllib.request

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    home_a = Path(tempfile.mkdtemp(prefix="vce-usernet-a-", dir="/tmp"))
    home_b = Path(tempfile.mkdtemp(prefix="vce-usernet-b-", dir="/tmp"))
    gateway_a = start_node(
        home_a,
        free_port(),
        name="node-usernet-a",
        token=token,
        log_path=Path("/tmp/ce-usernet-a.log"),
    )
    gateway_b = start_node(
        home_b,
        free_port(),
        name="node-usernet-b",
        token=token,
        log_path=Path("/tmp/ce-usernet-b.log"),
    )
    sid = f"usernet-ha-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")

    def exec_gateway_probe(gateway: NodeProc) -> list[dict[str, Any]]:
        payload = {
            "cmd": [
                "/bin/sh",
                "-c",
                "ping -c 1 -W 2 10.0.2.2",
            ],
            "timeout": 30,
        }
        request = urllib.request.Request(
            gateway.url.rstrip("/") + f"/v1/sandboxes/{quoted_sid}/exec",
            data=json.dumps(payload).encode(),
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=60.0) as response:
            raw = response.read().decode("utf-8", errors="replace")
        events: list[dict[str, Any]] = []
        for line in raw.splitlines():
            if line.startswith("data: "):
                event = json.loads(line.removeprefix("data: "))
                assert isinstance(event, dict), event
                events.append(event)
        return events

    try:
        setup = mesh_setup(gateway_a)
        join = mesh_join(gateway_b, setup["blob"])
        node_a = str(setup["node_id"])
        node_b = str(join["node_id"])
        wait_healthy([gateway_a, gateway_b])

        restart_node(gateway_b)
        restart_node(gateway_a)
        wait_healthy([gateway_a, gateway_b])

        api_json(
            "POST",
            gateway_a.url,
            "/v1/sandboxes",
            token,
            payload={
                "image": os.environ.get("VMON_E2E_IMAGE", "alpine:latest"),
                "name": sid,
                "block_network": False,
                "timeout_secs": 900,
                "idempotency_key": f"{sid}-create",
            },
            timeout=300.0,
        )

        def running_row() -> dict[str, Any] | None:
            for gw in (gateway_a, gateway_b):
                row = sandbox_row(
                    api_json("GET", gw.url, "/v1/sandboxes", token, timeout=10.0),
                    sid,
                )
                if row and row.get("status") == "running" and row.get("node"):
                    return row
            return None

        row = eventually(
            f"user-net sandbox {sid} to be running on the cluster",
            running_row,
            timeout=240.0,
            interval=2.0,
        )
        owner_node = str(row["node"])
        owner_gw, survivor_gw, survivor_node = (
            (gateway_a, gateway_b, node_b)
            if owner_node == node_a
            else (gateway_b, gateway_a, node_a)
        )

        eventually(
            f"{survivor_node} to hold a user-net replica for {sid}",
            lambda: (
                sid
                in api_json(
                    "GET",
                    survivor_gw.url,
                    "/v1/mesh/replica/list",
                    token,
                    timeout=5.0,
                ).get("sids", [])
            ),
            timeout=90.0,
            interval=2.0,
        )

        kill_node(owner_gw)
        assert owner_gw.proc is not None and owner_gw.proc.poll() is not None, (
            "owner gateway did not stop"
        )

        restored = eventually(
            f"user-net sandbox {sid} to restore on {survivor_node}",
            lambda: (
                r
                if (
                    r := sandbox_row(
                        api_json(
                            "GET",
                            survivor_gw.url,
                            "/v1/sandboxes",
                            token,
                            timeout=5.0,
                        ),
                        sid,
                    )
                )
                and r.get("node") == survivor_node
                and r.get("status") == "running"
                else None
            ),
            timeout=120.0,
            interval=2.0,
        )
        assert restored.get("node") == survivor_node, (
            f"restored user-net sandbox not owned by survivor: {restored!r}"
        )

        events = exec_gateway_probe(survivor_gw)
        assert any(event.get("exit") == 0 for event in events), events
    finally:
        for gateway in (gateway_b, gateway_a):
            if gateway.proc is not None and gateway.proc.poll() is None:
                with contextlib.suppress(Exception):
                    api_json(
                        "DELETE",
                        gateway.url,
                        f"/v1/sandboxes/{quoted_sid}/remove",
                        token,
                        timeout=10.0,
                    )
        gateway_a.stop()
        gateway_b.stop()
        shutil.rmtree(home_a, ignore_errors=True)
        shutil.rmtree(home_b, ignore_errors=True)


def test_fault_proxy_writable_volume_lease_handoff_has_no_active_overlap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A partitioned writable-volume owner stops before a survivor activates its lease.

    This is gated by the module-level VMON_CLUSTER_E2E skip. It intentionally uses
    process fault proxies rather than in-process fakes: the old owner must lose
    quorum renewals, stop its writer after the renew deadline, and only then may
    the surviving majority restore the sandbox with a successor volume lease.
    """

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    homes = [Path(tempfile.mkdtemp(prefix="vcl-", dir="/tmp")) for _ in range(3)]
    env = {"VMON_RESTORE_QUORUM": "1", "VMON_REPLICAS": "2", "VMON_REPLICATE_SEC": "1"}
    gateways = [
        start_node(
            homes[i],
            free_port(),
            env,
            name=f"lease-{label}",
            token=token,
            log_path=Path(f"/tmp/lease-{label}.log"),
            fault_proxy=True,
        )
        for i, label in enumerate("abc")
    ]
    owner_gw = gateways[0]
    sid = f"lease-handoff-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")
    vol_name = f"leasevol{os.getpid()}"
    image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")

    def volume_lease(row: dict[str, Any]) -> dict[str, Any]:
        leases = row.get("volume_leases")
        assert isinstance(leases, dict), f"missing volume leases in row: {row!r}"
        lease = leases.get(vol_name)
        assert isinstance(lease, dict), f"missing lease for {vol_name!r}: {leases!r}"
        return lease

    try:
        setup = mesh_setup(owner_gw)
        node_ids = [str(setup["node_id"])]
        for gateway in gateways[1:]:
            joined = mesh_join(gateway, setup["blob"])
            node_ids.append(str(joined["node_id"]))
        wait_healthy(gateways)
        for gateway in gateways:
            restart_node(gateway)
        wait_healthy(gateways)

        created = api_json(
            "POST",
            owner_gw.url,
            "/v1/sandboxes",
            token,
            payload={
                "image": image,
                "name": sid,
                "block_network": True,
                "timeout_secs": 900,
                "volumes": {"/data": {"name": vol_name, "read_only": False}},
                "idempotency_key": f"{sid}-create",
            },
            timeout=300.0,
        )
        first_lease = volume_lease(created)
        eventually(
            f"both survivors to hold a replica for {sid}",
            lambda: all(
                sid
                in api_json(
                    "GET",
                    gateway.url,
                    "/v1/mesh/replica/list",
                    token,
                    timeout=5.0,
                ).get("sids", [])
                for gateway in gateways[1:]
            ),
            timeout=120.0,
            interval=2.0,
        )

        # First isolate every proxy so the owner cannot renew on survivors. Once
        # it has stopped its local writer, reopen the survivor proxies and leave
        # the old owner's proxy dark; the surviving two nodes can then form the
        # strict majority required for the successor lease.
        partition_node(owner_gw, gateways)
        owner_stopped = eventually(
            f"partitioned writable-volume owner {node_ids[0]} to stop {sid}",
            lambda: (
                row
                if (
                    row := sandbox_row(
                        api_json(
                            "GET",
                            owner_gw.backend_url,
                            "/v1/sandboxes",
                            token,
                            timeout=5.0,
                            headers={"X-Vmon-Mesh-Hop": "1"},
                        ),
                        sid,
                    )
                )
                and row.get("status") != "running"
                else None
            ),
            timeout=120.0,
            interval=2.0,
        )
        assert owner_stopped.get("status") == "stopped", owner_stopped

        for gateway in gateways[1:]:
            assert gateway.proxy is not None
            gateway.proxy.partition(False)
            gateway.proxy.block_heartbeat_source(node_ids[0], True)

        restored = eventually(
            f"surviving majority to restore {sid} with a successor lease",
            lambda: next(
                (
                    row
                    for gateway, node_id in zip(gateways[1:], node_ids[1:], strict=True)
                    if (
                        row := sandbox_row(
                            api_json(
                                "GET",
                                gateway.backend_url,
                                "/v1/sandboxes",
                                token,
                                timeout=5.0,
                                headers={"X-Vmon-Mesh-Hop": "1"},
                            ),
                            sid,
                        )
                    )
                    and row.get("node") == node_id
                    and row.get("status") == "running"
                    and isinstance((row.get("volume_leases") or {}).get(vol_name), dict)
                ),
                None,
            ),
            timeout=180.0,
            interval=2.0,
        )
        successor_lease = volume_lease(restored)
        assert first_lease["renew_deadline"] <= successor_lease["granted_at"], (
            "successor lease activated before the old owner reached its stop deadline: "
            f"old={first_lease!r}, successor={successor_lease!r}"
        )
        assert_volume_lease_non_overlap([first_lease, successor_lease])
    finally:
        with contextlib.suppress(Exception):
            heal_node(owner_gw, gateways)
        for gateway in reversed(gateways):
            if gateway.proc is not None and gateway.proc.poll() is None:
                with contextlib.suppress(Exception):
                    api_json(
                        "DELETE",
                        gateway.url,
                        f"/v1/sandboxes/{quoted_sid}/remove",
                        token,
                        timeout=10.0,
                    )
        for gateway in gateways:
            gateway.stop()
        for home in homes:
            shutil.rmtree(home, ignore_errors=True)
