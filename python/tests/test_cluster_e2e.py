import contextlib
import json
import os
import platform
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest


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


_REPO_ROOT = Path(__file__).resolve().parents[2]
_PYTHON_ROOT = _REPO_ROOT / "python"


def _tail(path: Path, limit: int = 12_000) -> str:
    try:
        data = path.read_bytes()[-limit:]
    except OSError as exc:
        return f"<cannot read {path}: {exc}>"
    return data.decode("utf-8", errors="replace")


def _api_json(
    method: str,
    base_url: str,
    path: str,
    token: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"{method} {path} failed with HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise AssertionError(f"{method} {path} could not reach {base_url}: {exc.reason}") from exc
    if not raw:
        return {}
    parsed = json.loads(raw)
    assert isinstance(parsed, dict), f"{method} {path} returned non-object JSON: {parsed!r}"
    return parsed


def _eventually(
    description: str,
    fn: Callable[[], Any],
    *,
    timeout: float,
    interval: float = 0.5,
) -> Any:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = fn()
        except Exception as exc:  # keep polling across transient connection failures
            last = exc
        else:
            if value:
                return value
            last = None
        time.sleep(interval)
    detail = f"; last error: {last!r}" if last is not None else ""
    raise AssertionError(f"timed out waiting for {description}{detail}")


def _healthz(url: str) -> bool:
    with urllib.request.urlopen(url.rstrip("/") + "/healthz", timeout=2.0) as response:
        return response.status == 200


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class GatewayProcess:
    name: str
    port: int
    home: Path
    token: str
    log_path: Path
    proc: subprocess.Popen[bytes] | None = None
    _log: Any | None = None
    extra_env: dict[str, str] = field(default_factory=dict)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        assert self.proc is None or self.proc.poll() is not None, f"{self.name} already running"
        self.home.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.update(
            {
                "PYTHONUNBUFFERED": "1",
                "VMON_API_TOKEN": self.token,
                "VMON_HOME": str(self.home),
                "VMON_REPLICAS": "1",
                "VMON_REPLICATE_SEC": "5",
                # The gated e2e should not wait for the production five-minute reap window.
                "VMON_MESH_HEARTBEAT_SEC": "0.5",
                "VMON_MESH_REAP_SEC": "2.0",
                # Make the ingress gateway win normal fresh placements deterministically.
                "VMON_MESH_W_LOCAL": "1000000",
            }
        )
        env.update(self.extra_env)
        old_pythonpath = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            str(_PYTHON_ROOT)
            if not old_pythonpath
            else f"{_PYTHON_ROOT}{os.pathsep}{old_pythonpath}"
        )
        self._log = self.log_path.open("ab")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "vmon",
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--token",
                self.token,
                "--idle-timeout",
                "900",
            ],
            cwd=_REPO_ROOT,
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        _eventually(
            f"{self.name} healthz on {self.url}",
            self._healthy_or_exited,
            timeout=60.0,
            interval=0.25,
        )

    def _healthy_or_exited(self) -> bool:
        assert self.proc is not None
        rc = self.proc.poll()
        if rc is not None:
            raise AssertionError(f"{self.name} exited with {rc}; log tail:\n{_tail(self.log_path)}")
        return _healthz(self.url)

    def stop(self, *, timeout: float = 20.0) -> None:
        proc = self.proc
        if proc is not None and proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=timeout)
        if self._log is not None:
            self._log.close()
            self._log = None

    def restart(self) -> None:
        self.stop()
        self.start()


def _peer_healthy(status: dict[str, Any], peer_id: str) -> bool:
    return any(
        peer.get("node_id") == peer_id and peer.get("healthy") is True
        for peer in status.get("peers") or []
        if isinstance(peer, dict)
    )


def _wait_mesh_pair(
    gateway_a: GatewayProcess,
    gateway_b: GatewayProcess,
    token: str,
    node_a: str,
    node_b: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    def ready() -> tuple[dict[str, Any], dict[str, Any]] | None:
        status_a = _api_json("GET", gateway_a.url, "/v1/mesh/status", token, timeout=5.0)
        status_b = _api_json("GET", gateway_b.url, "/v1/mesh/status", token, timeout=5.0)
        if (
            status_a.get("enabled") is True
            and status_b.get("enabled") is True
            and _peer_healthy(status_a, node_b)
            and _peer_healthy(status_b, node_a)
        ):
            return status_a, status_b
        return None

    return _eventually("both mesh nodes to see each other healthy", ready, timeout=30.0)


def _sandbox_row(listing: dict[str, Any], sid: str) -> dict[str, Any] | None:
    for item in listing.get("sandboxes") or []:
        if not isinstance(item, dict):
            continue
        if item.get("id") == sid or item.get("name") == sid:
            return item
    return None


def _write_context_from_status(
    monkeypatch: pytest.MonkeyPatch,
    client_home: Path,
    seed_url: str,
    token: str,
) -> list[str]:
    monkeypatch.setenv("VMON_HOME", str(client_home))
    from vmon.context import Context, ContextStore, roster_from_status

    status = _api_json("GET", seed_url, "/v1/mesh/status", token)
    endpoints = roster_from_status(status, fallback=seed_url)
    assert seed_url in endpoints, f"context roster should include seed {seed_url}: {endpoints!r}"
    store = ContextStore()
    store.load()
    store.put(Context(name="cluster-e2e", endpoints=endpoints, updated=time.time()))
    store.use("cluster-e2e")
    reloaded = ContextStore()
    reloaded.load()
    current = reloaded.current()
    assert current is not None, "cluster context was not persisted"
    assert current.endpoints == endpoints, "persisted cluster context lost the mesh roster"
    return endpoints


def test_two_node_failover_and_restore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from vmon.client import MeshClient

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    home_a = Path(tempfile.mkdtemp(prefix="vce-"))
    home_b = Path(tempfile.mkdtemp(prefix="vce-"))
    gateway_a = GatewayProcess("node-a", _free_port(), home_a, token, Path("/tmp/ce-a.log"))
    gateway_b = GatewayProcess("node-b", _free_port(), home_b, token, Path("/tmp/ce-b.log"))
    sid = f"ce-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")

    try:
        gateway_a.start()
        gateway_b.start()

        setup = _api_json(
            "POST",
            gateway_a.url,
            "/v1/mesh/setup",
            token,
            {"advertise": gateway_a.url, "max_vcpus": 64, "max_mem_mib": 65_536},
        )
        join = _api_json(
            "POST",
            gateway_b.url,
            "/v1/mesh/join",
            token,
            {"blob": setup["blob"], "advertise": gateway_b.url},
        )
        node_a = str(setup["node_id"])
        node_b = str(join["node_id"])
        _wait_mesh_pair(gateway_a, gateway_b, token, node_a, node_b)

        # Mesh setup persists membership; restart so HA reconcile/replication tasks are
        # active from lifespan startup on both real gateways.
        gateway_b.restart()
        gateway_a.restart()
        _wait_mesh_pair(gateway_a, gateway_b, token, node_a, node_b)

        endpoints = _write_context_from_status(
            monkeypatch, tmp_path / "client-home", gateway_a.url, token
        )
        client = MeshClient(endpoints, token)
        image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
        # Cold create (image import + first boot) can take tens of seconds; issue it
        # directly with a generous timeout instead of the short-budget client retry.
        _api_json(
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
                row = _sandbox_row(
                    _api_json("GET", gw.url, "/v1/sandboxes", token, timeout=10.0), sid
                )
                if row and row.get("status") == "running" and row.get("node"):
                    return row
            return None

        row = _eventually(
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
        _eventually(
            f"{survivor_node} to hold a replica for {sid}",
            lambda: (
                sid
                in _api_json(
                    "GET", survivor_gw.url, "/v1/mesh/replica/list", token, timeout=5.0
                ).get("sids", [])
            ),
            timeout=90.0,
            interval=2.0,
        )

        owner_gw.stop()
        assert owner_gw.proc is not None and owner_gw.proc.poll() is not None, (
            "owner gateway did not stop"
        )

        # The client (both endpoints in its roster) fails over to the survivor.
        failed_over = client.call("ps")
        assert client.endpoints[0] == survivor_gw.url, (
            "MeshClient did not promote the survivor after the owner died; "
            f"endpoints are {client.endpoints!r}, ps returned {failed_over!r}"
        )
        assert isinstance(failed_over.get("vms"), list), f"failover list returned {failed_over!r}"

        # The orphaned sandbox is restored on the survivor and takes ownership.
        restored = _eventually(
            f"orphaned sandbox {sid} to restore on {survivor_node}",
            lambda: (
                r
                if (
                    r := _sandbox_row(
                        _api_json("GET", survivor_gw.url, "/v1/sandboxes", token, timeout=5.0),
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
                    _api_json(
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
    from vmon.client import MeshClient

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    home_a = Path(tempfile.mkdtemp(prefix="vce-"))
    home_b = Path(tempfile.mkdtemp(prefix="vce-"))
    gateway_a = GatewayProcess("vol-a", _free_port(), home_a, token, Path("/tmp/cev-a.log"))
    gateway_b = GatewayProcess("vol-b", _free_port(), home_b, token, Path("/tmp/cev-b.log"))
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

        gateway_a.start()
        gateway_b.start()
        setup = _api_json(
            "POST",
            gateway_a.url,
            "/v1/mesh/setup",
            token,
            {"advertise": gateway_a.url, "max_vcpus": 64, "max_mem_mib": 65_536},
        )
        join = _api_json(
            "POST",
            gateway_b.url,
            "/v1/mesh/join",
            token,
            {"blob": setup["blob"], "advertise": gateway_b.url},
        )
        node_a = str(setup["node_id"])
        node_b = str(join["node_id"])
        _wait_mesh_pair(gateway_a, gateway_b, token, node_a, node_b)
        gateway_b.restart()
        gateway_a.restart()
        _wait_mesh_pair(gateway_a, gateway_b, token, node_a, node_b)

        endpoints = _write_context_from_status(
            monkeypatch, tmp_path / "client-home", gateway_a.url, token
        )
        client = MeshClient(endpoints, token)
        image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
        # A volume-backed create pins local (mesh.pinned_local), so it lands on the
        # seeding node A. Cold create can take tens of seconds; use a long timeout.
        try:
            _api_json(
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

        owner_row = _eventually(
            f"volume sandbox {sid} running on node A",
            lambda: _sandbox_row(
                _api_json("GET", gateway_a.url, "/v1/sandboxes", token, timeout=10.0), sid
            ),
            timeout=240.0,
            interval=2.0,
        )
        assert owner_row.get("node") == node_a, f"volume sandbox not pinned to A: {owner_row!r}"

        # B must hold a replica (carrying the volume payload) before we kill A.
        _eventually(
            f"{node_b} to hold a replica for {sid}",
            lambda: (
                sid
                in _api_json("GET", gateway_b.url, "/v1/mesh/replica/list", token, timeout=5.0).get(
                    "sids", []
                )
            ),
            timeout=90.0,
            interval=2.0,
        )

        gateway_a.stop()
        assert gateway_a.proc is not None and gateway_a.proc.poll() is not None
        client.call("ps")  # promote the survivor in the client roster

        restored = _eventually(
            f"orphaned sandbox {sid} to restore on {node_b}",
            lambda: (
                r
                if (
                    r := _sandbox_row(
                        _api_json("GET", gateway_b.url, "/v1/sandboxes", token, timeout=5.0), sid
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
        got = _eventually(
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
                    _api_json(
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


def _replica_volume_payload(home: Path, sid: str, vol: str, fname: str) -> str | None:
    """Return the stripped contents of a survivor's replica volume file, if present.

    Reads ``<home>/replicas/<sid>.json`` -> ``snapshot_dir/volumes/<vol>/<fname>`` so
    the test can wait until the post-write checkpoint actually landed on a holder.
    """
    meta_path = home / "replicas" / f"{sid}.json"
    if not meta_path.is_file():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return None
    snapshot_dir = meta.get("snapshot_dir")
    if not isinstance(snapshot_dir, str):
        return None
    payload = Path(snapshot_dir) / "volumes" / vol / fname
    return payload.read_text(encoding="utf-8").strip() if payload.is_file() else None


def _wait_mesh_trio(pairs: list[tuple[GatewayProcess, str]], token: str) -> None:
    """Wait until every node sees the other two healthy (3-node mesh quorum-ready)."""
    ids = [nid for _, nid in pairs]

    def ready() -> bool | None:
        for gw, my_id in pairs:
            status = _api_json("GET", gw.url, "/v1/mesh/status", token, timeout=5.0)
            if any(other != my_id and not _peer_healthy(status, other) for other in ids):
                return None
        return True

    _eventually("all three mesh nodes to see each other healthy", ready, timeout=45.0)


def test_three_node_writable_volume_quorum_ha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With VMON_RESTORE_QUORUM on a 3-node cluster, a guest's writes to a *writable*
    named volume survive the owner's death: a surviving majority confirms the owner
    gone, restores the sandbox, materializes the writable volume, and the restored
    guest reads back its own written data."""
    from vmon.client import MeshClient

    token = "cluster-e2e-" + secrets.token_urlsafe(24)
    homes = [Path(tempfile.mkdtemp(prefix="vce-")) for _ in range(3)]
    quorum_env = {"VMON_RESTORE_QUORUM": "1", "VMON_REPLICAS": "2"}
    gws = [
        GatewayProcess(
            f"q-{label}",
            _free_port(),
            homes[i],
            token,
            Path(f"/tmp/ceq-{label}.log"),
            extra_env=quorum_env,
        )
        for i, label in enumerate("abc")
    ]
    sid = f"ceq-{os.getpid()}"
    quoted_sid = urllib.parse.quote(sid, safe="")
    vol_name = f"rwvol{os.getpid()}"
    sentinel = "HA_RW_OK"

    try:
        for gw in gws:
            gw.start()
        setup = _api_json(
            "POST",
            gws[0].url,
            "/v1/mesh/setup",
            token,
            {"advertise": gws[0].url, "max_vcpus": 64, "max_mem_mib": 65_536},
        )
        node_ids = [str(setup["node_id"])]
        for gw in gws[1:]:
            join = _api_json(
                "POST",
                gw.url,
                "/v1/mesh/join",
                token,
                {"blob": setup["blob"], "advertise": gw.url},
            )
            node_ids.append(str(join["node_id"]))
        pairs = list(zip(gws, node_ids, strict=True))
        _wait_mesh_trio(pairs, token)
        for gw in gws:
            gw.restart()
        _wait_mesh_trio(pairs, token)

        endpoints = _write_context_from_status(
            monkeypatch, tmp_path / "client-home", gws[0].url, token
        )
        client = MeshClient(endpoints, token)
        image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
        # Writable volume, created on node A (volume-backed create pins local).
        try:
            _api_json(
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

        _eventually(
            f"writable volume sandbox {sid} running on node A",
            lambda: _sandbox_row(
                _api_json("GET", gws[0].url, "/v1/sandboxes", token, timeout=10.0), sid
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
            _eventually(
                f"{nid}'s replica of {sid} to carry the post-write volume payload",
                lambda home=home: (
                    _replica_volume_payload(home, sid, vol_name, "written.txt") == sentinel
                ),
                timeout=120.0,
                interval=2.0,
            )

        gws[0].stop()
        assert gws[0].proc is not None and gws[0].proc.poll() is not None
        client.call("ps")  # promote a survivor in the client roster

        # A surviving majority (2 of 3) confirms A gone and restores the sandbox.
        restored = _eventually(
            f"orphaned sandbox {sid} to restore on a survivor",
            lambda: next(
                (
                    (gw, nid, r)
                    for gw, nid, _home in survivors
                    if (
                        r := _sandbox_row(
                            _api_json("GET", gw.url, "/v1/sandboxes", token, timeout=5.0), sid
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
        got = _eventually(
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
                    _api_json(
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
