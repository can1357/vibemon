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
from dataclasses import dataclass
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
            lambda: sid
            in _api_json(
                "GET", survivor_gw.url, "/v1/mesh/replica/list", token, timeout=5.0
            ).get("sids", []),
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
