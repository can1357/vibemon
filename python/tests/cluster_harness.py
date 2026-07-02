from __future__ import annotations

import contextlib
import http.client
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PYTHON_ROOT = _REPO_ROOT / "python"
_LOOPBACK = "127.0.0.1"
_DEFAULT_CAPS = {"max_vcpus": 64, "max_mem_mib": 65_536}
_HEARTBEAT_PATH = "/v1/mesh/heartbeat"


def tail(path: Path, limit: int = 12_000) -> str:
    try:
        data = path.read_bytes()[-limit:]
    except OSError as exc:
        return f"<cannot read {path}: {exc}>"
    return data.decode("utf-8", errors="replace")


def api_json(
    method: str,
    base_url: str,
    path: str,
    token: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode()
    merged = {"Authorization": f"Bearer {token}", **(headers or {})}
    if payload is not None:
        merged["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        method=method,
        headers=merged,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"{method} {path} failed with HTTP {exc.code}: {body}") from exc
    except (
        urllib.error.URLError,
        http.client.RemoteDisconnected,
        http.client.HTTPException,
    ) as exc:
        reason = getattr(exc, "reason", exc)
        raise AssertionError(f"{method} {path} could not reach {base_url}: {reason}") from exc
    if not raw:
        return {}
    parsed = json.loads(raw)
    assert isinstance(parsed, dict), f"{method} {path} returned non-object JSON: {parsed!r}"
    return parsed


def eventually(
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


def healthz(url: str) -> bool:
    with urllib.request.urlopen(url.rstrip("/") + "/healthz", timeout=2.0) as response:
        return response.status == 200


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((_LOOPBACK, 0))
        return int(sock.getsockname()[1])


def _path_matches(path: str, prefixes: tuple[str, ...] | None) -> bool:
    return prefixes is None or any(path.startswith(prefix) for prefix in prefixes)


@dataclass
class _DropRule:
    count: int
    prefixes: tuple[str, ...] | None


@dataclass
class _ForwardHook:
    prefixes: tuple[str, ...] | None
    callback: Callable[[], None]
    count: int = 1
    delay: float = 0.0


@dataclass
class TcpFaultProxy:
    """Threaded loopback TCP relay with deterministic process-level faults.

    The gateway process listens on ``target_port`` while peers and clients use this
    proxy's ``url``. Faults are therefore outside the FastAPI app and exercise the
    same socket-level failure modes as a real network partition.
    """

    listen_port: int
    target_port: int
    host: str = _LOOPBACK
    blocked: bool = False
    _server: socket.socket | None = None
    _thread: threading.Thread | None = None
    _stop: threading.Event = field(default_factory=threading.Event)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _drop_rules: list[_DropRule] = field(default_factory=list)
    _latency: float = 0.0
    _latency_prefixes: tuple[str, ...] | None = None
    _blocked_sources: set[str] = field(default_factory=set)
    _forward_hooks: list[_ForwardHook] = field(default_factory=list)
    _active: set[socket.socket] = field(default_factory=set)

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.listen_port}"

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.listen_port))
        server.listen()
        server.settimeout(0.2)
        self._server = server
        self._thread = threading.Thread(target=self._serve, name=f"vmon-proxy-{self.listen_port}")
        self._thread.daemon = True
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        server = self._server
        self._server = None
        if server is not None:
            with contextlib.suppress(OSError):
                server.close()
        # Wake a thread blocked in accept() on platforms where close() is not enough.
        with contextlib.suppress(OSError):
            with socket.create_connection((self.host, self.listen_port), timeout=0.2):
                pass
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._thread = None

    def partition(self, enabled: bool = True) -> None:
        with self._lock:
            self.blocked = enabled
        if enabled:
            # A real partition also kills established flows: sever live relays so
            # HTTP keep-alive connections cannot tunnel through the blockade.
            self.sever()

    def sever(self) -> None:
        """Tear down every established relay pair (partition or process death)."""
        with self._lock:
            active = list(self._active)
            self._active.clear()
        for sock in active:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                sock.close()

    def drop_next(self, count: int = 1, prefixes: Sequence[str] | None = None) -> None:
        with self._lock:
            self._drop_rules.append(_DropRule(count=count, prefixes=_prefixes(prefixes)))

    def set_latency(self, seconds: float, prefixes: Sequence[str] | None = None) -> None:
        with self._lock:
            self._latency = max(0.0, seconds)
            self._latency_prefixes = _prefixes(prefixes)

    def block_source(self, node_id: str, enabled: bool = True) -> None:
        """Drop requests whose body identifies *node_id* as the sending node.

        Covers every mesh RPC that carries a source marker (heartbeat state,
        lease holder, replica/migrate source, owner broadcast, record owner).
        Requests without a source marker are not affected.
        """
        with self._lock:
            if enabled:
                self._blocked_sources.add(node_id)
            else:
                self._blocked_sources.discard(node_id)
        if enabled:
            self.sever()

    def on_forward_once(
        self,
        callback: Callable[[], None],
        *,
        prefixes: Sequence[str] | None = None,
        delay: float = 0.0,
    ) -> None:
        with self._lock:
            self._forward_hooks.append(
                _ForwardHook(prefixes=_prefixes(prefixes), callback=callback, delay=delay)
            )

    def reset(self) -> None:
        with self._lock:
            self.blocked = False
            self._drop_rules.clear()
            self._latency = 0.0
            self._latency_prefixes = None
            self._blocked_sources.clear()
            self._forward_hooks.clear()

    def _serve(self) -> None:
        while not self._stop.is_set():
            server = self._server
            if server is None:
                return
            try:
                client, _addr = server.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            thread = threading.Thread(target=self._handle, args=(client,))
            thread.daemon = True
            thread.start()

    def _handle(self, client: socket.socket) -> None:
        with client:
            try:
                first = self._read_initial_request(client)
                if not first:
                    return
                path = _request_path(first)
                body = _request_body(first)
                action = self._fault_action(path, body)
                if action == "drop":
                    return
                if action == "delay":
                    delay = self._delay_for(path)
                    if delay > 0.0:
                        time.sleep(delay)
                backend = socket.create_connection((self.host, self.target_port), timeout=5.0)
            except OSError:
                return
            with self._lock:
                self._active.add(client)
                self._active.add(backend)
            with backend:
                try:
                    backend.sendall(first)
                except OSError:
                    return
                self._run_forward_hooks(path)
                left = threading.Thread(target=_relay, args=(client, backend))
                right = threading.Thread(target=_relay, args=(backend, client))
                left.daemon = True
                right.daemon = True
                left.start()
                right.start()
                left.join()
                right.join()
                with self._lock:
                    self._active.discard(client)
                    self._active.discard(backend)

    def _read_initial_request(self, client: socket.socket) -> bytes:
        client.settimeout(2.0)
        chunks: list[bytes] = []
        total = 0
        header_end = -1
        content_length = 0
        while total < 1_048_576:
            chunk = client.recv(65_536)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            data = b"".join(chunks)
            if header_end < 0:
                header_end = data.find(b"\r\n\r\n")
                if header_end >= 0:
                    content_length = _content_length(data[:header_end])
            if header_end >= 0 and len(data) >= header_end + 4 + content_length:
                return data
        return b"".join(chunks)

    def _fault_action(self, path: str, body: bytes) -> str:
        with self._lock:
            if self.blocked:
                return "drop"
            source = _request_source(path, body)
            if source is not None and (
                "*" in self._blocked_sources or source in self._blocked_sources
            ):
                return "drop"
            for rule in list(self._drop_rules):
                if _path_matches(path, rule.prefixes):
                    rule.count -= 1
                    if rule.count <= 0:
                        self._drop_rules.remove(rule)
                    return "drop"
            if self._latency > 0.0 and _path_matches(path, self._latency_prefixes):
                return "delay"
            return "forward"

    def _delay_for(self, path: str) -> float:
        with self._lock:
            return self._latency if _path_matches(path, self._latency_prefixes) else 0.0

    def _run_forward_hooks(self, path: str) -> None:
        callbacks: list[tuple[Callable[[], None], float]] = []
        with self._lock:
            for hook in list(self._forward_hooks):
                if not _path_matches(path, hook.prefixes):
                    continue
                callbacks.append((hook.callback, hook.delay))
                hook.count -= 1
                if hook.count <= 0:
                    self._forward_hooks.remove(hook)
        for callback, delay in callbacks:
            if delay > 0.0:
                timer = threading.Timer(delay, callback)
                timer.daemon = True
                timer.start()
            else:
                callback()


def _prefixes(prefixes: Sequence[str] | None) -> tuple[str, ...] | None:
    return None if prefixes is None else tuple(prefixes)


def _content_length(header: bytes) -> int:
    for line in header.decode("iso-8859-1", errors="ignore").split("\r\n"):
        name, sep, value = line.partition(":")
        if sep and name.lower() == "content-length":
            with contextlib.suppress(ValueError):
                return int(value.strip())
    return 0


def _request_path(data: bytes) -> str:
    first_line = data.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="ignore")
    parts = first_line.split()
    if len(parts) < 2:
        return ""
    return urllib.parse.urlsplit(parts[1]).path


def _request_body(data: bytes) -> bytes:
    header_end = data.find(b"\r\n\r\n")
    return b"" if header_end < 0 else data[header_end + 4 :]


def _request_source(path: str, body: bytes) -> str | None:
    """Best-effort sending-node id from a mesh RPC body.

    Heartbeats carry ``state.node_id``; lease ops carry ``holder_node``;
    replica/migrate receives carry ``source_node``; owner broadcasts carry
    ``node_id``; record puts carry ``owner``. Non-mesh requests return None.
    """
    if not path.startswith("/v1/mesh/"):
        return None
    try:
        payload = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError, json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    state = payload.get("state")
    if isinstance(state, dict) and isinstance(state.get("node_id"), str):
        return state["node_id"]
    for field_name in ("holder_node", "source_node", "node_id", "owner"):
        value = payload.get(field_name)
        if isinstance(value, str) and value:
            return value
    return None


def _relay(src: socket.socket, dst: socket.socket) -> None:
    try:
        # Long-running requests (cold image import + first boot) can sit silent
        # for minutes before the response; do not let the relay idle out under
        # them. Severing on partition still closes these sockets immediately.
        src.settimeout(300.0)
        while True:
            try:
                chunk = src.recv(65_536)
            except TimeoutError:
                return
            if not chunk:
                with contextlib.suppress(OSError):
                    dst.shutdown(socket.SHUT_WR)
                return
            dst.sendall(chunk)
    except OSError:
        return


@dataclass(eq=False)
class NodeProc:
    name: str
    port: int
    home: Path
    token: str
    log_path: Path
    env_overrides: dict[str, str] = field(default_factory=dict)
    backend_port: int | None = None
    proxy: TcpFaultProxy | None = None
    proc: subprocess.Popen[bytes] | None = None
    node_id: str | None = None
    _log: Any | None = None

    @property
    def url(self) -> str:
        return f"http://{_LOOPBACK}:{self.port}"

    @property
    def backend_url(self) -> str:
        return f"http://{_LOOPBACK}:{self.backend_port or self.port}"

    def start(self) -> None:
        assert self.proc is None or self.proc.poll() is not None, f"{self.name} already running"
        self.home.mkdir(parents=True, exist_ok=True)
        if self.proxy is not None:
            self.proxy.start()
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
        env.update(self.env_overrides)
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
                _LOOPBACK,
                "--port",
                str(self.backend_port or self.port),
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
        eventually(
            f"{self.name} healthz on {self.backend_url}",
            self._healthy_or_exited,
            timeout=60.0,
            interval=0.25,
        )

    def stop(self, *, timeout: float = 20.0, close_proxy: bool = True) -> None:
        self._signal_process(signal.SIGTERM, timeout=timeout)
        if self._log is not None:
            self._log.close()
            self._log = None
        if close_proxy and self.proxy is not None:
            self.proxy.close()

    def restart(self) -> None:
        self.stop(close_proxy=False)
        self.start()

    def _healthy_or_exited(self) -> bool:
        assert self.proc is not None
        rc = self.proc.poll()
        if rc is not None:
            raise AssertionError(f"{self.name} exited with {rc}; log tail:\n{tail(self.log_path)}")
        return healthz(self.backend_url)

    def _signal_process(self, sig: signal.Signals | int, *, timeout: float = 20.0) -> None:
        proc = self.proc
        if proc is None or proc.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, sig)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=timeout)


def start_node(
    home: Path,
    port: int,
    env_overrides: Mapping[str, str] | None = None,
    *,
    name: str | None = None,
    token: str | None = None,
    log_path: Path | None = None,
    fault_proxy: bool = False,
) -> NodeProc:
    node_name = name or f"node-{port}"
    node_token = (
        token or os.environ.get("VMON_API_TOKEN") or "cluster-e2e-" + secrets.token_urlsafe(24)
    )
    backend_port = free_port() if fault_proxy else port
    proxy = TcpFaultProxy(port, backend_port) if fault_proxy else None
    node = NodeProc(
        node_name,
        port,
        home,
        node_token,
        log_path or Path(tempfile.gettempdir()) / f"{node_name}.log",
        dict(env_overrides or {}),
        backend_port=backend_port,
        proxy=proxy,
    )
    node.start()
    return node


def kill_node(node: NodeProc, sig: signal.Signals | int = signal.SIGKILL) -> None:
    node._signal_process(sig, timeout=20.0)
    if node.proxy is not None:
        # The process is dead; its established relays must die with it, or a
        # client blocked on a response through the proxy hangs to its timeout.
        node.proxy.sever()


def signal_node(node: NodeProc, sig: signal.Signals | int) -> None:
    proc = node.proc
    assert proc is not None and proc.poll() is None, f"{node.name} is not running"
    os.killpg(proc.pid, sig)


def pause_node(node: NodeProc) -> None:
    signal_node(node, signal.SIGSTOP)


def resume_node(node: NodeProc) -> None:
    signal_node(node, signal.SIGCONT)


def restart_node(node: NodeProc) -> None:
    node.restart()


def mesh_setup(
    node: NodeProc,
    *,
    max_vcpus: int = _DEFAULT_CAPS["max_vcpus"],
    max_mem_mib: int = _DEFAULT_CAPS["max_mem_mib"],
    region: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "advertise": node.url,
        "max_vcpus": max_vcpus,
        "max_mem_mib": max_mem_mib,
    }
    if region:
        payload["region"] = region
    response = api_json("POST", node.url, "/v1/mesh/setup", node.token, payload)
    node.node_id = str(response["node_id"])
    return response


def mesh_join(node: NodeProc, blob: str, *, region: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {"blob": blob, "advertise": node.url}
    if region:
        payload["region"] = region
    response = api_json("POST", node.url, "/v1/mesh/join", node.token, payload)
    node.node_id = str(response["node_id"])
    return response


def peer_healthy(status: dict[str, Any], peer_id: str) -> bool:
    return any(
        peer.get("node_id") == peer_id and peer.get("healthy") is True
        for peer in status.get("peers") or []
        if isinstance(peer, dict)
    )


def wait_healthy(
    nodes: Sequence[NodeProc],
    node_ids: Mapping[NodeProc | str, str] | None = None,
    *,
    timeout: float = 45.0,
) -> list[dict[str, Any]]:
    ids = [_node_id(node, node_ids) for node in nodes]

    def ready() -> list[dict[str, Any]] | None:
        statuses = [
            api_json("GET", node.url, "/v1/mesh/status", node.token, timeout=5.0) for node in nodes
        ]
        for status, my_id in zip(statuses, ids, strict=True):
            if status.get("enabled") is not True:
                return None
            for other in ids:
                if other != my_id and not peer_healthy(status, other):
                    return None
        return statuses

    return eventually("mesh nodes to see each other healthy", ready, timeout=timeout)


def _node_id(node: NodeProc, mapping: Mapping[NodeProc | str, str] | None) -> str:
    if mapping is not None:
        for key in (node, node.name, node.url):
            if key in mapping:
                return mapping[key]
    assert node.node_id is not None, f"node id for {node.name} is not known"
    return node.node_id


def sandbox_row(listing: dict[str, Any], sid: str) -> dict[str, Any] | None:
    for item in listing.get("sandboxes") or []:
        if not isinstance(item, dict):
            continue
        if item.get("id") == sid or item.get("name") == sid:
            return item
    return None


def local_sandboxes(node: NodeProc, token: str, *, timeout: float = 5.0) -> dict[str, Any]:
    """List a node's OWN sandbox rows only.

    ``X-Vmon-Mesh-Hop`` bypasses both peer aggregation and owner proxying, so
    the result proves what THIS node runs locally — required for fencing
    assertions, where the aggregated view keeps showing the successor's row.
    """
    return api_json(
        "GET",
        node.url,
        "/v1/sandboxes",
        token,
        timeout=timeout,
        headers={"X-Vmon-Mesh-Hop": "1"},
    )


def write_context_from_status(
    monkeypatch: pytest.MonkeyPatch,
    client_home: Path,
    seed_url: str,
    token: str,
    *,
    name: str = "cluster-e2e",
) -> list[str]:
    monkeypatch.setenv("VMON_HOME", str(client_home))
    from vmon.context import (
        Context,
        ContextStore,
        roster_from_status,
    )

    status = api_json("GET", seed_url, "/v1/mesh/status", token)
    endpoints = roster_from_status(status, fallback=seed_url)
    assert seed_url in endpoints, f"context roster should include seed {seed_url}: {endpoints!r}"
    store = ContextStore()
    store.load()
    store.put(
        Context(
            name=name,
            endpoints=endpoints,
            updated=time.time(),
        )
    )
    store.use(name)
    reloaded = ContextStore()
    reloaded.load()
    current = reloaded.current()
    assert current is not None, "cluster context was not persisted"
    assert current.endpoints == endpoints, "persisted cluster context lost the mesh roster"
    return endpoints


def replica_volume_payload(home: Path, sid: str, vol: str, fname: str) -> str | None:
    """Return a held replica volume payload by following replica metadata."""

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


def partition_node(node: NodeProc, peers: Sequence[NodeProc], *, enabled: bool = True) -> None:
    """Isolate *node*: nothing reaches it, and peers drop its mesh RPCs.

    The node's own proxy blocks fully (ingress dead, live relays severed);
    every peer's proxy drops requests whose body identifies *node* as the
    sender (heartbeats, lease ops, owner broadcasts, record/replica pushes).
    Peers keep talking to each other and to clients — this isolates ONE node,
    it does not shatter the mesh.
    """
    assert node.proxy is not None, "partition_node requires a fault-proxy node"
    assert node.node_id is not None, "node must have joined a mesh before partition"
    node.proxy.partition(enabled)
    for peer in peers:
        if peer is node or peer.proxy is None:
            continue
        peer.proxy.block_source(node.node_id, enabled)


def heal_node(node: NodeProc, peers: Sequence[NodeProc]) -> None:
    partition_node(node, peers, enabled=False)


def inject_latency(
    node: NodeProc,
    seconds: float,
    *,
    paths: Sequence[str] | None = (_HEARTBEAT_PATH,),
) -> None:
    assert node.proxy is not None, "inject_latency requires a fault-proxy node"
    node.proxy.set_latency(seconds, paths)


def drop_requests(
    node: NodeProc,
    count: int = 1,
    *,
    paths: Sequence[str] | None = None,
) -> None:
    assert node.proxy is not None, "drop_requests requires a fault-proxy node"
    node.proxy.drop_next(count, paths)


@contextlib.contextmanager
def unwritable_replicas(node: NodeProc):
    replica_dir = node.home / "replicas"
    replica_dir.mkdir(parents=True, exist_ok=True)
    old_mode = replica_dir.stat().st_mode & 0o777
    replica_dir.chmod(0)
    try:
        yield replica_dir
    finally:
        with contextlib.suppress(OSError):
            replica_dir.chmod(old_mode)
