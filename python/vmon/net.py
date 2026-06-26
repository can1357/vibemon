"""Host-side network conveniences for sandbox microVMs.

The Rust VMM only consumes an existing TAP (and optional netns). This module is
for the Python SDK path that is allowed to create host policy: TAP links,
forwarding/NAT, and per-sandbox egress rules.
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import ipaddress
import json
import os
import socket
import subprocess
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

CAP_NET_ADMIN = 12
IP_FORWARD = Path("/proc/sys/net/ipv4/ip_forward")
RULE_PREFIX = "vmon"
POOL = ipaddress.ip_network("172.20.0.0/16")
DEFAULT_DNS = ("1.1.1.1", "8.8.8.8")
STATE = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))
LEASE_DIR = STATE / "network"
LEASE_FILE = LEASE_DIR / "leases.json"

type LeaseState = dict[str, dict[str, str]]


@dataclass(frozen=True)
class TapLease:
    name: str
    guest_ip: str
    host_ip: str
    prefix: int
    network: str
    egress_allow: tuple[str, ...]

    @property
    def guest_cidr(self) -> str:
        return f"{self.guest_ip}/32"

    @property
    def host_cidr(self) -> str:
        return f"{self.host_ip}/{self.prefix}"


class SandboxNetwork:
    def __init__(
        self,
        name: str,
        config: dict[str, object],
        tap: TapLease,
        tunnels: _TunnelSet,
        egress_allow: Sequence[str] | None,
        egress_allow_domains: Sequence[str] | None = None,
    ) -> None:
        self.name = name
        self.config = config
        self.tap = tap
        self._tunnels = tunnels
        self._egress_allow = tuple(egress_allow) if egress_allow is not None else None
        self._egress_allow_domains = (
            tuple(egress_allow_domains) if egress_allow_domains is not None else None
        )
        self.guest_config = {
            "tap": config["tap"],
            "guest_ip": config["guest_ip"],
            "host_ip": config["host_ip"],
            "prefix": config["prefix"],
            "dns": list(cast(Sequence[str], config["dns"])),
        }

    def tunnels(self) -> dict[int, tuple[str, int]]:
        return self._tunnels.mapping()

    def teardown(self) -> None:
        self._tunnels.close()
        teardown_tap(
            self.tap.name,
            self.tap.guest_ip,
            self.tap.host_ip,
            self.tap.prefix,
            self._egress_allow,
            self._egress_allow_domains,
        )
        release_guest_config(self.name)


class _TunnelSet:
    def __init__(
        self,
        guest_ip: str,
        ports: Sequence[int],
        inbound_cidr_allowlist: Sequence[str] | None = None,
    ) -> None:
        self._guest_ip = guest_ip
        self._allowed_nets = _parse_cidr_allowlist(inbound_cidr_allowlist)
        self._servers: dict[int, asyncio.AbstractServer] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        ports = tuple(dict.fromkeys(int(p) for p in ports))
        for port in ports:
            if port <= 0 or port > 65535:
                raise ValueError(f"invalid TCP port {port}")
        if not ports:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name="vmon-tunnels", daemon=True)
        self._thread.start()
        try:
            for port in ports:
                server = asyncio.run_coroutine_threadsafe(
                    self._start_server(port), self._loop
                ).result(timeout=5)
                self._servers[port] = server
        except Exception:
            self.close()
            raise

    def _run_loop(self) -> None:
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _start_server(self, guest_port: int) -> asyncio.AbstractServer:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            task = asyncio.current_task()
            if task is not None:
                self._tasks.add(task)
            try:
                await _proxy_tcp(reader, writer, self._guest_ip, guest_port, self._allowed_nets)
            finally:
                if task is not None:
                    self._tasks.discard(task)

        return await asyncio.start_server(handler, "127.0.0.1", 0)

    def mapping(self) -> dict[int, tuple[str, int]]:
        out: dict[int, tuple[str, int]] = {}
        for guest_port, server in self._servers.items():
            sockets = cast(Sequence[socket.socket] | None, getattr(server, "sockets", None))
            sock = sockets[0] if sockets else None
            if sock is not None:
                sockaddr = cast(tuple[object, ...], sock.getsockname())
                host = cast(str, sockaddr[0])
                port = int(cast(int | str, sockaddr[1]))
                out[guest_port] = (host, port)
        return out

    def close(self) -> None:
        if self._loop is None:
            return
        if self._servers:
            fut = asyncio.run_coroutine_threadsafe(
                _close_servers(tuple(self._servers.values())), self._loop
            )
            with contextlib.suppress(Exception):
                fut.result(timeout=5)
            self._servers.clear()
        if self._tasks:
            fut = asyncio.run_coroutine_threadsafe(_cancel_tasks(set(self._tasks)), self._loop)
            with contextlib.suppress(Exception):
                fut.result(timeout=5)
            self._tasks.clear()
        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._loop.close()
        self._loop = None
        self._thread = None


def setup_sandbox_network(
    name: str,
    ports: Sequence[int] | None = None,
    egress_allow: Sequence[str] | None = None,
    egress_allow_domains: Sequence[str] | None = None,
    inbound_cidr_allowlist: Sequence[str] | None = None,
) -> SandboxNetwork:
    config = allocate_guest_config(name)
    tunnels: _TunnelSet | None = None
    try:
        tap = setup_tap(
            str(config["tap"]),
            str(config["guest_ip"]),
            str(config["host_ip"]),
            int(cast(int | str, config["prefix"])),
            egress_allow,
            egress_allow_domains,
        )
        tunnels = _TunnelSet(str(config["guest_ip"]), ports or (), inbound_cidr_allowlist)
        return SandboxNetwork(name, config, tap, tunnels, egress_allow, egress_allow_domains)
    except Exception:
        if tunnels is not None:
            tunnels.close()
        teardown_tap(
            str(config["tap"]),
            str(config["guest_ip"]),
            str(config["host_ip"]),
            int(cast(int | str, config["prefix"])),
            egress_allow,
            egress_allow_domains,
        )
        release_guest_config(name)
        raise


def allocate_guest_config(name: str, dns: Sequence[str] = DEFAULT_DNS) -> dict[str, object]:
    def update(leases: LeaseState) -> dict[str, object]:
        entry = leases.get(name)
        if entry is None:
            used = {item["network"] for item in leases.values() if "network" in item}
            for network in POOL.subnets(new_prefix=30):
                network_s = str(network)
                if network_s not in used:
                    entry = {"network": network_s, "tap": _tap_name(name)}
                    leases[name] = entry
                    break
            else:
                raise RuntimeError(f"no free /30 networks left in {POOL}")
        network = ipaddress.ip_network(entry["network"])
        hosts = list(network.hosts())
        return {
            "tap": entry.get("tap") or _tap_name(name),
            "host_ip": str(hosts[0]),
            "guest_ip": str(hosts[1]),
            "prefix": 30,
            "network": str(network),
            "dns": list(dns),
        }

    return _update_leases(update)


def release_guest_config(name: str) -> None:
    def update(leases: LeaseState) -> None:
        leases.pop(name, None)

    _update_leases(update)


def lease_for(name: str) -> dict[str, object] | None:
    """Return the persisted /30 lease for *name* (tap + addresses), or ``None``.

    Read-only: unlike :func:`allocate_guest_config` this never allocates a new
    lease, so teardown paths (e.g. the engine stopping a rehydrated VM) can look
    up the TAP name and point-to-point addresses without creating fresh state.
    """
    try:
        leases = cast(dict[str, object], json.loads(LEASE_FILE.read_text()))
    except FileNotFoundError, json.JSONDecodeError:
        return None
    entry = leases.get(name)
    if not isinstance(entry, dict) or "network" not in entry:
        return None
    network_value = entry.get("network")
    if not isinstance(network_value, str):
        return None
    network = ipaddress.ip_network(network_value)
    hosts = list(network.hosts())
    return {
        "tap": cast(str, entry.get("tap")) or _tap_name(name),
        "host_ip": str(hosts[0]),
        "guest_ip": str(hosts[1]),
        "prefix": network.prefixlen,
        "network": str(network),
    }


async def _proxy_tcp(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    guest_ip: str,
    guest_port: int,
    allowed_nets: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...],
) -> None:
    peer = client_writer.get_extra_info("peername")
    peer_ip = peer[0] if peer else None
    if not _ip_allowed(peer_ip, allowed_nets):
        client_writer.close()
        with contextlib.suppress(Exception):
            await client_writer.wait_closed()
        return
    try:
        guest_reader, guest_writer = await asyncio.open_connection(guest_ip, guest_port)
    except OSError:
        client_writer.close()
        await client_writer.wait_closed()
        return
    await asyncio.gather(
        _pipe(client_reader, guest_writer),
        _pipe(guest_reader, client_writer),
        return_exceptions=True,
    )


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            data = await reader.read(64 * 1024)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def _close_servers(servers: Sequence[asyncio.AbstractServer]) -> None:
    for server in servers:
        server.close()
    await asyncio.gather(*(server.wait_closed() for server in servers), return_exceptions=True)


async def _cancel_tasks(tasks: set[asyncio.Task[None]]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _parse_cidr_allowlist(
    allowlist: Sequence[str] | None,
) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    """Parse an inbound source-IP allowlist; ``None`` means loopback-only."""
    items = tuple(allowlist) if allowlist is not None else ("127.0.0.0/8",)
    return tuple(ipaddress.ip_network(cidr, strict=False) for cidr in items)


def _ip_allowed(
    ip_str: str | None,
    nets: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...],
) -> bool:
    if ip_str is None:
        return False
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return any(addr in net for net in nets)


def _update_leases[LeaseUpdateResult](
    callback: Callable[[LeaseState], LeaseUpdateResult],
) -> LeaseUpdateResult:
    LEASE_DIR.mkdir(parents=True, exist_ok=True)
    with LEASE_FILE.open("a+", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        raw = f.read()
        leases: LeaseState = cast(LeaseState, json.loads(raw)) if raw.strip() else {}
        result = callback(leases)
        f.seek(0)
        f.truncate()
        json.dump(leases, f, indent=2, sort_keys=True)
        f.write("\n")
        fcntl.flock(f, fcntl.LOCK_UN)
        return result


def _tap_name(name: str) -> str:
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]
    return f"tv{digest}"


def has_net_admin() -> bool:
    if os.geteuid() == 0:
        return True
    try:
        status = Path("/proc/self/status").read_text().splitlines()
    except OSError:
        return False
    for line in status:
        if line.startswith("CapEff:"):
            try:
                return bool(int(line.split()[1], 16) & (1 << CAP_NET_ADMIN))
            except IndexError, ValueError:
                return False
    return False


def require_net_admin() -> None:
    if not has_net_admin():
        raise PermissionError("network setup needs root or CAP_NET_ADMIN")


_REFRESHERS: dict[str, _DomainRefresher] = {}
_REFRESHERS_LOCK = threading.Lock()


def _resolve_domain_ips(domains: Sequence[str]) -> list[str]:
    """Resolve each domain's IPv4 (A) addresses; a leading ``*.`` is stripped."""
    ips: set[str] = set()
    for domain in domains:
        host = domain.strip()
        if host.startswith("*."):
            host = host[2:]
        if not host:
            continue
        try:
            infos = socket.getaddrinfo(host, None, family=socket.AF_INET)
        except socket.gaierror, OSError:
            continue
        for info in infos:
            addr = info[4][0]
            if isinstance(addr, str):
                ips.add(addr)
    return sorted(ips, key=lambda value: ipaddress.ip_address(value))


def _domain_rule(action: str, name: str, guest_cidr: str, ip: str, idx: int) -> list[str]:
    return [
        action,
        "FORWARD",
        "-i",
        name,
        "-s",
        guest_cidr,
        "-d",
        f"{ip}/32",
        "-m",
        "comment",
        "--comment",
        _comment(name, f"domain-{idx}"),
        "-j",
        "ACCEPT",
    ]


class _DomainRefresher:
    """Daemon thread re-resolving an egress domain allowlist every 60s.

    Newly-seen IPv4 addresses are added as ACCEPT rules (inserted above the
    egress-deny REJECT); rules are never removed for the life of the tap so live
    connections survive DNS rotation. This is DNS-pinned allowlisting, NOT live
    TLS-SNI filtering.
    """

    INTERVAL = 60.0

    def __init__(
        self, name: str, guest_cidr: str, domains: Sequence[str], initial_ips: Sequence[str]
    ) -> None:
        self._name = name
        self._guest_cidr = guest_cidr
        self._domains = tuple(domains)
        self._seen: set[str] = set(initial_ips)
        self._next_idx = len(self._seen)
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name=f"vmon-egress-dns-{name}", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.wait(self.INTERVAL):
            try:
                current = _resolve_domain_ips(self._domains)
            except Exception:
                continue
            for ip in current:
                if ip in self._seen:
                    continue
                with contextlib.suppress(Exception):
                    _iptables_ensure(
                        _domain_rule("-I", self._name, self._guest_cidr, ip, self._next_idx)
                    )
                self._seen.add(ip)
                self._next_idx += 1


def _start_domain_refresher(
    name: str, guest_cidr: str, domains: Sequence[str], initial_ips: Sequence[str]
) -> None:
    _stop_domain_refresher(name)
    refresher = _DomainRefresher(name, guest_cidr, domains, initial_ips)
    with _REFRESHERS_LOCK:
        _REFRESHERS[name] = refresher
    refresher.start()


def _stop_domain_refresher(name: str) -> None:
    with _REFRESHERS_LOCK:
        refresher = _REFRESHERS.pop(name, None)
    if refresher is not None:
        refresher.stop()


def setup_tap(
    name: str,
    guest_ip: str,
    host_ip: str,
    prefix: int = 30,
    egress_allow: Sequence[str] | None = None,
    egress_allow_domains: Sequence[str] | None = None,
) -> TapLease:
    """Create/configure a TAP and idempotent IPv4 forwarding/NAT rules.

    ``guest_ip`` and ``host_ip`` must be in the same subnet. When
    ``egress_allow`` is supplied, guest-originated forwarding is accepted only
    to those CIDRs; an empty list blocks all egress.

    ``egress_allow_domains`` additionally allows egress to the IPv4 addresses
    each domain currently resolves to (a leading ``*.`` is stripped and the
    apex resolved, best-effort). Resolved IPs become ACCEPT rules and a daemon
    thread re-resolves every 60s, adding newly-seen IPs for the life of the tap
    (rules are never removed mid-session, to avoid tearing live connections).
    This is **DNS-pinned allowlisting, NOT live TLS-SNI filtering**. Domain and
    CIDR allowances are additive (their union is permitted); whenever either
    allowlist is set a trailing REJECT drops all other guest egress.
    """

    require_net_admin()
    lease = _lease(name, guest_ip, host_ip, prefix, egress_allow)

    if not _ip_link_exists(name):
        _run(["ip", "tuntap", "add", "dev", name, "mode", "tap"])
    _run(["ip", "addr", "replace", lease.host_cidr, "dev", name])
    _run(["ip", "link", "set", "dev", name, "up"])
    _enable_ip_forward()

    _iptables_ensure(
        [
            "-t",
            "nat",
            "-A",
            "POSTROUTING",
            "-s",
            lease.network,
            "!",
            "-d",
            lease.network,
            "-m",
            "comment",
            "--comment",
            _comment(name, "masq"),
            "-j",
            "MASQUERADE",
        ]
    )
    _iptables_ensure(
        [
            "-A",
            "FORWARD",
            "-o",
            name,
            "-d",
            lease.guest_cidr,
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-m",
            "comment",
            "--comment",
            _comment(name, "return"),
            "-j",
            "ACCEPT",
        ]
    )

    domains = tuple(d.strip() for d in (egress_allow_domains or ()) if d and d.strip())
    restricted = egress_allow is not None or bool(domains)

    # Avoid stale broad/reject rules when a caller repeats setup with a changed
    # egress policy for the same tap, and stop any prior DNS refresher.
    _stop_domain_refresher(name)
    _iptables_delete(
        [
            "-D",
            "FORWARD",
            "-i",
            name,
            "-s",
            lease.guest_cidr,
            "-m",
            "comment",
            "--comment",
            _comment(name, "egress"),
            "-j",
            "ACCEPT",
        ]
    )
    _iptables_delete(
        [
            "-D",
            "FORWARD",
            "-i",
            name,
            "-s",
            lease.guest_cidr,
            "-m",
            "comment",
            "--comment",
            _comment(name, "egress-deny"),
            "-j",
            "REJECT",
        ]
    )

    if not restricted:
        _iptables_ensure(
            [
                "-A",
                "FORWARD",
                "-i",
                name,
                "-s",
                lease.guest_cidr,
                "-m",
                "comment",
                "--comment",
                _comment(name, "egress"),
                "-j",
                "ACCEPT",
            ]
        )
    else:
        for idx, cidr in enumerate(lease.egress_allow):
            _iptables_ensure(
                [
                    "-A",
                    "FORWARD",
                    "-i",
                    name,
                    "-s",
                    lease.guest_cidr,
                    "-d",
                    cidr,
                    "-m",
                    "comment",
                    "--comment",
                    _comment(name, f"egress-{idx}"),
                    "-j",
                    "ACCEPT",
                ]
            )
        domain_ips = _resolve_domain_ips(domains)
        for idx, ip in enumerate(domain_ips):
            _iptables_ensure(_domain_rule("-I", name, lease.guest_cidr, ip, idx))
        _iptables_ensure(
            [
                "-A",
                "FORWARD",
                "-i",
                name,
                "-s",
                lease.guest_cidr,
                "-m",
                "comment",
                "--comment",
                _comment(name, "egress-deny"),
                "-j",
                "REJECT",
            ]
        )
        if domains:
            _start_domain_refresher(name, lease.guest_cidr, domains, domain_ips)

    return lease


def teardown_tap(
    name: str,
    guest_ip: str | None = None,
    host_ip: str | None = None,
    prefix: int = 30,
    egress_allow: Sequence[str] | None = None,
    egress_allow_domains: Sequence[str] | None = None,
) -> None:
    """Remove TAP and rules created by :func:`setup_tap`.

    Passing the original addresses lets teardown remove exact NAT/FORWARD rules;
    TAP deletion is attempted regardless and is idempotent.
    """

    _stop_domain_refresher(name)

    if not has_net_admin():
        return
    lease = None
    if guest_ip and host_ip:
        lease = _lease(name, guest_ip, host_ip, prefix, egress_allow)
        _iptables_delete(
            [
                "-t",
                "nat",
                "-D",
                "POSTROUTING",
                "-s",
                lease.network,
                "!",
                "-d",
                lease.network,
                "-m",
                "comment",
                "--comment",
                _comment(name, "masq"),
                "-j",
                "MASQUERADE",
            ]
        )
        _iptables_delete(
            [
                "-D",
                "FORWARD",
                "-o",
                name,
                "-d",
                lease.guest_cidr,
                "-m",
                "conntrack",
                "--ctstate",
                "ESTABLISHED,RELATED",
                "-m",
                "comment",
                "--comment",
                _comment(name, "return"),
                "-j",
                "ACCEPT",
            ]
        )
        _iptables_delete(
            [
                "-D",
                "FORWARD",
                "-i",
                name,
                "-s",
                lease.guest_cidr,
                "-m",
                "comment",
                "--comment",
                _comment(name, "egress"),
                "-j",
                "ACCEPT",
            ]
        )
        for idx, cidr in enumerate(lease.egress_allow):
            _iptables_delete(
                [
                    "-D",
                    "FORWARD",
                    "-i",
                    name,
                    "-s",
                    lease.guest_cidr,
                    "-d",
                    cidr,
                    "-m",
                    "comment",
                    "--comment",
                    _comment(name, f"egress-{idx}"),
                    "-j",
                    "ACCEPT",
                ]
            )
        _iptables_delete(
            [
                "-D",
                "FORWARD",
                "-i",
                name,
                "-s",
                lease.guest_cidr,
                "-m",
                "comment",
                "--comment",
                _comment(name, "egress-deny"),
                "-j",
                "REJECT",
            ]
        )
        for idx, ip in enumerate(_resolve_domain_ips(tuple(egress_allow_domains or ()))):
            _iptables_delete(_domain_rule("-D", name, lease.guest_cidr, ip, idx))

    if _ip_link_exists(name):
        _run(["ip", "link", "del", "dev", name], check=False)


def _lease(
    name: str,
    guest_ip: str,
    host_ip: str,
    prefix: int,
    egress_allow: Sequence[str] | None,
) -> TapLease:
    if not name or len(name.encode()) >= 16:
        raise ValueError("tap name must be non-empty and shorter than 16 bytes")
    guest = ipaddress.ip_address(guest_ip)
    host = ipaddress.ip_address(host_ip)
    if guest.version != 4 or host.version != 4:
        raise ValueError("sandbox TAP networking supports IPv4 only")
    network = ipaddress.ip_network(f"{host}/{prefix}", strict=False)
    if guest not in network or host not in network:
        raise ValueError(f"{guest} and {host} are not both in {network}")
    if prefix != 30:
        # The allocator and teardown rules are written around point-to-point /30s.
        raise ValueError("sandbox TAP networking requires a /30 prefix")
    allowed = tuple(str(ipaddress.ip_network(cidr, strict=False)) for cidr in (egress_allow or ()))
    return TapLease(name, str(guest), str(host), prefix, str(network), allowed)


def _enable_ip_forward() -> None:
    try:
        if IP_FORWARD.read_text().strip() == "1":
            return
        IP_FORWARD.write_text("1\n")
    except OSError as exc:
        raise RuntimeError(f"enabling IPv4 forwarding: {exc}") from exc


def _ip_link_exists(name: str) -> bool:
    return _run(["ip", "link", "show", "dev", name], check=False).returncode == 0


def _iptables_ensure(rule: list[str]) -> None:
    check_rule = _iptables_check_form(rule)
    if _run(["iptables", *check_rule], check=False).returncode == 0:
        return
    _run(["iptables", *rule])


def _iptables_delete(rule: list[str]) -> None:
    while _run(["iptables", *rule], check=False).returncode == 0:
        pass


def _iptables_check_form(rule: list[str]) -> list[str]:
    converted = list(rule)
    for idx, value in enumerate(converted):
        if value in ("-A", "-D", "-I"):
            converted[idx] = "-C"
            return converted
    raise ValueError(f"iptables rule missing -A/-D/-I: {rule}")


def _comment(name: str, tag: str) -> str:
    return f"{RULE_PREFIX}:{name}:{tag}"


def _run(args: Sequence[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        proc = subprocess.run(args, text=True, capture_output=True, check=False)
    except FileNotFoundError as exc:
        raise RuntimeError(f"required command not found: {args[0]}") from exc
    if check and proc.returncode != 0:
        stderr = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"{' '.join(args)} failed: {stderr}")
    return proc
