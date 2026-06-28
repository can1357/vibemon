"""Modal-style sandbox API backed by vmon templates and the guest agent."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import json
import os
import platform
import secrets as _secrets
import shutil
import textwrap
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any, cast

from .agent_client import AgentClosed, AgentConn, ByteStream, ExecSession
from .image import ImageSpec, cached_template, slot_tag
from .pool import WarmPool, wait_for_agent_ready
from .secret import Secret, merge_secrets
from .vmm import STATE, MicroVM, _instance_name, _validate_int_range, _validate_timeout_secs
from .volume import Volume

_POOLS: dict[str, WarmPool] = {}
_POOL_REFS: dict[str, str] = {}
_POOLS_LOCK = threading.Lock()
# Max named volumes served by the warm path: a request with up to this many
# volumes restores from a slot-provisioned template (one virtio-fs slot per
# volume, rebound on restore); beyond it the sandbox cold-boots. Capped at the
# VMM's hard limit of 8 total --volume mounts (config.rs); override via env.
_WARM_VOLUME_SLOTS = min(8, max(0, int(os.environ.get("VMON_WARM_VOLUME_SLOTS", "8"))))


def pool_inventory() -> dict[str, int]:
    """Ready warm-pool counts keyed by the portable image/template ref."""
    with _POOLS_LOCK:
        inventory: dict[str, int] = {}
        for key, pool in _POOLS.items():
            ref = _POOL_REFS.get(key)
            if ref is None:
                continue
            inventory[ref] = int(pool.stats().get("ready_count") or 0)
        return inventory


def prewarm(ref: str, *, count: int, **template_kwargs: Any) -> WarmPool:
    """Build ``ref``'s template and keep ``count`` warm clones of the host's
    pool-claimable sandbox flavor ready for instant claim.

    Which ``Sandbox.create`` actually claims the pool is host-dependent, because
    only some network flavors are poolable:

    - **macOS/HVF:** warms the user-mode-NAT NIC flavor, claimed by a default
      (networked) ``Sandbox.create(image=ref)`` — user-mode NAT is in-process, so
      each pre-forked clone is self-contained.
    - **Linux/KVM:** warms the block-network flavor (no NIC). A *networked* Linux
      sandbox needs a per-sandbox host TAP allocated at restore time, which a
      pre-forked pool cannot bake in, so a default ``Sandbox.create`` warm-restores
      onto a fresh TAP rather than claiming. The block-network pool is claimed by
      ``Sandbox.create(image=ref, block_network=True)`` — the shape the web panel's
      create form and ``vmon shell`` use.

    Returns the registered :class:`WarmPool`; pass it to :func:`shutdown_prewarms`
    to drain it.
    """
    count = int(count)
    if count < 0:
        raise ValueError("pool size must be >= 0")
    networked = platform.system() == "Darwin"
    cached = cached_template(image=ref, nic_slot=networked, **template_kwargs)
    key = str(cached.snapshot_dir)
    old_pool: WarmPool | None = None
    with _POOLS_LOCK:
        pool = _POOLS.get(key)
        if pool is None or pool.size != count:
            old_pool = pool
            pool = WarmPool(cached.snapshot_dir, count)
            _POOLS[key] = pool
        _POOL_REFS[key] = cached.spec.reference
    if old_pool is not None:
        old_pool.shutdown()
    return pool


def shutdown_prewarms(pools: Iterable[WarmPool]) -> None:
    """Unregister and stop warm pools created by ``prewarm``."""
    pool_set = set(pools)
    with _POOLS_LOCK:
        keys = [key for key, pool in _POOLS.items() if pool in pool_set]
        for key in keys:
            _POOLS.pop(key, None)
            _POOL_REFS.pop(key, None)
    for pool in pool_set:
        pool.shutdown()


def shutdown_all_pools() -> None:
    """Unregister and stop every warm pool; used on server teardown.

    Drains the whole registry, so pools created or replaced at runtime (a request
    with a different ``pool_size``) are reclaimed too, not just the startup set.
    """
    with _POOLS_LOCK:
        pools = list(_POOLS.values())
        _POOLS.clear()
        _POOL_REFS.clear()
    for pool in pools:
        pool.shutdown()


def _setup_sandbox_network(
    name: str,
    *,
    ports: Sequence[int] | None = None,
    egress_allow: Sequence[str] | None = None,
    egress_allow_domains: Sequence[str] | None = None,
    inbound_cidr_allowlist: Sequence[str] | None = None,
) -> Any:
    """Create the host TAP + port tunnels for a networked sandbox before launch.

    Returns a :class:`vmon.net.SandboxNetwork`; raises ``PermissionError`` without
    root/CAP_NET_ADMIN. The VM is then booted with ``--tap`` bound to this TAP and
    the guest side is configured (``net_config``) once the agent answers.
    """
    from . import net

    return net.setup_sandbox_network(
        name,
        ports=list(ports) if ports is not None else None,
        egress_allow=list(egress_allow) if egress_allow is not None else None,
        egress_allow_domains=list(egress_allow_domains)
        if egress_allow_domains is not None
        else None,
        inbound_cidr_allowlist=list(inbound_cidr_allowlist)
        if inbound_cidr_allowlist is not None
        else None,
    )


class _ProcessStdin:
    def __init__(self, session: ExecSession) -> None:
        self._session = session
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        if self._closed:
            raise ValueError("stdin is closed")
        self._session.write_stdin(data)

    def close(self) -> None:
        if not self._closed:
            try:
                self._session.close_stdin()
            finally:
                self._closed = True


class Process:
    """A process running inside a Sandbox."""

    def __init__(self, session: ExecSession) -> None:
        self._session = session
        self.stdout: ByteStream = session.stdout
        self.stderr: ByteStream = session.stderr
        self.stdin = _ProcessStdin(session)

    @property
    def returncode(self) -> int | None:
        return self._session.returncode

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        self.stdin.write(data)

    def close_stdin(self) -> None:
        self.stdin.close()

    def kill(self, signal: int = 15) -> None:
        self._session.kill(signal)

    def resize(self, rows: int, cols: int) -> None:
        self._session.resize(int(rows), int(cols))

    def wait(self, timeout: float | None = None) -> int:
        try:
            return self._session.wait(timeout)
        finally:
            self._close_streams()

    def _close_streams(self) -> None:
        with contextlib.suppress(Exception):
            self.stdout.close()
        with contextlib.suppress(Exception):
            self.stderr.close()


class Filesystem:
    """Filesystem RPC facade for a Sandbox."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def read_bytes(self, path: str) -> bytes:
        return self._sandbox.agent().fs_read(path)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def write_bytes(
        self, path: str, data: bytes | bytearray | memoryview, mode: int = 0o644
    ) -> None:
        self._sandbox.agent().fs_write(path, data, mode=mode)

    def write_text(self, path: str, text: str, encoding: str = "utf-8", mode: int = 0o644) -> None:
        self.write_bytes(path, text.encode(encoding), mode=mode)

    def list_files(self, path: str = ".") -> list[dict[str, Any]]:
        return self._sandbox.agent().fs_list(path)

    def make_directory(self, path: str, parents: bool = True) -> None:
        self._sandbox.agent().fs_mkdir(path, parents=parents)

    def remove(self, path: str, recursive: bool = False) -> None:
        self._sandbox.agent().fs_remove(path, recursive=recursive)

    def stat(self, path: str) -> dict[str, Any]:
        return self._sandbox.agent().fs_stat(path)


class Sandbox:
    """A warm-restored microVM with Modal-compatible exec and filesystem methods."""

    def __init__(
        self,
        vm: MicroVM,
        image_spec: ImageSpec | None = None,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        network: Any = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.vm = vm
        self.name = vm.name
        self.image_spec = image_spec
        self.workdir = workdir or (image_spec.workdir if image_spec else None) or "/"
        base: dict[str, str] = image_spec.env_dict() if image_spec else {}
        base.update(env or {})
        self.env = base
        self.tags: dict[str, str] = dict(tags or {})
        self.filesystem = Filesystem(self)
        self._agent: AgentConn | None = None
        self._network = network
        self._network_policy: dict[str, Any] = {}
        self._terminated = False
        self._secret_env: dict[str, str] = {}
        self._volumes: list[Volume] = []
        # Migration eligibility: only a block-network sandbox with no host-local
        # state (named volumes, fs_dir share) can move to another mesh node.
        self._block_network: bool = False
        self._fs_dir: str | None = None
        self.connect_token: str | None = None
        self._timeout_secs: int | None = None
        self._watchdog_stop: threading.Event | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._entry_returncode: int | None = None
        self._entry_process: Process | None = None

    @classmethod
    def create(
        cls,
        image: str | None = None,
        *,
        template: str | os.PathLike[str] | None = None,
        dockerfile: str | None = None,
        context: str = ".",
        name: str | None = None,
        cpus: int = 1,
        memory: int = 512,
        disk_mb: int = 1024,
        timeout: float = 300,
        timeout_secs: int | None = None,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        secrets: Iterable[Secret | dict[str, str]] | None = None,
        volumes: dict[str, Volume | tuple[Volume, bool]] | None = None,
        tags: dict[str, str] | None = None,
        fs_dir: str | None = None,
        block_network: bool = False,
        ports: Sequence[int] | None = None,
        egress_allow: Sequence[str] | None = None,
        egress_allow_domains: Sequence[str] | None = None,
        inbound_cidr_allowlist: Sequence[str] | None = None,
        readiness_probe: Any = None,
        pool_size: int = 0,
        remote_page_url: str | None = None,
        remote_page_token: str | None = None,
        remote_page_digest: str | None = None,
    ) -> Sandbox:
        if block_network and ports:
            raise ValueError("ports cannot be exposed when block_network=True")
        cpus = _validate_int_range(cpus, "cpus", maximum=64)
        memory = _validate_int_range(memory, "memory", maximum=64 * 1024, unit=" MiB")
        disk_mb = _validate_int_range(disk_mb, "disk_mb")
        if timeout_secs == 0:
            eff_timeout_secs = None
        else:
            eff_timeout_secs = (
                _validate_timeout_secs(timeout_secs) if timeout_secs is not None else 300
            )
        # A block_network, image-built request with named volumes (no fs_dir) can
        # warm-restore from a slot-provisioned template; an fs_dir-only request can
        # warm-restore from a template with the reserved "host" virtio-fs slot.
        # Restore re-attaches snapshot devices but cannot synthesize devices the
        # template lacks.
        n_vols = len(volumes or {})
        warm_volumes = (
            block_network
            and fs_dir is None
            and template is None
            and 0 < n_vols <= _WARM_VOLUME_SLOTS
        )
        host_slot = block_network and fs_dir is not None and template is None and n_vols == 0
        networked_warm = (
            not block_network
            and platform.system() == "Darwin"
            and template is None
            and fs_dir is None
            and n_vols == 0
            and not ports
            and not egress_allow
            and not egress_allow_domains
            and not inbound_cidr_allowlist
        )
        # Linux networked sandboxes warm-restore from a host-TAP template flavor
        # and rebind the NIC onto a per-sandbox TAP (full host policy at restore).
        networked_warm_linux = (
            not block_network
            and platform.system() != "Darwin"
            and template is None
            and fs_dir is None
            and n_vols == 0
        )
        fs_slots = n_vols if warm_volumes else 0
        template_dir, image_spec, image_ref = cls._resolve_template(
            image=image,
            template=template,
            dockerfile=dockerfile,
            context=context,
            disk_mb=disk_mb,
            timeout=timeout,
            memory=memory,
            cpus=cpus,
            fs_slots=fs_slots,
            host_slot=host_slot,
            nic_slot=networked_warm,
            tap_slot=networked_warm_linux,
        )
        # Modal-parity default: a 5-minute VMM-enforced wall-clock deadline unless
        # the caller sets timeout_secs (0 disables).
        acquired: list[Volume] = []
        volume_specs: list[dict[str, Any]] = []
        vm: MicroVM | None = None
        from_pool = False
        sb: Sandbox | None = None
        net_handle: Any = None
        user_net_sandbox = False
        try:
            used_tags: set[str] = set()
            for mountpoint, value in (volumes or {}).items():
                vol, read_only = cls._coerce_volume(value)
                vol.acquire()
                acquired.append(vol)
                host_dir = vol.ensure()
                tag = cls._unique_volume_tag(vol.tag, used_tags)
                volume_specs.append(
                    {
                        "mountpoint": str(mountpoint),
                        "volume": vol,
                        "tag": tag,
                        "host_dir": str(host_dir),
                        "read_only": read_only,
                    }
                )

            vol_tuples = [
                (spec["tag"], spec["host_dir"], bool(spec["read_only"])) for spec in volume_specs
            ]
            pool: WarmPool | None = None
            if (block_network or networked_warm) and not volume_specs and fs_dir is None:
                key = str(template_dir)
                old_pool: WarmPool | None = None
                with _POOLS_LOCK:
                    pool = _POOLS.get(key)
                    if pool_size > 0 and (pool is None or pool.size != int(pool_size)):
                        old_pool = pool
                        pool = WarmPool(template_dir, int(pool_size))
                        _POOLS[key] = pool
                    if pool is not None:
                        _POOL_REFS[key] = image_ref or str(template_dir)
                if old_pool is not None:
                    old_pool.shutdown()
                if pool is not None:
                    vm = pool.claim()
                    if vm is not None and name is not None:
                        vm.rename(name)
                    from_pool = vm is not None
                    if from_pool and networked_warm:
                        user_net_sandbox = True
            if vm is None and block_network and fs_dir is None and not volume_specs:
                # Fast warm path: a plain block_network sandbox (no NIC and no
                # virtio-fs shares/volumes to enumerate) restores from the
                # template snapshot.
                vm = MicroVM.restore(
                    template_dir,
                    name=name,
                    agent=True,
                    mem=memory,
                    cpus=cpus,
                    timeout_secs=eff_timeout_secs,
                    remote_page_url=remote_page_url,
                    remote_page_token=remote_page_token,
                    remote_page_digest=remote_page_digest,
                )
            elif vm is None and warm_volumes:
                # Warm volume path: restore the slot-provisioned template and rebind
                # each requested volume onto its reserved slot tag. The remap must
                # cover every provisioned slot exactly, else `resolve_backend` falls
                # back to the empty slot-void share baked into the snapshot.
                if len(volume_specs) != fs_slots:
                    raise RuntimeError("warm-volume slot/remap mismatch")
                for i, spec in enumerate(volume_specs):
                    spec["tag"] = slot_tag(i)
                slot_vols = [
                    (spec["tag"], spec["host_dir"], bool(spec["read_only"]))
                    for spec in volume_specs
                ]
                vm = MicroVM.restore(
                    template_dir,
                    name=name,
                    agent=True,
                    mem=memory,
                    cpus=cpus,
                    volumes=slot_vols,
                    timeout_secs=eff_timeout_secs,
                    remote_page_url=remote_page_url,
                    remote_page_token=remote_page_token,
                    remote_page_digest=remote_page_digest,
                )
            elif vm is None and host_slot:
                vm = MicroVM.restore(
                    template_dir,
                    name=name,
                    agent=True,
                    mem=memory,
                    cpus=cpus,
                    fs_dir=fs_dir,
                    timeout_secs=eff_timeout_secs,
                    remote_page_url=remote_page_url,
                    remote_page_token=remote_page_token,
                    remote_page_digest=remote_page_digest,
                )
            elif vm is None and networked_warm:
                vm = MicroVM.restore(
                    template_dir,
                    name=name,
                    agent=True,
                    mem=memory,
                    cpus=cpus,
                    timeout_secs=eff_timeout_secs,
                    remote_page_url=remote_page_url,
                    remote_page_token=remote_page_token,
                    remote_page_digest=remote_page_digest,
                )
                user_net_sandbox = True
            elif vm is None and networked_warm_linux:
                # Linux networked warm: allocate a per-sandbox host TAP + policy,
                # warm-restore the tap-slot template rebinding its NIC onto that TAP,
                # then replay the guest network config (net_handle branch below).
                name = name or _instance_name(Path(str(template_dir)).name or "sandbox")
                net_handle = _setup_sandbox_network(
                    name,
                    ports=ports,
                    egress_allow=egress_allow,
                    egress_allow_domains=egress_allow_domains,
                    inbound_cidr_allowlist=inbound_cidr_allowlist,
                )
                vm = MicroVM.restore(
                    template_dir,
                    name=name,
                    agent=True,
                    mem=memory,
                    cpus=cpus,
                    tap=str(net_handle.guest_config["tap"]),
                    timeout_secs=eff_timeout_secs,
                    remote_page_url=remote_page_url,
                    remote_page_token=remote_page_token,
                    remote_page_digest=remote_page_digest,
                )
            elif vm is None:
                # Fresh copy-on-write overlay boot from the template disk. vmon's
                # restore can only re-attach devices the snapshot already had, so
                # anything needing a device the template lacks must fresh-boot to
                # enumerate it at guest boot: a NIC for networked sandboxes, and
                # virtio-fs devices for fs_dir / volumes (block_network or not).
                name = name or _instance_name(Path(str(template_dir)).name or "sandbox")
                base_disk = Path(template_dir) / "rootfs.img"
                if not base_disk.is_file():
                    raise RuntimeError(
                        f"template {template_dir} has no rootfs.img; fresh-boot "
                        "sandboxes (networked, or with fs_dir/volumes) require a "
                        "disk-backed template"
                    )
                tap: str | None = None
                if not block_network:
                    if platform.system() == "Darwin":
                        for feature, requested in (
                            ("ports", ports),
                            ("egress_allow", egress_allow),
                            ("egress_allow_domains", egress_allow_domains),
                            ("inbound_cidr_allowlist", inbound_cidr_allowlist),
                        ):
                            if requested:
                                raise ValueError(
                                    f"{feature} requires Linux host networking (TAP); "
                                    "macOS user-mode NAT is outbound-egress only"
                                )
                        user_net_sandbox = True
                    else:
                        net_handle = _setup_sandbox_network(
                            name,
                            ports=ports,
                            egress_allow=egress_allow,
                            egress_allow_domains=egress_allow_domains,
                            inbound_cidr_allowlist=inbound_cidr_allowlist,
                        )
                        tap = str(net_handle.guest_config["tap"])
                vm = MicroVM.boot_rootfs(
                    base_disk,
                    name=name,
                    mem=memory,
                    cpus=cpus,
                    overlay=True,
                    rng=True,
                    agent=True,
                    tap=tap,
                    user_net=user_net_sandbox,
                    fs_dir=fs_dir,
                    volumes=vol_tuples,
                    timeout_secs=eff_timeout_secs,
                    snapshot_root=STATE / "snapshots",
                )

            sb = cls(vm, image_spec=image_spec, workdir=workdir, env=env, tags=tags)
            sb._network = net_handle
            sb._secret_env = merge_secrets(secrets)
            sb._volumes = list(acquired)
            sb._block_network = bool(block_network)
            sb._fs_dir = str(fs_dir) if fs_dir is not None else None
            sb._timeout_secs = eff_timeout_secs
            if sb._timeout_secs is not None:
                sb._start_timeout_watchdog(sb._timeout_secs)
            wait_for_agent_ready(sb.vm, timeout=timeout)
            if from_pool and sb._timeout_secs is not None:
                # Pool clones launch without --timeout-secs; arm the VMM-enforced
                # deadline via the extend control op (the host watchdog is a backstop).
                try:
                    sb.extend(sb._timeout_secs)
                except Exception:
                    pass
            for spec in volume_specs:
                sb.agent().mount(spec["tag"], spec["mountpoint"], ro=bool(spec["read_only"]))
            if net_handle is not None:
                gc = net_handle.guest_config
                sb.agent().net_config(
                    gc["guest_ip"],
                    int(gc["prefix"]),
                    gc["host_ip"],
                    gc.get("dns") or [],
                )
                sb._network_policy = {
                    "egress_allow": list(egress_allow) if egress_allow is not None else None,
                    "egress_allow_domains": list(egress_allow_domains)
                    if egress_allow_domains is not None
                    else None,
                    "inbound_cidr_allowlist": list(inbound_cidr_allowlist)
                    if inbound_cidr_allowlist is not None
                    else None,
                }
            elif user_net_sandbox:
                from . import net

                sb.agent().net_config(
                    net.USER_NET_GUEST_IP,
                    net.USER_NET_PREFIX,
                    net.USER_NET_GATEWAY,
                    list(net.USER_NET_DNS),
                )
            volumes_meta = {
                spec["mountpoint"]: {
                    "name": spec["volume"].name,
                    "tag": spec["tag"],
                    "read_only": bool(spec["read_only"]),
                }
                for spec in volume_specs
            }
            vm._save_meta(
                sandbox=True,
                image=image_ref,
                template=str(template_dir),
                workdir=sb.workdir,
                env_names=sorted(sb.env),
                secret_names=sorted(sb._secret_env),
                tags=sb.tags,
                volumes=volumes_meta,
                block_network=block_network,
                timeout_secs=sb._timeout_secs,
            )
            if readiness_probe is not None:
                sb.wait_until_ready(readiness_probe, timeout=timeout)
            return sb
        except BaseException:
            if sb is not None:
                sb.terminate(wait=True)
            else:
                if net_handle is not None:
                    try:
                        net_handle.teardown()
                    except Exception:
                        pass
                if vm is not None:
                    try:
                        vm.stop()
                    finally:
                        for vol in acquired:
                            vol.release()
                else:
                    for vol in acquired:
                        vol.release()
            raise

    @classmethod
    def from_snapshot(
        cls,
        name: str | os.PathLike[str],
        *,
        fork: bool = False,
        sandbox_name: str | None = None,
        timeout: float = 60,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        block_network: bool = False,
        ports: Sequence[int] | None = None,
        egress_allow: Sequence[str] | None = None,
    ) -> Sandbox:
        if block_network and ports:
            raise ValueError("ports cannot be exposed when block_network=True")
        net_handle = None
        sb: Sandbox | None = None
        try:
            if block_network:
                vm = (
                    MicroVM.fork_snapshot(name, name=sandbox_name, agent=True)
                    if fork
                    else MicroVM.restore(name, name=sandbox_name, agent=True)
                )
                sb = cls(vm, workdir=workdir, env=env)
                wait_for_agent_ready(sb.vm, timeout=timeout)
            else:
                if fork:
                    raise ValueError(
                        "networked from_snapshot fork is unsupported; use "
                        "block_network=True or Sandbox.create()"
                    )
                snap_dir = MicroVM._snapshot_dir(name)
                base_disk = snap_dir / "rootfs.img"
                if not base_disk.is_file():
                    raise RuntimeError(f"snapshot {name} has no rootfs.img for a networked boot")
                sandbox_name = sandbox_name or _instance_name(Path(str(name)).name or "snapshot")
                net_handle = _setup_sandbox_network(
                    sandbox_name, ports=ports, egress_allow=egress_allow
                )
                vm = MicroVM.boot_rootfs(
                    base_disk,
                    name=sandbox_name,
                    overlay=True,
                    rng=True,
                    agent=True,
                    tap=str(net_handle.guest_config["tap"]),
                    snapshot_root=STATE / "snapshots",
                )
                sb = cls(vm, workdir=workdir, env=env)
                sb._network = net_handle
                wait_for_agent_ready(sb.vm, timeout=timeout)
                gc = net_handle.guest_config
                sb.agent().net_config(
                    gc["guest_ip"],
                    int(gc["prefix"]),
                    gc["host_ip"],
                    gc.get("dns") or [],
                )
            vm._save_meta(
                sandbox=True,
                restored_snapshot=str(name),
                workdir=sb.workdir,
                env_names=sorted(sb.env),
                tags=sb.tags,
                secret_names=[],
                volumes={},
                block_network=block_network,
            )
            return sb
        except BaseException:
            if sb is not None:
                sb.terminate(wait=True)
            elif net_handle is not None:
                try:
                    net_handle.teardown()
                except Exception:
                    pass
            raise

    @classmethod
    def from_id(cls, id: str) -> Sandbox:
        return cls.attach(id)

    @classmethod
    def attach(cls, name: str) -> Sandbox:
        vm = MicroVM(name)
        meta = vm.meta
        workdir_value = meta.get("workdir")
        workdir = workdir_value if isinstance(workdir_value, str) else None
        env_value = meta.get("env")
        env = cast(dict[str, str], env_value) if isinstance(env_value, dict) else {}
        tags_value = meta.get("tags")
        tags = cast(dict[str, str], tags_value) if isinstance(tags_value, dict) else {}
        return cls(vm, workdir=workdir, env=env, tags=tags)

    @staticmethod
    def _resolve_template(
        *,
        image: str | None,
        template: str | os.PathLike[str] | None,
        dockerfile: str | None,
        context: str,
        disk_mb: int,
        timeout: float,
        memory: int,
        cpus: int,
        fs_slots: int = 0,
        host_slot: bool = False,
        nic_slot: bool = False,
        tap_slot: bool = False,
    ) -> tuple[Path, ImageSpec | None, str | None]:
        if template is not None:
            path = Path(template)
            template_dir = (
                path if path.exists() or path.is_absolute() else STATE / "templates" / str(template)
            )
            return template_dir, None, str(template)
        cached = cached_template(
            image=image,
            dockerfile=dockerfile,
            context=context,
            disk_mb=disk_mb,
            timeout=timeout,
            memory=memory,
            cpus=cpus,
            fs_slots=fs_slots,
            host_slot=host_slot,
            nic_slot=nic_slot,
            tap_slot=tap_slot,
        )
        return cached.snapshot_dir, cached.spec, cached.spec.reference

    @staticmethod
    def _coerce_volume(value: Volume | tuple[Volume, bool]) -> tuple[Volume, bool]:
        if isinstance(value, tuple):
            vol, read_only = value
            if not isinstance(vol, Volume):
                raise TypeError("volume tuple must contain a Volume")
            return vol, bool(read_only)
        if not isinstance(value, Volume):
            raise TypeError("volumes values must be Volume instances")
        return value, False

    @staticmethod
    def _unique_volume_tag(base: str, used: set[str]) -> str:
        stem = "".join(
            ch if ("a" <= ch <= "z" or "0" <= ch <= "9" or ch == "_") else "_"
            for ch in base.lower()
        )
        stem = (stem or "vol")[:32]
        candidate = stem
        suffix = 2
        while candidate in used:
            tail = f"_{suffix}"
            candidate = f"{stem[: 32 - len(tail)]}{tail}"
            suffix += 1
        used.add(candidate)
        return candidate

    def _start_timeout_watchdog(self, secs: int) -> None:
        stop = threading.Event()
        self._watchdog_stop = stop

        def watch() -> None:
            if not stop.wait(max(1, int(secs))):
                self.terminate(wait=False)

        thread = threading.Thread(target=watch, name=f"vmon-timeout-{self.name}", daemon=True)
        self._watchdog_thread = thread
        thread.start()

    def _stop_timeout_watchdog(self) -> None:
        stop = self._watchdog_stop
        thread = self._watchdog_thread
        self._watchdog_stop = None
        self._watchdog_thread = None
        if stop is not None:
            stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.1)

    def agent(self, connect_timeout: float | None = None) -> AgentConn:
        if self._terminated:
            raise AgentClosed("sandbox has been terminated")
        if self._agent is None or self._agent.closed:
            self._agent = self.vm.agent(connect_timeout=connect_timeout)
        return self._agent

    def exec(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        pty: bool = False,
        tty: bool = False,
        _track_entry: bool = True,
    ) -> Process:
        if len(cmd) == 1 and not isinstance(cmd[0], str):
            argv = [str(c) for c in cmd[0]]
        else:
            argv = [str(c) for c in cmd]
        if not argv:
            raise ValueError("exec command must not be empty")
        merged_env = {**self.env, **self._secret_env}
        if env:
            merged_env.update({str(k): str(v) for k, v in env.items()})
        session = self.agent().exec(
            argv, cwd=workdir or self.workdir, env=merged_env, timeout=timeout, tty=bool(pty or tty)
        )
        proc = Process(session)
        if _track_entry and self._entry_process is None:
            self._entry_process = (
                proc  # first user exec is the sandbox "entry" for poll()/returncode
            )
        return proc

    def wait_until_ready(self, probe: Any = None, timeout: float = 300) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout))
        last: BaseException | None = None
        if probe is None:
            wait_for_agent_ready(self.vm, timeout=timeout)
            return
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                if isinstance(probe, int):
                    if self.agent().tcp_probe(int(probe)):
                        return
                elif isinstance(probe, dict) and "port" in probe:
                    host = str(probe.get("host") or "127.0.0.1")
                    if self.agent().tcp_probe(
                        int(probe["port"]), host=host, timeout=min(1.0, remaining)
                    ):
                        return
                else:
                    argv = (
                        ["sh", "-lc", probe] if isinstance(probe, str) else [str(p) for p in probe]
                    )
                    proc = self.exec(argv, timeout=min(5.0, remaining), _track_entry=False)
                    try:
                        if proc.wait(timeout=min(5.0, remaining)) == 0:
                            return
                    except TimeoutError:
                        proc.kill()
                        with contextlib.suppress(Exception):
                            proc.wait(timeout=1.0)
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
            except BaseException as exc:
                last = exc
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        raise TimeoutError(f"sandbox readiness probe timed out after {timeout}s") from last

    def _status(self) -> dict[str, Any] | None:
        paths = [Path(self.vm.control_sock).parent / "status.json", self.vm.dir / "status.json"]
        seen: set[Path] = set()
        for path in paths:
            if path in seen:
                continue
            seen.add(path)
            try:
                data = json.loads(path.read_text())
            except FileNotFoundError, json.JSONDecodeError, OSError:
                continue
            return data if isinstance(data, dict) else None
        return None

    def poll(self) -> int | None:
        if self._entry_returncode is None and self._entry_process is not None:
            rc = self._entry_process.returncode
            if rc is not None:
                self._entry_returncode = rc
        if self._entry_returncode is not None:
            return self._entry_returncode
        status = self._status()
        if status is None:
            return None
        code = status.get("vmm_returncode")
        if isinstance(code, int):
            return code
        reason = status.get("reason")
        if isinstance(reason, str):
            return {"timeout": 124, "quit": 137, "killed": 137, "shutdown": 0}.get(reason)
        return None

    @property
    def returncode(self) -> int | None:
        return self.poll()

    def snapshot(self, name: str | None = None) -> str:
        if self._terminated:
            raise RuntimeError("sandbox has been terminated")
        snap_name = name or f"{self.name}-{int(time.time() * 1000)}"
        self.vm.snapshot(snap_name, keep_running=True, disk_src=self.vm.rootfs_img)
        return snap_name

    def snapshot_filesystem(self, name: str | None = None, ttl: int | None = 30 * 24 * 3600) -> str:
        if self._terminated:
            raise RuntimeError("sandbox has been terminated")
        snap_name = name or f"{self.name}-img-{int(time.time())}"
        # vmon writes snapshot state under the VM's launch --snapshot-root, so
        # capture there (default root) and then relocate the complete snapshot
        # (state + rootfs.img) into the template store, where
        # Sandbox.create(template=...) resolves it by name.
        src_dir = self.vm.snapshot(snap_name, keep_running=True, disk_src=self.vm.rootfs_img)
        dst_dir = STATE / "templates" / snap_name
        dst_dir.parent.mkdir(parents=True, exist_ok=True)
        if src_dir.resolve() != dst_dir.resolve():
            if dst_dir.exists():
                shutil.rmtree(dst_dir)
            os.replace(src_dir, dst_dir)
        meta = {"created_unix": int(time.time()), "ttl": ttl, "image": snap_name}
        (dst_dir / "image.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
        return snap_name

    def terminate(self, wait: bool = True) -> None:
        if self._terminated:
            return
        self._terminated = True
        try:
            if self._agent is not None:
                with contextlib.suppress(Exception):
                    self._agent.close()
            self._teardown_network()
            self.vm.stop(wait=wait)
        finally:
            for vol in self._volumes:
                vol.release()
            self._volumes.clear()

    def tunnels(self) -> dict[int, tuple[str, int]]:
        if self._network is None:
            return {}
        return dict(self._network.tunnels())

    def create_connect_token(self) -> str:
        if self.connect_token is None:
            self.connect_token = _secrets.token_urlsafe(32)
        return self.connect_token

    def extend(self, secs: int) -> dict[str, Any]:
        result = self.vm.control.extend(int(secs))
        # Realign the host watchdog backstop with the new VMM deadline so it does
        # not terminate the sandbox at the original (now-stale) deadline.
        self._timeout_secs = int(secs)
        self._stop_timeout_watchdog()
        self._start_timeout_watchdog(int(secs))
        return result if isinstance(result, dict) else {}

    def metrics(self) -> dict[str, Any]:
        """Live additive runtime counters from the VMM control API."""
        return dict(self.vm.control.metrics() or {})

    def set_network_policy(
        self,
        block_network: bool | None = None,
        cidr_allow: Sequence[str] | None = None,
        domain_allow: Sequence[str] | None = None,
    ) -> None:
        if self._network is None:
            return
        from . import net

        cfg = self._network.guest_config
        tap = str(cfg["tap"])
        guest_ip = str(cfg["guest_ip"])
        host_ip = str(cfg["host_ip"])
        prefix = int(cfg["prefix"])
        if block_network is True:
            allow_list: list[str] | None = []
        elif cidr_allow is not None:
            allow_list = list(cidr_allow)
        else:
            allow_list = self._network_policy.get("egress_allow")
        domain_list = (
            list(domain_allow)
            if domain_allow is not None
            else self._network_policy.get("egress_allow_domains")
        )
        net.setup_tap(
            tap,
            guest_ip,
            host_ip,
            prefix,
            egress_allow=allow_list,
            egress_allow_domains=domain_list,
        )
        self._network_policy.update(
            {
                "block_network": bool(block_network) if block_network is not None else False,
                "egress_allow": allow_list,
                "egress_allow_domains": domain_list,
            }
        )

    @property
    def aio(self) -> _AsyncSandbox:
        return _AsyncSandbox(self)

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.terminate()

    def _teardown_network(self) -> None:
        network = self._network
        self._network = None
        if network is not None:
            network.teardown()


class _AsyncSandbox:
    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    async def exec(self, *cmd: str | Iterable[str], **kwargs: Any) -> Process:
        return await asyncio.to_thread(self._sandbox.exec, *cmd, **kwargs)

    async def terminate(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.terminate, *args, **kwargs)

    async def wait_until_ready(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.wait_until_ready, *args, **kwargs)

    async def snapshot_filesystem(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot_filesystem, *args, **kwargs)

    async def poll(self) -> int | None:
        return await asyncio.to_thread(self._sandbox.poll)

    async def set_network_policy(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(self._sandbox.set_network_policy, *args, **kwargs)


_REMOTE_FUNCTION_RUNNER = r"""
import asyncio
import inspect
import io
import json
import sys
import traceback

payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
namespace = {"__builtins__": __builtins__, "__name__": "__vmon_remote_function__"}
response = {"ok": False}
captured_stdout = io.StringIO()
real_stdout = sys.stdout
try:
    exec(payload["source"], namespace)
    fn = namespace[payload["name"]]
    sys.stdout = captured_stdout
    result = fn(*payload["args"], **payload["kwargs"])
    if inspect.isawaitable(result):
        result = asyncio.run(result)
    sys.stdout = real_stdout
    try:
        json.dumps(result)
    except TypeError as exc:
        raise TypeError("remote function result must be JSON-serializable") from exc
    response = {"ok": True, "result": result, "stdout": captured_stdout.getvalue()}
except BaseException as exc:
    sys.stdout = real_stdout
    response = {
        "ok": False,
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
        "stdout": captured_stdout.getvalue(),
    }
sys.stdout.buffer.write(json.dumps(response, separators=(",", ":")).encode("utf-8"))
"""


class RemoteFunctionError(RuntimeError):
    """A remote function call failed inside the sandbox."""

    def __init__(self, message: str, *, remote_type: str = "", traceback_text: str = "") -> None:
        super().__init__(message)
        self.remote_type = remote_type
        self.traceback = traceback_text


class RemoteFunction:
    """A source-serialized Python function that can run inside a warm sandbox."""

    def __init__(self, fn: Callable[..., Any], sandbox_kwargs: dict[str, Any]) -> None:
        self._fn = fn
        self._sandbox_kwargs = dict(sandbox_kwargs)
        self._sandbox: Sandbox | None = None
        self._python_checked = False
        self._lock = threading.Lock()
        self._runner_path = "/tmp/vmon-function-runner.py"
        functools.update_wrapper(self, fn)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._fn(*args, **kwargs)

    def remote(self, *args: Any, timeout: float | None = None, **kwargs: Any) -> Any:
        """Run the function in a sandbox and return its deserialized result."""
        sandbox = self._ensure_sandbox()
        sandbox.filesystem.write_text(self._runner_path, _REMOTE_FUNCTION_RUNNER)
        payload = _remote_function_payload(self._fn, args, kwargs)
        proc = sandbox.exec(
            "python3",
            "-u",
            self._runner_path,
            timeout=timeout,
            _track_entry=False,
        )
        proc.write_stdin(payload)
        proc.close_stdin()
        rc = proc.wait(timeout=timeout)
        stdout = proc.stdout.read()
        stderr = proc.stderr.read().decode("utf-8", "replace")
        if rc != 0:
            detail = stderr.strip() or f"remote function runner exited with {rc}"
            raise RemoteFunctionError(detail)
        return _remote_function_result(stdout)

    def terminate(self) -> None:
        """Terminate the cached sandbox backing this function, if it exists."""
        with self._lock:
            sandbox = self._sandbox
            self._sandbox = None
        if sandbox is not None:
            sandbox.terminate()

    def _ensure_sandbox(self) -> Sandbox:
        with self._lock:
            if self._sandbox is None or self._sandbox.poll() is not None:
                kwargs = {"image": "python:3.14-slim", **self._sandbox_kwargs}
                self._sandbox = Sandbox.create(**kwargs)
                self._python_checked = False
            sandbox = self._sandbox
            if not self._python_checked:
                self._check_python(sandbox)
                self._python_checked = True
            return sandbox

    @staticmethod
    def _check_python(sandbox: Sandbox) -> None:
        proc = sandbox.exec("python3", "--version", timeout=10, _track_entry=False)
        rc = proc.wait(timeout=10)
        if rc != 0:
            raise RemoteFunctionError(
                "remote function images must provide python3; pass image='python:3.14-slim' "
                "or another Python-capable image"
            )


def _remote_function_payload(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> bytes:
    if fn.__closure__:
        raise ValueError("remote functions cannot close over local variables")
    try:
        source = inspect.getsource(fn)
    except OSError as exc:
        raise ValueError("remote functions must be defined in source files") from exc
    source = _strip_decorators(textwrap.dedent(source))
    payload = {"source": source, "name": fn.__name__, "args": args, "kwargs": kwargs}
    try:
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")
    except TypeError as exc:
        raise ValueError("remote function arguments must be JSON-serializable") from exc


def _strip_decorators(source: str) -> str:
    lines = source.splitlines()
    while lines and lines[0].lstrip().startswith("@"):
        lines.pop(0)
    return "\n".join(lines) + "\n"


def _remote_function_result(raw: bytes) -> Any:
    try:
        response = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RemoteFunctionError("remote function returned an invalid response") from exc
    if not isinstance(response, dict):
        raise RemoteFunctionError("remote function returned a non-object response")
    if response.get("ok") is True:
        return response.get("result")
    remote_type = str(response.get("type") or "RemoteError")
    message = str(response.get("message") or "remote function failed")
    traceback_text = str(response.get("traceback") or "")
    raise RemoteFunctionError(message, remote_type=remote_type, traceback_text=traceback_text)


def function(fn: Callable[..., Any] | None = None, **sandbox_kwargs: Any) -> Any:
    """Decorate a source-available function with a ``.remote(...)`` sandbox call."""

    def decorate(inner: Callable[..., Any]) -> RemoteFunction:
        return RemoteFunction(inner, sandbox_kwargs)

    if fn is not None:
        return decorate(fn)
    return decorate


def default_command(spec: ImageSpec | None, override: list[str] | None = None) -> list[str]:
    if spec is None:
        return list(override or ["/bin/sh"])
    argv = spec.argv(override)
    return argv or ["/bin/sh"]
