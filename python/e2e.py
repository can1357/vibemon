#!/usr/bin/env python3.14
"""End-to-end smoke test for the vmon / vmon sandbox stack.

Boots real microVMs on a Linux/KVM or macOS/HVF host and exercises every
user-facing feature in a single run:

  * exec (argv / env / workdir) and the filesystem RPC facade
  * pty exec (isatty + resize)
  * suspend + resume (vCPU parking proven via guest-clock continuity)
  * snapshot + warm restore (disk + memory state)
  * copy-on-write fork (shared base, isolated writes)
  * delta / incremental snapshots (smaller memory file, base+delta restore)
  * writable and read-only virtio-fs volumes that persist across sandboxes
  * read-only host directory shares (``fs_dir``)
  * secrets (injected into exec env, never written to meta.json)
  * networking: ``block_network`` egress denial, guest IP/route over a host TAP,
    exposed-port tunnels, and host-side IP lease allocation
  * tags + listing
  * warm pools (pre-forked clones)
  * VMM-enforced timeouts (return code 124) and live deadline ``extend``
  * the async (``aio``) facade

This complements the pytest suite in ``tests/test_e2e.py`` with a single
self-contained driver that prints a PASS/FAIL/SKIP table and returns a
non-zero exit code if anything fails.

Prerequisites (a Linux/KVM or macOS/HVF host; e.g. bare metal, an Apple-silicon
Mac, or a nested-KVM Lima VM):
  * a usable hypervisor          (Linux ``/dev/kvm`` or macOS Hypervisor.framework)
  * a built vmm binary           (``cargo build --release`` or ``VMON_BIN``)
  * a static guest agent         (``just agent-musl`` or ``VMON_AGENT``)
  * a guest kernel               (``/boot/vmlinuz-$(uname -r)``, ``VMON_KERNEL``,
                                  or auto-downloaded into ``$VMON_HOME`` on macOS)
  * ``docker`` or ``podman``     (to build the image rootfs)
  * ``mkfs.ext4`` / ``mke2fs``   (e2fsprogs; ``brew install e2fsprogs`` on macOS)

Note: feature sandboxes below boot with ``block_network=True`` (no NIC, no root).
The dedicated networking + tunnel tests opt into a networked sandbox bound to a
host TAP, which is Linux-only and needs root / ``CAP_NET_ADMIN`` -- they SKIP on
macOS or without that capability.

Usage:
  python3.14 python/e2e.py                 # run everything
  python3.14 python/e2e.py exec snapshot   # run a subset (substring match)
  python3.14 python/e2e.py --list          # list test names
  python3.14 python/e2e.py --keep          # leave volumes/snapshots in place
  VMON_E2E_IMAGE=alpine:latest python3.14 python/e2e.py
"""

import argparse
import asyncio
import json
import os
import platform
import shutil
import socket
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from vmon.agent_client import AgentError
from vmon.image import _find_mkfs_ext4, cached_template, detect_engine, find_agent_binary
from vmon.pool import WarmPool
from vmon.sandbox import Sandbox
from vmon.secret import Secret
from vmon.vmm import STATE, MicroVM, default_kernel, find_binary, hypervisor_present
from vmon.volume import Volume

IMAGE = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
RUN = f"e2e{os.getpid()}"

_seq = 0
_template: Path | None = None
_virtiofs: bool | None = None


# --------------------------------------------------------------------------- #
# harness
# --------------------------------------------------------------------------- #


class Skip(Exception):
    """Raised by a test to report it cannot run in this environment."""


TESTS: list[Callable[[object], None]] = []
VOLUMES: set[str] = set()


def e2e(fn: Callable[[object], None]) -> Callable[[object], None]:
    """Register a test, preserving definition order."""
    TESTS.append(fn)
    return fn


def uid(prefix: str) -> str:
    """A per-run unique, snapshot/volume-name-safe identifier."""
    global _seq
    _seq += 1
    return f"{prefix}-{RUN}-{_seq}"


def template_dir() -> Path:
    """Build (once) and return the cached image template snapshot directory."""
    global _template
    if _template is None:
        _template = cached_template(image=IMAGE).snapshot_dir
    return _template


def make_sandbox(**kw: object) -> Sandbox:
    """Create a sandbox, defaulting to no network.

    Feature tests boot with ``block_network=True`` since they don't need
    networking; the dedicated networking/tunnel tests opt in explicitly.
    """
    kw.setdefault("block_network", True)
    return Sandbox.create(image=IMAGE, **kw)


def run(
    sb: Sandbox,
    *argv: str,
    timeout: float = 30,
    pty: bool = False,
    env: dict[str, str] | None = None,
    workdir: str | None = None,
) -> tuple[int | None, str, str]:
    """Exec ``argv`` in ``sb``; return ``(returncode, stdout, stderr)`` as text."""
    proc = sb.exec(*argv, timeout=timeout, pty=pty, env=env, workdir=workdir, _track_entry=False)
    try:
        rc = proc.wait(timeout=timeout)
    except TimeoutError:
        proc.kill()
        rc = None
    out = proc.stdout.read().decode("utf-8", "replace")
    err = proc.stderr.read().decode("utf-8", "replace")
    return rc, out, err


def uptime(sb: Sandbox) -> float:
    """Guest uptime (seconds) from ``/proc/uptime``."""
    _, out, _ = run(sb, "cat", "/proc/uptime", timeout=15)
    return float(out.split()[0])


def snapshot_memory_bytes(snap_dir: Path) -> int:
    """Size of the current generation's memory file (delta vs full comparison)."""
    gen = (snap_dir / "current-generation").read_text().strip()
    return (snap_dir / f"memory.{gen}.bin").stat().st_size


def read_status(vm: MicroVM) -> dict[str, object] | None:
    """The VMM's exit ``status.json`` (beside the control socket), or None."""
    for p in (Path(vm.control_sock).parent / "status.json", vm.dir / "status.json"):
        try:
            return json.loads(p.read_text())
        except FileNotFoundError, json.JSONDecodeError, OSError:
            continue
    return None


def wait_status(vm: MicroVM, deadline: float) -> dict[str, object] | None:
    """Poll for a finished ``status.json`` (one with a ``reason``) until deadline."""
    end = time.time() + deadline
    while time.time() < end:
        s = read_status(vm)
        if s is not None and s.get("reason"):
            return s
        time.sleep(0.2)
    return read_status(vm)


def rm_snapshot(name: str) -> None:
    for root in (STATE / "snapshots", STATE / "templates"):
        shutil.rmtree(root / name, ignore_errors=True)


def net_admin() -> bool:
    """True if the host can create TAP devices (Linux + root/CAP_NET_ADMIN).

    Host-side TAP/iptables networking is Linux-only; macOS sandboxes would use
    the VMM's user-mode NAT, which the Python SDK does not drive, so the
    networking tests SKIP there.
    """
    if platform.system() != "Linux":
        return False
    try:
        from vmon import net

        return bool(net.has_net_admin())
    except Exception:
        return False


def virtiofs_supported() -> bool:
    """True if the guest kernel can mount virtio-fs (cached one-shot probe).

    The auto-provisioned firecracker-ci kernel is built without
    ``CONFIG_VIRTIO_FS``, so volume / host-share mounts fail with ENODEV; set
    ``VMON_KERNEL`` to a virtio-fs-capable kernel to exercise them. Probed by
    attaching a throwaway host share and trying to mount it.
    """
    global _virtiofs
    if _virtiofs is not None:
        return _virtiofs
    share = Path(tempfile.mkdtemp(prefix="vmon-vfs-probe-"))
    sb = make_sandbox(fs_dir=str(share))
    try:
        sb.agent().mount("host", "/mnt/_vfs_probe", ro=True)
        _virtiofs = True
    except AgentError as exc:
        # ENODEV ("no such device") == the guest kernel registers no virtiofs
        # filesystem type. Only that means "unsupported"; surface any other
        # failure so a real mount/agent regression is not masked as a skip.
        msg = str(exc).lower()
        if "os error 19" not in msg and "no such device" not in msg:
            raise
        _virtiofs = False
    finally:
        sb.terminate()
        shutil.rmtree(share, ignore_errors=True)
    return _virtiofs


def require_virtiofs() -> None:
    """Skip the calling test unless the guest kernel can mount virtio-fs."""
    if not virtiofs_supported():
        raise Skip("guest kernel lacks virtio-fs; set VMON_KERNEL to a virtio-fs-capable kernel")


# --------------------------------------------------------------------------- #
# tests
# --------------------------------------------------------------------------- #


@e2e
def t_exec(_):
    """exec returns stdout + exit code; env and workdir are honored."""
    sb = make_sandbox()
    try:
        rc, out, _ = run(sb, "sh", "-lc", "printf ok")
        assert rc == 0 and out == "ok", f"rc={rc} out={out!r}"
        _, env_out, _ = run(sb, "sh", "-lc", 'printf %s "$E2E"', env={"E2E": "val"})
        assert env_out == "val", f"env not injected: {env_out!r}"
        _, wd_out, _ = run(sb, "pwd", workdir="/tmp")
        assert wd_out.strip() == "/tmp", f"workdir ignored: {wd_out!r}"
    finally:
        sb.terminate()


@e2e
def t_pty(_):
    """pty exec presents a tty on stdout; plain exec does not; resize is accepted."""
    sb = make_sandbox()
    try:
        _, tty, _ = run(sb, "sh", "-lc", "test -t 1 && echo TTY || echo NOTTY", pty=True)
        assert "TTY" in tty and "NOTTY" not in tty, f"pty stdout not a tty: {tty!r}"
        _, notty, _ = run(sb, "sh", "-lc", "test -t 1 && echo TTY || echo NOTTY")
        assert "NOTTY" in notty, f"plain exec unexpectedly a tty: {notty!r}"
        proc = sb.exec("sh", pty=True, timeout=15, _track_entry=False)
        proc.resize(40, 120)
        proc.write_stdin("exit\n")
        assert proc.wait(timeout=15) is not None
    finally:
        sb.terminate()


@e2e
def t_filesystem(_):
    """Filesystem RPC: mkdir, write/read round-trip, list, stat, remove."""
    sb = make_sandbox()
    try:
        fs = sb.filesystem
        fs.make_directory("/e2e/sub")
        fs.write_text("/e2e/sub/file.txt", "hello-fs")
        assert fs.read_text("/e2e/sub/file.txt") == "hello-fs"
        assert "file.txt" in json.dumps(fs.list_files("/e2e/sub"))
        assert isinstance(fs.stat("/e2e/sub/file.txt"), dict)
        fs.remove("/e2e/sub/file.txt")
        try:
            fs.read_text("/e2e/sub/file.txt")
            raise AssertionError("read of a removed file should fail")
        except OSError:
            pass
    finally:
        sb.terminate()


@e2e
def t_suspend_resume(_):
    """pause parks the vCPUs (guest clock stalls) and resume continues execution."""
    sb = make_sandbox()
    try:
        sb.filesystem.write_text("/root/pre-pause", "kept")
        u0 = uptime(sb)
        t0 = time.time()
        sb.vm.pause()
        time.sleep(3)
        sb.vm.resume()
        host = time.time() - t0
        guest = uptime(sb) - u0
        assert guest < host - 2, f"guest advanced {guest:.2f}s over {host:.2f}s host; not parked"
        assert sb.filesystem.read_text("/root/pre-pause") == "kept"
        rc, out, _ = run(sb, "sh", "-lc", "echo alive")
        assert rc == 0 and "alive" in out
    finally:
        sb.terminate()


@e2e
def t_snapshot_restore(_):
    """snapshot then warm restore preserves disk state and continues the guest clock."""
    sb = make_sandbox(memory=256)
    name = uid("snap")
    restored = None
    try:
        sb.filesystem.write_text("/root/marker", "snapshotted")
        # Grow guest uptime well past a fresh boot so a cold reboot (uptime ~0)
        # cannot masquerade as a warm restore.
        deadline = time.time() + 20
        u0 = uptime(sb)
        while u0 < 6 and time.time() < deadline:
            time.sleep(0.5)
            u0 = uptime(sb)
        assert u0 >= 6, f"guest uptime never reached 6s (got {u0:.1f}); cannot prove restore"
        sb.snapshot(name)
        restored = Sandbox.from_snapshot(name, block_network=True)
        assert restored.filesystem.read_text("/root/marker") == "snapshotted"
        u1 = uptime(restored)
        assert u1 >= u0 - 1, (
            f"restored uptime {u1:.1f} < pre-snapshot {u0:.1f} - 1; cold boot, not restore"
        )
        rc, out, _ = run(restored, "sh", "-lc", "echo warm")
        assert rc == 0 and "warm" in out
    finally:
        if restored is not None:
            restored.terminate()
        sb.terminate()
        rm_snapshot(name)


@e2e
def t_fork(_):
    """CoW fork: clones share the base image but writes stay private to each clone."""
    sb = make_sandbox(memory=256)
    name = uid("forkbase")
    clones: list[Sandbox] = []
    try:
        sb.filesystem.write_text("/root/shared", "base")
        sb.snapshot(name)
        clones = [Sandbox.from_snapshot(name, fork=True, block_network=True) for _ in range(2)]
        for c in clones:
            assert c.filesystem.read_text("/root/shared") == "base"
        clones[0].filesystem.write_text("/root/only0", "c0")
        try:
            clones[1].filesystem.read_text("/root/only0")
            raise AssertionError("fork clones are not CoW-isolated")
        except OSError:
            pass
    finally:
        for c in clones:
            try:
                c.terminate()
            except Exception:
                pass
        sb.terminate()
        rm_snapshot(name)


@e2e
def t_delta_snapshot(_):
    """A delta snapshot stores only changed pages (smaller) and restores base+delta."""
    sb = make_sandbox(memory=256)
    base, delta = uid("dbase"), uid("ddelta")
    restored = None
    try:
        base_dir = sb.vm.snapshot(base, keep_running=True, disk_src=sb.vm.rootfs_img)
        # Mutate state after the base snapshot so the delta carries real content.
        rc, _, err = run(sb, "sh", "-lc", "echo post-base > /root/fill && sync")
        assert rc == 0, f"post-base mutation failed: {err!r}"
        delta_dir = sb.vm.snapshot(delta, keep_running=True, disk_src=sb.vm.rootfs_img, base=base)
        base_bytes = snapshot_memory_bytes(base_dir)
        delta_bytes = snapshot_memory_bytes(delta_dir)
        assert delta_bytes < base_bytes, f"delta memory {delta_bytes} not < base {base_bytes}"
        restored = Sandbox.from_snapshot(delta, block_network=True)
        assert restored.filesystem.read_text("/root/fill").strip() == "post-base", (
            "post-base state not visible after delta restore"
        )
        rc, out, _ = run(restored, "sh", "-lc", "echo delta-ok")
        assert rc == 0 and "delta-ok" in out
    finally:
        if restored is not None:
            restored.terminate()
        sb.terminate()
        rm_snapshot(delta)
        rm_snapshot(base)


@e2e
def t_snapshot_filesystem_image(_):
    """snapshot_filesystem yields a template that Sandbox.create(template=...) can boot."""
    sb = make_sandbox(memory=256)
    img = uid("img")
    clone = None
    try:
        sb.filesystem.write_text("/root/img-marker", "image")
        sb.snapshot_filesystem(img)
        clone = make_sandbox(template=img)
        assert clone.filesystem.read_text("/root/img-marker") == "image"
    finally:
        if clone is not None:
            clone.terminate()
        sb.terminate()
        rm_snapshot(img)


@e2e
def t_volume_persist(_):
    """A writable named volume keeps data across sandbox lifetimes."""
    require_virtiofs()
    name = uid("vol")
    VOLUMES.add(name)
    sb1 = make_sandbox(volumes={"/data": Volume(name)})
    try:
        rc, _, err = run(sb1, "sh", "-lc", "echo persisted > /data/x && sync")
        assert rc == 0, f"volume write failed: {err!r}"
    finally:
        sb1.terminate()
    sb2 = make_sandbox(volumes={"/data": Volume(name)})
    try:
        assert sb2.filesystem.read_text("/data/x").strip() == "persisted"
    finally:
        sb2.terminate()


@e2e
def t_volume_readonly(_):
    """A read-only volume is readable but rejects writes."""
    require_virtiofs()
    name = uid("rovol")
    VOLUMES.add(name)
    sb = make_sandbox(volumes={"/data": Volume(name)})
    try:
        run(sb, "sh", "-lc", "echo seed > /data/seed && sync")
    finally:
        sb.terminate()
    sb2 = make_sandbox(volumes={"/data": (Volume(name), True)})
    try:
        assert sb2.filesystem.read_text("/data/seed").strip() == "seed"
        rc, _, _ = run(sb2, "sh", "-lc", "echo nope > /data/seed2")
        assert rc != 0, "write to a read-only volume unexpectedly succeeded"
    finally:
        sb2.terminate()


@e2e
def t_host_share(_):
    """A read-only host directory is visible inside the guest over virtio-fs."""
    require_virtiofs()
    share = Path(tempfile.mkdtemp(prefix="e2e-share-"))
    (share / "hello.txt").write_text("from-host")
    sb = make_sandbox(fs_dir=str(share))
    try:
        sb.agent().mount("host", "/mnt/host", ro=True)
        assert sb.filesystem.read_text("/mnt/host/hello.txt") == "from-host"
    finally:
        sb.terminate()
        shutil.rmtree(share, ignore_errors=True)


@e2e
def t_secrets(_):
    """Secrets reach the exec env but are never written into meta.json."""
    value = "s3cr3t-" + RUN
    sb = make_sandbox(secrets=[Secret.from_dict({"E2E_SECRET": value})])
    try:
        _, out, _ = run(sb, "sh", "-lc", 'printf %s "$E2E_SECRET"')
        assert out == value, f"secret not injected: {out!r}"
        assert value not in sb.vm.meta_path.read_text(), "secret value leaked into meta.json"
        assert "E2E_SECRET" in (sb.vm.meta.get("secret_names") or []), "secret name not recorded"
    finally:
        sb.terminate()


@e2e
def t_network_block(_):
    """A block_network sandbox boots and exec works, but has no external egress."""
    sb = make_sandbox(block_network=True)
    try:
        rc, out, _ = run(sb, "sh", "-lc", "echo up")
        assert rc == 0 and "up" in out, "block_network sandbox failed to boot/exec"
        t0 = time.time()
        rc, _, _ = run(sb, "sh", "-lc", "wget -T 3 -q -O- http://example.com", timeout=12)
        assert rc != 0, "egress unexpectedly succeeded in a block_network sandbox"
        assert time.time() - t0 < 12, "egress attempt did not fail fast"
    finally:
        sb.terminate()


@e2e
def t_network_lease(_):
    """Host-side networking allocates a deterministic, non-overlapping guest lease."""
    from vmon import net

    name = uid("net")
    cfg = net.allocate_guest_config(name)
    try:
        for key in ("tap", "guest_ip", "host_ip", "prefix"):
            assert key in cfg, f"lease missing {key}: {cfg}"
        assert cfg["guest_ip"] != cfg["host_ip"], f"guest/host IP collision: {cfg}"
    finally:
        net.release_guest_config(name)


@e2e
def t_networking(_):
    """A networked sandbox fresh-boots with a guest IPv4 address and default route."""
    if not net_admin():
        raise Skip("needs root/CAP_NET_ADMIN for TAP networking")
    sb = Sandbox.create(image=IMAGE, block_network=False)
    try:
        _, addr, _ = run(sb, "ip", "-4", "addr", "show", "eth0")
        assert "inet " in addr, f"eth0 has no IPv4: {addr!r}"
        _, route, _ = run(sb, "ip", "route")
        assert "default" in route, f"no default route: {route!r}"
    finally:
        sb.terminate()


@e2e
def t_ports_tunnels(_):
    """An exposed guest port is reachable end-to-end through the host tunnel."""
    if not net_admin():
        raise Skip("needs root/CAP_NET_ADMIN for TAP networking")
    sb = Sandbox.create(image=IMAGE, ports=[18080])
    listener = None
    try:
        tunnels = sb.tunnels()
        assert 18080 in tunnels, f"port 18080 not tunneled: {tunnels}"
        host, hport = tunnels[18080]
        assert host and int(hport) > 0, tunnels
        assert isinstance(sb.create_connect_token(), str) and sb.create_connect_token()
        # Serve a known response inside the guest, then fetch it through the tunnel.
        listener = sb.exec(
            "sh",
            "-lc",
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nhi'"
            " | nc -l -p 18080; done",
            timeout=60,
            _track_entry=False,
        )
        deadline = time.time() + 10
        while time.time() < deadline and not sb.agent().tcp_probe(18080):
            time.sleep(0.3)
        assert sb.agent().tcp_probe(18080), "guest listener never bound :18080"
        with socket.create_connection((host, int(hport)), timeout=5) as conn:
            conn.sendall(b"GET / HTTP/1.0\r\n\r\n")
            conn.settimeout(5)
            body = b""
            try:
                while True:
                    chunk = conn.recv(256)
                    if not chunk:
                        break
                    body += chunk
            except TimeoutError:
                pass
        assert b"hi" in body, f"tunnel did not deliver the guest response: {body!r}"
    finally:
        if listener is not None:
            try:
                listener.kill()
            except Exception:
                pass
        sb.terminate()


@e2e
def t_tags_and_list(_):
    """Tags persist on the sandbox/meta and the VM appears in MicroVM.list()."""
    sb = make_sandbox(tags={"e2e": "yes", "run": RUN})
    try:
        assert sb.tags.get("e2e") == "yes"
        assert sb.vm.meta.get("tags", {}).get("run") == RUN
        assert sb.name in [vm.name for vm in MicroVM.list()]
    finally:
        sb.terminate()


@e2e
def t_warm_pool(_):
    """A warm pool pre-forks clones that claim instantly and run exec."""
    pool = WarmPool(template_dir(), 2)
    claimed = None
    try:
        end = time.time() + 60
        while time.time() < end and pool.stats()["ready_count"] < 2:
            time.sleep(0.1)
        assert pool.stats()["ready_count"] >= 1, f"pool never warmed: {pool.stats()}"
        vm = pool.claim()
        assert vm is not None, "claim returned None despite ready clones"
        assert pool.stats()["hits"] >= 1
        claimed = Sandbox(vm)
        rc, out, _ = run(claimed, "sh", "-lc", "echo pooled")
        assert rc == 0 and "pooled" in out
    finally:
        if claimed is not None:
            claimed.terminate()
        pool.shutdown()


@e2e
def t_timeout(_):
    """A VMM-enforced deadline self-terminates with reason=timeout, return code 124."""
    vm = MicroVM.restore(template_dir(), name=uid("timeout"), agent=False, timeout_secs=3)
    try:
        status = wait_status(vm, deadline=20)
        assert status is not None, "no status.json after the deadline"
        assert status.get("reason") == "timeout", f"reason={status.get('reason')!r}"
        assert status.get("vmm_returncode") == 124, f"return code={status.get('vmm_returncode')!r}"
    finally:
        try:
            vm.stop()
        except Exception:
            pass
        vm.remove()


@e2e
def t_extend(_):
    """extend moves the deadline so the VM survives past its original timeout."""
    vm = MicroVM.restore(template_dir(), name=uid("extend"), agent=False, timeout_secs=4)
    try:
        vm.control.extend(60)
        time.sleep(7)  # past the original 4s deadline
        assert vm.is_running(), "VM died at the original deadline despite extend"
        status = read_status(vm)
        assert status is None or status.get("reason") != "timeout", f"unexpected timeout: {status}"
    finally:
        try:
            vm.stop()
        except Exception:
            pass
        vm.remove()


@e2e
def t_async(_):
    """The aio facade mirrors the sync SDK for exec."""
    sb = make_sandbox()
    try:

        async def go():
            proc = await sb.aio.exec("sh", "-lc", "printf aio")
            rc = proc.wait(timeout=15)
            return rc, proc.stdout.read().decode()

        rc, out = asyncio.run(go())
        assert rc == 0 and out == "aio", f"rc={rc} out={out!r}"
    finally:
        sb.terminate()


# --------------------------------------------------------------------------- #
# runner
# --------------------------------------------------------------------------- #


def preflight() -> str | None:
    """Return a human-readable reason the e2e cannot run, or None when ready."""
    system = platform.system()
    if system not in ("Linux", "Darwin"):
        return f"needs Linux + /dev/kvm or macOS + Hypervisor.framework; this is {system}"
    if not hypervisor_present():
        return (
            "/dev/kvm not present; run on a KVM host (or a nested-KVM Lima VM)"
            if system == "Linux"
            else "no usable hypervisor; needs an Apple-silicon Mac with "
            "Hypervisor.framework (kern.hv_support=1)"
        )
    for probe, hint in (
        (find_binary, "vmm binary"),
        (default_kernel, "guest kernel"),
        (detect_engine, "container engine"),
        (find_agent_binary, "static guest agent"),
    ):
        try:
            probe()
        except RuntimeError as exc:
            return f"{hint}: {exc}"
    if _find_mkfs_ext4() is None:
        return "mkfs.ext4/mke2fs not found (install e2fsprogs; on macOS: `brew install e2fsprogs`)"
    return None


def display_name(fn: Callable[[object], None]) -> str:
    return fn.__name__[2:] if fn.__name__.startswith("t_") else fn.__name__


def main(argv: Sequence[str] | None = None) -> int:
    global IMAGE
    ap = argparse.ArgumentParser(description="vmon / vmon end-to-end smoke test")
    ap.add_argument("tests", nargs="*", help="substring filters (default: run all)")
    ap.add_argument("--list", action="store_true", help="list test names and exit")
    ap.add_argument("--keep", action="store_true", help="leave volumes/snapshots on exit")
    ap.add_argument("--image", help="container image (default $VMON_E2E_IMAGE or alpine:latest)")
    args = ap.parse_args(argv)

    if args.list:
        for fn in TESTS:
            print(display_name(fn))
        return 0
    if args.image:
        IMAGE = args.image

    selected = [
        fn for fn in TESTS if not args.tests or any(f in display_name(fn) for f in args.tests)
    ]

    reason = preflight()
    if reason:
        print(f"SKIP: {reason}")
        return 0

    print(f"e2e: image={IMAGE} state={STATE} tests={len(selected)}")

    results: list[tuple[str, str, float, str]] = []
    for fn in selected:
        name = display_name(fn)
        t0 = time.time()
        try:
            fn(None)
            dt = time.time() - t0
            results.append(("PASS", name, dt, ""))
            print(f"  PASS  {name}  ({dt:.1f}s)")
        except Skip as exc:
            results.append(("SKIP", name, time.time() - t0, str(exc)))
            print(f"  SKIP  {name}: {exc}")
        except BaseException as exc:  # noqa: BLE001 - report and continue
            results.append(("FAIL", name, time.time() - t0, repr(exc)))
            print(f"  FAIL  {name}: {exc}")
            traceback.print_exc()

    if not args.keep:
        for name in VOLUMES:
            shutil.rmtree(STATE / "volumes" / name, ignore_errors=True)

    npass = sum(r[0] == "PASS" for r in results)
    nfail = sum(r[0] == "FAIL" for r in results)
    nskip = sum(r[0] == "SKIP" for r in results)
    print(f"\n{'=' * 52}\n{npass} passed, {nfail} failed, {nskip} skipped")
    if nfail:
        print("failed: " + ", ".join(r[1] for r in results if r[0] == "FAIL"))
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())
