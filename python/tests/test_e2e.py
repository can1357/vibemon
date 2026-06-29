import math
import os
from pathlib import Path

import pytest

from vmon import function

pytestmark = pytest.mark.skipif(
    not os.environ.get("VMON_KVM_E2E"),
    reason="needs a real-VM e2e host (Linux/KVM or macOS/HVF)",
)


def _image():
    return os.environ.get("VMON_E2E_IMAGE", "alpine:latest")


def _read_stdout(process):
    process.wait(timeout=30)
    data = process.stdout.read()
    return data.decode("utf-8", errors="replace") if isinstance(data, bytes) else str(data)


def test_real_sandbox_create_and_exec_roundtrip():
    from vmon.sandbox import Sandbox

    sb = Sandbox.create(image=_image(), block_network=True, timeout=300)
    try:
        proc = sb.exec("sh", "-lc", "printf ok")
        assert _read_stdout(proc) == "ok"
        assert proc.returncode == 0
    finally:
        sb.terminate()


def test_writable_volume_persists_across_sandboxes():
    from vmon.agent_client import AgentError
    from vmon.sandbox import Sandbox
    from vmon.volume import Volume

    volume = Volume("pytest-persist")
    try:
        sb = Sandbox.create(
            image=_image(), volumes={"/data": volume}, block_network=True, timeout=300
        )
    except AgentError as exc:
        # ENODEV == the guest kernel registers no virtiofs filesystem type (a
        # custom VMON_KERNEL without virtio-fs); skip.
        if "os error 19" in str(exc).lower() or "no such device" in str(exc).lower():
            pytest.skip(
                "guest kernel lacks virtio-fs; set VMON_KERNEL to a virtio-fs-capable kernel"
            )
        raise

    try:
        # The image-built volume request must warm-restore from a slot-provisioned
        # template (one virtio-fs slot per volume), never cold-boot.
        restored = Path(str(sb.vm.meta.get("restored_from") or ""))
        assert restored.name.endswith("-s1") and restored.exists(), restored
        assert sb.exec("sh", "-lc", "echo hi >/data/x").wait(timeout=30) == 0
    finally:
        sb.terminate()

    sb2 = Sandbox.create(image=_image(), volumes={"/data": volume}, block_network=True, timeout=300)
    try:
        # The second create reuses the same provisioned template (warm restore).
        restored2 = Path(str(sb2.vm.meta.get("restored_from") or ""))
        assert restored2.name.endswith("-s1") and restored2.exists(), restored2
        assert str(restored2) == str(restored)
        proc = sb2.exec("cat", "/data/x")
        assert _read_stdout(proc).strip() == "hi"
    finally:
        sb2.terminate()


def test_two_volume_warm_restore_provisions_two_slots():
    from vmon.agent_client import AgentError
    from vmon.sandbox import Sandbox
    from vmon.volume import Volume

    a, b = Volume("pytest-slot-a"), Volume("pytest-slot-b")
    try:
        sb = Sandbox.create(
            image=_image(), volumes={"/a": a, "/b": b}, block_network=True, timeout=300
        )
    except AgentError as exc:
        if "os error 19" in str(exc).lower() or "no such device" in str(exc).lower():
            pytest.skip(
                "guest kernel lacks virtio-fs; set VMON_KERNEL to a virtio-fs-capable kernel"
            )
        raise
    try:
        # Two volumes -> a 2-slot provisioned template; distinct per-mount content
        # proves each slot rebinds to its own host dir (no aliasing).
        restored = Path(str(sb.vm.meta.get("restored_from") or ""))
        assert restored.name.endswith("-s2") and restored.exists(), restored
        assert sb.exec("sh", "-lc", "echo AA >/a/m && echo BB >/b/m").wait(timeout=30) == 0
        assert _read_stdout(sb.exec("cat", "/a/m")).strip() == "AA"
        assert _read_stdout(sb.exec("cat", "/b/m")).strip() == "BB"
    finally:
        sb.terminate()

    sb2 = Sandbox.create(
        image=_image(), volumes={"/a": a, "/b": b}, block_network=True, timeout=300
    )
    try:
        # The second create reuses the same 2-slot provisioned template (warm), and
        # both slots still map to their own persistent host dirs.
        restored2 = Path(str(sb2.vm.meta.get("restored_from") or ""))
        assert restored2.name.endswith("-s2") and restored2.exists(), restored2
        assert str(restored2) == str(restored)
        assert _read_stdout(sb2.exec("cat", "/a/m")).strip() == "AA"
        assert _read_stdout(sb2.exec("cat", "/b/m")).strip() == "BB"
    finally:
        sb2.terminate()


def test_pty_exec_reports_isatty_true():
    from vmon.sandbox import Sandbox

    sb = Sandbox.create(image=_image(), block_network=True, timeout=300)
    try:
        # A PTY exec must allocate a real /dev/pts slave. This regressed when the
        # guest agent did not mount devpts, so assert the mount and the slave tty
        # explicitly (portable shell, no python3 dependency on the guest image).
        proc = sb.exec(
            "sh",
            "-lc",
            "tty; test -t 1 && echo ISATTY; "
            "mount | grep -q 'on /dev/pts type devpts' && echo DEVPTS",
            pty=True,
        )
        out = _read_stdout(proc)
        assert "/dev/pts/" in out, f"pty not backed by a devpts slave: {out!r}"
        assert "ISATTY" in out, f"stdout is not a tty: {out!r}"
        assert "DEVPTS" in out, f"devpts not mounted in guest: {out!r}"
    finally:
        sb.terminate()


def test_block_network_blocks_egress():
    from vmon.sandbox import Sandbox

    sb = Sandbox.create(image=_image(), block_network=True, timeout=300)
    try:
        proc = sb.exec("ip", "-4", "addr", "show", "scope", "global", timeout=10)
        proc.close_stdin()
        rc = proc.wait(timeout=15)
        data = proc.stdout.read()
        addr = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else str(data)
        assert rc == 0
        assert "inet " not in addr, f"block_network sandbox has a guest IPv4 address: {addr!r}"
        proc = sb.exec("ip", "route", timeout=10)
        proc.close_stdin()
        rc = proc.wait(timeout=15)
        data = proc.stdout.read()
        route = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else str(data)
        assert rc == 0
        assert "default" not in route, f"block_network sandbox has a default route: {route!r}"
    finally:
        sb.terminate()


_RF_CONST = 7


def _rf_helper(n: int) -> int:
    return n * 3


@function(
    block_network=True,
)
def _rf_compute(x: int) -> dict[str, int]:
    # Real-VM cover for remote functions: a module import (math), module helper
    # (_rf_helper), module constant (_RF_CONST), a multi-line decorator, and
    # forwarded stdout must all survive shipping into a python:3.14-slim guest.
    print(f"rf-e2e x={x}")
    return {"isqrt": math.isqrt(x), "tripled": _rf_helper(x), "const": _RF_CONST}


def test_remote_function_runs_in_real_vm(capsys):
    assert _rf_compute.remote(16) == {"isqrt": 4, "tripled": 48, "const": 7}
    # The guest's print() must be forwarded to the host by _remote_function_result.
    assert "rf-e2e x=16" in capsys.readouterr().out
    try:
        assert _rf_compute.map([1, 4, 9]) == [
            {"isqrt": 1, "tripled": 3, "const": 7},
            {"isqrt": 2, "tripled": 12, "const": 7},
            {"isqrt": 3, "tripled": 27, "const": 7},
        ]
    finally:
        _rf_compute.terminate()
