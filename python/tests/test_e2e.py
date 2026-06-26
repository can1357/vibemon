import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("VMON_KVM_E2E"),
    reason="needs KVM e2e host",
)


def _image():
    return os.environ.get("VMON_E2E_IMAGE", "alpine:latest")


def _read_stdout(process):
    process.wait(timeout=30)
    data = process.stdout.read()
    return data.decode("utf-8", errors="replace") if isinstance(data, bytes) else str(data)


def test_real_sandbox_create_and_exec_roundtrip():
    from vmon.sandbox import Sandbox

    sb = Sandbox.create(image=_image(), timeout=300)
    try:
        proc = sb.exec("sh", "-lc", "printf ok")
        assert _read_stdout(proc) == "ok"
        assert proc.returncode == 0
    finally:
        sb.terminate()


def test_writable_volume_persists_across_sandboxes():
    from vmon.sandbox import Sandbox
    from vmon.volume import Volume

    volume = Volume("pytest-persist")
    sb = Sandbox.create(image=_image(), volumes={"/data": volume}, timeout=300)
    try:
        assert sb.exec("sh", "-lc", "echo hi >/data/x").wait(timeout=30) == 0
    finally:
        sb.terminate()

    sb2 = Sandbox.create(image=_image(), volumes={"/data": volume}, timeout=300)
    try:
        proc = sb2.exec("cat", "/data/x")
        assert _read_stdout(proc).strip() == "hi"
    finally:
        sb2.terminate()


def test_pty_exec_reports_isatty_true():
    from vmon.sandbox import Sandbox

    sb = Sandbox.create(image=_image(), timeout=300)
    try:
        proc = sb.exec("python3", "-c", "import sys; print(sys.stdout.isatty())", pty=True)
        assert _read_stdout(proc).strip() == "True"
    finally:
        sb.terminate()


def test_block_network_blocks_egress():
    from vmon.sandbox import Sandbox

    sb = Sandbox.create(image=_image(), block_network=True, timeout=300)
    try:
        start = time.monotonic()
        proc = sb.exec("sh", "-lc", "wget -T 3 -q -O- https://example.com", timeout=10)
        rc = proc.wait(timeout=15)
        assert rc != 0
        assert time.monotonic() - start < 15
    finally:
        sb.terminate()
