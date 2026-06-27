"""Confirm the SYNC-handshake agent change works on aarch64/HVF (throwaway)."""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from vmon.image import cached_template
from vmon.sandbox import Sandbox
from vmon.vmm import hypervisor_present

IMAGE = "alpine:latest"
results: list[tuple[str, bool, str]] = []


def check(name, fn):
    try:
        d = fn()
        results.append((name, True, d or ""))
        print(f"PASS {name}: {d or ''}", flush=True)
    except Exception as exc:  # noqa: BLE001
        results.append((name, False, repr(exc)))
        print(f"FAIL {name}: {exc!r}", flush=True)
        traceback.print_exc()


def warm_block_network():
    sb = Sandbox.create(image=IMAGE, block_network=True)
    try:
        assert sb.vm.meta.get("restored_from"), "expected warm restore"
        # several execs to exercise repeated framing over the channel
        for i in range(3):
            out = sb.exec(["echo", f"r{i}"]).stdout.read().decode()
            assert f"r{i}" in out, out
        return "restored_from set; 3 execs ok"
    finally:
        sb.terminate()


def networked_warm():
    sb = Sandbox.create(image=IMAGE, block_network=False)
    try:
        assert sb.vm.meta.get("restored_from"), "expected networked warm restore"
        out = sb.exec(["ip", "-4", "addr"]).stdout.read().decode()
        assert "10.0.2.15" in out, out
        return "restored_from set; guest user-net ip present"
    finally:
        sb.terminate()


def main():
    if not hypervisor_present():
        print("SKIP: no hypervisor")
        return 0
    print("building template ...", flush=True)
    cached_template(image=IMAGE)
    check("warm_block_network", warm_block_network)
    check("networked_warm", networked_warm)
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed; failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
