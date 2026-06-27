"""Measure create-to-first-useful-byte latency for cold and warm sandboxes."""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import platform
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path

from vmon.assets import ensure_kernel
from vmon.image import (
    CachedTemplate,
    _find_mkfs_ext4,
    cached_template,
    detect_engine,
    ensure_agent,
)
from vmon.pool import wait_for_agent_ready
from vmon.sandbox import Sandbox
from vmon.vmm import STATE, MicroVM, default_kernel, find_binary, hypervisor_present

DEFAULT_IMAGE = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
OP_NAME = "create->exec:echo-ready"


def preflight() -> str | None:
    """Return a human-readable skip reason, or None when the benchmark can run."""
    if os.environ.get("VMON_BENCH") != "1":
        return "set VMON_BENCH=1 to run the real-VM benchmark"
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
    # Pin the microVM kernel before resolving anything: a host /boot/vmlinuz is
    # modular and will not direct-boot in the minimal guest. Respects an explicit
    # VMON_KERNEL, and keeps preflight and runtime on the same default_kernel().
    os.environ.setdefault("VMON_KERNEL", str(ensure_kernel()))
    for probe, hint in (
        (find_binary, "vmm binary"),
        (default_kernel, "guest kernel"),
        (detect_engine, "container engine"),
        (ensure_agent, "static guest agent"),
    ):
        try:
            probe()
        except RuntimeError as exc:
            return f"{hint}: {exc}"
    if _find_mkfs_ext4() is None:
        return "mkfs.ext4/mke2fs not found (install e2fsprogs; on macOS: `brew install e2fsprogs`)"
    return None


def percentile_ms(samples: list[float], q: float) -> float:
    if not samples:
        raise ValueError("cannot compute a percentile of no samples")
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))
    return ordered[index]


def summarize(samples: list[float]) -> dict[str, object]:
    return {
        "p50_ms": percentile_ms(samples, 0.50),
        "p95_ms": percentile_ms(samples, 0.95),
        "samples_ms": samples,
    }


def _first_useful_byte(sandbox: Sandbox, *, start_ns: int, timeout: float) -> float:
    proc = None
    try:
        proc = sandbox.exec(["echo", "ready"], timeout=timeout, _track_entry=False)
        first = proc.stdout.read(1)
        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
        if first != b"r":
            raise RuntimeError(f"expected first useful byte b'r', got {first!r}")
        rc = proc.wait(timeout=timeout)
        if rc != 0:
            raise RuntimeError(f"echo ready exited {rc}")
        return elapsed_ms
    finally:
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(Exception):
                proc.kill()


def measure_cold_once(template: CachedTemplate, *, timeout: float) -> float:
    """Fresh-boot from the cached rootfs, then time the first guest-agent byte."""
    vm = None
    sandbox: Sandbox | None = None
    start = time.perf_counter_ns()
    try:
        vm = MicroVM.boot_rootfs(
            template.rootfs,
            name=f"bench-cold-{os.getpid()}-{time.time_ns()}",
            overlay=True,
            agent=True,
            mem=512,
            cpus=1,
            snapshot_root=STATE / "snapshots",
            image=template.spec.reference,
        )
        wait_for_agent_ready(vm, timeout=timeout)
        sandbox = Sandbox(vm, image_spec=template.spec)
        return _first_useful_byte(sandbox, start_ns=start, timeout=timeout)
    finally:
        if sandbox is not None:
            sandbox.terminate()
            sandbox.vm.remove()
        elif vm is not None:
            vm.remove()


def measure_warm_once(image: str, *, disk_mb: int, timeout: float) -> float:
    """Warm-restore from the cached template, then time the first guest-agent byte."""
    sandbox: Sandbox | None = None
    start = time.perf_counter_ns()
    try:
        sandbox = Sandbox.create(
            image=image,
            block_network=True,
            disk_mb=disk_mb,
            timeout=timeout,
            timeout_secs=0,
        )
        return _first_useful_byte(sandbox, start_ns=start, timeout=timeout)
    finally:
        if sandbox is not None:
            sandbox.terminate()
            sandbox.vm.remove()


def measure_series(
    label: str,
    measure: Callable[[], float],
    *,
    iters: int,
    warmup: int,
    out,
    retries: int = 2,
) -> tuple[list[float], int]:
    total = warmup + iters
    samples: list[float] = []
    retries_used = 0
    for i in range(total):
        elapsed_ms = 0.0
        for attempt in range(retries + 1):
            try:
                elapsed_ms = measure()
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == retries:
                    raise
                retries_used += 1
                print(
                    f"  {label} {i + 1}/{total}: transient failure, retrying: {exc!r}",
                    file=out,
                    flush=True,
                )
        if i >= warmup:
            samples.append(elapsed_ms)
        print(
            f"  {label} {i + 1}/{total}: {elapsed_ms:.1f} ms{' (warmup)' if i < warmup else ''}",
            file=out,
            flush=True,
        )
    return samples, retries_used


def result_payload(
    *,
    image: str,
    disk_mb: int,
    iters: int,
    warmup: int,
    cold: list[float],
    warm: list[float],
    cold_retries: int = 0,
    warm_retries: int = 0,
) -> dict[str, object]:
    cold_summary = summarize(cold)
    warm_summary = summarize(warm)
    warm_p50 = percentile_ms(warm, 0.50)
    speedup = percentile_ms(cold, 0.50) / warm_p50 if warm_p50 else None
    return {
        "status": "ok",
        "op": OP_NAME,
        "image": image,
        "disk_mb": disk_mb,
        "iters": iters,
        "warmup": warmup,
        "state": str(STATE),
        "cold": cold_summary,
        "warm": warm_summary,
        "speedup": speedup,
        "cold_retries": cold_retries,
        "warm_retries": warm_retries,
        "flaky": bool(cold_retries or warm_retries),
    }


def print_table(payload: dict[str, object], out) -> None:
    cold = payload["cold"]
    warm = payload["warm"]
    if not isinstance(cold, dict) or not isinstance(warm, dict):
        raise TypeError("benchmark payload has invalid summaries")
    speedup = payload["speedup"]
    speedup_s = f"{speedup:.2f}x" if isinstance(speedup, float) else "n/a"
    print("", file=out)
    print(
        "op                         cold p50  cold p95  warm p50  warm p95  speedup",
        file=out,
    )
    print("-------------------------  --------  --------  --------  --------  -------", file=out)
    print(
        f"{str(payload['op'])[:25]:25}  "
        f"{float(cold['p50_ms']):8.1f}  {float(cold['p95_ms']):8.1f}  "
        f"{float(warm['p50_ms']):8.1f}  {float(warm['p95_ms']):8.1f}  "
        f"{speedup_s:>7}",
        file=out,
    )
    cold_retries = payload.get("cold_retries") or 0
    warm_retries = payload.get("warm_retries") or 0
    if cold_retries or warm_retries:
        print(
            f"WARNING flaky run: cold_retries={cold_retries} warm_retries={warm_retries} "
            "(transient boot failures were resampled and excluded from p50)",
            file=out,
        )


def emit_json(payload: dict[str, object], dest: str | None) -> None:
    if dest is None:
        return
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if dest == "-":
        print(text, end="")
        return
    path = Path(dest)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure p50/p95 sandbox create-to-first-useful-byte latency."
    )
    parser.add_argument(
        "--image", default=DEFAULT_IMAGE, help="container image (default: alpine:latest)"
    )
    parser.add_argument("--iters", type=int, default=10, help="measured iterations per path")
    parser.add_argument("--warmup", type=int, default=1, help="warmup iterations to discard")
    parser.add_argument("--disk-mb", type=int, default=1024, help="template rootfs size in MiB")
    parser.add_argument(
        "--timeout", type=float, default=300.0, help="per-sandbox timeout in seconds"
    )
    parser.add_argument(
        "--json",
        nargs="?",
        const="-",
        default=None,
        metavar="PATH",
        help="write machine-readable JSON to PATH, or stdout if PATH is omitted",
    )
    args = parser.parse_args(argv)
    if args.iters <= 0:
        parser.error("--iters must be positive")
    if args.warmup < 0:
        parser.error("--warmup must be non-negative")
    if args.disk_mb <= 0:
        parser.error("--disk-mb must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    text_out = sys.stderr if args.json == "-" else sys.stdout

    reason = preflight()
    if reason:
        payload: dict[str, object] = {"status": "skip", "reason": reason}
        print(f"SKIP: {reason}", file=text_out)
        emit_json(payload, args.json)
        return 0

    print(
        f"bench: image={args.image} state={STATE} iters={args.iters} warmup={args.warmup}",
        file=text_out,
    )
    cached = cached_template(
        image=args.image,
        disk_mb=args.disk_mb,
        timeout=args.timeout,
    )

    cold, cold_retries = measure_series(
        "cold",
        lambda: measure_cold_once(cached, timeout=args.timeout),
        iters=args.iters,
        warmup=args.warmup,
        out=text_out,
    )
    warm, warm_retries = measure_series(
        "warm",
        lambda: measure_warm_once(args.image, disk_mb=args.disk_mb, timeout=args.timeout),
        iters=args.iters,
        warmup=args.warmup,
        out=text_out,
    )

    payload = result_payload(
        image=args.image,
        disk_mb=args.disk_mb,
        iters=args.iters,
        warmup=args.warmup,
        cold=cold,
        warm=warm,
        cold_retries=cold_retries,
        warm_retries=warm_retries,
    )
    print_table(payload, text_out)
    emit_json(payload, args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
