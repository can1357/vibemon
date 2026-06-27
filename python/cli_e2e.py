#!/usr/bin/env python3.14
"""End-to-end smoke test for the ``vmon`` **CLI** (client -> vmond -> engine -> VMM).

Where ``e2e.py`` drives the Python SDK in-process, this driver shells out to the
``vmon`` command and asserts on its stdout / exit codes, exercising the full
Docker-like control plane on a Linux/KVM or macOS/HVF host:

  * the auto-started local daemon (``vmon daemon status``, first-call auto-start)
  * ``vmon run`` foreground (streamed entry output + propagated exit code)
  * ``vmon run -d`` / ``ps`` / ``exec`` / ``stop`` / ``rm`` lifecycle
  * ``vmon cp`` host<->guest round-trip
  * ``vmon logs`` against an agent-backed VM
  * ``vmon pause`` / ``vmon resume``
  * ``vmon snapshot --stop`` + ``vmon restore`` (disk state preserved)
  * ``vmon fork`` copy-on-write clones
  * ``vmon daemon stop`` (socket removed)

It runs in an isolated ``$VMON_HOME`` (a short ``/tmp`` dir so the daemon's Unix
socket fits the ``AF_UNIX`` path limit) and prints a PASS/FAIL/SKIP table,
returning non-zero if anything fails.

Prerequisites (same as ``e2e.py``): a Linux/KVM or macOS/HVF host with a built
``vmon`` binary, a static guest agent, a guest kernel, ``docker``/``podman``,
and ``mkfs.ext4``/``mke2fs`` (e2fsprogs).

Usage:
  python3.14 python/cli_e2e.py                  # run everything
  python3.14 python/cli_e2e.py run exec         # subset (substring match)
  python3.14 python/cli_e2e.py --list           # list test names
  python3.14 python/cli_e2e.py --keep           # leave $VMON_HOME in place
  python3.14 python/cli_e2e.py --home ~/.vmon    # reuse a home (skip template rebuild)
  VMON_E2E_IMAGE=alpine:latest python3.14 python/cli_e2e.py
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from vmon.image import _find_mkfs_ext4, detect_engine, find_agent_binary
from vmon.vmm import default_kernel, find_binary, hypervisor_present

IMAGE = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
RUN = f"clie2e{os.getpid()}"

# Slow paths build the image template / boot a VM; fast paths just RPC the daemon.
BOOT_TIMEOUT = 300
FAST_TIMEOUT = 60

_seq = 0
HOME: str = ""
ENV: dict[str, str] = {}
CREATED: set[str] = set()


# --------------------------------------------------------------------------- #
# harness
# --------------------------------------------------------------------------- #


class Skip(Exception):
    """Raised by a test to report it cannot run in this environment."""


TESTS: list[Callable[[object], None]] = []


def e2e(fn: Callable[[object], None]) -> Callable[[object], None]:
    """Register a test, preserving definition order."""
    TESTS.append(fn)
    return fn


def uid(prefix: str) -> str:
    """A per-run unique, VM-name-safe identifier."""
    global _seq
    _seq += 1
    return f"{prefix}-{RUN}-{_seq}"


def cli(
    *args: str, timeout: float = FAST_TIMEOUT, input_bytes: bytes | None = None, check: bool = False
) -> tuple[int, str, str]:
    """Run ``vmon <args>`` against the isolated home; return ``(rc, stdout, stderr)``."""
    proc = subprocess.run(
        [sys.executable, "-m", "vmon", *args],
        env=ENV,
        input=input_bytes,
        capture_output=True,
        timeout=timeout,
    )
    out = proc.stdout.decode("utf-8", "replace")
    err = proc.stderr.decode("utf-8", "replace")
    if check and proc.returncode != 0:
        raise AssertionError(
            f"`vmon {' '.join(args)}` exited {proc.returncode}\n"
            f"--stdout--\n{out}\n--stderr--\n{err}"
        )
    return proc.returncode, out, err


def track(name: str) -> None:
    CREATED.add(name)


def untrack(name: str) -> None:
    CREATED.discard(name)


def ps_status(name: str) -> str | None:
    """The STATUS column for *name* in ``vmon ps`` output, or None if absent."""
    _, out, _ = cli("ps", timeout=FAST_TIMEOUT)
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0] == name:
            return parts[1] if len(parts) > 1 else None
    return None


def start_detached(name: str) -> None:
    """Boot a long-lived detached VM that idles, so later commands can target it."""
    args = ["run", "-d", "--block-network", "--name", name]
    args += [IMAGE, "--", "sh", "-c", "echo ready-" + RUN + "; sleep 300"]
    cli(*args, timeout=BOOT_TIMEOUT, check=True)
    track(name)
    deadline = time.time() + 30
    while time.time() < deadline:
        if ps_status(name) == "running":
            return
        time.sleep(0.5)
    raise AssertionError(f"{name} never reached running in ps after detached run")


def exec_until(name: str, *argv: str, want: str, timeout: float = 40) -> str:
    """Retry ``vmon exec`` until stdout contains *want*.

    ``restore -d`` returns once the control socket is up, but the guest agent
    socket can lag, and ``vmon exec`` (a fresh attach) does not retry the connect.
    """
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        rc, out, err = cli("exec", name, "--", *argv)
        if rc == 0 and want in out:
            return out
        last = f"rc={rc} out={out!r} err={err!r}"
        time.sleep(1.0)
    raise AssertionError(f"exec on {name} never produced {want!r} in {timeout:.0f}s; last: {last}")


def wait_daemon_stopped(timeout: float = 10) -> None:
    """Block until the daemon socket is gone and ``status`` reports not-running."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rc, out, _ = cli("daemon", "status")
        if rc != 0 and "not running" in out and not (Path(HOME) / "vmond.sock").exists():
            return
        time.sleep(0.2)
    raise AssertionError("daemon still reachable after stop")


# --------------------------------------------------------------------------- #
# tests
# --------------------------------------------------------------------------- #


@e2e
def t_daemon_lifecycle(_):
    """status is not-running on a clean home; the first command auto-starts vmond."""
    cli("daemon", "stop")  # ensure a clean state (idempotent); runs first, no VMs yet
    wait_daemon_stopped()  # SIGTERM is async; wait for the socket to disappear
    rc, out, _ = cli("ps")
    assert rc == 0, f"ps (auto-start) failed: {out!r}"
    assert "NAME" in out or "no microVMs" in out, f"unexpected ps output: {out!r}"
    rc, out, _ = cli("daemon", "status")
    assert rc == 0 and "running" in out and "vmond.sock" in out, f"status: {out!r}"
    assert (Path(HOME) / "vmond.sock").exists(), "vmond.sock missing after auto-start"


@e2e
def t_run_foreground(_):
    """run streams the entry command's stdout and exits 0."""
    rc, out, err = cli(
        "run",
        "--block-network",
        IMAGE,
        "--",
        "sh",
        "-c",
        "echo hello-" + RUN + "; uname -s",
        timeout=BOOT_TIMEOUT,
    )
    assert rc == 0, f"rc={rc} err={err}"
    assert ("hello-" + RUN) in out, f"streamed stdout missing marker: {out!r}"
    assert "Linux" in out, f"uname -s not Linux: {out!r}"


@e2e
def t_run_exit_code(_):
    """A foreground run returns the entry process's exit code (here 7)."""
    rc, _, err = cli(
        "run", "--block-network", IMAGE, "--", "sh", "-c", "exit 7", timeout=BOOT_TIMEOUT
    )
    assert rc == 7, f"expected entry exit code 7 through the daemon, got {rc} (err={err})"


@e2e
def t_detach_ps_exec_stop_rm(_):
    """run -d registers a VM; ps/exec target it; stop+rm tear it down."""
    name = uid("vm")
    start_detached(name)
    assert ps_status(name) == "running", f"ps did not show {name} running"
    rc, out, err = cli("exec", name, "--", "sh", "-lc", "echo execd-" + RUN + "; pwd")
    assert rc == 0 and ("execd-" + RUN) in out, f"exec rc={rc} out={out!r} err={err}"
    cli("stop", name, check=True)
    cli("rm", name, check=True)
    untrack(name)
    assert ps_status(name) is None, f"{name} still listed after rm"


@e2e
def t_shell_ephemeral(_):
    """shell -c boots a fresh ephemeral VM, runs the command, and removes it."""
    marker = "shelled-" + RUN
    rc, out, err = cli(
        "shell", "--image", IMAGE, "-c", f"echo {marker}; uname -s", timeout=BOOT_TIMEOUT
    )
    assert rc == 0, f"shell rc={rc} err={err}"
    assert marker in out, f"shell stdout missing marker: {out!r}"
    assert "Linux" in out, f"uname -s not Linux: {out!r}"
    # Ephemeral: no leftover VM is registered once the shell exits.
    _, ps, _ = cli("ps")
    assert marker not in ps and "shell" not in ps, f"ephemeral shell VM leaked:\n{ps}"


@e2e
def t_shell_attach(_):
    """shell REF attaches to a running VM and leaves it running afterwards."""
    name = uid("sh")
    marker = "attach-" + RUN
    start_detached(name)
    try:
        rc, out, err = cli("shell", name, "-c", f"echo {marker}", timeout=FAST_TIMEOUT)
        assert rc == 0 and marker in out, f"attach shell rc={rc} out={out!r} err={err}"
        assert ps_status(name) == "running", f"{name} not running after attach shell"
    finally:
        cli("rm", name)
        untrack(name)


@e2e
def t_cp(_):
    """cp copies a file host->guest and back, round-tripping its contents."""
    name = uid("cp")
    start_detached(name)
    work = Path(tempfile.mkdtemp(prefix="cli-e2e-cp-"))
    payload = "cli-cp-" + RUN
    try:
        src = work / "up.txt"
        src.write_text(payload)
        cli("cp", str(src), f"{name}:/root/up.txt", check=True)
        rc, out, err = cli("exec", name, "--", "cat", "/root/up.txt")
        assert rc == 0 and payload in out, f"guest content: rc={rc} out={out!r} err={err}"
        dst = work / "down.txt"
        cli("cp", f"{name}:/root/up.txt", str(dst), check=True)
        assert dst.read_text() == payload, f"download mismatch: {dst.read_text()!r}"
    finally:
        cli("rm", name)
        untrack(name)
        shutil.rmtree(work, ignore_errors=True)


@e2e
def t_logs(_):
    """logs reads the VM serial console without affecting an agent-backed VM."""
    name = uid("logs")
    start_detached(name)
    try:
        rc, _, err = cli("logs", name)
        assert rc == 0, f"logs failed: {err!r}"
        assert ps_status(name) == "running", "logs should not stop a running VM"
    finally:
        cli("stop", name)
        cli("rm", name)
        untrack(name)


@e2e
def t_pause_resume(_):
    """pause then resume leaves the VM usable (exec succeeds afterward)."""
    name = uid("pr")
    start_detached(name)
    try:
        cli("pause", name, check=True)
        cli("resume", name, check=True)
        rc, out, err = cli("exec", name, "--", "sh", "-lc", "echo alive-" + RUN)
        assert rc == 0 and ("alive-" + RUN) in out, f"exec after resume: {out!r} {err}"
    finally:
        cli("rm", name)
        untrack(name)


@e2e
def t_snapshot_restore(_):
    """snapshot --stop then restore preserves on-disk guest state."""
    name = uid("snap")
    tpl = uid("tpl")
    restored = uid("rs")
    marker = "snap-" + RUN
    try:
        start_detached(name)
        cli("exec", name, "--", "sh", "-lc", f"echo {marker} > /root/m && sync", check=True)
        cli("snapshot", name, tpl, "--stop", timeout=BOOT_TIMEOUT, check=True)
        cli("rm", name, check=True)
        untrack(name)
        cli("restore", tpl, "--name", restored, "--agent", "-d", timeout=BOOT_TIMEOUT, check=True)
        track(restored)
        exec_until(restored, "cat", "/root/m", want=marker)
    finally:
        for n in (name, restored):
            if n in CREATED:
                cli("rm", n)
                untrack(n)


@e2e
def t_fork(_):
    """fork makes N copy-on-write clones that appear in ps."""
    name = uid("fk")
    tpl = uid("fktpl")
    clones: list[str] = []
    try:
        start_detached(name)
        cli("snapshot", name, tpl, timeout=BOOT_TIMEOUT, check=True)
        cli("rm", name, check=True)
        untrack(name)
        rc, out, err = cli("fork", tpl, "--count", "2", timeout=BOOT_TIMEOUT)
        assert rc == 0, f"fork failed: {err}"
        clones = [line.split()[0] for line in out.splitlines() if "(pid" in line and line.split()]
        assert len(clones) == 2, f"expected 2 clones, parsed {clones} from:\n{out}"
        for c in clones:
            track(c)
        _, ps, _ = cli("ps")
        for c in clones:
            assert c in ps, f"clone {c} not listed in ps:\n{ps}"
    finally:
        for n in (name, *clones):
            if n in CREATED:
                cli("rm", n)
                untrack(n)


@e2e
def t_daemon_stop(_):
    """daemon stop terminates vmond and removes its socket (runs last)."""
    # Clear any VMs this suite still owns so stopping the daemon leaks nothing.
    for name in list(CREATED):
        cli("rm", name)
        untrack(name)
    rc, out, _ = cli("daemon", "stop")
    assert rc == 0, f"daemon stop: {out!r}"
    wait_daemon_stopped()


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


def cleanup() -> None:
    """Best-effort teardown: remove any VMs we still own, then stop the daemon."""
    for name in list(CREATED):
        try:
            cli("rm", name)
        except Exception:
            pass
        untrack(name)
    try:
        cli("daemon", "stop")
    except Exception:
        pass


def main(argv: Sequence[str] | None = None) -> int:
    global IMAGE, HOME, ENV
    ap = argparse.ArgumentParser(description="vmon CLI end-to-end smoke test")
    ap.add_argument("tests", nargs="*", help="substring filters (default: run all)")
    ap.add_argument("--list", action="store_true", help="list test names and exit")
    ap.add_argument("--keep", action="store_true", help="leave $VMON_HOME on exit")
    ap.add_argument("--home", help="reuse this VMON_HOME (skips template rebuild)")
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

    owns_home = args.home is None
    # Root the temp home at /tmp, not $TMPDIR: macOS $TMPDIR (/var/folders/.../T)
    # is long enough that a per-VM Unix socket path under it
    # (<home>/vms/<name>/agent.sock) would overflow the 104-byte sun_path limit.
    HOME = args.home or tempfile.mkdtemp(prefix="vmon-e2e-", dir="/tmp")
    # Install the isolated home before any probe runs: default_kernel() may
    # auto-download a guest kernel into $VMON_HOME/assets on macOS, and the
    # daemon the CLI spawns must resolve the very same home.
    os.environ["VMON_HOME"] = HOME
    ENV = dict(os.environ)
    py_dir = str(Path(__file__).resolve().parent)
    ENV["PYTHONPATH"] = py_dir + (os.pathsep + ENV["PYTHONPATH"] if ENV.get("PYTHONPATH") else "")
    # A stable, wide width keeps rich from soft-wrapping long values (e.g. the
    # socket path under a long macOS $TMPDIR) mid-token in scraped CLI output.
    ENV["COLUMNS"] = "200"

    reason = preflight()
    if reason:
        if owns_home and not args.keep:
            shutil.rmtree(HOME, ignore_errors=True)
        print(f"SKIP: {reason}")
        return 0

    print(f"cli-e2e: image={IMAGE} home={HOME} tests={len(selected)}")

    results: list[tuple[str, str, float, str]] = []
    try:
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
    finally:
        cleanup()
        if owns_home and not args.keep:
            shutil.rmtree(HOME, ignore_errors=True)

    npass = sum(r[0] == "PASS" for r in results)
    nfail = sum(r[0] == "FAIL" for r in results)
    nskip = sum(r[0] == "SKIP" for r in results)
    print(f"\n{'=' * 52}\n{npass} passed, {nfail} failed, {nskip} skipped")
    if nfail:
        print("failed: " + ", ".join(r[1] for r in results if r[0] == "FAIL"))
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())
