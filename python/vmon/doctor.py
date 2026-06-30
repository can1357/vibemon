"""Diagnostic preflight checks for the local vmon developer environment."""

import os
import platform
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from . import console as ui
from .client import DaemonClient, DaemonError
from .image import detect_image_tools
from .vmm import default_kernel, find_binary

type Status = Literal["ok", "warn", "fail"]


@dataclass
class Check:
    """One prerequisite result with an optional remediation hint."""

    name: str
    status: Status
    detail: str
    hint: str = ""


def _arch() -> str:
    machine = platform.machine().lower().replace("-", "_")
    return {"arm64": "aarch64", "amd64": "x86_64", "x64": "x86_64"}.get(machine, machine)


def _home() -> Path:
    return Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon"))).expanduser()


def _check_vmm_binary() -> tuple[Check, Path | None]:
    try:
        binary = find_binary()
    except Exception as exc:
        return (
            Check(
                "vmm binary",
                "fail",
                f"not found: {exc}",
                "run `cargo build` from the repository root or set VMON_BIN=/path/to/vmm",
            ),
            None,
        )
    if not binary:
        return (
            Check(
                "vmm binary",
                "fail",
                "not found",
                "run `cargo build` from the repository root or set VMON_BIN=/path/to/vmm",
            ),
            None,
        )
    path = Path(binary).expanduser()
    return Check("vmm binary", "ok", str(path)), path


def _check_codesign(binary: Path | None) -> Check:
    if binary is None:
        return Check(
            "codesign entitlement",
            "fail",
            "cannot inspect entitlements because the vmm binary is missing",
            "run `cargo build`, then `just codesign` or `just build`",
        )
    try:
        proc = subprocess.run(
            ["codesign", "-d", "--entitlements", "-", str(binary)],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception as exc:
        return Check(
            "codesign entitlement",
            "fail",
            f"codesign failed: {exc}",
            "run `just build` or `just codesign` to grant the Hypervisor entitlement",
        )
    output = f"{proc.stdout}\n{proc.stderr}"
    if "com.apple.security.hypervisor" in output:
        return Check("codesign entitlement", "ok", "Hypervisor entitlement present")
    detail = "Hypervisor entitlement missing"
    if proc.returncode:
        detail = f"codesign exited {proc.returncode}; {detail}"
    return Check(
        "codesign entitlement",
        "fail",
        detail,
        "run `just build` or `just codesign` to grant the Hypervisor entitlement",
    )


def _check_hypervisor(system: str) -> Check:
    if system == "Darwin":
        try:
            proc = subprocess.run(
                ["sysctl", "-n", "kern.hv_support"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
        except Exception as exc:
            return Check(
                "hypervisor",
                "fail",
                f"could not query kern.hv_support: {exc}",
                "run on Apple hardware with Hypervisor.framework support enabled",
            )
        if proc.stdout.strip() == "1":
            return Check("hypervisor", "ok", "kern.hv_support=1")
        detail = proc.stderr.strip() or proc.stdout.strip() or "kern.hv_support is not 1"
        return Check(
            "hypervisor",
            "fail",
            detail,
            "run on Apple hardware with Hypervisor.framework support enabled",
        )
    if system == "Linux":
        kvm = Path("/dev/kvm")
        if kvm.exists() and os.access(kvm, os.R_OK | os.W_OK):
            return Check("hypervisor", "ok", "/dev/kvm is present and writable")
        return Check(
            "hypervisor",
            "fail",
            "/dev/kvm is missing or not writable",
            "enable KVM and add your user to the kvm group, then re-login",
        )
    return Check(
        "hypervisor",
        "warn",
        f"host platform {system or 'unknown'} is not checked",
        "vmon expects macOS HVF or Linux KVM for local microVMs",
    )


def _check_image_tools() -> Check:
    try:
        tools = detect_image_tools()
    except RuntimeError as exc:
        return Check(
            "image tools",
            "warn",
            str(exc),
            "install skopeo and umoci before using `vmon run` with image references",
        )
    return Check("image tools", "ok", f"skopeo at {tools.skopeo}; umoci at {tools.umoci}")


def _check_mkfs(system: str) -> Check:
    mkfs = shutil.which("mkfs.ext4")
    homebrew_mkfs = Path("/opt/homebrew/opt/e2fsprogs/sbin/mkfs.ext4")
    if not mkfs and system == "Darwin" and homebrew_mkfs.exists():
        mkfs = str(homebrew_mkfs)
    if mkfs:
        return Check("mkfs.ext4", "ok", str(mkfs))
    hint = "brew install e2fsprogs" if system == "Darwin" else "install e2fsprogs"
    return Check("mkfs.ext4", "warn", "mkfs.ext4 not found", hint)


def _cached_kernel() -> Path | None:
    assets = _home() / "assets"
    try:
        for candidate in sorted(assets.iterdir()):
            if candidate.is_file() and candidate.name.startswith(("Image", "bzImage", "vmlinuz")):
                return candidate
    except OSError:
        return None
    return None


def _check_guest_kernel() -> Check:
    env_kernel = os.environ.get("VMON_KERNEL")
    try:
        kernel = Path(default_kernel()).expanduser()
        if kernel.exists():
            prefix = "VMON_KERNEL=" if env_kernel else ""
            return Check("guest kernel", "ok", f"{prefix}{kernel}")
        return Check(
            "guest kernel",
            "warn",
            f"configured kernel path does not exist: {kernel}",
            "set VMON_KERNEL to a bootable guest kernel or let vmon auto-provision one",
        )
    except Exception as exc:
        if cached := _cached_kernel():
            return Check("guest kernel", "ok", f"cached kernel at {cached}")
        return Check(
            "guest kernel",
            "warn",
            f"not available yet: {exc}",
            "first boot auto-downloads a pinned kernel on macOS; otherwise set VMON_KERNEL",
        )


def _check_bundled_agent() -> Check:
    agent = Path(__file__).with_name("_agent") / f"vmon-agent-{_arch()}"
    if agent.is_file():
        return Check("bundled agent", "ok", str(agent))
    return Check(
        "bundled agent",
        "warn",
        f"missing {agent.name}",
        "run `just agent-musl` to build the static guest agent",
    )


def _check_daemon() -> Check:
    sock = _home() / "vmond.sock"
    if not sock.exists():
        return Check(
            "daemon", "warn", f"vmond socket not present at {sock}", "run `vmon daemon start`"
        )
    try:
        info = DaemonClient(sock_path=sock, autostart=False).call("info")
    except DaemonError as exc:
        return Check(
            "daemon",
            "warn",
            f"socket is present but not responsive: {exc}",
            "run `vmon daemon start`",
        )
    except Exception as exc:
        return Check("daemon", "warn", f"socket probe failed: {exc}", "run `vmon daemon start`")
    pid = info.get("pid")
    version = info.get("version")
    detail = "responsive"
    if pid is not None:
        detail += f" pid={pid}"
    if version:
        detail += f" version={version}"
    return Check("daemon", "ok", detail)


def _check_environment() -> Check:
    version = ".".join(str(part) for part in sys.version_info[:3])
    return Check("environment", "ok", f"Python {version} on {platform.system()} {_arch()}")


def collect_checks() -> list[Check]:
    """Collect first-run diagnostic checks without raising on probe failures."""
    checks: list[Check] = []
    try:
        vmm_check, binary = _check_vmm_binary()
        checks.append(vmm_check)
    except Exception as exc:
        binary = None
        checks.append(
            Check(
                "vmm binary",
                "fail",
                f"probe crashed: {exc}",
                "run `cargo build` from the repository root or set VMON_BIN=/path/to/vmm",
            )
        )

    try:
        system = platform.system()
    except Exception:
        system = ""

    probes: list[tuple[str, Status, Callable[[], Check]]] = []
    if system == "Darwin":
        probes.append(("codesign entitlement", "fail", lambda: _check_codesign(binary)))
    probes.extend(
        [
            ("hypervisor", "fail", lambda: _check_hypervisor(system)),
            ("image tools", "warn", _check_image_tools),
            ("mkfs.ext4", "warn", lambda: _check_mkfs(system)),
            ("guest kernel", "warn", _check_guest_kernel),
            ("bundled agent", "warn", _check_bundled_agent),
            ("daemon", "warn", _check_daemon),
            ("environment", "warn", _check_environment),
        ]
    )
    for name, fallback_status, probe in probes:
        try:
            checks.append(probe())
        except Exception as exc:
            checks.append(Check(name, fallback_status, f"probe crashed: {exc}"))
    return checks


def run_doctor() -> int:
    """Render diagnostic checks and return a shell exit code for hard failures."""
    checks = collect_checks()
    ui.doctor_table(checks)
    return 1 if any(check.status == "fail" for check in checks) else 0
