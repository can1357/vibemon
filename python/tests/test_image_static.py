import hashlib
import json
import struct
from pathlib import Path

import pytest


def _write_elf64(path: Path, p_type: int) -> Path:
    ident = b"\x7fELF" + bytes([2, 1, 1]) + b"\x00" * 9
    header = struct.pack(
        "<16sHHIQQQIHHHHHH",
        ident,
        2,  # ET_EXEC
        62,  # EM_X86_64
        1,
        0,
        64,  # e_phoff immediately after ELF header
        0,
        0,
        64,
        56,
        1,
        0,
        0,
        0,
    )
    phdr = struct.pack("<IIQQQQQQ", p_type, 0, 0, 0, 0, 0, 0, 0)
    path.write_bytes(header + phdr)
    return path


def test_is_static_elf_detects_pt_interp(tmp_path):
    from vmon.image import _is_static_elf

    dynamic = _write_elf64(tmp_path / "dynamic-agent", 3)
    static = _write_elf64(tmp_path / "static-agent", 1)
    text = tmp_path / "not-elf"
    text.write_text("hello", encoding="utf-8")

    assert _is_static_elf(dynamic) is False
    assert _is_static_elf(static) is True
    assert _is_static_elf(text) is False


def test_find_agent_binary_validates_mvm_agent_override(monkeypatch, tmp_path):
    from vmon.image import find_agent_binary

    dynamic = _write_elf64(tmp_path / "dynamic-agent", 3)
    dynamic.chmod(0o755)
    static = _write_elf64(tmp_path / "static-agent", 1)
    static.chmod(0o755)

    monkeypatch.setenv("VMON_AGENT", str(dynamic))
    with pytest.raises(RuntimeError, match="must be a static \\(musl\\) binary"):
        find_agent_binary()

    monkeypatch.setenv("VMON_AGENT", str(static))
    assert find_agent_binary() == static


def test_first_static_skips_dynamic_and_non_elf(tmp_path):
    from vmon.image import _first_static

    dynamic = _write_elf64(tmp_path / "dynamic-agent", 3)
    static = _write_elf64(tmp_path / "static-agent", 1)
    text = tmp_path / "not-elf"
    text.write_text("hello", encoding="utf-8")
    for p in (dynamic, static, text):
        p.chmod(0o755)

    # A stray dynamically linked (or non-ELF) build must not mask a usable
    # static one earlier-or-later in the candidate list.
    assert _first_static([text, dynamic, static]) == static
    assert _first_static([text, dynamic]) is None
    assert _first_static([tmp_path / "missing"]) is None


def test_first_static_accepts_non_executable(tmp_path):
    from vmon.image import _first_static

    # Wheel/package-data installs can drop the +x bit on the bundled agent;
    # it must still be selected (the rootfs copy is chmod'd at inject time).
    static = _write_elf64(tmp_path / "bundled-agent", 1)
    static.chmod(0o644)
    assert _first_static([static]) == static


def test_find_agent_binary_expands_user_override(monkeypatch, tmp_path):
    from vmon.image import find_agent_binary

    home = tmp_path / "home"
    agent = home / "bin" / "agent"
    agent.parent.mkdir(parents=True)
    _write_elf64(agent, 1)

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("VMON_AGENT", "~/bin/agent")

    assert find_agent_binary() == agent


def test_find_agent_binary_reports_missing_arch_and_checked_asset(monkeypatch, tmp_path):
    import vmon.image as image

    fake_pkg = tmp_path / "pkg" / "vmon" / "image.py"
    monkeypatch.setattr(image, "__file__", str(fake_pkg))
    monkeypatch.setattr(image.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(image.shutil, "which", lambda _name: None)
    monkeypatch.delenv("VMON_AGENT", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)

    with pytest.raises(RuntimeError, match=r"x86_64.*vmon-agent-x86_64"):
        image.find_agent_binary()


def test_ensure_agent_returns_found_binary_without_build(monkeypatch, tmp_path):
    import vmon.image as image

    agent = tmp_path / "vmon-agent"
    agent.write_bytes(b"static")
    build_calls: list[str] = []

    monkeypatch.setattr(image, "find_agent_binary", lambda arch=None: agent)
    monkeypatch.setattr(image, "_build_static_agent", lambda arch: build_calls.append(arch) or True)

    assert image.ensure_agent("arm64") == agent
    assert build_calls == []


def test_ensure_agent_builds_when_missing_and_toolchain_available(monkeypatch, tmp_path):
    import vmon.image as image

    repo = tmp_path / "repo"
    module = repo / "python" / "vmon" / "image.py"
    target = repo / "target" / "aarch64-unknown-linux-musl" / "release" / "vmon-agent"
    module.parent.mkdir(parents=True)
    (repo / "Cargo.toml").write_text("[workspace]\n", encoding="utf-8")
    monkeypatch.setattr(image, "__file__", str(module))
    monkeypatch.setattr(image.platform, "machine", lambda: "arm64")
    monkeypatch.delenv("VMON_AGENT", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)

    def fake_which(name: str) -> str | None:
        return {
            "cargo": "/usr/bin/cargo",
            "rustup": "/usr/bin/rustup",
            "just": "/usr/bin/just",
        }.get(name)

    calls: list[tuple[list[str], dict[str, object]]] = []

    class Result:
        def __init__(self, stdout: str = ""):
            self.stdout = stdout

    def fake_run(cmd: list[str], **kwargs: object) -> Result:
        calls.append((cmd, kwargs))
        if cmd[:4] == ["/usr/bin/rustup", "target", "list", "--installed"]:
            return Result("aarch64-unknown-linux-musl\n")
        if cmd == ["/usr/bin/just", "agent-musl"]:
            target.parent.mkdir(parents=True)
            _write_elf64(target, 1)
            return Result()
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(image.shutil, "which", fake_which)
    monkeypatch.setattr(image.subprocess, "run", fake_run)

    assert image.ensure_agent("aarch64") == target
    assert calls[0][0] == ["/usr/bin/rustup", "target", "list", "--installed"]
    assert calls[0][1]["timeout"] == 10
    assert calls[1][0] == ["/usr/bin/just", "agent-musl"]
    assert calls[1][1]["timeout"] == image._AGENT_BUILD_TIMEOUT_SECS


def test_ensure_agent_reraises_clear_error_without_toolchain(monkeypatch):
    import vmon.image as image

    clear = RuntimeError("vmon-agent binary for x86_64 not found; checked: fake")

    def missing(_arch=None):
        raise clear

    monkeypatch.setattr(image, "find_agent_binary", missing)
    monkeypatch.setattr(image.shutil, "which", lambda _name: None)

    with pytest.raises(RuntimeError, match="checked: fake") as exc:
        image.ensure_agent()
    assert exc.value is clear


def test_build_or_pull_normalizes_and_rejects_image_refs(monkeypatch):
    import vmon.image as image

    calls: list[tuple[list[str], dict[str, object]]] = []

    class Result:
        returncode = 0

    def fake_run(cmd: list[str], **kwargs: object) -> Result:
        calls.append((cmd, kwargs))
        return Result()

    monkeypatch.setattr(image.subprocess, "run", fake_run)

    assert image.build_or_pull(" alpine:latest ", None, engine="docker") == (
        "docker",
        "alpine:latest",
    )
    assert calls[0][0] == ["docker", "image", "inspect", "alpine:latest"]

    with pytest.raises(ValueError, match="must not contain whitespace"):
        image.build_or_pull("bad ref", None, engine="docker")


def test_template_marker_current_requires_matching_kernel_sha(tmp_path):
    import vmon.image as image

    kernel_sha = "current-kernel-sha"
    marker = tmp_path / "agent-ready.json"

    def write(**fields):
        payload = {"boot_version": image._TEMPLATE_BOOT_VERSION, **fields}
        marker.write_text(json.dumps(payload), encoding="utf-8")

    base = {"kernel_sha": kernel_sha, "memory": 512, "cpus": 1}

    write(**base)
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, False) is True

    # kernel sha mismatch forces a rebuild
    write(kernel_sha="previous-kernel-sha", memory=512, cpus=1)
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, False) is False

    # missing kernel sha forces a rebuild
    write(memory=512, cpus=1)
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, False) is False

    # missing marker file forces a rebuild
    missing = tmp_path / "missing.json"
    assert image._template_marker_current(missing, kernel_sha, 512, 1, 0, False, False) is False

    # corrupt JSON forces a rebuild
    marker.write_text("{", encoding="utf-8")
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, False) is False

    # resource shape (memory/cpus) must match, else the baked snapshot is wrong-sized
    write(**base)
    assert image._template_marker_current(marker, kernel_sha, 1024, 1, 0, False, False) is False
    assert image._template_marker_current(marker, kernel_sha, 512, 2, 0, False, False) is False

    # slot topology must match the requested fs_slots
    write(kernel_sha=kernel_sha, memory=512, cpus=1, fs_slots=2)
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 2, False, False) is True
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, False) is False

    # reserved host slot must match
    write(kernel_sha=kernel_sha, memory=512, cpus=1, host_slot=True)
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, True, False) is True
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, False) is False

    # network NIC slot must match
    write(kernel_sha=kernel_sha, memory=512, cpus=1, nic_slot=True)
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, True) is True
    assert image._template_marker_current(marker, kernel_sha, 512, 1, 0, False, False) is False


def test_ensure_kernel_expands_home_and_normalizes_arch(monkeypatch, tmp_path):
    import vmon.assets as assets

    home = tmp_path / "home"
    data = b"kernel"
    digest = hashlib.sha256(data).hexdigest()
    cached = home / "vmon-state" / "assets" / "Image"
    cached.parent.mkdir(parents=True)
    cached.write_bytes(data)

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("VMON_HOME", "~/vmon-state")
    monkeypatch.setattr(
        assets, "_KERNELS", {"aarch64": ("Image", "https://example.invalid/kernel", digest)}
    )

    assert assets.ensure_kernel("arm64") == cached


def test_preflight_assets_returns_kernel_and_agent_paths(monkeypatch, tmp_path):
    import vmon.assets as assets
    import vmon.image as image

    kernel = tmp_path / "Image"
    agent = tmp_path / "vmon-agent"

    monkeypatch.setattr(assets, "ensure_kernel", lambda arch=None: kernel)
    monkeypatch.setattr(image, "ensure_agent", lambda arch=None: agent)

    assert assets.preflight_assets("arm64") == {
        "kernel": str(kernel),
        "agent": str(agent),
    }


def test_default_kernel_falls_back_to_auto_provisioned_asset(monkeypatch, tmp_path):
    import vmon.assets as assets
    import vmon.vmm as vmm

    kernel = tmp_path / "Image"

    class MissingBootKernel:
        def __init__(self, _path: str):
            pass

        def is_file(self) -> bool:
            return False

    monkeypatch.delenv("VMON_KERNEL", raising=False)
    monkeypatch.setattr(vmm, "Path", MissingBootKernel)
    monkeypatch.setattr(assets, "ensure_kernel", lambda: kernel)

    assert vmm.default_kernel() == str(kernel)
