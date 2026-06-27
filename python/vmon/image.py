"""Container image helpers for vmon microVM and sandbox boot flows."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import platform
import re
import shutil
import struct
import subprocess
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class ImageSpec:
    """The bits of an OCI image config the sandbox default command needs."""

    reference: str
    entrypoint: list[str] = field(default_factory=list)
    cmd: list[str] = field(default_factory=list)
    env: list[str] = field(default_factory=list)
    workdir: str = "/"

    def argv(self, override: list[str] | None) -> list[str]:
        """Final process argv (Docker semantics: entrypoint + cmd; override replaces cmd)."""
        if override:
            return [*self.entrypoint, *override] if self.entrypoint else list(override)
        return [*self.entrypoint, *self.cmd]

    def env_dict(self) -> dict[str, str]:
        return dict(e.split("=", 1) for e in self.env if "=" in e)


@dataclass
class CachedTemplate:
    """A boot-verified sandbox template snapshot and its immutable base disk."""

    name: str
    snapshot_dir: Path
    rootfs: Path
    spec: ImageSpec
    image_digest: str
    disk_mb: int
    # Count of pre-provisioned virtio-fs slots (`vmon.slot0..`) baked into the
    # snapshot for warm volume rebinding on restore; 0 is a plain (no-slot) template.
    memory: int = 512
    cpus: int = 1
    fs_slots: int = 0
    # Whether the snapshot includes the reserved "host" virtio-fs device used by fs_dir.
    host_slot: bool = False
    # Whether the snapshot includes a user-mode NAT NIC for warm networked macOS sandboxes.
    nic_slot: bool = False
    # Stable content-addressed digest over the template snapshot, rootfs, and marker.
    digest: str = ""


def _state() -> Path:
    return Path(os.environ.get("VMON_HOME") or str(Path.home() / ".vmon")).expanduser()


def detect_engine() -> str:
    for engine in ("docker", "podman"):
        if shutil.which(engine):
            return engine
    raise RuntimeError("no container engine found (need `docker` or `podman` on PATH)")


def _run(cmd: list[str], **kw) -> str:
    return subprocess.run(cmd, check=True, text=True, capture_output=True, **kw).stdout


def _normalize_reference(reference: str | None) -> str | None:
    if reference is None:
        return None
    ref = str(reference).strip()
    if not ref:
        return None
    if any(ch.isspace() for ch in ref):
        raise ValueError(f"image reference must not contain whitespace: {reference!r}")
    return ref


def build_or_pull(
    reference: str | None, dockerfile: str | None, context: str = ".", engine: str | None = None
) -> tuple[str, str]:
    """Return (engine, image_reference), building the Dockerfile or pulling the ref."""
    engine = engine or detect_engine()
    if dockerfile:
        dockerfile_s = os.fspath(dockerfile)
        context_s = os.fspath(context)
        h = hashlib.sha1(
            (os.path.abspath(dockerfile_s) + "\0" + os.path.abspath(context_s)).encode()
        ).hexdigest()
        tag = "vmon-" + h[:12]
        subprocess.run([engine, "build", "-f", dockerfile_s, "-t", tag, context_s], check=True)
        return engine, tag
    reference = _normalize_reference(reference)
    if not reference:
        raise ValueError("provide an image reference or a --dockerfile")
    if subprocess.run([engine, "image", "inspect", reference], capture_output=True).returncode != 0:
        subprocess.run([engine, "pull", reference], check=True)
    return engine, reference


def _inspect_raw(engine: str, reference: str) -> dict:
    out = _run([engine, "inspect", reference])
    data = json.loads(out)
    if not data:
        raise RuntimeError(f"container image {reference!r} did not inspect to an image")
    return data[0]


def _inspect(engine: str, reference: str) -> ImageSpec:
    cfg = _inspect_raw(engine, reference).get("Config") or {}
    return ImageSpec(
        reference=reference,
        entrypoint=cfg.get("Entrypoint") or [],
        cmd=cfg.get("Cmd") or [],
        env=cfg.get("Env") or [],
        workdir=cfg.get("WorkingDir") or "/",
    )


def _image_digest(engine: str, reference: str) -> str:
    raw = _inspect_raw(engine, reference)
    digest = raw.get("Id") or ""
    repo_digests = raw.get("RepoDigests") or []
    if repo_digests:
        digest = repo_digests[0].split("@", 1)[-1]
    if not digest:
        digest = hashlib.sha256(reference.encode()).hexdigest()
    return re.sub(r"[^A-Za-z0-9_.-]", "-", digest.replace("sha256:", ""))


# Bumped when a template's *booted* state changes incompatibly (e.g. the guest
# agent boot cmdline), so an upgraded vmon rebuilds snapshots an older one took
# instead of silently reusing them. v2: rootfs mounted rw (older builds were ro).
# v3: virtio-fs-capable aarch64 kernel. v4: template identity records resource
# shape and reserved virtio-fs host slots. v5: networked templates can bake a
# user-mode NAT NIC for warm macOS sandboxes.
_TEMPLATE_BOOT_VERSION = 5


def _template_marker_current(
    marker: Path,
    kernel_sha: str,
    memory: int,
    cpus: int,
    fs_slots: int,
    host_slot: bool,
    nic_slot: bool,
) -> bool:
    """True if *marker* records a template booted by the current vmon and kernel.

    Guards against reusing a snapshot whose baked-in boot state predates a
    breaking change (see :data:`_TEMPLATE_BOOT_VERSION`) or whose booted kernel
    differs from the current guest kernel; a missing or older marker forces a
    rebuild.
    """
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return False
    return (
        data.get("boot_version") == _TEMPLATE_BOOT_VERSION
        and data.get("kernel_sha") == kernel_sha
        and data.get("memory") == memory
        and data.get("cpus") == cpus
        and data.get("fs_slots", 0) == fs_slots
        and data.get("host_slot", False) == host_slot
        and data.get("nic_slot", False) == nic_slot
    )


def _marker_content_digest(marker: Path) -> str | None:
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return None
    value = data.get("content_digest")
    if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
        return value
    return None


def _store_marker_content_digest(marker: Path, content_digest: str) -> None:
    data = json.loads(marker.read_text(encoding="utf-8"))
    data["content_digest"] = content_digest
    marker.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _index_cached_template(template: CachedTemplate, marker: Path) -> None:
    # The content digest is authoritative when computed from freshly-built content;
    # templates are immutable after build, so on a cache hit we reuse the stored
    # digest only when its CAS pointer is still live and points here. Otherwise we
    # recompute from content rather than blindly re-index an unverified digest.
    # (Cross-node pulls verify received content against the requested digest.)
    with contextlib.suppress(Exception):
        from . import cas

        cached_digest = _marker_content_digest(marker)
        if cached_digest and cas.resolve_digest(cached_digest) == template.snapshot_dir:
            template.digest = cached_digest
            return
        content_digest = cas.index_template(template.snapshot_dir)
        template.digest = content_digest
        if content_digest != cached_digest:
            _store_marker_content_digest(marker, content_digest)


def slot_tag(i: int) -> str:
    """Reserved virtio-fs tag for warm-volume slot *i*.

    Used identically by the provisioned template (where the slot devices are
    enumerated at boot) and the warm-restore remap (where a request's volumes
    rebind onto these tags). Matches the CLI tag charset ``[a-z0-9_]{1,32}``.
    """
    return f"vmon_slot{i}"


def cached_template(
    image: str | None = None,
    *,
    dockerfile: str | None = None,
    context: str = ".",
    disk_mb: int = 1024,
    engine: str | None = None,
    timeout: float = 300,
    memory: int = 512,
    cpus: int = 1,
    fs_slots: int = 0,
    host_slot: bool = False,
    nic_slot: bool = False,
) -> CachedTemplate:
    """Build/cache an agent-capable ext4 rootfs and a ping-verified VM snapshot."""
    if disk_mb <= 0:
        raise ValueError("disk_mb must be positive")
    if nic_slot and (fs_slots > 0 or host_slot):
        raise ValueError("nic_slot cannot be combined with fs_slots or host_slot")
    engine, reference = build_or_pull(image, dockerfile, context, engine)
    spec = _inspect(engine, reference)
    digest = _image_digest(engine, reference)
    key = f"{digest}-{disk_mb}"
    image_dir = _state() / "images" / key
    rootfs_ext4 = image_dir / "rootfs.ext4"
    spec_path = image_dir / "spec.json"
    image_dir.mkdir(parents=True, exist_ok=True)

    if not rootfs_ext4.is_file():
        with tempfile.TemporaryDirectory(prefix="vmon-image-") as tmp_s:
            tmp = Path(tmp_s)
            rootfs = tmp / "rootfs"
            rootfs.mkdir()
            _export_image(engine, reference, rootfs, tmp)
            _inject_agent(rootfs)
            # Stage the ext4 next to its final path so the atomic rename stays
            # on one filesystem: TemporaryDirectory may sit on tmpfs while the
            # image cache lives on the home/data disk, which makes a /tmp ->
            # cache rename fail with EXDEV ("Invalid cross-device link").
            fd, tmp_ext4_s = tempfile.mkstemp(dir=image_dir, prefix=".rootfs.ext4.tmp-")
            os.close(fd)
            tmp_ext4 = Path(tmp_ext4_s)
            try:
                _mkfs_ext4(rootfs, tmp_ext4, disk_mb)
                os.replace(tmp_ext4, rootfs_ext4)
            finally:
                tmp_ext4.unlink(missing_ok=True)
    spec_path.write_text(json.dumps(asdict(spec), indent=2), encoding="utf-8")

    suffix = (
        f"-m{memory}-c{cpus}"
        + (f"-s{fs_slots}" if fs_slots else "")
        + ("-h" if host_slot else "")
        + ("-n" if nic_slot else "")
    )
    tpl_name = f"tpl-{digest[:12]}-{disk_mb}{suffix}"
    tpl_dir = _state() / "templates" / tpl_name
    template = CachedTemplate(
        name=tpl_name,
        snapshot_dir=tpl_dir,
        rootfs=rootfs_ext4,
        spec=spec,
        image_digest=digest,
        disk_mb=disk_mb,
        memory=memory,
        cpus=cpus,
        fs_slots=fs_slots,
        host_slot=host_slot,
        nic_slot=nic_slot,
    )
    from .vmm import _snapshot_state_present, default_kernel

    kernel_path = default_kernel()
    with open(kernel_path, "rb") as fh:
        kernel_sha = hashlib.file_digest(fh, "sha256").hexdigest()
    marker = tpl_dir / "agent-ready.json"
    if (
        _snapshot_state_present(tpl_dir)
        and (tpl_dir / "rootfs.img").is_file()
        and _template_marker_current(
            marker, kernel_sha, memory, cpus, fs_slots, host_slot, nic_slot
        )
    ):
        _index_cached_template(template, marker)
        return template
    if tpl_dir.exists():
        shutil.rmtree(tpl_dir)

    from .vmm import MicroVM

    vm_name = f"_template-{digest[:12]}-{disk_mb}{suffix}"
    old = MicroVM(vm_name)
    if old.is_running():
        old.stop()
    old.remove()

    slot_void = _state() / "slot-void"
    slot_void.mkdir(parents=True, exist_ok=True)
    slot_vols: list[tuple[str, str | os.PathLike[str], bool]] = [
        (slot_tag(i), str(slot_void), True) for i in range(fs_slots)
    ]
    host_fs_dir = str(slot_void) if host_slot else None
    vm = MicroVM.boot_rootfs(
        rootfs_ext4,
        name=vm_name,
        mem=memory,
        cpus=cpus,
        volumes=slot_vols,
        fs_dir=host_fs_dir,
        user_net=nic_slot,
        snapshot_root=_state() / "templates",
        image=spec.reference,
    )
    try:
        vm.wait_for_agent(timeout=timeout)
        vm.snapshot(
            tpl_name,
            keep_running=False,
            disk_src=rootfs_ext4,
            snapshot_root=_state() / "templates",
            allow_user_net=nic_slot,
        )
        marker.write_text(
            json.dumps(
                {
                    "image": spec.reference,
                    "digest": digest,
                    "disk_mb": disk_mb,
                    "boot_version": _TEMPLATE_BOOT_VERSION,
                    "kernel_sha": kernel_sha,
                    "fs_slots": fs_slots,
                    "memory": memory,
                    "cpus": cpus,
                    "host_slot": host_slot,
                    "nic_slot": nic_slot,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    finally:
        if vm.is_running():
            vm.stop()
        vm.remove()
    _index_cached_template(template, marker)
    return template


def _is_static_elf(path: Path) -> bool:
    """True if the ELF at *path* is statically linked (no PT_INTERP segment).

    A PT_INTERP program header (p_type == 3) names a dynamic loader; its
    presence means the binary is NOT static and would fail on an arbitrary or
    distroless guest rootfs. A fully static or static-PIE binary has none.
    """
    PT_INTERP = 3
    with path.open("rb") as fh:
        header = fh.read(64)
        if len(header) < 16 or header[:4] != b"\x7fELF":
            return False
        ei_class = header[4]  # 1 = ELFCLASS32, 2 = ELFCLASS64
        ei_data = header[5]  # 1 = little-endian, 2 = big-endian
        if ei_data not in (1, 2):
            return False
        endian = "<" if ei_data == 1 else ">"
        if ei_class == 1:
            if len(header) < 48:
                return False
            (e_phoff,) = struct.unpack_from(endian + "I", header, 28)
            e_phentsize, e_phnum = struct.unpack_from(endian + "HH", header, 42)
        elif ei_class == 2:
            if len(header) < 64:
                return False
            (e_phoff,) = struct.unpack_from(endian + "Q", header, 32)
            e_phentsize, e_phnum = struct.unpack_from(endian + "HH", header, 54)
        else:
            return False
        if e_phoff == 0 or e_phnum == 0:
            return True
        for i in range(e_phnum):
            fh.seek(e_phoff + i * e_phentsize)
            entry = fh.read(4)
            if len(entry) < 4:
                break
            (p_type,) = struct.unpack(endian + "I", entry)
            if p_type == PT_INTERP:
                return False
    return True


def _agent_arch(arch: str | None = None) -> str:
    machine = arch or platform.machine()
    return "aarch64" if machine in ("aarch64", "arm64") else "x86_64"


def _first_static(paths: list[Path]) -> Path | None:
    """First path that exists and is a static ELF, else ``None``.

    Executability is intentionally not required: wheel/package-data installs may
    strip the ``+x`` bit from the bundled agent, and ``_inject_agent`` chmods the
    copy in the rootfs regardless.
    """
    for p in paths:
        if p.is_file() and _is_static_elf(p):
            return p
    return None


# The injected agent must be static so it runs on an arbitrary (even
# distroless/scratch) guest rootfs; a dynamically linked one is rejected.
_STATIC_AGENT_HINT = (
    "Build and bundle it with `just agent-musl`, or set VMON_AGENT=/path/to/static-agent."
)

_AGENT_BUILD_TIMEOUT_SECS = 120


def find_agent_binary(arch: str | None = None) -> Path:
    """Locate the static (musl) guest agent injected into every rootfs.

    Resolution order: the ``$VMON_AGENT`` override, the copy bundled in this
    package (``vmon/_agent/vmon-agent-<arch>`` — shipped in the wheel, staged into
    a source checkout by ``just agent-musl``), the cargo target dirs of a
    checkout, then ``PATH``. Non-static candidates are skipped so a stray
    dynamically linked build never masks a usable static one.
    """
    arch = _agent_arch(arch)
    if env := os.environ.get("VMON_AGENT"):
        p = Path(env).expanduser()
        if not p.is_file():
            raise FileNotFoundError(f"VMON_AGENT points to missing file: {p}")
        if not _is_static_elf(p):
            raise RuntimeError(
                "vmon-agent must be a static (musl) binary for arbitrary guest rootfs. "
                f"{_STATIC_AGENT_HINT}"
            )
        return p

    here = Path(__file__).resolve()
    repo = here.parents[2]
    candidates = [
        here.parent / "_agent" / f"vmon-agent-{arch}",
        repo / "target" / f"{arch}-unknown-linux-musl" / "release" / "vmon-agent",
        repo / "target" / "release" / "vmon-agent",
        repo / "target" / f"{arch}-unknown-linux-gnu" / "release" / "vmon-agent",
    ]
    if target_dir := os.environ.get("CARGO_TARGET_DIR"):
        candidates.append(
            Path(target_dir) / f"{arch}-unknown-linux-musl" / "release" / "vmon-agent"
        )
        candidates.append(Path(target_dir) / "release" / "vmon-agent")
    if found := shutil.which("vmon-agent"):
        candidates.append(Path(found))

    checked = ", ".join(str(p) for p in candidates)
    if selected := _first_static(candidates):
        return selected
    if any(p.is_file() for p in candidates):
        raise RuntimeError(
            f"a vmon-agent for {arch} was found but is not a static (musl) binary "
            f"required for arbitrary guest rootfs; checked: {checked}. {_STATIC_AGENT_HINT}"
        )
    raise RuntimeError(
        f"vmon-agent binary for {arch} not found; checked: {checked}. {_STATIC_AGENT_HINT}"
    )


def _musl_target_installed(target: str) -> bool:
    rustup = shutil.which("rustup")
    if not rustup:
        return False
    try:
        result = subprocess.run(
            [rustup, "target", "list", "--installed"],
            check=True,
            text=True,
            capture_output=True,
            timeout=10,
        )
    except OSError, subprocess.SubprocessError:
        return False
    return target in result.stdout.splitlines()


def _build_static_agent(arch: str) -> bool:
    cargo = shutil.which("cargo")
    if not cargo:
        return False
    target = f"{arch}-unknown-linux-musl"
    if not _musl_target_installed(target):
        return False

    repo = Path(__file__).resolve().parents[2]
    if not (repo / "Cargo.toml").is_file():
        return False

    just = shutil.which("just")
    if just and arch == _agent_arch():
        cmd = [just, "agent-musl"]
    else:
        cmd = [cargo, "build", "-p", "vmon-agent", "--target", target, "--release"]
    try:
        subprocess.run(
            cmd,
            cwd=repo,
            check=True,
            text=True,
            capture_output=True,
            timeout=_AGENT_BUILD_TIMEOUT_SECS,
        )
    except OSError, subprocess.SubprocessError:
        return False
    return True


def ensure_agent(arch: str | None = None) -> Path:
    """Return a usable static guest agent, building it from a checkout if needed."""
    try:
        return find_agent_binary(arch)
    except FileNotFoundError, RuntimeError:
        agent_arch = _agent_arch(arch)
        if not _build_static_agent(agent_arch):
            raise
    return find_agent_binary(arch)


def _export_image(engine: str, reference: str, rootfs: Path, work: Path) -> None:
    cid = _run([engine, "create", reference]).strip()
    try:
        tar = work / "rootfs.tar"
        with tar.open("wb") as fh:
            subprocess.run([engine, "export", cid], check=True, stdout=fh)
    finally:
        subprocess.run([engine, "rm", "-f", cid], capture_output=True)
    subprocess.run(["tar", "-xf", str(tar), "-C", str(rootfs)], check=True)
    tar.unlink(missing_ok=True)


def _inject_agent(rootfs: Path) -> None:
    dst_dir = rootfs / ".vmon"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "agent"
    shutil.copy2(ensure_agent(), dst)
    dst.chmod(0o755)


# e2fsprogs binaries are not always on PATH: Homebrew installs the package
# keg-only on macOS (it conflicts with the system filesystem tools), so its
# sbin never lands on the default PATH. Probe the well-known locations.
_E2FSPROGS_DIRS = (
    "/opt/homebrew/opt/e2fsprogs/sbin",
    "/usr/local/opt/e2fsprogs/sbin",
    "/opt/homebrew/sbin",
    "/usr/local/sbin",
    "/sbin",
    "/usr/sbin",
)


def _find_mkfs_ext4() -> str | None:
    """Locate an ``mkfs.ext4`` (or ``mke2fs``) binary, keg-only installs included.

    ``shutil.which`` only consults ``PATH``; fall back to the standard sbin
    locations so a keg-only Homebrew e2fsprogs is found without PATH surgery.
    """
    for name in ("mkfs.ext4", "mke2fs"):
        if found := shutil.which(name):
            return found
    for directory in _E2FSPROGS_DIRS:
        for name in ("mkfs.ext4", "mke2fs"):
            candidate = Path(directory) / name
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)
    return None


def _mkfs_ext4(rootfs: Path, out: Path, disk_mb: int) -> None:
    mkfs = _find_mkfs_ext4()
    if not mkfs:
        raise RuntimeError(
            "mkfs.ext4 not found (install e2fsprogs; on macOS: `brew install e2fsprogs`)"
        )
    with out.open("wb") as fh:
        fh.truncate(disk_mb * 1024 * 1024)
    cmd = [mkfs, "-q", "-F", "-d", str(rootfs), str(out)]
    # `mke2fs` defaults to ext2; force ext4 when we fell back to it.
    if Path(mkfs).name == "mke2fs":
        cmd[1:1] = ["-t", "ext4"]
    subprocess.run(cmd, check=True)
