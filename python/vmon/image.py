"""Container image helpers for vmon microVM and sandbox boot flows."""

from __future__ import annotations

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
    """The bits of an OCI image config the microVM init or sandbox default command needs."""

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
    digest: str
    disk_mb: int


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


def _sh_quote(arg: str) -> str:
    return "'" + arg.replace("'", "'\\''") + "'"


def _init_script(spec: ImageSpec, argv: list[str]) -> str:
    exports = "\n".join(
        f"export {e.split('=', 1)[0]}={_sh_quote(e.split('=', 1)[1])}" for e in spec.env if "=" in e
    )
    run_line = " ".join(_sh_quote(a) for a in argv) if argv else "/bin/sh"
    workdir = spec.workdir or "/"
    return f"""#!/bin/sh
# Auto-generated microVM PID 1 for image: {spec.reference}
mount -t devtmpfs dev /dev 2>/dev/null
exec >/dev/console 2>&1 </dev/console
mount -t proc proc /proc 2>/dev/null
mount -t sysfs sysfs /sys 2>/dev/null
{exports}
cd {_sh_quote(workdir)} 2>/dev/null || cd /
echo "[vmon] container up: {spec.reference} ($(uname -srm))"
{run_line}
echo "[vmon] container exited rc=$?"
reboot -f 2>/dev/null || poweroff -f 2>/dev/null || while true; do sleep 3600; done
"""


def build_rootfs(
    reference: str | None = None,
    dockerfile: str | None = None,
    context: str = ".",
    cmd_override: list[str] | None = None,
    out_dir: str | None = None,
    engine: str | None = None,
) -> tuple[Path, ImageSpec]:
    """Build a legacy initramfs cpio.gz for the image/Dockerfile."""
    engine, reference = build_or_pull(reference, dockerfile, context, engine)
    spec = _inspect(engine, reference)
    argv = spec.argv(cmd_override)

    work = Path(out_dir or tempfile.mkdtemp(prefix="vmon-rootfs-"))
    rootfs = work / "rootfs"
    rootfs.mkdir(parents=True, exist_ok=True)

    _export_image(engine, reference, rootfs, work)

    init = rootfs / "init"
    init.write_text(_init_script(spec, argv))
    init.chmod(0o755)

    initramfs = work / "initramfs.cpio.gz"
    _pack_cpio(rootfs, initramfs)
    return initramfs, spec


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
) -> CachedTemplate:
    """Build/cache an agent-capable ext4 rootfs and a ping-verified VM snapshot."""
    if disk_mb <= 0:
        raise ValueError("disk_mb must be positive")
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
    spec_path.write_text(json.dumps(asdict(spec), indent=2))

    tpl_name = f"tpl-{digest[:12]}-{disk_mb}"
    tpl_dir = _state() / "templates" / tpl_name
    template = CachedTemplate(tpl_name, tpl_dir, rootfs_ext4, spec, digest, disk_mb)
    from .vmm import _snapshot_state_present

    marker = tpl_dir / "agent-ready.json"
    if _snapshot_state_present(tpl_dir) and (tpl_dir / "rootfs.img").is_file() and marker.is_file():
        return template
    if tpl_dir.exists():
        shutil.rmtree(tpl_dir)

    from .vmm import MicroVM

    vm_name = f"_template-{digest[:12]}-{disk_mb}"
    old = MicroVM(vm_name)
    if old.is_running():
        old.stop()
    old.remove()

    vm = MicroVM.boot_rootfs(
        rootfs_ext4,
        name=vm_name,
        mem=memory,
        cpus=cpus,
        snapshot_root=_state() / "templates",
        image=spec.reference,
    )
    try:
        vm.wait_for_agent(timeout=timeout)
        vm.snapshot(
            tpl_name, keep_running=False, disk_src=rootfs_ext4, snapshot_root=_state() / "templates"
        )
        marker.write_text(
            json.dumps({"image": spec.reference, "digest": digest, "disk_mb": disk_mb}, indent=2)
        )
    finally:
        if vm.is_running():
            vm.stop()
        vm.remove()
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


def _agent_arch() -> str:
    return "aarch64" if platform.machine() in ("aarch64", "arm64") else "x86_64"


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
    "Build and bundle it with `just agent-musl`, set VMON_AGENT=/path/to/static-agent, "
    "or pass `--no-agent` to use the legacy console path."
)


def find_agent_binary() -> Path:
    """Locate the static (musl) guest agent injected into every rootfs.

    Resolution order: the ``$VMON_AGENT`` override, the copy bundled in this
    package (``vmon/_agent/vmon-agent-<arch>`` — shipped in the wheel, staged into
    a source checkout by ``just agent-musl``), the cargo target dirs of a
    checkout, then ``PATH``. Non-static candidates are skipped so a stray
    dynamically linked build never masks a usable static one.
    """
    arch = _agent_arch()
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
    shutil.copy2(find_agent_binary(), dst)
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


def _pack_cpio(rootfs: Path, out: Path) -> None:
    find = subprocess.Popen(["find", ".", "-print0"], cwd=rootfs, stdout=subprocess.PIPE)
    cpio = subprocess.Popen(
        ["cpio", "--null", "-o", "-H", "newc"],
        cwd=rootfs,
        stdin=find.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    with out.open("wb") as fh:
        gzip = subprocess.Popen(["gzip", "-1"], stdin=cpio.stdout, stdout=fh)
    if find.stdout is not None:
        find.stdout.close()
    if cpio.stdout is not None:
        cpio.stdout.close()
    gzip.communicate()
    find.wait()
    cpio.wait()
    if gzip.returncode != 0 or cpio.returncode not in (0, None) or find.returncode not in (0, None):
        raise RuntimeError("failed to pack initramfs")
