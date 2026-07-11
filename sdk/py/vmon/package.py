from __future__ import annotations

import fnmatch
import hashlib
import importlib
import inspect
import io
import json
import os
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Callable, Literal

MANIFEST_PATH = "vmon-package.json"
PYTHON_ABI = sys.implementation.cache_tag or f"py{sys.version_info.major}{sys.version_info.minor}"
_DEFAULT_EXCLUDES = (
    ".git/**",
    ".hg/**",
    ".svn/**",
    ".venv/**",
    "venv/**",
    "env/**",
    "__pycache__/**",
    "*.py[cod]",
    ".pytest_cache/**",
    ".mypy_cache/**",
    ".ruff_cache/**",
    ".tox/**",
    ".nox/**",
    "build/**",
    "dist/**",
    "*.egg-info/**",
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "*credentials*",
    "*secret*",
    ".DS_Store",
)


class PackageError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ManifestFile:
    path: str
    size: int
    sha256: str

    def as_dict(self) -> dict[str, object]:
        return {"path": self.path, "sha256": self.sha256, "size": self.size}


@dataclass(frozen=True, slots=True)
class PackageManifest:
    version: int
    python_abi: str
    module: str
    qualname: str
    files: tuple[ManifestFile, ...]

    def to_bytes(self) -> bytes:
        value = {
            "files": [entry.as_dict() for entry in self.files],
            "module": self.module,
            "python_abi": self.python_abi,
            "qualname": self.qualname,
            "version": self.version,
        }
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> PackageManifest:
        value = json.loads(data)
        return cls(
            version=value["version"],
            python_abi=value["python_abi"],
            module=value["module"],
            qualname=value["qualname"],
            files=tuple(ManifestFile(**entry) for entry in value["files"]),
        )


@dataclass(frozen=True, slots=True)
class PackageArtifact:
    data: bytes
    sha256: str
    manifest: PackageManifest
    codec: Literal["zip"] = "zip"

    def resolve(self) -> object:
        return resolve_qualname(importlib.import_module(self.manifest.module), self.manifest.qualname)


@dataclass(frozen=True, slots=True)
class SerializedCallable:
    data: bytes
    sha256: str
    python_abi: str
    codec: Literal["cloudpickle"] = "cloudpickle"
    codec_version: str = ""

    def load(self, *, trusted: bool = False) -> Callable[..., object]:
        if not trusted:
            raise PermissionError("cloudpickle callable loading requires trusted=True")
        if self.python_abi != PYTHON_ABI:
            raise PackageError(
                f"Python ABI mismatch: artifact={self.python_abi}, runtime={PYTHON_ABI}"
            )
        import cloudpickle

        if self.codec_version != cloudpickle.__version__:
            raise PackageError(
                f"cloudpickle codec mismatch: artifact={self.codec_version}, "
                f"runtime={cloudpickle.__version__}"
            )
        value = cloudpickle.loads(self.data)
        if not callable(value):
            raise PackageError("serialized value is not callable")
        return value


def resolve_qualname(module: ModuleType, qualname: str) -> object:
    """Resolve an importable qualified name from a module."""
    value: object = module
    for component in qualname.split("."):
        if component == "<locals>" or not component:
            raise PackageError(f"{qualname!r} is not an importable qualified name")
        try:
            value = getattr(value, component)
        except AttributeError as exc:
            raise PackageError(f"cannot resolve {module.__name__}.{qualname}") from exc
    return value


def inspect_manifest(archive: bytes | bytearray | memoryview | str | os.PathLike[str]) -> PackageManifest:
    """Read and validate the embedded manifest from package ZIP bytes or a path."""
    if isinstance(archive, (bytes, bytearray, memoryview)):
        source: io.BytesIO | str | os.PathLike[str] = io.BytesIO(bytes(archive))
    else:
        source = archive
    with zipfile.ZipFile(source) as package:
        try:
            return PackageManifest.from_bytes(package.read(MANIFEST_PATH))
        except KeyError as exc:
            raise PackageError(f"archive does not contain {MANIFEST_PATH}") from exc


def build_package(
    root: str | os.PathLike[str],
    *,
    module: str,
    qualname: str,
    include: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    local_packages: tuple[str | os.PathLike[str], ...] = (),
) -> PackageArtifact:
    """Build a deterministic ZIP artifact for an importable package tree."""
    root_path = Path(root).resolve()
    if not root_path.is_dir():
        raise PackageError(f"package root is not a directory: {root_path}")
    if "<locals>" in qualname.split("."):
        raise PackageError("package mode cannot package closures; use mode='cloudpickle'")

    selected: dict[str, bytes] = {}
    _collect_tree(root_path, root_path, selected, include, exclude)
    for package in local_packages:
        package_path = Path(package).resolve()
        if not package_path.is_dir():
            raise PackageError(f"local package is not a directory: {package_path}")
        prefix = package_path.name
        _collect_tree(package_path, package_path, selected, include, exclude, prefix=prefix)
    if not selected:
        raise PackageError("package contains no files")

    entries = tuple(
        ManifestFile(path, len(data), hashlib.sha256(data).hexdigest())
        for path, data in sorted(selected.items())
    )
    manifest = PackageManifest(1, PYTHON_ABI, module, qualname, entries)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path, data in sorted(selected.items()):
            _write_zip_entry(archive, path, data)
        _write_zip_entry(archive, MANIFEST_PATH, manifest.to_bytes())
    artifact = output.getvalue()
    return PackageArtifact(artifact, hashlib.sha256(artifact).hexdigest(), manifest)


def package_callable(
    function: Callable[..., object],
    *,
    mode: Literal["package", "cloudpickle"] = "package",
    root: str | os.PathLike[str] | None = None,
    include: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    local_packages: tuple[str | os.PathLike[str], ...] = (),
) -> PackageArtifact | SerializedCallable:
    """Package an importable callable or explicitly serialize trusted Python code."""
    if mode == "cloudpickle":
        import cloudpickle

        data = cloudpickle.dumps(function, protocol=5)
        return SerializedCallable(
            data=data,
            sha256=hashlib.sha256(data).hexdigest(),
            python_abi=PYTHON_ABI,
            codec_version=cloudpickle.__version__,
        )
    if mode != "package":
        raise PackageError(f"unsupported callable package mode: {mode}")
    module = inspect.getmodule(function)
    module_name = getattr(function, "__module__", None)
    qualname = getattr(function, "__qualname__", None)
    if module is None or not module_name or not qualname or module_name == "__main__":
        raise PackageError("package mode requires a callable from a normal importable module")
    if "<locals>" in qualname.split(".") or inspect.isfunction(function) and function.__closure__:
        raise PackageError("package mode cannot package closures; use mode='cloudpickle'")
    module_file = getattr(module, "__file__", None)
    if module_file is None:
        raise PackageError(f"module {module_name!r} has no filesystem source")
    package_root = Path(root).resolve() if root is not None else _module_root(Path(module_file), module_name)
    return build_package(
        package_root,
        module=module_name,
        qualname=qualname,
        include=include,
        exclude=exclude,
        local_packages=local_packages,
    )


def _module_root(module_file: Path, module_name: str) -> Path:
    path = module_file.resolve()
    components = module_name.split(".")
    root = path.parent
    for _ in range(max(0, len(components) - 1)):
        root = root.parent
    return root


def _collect_tree(
    tree: Path,
    relative_to: Path,
    selected: dict[str, bytes],
    include: tuple[str, ...],
    exclude: tuple[str, ...],
    *,
    prefix: str = "",
) -> None:
    for path in sorted(tree.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(relative_to).as_posix()
        archive_path = PurePosixPath(prefix, relative).as_posix() if prefix else relative
        if _matches(archive_path, _DEFAULT_EXCLUDES) or _matches(relative, _DEFAULT_EXCLUDES):
            continue
        if exclude and (_matches(archive_path, exclude) or _matches(relative, exclude)):
            continue
        if include and not (_matches(archive_path, include) or _matches(relative, include)):
            continue
        if archive_path == MANIFEST_PATH:
            raise PackageError(f"{MANIFEST_PATH} is reserved")
        if archive_path in selected:
            raise PackageError(f"duplicate archive path: {archive_path}")
        selected[archive_path] = path.read_bytes()


def _matches(path: str, patterns: tuple[str, ...]) -> bool:
    parts = PurePosixPath(path).parts
    suffixes = ("/".join(parts[index:]) for index in range(len(parts)))
    candidates = tuple(suffixes)
    for pattern in patterns:
        normalized = pattern.replace("\\", "/").lstrip("/")
        if any(fnmatch.fnmatchcase(candidate, normalized) for candidate in candidates):
            return True
        if "/" not in normalized and any(fnmatch.fnmatchcase(part, normalized) for part in parts):
            return True
    return False


def _write_zip_entry(archive: zipfile.ZipFile, path: str, data: bytes) -> None:
    info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    info.flag_bits = 0
    archive.writestr(info, data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
