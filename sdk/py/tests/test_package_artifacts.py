from __future__ import annotations

import importlib
import io
import sys
import zipfile
from dataclasses import replace

import pytest

from vmon.package import (
    PackageError,
    build_package,
    inspect_manifest,
    package_callable,
    resolve_qualname,
)


def test_package_zip_is_deterministic_and_excludes_sensitive_files(tmp_path):
    root = tmp_path / "project"
    package = root / "demo"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("from .worker import run\n")
    (package / "worker.py").write_text("def run(value):\n    return value + 1\n")
    (root / "uv.lock").write_text("version = 1\n")
    (root / ".env.production").write_text("TOKEN=nope\n")
    (root / "credentials.json").write_text("{}")
    cache = package / "__pycache__"
    cache.mkdir()
    (cache / "worker.pyc").write_bytes(b"cache")

    first = build_package(root, module="demo.worker", qualname="run")
    second = build_package(root, module="demo.worker", qualname="run")

    assert first.data == second.data
    assert first.sha256 == second.sha256
    manifest = inspect_manifest(first.data)
    assert first.manifest.python_abi == f"cp{sys.version_info.major}{sys.version_info.minor}"
    assert manifest == first.manifest
    assert [entry.path for entry in manifest.files] == [
        "demo/__init__.py",
        "demo/worker.py",
        "uv.lock",
    ]
    with zipfile.ZipFile(io.BytesIO(first.data)) as archive:
        assert archive.namelist() == [
            "demo/__init__.py",
            "demo/worker.py",
            "uv.lock",
            "vmon-package.json",
        ]
        assert {entry.date_time for entry in archive.infolist()} == {(1980, 1, 1, 0, 0, 0)}


def test_include_exclude_and_external_local_package(tmp_path):
    root = tmp_path / "app-root"
    (root / "app").mkdir(parents=True)
    (root / "app" / "__init__.py").write_text("")
    (root / "app" / "main.py").write_text("def run(): return 'ok'\n")
    (root / "app" / "debug.log").write_text("omit")
    shared = tmp_path / "shared_lib"
    shared.mkdir()
    (shared / "__init__.py").write_text("")
    (shared / "helper.py").write_text("VALUE = 4\n")

    artifact = build_package(
        root,
        module="app.main",
        qualname="run",
        exclude=("*.log",),
        local_packages=(shared,),
    )

    assert [entry.path for entry in artifact.manifest.files] == [
        "app/__init__.py",
        "app/main.py",
        "shared_lib/__init__.py",
        "shared_lib/helper.py",
    ]


def test_packaged_module_and_qualname_resolve_after_extraction(tmp_path):
    root = tmp_path / "source"
    package = root / "multi"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "service.py").write_text(
        "from .util import OFFSET\n"
        "class Jobs:\n"
        "    @staticmethod\n"
        "    def run(value): return value + OFFSET\n"
    )
    (package / "util.py").write_text("OFFSET = 7\n")
    artifact = build_package(root, module="multi.service", qualname="Jobs.run")
    destination = tmp_path / "extracted"
    with zipfile.ZipFile(io.BytesIO(artifact.data)) as archive:
        archive.extractall(destination)
    sys.path.insert(0, str(destination))
    try:
        module = importlib.import_module(artifact.manifest.module)
        function = resolve_qualname(module, artifact.manifest.qualname)
        assert function(5) == 12
    finally:
        sys.path.remove(str(destination))
        for name in ("multi.service", "multi.util", "multi"):
            sys.modules.pop(name, None)


def test_closures_require_explicit_trusted_cloudpickle_mode():
    captured = 9

    def closure(value):
        return value + captured

    with pytest.raises(PackageError, match="cloudpickle"):
        package_callable(closure)

    artifact = package_callable(closure, mode="cloudpickle")
    assert artifact.python_abi == f"cp{sys.version_info.major}{sys.version_info.minor}"
    with pytest.raises(PermissionError, match="trusted=True"):
        artifact.load()
    assert artifact.load(trusted=True)(3) == 12
    with pytest.raises(PackageError, match="ABI mismatch"):
        replace(artifact, python_abi="cp999").load(trusted=True)
    with pytest.raises(PackageError, match="codec mismatch"):
        replace(artifact, codec_version="0.0").load(trusted=True)
