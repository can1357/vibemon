from __future__ import annotations

import importlib
import io
import sys
import zipfile

from vmon.package import build_package, package_callable, resolve_qualname


def test_package_callable_from_initializer_imports_after_extraction(tmp_path):
    source = tmp_path / "source"
    package = source / "initializer_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("def run(value):\n    return value + 3\n")
    (package / "included.txt").write_text("included")
    (package / "omitted.txt").write_text("omitted")

    sys.path.insert(0, str(source))
    try:
        module = importlib.import_module("initializer_pkg")
        artifact = package_callable(
            module.run,
            include=("/initializer_pkg/__init__.py", "/initializer_pkg/included.txt"),
        )
    finally:
        sys.path.remove(str(source))
        sys.modules.pop("initializer_pkg", None)

    assert artifact.manifest.module == "initializer_pkg"
    assert [entry.path for entry in artifact.manifest.files] == [
        "initializer_pkg/__init__.py",
        "initializer_pkg/included.txt",
    ]

    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(io.BytesIO(artifact.data)) as archive:
        archive.extractall(extracted)
    sys.path.insert(0, str(extracted))
    try:
        extracted_module = importlib.import_module(artifact.manifest.module)
        callable_ = resolve_qualname(extracted_module, artifact.manifest.qualname)
        assert callable_(4) == 7
        assert artifact.resolve()(5) == 8
    finally:
        sys.path.remove(str(extracted))
        sys.modules.pop("initializer_pkg", None)


def test_globs_distinguish_single_and_recursive_wildcards(tmp_path):
    root = tmp_path / "root"
    (root / "pkg" / "nested").mkdir(parents=True)
    (root / "pkg" / "direct.txt").write_text("direct")
    (root / "pkg" / "nested" / "deep.txt").write_text("deep")

    direct = build_package(root, module="pkg", qualname="run", include=("pkg/*.txt",))
    recursive = build_package(root, module="pkg", qualname="run", include=("pkg/**/*.txt",))

    assert [entry.path for entry in direct.manifest.files] == ["pkg/direct.txt"]
    assert [entry.path for entry in recursive.manifest.files] == [
        "pkg/direct.txt",
        "pkg/nested/deep.txt",
    ]


def test_leading_slash_anchors_glob_to_archive_root(tmp_path):
    root = tmp_path / "root"
    (root / "nested").mkdir(parents=True)
    (root / "choice.txt").write_text("root")
    (root / "nested" / "choice.txt").write_text("nested")

    anchored = build_package(root, module="unused", qualname="unused", include=("/choice.txt",))
    unanchored = build_package(root, module="unused", qualname="unused", include=("choice.txt",))

    assert [entry.path for entry in anchored.manifest.files] == ["choice.txt"]
    assert [entry.path for entry in unanchored.manifest.files] == [
        "choice.txt",
        "nested/choice.txt",
    ]
