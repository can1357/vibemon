"""Internal protobuf inspection bridge used by the Rust CLI.

This module is deliberately not a user-facing CLI.  It imports one local app
module, packages its durable definitions, and writes a compact binary envelope
whose typed payloads are generated ``FunctionSpec`` protobuf messages.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import struct
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, BinaryIO, Literal, cast

from ._function_proto import artifact_ref, options_to_proto, package_to_proto
from .app import App
from .options import FunctionOptions
from .package import package_callable
from .remote import RemoteFunction
from .v1 import api_pb2 as pb

_MAGIC = b"VMONCLI1"


def _field(stream: BinaryIO, value: bytes) -> None:
    stream.write(struct.pack(">I", len(value)))
    stream.write(value)


def _load(path: Path) -> ModuleType:
    if not path.is_file() or path.suffix != ".py":
        raise ValueError(f"target must be an existing Python file: {path}")
    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"cannot import target: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    sys.path.insert(0, str(path.parent))
    try:
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
            spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def _definitions(module: ModuleType, selected: str | None) -> tuple[str, str, list[tuple[str, Any]]]:
    apps = [value for value in vars(module).values() if isinstance(value, App)]
    if selected:
        value: Any = module
        for component in selected.split("."):
            value = getattr(value, component)
        if isinstance(value, App):
            return value.namespace, value.name, sorted(value)
        name = getattr(value, "name", None) or getattr(value, "__name__", selected)
        if not isinstance(name, str) or not name:
            raise ValueError("selected definition has no string name")
        return "default", module.__name__, [(name, value)]
    if len(apps) > 1:
        raise ValueError("target defines multiple App objects; select one with file.py::name")
    if apps:
        app = apps[0]
        return app.namespace, app.name, sorted(app)
    definitions = []
    for name, value in vars(module).items():
        if isinstance(value, RemoteFunction) or hasattr(value, "__vmon_options__"):
            definitions.append((name, value))
    if not definitions:
        raise ValueError("target contains no durable function definitions")
    return "default", module.__name__, sorted(definitions)


def _inspect(definition: Any, root: Path) -> tuple[bytes, bytes]:
    if isinstance(definition, RemoteFunction):
        function = definition._function
        if function is None:
            raise ValueError(f"{definition.name!r} is a lookup handle, not a local definition")
        namespace, name, options = definition.namespace, definition.name, definition.options
        mode = cast(Literal["package", "cloudpickle"], definition.package_mode)
        package_root = definition.package_root
        include, exclude, local_packages = definition.include, definition.exclude, definition.local_packages
    else:
        function = definition
        namespace, name = "default", definition.__name__
        options = getattr(definition, "__vmon_options__", FunctionOptions())
        mode, package_root, include, exclude, local_packages = "package", None, (), (), ()
    package = package_callable(
        function,
        mode=mode,
        root=package_root or root,
        include=include,
        exclude=exclude,
        local_packages=local_packages,
    )
    source = artifact_ref(package.sha256)
    spec = pb.FunctionSpec(
        function=pb.FunctionRef(namespace=namespace, name=name),
        package=package_to_proto(package, source),
        **options_to_proto(options),
    )
    return package.data, spec.SerializeToString(deterministic=True)


def inspect_target(target: str, stream: BinaryIO) -> None:
    """Inspect ``file.py[::qualname]`` and emit framed protobuf definitions."""
    raw_path, separator, selected = target.partition("::")
    path = Path(raw_path).resolve()
    module = _load(path)
    namespace, app_name, definitions = _definitions(module, selected if separator else None)
    stream.write(_MAGIC)
    _field(stream, namespace.encode())
    _field(stream, app_name.encode())
    stream.write(struct.pack(">I", len(definitions)))
    for binding, definition in definitions:
        package, spec = _inspect(definition, path.parent)
        _field(stream, binding.encode())
        _field(stream, package)
        _field(stream, spec)


def main() -> int:
    if len(sys.argv) != 2:
        print("internal inspection helper expects exactly one target", file=sys.stderr)
        return 2
    try:
        inspect_target(sys.argv[1], sys.stdout.buffer)
    except Exception as exc:
        print(f"target inspection failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
