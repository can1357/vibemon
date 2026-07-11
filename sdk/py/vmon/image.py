from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import ClassVar, Literal, Self


class ImageError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ImageSource:
    kind: Literal["python", "registry", "dockerfile", "template"]
    value: str
    context: str | None = None


type ImageStepKind = Literal[
    "uv_sync",
    "uv_install",
    "apt_install",
    "env",
    "run_commands",
    "add_local_file",
    "add_local_python_source",
]


@dataclass(frozen=True, slots=True)
class ImageStep:
    kind: ImageStepKind
    values: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Image:
    source: ImageSource
    steps: tuple[ImageStep, ...] = ()

    _PYTHON_VERSION: ClassVar[re.Pattern[str]] = re.compile(r"^\d+\.\d+(?:\.\d+)?$")

    @classmethod
    def python(cls, version: str = "3.14", *, slim: bool = True) -> Self:
        if not cls._PYTHON_VERSION.fullmatch(version):
            raise ImageError(f"invalid Python version: {version!r}")
        suffix = "-slim" if slim else ""
        return cls(ImageSource("python", f"{version}{suffix}"))

    @classmethod
    def from_registry(cls, reference: str) -> Self:
        return cls(ImageSource("registry", _nonempty(reference, "registry reference")))

    @classmethod
    def from_dockerfile(
        cls,
        dockerfile: str | os.PathLike[str] = "Dockerfile",
        *,
        context: str | os.PathLike[str] = ".",
    ) -> Self:
        return cls(
            ImageSource(
                "dockerfile",
                os.fspath(dockerfile),
                context=os.fspath(context),
            )
        )

    @classmethod
    def from_template(cls, template: str) -> Self:
        return cls(ImageSource("template", _nonempty(template, "template")))

    def uv_sync(
        self,
        *,
        frozen: bool = True,
        project: str | os.PathLike[str] = ".",
        groups: tuple[str, ...] = (),
    ) -> Self:
        values = (os.fspath(project), "frozen" if frozen else "unlocked", *_strings(groups, "group"))
        return self._step("uv_sync", values)

    def uv_install(self, *packages: str) -> Self:
        return self._step("uv_install", _strings(packages, "package", required=True))

    def apt_install(self, *packages: str) -> Self:
        return self._step("apt_install", _strings(packages, "package", required=True))

    def env(self, values: dict[str, str] | None = None, /, **variables: str) -> Self:
        merged = dict(values or {})
        merged.update(variables)
        if not merged:
            raise ImageError("env requires at least one variable")
        entries: list[str] = []
        for key, value in sorted(merged.items()):
            if not key or "=" in key or "\x00" in key:
                raise ImageError(f"invalid environment variable name: {key!r}")
            if not isinstance(value, str) or "\x00" in value:
                raise ImageError(f"invalid value for environment variable {key!r}")
            entries.extend((key, value))
        return self._step("env", tuple(entries))

    def run_commands(self, *commands: str) -> Self:
        return self._step("run_commands", _strings(commands, "command", required=True))

    def add_local_file(
        self,
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
    ) -> Self:
        return self._step(
            "add_local_file",
            (os.fspath(source), _container_path(destination)),
        )

    def add_local_python_source(
        self,
        source: str | os.PathLike[str],
        *,
        destination: str | os.PathLike[str] = "/opt/vmon/src",
    ) -> Self:
        return self._step(
            "add_local_python_source",
            (os.fspath(source), _container_path(destination)),
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "source": {
                "context": self.source.context,
                "kind": self.source.kind,
                "value": self.source.value,
            },
            "steps": [
                {"kind": step.kind, "values": list(step.values)} for step in self.steps
            ],
        }

    @property
    def digest(self) -> str:
        encoded = json.dumps(
            self.canonical_dict(), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    def _step(self, kind: ImageStepKind, values: tuple[str, ...]) -> Self:
        return replace(self, steps=(*self.steps, ImageStep(kind, values)))


def _nonempty(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ImageError(f"{label} must be a non-empty string")
    return value


def _strings(
    values: tuple[str, ...],
    label: str,
    *,
    required: bool = False,
) -> tuple[str, ...]:
    if required and not values:
        raise ImageError(f"at least one {label} is required")
    return tuple(_nonempty(value, label) for value in values)


def _container_path(value: str | os.PathLike[str]) -> str:
    path = os.fspath(value)
    if not path.startswith("/") or ".." in PurePosixPath(path).parts:
        raise ImageError(f"container destination must be an absolute normalized path: {path!r}")
    return path
