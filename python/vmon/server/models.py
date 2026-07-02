"""Request models and input parsing helpers for the server API."""

from __future__ import annotations

import ipaddress
import math
from typing import Any

from fastapi import HTTPException, status
from pydantic import BaseModel, Field

from ..mesh import MeshError, validate_request_arch
from ..records import ALLOWED_HA, restart_policy_for_ha


class SandboxCreate(BaseModel):
    image: str | None = None
    template: str | None = None
    dockerfile: str | None = None
    context: str = "."
    name: str | None = None
    cpus: int = 1
    memory: int = 512
    disk_mb: int = 1024
    timeout: float | None = 300
    timeout_secs: int | None = None
    workdir: str | None = None
    env: dict[str, str] | None = None
    secrets: list[dict[str, str]] | None = None
    volumes: dict[str, str | dict[str, Any]] | None = None
    tags: dict[str, str] | None = None
    fs_dir: str | None = None
    block_network: bool = False
    ports: list[int] | None = None
    egress_allow: list[str] | None = None
    egress_allow_domains: list[str] | None = None
    inbound_cidr_allowlist: list[str] | None = None
    readiness_probe: Any | None = None
    pool_size: int = 0
    remote_page_url: str | None = None
    remote_page_token: str | None = None
    remote_page_digest: str | None = None
    ha: str | None = None
    arch: str | None = None
    idempotency_key: str | None = None


class ExecRequest(BaseModel):
    cmd: list[str] = Field(default_factory=list)
    workdir: str | None = None
    cwd: str | None = None
    env: dict[str, str] | None = None
    timeout: float | None = None
    tty: bool = False


class SnapshotRequest(BaseModel):
    name: str | None = None


class SnapshotTemplateRequest(BaseModel):
    snapshot: str
    stop: bool = False


class ExtendRequest(BaseModel):
    secs: int


class NetworkRequest(BaseModel):
    block_network: bool | None = None
    cidr_allow: list[str] | None = None
    domain_allow: list[str] | None = None


def _bad_request(message: str) -> HTTPException:
    return HTTPException(status.HTTP_400_BAD_REQUEST, detail=message)


def _require_positive_int(name: str, value: int) -> None:
    if value <= 0:
        raise _bad_request(f"{name} must be positive")


def _require_non_negative_int(name: str, value: int) -> None:
    if value < 0:
        raise _bad_request(f"{name} must be non-negative")


def _require_non_negative_float(name: str, value: float | None) -> None:
    if value is not None and (not math.isfinite(float(value)) or value < 0):
        raise _bad_request(f"{name} must be non-negative")


def _validate_ports(ports: list[int] | None) -> None:
    for port in ports or ():
        if port <= 0 or port > 65535:
            raise _bad_request("ports must be TCP port numbers from 1 to 65535")


def _validate_cidrs(name: str, cidrs: list[str] | None) -> None:
    for cidr in cidrs or ():
        try:
            ipaddress.ip_network(cidr, strict=False)
        except ValueError as exc:
            raise _bad_request(f"{name} entries must be valid CIDR networks") from exc


def _validate_domains(name: str, domains: list[str] | None) -> None:
    for domain in domains or ():
        if not domain.strip():
            raise _bad_request(f"{name} entries must be non-empty")


def _validate_ha_value(value: str | None) -> None:
    if value is not None and value not in ALLOWED_HA:
        allowed = ", ".join(sorted(ALLOWED_HA))
        raise _bad_request(f"ha must be one of: {allowed}")


def _validate_arch_value(value: Any) -> None:
    try:
        validate_request_arch(value)
    except MeshError as exc:
        raise _bad_request(exc.message) from exc


def _validate_arch_param(params: dict[str, Any]) -> None:
    _validate_arch_value(params.get("arch"))


def _default_ha(mesh_enabled: bool, config: Any | None = None) -> str:
    default_ha = getattr(config, "default_ha", None)
    if callable(default_ha):
        value = default_ha(mesh_enabled=mesh_enabled)
        if isinstance(value, str) and value in ALLOWED_HA:
            return value
    return "async" if mesh_enabled else "off"


def _apply_ha_defaults(
    params: dict[str, Any], *, mesh_enabled: bool, config: Any | None = None
) -> str:
    raw = params.get("ha")
    ha = _default_ha(mesh_enabled, config) if raw is None or raw == "" else str(raw)
    if ha not in ALLOWED_HA:
        allowed = ", ".join(sorted(ALLOWED_HA))
        raise ValueError(f"ha must be one of: {allowed}")
    params["ha"] = ha
    params["restart_policy"] = restart_policy_for_ha(ha)
    return ha


def _validate_create_request(request: SandboxCreate) -> None:
    _require_positive_int("cpus", request.cpus)
    _require_positive_int("memory", request.memory)
    _require_positive_int("disk_mb", request.disk_mb)
    _require_non_negative_int("pool_size", request.pool_size)
    _require_non_negative_float("timeout", request.timeout)
    if request.timeout_secs is not None:
        _require_non_negative_int("timeout_secs", request.timeout_secs)
    if request.remote_page_url or request.remote_page_token or request.remote_page_digest:
        raise _bad_request("remote_page_* fields are server-internal")
    if request.block_network and request.ports:
        raise _bad_request("ports cannot be exposed when block_network=True")
    _validate_ports(request.ports)
    _validate_cidrs("egress_allow", request.egress_allow)
    _validate_cidrs("inbound_cidr_allowlist", request.inbound_cidr_allowlist)
    _validate_domains("egress_allow_domains", request.egress_allow_domains)
    _validate_ha_value(request.ha)
    _validate_arch_value(request.arch)


def _validate_exec_request(request: ExecRequest) -> None:
    if not request.cmd:
        raise _bad_request("exec cmd must not be empty")
    if not request.cmd[0]:
        raise _bad_request("exec cmd[0] must not be empty")
    _require_non_negative_float("timeout", request.timeout)


def _validate_extend_request(request: ExtendRequest) -> None:
    _require_positive_int("secs", request.secs)


def _validate_network_request(request: NetworkRequest) -> None:
    if (
        request.block_network is None
        and request.cidr_allow is None
        and request.domain_allow is None
    ):
        raise _bad_request("network request must set at least one field")
    _validate_cidrs("cidr_allow", request.cidr_allow)
    _validate_domains("domain_allow", request.domain_allow)


def _parse_warm_images(value: str | None, *, default_count: int = 1) -> list[tuple[str, int]]:
    if not value:
        return []
    if default_count < 0:
        raise ValueError("VMON_WARM_IMAGES counts must be non-negative integers")
    entries: list[tuple[str, int]] = []
    for raw_entry in value.split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        ref = entry
        count = default_count
        if "=" in entry:
            ref, raw_count = entry.rsplit("=", 1)
            count = _parse_warm_image_count(raw_count)
        ref = ref.strip()
        if not ref:
            raise ValueError("VMON_WARM_IMAGES entries must include an image reference")
        entries.append((ref, count))
    return entries


def _parse_warm_image_count(raw: str) -> int:
    try:
        count = int(raw.strip())
    except ValueError as exc:
        raise ValueError("VMON_WARM_IMAGES counts must be non-negative integers") from exc
    if count < 0:
        raise ValueError("VMON_WARM_IMAGES counts must be non-negative integers")
    return count
