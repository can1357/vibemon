"""Serve-time configuration resolution for the vmon gateway.

The gateway has one configuration choke point: dataclass defaults, then an
optional TOML file, then ``VMON_*`` environment variables, then explicit CLI
flags. Runtime modules consume :class:`ServeConfig` instead of reading these
variables directly.
"""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Literal, get_args

ConfigSource = Literal["default", "file", "env", "flag"]
RestoreQuorum = bool | None


class ConfigError(ValueError):
    """A serve configuration source is malformed."""


@dataclass(frozen=True)
class WarmImage:
    """One image reference to prewarm and its requested pool size."""

    ref: str
    count: int


@dataclass(frozen=True)
class ServeConfig:
    """Resolved configuration consumed by ``vmon serve`` and ``vmon doctor --serve``."""

    home: Path = field(default_factory=lambda: Path.home() / ".vmon")
    host: str = "127.0.0.1"
    port: int = 8000
    token: str | None = None
    client_token: str | None = None
    tls_cert: str | None = None
    tls_key: str | None = None
    idle_timeout: float = 300.0
    replicate_sec: float | None = None
    replicas: int = 1
    replicate_concurrency: int = 2
    restore_quorum: RestoreQuorum = None
    warm_pool_size: int = 1
    warm_images: tuple[WarmImage, ...] = ()
    mesh_heartbeat_sec: float = 3.0
    mesh_reap_sec: float = 300.0
    mesh_idem_ttl_sec: float = 900.0
    mesh_create_timeout_sec: float = 120.0
    mesh_w_warm: float = 1000.0
    mesh_w_free: float = 100.0
    mesh_w_local: float = 50.0
    mesh_w_region: float = 30.0
    mesh_w_inflight: float = 80.0
    _sources: dict[str, ConfigSource] = field(
        default_factory=dict,
        init=False,
        repr=False,
        compare=False,
        metadata={"internal": True},
    )

    def source(self, key: str) -> ConfigSource:
        """Return the source that supplied a field's final value."""

        if key not in SERVE_CONFIG_KEYS:
            raise KeyError(key)
        return self._sources.get(key, "default")

    def effective_replicate_sec(self, *, mesh_enabled: bool) -> float:
        """Return the actual checkpoint cadence for the current mesh state."""

        if self.replicate_sec is not None:
            return self.replicate_sec
        return 60.0 if mesh_enabled else 0.0

    def restore_quorum_enabled(self, *, expected_members: int) -> bool:
        """Return whether automatic restore should require majority confirmation."""

        if self.restore_quorum is not None:
            return self.restore_quorum
        return expected_members >= 3

    def default_ha(self, *, mesh_enabled: bool) -> Literal["off", "async"]:
        """Return the default per-sandbox HA tier for a create request."""

        return "async" if mesh_enabled else "off"

    def placement_weights(self) -> dict[str, float]:
        """Return placement scoring weights in the names used by :mod:`vmon.mesh`."""

        return {
            "warm": self.mesh_w_warm,
            "free": self.mesh_w_free,
            "local": self.mesh_w_local,
            "region": self.mesh_w_region,
            "inflight": self.mesh_w_inflight,
        }


SERVE_CONFIG_KEYS = tuple(
    field_.name for field_ in fields(ServeConfig) if field_.metadata.get("internal") is not True
)

ENV_KEYS: dict[str, str] = {
    "home": "VMON_HOME",
    "host": "VMON_SERVE_HOST",
    "port": "VMON_SERVE_PORT",
    "token": "VMON_API_TOKEN",
    "client_token": "VMON_CLIENT_TOKEN",
    "tls_cert": "VMON_TLS_CERT",
    "tls_key": "VMON_TLS_KEY",
    "idle_timeout": "VMON_IDLE_TIMEOUT",
    "replicate_sec": "VMON_REPLICATE_SEC",
    "replicas": "VMON_REPLICAS",
    "replicate_concurrency": "VMON_REPLICATE_CONCURRENCY",
    "restore_quorum": "VMON_RESTORE_QUORUM",
    "warm_pool_size": "VMON_WARM_POOL_SIZE",
    "warm_images": "VMON_WARM_IMAGES",
    "mesh_heartbeat_sec": "VMON_MESH_HEARTBEAT_SEC",
    "mesh_reap_sec": "VMON_MESH_REAP_SEC",
    "mesh_idem_ttl_sec": "VMON_MESH_IDEM_TTL_SEC",
    "mesh_create_timeout_sec": "VMON_MESH_CREATE_TIMEOUT_SEC",
    "mesh_w_warm": "VMON_MESH_W_WARM",
    "mesh_w_free": "VMON_MESH_W_FREE",
    "mesh_w_local": "VMON_MESH_W_LOCAL",
    "mesh_w_region": "VMON_MESH_W_REGION",
    "mesh_w_inflight": "VMON_MESH_W_INFLIGHT",
}

CLI_OPTIONS: dict[str, str] = {
    "home": "--home",
    "host": "--host",
    "port": "--port",
    "token": "--token",
    "client_token": "--client-token",
    "tls_cert": "--tls-cert",
    "tls_key": "--tls-key",
    "idle_timeout": "--idle-timeout",
    "replicate_sec": "--replicate-sec",
    "replicas": "--replicas",
    "replicate_concurrency": "--replicate-concurrency",
    "restore_quorum": "--restore-quorum/--no-restore-quorum",
    "warm_pool_size": "--warm-pool-size",
    "warm_images": "--warm-images",
    "mesh_heartbeat_sec": "--mesh-heartbeat-sec",
    "mesh_reap_sec": "--mesh-reap-sec",
    "mesh_idem_ttl_sec": "--mesh-idem-ttl-sec",
    "mesh_create_timeout_sec": "--mesh-create-timeout-sec",
    "mesh_w_warm": "--mesh-w-warm",
    "mesh_w_free": "--mesh-w-free",
    "mesh_w_local": "--mesh-w-local",
    "mesh_w_region": "--mesh-w-region",
    "mesh_w_inflight": "--mesh-w-inflight",
}

_CONFIG_PATH_KEYS = {"config", "config_path"}
_BOOL_TRUE = {"1", "true", "yes", "on"}
_BOOL_FALSE = {"0", "false", "no", "off"}


def resolve_serve_config(cli_overrides: Mapping[str, Any] | None = None) -> ServeConfig:
    """Resolve ``vmon serve`` configuration from defaults, file, env, then flags."""

    overrides = _clean_cli_overrides(cli_overrides or {})
    config_path = _resolve_config_path(overrides)
    file_values = _read_config_file(config_path) if config_path is not None else {}
    env_values = {key: os.environ[env] for key, env in ENV_KEYS.items() if env in os.environ}

    values: dict[str, Any] = {key: getattr(ServeConfig(), key) for key in SERVE_CONFIG_KEYS}
    sources: dict[str, ConfigSource] = dict.fromkeys(SERVE_CONFIG_KEYS, "default")
    _overlay(values, sources, file_values, "file")
    _overlay(values, sources, env_values, "env")
    _overlay(values, sources, overrides, "flag")

    config = ServeConfig(**values)
    object.__setattr__(config, "_sources", sources)
    return config


def config_source(config: ServeConfig, key: str) -> ConfigSource:
    """Convenience wrapper for callers that render source metadata."""

    return config.source(key)


def format_config_value(value: object) -> str:
    """Return a stable human-readable representation for doctor/docs tests."""

    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple) and all(isinstance(item, WarmImage) for item in value):
        if not value:
            return "[]"
        return ",".join(f"{item.ref}={item.count}" for item in value)
    if value is None:
        return "auto" if value is None else ""
    return str(value)


def _clean_cli_overrides(overrides: Mapping[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    unknown = set(overrides) - set(SERVE_CONFIG_KEYS) - _CONFIG_PATH_KEYS
    if unknown:
        raise ConfigError(f"unknown CLI override key(s): {', '.join(sorted(unknown))}")
    for key, value in overrides.items():
        if key in _CONFIG_PATH_KEYS:
            if value not in (None, ""):
                cleaned[key] = value
            continue
        if value is None:
            continue
        if key == "warm_images" and value in ((), [], ""):
            continue
        cleaned[key] = value
    return cleaned


def _resolve_config_path(overrides: Mapping[str, Any]) -> Path | None:
    raw = overrides.get("config", overrides.get("config_path"))
    if raw is None:
        raw = os.environ.get("VMON_CONFIG")
    if raw is None or str(raw).strip() == "":
        return None
    return Path(str(raw)).expanduser()


def _read_config_file(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except OSError as exc:
        raise ConfigError(f"could not read config file {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"config file {path} must contain a TOML table")
    if "serve" in data:
        extra = set(data) - {"serve"}
        if extra:
            raise ConfigError(
                f"unknown top-level config key(s): {', '.join(sorted(str(key) for key in extra))}"
            )
        table = data["serve"]
        if not isinstance(table, dict):
            raise ConfigError("config key 'serve' must be a TOML table")
        data = table
    unknown = set(data) - set(SERVE_CONFIG_KEYS)
    if unknown:
        keys = ", ".join(sorted(str(key) for key in unknown))
        raise ConfigError(f"unknown serve config key(s): {keys}")
    return dict(data)


def _overlay(
    values: dict[str, Any],
    sources: dict[str, ConfigSource],
    incoming: Mapping[str, Any],
    source: ConfigSource,
) -> None:
    for key in SERVE_CONFIG_KEYS:
        if key not in incoming:
            continue
        values[key] = _coerce_value(key, incoming[key], values)
        sources[key] = source


def _coerce_value(key: str, value: Any, values: Mapping[str, Any]) -> Any:
    try:
        if key == "home":
            return Path(_coerce_string(key, value)).expanduser()
        if key in {"token", "client_token", "tls_cert", "tls_key"}:
            text = _coerce_string(key, value).strip()
            return text or None
        if key == "host":
            host = _coerce_string(key, value).strip()
            if not host:
                raise ConfigError("host must not be empty")
            return host
        if key == "port":
            port = _coerce_int(key, value)
            if port < 0 or port > 65535:
                raise ConfigError("port must be between 0 and 65535")
            return port
        if key in {"replicas", "warm_pool_size"}:
            parsed = _coerce_int(key, value)
            if parsed < 0:
                raise ConfigError(f"{key} must be non-negative")
            return parsed
        if key == "replicate_concurrency":
            parsed = _coerce_int(key, value)
            if parsed <= 0:
                raise ConfigError("replicate_concurrency must be positive")
            return parsed
        if key == "restore_quorum":
            return _coerce_bool(key, value)
        if key == "warm_images":
            return _coerce_warm_images(value, int(values.get("warm_pool_size") or 1))
        if key in {
            "idle_timeout",
            "replicate_sec",
            "mesh_heartbeat_sec",
            "mesh_reap_sec",
            "mesh_idem_ttl_sec",
            "mesh_create_timeout_sec",
            "mesh_w_warm",
            "mesh_w_free",
            "mesh_w_local",
            "mesh_w_region",
            "mesh_w_inflight",
        }:
            parsed_float = _coerce_float(key, value)
            if key == "replicate_sec" and parsed_float == 0:
                return 0.0
            if parsed_float <= 0:
                raise ConfigError(f"{key} must be positive")
            return parsed_float
    except ConfigError:
        raise
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key} has invalid value {value!r}") from exc
    raise ConfigError(f"unsupported serve config key {key!r}")


def _coerce_string(key: str, value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    if not isinstance(value, str):
        raise ConfigError(f"{key} must be a string")
    return value


def _coerce_int(key: str, value: Any) -> int:
    if isinstance(value, bool):
        raise ConfigError(f"{key} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text:
            return int(text)
    raise ConfigError(f"{key} must be an integer")


def _coerce_float(key: str, value: Any) -> float:
    if isinstance(value, bool):
        raise ConfigError(f"{key} must be a number")
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if text:
            return float(text)
    raise ConfigError(f"{key} must be a number")


def _coerce_bool(key: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _BOOL_TRUE:
            return True
        if text in _BOOL_FALSE:
            return False
    raise ConfigError(f"{key} must be a boolean")


def _coerce_warm_images(value: Any, default_count: int) -> tuple[WarmImage, ...]:
    if default_count < 0:
        raise ConfigError("warm_pool_size must be non-negative")
    if value is None:
        return ()
    if isinstance(value, str):
        entries = list(value.split(","))
    elif isinstance(value, list | tuple):
        entries = list(value)
    else:
        raise ConfigError("warm_images must be a string or list")
    parsed: list[WarmImage] = []
    for entry in entries:
        if not isinstance(entry, str):
            raise ConfigError("warm_images entries must be strings")
        item = entry.strip()
        if not item:
            continue
        ref = item
        count = default_count
        if "=" in item:
            ref, raw_count = item.rsplit("=", 1)
            count = _coerce_int("warm_images count", raw_count)
        ref = ref.strip()
        if not ref:
            raise ConfigError("warm_images entries must include an image reference")
        if count < 0:
            raise ConfigError("warm_images counts must be non-negative integers")
        parsed.append(WarmImage(ref, count))
    return tuple(parsed)


def env_var_for(key: str) -> str | None:
    """Return the environment variable name for a config field, if it has one."""

    return ENV_KEYS.get(key)


def cli_option_for(key: str) -> str | None:
    """Return the CLI option spelling for a config field, if it has one."""

    return CLI_OPTIONS.get(key)


def validate_knob_inventory() -> None:
    """Internal sanity check used by tests to keep metadata aligned with fields."""

    fields_set = set(SERVE_CONFIG_KEYS)
    missing_env = fields_set - set(ENV_KEYS)
    missing_cli = fields_set - set(CLI_OPTIONS)
    if missing_env or missing_cli:
        raise ConfigError(
            "incomplete serve config metadata: "
            f"missing env={sorted(missing_env)}, cli={sorted(missing_cli)}"
        )
    source_values = set(get_args(ConfigSource))
    if source_values != {"default", "file", "env", "flag"}:
        raise ConfigError("unexpected config source literals")
