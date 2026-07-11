//! Typed v1 API request models — the OpenAPI-first contract.
//!
//! Field sets mirror `python/vmon/server/models.py` (§2.3/§2.4 of the
//! migration plan); sandbox VIEWS stay open JSON objects because
//! `record.view()` spreads a free-form detail map under canonical keys.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// `POST /v1/sandboxes` body: today's `SandboxCreate` field set plus
/// `command` (foreground entrypoint override composed by the CLI).
/// External `remote_page_*` fields are rejected by validation.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SandboxCreate {
	pub image:                  Option<String>,
	pub template:               Option<String>,
	pub dockerfile:             Option<String>,
	#[serde(default = "default_context")]
	pub context:                String,
	pub name:                   Option<String>,
	#[serde(default = "default_cpus")]
	pub cpus:                   u32,
	#[serde(default = "default_memory")]
	pub memory:                 u32,
	#[serde(default = "default_disk_mb")]
	pub disk_mb:                u32,
	#[serde(default = "default_timeout")]
	pub timeout:                Option<f64>,
	pub timeout_secs:           Option<u64>,
	pub workdir:                Option<String>,
	pub env:                    Option<HashMap<String, String>>,
	/// Request-scoped secrets: `[{name, values}]`; never persisted.
	pub secrets:                Option<Vec<Value>>,
	/// Mountpoint -> volume name or `{name, read_only}`.
	pub volumes:                Option<HashMap<String, Value>>,
	pub tags:                   Option<HashMap<String, String>>,
	pub fs_dir:                 Option<String>,
	#[serde(default)]
	pub block_network:          bool,
	pub ports:                  Option<Vec<u16>>,
	pub egress_allow:           Option<Vec<String>>,
	pub egress_allow_domains:   Option<Vec<String>>,
	pub inbound_cidr_allowlist: Option<Vec<String>>,
	pub readiness_probe:        Option<Value>,
	#[serde(default)]
	pub pool_size:              u32,
	/// Rejected for external callers (mesh-internal restore plumbing).
	pub remote_page_url:        Option<String>,
	pub remote_page_token:      Option<String>,
	pub remote_page_digest:     Option<String>,
	pub ha:                     Option<String>,
	pub arch:                   Option<String>,
	pub idempotency_key:        Option<String>,
	/// Foreground entrypoint override (`command?: [str]`, new in v1).
	pub command:                Option<Vec<String>>,
}

fn default_context() -> String {
	".".to_string()
}
const fn default_cpus() -> u32 {
	1
}
const fn default_memory() -> u32 {
	512
}
const fn default_disk_mb() -> u32 {
	1024
}
#[allow(clippy::unnecessary_wraps, reason = "serde default fns must return the field type")]
const fn default_timeout() -> Option<f64> {
	Some(300.0)
}

/// `POST /{id}/exec` body and the first WS exec frame (§2.4).
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct ExecBody {
	#[serde(default)]
	pub cmd:     Vec<String>,
	pub workdir: Option<String>,
	/// Legacy alias for `workdir` accepted on input.
	pub cwd:     Option<String>,
	pub env:     Option<HashMap<String, String>>,
	pub timeout: Option<f64>,
	#[serde(default)]
	pub tty:     bool,
}

/// `POST /{id}/snapshots` body.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct SnapshotBody {
	pub name: Option<String>,
	#[serde(default)]
	pub stop: bool,
}

/// `POST /{id}/snapshots/fs` body.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct SnapshotFsBody {
	pub name: Option<String>,
}

/// `POST /{id}/extend` body.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ExtendBody {
	pub secs: u64,
}

/// Optional body for `POST /{id}/stop`, allowing foreground CLI runs to
/// preserve the process exit code they just observed.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct StopBody {
	pub returncode: Option<i64>,
}

/// `GET|PUT /{id}/network` body (same field names as today).
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct NetworkBody {
	pub block_network: Option<bool>,
	pub cidr_allow:    Option<Vec<String>>,
	pub domain_allow:  Option<Vec<String>>,
}

/// `POST /v1/snapshots/{name}/restore` body.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct RestoreBody {
	pub name:  Option<String>,
	/// Wait for the guest agent before returning.
	pub agent: Option<bool>,
	/// Additional create-style overrides (network, tags, …).
	#[serde(flatten)]
	pub extra: HashMap<String, Value>,
}

/// `POST /v1/snapshots/{name}/fork` body.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ForkBody {
	pub count: u32,
	#[serde(flatten)]
	pub extra: HashMap<String, Value>,
}

/// `PUT /v1/pools/{ref}` body (= today's `prewarm`).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct PoolPutBody {
	pub size:  u32,
	/// Template kwargs forwarded to the image pipeline.
	#[serde(flatten)]
	pub extra: HashMap<String, Value>,
}

/// `POST /{id}/migrate` body (admin; mesh).
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct MigrateBody {
	pub target: String,
}
