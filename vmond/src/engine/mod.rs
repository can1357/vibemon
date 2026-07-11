//! Engine: single owner of the sandbox registry and all VM lifecycle logic.
//!
//! [`EngineApi`] is the seam the v1 API layer talks through (and API tests
//! fake); [`Engine`] is the real implementation, a method-for-method port of
//! `python/vmon/core.py` on top of [`spawn::MicroVm`], [`agent::AgentConn`],
//! the image pipeline, and host networking. The engine is synchronous; the
//! async API layer reaches it via `tokio::task::spawn_blocking`.

pub mod agent;
pub mod control;
mod facade;
pub mod spawn;

use std::collections::HashMap;

pub use facade::Engine;
use serde_json::Value;

use crate::{
	error::Result,
	models::{ForkBody, NetworkBody, PoolPutBody, RestoreBody, SandboxCreate},
};

/// A non-interactive or streaming exec request (shared by `POST /exec`, the
/// exec WebSocket, and the shell gateway; §2.4 of the migration contract).
#[derive(Clone, Debug, Default)]
pub struct ExecRequest {
	pub cmd:     Vec<String>,
	pub tty:     bool,
	pub env:     Option<HashMap<String, String>>,
	pub workdir: Option<String>,
	/// Seconds; `POST /exec` callers are additionally capped server-side (60s).
	pub timeout: Option<f64>,
}

/// Captured result of a non-interactive exec.
#[derive(Clone, Debug)]
pub struct ExecCapture {
	pub exit:   i64,
	pub stdout: Vec<u8>,
	pub stderr: Vec<u8>,
}

/// Exit of a streamed exec: `{exit, signal}` per the WS frame protocol.
#[derive(Clone, Copy, Debug)]
pub struct ExecExit {
	pub code:   i64,
	pub signal: Option<i64>,
}

/// Control half of a live exec stream: stdin, resize, kill.
pub trait ExecControl: Send {
	fn write_stdin(&mut self, data: &[u8]) -> Result<()>;
	fn close_stdin(&mut self) -> Result<()>;
	fn resize(&mut self, rows: u16, cols: u16) -> Result<()>;
	fn kill(&mut self, signal: i32) -> Result<()>;
}

/// A live streaming exec: channel receivers work from both sync and async
/// (flume) consumers. Streams close on EOF; `exit` yields exactly one value.
pub struct ExecStream {
	pub control: Box<dyn ExecControl>,
	pub stdout:  flume::Receiver<Vec<u8>>,
	pub stderr:  flume::Receiver<Vec<u8>>,
	pub exit:    flume::Receiver<ExecExit>,
}

/// A started shell session (`WS /v1/shell`): the resolved sandbox name, the
/// interactive stream, and whether the sandbox is ephemeral (cleaned up on
/// disconnect via [`EngineApi::shell_cleanup`]).
pub struct ShellSession {
	pub name:      String,
	pub stream:    ExecStream,
	pub ephemeral: bool,
}

/// The engine surface the v1 API layer consumes. Sync and blocking: the API
/// layer wraps calls in `spawn_blocking`. Views are `serde_json::Value`
/// objects with the canonical `record.view()` keys.
pub trait EngineApi: Send + Sync + 'static {
	// -- sandboxes --------------------------------------------------------
	fn create(&self, params: SandboxCreate) -> Result<Value>;
	fn list(&self, tags: Option<HashMap<String, String>>) -> Result<Vec<Value>>;
	fn get(&self, id: &str) -> Result<Value>;
	fn stop(&self, id: &str) -> Result<Value>;
	/// Stop a sandbox while preserving a caller-owned process return code.
	fn stop_with_returncode(&self, id: &str, returncode: Option<i64>) -> Result<Value> {
		let _ = returncode;
		self.stop(id)
	}
	/// Stop + delete state (today's `/remove`; `DELETE /v1/sandboxes/{id}`).
	fn remove(&self, id: &str) -> Result<Value>;
	fn terminate(&self, id: &str, reason: &str) -> Result<Value>;
	fn pause(&self, id: &str) -> Result<Value>;
	fn resume(&self, id: &str) -> Result<Value>;
	/// Returns `{"deadline_unix": …}`.
	fn extend(&self, id: &str, secs: u64) -> Result<Value>;
	fn metrics(&self, id: &str) -> Result<Value>;

	// -- console / logs ---------------------------------------------------
	/// Full console log contents (non-follow `GET /logs`).
	fn logs(&self, id: &str) -> Result<Vec<u8>>;
	/// Follow the console log: receiver of appended chunks; closes when the
	/// VM reaches a terminal state. Also backs `WS /{id}/attach`.
	fn logs_follow(&self, id: &str) -> Result<flume::Receiver<Vec<u8>>>;

	// -- exec / shell -----------------------------------------------------
	/// Non-interactive exec with captured output (server-capped timeout).
	fn exec_capture(&self, id: &str, req: ExecRequest) -> Result<ExecCapture>;
	/// Streaming exec for the WS protocol (§2.4).
	fn exec_stream(&self, id: &str, req: ExecRequest) -> Result<ExecStream>;
	/// Boot-or-attach an interactive shell (`WS /v1/shell` semantics; params
	/// are the loose query/first-frame shell parameters).
	fn shell_start(&self, params: Value) -> Result<ShellSession>;
	/// Tear down an ephemeral shell sandbox after its WS disconnects.
	fn shell_cleanup(&self, name: &str);

	// -- guest filesystem -------------------------------------------------
	fn file_read(&self, id: &str, path: &str) -> Result<Vec<u8>>;
	fn file_write(&self, id: &str, path: &str, data: &[u8]) -> Result<()>;
	fn file_delete(&self, id: &str, path: &str, recursive: bool) -> Result<()>;
	fn file_list(&self, id: &str, path: &str) -> Result<Value>;
	fn file_stat(&self, id: &str, path: &str) -> Result<Value>;

	// -- networking -------------------------------------------------------
	fn network_get(&self, id: &str) -> Result<Value>;
	fn network_set(&self, id: &str, policy: NetworkBody) -> Result<Value>;
	/// Returns `{"tunnels": {...}, "connect_token": "…"}`.
	fn tunnels(&self, id: &str) -> Result<Value>;
	/// Host target `(ip, port)` for the tunnel HTTP/WS proxy.
	fn tunnel_target(&self, id: &str, port: u16) -> Result<(String, u16)>;

	// -- snapshots --------------------------------------------------------
	/// Machine snapshot -> `{"snapshot": name, "dir": path}`.
	fn snapshot(&self, id: &str, name: Option<String>, stop: bool) -> Result<Value>;
	/// Filesystem snapshot -> `{"image": name}`.
	fn snapshot_fs(&self, id: &str, name: Option<String>) -> Result<Value>;
	fn snapshots(&self) -> Result<Vec<String>>;
	/// Restore a named snapshot into a fresh sandbox -> view (201).
	fn restore(&self, snapshot: &str, body: RestoreBody) -> Result<Value>;
	/// Fork `CoW` clones -> `{"clones": [{name, pid, reconstruct_ms}]}`.
	fn fork(&self, snapshot: &str, body: ForkBody) -> Result<Value>;

	// -- volumes / pools (server-owned) ------------------------------------
	fn volume_list(&self) -> Result<Vec<String>>;
	fn volume_create(&self, name: &str) -> Result<()>;
	fn volume_delete(&self, name: &str) -> Result<()>;
	/// `{ref: {ready, hits, misses, size}}`.
	fn pool_list(&self) -> Result<Value>;
	fn pool_set(&self, reference: &str, body: PoolPutBody) -> Result<Value>;
	fn pool_delete(&self, reference: &str) -> Result<()>;

	// -- system -----------------------------------------------------------
	/// `{version, platform, arch, backend, capabilities}`.
	fn info(&self) -> Result<Value>;
	/// Subscribe to lifecycle events for the SSE stream (`{event, ts, …}`).
	fn subscribe_events(&self) -> flume::Receiver<Value>;
	/// Prometheus text exposition for `GET /metrics`.
	fn prometheus_metrics(&self) -> String;
	/// Mesh migrate (Phase 4); `unsupported` until the mesh port lands.
	fn migrate(&self, id: &str, target: &str) -> Result<Value>;
}
