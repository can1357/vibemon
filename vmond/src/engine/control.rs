//! Control-socket client (newline JSON). Port of python/vmon/control.py.

use std::{
	cmp,
	io::{self, BufRead, BufReader, Write},
	os::unix::net::UnixStream,
	path::{Path, PathBuf},
	thread,
	time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::{EngineError, Result};

const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(10);
/// RAM dumps scale with guest size; match the VMM's control reply window
/// (`vmm::control::CONTROL_REPLY_TIMEOUT`) instead of the per-line default.
const SNAPSHOT_REPLY_TIMEOUT: Duration = Duration::from_mins(1);
const READY_SLEEP: Duration = Duration::from_millis(5);
const MIN_CONNECT_SLICE: Duration = Duration::from_millis(1);

/// Synchronous newline-JSON client for a VMM lifecycle control socket.
///
/// The server closes idle connections after roughly five seconds; callers
/// should keep a client only for one short command burst, not as a long-lived
/// handle.
#[allow(
	clippy::module_name_repetitions,
	reason = "Name is the public Python-compatible client type."
)]
pub struct ControlClient {
	path:    PathBuf,
	writer:  UnixStream,
	reader:  BufReader<UnixStream>,
	next_id: u64,
}

impl ControlClient {
	/// Connect to `path`, retrying absent or refused sockets until `timeout`
	/// elapses.
	pub fn connect(path: impl AsRef<Path>, timeout: Duration) -> Result<Self> {
		let path = path.as_ref().to_path_buf();
		let deadline = Instant::now() + timeout;
		loop {
			match UnixStream::connect(&path) {
				Ok(stream) => return Self::from_stream(path, stream, timeout_remaining(deadline)),
				Err(e) if retryable_connect_error(&e) && Instant::now() < deadline => {
					thread::sleep(cmp::min(READY_SLEEP, timeout_remaining(deadline)));
				},
				Err(e) if retryable_connect_error(&e) => {
					return Err(EngineError::engine(format!(
						"control socket {} not ready: {e}",
						path.display()
					)));
				},
				Err(e) => return Err(EngineError::from(e)),
			}
		}
	}

	fn from_stream(path: PathBuf, stream: UnixStream, banner_timeout: Duration) -> Result<Self> {
		let banner_timeout = cmp::max(banner_timeout, MIN_CONNECT_SLICE);
		stream.set_read_timeout(Some(banner_timeout))?;
		stream.set_write_timeout(Some(DEFAULT_IO_TIMEOUT))?;
		let reader_stream = stream.try_clone()?;
		reader_stream.set_read_timeout(Some(banner_timeout))?;
		let mut client =
			Self { path, writer: stream, reader: BufReader::new(reader_stream), next_id: 1 };
		client.read_banner()?;
		client.writer.set_read_timeout(Some(DEFAULT_IO_TIMEOUT))?;
		client.writer.set_write_timeout(Some(DEFAULT_IO_TIMEOUT))?;
		client
			.reader
			.get_mut()
			.set_read_timeout(Some(DEFAULT_IO_TIMEOUT))?;
		client
			.reader
			.get_mut()
			.set_write_timeout(Some(DEFAULT_IO_TIMEOUT))?;
		Ok(client)
	}

	fn read_banner(&mut self) -> Result<()> {
		let line = self.read_line("banner")?;
		let banner: Banner = serde_json::from_str(&line)
			.map_err(|e| EngineError::engine(format!("invalid control banner: {e}")))?;
		if banner.api != 1 {
			return Err(EngineError::engine(format!(
				"unsupported control API banner from {}: api {}",
				self.path.display(),
				banner.api
			)));
		}
		Ok(())
	}

	/// Send one request and return the raw JSON result object or value.
	pub fn request(&mut self, method: &str, params: Value) -> Result<Value> {
		let id = self.next_id;
		self.next_id = self.next_id.saturating_add(1);
		let request = json!({ "id": id, "method": method, "params": params });
		serde_json::to_writer(&mut self.writer, &request)?;
		self.writer.write_all(b"\n")?;
		self.writer.flush()?;

		let line = self.read_line("reply")?;
		let reply: Reply = serde_json::from_str(&line)
			.map_err(|e| EngineError::engine(format!("invalid control reply: {e}")))?;
		if reply.id != Some(id) {
			return Err(EngineError::engine(format!(
				"control reply id mismatch: expected {id}, got {:?}",
				reply.id
			)));
		}
		if !reply.ok {
			let message = reply
				.error
				.as_ref()
				.and_then(|e| e.message.as_deref())
				.unwrap_or("control request failed");
			return Err(EngineError::engine(message));
		}
		Ok(reply.result.unwrap_or_else(|| json!({})))
	}

	fn read_line(&mut self, label: &str) -> Result<String> {
		let mut line = String::new();
		let n = self.reader.read_line(&mut line)?;
		if n == 0 {
			return Err(EngineError::engine(format!("control socket closed before {label}")));
		}
		Ok(line)
	}

	/// Round-trip a `ping` request.
	pub fn ping(&mut self) -> Result<Value> {
		self.request("ping", Value::Null)
	}

	/// Return the live machine information payload.
	pub fn info(&mut self) -> Result<Value> {
		self.request("info", Value::Null)
	}

	/// Pause all vCPUs and device workers.
	pub fn pause(&mut self) -> Result<Value> {
		self.request("pause", Value::Null)
	}

	/// Resume a paused VM.
	pub fn resume(&mut self) -> Result<Value> {
		self.request("resume", Value::Null)
	}

	/// Snapshot guest state into `name`, optionally as a delta against `base`.
	/// `track` arms dirty-page tracking so a later delta against `name` is
	/// O(dirty pages).
	///
	/// The read timeout is raised to the server's reply window for this
	/// request only: the VMM writes the reply after dumping guest RAM.
	pub fn snapshot(&mut self, name: &str, base: Option<&str>, track: bool) -> Result<Value> {
		let mut params = serde_json::Map::new();
		params.insert("name".to_owned(), json!(name));
		if let Some(base) = base {
			params.insert("base".to_owned(), json!(base));
		}
		if track {
			params.insert("track".to_owned(), json!(true));
		}
		self
			.reader
			.get_mut()
			.set_read_timeout(Some(SNAPSHOT_REPLY_TIMEOUT))?;
		let result = self.request("snapshot", Value::Object(params));
		self
			.reader
			.get_mut()
			.set_read_timeout(Some(DEFAULT_IO_TIMEOUT))?;
		result
	}

	/// Capture a disk-only recovery artifact without serializing RAM or device
	/// state. The VMM performs its own short block-worker quiesce.
	pub fn disk_snapshot(&mut self, name: &str) -> Result<Value> {
		self
			.reader
			.get_mut()
			.set_read_timeout(Some(SNAPSHOT_REPLY_TIMEOUT))?;
		let result = self.request("disk_snapshot", json!({ "name": name }));
		self
			.reader
			.get_mut()
			.set_read_timeout(Some(DEFAULT_IO_TIMEOUT))?;
		result
	}

	/// Re-arm the VMM wall-clock deadline and return its Unix timestamp.
	pub fn extend(&mut self, secs: u64) -> Result<i64> {
		let result = self.request("extend", json!({ "secs": secs }))?;
		result
			.get("deadline_unix")
			.and_then(Value::as_i64)
			.ok_or_else(|| EngineError::engine("control extend reply missing deadline_unix"))
	}

	/// Re-arm only the VMM production ownership watchdog and return its Unix
	/// deadline, leaving any user lifetime timeout untouched.
	pub fn rearm_owner_lease(&mut self, secs: u64) -> Result<i64> {
		let result = self.request("rearm_owner_lease", json!({ "secs": secs }))?;
		result
			.get("deadline_unix")
			.and_then(Value::as_i64)
			.ok_or_else(|| {
				EngineError::engine("control rearm_owner_lease reply missing deadline_unix")
			})
	}

	/// Return the live metrics JSON payload.
	pub fn metrics(&mut self) -> Result<Value> {
		self.request("metrics", Value::Null)
	}

	/// Ask the VMM to stop cleanly.
	pub fn quit(&mut self) -> Result<Value> {
		self.request("quit", Value::Null)
	}
}

#[derive(Deserialize)]
struct Banner {
	api: u8,
}

#[derive(Deserialize)]
struct Reply {
	id:     Option<u64>,
	ok:     bool,
	result: Option<Value>,
	error:  Option<ReplyError>,
}

#[derive(Deserialize)]
struct ReplyError {
	message: Option<String>,
}

fn retryable_connect_error(e: &io::Error) -> bool {
	matches!(e.kind(), io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused)
}

fn timeout_remaining(deadline: Instant) -> Duration {
	deadline.saturating_duration_since(Instant::now())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn snapshot_payload_includes_base_only_when_present() {
		let mut params = serde_json::Map::new();
		params.insert("name".to_owned(), json!("t"));
		assert_eq!(Value::Object(params.clone()), snapshot_params("t", None));
		params.insert("base".to_owned(), json!("b"));
		assert_eq!(Value::Object(params), snapshot_params("t", Some("b")));
	}

	fn snapshot_params(name: &str, base: Option<&str>) -> Value {
		let mut params = serde_json::Map::new();
		params.insert("name".to_owned(), json!(name));
		if let Some(base) = base {
			params.insert("base".to_owned(), json!(base));
		}
		Value::Object(params)
	}
}
