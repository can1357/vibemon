//! Bounded protocol-v2 client for the embedded Python guest runner.

use std::{
	collections::HashMap,
	fmt,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::sync::oneshot;

use crate::engine::ExecStream;

/// Runner protocol version supported by this daemon.
pub const PROTOCOL_VERSION: u64 = 2;
/// Maximum accepted NDJSON frame size.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Default number of event frames retained per active request.
pub const DEFAULT_EVENT_CAPACITY: usize = 64;

/// A validated protocol frame emitted by the runner.
#[derive(Clone, Debug)]
pub struct Frame(pub Value);

impl Frame {
	/// Return the event discriminator.
	pub fn event(&self) -> Option<&str> {
		self
			.0
			.get("event")
			.or_else(|| self.0.get("type"))
			.and_then(Value::as_str)
	}

	/// Return the request identifier, if this is a request-scoped frame.
	pub fn request_id(&self) -> Option<&str> {
		self.0.get("request_id").and_then(Value::as_str)
	}

	/// Whether this frame finishes a request.
	pub fn is_terminal(&self) -> bool {
		matches!(self.event(), Some("result" | "batch_result" | "error" | "cancelled" | "status"))
	}
}

/// Failure of the transport or runner protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
	/// The runner did not complete an operation before its deadline.
	Timeout,
	/// The peer does not implement protocol v2.
	Version { received: Option<u64> },
	/// A malformed or oversized frame was received.
	InvalidFrame(String),
	/// A bounded request consumer did not keep up with the runner.
	Backpressure(String),
	/// The runner stream closed or an engine operation failed.
	Disconnected(String),
}

impl fmt::Display for ProtocolError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Timeout => f.write_str("runner protocol deadline exceeded"),
			Self::Version { received } => {
				write!(f, "runner protocol version mismatch: expected 2, received {received:?}")
			},
			Self::InvalidFrame(message)
			| Self::Backpressure(message)
			| Self::Disconnected(message) => f.write_str(message),
		}
	}
}

impl std::error::Error for ProtocolError {}

struct Pending {
	events:   flume::Sender<Frame>,
	terminal: oneshot::Sender<Result<Frame, ProtocolError>>,
}

struct Inner {
	control:        Mutex<Box<dyn crate::engine::ExecControl>>,
	pending:        Mutex<HashMap<String, Pending>>,
	hello:          Mutex<Option<oneshot::Sender<Result<Frame, ProtocolError>>>>,
	closed:         AtomicBool,
	event_capacity: usize,
}

/// One in-flight request. Events are bounded and completion is independent of
/// event consumption so a terminal result can never be stolen by a watcher.
pub struct ProtocolRequest {
	/// Ordered, request-scoped log/yield/status/result frames.
	pub events: flume::Receiver<Frame>,
	completion: oneshot::Receiver<Result<Frame, ProtocolError>>,
}

impl ProtocolRequest {
	/// Wait for the terminal frame, enforcing a host-side deadline.
	pub async fn complete(self, timeout: Duration) -> Result<Frame, ProtocolError> {
		match tokio::time::timeout(timeout, self.completion).await {
			Ok(Ok(result)) => result,
			Ok(Err(_)) => Err(ProtocolError::Disconnected("runner completion channel closed".into())),
			Err(_) => Err(ProtocolError::Timeout),
		}
	}
}

/// Concurrent NDJSON session over an engine exec stream.
#[derive(Clone)]
pub struct ProtocolSession {
	inner: Arc<Inner>,
}

impl ProtocolSession {
	/// Attach to a newly started or reconnected runner and validate its hello.
	pub async fn connect(
		stream: ExecStream,
		timeout: Duration,
		event_capacity: usize,
	) -> Result<Self, ProtocolError> {
		let (hello_tx, hello_rx) = oneshot::channel();
		let inner = Arc::new(Inner {
			control:        Mutex::new(stream.control),
			pending:        Mutex::new(HashMap::new()),
			hello:          Mutex::new(Some(hello_tx)),
			closed:         AtomicBool::new(false),
			event_capacity: event_capacity.max(1),
		});
		Self::spawn_reader(Arc::clone(&inner), stream.stdout, stream.stderr, stream.exit);
		let hello = match tokio::time::timeout(timeout, hello_rx).await {
			Ok(Ok(result)) => result?,
			Ok(Err(_)) => {
				return Err(ProtocolError::Disconnected("runner closed before hello".into()));
			},
			Err(_) => return Err(ProtocolError::Timeout),
		};
		let received = hello.0.get("version").and_then(Value::as_u64);
		if received != Some(PROTOCOL_VERSION) {
			inner.closed.store(true, Ordering::Release);
			return Err(ProtocolError::Version { received });
		}
		Ok(Self { inner })
	}

	fn spawn_reader(
		inner: Arc<Inner>,
		stdout: flume::Receiver<Vec<u8>>,
		stderr: flume::Receiver<Vec<u8>>,
		exit: flume::Receiver<crate::engine::ExecExit>,
	) {
		let diagnostics = stderr;
		std::thread::Builder::new()
			.name("vmon-runner-stderr".into())
			.spawn(move || while diagnostics.recv().is_ok() {})
			.expect("spawn runner stderr drain");
		let exit_inner = Arc::clone(&inner);
		std::thread::Builder::new()
			.name("vmon-runner-exit".into())
			.spawn(move || {
				let message = exit.recv().map_or_else(
					|_| "runner exited".to_string(),
					|status| format!("runner exited with status {}", status.code),
				);
				fail_all(&exit_inner, ProtocolError::Disconnected(message));
			})
			.expect("spawn runner exit watcher");
		std::thread::Builder::new()
			.name("vmon-runner-protocol".into())
			.spawn(move || {
				let mut buffered = Vec::new();
				while let Ok(chunk) = stdout.recv() {
					buffered.extend_from_slice(&chunk);
					if buffered.len() > MAX_FRAME_BYTES && !buffered.contains(&b'\n') {
						fail_all(
							&inner,
							ProtocolError::InvalidFrame("runner frame exceeds one MiB".into()),
						);
						return;
					}
					while let Some(newline) = buffered.iter().position(|byte| *byte == b'\n') {
						let line: Vec<u8> = buffered.drain(..=newline).collect();
						if let Err(error) = dispatch(&inner, &line[..line.len() - 1]) {
							fail_all(&inner, error);
							return;
						}
					}
				}
				fail_all(&inner, ProtocolError::Disconnected("runner stdout closed".into()));
			})
			.expect("spawn runner protocol reader");
	}

	/// Submit one request. Duplicate IDs are rejected and each event buffer is
	/// bounded.
	pub fn request(&self, mut frame: Value) -> Result<ProtocolRequest, ProtocolError> {
		if self.inner.closed.load(Ordering::Acquire) {
			return Err(ProtocolError::Disconnected("runner session is closed".into()));
		}
		let request_id = frame
			.get("request_id")
			.and_then(Value::as_str)
			.ok_or_else(|| ProtocolError::InvalidFrame("request_id is required".into()))?
			.to_owned();
		frame["protocol"] = json!(PROTOCOL_VERSION);
		let (events_tx, events) = flume::bounded(self.inner.event_capacity);
		let (terminal, completion) = oneshot::channel();
		{
			let mut pending = self.inner.pending.lock();
			if pending.contains_key(&request_id) {
				return Err(ProtocolError::InvalidFrame(format!("duplicate request_id {request_id}")));
			}
			pending.insert(request_id.clone(), Pending { events: events_tx, terminal });
		}
		let mut encoded = match serde_json::to_vec(&frame) {
			Ok(encoded) if encoded.len() <= MAX_FRAME_BYTES => encoded,
			Ok(_) => {
				self.inner.pending.lock().remove(&request_id);
				return Err(ProtocolError::InvalidFrame("request frame exceeds one MiB".into()));
			},
			Err(error) => {
				self.inner.pending.lock().remove(&request_id);
				return Err(ProtocolError::InvalidFrame(error.to_string()));
			},
		};
		encoded.push(b'\n');
		if let Err(error) = self.inner.control.lock().write_stdin(&encoded) {
			self.inner.pending.lock().remove(&request_id);
			return Err(ProtocolError::Disconnected(error.to_string()));
		}
		Ok(ProtocolRequest { events, completion })
	}

	/// Cooperatively cancel one asynchronous request without affecting peers.
	pub fn cancel(&self, request_id: &str) -> Result<(), ProtocolError> {
		let cancel_id = format!("cancel:{request_id}");
		let frame = json!({"protocol": PROTOCOL_VERSION, "type":"cancel", "request_id":cancel_id, "target_request_id":request_id});
		let mut encoded =
			serde_json::to_vec(&frame).map_err(|e| ProtocolError::InvalidFrame(e.to_string()))?;
		encoded.push(b'\n');
		self
			.inner
			.control
			.lock()
			.write_stdin(&encoded)
			.map_err(|e| ProtocolError::Disconnected(e.to_string()))
	}

	/// Request runner shutdown and close stdin.
	pub fn shutdown(&self) -> Result<(), ProtocolError> {
		self.inner.closed.store(true, Ordering::Release);
		let mut control = self.inner.control.lock();
		control
			.write_stdin(b"{\"protocol\":2,\"type\":\"shutdown\",\"request_id\":\"shutdown\"}\n")
			.map_err(|e| ProtocolError::Disconnected(e.to_string()))?;
		control
			.close_stdin()
			.map_err(|e| ProtocolError::Disconnected(e.to_string()))
	}

	/// Kill the transport process. Required when a synchronous Python call
	/// cannot be interrupted; every concurrent request then fails as
	/// infrastructure loss.
	pub fn kill(&self) -> Result<(), ProtocolError> {
		self.inner.closed.store(true, Ordering::Release);
		self
			.inner
			.control
			.lock()
			.kill(9)
			.map_err(|e| ProtocolError::Disconnected(e.to_string()))
	}
}

fn dispatch(inner: &Arc<Inner>, bytes: &[u8]) -> Result<(), ProtocolError> {
	if bytes.is_empty() {
		return Ok(());
	}
	let value: Value =
		serde_json::from_slice(bytes).map_err(|e| ProtocolError::InvalidFrame(e.to_string()))?;
	if value.get("protocol").and_then(Value::as_u64) != Some(PROTOCOL_VERSION) {
		return Err(ProtocolError::Version {
			received: value.get("protocol").and_then(Value::as_u64),
		});
	}
	let frame = Frame(value);
	if frame.event() == Some("hello") {
		if let Some(sender) = inner.hello.lock().take() {
			let _ = sender.send(Ok(frame));
		}
		return Ok(());
	}
	let Some(request_id) = frame.request_id().map(str::to_owned) else {
		return Ok(());
	};
	let terminal = frame.is_terminal();
	let mut pending = inner.pending.lock();
	let Some(entry) = pending.get(&request_id) else {
		return Ok(());
	};
	if terminal {
		let entry = pending.remove(&request_id).expect("pending entry existed");
		let _ = entry.terminal.send(Ok(frame));
	} else {
		entry.events.try_send(frame).map_err(|_| {
			ProtocolError::Backpressure(format!("event buffer full for request {request_id}"))
		})?;
	}
	Ok(())
}

fn fail_all(inner: &Arc<Inner>, error: ProtocolError) {
	if inner.closed.swap(true, Ordering::AcqRel) {
		return;
	}
	if let Some(sender) = inner.hello.lock().take() {
		let _ = sender.send(Err(error.clone()));
	}
	for (_, pending) in inner.pending.lock().drain() {
		let _ = pending.terminal.send(Err(error.clone()));
	}
}

#[cfg(test)]
#[path = "protocol_test.rs"]
mod tests;
