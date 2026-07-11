use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use serde_json::{Value, json};

use super::{ProtocolError, ProtocolRequirements, ProtocolSession};
use crate::{
	Result,
	engine::{ExecControl, ExecExit, ExecStream},
};

struct Control {
	writes: flume::Sender<Vec<u8>>,
	killed: Arc<AtomicBool>,
}

impl ExecControl for Control {
	fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
		self
			.writes
			.send(data.to_vec())
			.map_err(|e| crate::EngineError::engine(e.to_string()))
	}

	fn close_stdin(&mut self) -> Result<()> {
		Ok(())
	}

	fn resize(&mut self, _: u16, _: u16) -> Result<()> {
		Ok(())
	}

	fn kill(&mut self, _: i32) -> Result<()> {
		self.killed.store(true, Ordering::Release);
		Ok(())
	}
}

struct Peer {
	stdout: flume::Sender<Vec<u8>>,
	writes: flume::Receiver<Vec<u8>>,
	_exit:  flume::Sender<ExecExit>,
	killed: Arc<AtomicBool>,
}

fn stream() -> (ExecStream, Peer) {
	let (stdout_tx, stdout) = flume::unbounded();
	let (_stderr_tx, stderr) = flume::unbounded();
	let (exit_tx, exit) = flume::unbounded();
	let (writes_tx, writes) = flume::unbounded();
	let killed = Arc::new(AtomicBool::new(false));
	let stream = ExecStream {
		control: Box::new(Control { writes: writes_tx, killed: Arc::clone(&killed) }),
		stdout,
		stderr,
		exit,
	};
	(stream, Peer { stdout: stdout_tx, writes, _exit: exit_tx, killed })
}

fn line(value: Value) -> Vec<u8> {
	let mut bytes = serde_json::to_vec(&value).unwrap();
	bytes.push(b'\n');
	bytes
}

async fn connected(capacity: usize) -> (ProtocolSession, Peer) {
	let (stream, peer) = stream();
	peer
		.stdout
		.send(line(json!({"protocol":2,"type":"hello","version":2,"envelope_version":1,"formats":["json"],"compressions":["none","gzip"]})))
		.unwrap();
	let session = ProtocolSession::connect(
		stream,
		Duration::from_secs(1),
		capacity,
		ProtocolRequirements::default(),
	)
	.await
	.unwrap();
	(session, peer)
}

#[tokio::test]
async fn rejects_wrong_runner_version() {
	let (stream, peer) = stream();
	peer
		.stdout
		.send(line(json!({"protocol":2,"type":"hello","version":3,"envelope_version":1,"formats":["json"],"compressions":["none","gzip"]})))
		.unwrap();
	let error =
		ProtocolSession::connect(stream, Duration::from_secs(1), 2, ProtocolRequirements::default())
			.await
			.err()
			.unwrap();
	assert_eq!(error, ProtocolError::Version { received: Some(3) });
}

async fn negotiate(
	hello: Value,
	requirements: ProtocolRequirements,
) -> Result<ProtocolSession, ProtocolError> {
	let (stream, peer) = stream();
	peer.stdout.send(line(hello)).unwrap();
	ProtocolSession::connect(stream, Duration::from_secs(1), 2, requirements).await
}

fn hello(formats: Value, compressions: Value) -> Value {
	json!({
		"protocol": 2,
		"type": "hello",
		"version": 2,
		"envelope_version": 1,
		"formats": formats,
		"compressions": compressions,
	})
}

#[tokio::test]
async fn rejects_wrong_envelope_version() {
	let mut value = hello(json!(["json"]), json!(["none", "gzip"]));
	value["envelope_version"] = json!(2);
	assert_eq!(
		negotiate(value, ProtocolRequirements::default())
			.await
			.err()
			.unwrap(),
		ProtocolError::EnvelopeVersion { received: Some(2) }
	);
}

#[tokio::test]
async fn rejects_malformed_capability_lists() {
	for (field, formats, compressions) in [
		("formats", json!("json"), json!(["none", "gzip"])),
		("formats", json!(["json", 1]), json!(["none", "gzip"])),
		("compressions", json!(["json"]), json!({"none": true})),
		("compressions", json!(["json"]), json!(["none", false])),
	] {
		assert_eq!(
			negotiate(hello(formats, compressions), ProtocolRequirements::default())
				.await
				.err()
				.unwrap(),
			ProtocolError::InvalidCapabilities { field }
		);
	}
}

#[tokio::test]
async fn rejects_missing_policy_codecs() {
	for (field, capability, requirements) in [
		("formats", "cbor", ProtocolRequirements::default().require_format("cbor")),
		("formats", "cloudpickle", ProtocolRequirements::default().require_format("cloudpickle")),
		("compressions", "zstd", ProtocolRequirements::default().require_compression("zstd")),
	] {
		assert_eq!(
			negotiate(hello(json!(["json"]), json!(["none", "gzip"])), requirements)
				.await
				.err()
				.unwrap(),
			ProtocolError::MissingCapability { field, capability: capability.into() }
		);
	}
}

#[tokio::test]
async fn accepts_matching_policy_capabilities() {
	let requirements = ProtocolRequirements::default()
		.require_format("cbor")
		.require_format("cloudpickle")
		.require_compression("zstd");
	negotiate(
		hello(json!(["json", "cbor", "cloudpickle"]), json!(["none", "gzip", "zstd"])),
		requirements,
	)
	.await
	.unwrap();
}

#[tokio::test]
async fn routes_concurrent_events_and_terminal_frames() {
	let (session, peer) = connected(4).await;
	let first = session
		.request(json!({"type":"call","request_id":"a"}))
		.unwrap();
	let second = session
		.request(json!({"type":"call","request_id":"b"}))
		.unwrap();
	assert_eq!(
		serde_json::from_slice::<Value>(&peer.writes.recv().unwrap()).unwrap()["protocol"],
		2
	);
	let _ = peer.writes.recv().unwrap();
	peer
		.stdout
		.send(
			[
				line(json!({"protocol":2,"type":"log","request_id":"b","message":"b"})),
				line(json!({"protocol":2,"type":"yield","request_id":"a","value":{}})),
				line(json!({"protocol":2,"type":"result","request_id":"b","value":{}})),
				line(json!({"protocol":2,"type":"result","request_id":"a","value":{}})),
			]
			.concat(),
		)
		.unwrap();
	assert_eq!(
		second
			.events
			.recv_timeout(Duration::from_secs(1))
			.unwrap()
			.event(),
		Some("log")
	);
	assert_eq!(
		first
			.events
			.recv_timeout(Duration::from_secs(1))
			.unwrap()
			.event(),
		Some("yield")
	);
	assert_eq!(
		second
			.complete(Duration::from_secs(1))
			.await
			.unwrap()
			.request_id(),
		Some("b")
	);
	assert_eq!(
		first
			.complete(Duration::from_secs(1))
			.await
			.unwrap()
			.request_id(),
		Some("a")
	);
}

#[tokio::test]
async fn overflow_fails_session_instead_of_growing_unbounded() {
	let (session, peer) = connected(1).await;
	let request = session
		.request(json!({"type":"call","request_id":"slow"}))
		.unwrap();
	let _ = peer.writes.recv().unwrap();
	peer
		.stdout
		.send(
			[
				line(json!({"protocol":2,"type":"log","request_id":"slow"})),
				line(json!({"protocol":2,"type":"log","request_id":"slow"})),
			]
			.concat(),
		)
		.unwrap();
	let error = request.complete(Duration::from_secs(1)).await.unwrap_err();
	assert!(matches!(error, ProtocolError::Backpressure(_)));
}

#[tokio::test]
async fn cancellation_targets_one_request_and_kill_is_explicit() {
	let (session, peer) = connected(2).await;
	session.cancel("active").unwrap();
	let frame: Value = serde_json::from_slice(&peer.writes.recv().unwrap()).unwrap();
	assert_eq!(frame["type"], "cancel");
	assert_eq!(frame["target_request_id"], "active");
	assert!(!peer.killed.load(Ordering::Acquire));
	session.kill().unwrap();
	assert!(peer.killed.load(Ordering::Acquire));
}
