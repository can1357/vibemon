//! Client-served TCP gateway for a sandbox's private TAP address.

use std::{
	collections::{HashMap, HashSet},
	net::{IpAddr, SocketAddr},
	sync::Arc,
};

use parking_lot::Mutex;
use tokio::{
	io::{AsyncReadExt as _, AsyncWriteExt as _},
	net::{TcpListener, TcpStream},
	sync::mpsc,
	task::{JoinHandle, JoinSet},
};

use crate::{EngineError, Result};

/// Bytes and lifecycle commands sent from the attached client to the listener.
#[derive(Debug)]
pub enum HostGatewayCommand {
	Data { conn: u64, data: Vec<u8> },
	Close { conn: u64 },
}

/// Accepted connections and bytes sent from the listener to the attached
/// client.
#[derive(Debug)]
pub enum HostGatewayEvent {
	Open { conn: u64 },
	Data { conn: u64, data: Vec<u8> },
	Close { conn: u64 },
}

/// One active gateway claim per sandbox.
#[derive(Debug, Default)]
pub struct HostGatewayClaims {
	attached: Arc<Mutex<HashSet<String>>>,
}

impl HostGatewayClaims {
	pub fn claim(&self, sandbox_id: &str) -> Result<HostGatewayClaim> {
		let mut attached = self.attached.lock();
		if !attached.insert(sandbox_id.to_owned()) {
			return Err(EngineError::busy(format!(
				"a host gateway client is already attached to sandbox '{sandbox_id}'"
			)));
		}
		Ok(HostGatewayClaim {
			sandbox_id: sandbox_id.to_owned(),
			attached:   Arc::clone(&self.attached),
		})
	}
}

#[derive(Debug)]
pub struct HostGatewayClaim {
	sandbox_id: String,
	attached:   Arc<Mutex<HashSet<String>>>,
}

impl Drop for HostGatewayClaim {
	fn drop(&mut self) {
		self.attached.lock().remove(&self.sandbox_id);
	}
}

/// Live private listener and its client-facing relay channels.
pub struct HostGateway {
	endpoint: String,
	commands: mpsc::Sender<HostGatewayCommand>,
	events:   Option<mpsc::Receiver<HostGatewayEvent>>,
	task:     JoinHandle<()>,
	_claim:   HostGatewayClaim,
}

impl std::fmt::Debug for HostGateway {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("HostGateway")
			.field("endpoint", &self.endpoint)
			.finish_non_exhaustive()
	}
}

impl HostGateway {
	/// Bind the sandbox's fixed TAP gateway port and start relaying connections.
	pub fn start(
		runtime: &tokio::runtime::Runtime,
		bind_ip: IpAddr,
		guest_ip: IpAddr,
		bind_port: u16,
		claim: HostGatewayClaim,
	) -> Result<Self> {
		let listener = runtime.block_on(TcpListener::bind(SocketAddr::new(bind_ip, bind_port)))?;
		let port = listener.local_addr()?.port();
		let endpoint = format!("http://{guest_ip}:{port}");
		let (command_tx, command_rx) = mpsc::channel(64);
		let (event_tx, event_rx) = mpsc::channel(64);
		let task = runtime.spawn(run_mux(listener, command_rx, event_tx));
		Ok(Self { endpoint, commands: command_tx, events: Some(event_rx), task, _claim: claim })
	}

	/// Guest-visible listener URL.
	pub fn endpoint(&self) -> &str {
		&self.endpoint
	}

	/// Sender for bytes and close commands from the attached client.
	pub fn commands(&self) -> mpsc::Sender<HostGatewayCommand> {
		self.commands.clone()
	}

	/// Receiver for accepted connections and guest bytes.
	pub const fn take_events(&mut self) -> mpsc::Receiver<HostGatewayEvent> {
		self
			.events
			.take()
			.expect("host gateway events may only be taken once")
	}
}

impl Drop for HostGateway {
	fn drop(&mut self) {
		self.task.abort();
	}
}

enum ConnectionCommand {
	Data(Vec<u8>),
	Close,
}

async fn run_mux(
	listener: TcpListener,
	mut commands: mpsc::Receiver<HostGatewayCommand>,
	events: mpsc::Sender<HostGatewayEvent>,
) {
	let mut next_conn = 1_u64;
	let mut connections = HashMap::<u64, mpsc::Sender<ConnectionCommand>>::new();
	let mut tasks = JoinSet::new();
	loop {
		tokio::select! {
			accepted = listener.accept() => {
				let Ok((stream, _)) = accepted else {
					break;
				};
				let conn = next_conn;
				next_conn = next_conn.wrapping_add(1).max(1);
				if events.send(HostGatewayEvent::Open { conn }).await.is_err() {
					break;
				}
				let (tx, rx) = mpsc::channel(32);
				connections.insert(conn, tx);
				let event_tx = events.clone();
				tasks.spawn(async move {
					run_connection(conn, stream, rx, event_tx).await;
					conn
				});
			},
			command = commands.recv() => {
				let Some(command) = command else {
					break;
				};
				match command {
					HostGatewayCommand::Data { conn, data } => {
						if let Some(connection) = connections.get(&conn) {
							let _ = connection.send(ConnectionCommand::Data(data)).await;
						}
					},
					HostGatewayCommand::Close { conn } => {
						if let Some(connection) = connections.remove(&conn) {
							let _ = connection.send(ConnectionCommand::Close).await;
						}
					},
				}
			},
			completed = tasks.join_next(), if !tasks.is_empty() => {
				if let Some(Ok(conn)) = completed {
					connections.remove(&conn);
				}
			},
		}
	}
	tasks.abort_all();
}

async fn run_connection(
	conn: u64,
	mut stream: TcpStream,
	mut commands: mpsc::Receiver<ConnectionCommand>,
	events: mpsc::Sender<HostGatewayEvent>,
) {
	let mut buffer = vec![0_u8; 64 * 1024];
	loop {
		tokio::select! {
			read = stream.read(&mut buffer) => match read {
				Ok(0) | Err(_) => break,
				Ok(count) => {
					if events
						.send(HostGatewayEvent::Data {
							conn,
							data: buffer[..count].to_vec(),
						})
						.await
						.is_err()
					{
						break;
					}
				},
			},
			command = commands.recv() => match command {
				Some(ConnectionCommand::Data(data)) => {
					if stream.write_all(&data).await.is_err() {
						break;
					}
				},
				Some(ConnectionCommand::Close) | None => break,
			},
		}
	}
	let _ = events.send(HostGatewayEvent::Close { conn }).await;
}

#[cfg(test)]
mod tests {
	use std::net::{IpAddr, Ipv4Addr};

	use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

	use super::*;
	use crate::ErrorCode;

	#[test]
	fn claim_is_exclusive_and_drop_releases_it() {
		let claims = HostGatewayClaims::default();
		let claim = claims.claim("sandbox").expect("first claim");
		let error = claims.claim("sandbox").expect_err("second claim must fail");
		assert_eq!(error.code, ErrorCode::Busy);
		drop(claim);
		claims.claim("sandbox").expect("claim after detach");
	}

	#[test]
	fn mux_relays_bytes_in_both_directions() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let claims = HostGatewayClaims::default();
		let mut gateway = HostGateway::start(
			&runtime,
			IpAddr::V4(Ipv4Addr::LOCALHOST),
			IpAddr::V4(Ipv4Addr::LOCALHOST),
			0,
			claims.claim("sandbox").expect("claim"),
		)
		.expect("gateway");
		let address = gateway
			.endpoint()
			.strip_prefix("http://")
			.expect("HTTP endpoint")
			.to_owned();
		let mut events = gateway.take_events();
		let commands = gateway.commands();
		runtime.block_on(async move {
			let mut guest = TcpStream::connect(address).await.expect("guest connection");
			let HostGatewayEvent::Open { conn } = events.recv().await.expect("open") else {
				panic!("expected open")
			};
			guest.write_all(b"from guest").await.expect("guest write");
			let HostGatewayEvent::Data { conn: data_conn, data } =
				events.recv().await.expect("guest data")
			else {
				panic!("expected data")
			};
			assert_eq!(data_conn, conn);
			assert_eq!(data, b"from guest");
			commands
				.send(HostGatewayCommand::Data { conn, data: b"to guest".to_vec() })
				.await
				.expect("client data");
			let mut reply = [0_u8; 8];
			guest.read_exact(&mut reply).await.expect("guest read");
			assert_eq!(&reply, b"to guest");
		});
	}
}
