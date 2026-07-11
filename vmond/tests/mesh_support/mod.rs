#![allow(
	dead_code,
	reason = "shared harness consumed by Phase 4 mesh behavior suites as production APIs land"
)]

use std::{collections::VecDeque, net::SocketAddr, sync::Arc, time::Duration};

use axum::Router;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	sync::oneshot,
	task::JoinHandle,
};

#[derive(Debug)]
pub struct InProcNode {
	name:     String,
	token:    String,
	addr:     SocketAddr,
	shutdown: Option<oneshot::Sender<()>>,
	handle:   JoinHandle<()>,
	client:   reqwest::Client,
}

impl InProcNode {
	pub async fn spawn(name: impl Into<String>, token: impl Into<String>, router: Router) -> Self {
		let name = name.into();
		let token = token.into();
		let listener = TcpListener::bind(("127.0.0.1", 0))
			.await
			.expect("bind in-proc axum listener");
		let addr = listener.local_addr().expect("in-proc listener local addr");
		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let handle = tokio::spawn(async move {
			axum::serve(listener, router)
				.with_graceful_shutdown(async move {
					let _ = shutdown_rx.await;
				})
				.await
				.expect("in-proc axum server failed");
		});
		Self {
			name,
			token,
			addr,
			shutdown: Some(shutdown_tx),
			handle,
			client: reqwest::Client::new(),
		}
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn token(&self) -> &str {
		&self.token
	}

	pub fn url(&self) -> String {
		format!("http://{}", self.addr)
	}

	pub const fn addr(&self) -> SocketAddr {
		self.addr
	}

	pub async fn get_json(&self, path: &str) -> reqwest::Response {
		self
			.client
			.get(format!("{}{}", self.url(), path))
			.bearer_auth(&self.token)
			.header("X-Vmon-Mesh-Hop", "1")
			.send()
			.await
			.expect("GET request to in-proc node")
	}

	pub async fn post_json(&self, path: &str, payload: &Value) -> reqwest::Response {
		self
			.client
			.post(format!("{}{}", self.url(), path))
			.bearer_auth(&self.token)
			.header("X-Vmon-Mesh-Hop", "1")
			.json(payload)
			.send()
			.await
			.expect("POST request to in-proc node")
	}

	pub async fn stop(mut self) {
		if let Some(shutdown) = self.shutdown.take() {
			let _ = shutdown.send(());
		}
		let _ = self.handle.await;
	}
}

#[derive(Clone, Debug)]
pub struct FaultProxy {
	listen:   SocketAddr,
	state:    Arc<Mutex<FaultState>>,
	shutdown: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[derive(Debug)]
struct FaultState {
	target:           SocketAddr,
	blocked:          bool,
	blocked_sources:  Vec<String>,
	drops:            VecDeque<DropRule>,
	latency:          Duration,
	latency_prefixes: Option<Vec<String>>,
}

#[derive(Debug)]
struct DropRule {
	remaining: usize,
	prefixes:  Option<Vec<String>>,
}

impl FaultProxy {
	pub async fn start(target: SocketAddr) -> Self {
		let listener = TcpListener::bind(("127.0.0.1", 0))
			.await
			.expect("bind fault proxy listener");
		let listen = listener.local_addr().expect("fault proxy local addr");
		let state = Arc::new(Mutex::new(FaultState {
			target,
			blocked: false,
			blocked_sources: Vec::new(),
			drops: VecDeque::new(),
			latency: Duration::ZERO,
			latency_prefixes: None,
		}));
		let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
		let server_state = Arc::clone(&state);
		tokio::spawn(async move {
			loop {
				tokio::select! {
					 biased;
					 _ = &mut shutdown_rx => break,
					 accepted = listener.accept() => {
						  let Ok((stream, _peer)) = accepted else { break };
						  let state = Arc::clone(&server_state);
						  tokio::spawn(async move {
								let _ = handle_proxy_stream(stream, state).await;
						  });
					 }
				}
			}
		});
		Self { listen, state, shutdown: Arc::new(Mutex::new(Some(shutdown_tx))) }
	}

	pub fn url(&self) -> String {
		format!("http://{}", self.listen)
	}

	pub fn partition(&self, enabled: bool) {
		self.state.lock().blocked = enabled;
	}

	pub fn drop_next(&self, count: usize, prefixes: Option<Vec<&str>>) {
		self.state.lock().drops.push_back(DropRule {
			remaining: count,
			prefixes:  prefixes.map(|items| items.into_iter().map(str::to_owned).collect()),
		});
	}

	pub fn set_latency(&self, latency: Duration, prefixes: Option<Vec<&str>>) {
		let mut state = self.state.lock();
		state.latency = latency;
		state.latency_prefixes = prefixes.map(|items| items.into_iter().map(str::to_owned).collect());
	}

	pub fn block_source(&self, node_id: &str, enabled: bool) {
		let mut state = self.state.lock();
		if enabled {
			if !state.blocked_sources.iter().any(|item| item == node_id) {
				state.blocked_sources.push(node_id.to_owned());
			}
		} else {
			state.blocked_sources.retain(|item| item != node_id);
		}
	}

	pub fn reset(&self) {
		let mut state = self.state.lock();
		state.blocked = false;
		state.blocked_sources.clear();
		state.drops.clear();
		state.latency = Duration::ZERO;
		state.latency_prefixes = None;
	}

	pub fn close(&self) {
		let shutdown = self.shutdown.lock().take();
		if let Some(shutdown) = shutdown {
			let _ = shutdown.send(());
		}
	}
}

async fn handle_proxy_stream(
	mut client: TcpStream,
	state: Arc<Mutex<FaultState>>,
) -> std::io::Result<()> {
	let first = read_initial_request(&mut client).await?;
	if first.is_empty() {
		return Ok(());
	}
	let path = request_path(&first);
	let body = request_body(&first);
	let (target, action, latency) = {
		let mut state = state.lock();
		let action = state.action_for(&path, body);
		let latency = if matches_prefix(&path, state.latency_prefixes.as_deref()) {
			state.latency
		} else {
			Duration::ZERO
		};
		(state.target, action, latency)
	};
	match action {
		FaultAction::Drop => return Ok(()),
		FaultAction::Forward => {},
	}
	if !latency.is_zero() {
		tokio::time::sleep(latency).await;
	}
	let mut backend = TcpStream::connect(target).await?;
	backend.write_all(&first).await?;
	let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
	Ok(())
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum FaultAction {
	Forward,
	Drop,
}

impl FaultState {
	fn action_for(&mut self, path: &str, body: &[u8]) -> FaultAction {
		if self.blocked {
			return FaultAction::Drop;
		}
		if let Some(source) = request_source(path, body)
			&& self
				.blocked_sources
				.iter()
				.any(|item| item == "*" || item == &source)
		{
			return FaultAction::Drop;
		}
		let mut remove_index = None;
		for (index, rule) in self.drops.iter_mut().enumerate() {
			if matches_prefix(path, rule.prefixes.as_deref()) {
				rule.remaining = rule.remaining.saturating_sub(1);
				if rule.remaining == 0 {
					remove_index = Some(index);
				}
				if let Some(index) = remove_index {
					self.drops.remove(index);
				}
				return FaultAction::Drop;
			}
		}
		FaultAction::Forward
	}
}

async fn read_initial_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
	let mut data = Vec::new();
	let mut buf = [0_u8; 8192];
	let mut header_end = None;
	let mut content_length = 0_usize;
	while data.len() < 1_048_576 {
		let n = stream.read(&mut buf).await?;
		if n == 0 {
			break;
		}
		data.extend_from_slice(&buf[..n]);
		if header_end.is_none()
			&& let Some(pos) = find_bytes(&data, b"\r\n\r\n")
		{
			header_end = Some(pos);
			content_length = parse_content_length(&data[..pos]);
		}
		if let Some(pos) = header_end
			&& data.len() >= pos + 4 + content_length
		{
			break;
		}
	}
	Ok(data)
}

fn parse_content_length(header: &[u8]) -> usize {
	let Ok(text) = std::str::from_utf8(header) else {
		return 0;
	};
	for line in text.split("\r\n") {
		let Some((name, value)) = line.split_once(':') else {
			continue;
		};
		if name.eq_ignore_ascii_case("content-length")
			&& let Ok(len) = value.trim().parse()
		{
			return len;
		}
	}
	0
}

fn request_path(data: &[u8]) -> String {
	let first = data.split(|byte| *byte == b'\n').next().unwrap_or_default();
	let Ok(line) = std::str::from_utf8(first) else {
		return String::new();
	};
	line
		.split_whitespace()
		.nth(1)
		.unwrap_or_default()
		.split('?')
		.next()
		.unwrap_or_default()
		.to_owned()
}

fn request_body(data: &[u8]) -> &[u8] {
	let Some(pos) = find_bytes(data, b"\r\n\r\n") else {
		return &[];
	};
	&data[pos + 4..]
}

fn request_source(path: &str, body: &[u8]) -> Option<String> {
	if !path.starts_with("/v1/mesh/") {
		return None;
	}
	let value: Value = serde_json::from_slice(body).ok()?;
	let object = value.as_object()?;
	if let Some(node_id) = object
		.get("state")
		.and_then(Value::as_object)
		.and_then(|state| state.get("node_id"))
		.and_then(Value::as_str)
	{
		return Some(node_id.to_owned());
	}
	for field in ["holder_node", "source_node", "node_id", "owner"] {
		if let Some(value) = object.get(field).and_then(Value::as_str)
			&& !value.is_empty()
		{
			return Some(value.to_owned());
		}
	}
	None
}

fn matches_prefix(path: &str, prefixes: Option<&[String]>) -> bool {
	prefixes.is_none_or(|items| items.iter().any(|prefix| path.starts_with(prefix)))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

pub fn auth_header(token: &str) -> (&'static str, String) {
	("Authorization", format!("Bearer {token}"))
}
