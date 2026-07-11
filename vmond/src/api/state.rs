use std::{
	io,
	os::fd::AsRawFd,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use axum::{
	body::Body,
	extract::State,
	http::{Method, Request, header},
	middleware::Next,
	response::Response,
	serve::Listener,
};
use tokio::net::{UnixListener, UnixStream};

use super::error::ApiError;
use crate::{config::ServeConfig, engine::EngineApi, mesh::runtime::MeshRuntime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transport {
	Tcp,
	Unix,
}

#[derive(Clone)]
pub struct ApiState {
	pub engine:        Arc<dyn EngineApi>,
	pub config:        Arc<ServeConfig>,
	pub auth_failures: Arc<AtomicU64>,
	pub transport:     Transport,
	pub mesh:          Option<Arc<MeshRuntime>>,
}

impl ApiState {
	pub fn new(engine: Arc<dyn EngineApi>, config: ServeConfig, transport: Transport) -> Self {
		Self {
			engine,
			config: Arc::new(config),
			auth_failures: Arc::new(AtomicU64::new(0)),
			transport,
			mesh: None,
		}
	}

	pub fn with_transport(&self, transport: Transport) -> Self {
		Self { transport, ..self.clone() }
	}

	pub fn with_mesh(&self, mesh: Arc<MeshRuntime>) -> Self {
		Self { mesh: Some(mesh), ..self.clone() }
	}

	pub fn count_auth_failure(&self) {
		self.auth_failures.fetch_add(1, Ordering::Relaxed);
	}

	pub fn auth_failure_count(&self) -> u64 {
		self.auth_failures.load(Ordering::Relaxed)
	}
}

#[derive(Clone, Debug)]
pub struct UdsPeerInfo;

pub struct UdsPeerListener {
	inner:      UnixListener,
	server_uid: u32,
}

impl UdsPeerListener {
	pub const fn new(inner: UnixListener, server_uid: u32) -> Self {
		Self { inner, server_uid }
	}
}

impl Listener for UdsPeerListener {
	type Addr = UdsPeerInfo;
	type Io = UnixStream;

	async fn accept(&mut self) -> (Self::Io, Self::Addr) {
		loop {
			match self.inner.accept().await {
				Ok((stream, _addr)) => {
					let uid = peer_uid(&stream).ok();
					let allowed = uid.is_some_and(|uid| uid == 0 || uid == self.server_uid);
					if allowed {
						return (stream, UdsPeerInfo);
					}
					tracing::warn!(uid = ?uid, "rejecting unauthorized UDS peer");
				},
				Err(err) => {
					tracing::error!("UDS accept error: {err}");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	fn local_addr(&self) -> io::Result<Self::Addr> {
		Ok(UdsPeerInfo)
	}
}

pub async fn auth_middleware(
	State(state): State<ApiState>,
	request: Request<Body>,
	next: Next,
) -> Result<Response, ApiError> {
	if is_public_path(request.method(), request.uri().path()) {
		return Ok(next.run(request).await);
	}
	match state.transport {
		Transport::Unix => Ok(next.run(request).await),
		Transport::Tcp => authorize_tcp(&state, request, next).await,
	}
}

async fn authorize_tcp(
	state: &ApiState,
	request: Request<Body>,
	next: Next,
) -> Result<Response, ApiError> {
	let supplied = request_bearer_token(&request);
	let admin = token_matches_any(supplied.as_deref(), state.config.token.as_deref());
	let client = token_matches_any(supplied.as_deref(), state.config.client_token.as_deref());
	let tokens_configured = token_configured(state.config.token.as_deref())
		|| token_configured(state.config.client_token.as_deref());
	if tokens_configured && !(admin || client) {
		state.count_auth_failure();
		return Err(ApiError::unauthorized("unauthorized"));
	}
	if client && !admin && is_admin_path(request.uri().path()) {
		return Err(ApiError::forbidden("forbidden"));
	}
	Ok(next.run(request).await)
}

pub fn is_public_path(method: &Method, path: &str) -> bool {
	*method == Method::GET && (path == "/healthz" || is_static_web_path(path))
}

fn is_static_web_path(path: &str) -> bool {
	!(path.starts_with("/v1")
		|| path.starts_with("/healthz")
		|| path.starts_with("/metrics")
		|| path.starts_with("/vmon.v1.")
		|| path == "/grpc")
}

pub fn is_admin_path(path: &str) -> bool {
	if path.starts_with("/v1/mesh/") {
		return true;
	}
	// Client credentials may invoke functions and observe/cancel calls, but
	// deployment, secret-bearing registration, artifact upload, and durable
	// schedule mutations require an administrator credential.
	if let Some(rest) = path.strip_prefix("/vmon.v1.") {
		return matches!(
			rest,
			"VolumeService/Create"
				| "VolumeService/Delete"
				| "PoolService/Set"
				| "PoolService/Delete"
				| "SandboxService/Migrate"
				| "ArtifactService/Put"
				| "FunctionService/Register"
				| "FunctionService/Activate"
				| "FunctionService/Delete"
				| "FunctionService/ActivateApp"
				| "FunctionService/RollbackApp"
				| "FunctionService/CreateSchedule"
				| "FunctionService/DeleteSchedule"
		);
	}
	false
}

/// Whether a `/grpc` bridge connection is limited to non-admin RPCs: a TCP
/// connection authenticated with the client token only. UDS peer-cred
/// connections and admin bearers get the full surface.
pub(super) fn bridge_restricted(
	state: &ApiState,
	headers: &axum::http::HeaderMap,
	query: Option<&str>,
) -> bool {
	if state.transport == Transport::Unix {
		return false;
	}
	let supplied = bearer_token(headers.get(header::AUTHORIZATION)).or_else(|| query_token(query));
	let admin = token_matches_any(supplied.as_deref(), state.config.token.as_deref());
	let client = token_matches_any(supplied.as_deref(), state.config.client_token.as_deref());
	client && !admin
}

pub fn bearer_token(header: Option<&axum::http::HeaderValue>) -> Option<String> {
	let header = header?.to_str().ok()?;
	let (scheme, value) = header.split_once(' ')?;
	if scheme.eq_ignore_ascii_case("bearer") && !value.trim().is_empty() {
		Some(value.trim().to_owned())
	} else {
		None
	}
}

fn request_bearer_token(request: &Request<Body>) -> Option<String> {
	bearer_token(request.headers().get(header::AUTHORIZATION))
		.or_else(|| websocket_query_token(request))
}

fn websocket_query_token(request: &Request<Body>) -> Option<String> {
	if request.method() != Method::GET {
		return None;
	}
	if !is_query_token_websocket_path(request.uri().path()) {
		return None;
	}
	if !request
		.headers()
		.get(header::UPGRADE)
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
	{
		return None;
	}
	query_token(request.uri().query())
}

fn is_query_token_websocket_path(path: &str) -> bool {
	if path == "/grpc" {
		return true;
	}
	let segments = path.trim_start_matches('/').split('/').collect::<Vec<_>>();
	matches!(
		segments.as_slice(),
		["v1", "sandboxes", id, "ports", port, "ws"]
		| ["v1", "sandboxes", id, "ports", port, "ws", ..]
			if !id.is_empty() && !port.is_empty()
	)
}

fn query_token(query: Option<&str>) -> Option<String> {
	for pair in query?.split('&') {
		let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
		if matches!(key, "token" | "access_token") && !value.is_empty() {
			return Some(percent_decode_query(value));
		}
	}
	None
}

fn percent_decode_query(value: &str) -> String {
	let bytes = value.as_bytes();
	let mut out = Vec::with_capacity(bytes.len());
	let mut index = 0;
	while index < bytes.len() {
		match bytes[index] {
			b'%' if index + 2 < bytes.len() => {
				if let Some(decoded) = hex_pair(bytes[index + 1], bytes[index + 2]) {
					out.push(decoded);
					index += 3;
				} else {
					out.push(bytes[index]);
					index += 1;
				}
			},
			b'+' => {
				out.push(b' ');
				index += 1;
			},
			byte => {
				out.push(byte);
				index += 1;
			},
		}
	}
	String::from_utf8_lossy(&out).into_owned()
}

fn hex_pair(high: u8, low: u8) -> Option<u8> {
	Some(hex_value(high)? << 4 | hex_value(low)?)
}

const fn hex_value(byte: u8) -> Option<u8> {
	match byte {
		b'0'..=b'9' => Some(byte - b'0'),
		b'a'..=b'f' => Some(byte - b'a' + 10),
		b'A'..=b'F' => Some(byte - b'A' + 10),
		_ => None,
	}
}

pub fn token_matches_any(supplied: Option<&str>, expected: Option<&str>) -> bool {
	let Some(supplied) = supplied else {
		return false;
	};
	expected
		.unwrap_or_default()
		.split(',')
		.map(str::trim)
		.filter(|token| !token.is_empty())
		.any(|token| token == supplied)
}

fn token_configured(expected: Option<&str>) -> bool {
	expected
		.unwrap_or_default()
		.split(',')
		.map(str::trim)
		.any(|token| !token.is_empty())
}

#[cfg(test)]
#[allow(
	clippy::items_after_test_module,
	reason = "cfg-specific peer credential helpers stay outside the tests module"
)]
mod tests {
	use super::*;

	fn request(method: Method, uri: &str, upgrade: bool) -> Request<Body> {
		let mut builder = Request::builder().method(method).uri(uri);
		if upgrade {
			builder = builder.header(header::UPGRADE, "websocket");
		}
		builder.body(Body::empty()).expect("test request builds")
	}

	#[test]
	fn query_token_auth_is_limited_to_get_websocket_routes() {
		let bridge = request(Method::GET, "/grpc?token=a%20b", true);
		assert_eq!(request_bearer_token(&bridge), Some("a b".to_owned()));

		let bridge_access = request(Method::GET, "/grpc?access_token=tok", true);
		assert_eq!(request_bearer_token(&bridge_access), Some("tok".to_owned()));

		let port_ws = request(Method::GET, "/v1/sandboxes/sb/ports/8080/ws/path?token=tok", true);
		assert_eq!(request_bearer_token(&port_ws), Some("tok".to_owned()));

		let post_bridge = request(Method::POST, "/grpc?token=tok", true);
		assert_eq!(request_bearer_token(&post_bridge), None);

		let upgraded_rpc = request(Method::GET, "/vmon.v1.SystemService/Info?token=tok", true);
		assert_eq!(request_bearer_token(&upgraded_rpc), None);

		let non_upgrade_bridge = request(Method::GET, "/grpc?token=tok", false);
		assert_eq!(request_bearer_token(&non_upgrade_bridge), None);
	}

	#[test]
	fn function_admin_policy_preserves_client_invocation_access() {
		for path in [
			"/vmon.v1.ArtifactService/Put",
			"/vmon.v1.FunctionService/Register",
			"/vmon.v1.FunctionService/Activate",
			"/vmon.v1.FunctionService/Delete",
			"/vmon.v1.FunctionService/ActivateApp",
			"/vmon.v1.FunctionService/RollbackApp",
			"/vmon.v1.FunctionService/CreateSchedule",
			"/vmon.v1.FunctionService/DeleteSchedule",
		] {
			assert!(is_admin_path(path), "{path}");
		}
		for path in [
			"/vmon.v1.ArtifactService/Get",
			"/vmon.v1.ArtifactService/Stat",
			"/vmon.v1.FunctionService/Get",
			"/vmon.v1.FunctionService/List",
			"/vmon.v1.FunctionService/GetApp",
			"/vmon.v1.FunctionService/GetSchedule",
			"/vmon.v1.FunctionService/ListSchedules",
			"/vmon.v1.CallService/Create",
			"/vmon.v1.CallService/Get",
			"/vmon.v1.CallService/Watch",
			"/vmon.v1.CallService/Cancel",
			"/vmon.v1.ActorService/Create",
			"/vmon.v1.ActorService/Checkpoint",
			"/vmon.v1.ActorService/Restore",
			"/vmon.v1.ActorService/Fork",
			"/vmon.v1.ActorService/Delete",
		] {
			assert!(!is_admin_path(path), "{path}");
		}
	}
}

#[cfg(target_os = "linux")]
fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
	let mut cred = libc::ucred { pid: 0, uid: 0, gid: 0 };
	let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
	// SAFETY: all pointers reference initialized stack storage valid for the call;
	// `stream.as_raw_fd()` is an open socket descriptor.
	let rc = unsafe {
		libc::getsockopt(
			stream.as_raw_fd(),
			libc::SOL_SOCKET,
			libc::SO_PEERCRED,
			std::ptr::addr_of_mut!(cred).cast(),
			std::ptr::addr_of_mut!(len),
		)
	};
	if rc == 0 {
		Ok(cred.uid)
	} else {
		Err(io::Error::last_os_error())
	}
}

#[cfg(target_os = "macos")]
fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
	let mut euid: libc::uid_t = 0;
	let mut egid: libc::gid_t = 0;
	// SAFETY: `euid` and `egid` are valid out-pointers and `stream.as_raw_fd()` is
	// an open socket descriptor.
	let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut euid, &mut egid) };
	if rc == 0 {
		Ok(euid)
	} else {
		Err(io::Error::last_os_error())
	}
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn peer_uid(_stream: &UnixStream) -> io::Result<u32> {
	Err(io::Error::new(
		io::ErrorKind::Unsupported,
		"peer credentials are unsupported on this platform",
	))
}
