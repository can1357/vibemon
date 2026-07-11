//! Owner routing, peer locate, and mesh HTTP/WebSocket forwarding.
//!
//! This is the Rust port of `python/vmon/server/proxy.py`'s mesh-owned
//! sandbox routing layer.  Route wiring supplies implementations of the small
//! traits below; the wire behavior here keeps the Python peer semantics:
//! `Authorization: Bearer ...`, `X-Vmon-Mesh-Hop: 1`, locate scatter to
//! `/v1/mesh/locate/{sid}`, and ambiguous-vs-unreachable error taxonomy.

use std::{collections::HashSet, fmt, time::Duration};

use axum::{
	body::{Body, to_bytes},
	extract::ws::{Message, WebSocket},
	http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, header},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use futures_util::SinkExt;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sha1::{Digest, Sha1};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	time::timeout,
};

const BODY_LIMIT: usize = 64 * 1024 * 1024;
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const PEER_TIMEOUT: Duration = Duration::from_secs(5);
const WS_UPGRADE_TIMEOUT: Duration = Duration::from_secs(10);
const HOP_HEADER: &str = "x-vmon-mesh-hop";
const HOP_VALUE: &str = "1";
const HOP_HEADERS: &[&str] = &["host", "connection", "content-length", "transfer-encoding"];
const RESERVED_SANDBOX_SEGMENTS: &[&str] = &["from_id"];

pub type MeshResult<T> = std::result::Result<T, MeshError>;

/// Mesh-layer error with Python's stable code strings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeshError {
	pub code:    String,
	pub message: String,
}

impl MeshError {
	pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
		Self { code: code.into(), message: message.into() }
	}

	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new("invalid", message)
	}

	pub fn unauthorized(message: impl Into<String>) -> Self {
		Self::new("unauthorized", message)
	}

	pub fn unreachable(message: impl Into<String>) -> Self {
		Self::new("unreachable", message)
	}

	pub fn ambiguous(message: impl Into<String>) -> Self {
		Self::new("ambiguous", message)
	}

	pub fn conflict(message: impl Into<String>) -> Self {
		Self::new("conflict", message)
	}

	pub fn status(&self) -> StatusCode {
		mesh_http_status(&self.code)
	}
}

impl fmt::Display for MeshError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}: {}", self.code, self.message)
	}
}

impl std::error::Error for MeshError {}

impl From<reqwest::Error> for MeshError {
	fn from(err: reqwest::Error) -> Self {
		if err.is_connect() || err.is_timeout() {
			Self::unreachable(err.to_string())
		} else if let Some(status) = err.status() {
			match status {
				StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Self::unauthorized(err.to_string()),
				StatusCode::CONFLICT => Self::conflict(err.to_string()),
				status if status.is_server_error() => Self::ambiguous(err.to_string()),
				_ => Self::invalid(err.to_string()),
			}
		} else {
			Self::ambiguous(err.to_string())
		}
	}
}

impl From<std::io::Error> for MeshError {
	fn from(err: std::io::Error) -> Self {
		Self::unreachable(err.to_string())
	}
}

/// Peer advertised by the membership layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeshPeer {
	pub node_id:   String,
	pub advertise: String,
}

/// Durable owner record fallback used after live locate scatter fails.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerRecord {
	pub owner: String,
	pub epoch: u64,
}

/// Minimal state seam required by owner routing.
pub trait OwnerRouter: Send + Sync {
	fn mesh_enabled(&self) -> bool;
	fn local_node_id(&self) -> &str;
	fn owner_of(&self, sandbox_id: &str) -> Option<String>;
	fn peer_url(&self, node_id: &str) -> Option<String>;
	fn peers(&self) -> Vec<MeshPeer>;
	fn record_owner(&self, sandbox_id: &str, owner: &str, epoch: u64);
}

/// Engine/supervisor presence check; true means this node should serve locally.
pub trait SandboxPresence: Send + Sync {
	fn has_sandbox(&self, sandbox_id: &str) -> bool;
}

/// Optional durable create-record owner fallback.
pub trait RecordOwnerLookup: Send + Sync {
	fn owner_record(&self, sandbox_id: &str) -> Option<OwnerRecord>;
}

/// Result of the mesh owner router for an incoming sandbox route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnerProxyDecision {
	ServeLocal,
	Forward { owner: String, peer_url: String },
}

/// Parsed peer URL authority for the raw WebSocket bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerAuthority {
	pub host: String,
	pub port: u16,
	pub tls:  bool,
}

#[derive(Debug, Deserialize)]
struct LocateResponse {
	owner: Option<String>,
	epoch: Option<u64>,
}

/// Extract the sandbox id from `/v1/sandboxes/{id}/...` and the old
/// `/v1/sandboxes/from_id/{id}/...` compatibility path.
pub fn sandbox_id_in_path(path: &str) -> Option<String> {
	let segments = path.trim_start_matches('/').split('/').collect::<Vec<_>>();
	match segments.as_slice() {
		["v1", "sandboxes", sid, ..]
			if !sid.is_empty() && !RESERVED_SANDBOX_SEGMENTS.contains(sid) =>
		{
			Some((*sid).to_owned())
		},
		["v1", "sandboxes", "from_id", sid, ..] if !sid.is_empty() => Some((*sid).to_owned()),
		_ => None,
	}
}

pub fn is_mesh_hop(headers: &HeaderMap) -> bool {
	headers.contains_key(HOP_HEADER)
}

pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
	let header = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
	let (scheme, value) = header.split_once(' ')?;
	if scheme.eq_ignore_ascii_case("bearer") && !value.trim().is_empty() {
		Some(value.trim().to_owned())
	} else {
		None
	}
}

pub fn bearer_token_authorized(
	supplied: Option<&str>,
	expected_token: Option<&str>,
	client_token: Option<&str>,
) -> bool {
	token_matches_any(supplied, expected_token) || token_matches_any(supplied, client_token)
}

pub fn client_token_only(
	supplied: Option<&str>,
	expected_token: Option<&str>,
	client_token: Option<&str>,
) -> bool {
	token_matches_any(supplied, client_token) && !token_matches_any(supplied, expected_token)
}

pub fn require_bearer(
	headers: &HeaderMap,
	expected_token: Option<&str>,
	client_token: Option<&str>,
) -> MeshResult<()> {
	let supplied = bearer_token(headers);
	if bearer_token_authorized(supplied.as_deref(), expected_token, client_token) {
		Ok(())
	} else {
		Err(MeshError::unauthorized("unauthorized"))
	}
}

/// Python's mesh error-code to HTTP status map.
pub fn mesh_http_status(code: &str) -> StatusCode {
	match code {
		"unauthorized" => StatusCode::UNAUTHORIZED,
		"unreachable" => StatusCode::BAD_GATEWAY,
		"ambiguous" => StatusCode::GATEWAY_TIMEOUT,
		"conflict" => StatusCode::CONFLICT,
		"record_unreplicated" => StatusCode::SERVICE_UNAVAILABLE,
		"unplaceable" => StatusCode::CONFLICT,
		"arch_required" => StatusCode::UNPROCESSABLE_ENTITY,
		_ => StatusCode::BAD_REQUEST,
	}
}

pub fn coded_response(status: StatusCode, code: &str, message: &str) -> Response<Body> {
	let payload = json!({"code": code, "message": message}).to_string();
	Response::builder()
		.status(status)
		.header(header::CONTENT_TYPE, "application/json")
		.body(Body::from(payload))
		.unwrap_or_else(|_| Response::new(Body::empty()))
}

pub fn mesh_error_response(err: &MeshError) -> Response<Body> {
	coded_response(err.status(), &err.code, &err.message)
}

/// Locate the current owner by querying live peers first, then falling back to
/// durable create records.  Successful peer answers are recorded in local mesh
/// state with their epoch.
pub async fn scatter_locate<M, R>(
	mesh: &M,
	record_store: Option<&R>,
	client: &Client,
	sandbox_id: &str,
	outbound_token: &str,
) -> Option<String>
where
	M: OwnerRouter,
	R: RecordOwnerLookup + ?Sized,
{
	for peer in mesh.peers() {
		if peer.advertise.is_empty() {
			continue;
		}
		let url = format!("{}/v1/mesh/locate/{}", peer.advertise.trim_end_matches('/'), sandbox_id);
		let response = client
			.get(url)
			.bearer_auth(outbound_token)
			.header("X-Vmon-Mesh-Hop", HOP_VALUE)
			.timeout(PEER_TIMEOUT)
			.send()
			.await;
		let Ok(response) = response else {
			continue;
		};
		if !response.status().is_success() {
			continue;
		}
		let Ok(payload) = response.json::<LocateResponse>().await else {
			continue;
		};
		let Some(owner) = payload.owner.filter(|owner| !owner.is_empty()) else {
			continue;
		};
		mesh.record_owner(sandbox_id, &owner, payload.epoch.unwrap_or(0));
		return Some(owner);
	}
	if let Some(record) = record_store.and_then(|store| store.owner_record(sandbox_id))
		&& !record.owner.is_empty()
	{
		mesh.record_owner(sandbox_id, &record.owner, record.epoch);
		return Some(record.owner);
	}
	None
}

/// Decide whether a sandbox request should be served locally or forwarded to a
/// remote owner.
///
/// This mirrors the Python middleware's early exits exactly: disabled mesh,
/// hop header, non-sandbox path, or local engine hit all fall through to local
/// handling.
pub async fn owner_proxy_decision<M, P, R>(
	mesh: &M,
	presence: &P,
	record_store: Option<&R>,
	client: &Client,
	path: &str,
	headers: &HeaderMap,
	outbound_token: &str,
) -> MeshResult<OwnerProxyDecision>
where
	M: OwnerRouter,
	P: SandboxPresence + ?Sized,
	R: RecordOwnerLookup + ?Sized,
{
	if !mesh.mesh_enabled() || is_mesh_hop(headers) {
		return Ok(OwnerProxyDecision::ServeLocal);
	}
	let Some(sandbox_id) = sandbox_id_in_path(path) else {
		return Ok(OwnerProxyDecision::ServeLocal);
	};
	if presence.has_sandbox(&sandbox_id) {
		return Ok(OwnerProxyDecision::ServeLocal);
	}
	let owner = match mesh.owner_of(&sandbox_id) {
		Some(owner) => Some(owner),
		None => scatter_locate(mesh, record_store, client, &sandbox_id, outbound_token).await,
	};
	let Some(owner) = owner else {
		return Ok(OwnerProxyDecision::ServeLocal);
	};
	if owner == mesh.local_node_id() {
		return Err(MeshError::unreachable("sandbox owner unreachable"));
	}
	let Some(peer_url) = mesh.peer_url(&owner) else {
		return Err(MeshError::unreachable("sandbox owner unreachable"));
	};
	Ok(OwnerProxyDecision::Forward { owner, peer_url })
}

/// Forward an HTTP request to a peer owner, replacing Authorization and adding
/// the mesh hop header.
///
/// Connection failures are `unreachable`; errors after an attempt has been made
/// are `ambiguous`, matching Python's retry contract.
pub async fn proxy_to_peer(
	client: &Client,
	request: Request<Body>,
	peer_url: &str,
	outbound_token: &str,
) -> MeshResult<Response<Body>> {
	let (parts, body) = request.into_parts();
	let body = to_bytes(body, BODY_LIMIT)
		.await
		.map_err(|err| MeshError::invalid(format!("invalid request body: {err}")))?;
	let target = peer_target_url(peer_url, &parts.uri);
	let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
		.map_err(|err| MeshError::invalid(format!("invalid method: {err}")))?;
	let mut builder = client
		.request(method, target)
		.bearer_auth(outbound_token)
		.header("X-Vmon-Mesh-Hop", HOP_VALUE)
		.body(body);
	for (name, value) in filtered_peer_request_headers(&parts.headers) {
		builder = builder.header(name, value);
	}
	let response = builder.send().await.map_err(classify_peer_send_error)?;
	let status = response.status();
	let headers = response.headers().clone();
	let bytes = response
		.bytes()
		.await
		.map_err(|err| MeshError::ambiguous(format!("peer response stream failed: {err}")))?;
	let mut out = Response::builder().status(status);
	for (name, value) in filtered_peer_response_headers(&headers) {
		out = out.header(name, value);
	}
	out.body(Body::from(bytes))
		.map_err(|err| MeshError::ambiguous(format!("invalid peer response: {err}")))
}

/// Convenience owner-router wrapper for middleware-style callers.
pub async fn proxy_owner_http<M, P, R>(
	mesh: &M,
	presence: &P,
	record_store: Option<&R>,
	client: &Client,
	request: Request<Body>,
	expected_token: Option<&str>,
	client_token: Option<&str>,
	outbound_token: &str,
) -> MeshResult<Option<Response<Body>>>
where
	M: OwnerRouter,
	P: SandboxPresence + ?Sized,
	R: RecordOwnerLookup + ?Sized,
{
	let decision = owner_proxy_decision(
		mesh,
		presence,
		record_store,
		client,
		request.uri().path(),
		request.headers(),
		outbound_token,
	)
	.await?;
	match decision {
		OwnerProxyDecision::ServeLocal => Ok(None),
		OwnerProxyDecision::Forward { peer_url, .. } => {
			require_bearer(request.headers(), expected_token, client_token)?;
			proxy_to_peer(client, request, &peer_url, outbound_token)
				.await
				.map(Some)
		},
	}
}

pub fn split_http_authority(url: &str) -> MeshResult<PeerAuthority> {
	let parsed = reqwest::Url::parse(url).map_err(|_| MeshError::invalid("invalid peer URL"))?;
	match parsed.scheme() {
		"http" | "https" => {},
		_ => return Err(MeshError::invalid("unsupported peer URL scheme")),
	}
	let Some(host) = parsed.host_str() else {
		return Err(MeshError::invalid("invalid peer URL"));
	};
	let tls = parsed.scheme() == "https";
	Ok(PeerAuthority {
		host: host.to_owned(),
		port: parsed.port().unwrap_or(if tls { 443 } else { 80 }),
		tls,
	})
}

/// Build a path for peer WebSocket forwarding.
///
/// When `preserve_query` is false, `token` and `access_token` are stripped like
/// Python's tunnel proxy; owner proxying passes true to preserve the exact peer
/// API query string.
pub fn websocket_peer_path(path: &str, query: Option<&str>, preserve_query: bool) -> String {
	let mut out = if path.starts_with('/') {
		path.to_owned()
	} else {
		format!("/{path}")
	};
	let query = if preserve_query {
		query.map(str::to_owned)
	} else {
		target_query(query)
	};
	if let Some(query) = query.filter(|query| !query.is_empty()) {
		out.push('?');
		out.push_str(&query);
	}
	out
}

/// Raw RFC-6455 bridge used by owner WebSocket proxying.
///
/// Axum has already accepted the public WebSocket; this function performs a
/// client handshake to the peer and then relays text/binary/close/ping/pong
/// frames.
#[allow(
	clippy::collapsible_match,
	reason = "async frame forwarding keeps ownership and side effects explicit per opcode"
)]
pub async fn proxy_websocket_to_peer(
	mut public: WebSocket,
	peer_url: &str,
	path: &str,
	query: Option<&str>,
	outbound_token: &str,
) {
	let target = match split_http_authority(peer_url) {
		Ok(target) => target,
		Err(_err) => {
			let _ = public.close().await;
			return;
		},
	};
	if target.tls {
		let _ = public.close().await;
		return;
	}
	let Ok(Ok(stream)) =
		timeout(WS_UPGRADE_TIMEOUT, TcpStream::connect((&*target.host, target.port))).await
	else {
		let _ = public.close().await;
		return;
	};
	let (mut reader, mut writer) = stream.into_split();
	let path = websocket_peer_path(path, query, true);
	if websocket_client_handshake(&mut reader, &mut writer, &target, &path, outbound_token)
		.await
		.is_err()
	{
		let _ = public.close().await;
		return;
	}
	loop {
		tokio::select! {
			message = public.recv() => {
				let Some(Ok(message)) = message else {
					let _ = writer.write_all(&encode_ws_frame(8, &[])).await;
					let _ = writer.shutdown().await;
					return;
				};
				match message {
					Message::Text(text) => {
						if writer.write_all(&encode_ws_frame(1, text.as_str().as_bytes())).await.is_err() { return; }
					},
					Message::Binary(bytes) => {
						if writer.write_all(&encode_ws_frame(2, &bytes)).await.is_err() { return; }
					},
					Message::Ping(bytes) => {
						if writer.write_all(&encode_ws_frame(9, &bytes)).await.is_err() { return; }
					},
					Message::Pong(bytes) => {
						if writer.write_all(&encode_ws_frame(10, &bytes)).await.is_err() { return; }
					},
					Message::Close(_) => {
						let _ = writer.write_all(&encode_ws_frame(8, &[])).await;
						let _ = writer.shutdown().await;
						return;
					},
				}
			},
			frame = read_ws_frame(&mut reader) => {
				let Ok((opcode, payload)) = frame else {
					let _ = public.close().await;
					return;
				};
				match opcode {
					1 => {
						if public
							.send(Message::Text(String::from_utf8_lossy(&payload).into_owned().into()))
							.await
							.is_err()
						{
							return;
						}
					},
					2 => {
						if public.send(Message::Binary(payload.into())).await.is_err() {
							return;
						}
					},
					8 => {
						let _ = public.close().await;
						return;
					},
					9 => {
						if writer.write_all(&encode_ws_frame(10, &payload)).await.is_err() {
							return;
						}
					},
					10 => {
						if public.send(Message::Pong(payload.into())).await.is_err() {
							return;
						}
					},
					_ => {},
				}
			},
		}
	}
}

fn peer_target_url(peer_url: &str, uri: &Uri) -> String {
	let mut target = peer_url.trim_end_matches('/').to_owned();
	target.push_str(uri.path());
	if let Some(query) = uri.query() {
		target.push('?');
		target.push_str(query);
	}
	target
}

fn filtered_peer_request_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
	headers
		.iter()
		.filter_map(|(name, value)| {
			let lower = name.as_str().to_ascii_lowercase();
			if HOP_HEADERS.contains(&lower.as_str()) {
				return None;
			}
			if lower == "authorization" {
				return None;
			}
			Some((name.clone(), value.clone()))
		})
		.collect()
}

fn filtered_peer_response_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
	headers
		.iter()
		.filter_map(|(name, value)| {
			let lower = name.as_str().to_ascii_lowercase();
			if matches!(lower.as_str(), "connection" | "content-length" | "transfer-encoding") {
				return None;
			}
			Some((name.clone(), value.clone()))
		})
		.collect()
}

fn classify_peer_send_error(err: reqwest::Error) -> MeshError {
	if err.is_connect() || err.is_timeout() {
		MeshError::unreachable("sandbox owner unreachable")
	} else {
		MeshError::ambiguous("sandbox owner response ambiguous; retry with the same idempotency key")
	}
}

fn target_query(query: Option<&str>) -> Option<String> {
	let query = query?;
	let filtered = query
		.split('&')
		.filter(|part| {
			let key = part.split_once('=').map_or(*part, |(key, _)| key);
			!matches!(key, "token" | "access_token")
		})
		.collect::<Vec<_>>()
		.join("&");
	(!filtered.is_empty()).then_some(filtered)
}

fn token_matches_any(supplied: Option<&str>, expected: Option<&str>) -> bool {
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

async fn websocket_client_handshake(
	reader: &mut tokio::net::tcp::OwnedReadHalf,
	writer: &mut tokio::net::tcp::OwnedWriteHalf,
	target: &PeerAuthority,
	path: &str,
	outbound_token: &str,
) -> std::io::Result<()> {
	let key = B64.encode(rand::random::<[u8; 16]>());
	let request = [
		format!("GET {path} HTTP/1.1"),
		format!("Host: {}:{}", target.host, target.port),
		"Upgrade: websocket".to_owned(),
		"Connection: Upgrade".to_owned(),
		format!("Sec-WebSocket-Key: {key}"),
		"Sec-WebSocket-Version: 13".to_owned(),
		format!("Authorization: Bearer {outbound_token}"),
		"X-Vmon-Mesh-Hop: 1".to_owned(),
	]
	.join("\r\n")
		+ "\r\n\r\n";
	writer.write_all(request.as_bytes()).await?;
	writer.flush().await?;
	let response = timeout(WS_UPGRADE_TIMEOUT, read_http_head(reader))
		.await
		.map_err(|_| {
			std::io::Error::new(std::io::ErrorKind::TimedOut, "websocket upgrade timed out")
		})??;
	let expected = B64.encode(Sha1::digest(format!("{key}{WS_GUID}").as_bytes()));
	let status_ok = response
		.split(|byte| *byte == b'\n')
		.next()
		.is_some_and(|line| line.windows(5).any(|window| window == b" 101 "));
	if status_ok
		&& response
			.windows(expected.len())
			.any(|window| window == expected.as_bytes())
	{
		Ok(())
	} else {
		Err(std::io::Error::other("websocket upgrade failed"))
	}
}

async fn read_http_head<R>(reader: &mut R) -> std::io::Result<Vec<u8>>
where
	R: AsyncRead + Unpin,
{
	let mut response = Vec::with_capacity(1024);
	let mut byte = [0_u8; 1];
	while !response.ends_with(b"\r\n\r\n") {
		reader.read_exact(&mut byte).await?;
		response.push(byte[0]);
		if response.len() > 64 * 1024 {
			return Err(std::io::Error::other("websocket upgrade response too large"));
		}
	}
	Ok(response)
}

fn encode_ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
	let first = 0x80 | (opcode & 0x0f);
	let mask_key = rand::random::<[u8; 4]>();
	let mut out = Vec::with_capacity(payload.len() + 14);
	out.push(first);
	match payload.len() {
		len if len < 126 => out.push(0x80 | len as u8),
		len if len <= 0xffff => {
			out.push(0x80 | 0x7e);
			out.extend_from_slice(&(len as u16).to_be_bytes());
		},
		len => {
			out.push(0x80 | 0x7f);
			out.extend_from_slice(&(len as u64).to_be_bytes());
		},
	}
	out.extend_from_slice(&mask_key);
	out.extend(
		payload
			.iter()
			.enumerate()
			.map(|(idx, byte)| byte ^ mask_key[idx % 4]),
	);
	out
}

async fn read_ws_frame<R>(reader: &mut R) -> std::io::Result<(u8, Vec<u8>)>
where
	R: AsyncRead + Unpin,
{
	let mut head = [0_u8; 2];
	reader.read_exact(&mut head).await?;
	let opcode = head[0] & 0x0f;
	let masked = head[1] & 0x80 != 0;
	let mut len = u64::from(head[1] & 0x7f);
	if len == 126 {
		let mut buf = [0_u8; 2];
		reader.read_exact(&mut buf).await?;
		len = u64::from(u16::from_be_bytes(buf));
	} else if len == 127 {
		let mut buf = [0_u8; 8];
		reader.read_exact(&mut buf).await?;
		len = u64::from_be_bytes(buf);
	}
	let mut mask = [0_u8; 4];
	if masked {
		reader.read_exact(&mut mask).await?;
	}
	let mut payload = vec![0_u8; len as usize];
	if len > 0 {
		reader.read_exact(&mut payload).await?;
	}
	if masked {
		for (idx, byte) in payload.iter_mut().enumerate() {
			*byte ^= mask[idx % 4];
		}
	}
	Ok((opcode, payload))
}

/// Helper for callers deciding if an admin-only route was attempted with only
/// the restricted client token.
pub fn forbidden_client_token_response() -> Response<Body> {
	Response::builder()
		.status(StatusCode::FORBIDDEN)
		.header(header::CONTENT_TYPE, "application/json")
		.body(Body::from(r#"{"detail":"forbidden"}"#))
		.unwrap_or_else(|_| Response::new(Body::empty()))
}

/// Returns true for peerable owner paths that are unsafe to re-forward.
pub fn local_hop_guard(headers: &HeaderMap) -> bool {
	is_mesh_hop(headers)
}

/// Convenience for route code that only has a `Uri`.
pub fn owner_path_from_uri(uri: &Uri) -> Option<String> {
	sandbox_id_in_path(uri.path())
}

/// Method set accepted by the owner HTTP proxy.  The Python middleware proxies
/// every method; this helper exists for route layers that want an explicit
/// gate.
pub const fn method_proxyable(_method: &Method) -> bool {
	true
}

/// Return every owner id known locally or through durable fallback.  Useful for
/// route-level diagnostics without making a peer request.
pub fn known_owner<M, R>(mesh: &M, record_store: Option<&R>, sandbox_id: &str) -> Option<String>
where
	M: OwnerRouter,
	R: RecordOwnerLookup + ?Sized,
{
	mesh.owner_of(sandbox_id).or_else(|| {
		record_store.and_then(|store| store.owner_record(sandbox_id).map(|record| record.owner))
	})
}

/// Build the exact peer headers used by both HTTP and WebSocket owner
/// forwarding.
pub fn mesh_peer_headers(outbound_token: &str) -> HeaderMap {
	let mut headers = HeaderMap::new();
	if let Ok(value) = HeaderValue::from_str(&format!("Bearer {outbound_token}")) {
		headers.insert(header::AUTHORIZATION, value);
	}
	headers.insert(HeaderName::from_static("x-vmon-mesh-hop"), HeaderValue::from_static(HOP_VALUE));
	headers
}

/// Utility for reconcilers/proxies that need a deduplicated exclusion set.
pub fn singleton_exclusion(node_id: &str) -> HashSet<String> {
	HashSet::from([node_id.to_owned()])
}
