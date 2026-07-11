//! Peer heartbeat transport and gossip.
//!
//! The public v1 API is intentionally redesigned, but the node-to-node mesh
//! transport keeps the Python wire rules: JSON object responses, bearer auth,
//! `X-Vmon-Mesh-Hop: 1`, short heartbeat timeouts, and the same retry taxonomy.

use std::{fmt, sync::Arc, time::Duration};

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use reqwest::Method;
use serde::Serialize;
use serde_json::{Map, Value, json};

pub const PEER_DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
pub const MESH_HOP_HEADER: &str = "X-Vmon-Mesh-Hop";
pub const MESH_HOP_VALUE: &str = "1";

pub type JsonObject = Map<String, Value>;
pub type MeshResult<T> = std::result::Result<T, MeshError>;

/// Stable mesh error code/message pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeshError {
	pub code:    String,
	pub message: String,
}

impl MeshError {
	pub fn new(message: impl Into<String>) -> Self {
		let message = message.into();
		let code = if known_mesh_code(&message) {
			message.clone()
		} else {
			"invalid".to_owned()
		};
		Self { code, message }
	}

	pub fn with_code(message: impl Into<String>, code: impl Into<String>) -> Self {
		Self { code: code.into(), message: message.into() }
	}

	pub fn invalid(message: impl Into<String>) -> Self {
		Self::with_code(message, "invalid")
	}

	pub fn unauthorized(message: impl Into<String>) -> Self {
		Self::with_code(message, "unauthorized")
	}

	pub fn unreachable(message: impl Into<String>) -> Self {
		Self::with_code(message, "unreachable")
	}

	pub fn ambiguous(message: impl Into<String>) -> Self {
		Self::with_code(message, "ambiguous")
	}

	pub fn conflict(message: impl Into<String>) -> Self {
		Self::with_code(message, "conflict")
	}

	pub fn http_status(&self) -> StatusCode {
		mesh_http_status(&self.code)
	}
}

impl fmt::Display for MeshError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.message)
	}
}

impl std::error::Error for MeshError {}

impl From<serde_json::Error> for MeshError {
	fn from(value: serde_json::Error) -> Self {
		Self::invalid(value.to_string())
	}
}

impl From<super::state::MeshError> for MeshError {
	fn from(value: super::state::MeshError) -> Self {
		Self::with_code(value.message, value.code)
	}
}

impl From<crate::EngineError> for MeshError {
	fn from(value: crate::EngineError) -> Self {
		Self::invalid(value.message)
	}
}

pub fn known_mesh_code(code: &str) -> bool {
	matches!(
		code,
		"invalid"
			| "unauthorized"
			| "unreachable"
			| "ambiguous"
			| "conflict"
			| "unplaceable"
			| "arch_required"
			| "record_unreplicated"
			| "unsupported"
	)
}

/// Map HTTP status codes returned by peers to the Python mesh retry taxonomy.
pub const fn mesh_code_for_status(status: StatusCode) -> &'static str {
	match status.as_u16() {
		401 | 403 => "unauthorized",
		409 => "conflict",
		500..=599 => "ambiguous",
		_ => "invalid",
	}
}

/// Map mesh error codes to peer-facing HTTP statuses.
pub fn mesh_http_status(code: &str) -> StatusCode {
	match code {
		"unauthorized" => StatusCode::UNAUTHORIZED,
		"unreachable" => StatusCode::BAD_GATEWAY,
		"ambiguous" => StatusCode::GATEWAY_TIMEOUT,
		"conflict" | "unplaceable" => StatusCode::CONFLICT,
		"arch_required" => StatusCode::UNPROCESSABLE_ENTITY,
		"record_unreplicated" => StatusCode::SERVICE_UNAVAILABLE,
		"unsupported" => StatusCode::NOT_IMPLEMENTED,
		_ => StatusCode::BAD_REQUEST,
	}
}

/// Return the bearer token from an `Authorization: Bearer ...` header.
pub fn bearer_token(headers: &HeaderMap) -> Option<&str> {
	let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?.trim();
	let (scheme, token) = value.split_once(' ')?;
	if scheme.eq_ignore_ascii_case("bearer") {
		Some(token.trim())
	} else {
		None
	}
}

/// True iff the request was explicitly marked as one mesh hop.
pub fn has_mesh_hop(headers: &HeaderMap) -> bool {
	headers
		.get(MESH_HOP_HEADER)
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value.trim() == MESH_HOP_VALUE)
}

/// Headers every peer-to-peer mesh request sends.
pub fn peer_headers(token: &str) -> HeaderMap {
	let mut headers = HeaderMap::new();
	let auth = format!("Bearer {token}");
	if let Ok(value) = HeaderValue::from_str(&auth) {
		headers.insert(header::AUTHORIZATION, value);
	}
	headers.insert(MESH_HOP_HEADER, HeaderValue::from_static(MESH_HOP_VALUE));
	headers
}

#[derive(Clone, Debug)]
pub struct PeerEndpoint {
	pub node_id:   String,
	pub advertise: String,
}

/// Minimal membership view needed by the heartbeat driver.
pub trait HeartbeatView {
	fn heartbeat_peers(&self) -> Vec<PeerEndpoint>;
	fn self_state_wire(&self) -> Value;
	fn known_members_wire(&self) -> Vec<Value>;
	fn merge_heartbeat_response(&self, response: JsonObject) -> MeshResult<()>;
	fn reap_stale_peers(&self);
}

/// Reqwest-backed node-to-node client.
///
/// `client` carries the control-plane deadline (every JSON exchange must
/// finish within it); `bulk` has only a connect timeout, because template
/// archive pulls stream multi-GiB bodies whose duration scales with image
/// size, not network health.
#[derive(Clone)]
pub struct PeerHttpClient {
	client:          reqwest::Client,
	bulk:            reqwest::Client,
	token:           Arc<str>,
	default_timeout: Duration,
}

impl PeerHttpClient {
	pub fn new(token: impl Into<String>) -> MeshResult<Self> {
		Self::with_timeout(token, PEER_DEFAULT_TIMEOUT)
	}

	pub fn with_timeout(token: impl Into<String>, default_timeout: Duration) -> MeshResult<Self> {
		let client = reqwest::Client::builder()
			.timeout(default_timeout)
			.build()
			.map_err(|err| MeshError::invalid(format!("mesh HTTP client init failed: {err}")))?;
		let bulk = reqwest::Client::builder()
			.connect_timeout(default_timeout)
			.build()
			.map_err(|err| MeshError::invalid(format!("mesh bulk client init failed: {err}")))?;
		Ok(Self { client, bulk, token: Arc::from(token.into()), default_timeout })
	}

	pub fn with_client(
		client: reqwest::Client,
		token: impl Into<String>,
		default_timeout: Duration,
	) -> Self {
		let bulk = client.clone();
		Self { client, bulk, token: Arc::from(token.into()), default_timeout }
	}

	pub const fn client(&self) -> &reqwest::Client {
		&self.client
	}

	/// Deadline-free client for streaming template archive pulls.
	pub const fn bulk_client(&self) -> &reqwest::Client {
		&self.bulk
	}

	pub fn token(&self) -> &str {
		&self.token
	}

	pub const fn default_timeout(&self) -> Duration {
		self.default_timeout
	}

	pub async fn post<T>(
		&self,
		base_url: &str,
		path: &str,
		payload: &T,
		timeout: Option<Duration>,
	) -> MeshResult<JsonObject>
	where
		T: Serialize + Sync + ?Sized,
	{
		self
			.request(Method::POST, base_url, path, Some(payload), timeout)
			.await
	}

	pub async fn get(
		&self,
		base_url: &str,
		path: &str,
		timeout: Option<Duration>,
	) -> MeshResult<JsonObject> {
		self
			.request::<Value>(Method::GET, base_url, path, None, timeout)
			.await
	}

	async fn request<T>(
		&self,
		method: Method,
		base_url: &str,
		path: &str,
		payload: Option<&T>,
		timeout: Option<Duration>,
	) -> MeshResult<JsonObject>
	where
		T: Serialize + Sync + ?Sized,
	{
		let url = format!("{}{}", base_url.trim_end_matches('/'), path);
		let mut request = self
			.client
			.request(method, url)
			.header(header::AUTHORIZATION, format!("Bearer {}", self.token))
			.header(MESH_HOP_HEADER, MESH_HOP_VALUE);
		if let Some(timeout) = timeout {
			request = request.timeout(timeout);
		}
		if let Some(payload) = payload {
			request = request.json(payload);
		}
		let response = request.send().await.map_err(classify_reqwest_error)?;
		let status = response.status();
		if !status.is_success() {
			let detail = response_detail(response).await;
			return Err(MeshError::with_code(
				if detail.is_empty() {
					"mesh peer rejected request".to_owned()
				} else {
					detail
				},
				mesh_code_for_status(status),
			));
		}
		let bytes = response.bytes().await.map_err(classify_reqwest_error)?;
		if bytes.is_empty() {
			return Ok(JsonObject::new());
		}
		let value: Value = serde_json::from_slice(&bytes)
			.map_err(|err| MeshError::invalid(format!("mesh peer returned invalid JSON: {err}")))?;
		match value {
			Value::Object(object) => Ok(object),
			_ => Err(MeshError::invalid("mesh peer returned non-object JSON")),
		}
	}
}

/// Run one gossip round: POST `{"state": ..., "known": [...]}` to each peer,
/// merge successful responses, ignore failed peers, then reap stale membership.
pub async fn heartbeat_once<G>(mesh: &G, transport: &PeerHttpClient) -> usize
where
	G: HeartbeatView + Sync,
{
	let peers = mesh.heartbeat_peers();
	let mut merged = 0usize;
	for peer in peers {
		let payload = json!({
			"state": mesh.self_state_wire(),
			"known": mesh.known_members_wire(),
		});
		let Ok(response) = transport
			.post(&peer.advertise, "/v1/mesh/heartbeat", &payload, None)
			.await
		else {
			continue;
		};
		if mesh.merge_heartbeat_response(response).is_ok() {
			merged += 1;
		}
	}
	mesh.reap_stale_peers();
	merged
}

fn classify_reqwest_error(err: reqwest::Error) -> MeshError {
	if err.is_builder() || err.is_request() {
		return MeshError::invalid(format!("mesh request could not be built: {err}"));
	}
	if err.is_connect() {
		// Connection never established: the peer provably did not run the request.
		return MeshError::unreachable("peer unreachable");
	}
	// Bytes may have been sent (read timeout, reset, protocol/body error): the
	// caller must not reroute mutating work because the peer may have committed.
	MeshError::ambiguous("peer response ambiguous")
}

async fn response_detail(response: reqwest::Response) -> String {
	let text = response.text().await.unwrap_or_default();
	if let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&text) {
		if let Some(Value::String(detail)) = object.get("detail") {
			return detail.clone();
		}
		if let Some(Value::Object(detail)) = object.get("detail")
			&& let Some(Value::String(message)) = detail.get("message")
		{
			return message.clone();
		}
		if let Some(Value::String(message)) = object.get("message") {
			return message.clone();
		}
	}
	text
}
