//! Per-sandbox HTTP broker that injects scoped credentials outside the guest.

use std::{
	collections::{HashMap, HashSet},
	io,
	net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
	sync::Arc,
	time::{Duration, Instant},
};

use axum::{
	Json, Router,
	extract::{Path, State},
	http::{HeaderName, HeaderValue, StatusCode, header},
	response::{IntoResponse, Response},
	routing::post,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use parking_lot::Mutex;
use reqwest::{
	Method, Url,
	dns::{Addrs, Name, Resolve, Resolving},
	redirect::Policy,
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, task::JoinHandle};
use zeroize::Zeroize;

use super::{
	audit::{AuditEvent, AuditLog},
	credentials::{Credential, CredentialProvider},
};
use crate::{EngineError, Result};

const MAX_REQUEST_BODY: usize = 16 * 1024 * 1024;
const MAX_RESPONSE_BODY: usize = 16 * 1024 * 1024;
const RATE_WINDOW: Duration = Duration::from_mins(1);

#[derive(Debug)]
struct PublicDnsResolver;

impl Resolve for PublicDnsResolver {
	fn resolve(&self, name: Name) -> Resolving {
		let host = name.as_str().to_owned();
		Box::pin(async move {
			let addresses = match tokio::net::lookup_host((host.as_str(), 0)).await {
				Ok(addresses) => addresses.collect::<Vec<_>>(),
				Err(error) => {
					return Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync + 'static>);
				},
			};
			if let Err(error) = validate_resolved_addresses(&host, &addresses) {
				return Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync + 'static>);
			}
			Ok(Box::new(addresses.into_iter()) as Addrs)
		})
	}
}

/// Fixed per-TAP gateway port; each Linux sandbox binds a distinct host IP.
pub const CREDENTIAL_GATEWAY_PORT: u16 = 17_973;

/// Live per-sandbox gateway. Dropping it closes the private listener.
pub struct CredentialGateway {
	endpoint: String,
	port:     u16,
	task:     JoinHandle<()>,
}

impl std::fmt::Debug for CredentialGateway {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("CredentialGateway")
			.field("endpoint", &self.endpoint)
			.finish()
	}
}

impl CredentialGateway {
	/// Start a gateway bound to a host-only address and return its guest URL.
	#[allow(
		clippy::literal_string_with_formatting_args,
		reason = "Axum denotes path captures with braces"
	)]
	pub fn start(
		runtime: &tokio::runtime::Runtime,
		bind_ip: IpAddr,
		guest_ip: IpAddr,
		bind_port: u16,
		tenant: String,
		actor: String,
		credential_names: Vec<String>,
		provider: Arc<dyn CredentialProvider>,
		audit: AuditLog,
	) -> Result<Self> {
		let listener = runtime.block_on(TcpListener::bind(SocketAddr::new(bind_ip, bind_port)))?;
		let port = listener.local_addr()?.port();
		let capability = hex::encode(rand::random::<[u8; 24]>());
		let endpoint = format!("http://{guest_ip}:{port}/{capability}");
		let client = reqwest::Client::builder()
			.redirect(Policy::none())
			.no_proxy()
			.dns_resolver(Arc::new(PublicDnsResolver))
			.connect_timeout(Duration::from_secs(10))
			.timeout(Duration::from_mins(1))
			.build()
			.map_err(|error| {
				EngineError::engine(format!("building credential gateway client: {error}"))
			})?;
		let state = GatewayState {
			tenant,
			actor,
			capability,
			credential_names: credential_names.into_iter().collect(),
			provider,
			audit,
			client,
			rate: Arc::new(Mutex::new(HashMap::new())),
		};
		let router = Router::new()
			.route("/{capability}", post(broker_request))
			.with_state(state);
		let task = runtime.spawn(async move {
			if let Err(error) = axum::serve(listener, router).await {
				tracing::warn!(%error, "credential gateway stopped");
			}
		});
		Ok(Self { endpoint, port, task })
	}

	/// Guest-visible non-secret broker endpoint.
	pub fn endpoint(&self) -> &str {
		&self.endpoint
	}

	/// Host listener port used by the user-network packet filter.
	pub const fn port(&self) -> u16 {
		self.port
	}
}

impl Drop for CredentialGateway {
	fn drop(&mut self) {
		self.task.abort();
	}
}

#[derive(Clone)]
struct GatewayState {
	tenant:           String,
	actor:            String,
	capability:       String,
	credential_names: HashSet<String>,
	provider:         Arc<dyn CredentialProvider>,
	audit:            AuditLog,
	client:           reqwest::Client,
	rate:             Arc<Mutex<HashMap<String, RateWindow>>>,
}

#[derive(Clone, Copy)]
struct RateWindow {
	started: Instant,
	count:   u32,
}

#[derive(Deserialize)]
struct BrokerRequest {
	credential:  String,
	method:      String,
	url:         String,
	#[serde(default)]
	headers:     HashMap<String, String>,
	#[serde(default)]
	body_base64: String,
}

#[derive(Serialize)]
struct BrokerResponse {
	status:      u16,
	headers:     HashMap<String, String>,
	body_base64: String,
}

async fn broker_request(
	State(state): State<GatewayState>,
	Path(capability): Path<String>,
	Json(request): Json<BrokerRequest>,
) -> std::result::Result<Json<BrokerResponse>, GatewayError> {
	if !constant_time_eq(capability.as_bytes(), state.capability.as_bytes()) {
		record(&state, &request.credential, "denied", Some("invalid_capability"), None);
		return Err(GatewayError::forbidden("invalid gateway capability"));
	}
	if !state.credential_names.contains(&request.credential) {
		record(&state, &request.credential, "denied", Some("not_attached"), None);
		return Err(GatewayError::forbidden("credential is not attached to this sandbox"));
	}
	record_required(&state, &request.credential, "attempted", None, None)?;
	let credential = state
		.provider
		.get(&state.tenant, &request.credential)
		.map_err(|error| GatewayError::forbidden(error.message))?;
	let now = unix_millis();
	if !credential.active_at(now) {
		record(&state, &request.credential, "denied", Some("expired"), None);
		return Err(GatewayError::forbidden("credential has expired"));
	}
	check_rate(&state, &credential)?;
	let url =
		Url::parse(&request.url).map_err(|_| GatewayError::bad_request("invalid target URL"))?;
	if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
		return Err(GatewayError::bad_request(
			"credential targets must be HTTPS URLs without embedded credentials",
		));
	}
	let host = url
		.host_str()
		.ok_or_else(|| GatewayError::bad_request("credential target has no host"))?;
	if !credential.permits_domain(host) {
		record(&state, &request.credential, "denied", Some("domain_scope"), Some(host));
		return Err(GatewayError::forbidden("target is outside the credential domain scope"));
	}
	validate_public_host(host)?;
	let method = Method::from_bytes(request.method.as_bytes())
		.map_err(|_| GatewayError::bad_request("invalid HTTP method"))?;
	if matches!(method, Method::CONNECT | Method::TRACE) {
		return Err(GatewayError::bad_request("HTTP method is not brokerable"));
	}
	let mut body = BASE64
		.decode(request.body_base64.as_bytes())
		.map_err(|_| GatewayError::bad_request("body_base64 is invalid"))?;
	if body.len() > MAX_REQUEST_BODY {
		body.zeroize();
		return Err(GatewayError::payload_too_large("broker request body exceeds 16 MiB"));
	}
	let mut upstream = state.client.request(method, url.clone());
	for (name, value) in request.headers {
		let name = HeaderName::try_from(name)
			.map_err(|_| GatewayError::bad_request("invalid request header name"))?;
		if is_hop_header(name.as_str())
			|| credential
				.headers
				.keys()
				.any(|secret| secret.eq_ignore_ascii_case(name.as_str()))
		{
			continue;
		}
		let value = HeaderValue::try_from(value)
			.map_err(|_| GatewayError::bad_request("invalid request header value"))?;
		upstream = upstream.header(name, value);
	}
	for (name, value) in &credential.headers {
		let name = HeaderName::try_from(name)
			.map_err(|_| GatewayError::internal("stored credential header is invalid"))?;
		let value = HeaderValue::from_bytes(value)
			.map_err(|_| GatewayError::internal("stored credential value is invalid"))?;
		upstream = upstream.header(name, value);
	}
	upstream = upstream.header(header::ACCEPT_ENCODING, "identity");
	let result = upstream.body(std::mem::take(&mut body)).send().await;
	body.zeroize();
	let mut response = match result {
		Ok(response) => response,
		Err(error) => {
			record(&state, &request.credential, "failed", Some("upstream"), Some(host));
			return Err(GatewayError::bad_gateway(format!("credential upstream failed: {error}")));
		},
	};
	if response
		.headers()
		.get(header::CONTENT_ENCODING)
		.is_some_and(|value| {
			value
				.to_str()
				.map_or(true, |value| !value.trim().eq_ignore_ascii_case("identity"))
		}) {
		record(&state, &request.credential, "failed", Some("encoded_response"), Some(host));
		return Err(GatewayError::bad_gateway("credential upstream returned an encoded response"));
	}
	let status = response.status().as_u16();
	let mut headers = HashMap::new();
	for (name, value) in response.headers() {
		if is_hop_header(name.as_str()) || contains_secret(value.as_bytes(), &credential) {
			continue;
		}
		if let Ok(value) = value.to_str() {
			headers.insert(name.as_str().to_owned(), value.to_owned());
		}
	}
	let mut response_body = Vec::new();
	while let Some(chunk) = response
		.chunk()
		.await
		.map_err(|error| GatewayError::bad_gateway(format!("reading upstream response: {error}")))?
	{
		if response_body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BODY {
			response_body.zeroize();
			return Err(GatewayError::payload_too_large("broker response body exceeds 16 MiB"));
		}
		response_body.extend_from_slice(&chunk);
	}
	redact_secrets(&mut response_body, &credential);
	record(&state, &request.credential, "allowed", None, Some(host));
	let body_base64 = BASE64.encode(&response_body);
	response_body.zeroize();
	Ok(Json(BrokerResponse { status, headers, body_base64 }))
}

fn check_rate(
	state: &GatewayState,
	credential: &Credential,
) -> std::result::Result<(), GatewayError> {
	let now = Instant::now();
	let mut rates = state.rate.lock();
	let window = rates
		.entry(credential.name.clone())
		.or_insert(RateWindow { started: now, count: 0 });
	if now.duration_since(window.started) >= RATE_WINDOW {
		*window = RateWindow { started: now, count: 0 };
	}
	if window.count >= credential.requests_per_minute {
		record(state, &credential.name, "denied", Some("rate_limit"), None);
		return Err(GatewayError::too_many_requests("credential request rate exceeded"));
	}
	window.count += 1;
	Ok(())
}

fn record_required(
	state: &GatewayState,
	credential: &str,
	outcome: &str,
	reason: Option<&str>,
	domain: Option<&str>,
) -> std::result::Result<(), GatewayError> {
	let event = audit_event(state, credential, outcome, reason, domain);
	state
		.audit
		.record(&event)
		.map_err(|_| GatewayError::internal("credential audit log is unavailable"))
}

fn record(
	state: &GatewayState,
	credential: &str,
	outcome: &str,
	reason: Option<&str>,
	domain: Option<&str>,
) {
	let event = audit_event(state, credential, outcome, reason, domain);
	if let Err(error) = state.audit.record(&event) {
		tracing::error!(%error, "writing credential audit event failed");
	}
}

fn audit_event(
	state: &GatewayState,
	credential: &str,
	outcome: &str,
	reason: Option<&str>,
	domain: Option<&str>,
) -> AuditEvent {
	let mut event =
		AuditEvent::new(&state.tenant, &state.actor, "credential.use", credential, outcome);
	if let Some(reason) = reason {
		event = event.with_attribute("reason", reason);
	}
	if let Some(domain) = domain {
		event = event.with_attribute("domain", domain);
	}
	event
}

fn validate_public_host(host: &str) -> std::result::Result<(), GatewayError> {
	if host.parse::<IpAddr>().is_ok_and(is_protected) {
		return Err(GatewayError::forbidden(
			"credential target resolved to a protected network address",
		));
	}
	Ok(())
}

fn validate_resolved_addresses(host: &str, addresses: &[SocketAddr]) -> io::Result<()> {
	if addresses.is_empty() {
		return Err(io::Error::new(
			io::ErrorKind::AddrNotAvailable,
			format!("credential target DNS lookup returned no addresses for {host}"),
		));
	}
	if addresses.iter().any(|address| is_protected(address.ip())) {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			format!("credential target resolved to a protected network address: {host}"),
		));
	}
	Ok(())
}

fn is_protected(address: IpAddr) -> bool {
	match address {
		IpAddr::V4(address) => {
			address.is_private()
				|| address.is_loopback()
				|| address.is_link_local()
				|| address.is_broadcast()
				|| address.is_documentation()
				|| address.is_unspecified()
				|| address.is_multicast()
				|| in_v4(address, Ipv4Addr::UNSPECIFIED, 8)
				|| in_v4(address, Ipv4Addr::new(100, 64, 0, 0), 10)
				|| in_v4(address, Ipv4Addr::new(192, 0, 0, 0), 24)
				|| in_v4(address, Ipv4Addr::new(192, 88, 99, 0), 24)
				|| in_v4(address, Ipv4Addr::new(198, 18, 0, 0), 15)
				|| in_v4(address, Ipv4Addr::new(240, 0, 0, 0), 4)
		},
		IpAddr::V6(address) => {
			address
				.to_ipv4()
				.is_some_and(|address| is_protected(IpAddr::V4(address)))
				|| address.is_loopback()
				|| address.is_unspecified()
				|| address.is_multicast()
				|| in_v6(address, Ipv6Addr::new(0x0064, 0xff9b, 0, 0, 0, 0, 0, 0), 96)
				|| in_v6(address, Ipv6Addr::new(0x0064, 0xff9b, 1, 0, 0, 0, 0, 0), 48)
				|| in_v6(address, Ipv6Addr::new(0x0100, 0, 0, 0, 0, 0, 0, 0), 64)
				|| in_v6(address, Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23)
				|| in_v6(address, Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16)
				|| in_v6(address, Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7)
				|| in_v6(address, Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0), 10)
				|| in_v6(address, Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10)
		},
	}
}

fn in_v4(address: Ipv4Addr, network: Ipv4Addr, prefix: u8) -> bool {
	let mask = if prefix == 0 {
		0
	} else {
		u32::MAX << (32 - prefix)
	};
	u32::from(address) & mask == u32::from(network) & mask
}

fn in_v6(address: Ipv6Addr, network: Ipv6Addr, prefix: u8) -> bool {
	let mask = if prefix == 0 {
		0
	} else {
		u128::MAX << (128 - prefix)
	};
	u128::from(address) & mask == u128::from(network) & mask
}

fn contains_secret(value: &[u8], credential: &Credential) -> bool {
	credential
		.headers
		.values()
		.any(|secret| !secret.is_empty() && find_bytes(value, secret).is_some())
}

fn redact_secrets(value: &mut Vec<u8>, credential: &Credential) {
	let secrets = credential
		.headers
		.values()
		.map(Vec::as_slice)
		.filter(|secret| !secret.is_empty())
		.collect::<Vec<_>>();
	if secrets.is_empty() {
		return;
	}
	let mut source = std::mem::take(value);
	let mut redacted = Vec::with_capacity(source.len());
	let mut cursor = 0;
	while cursor < source.len() {
		let matched = secrets
			.iter()
			.filter(|secret| source[cursor..].starts_with(secret))
			.map(|secret| secret.len())
			.max();
		if let Some(length) = matched {
			redacted.resize(redacted.len() + length, b'*');
			cursor += length;
		} else {
			redacted.push(source[cursor]);
			cursor += 1;
		}
	}
	source.zeroize();
	*value = redacted;
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

fn is_hop_header(name: &str) -> bool {
	matches!(
		name.to_ascii_lowercase().as_str(),
		"connection"
			| "keep-alive"
			| "proxy-authenticate"
			| "proxy-authorization"
			| "te" | "trailer"
			| "transfer-encoding"
			| "upgrade"
			| "host"
			| "content-length"
			| "accept-encoding"
	)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
	let mut different = left.len() ^ right.len();
	let width = left.len().max(right.len());
	for index in 0..width {
		different |= usize::from(
			left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
		);
	}
	different == 0
}

fn unix_millis() -> u64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

struct GatewayError {
	status:  StatusCode,
	message: String,
}

impl GatewayError {
	fn bad_request(message: impl Into<String>) -> Self {
		Self { status: StatusCode::BAD_REQUEST, message: message.into() }
	}

	fn forbidden(message: impl Into<String>) -> Self {
		Self { status: StatusCode::FORBIDDEN, message: message.into() }
	}

	fn payload_too_large(message: impl Into<String>) -> Self {
		Self { status: StatusCode::PAYLOAD_TOO_LARGE, message: message.into() }
	}

	fn too_many_requests(message: impl Into<String>) -> Self {
		Self { status: StatusCode::TOO_MANY_REQUESTS, message: message.into() }
	}

	fn bad_gateway(message: impl Into<String>) -> Self {
		Self { status: StatusCode::BAD_GATEWAY, message: message.into() }
	}

	fn internal(message: impl Into<String>) -> Self {
		Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: message.into() }
	}
}

impl IntoResponse for GatewayError {
	fn into_response(self) -> Response {
		(self.status, Json(serde_json::json!({ "error": self.message }))).into_response()
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::BTreeMap, sync::Arc};

	use tempfile::TempDir;

	use super::{is_protected, redact_secrets, validate_public_host, validate_resolved_addresses};
	use crate::{
		home::Home,
		security::{
			Credential,
			credentials::{CredentialProvider, CredentialStore},
			crypto::Keyring,
		},
	};

	#[test]
	fn gateway_redacts_secret_bytes_from_upstream_data() {
		let mut credential = Credential {
			name:                   "api".into(),
			allowed_domains:        vec!["api.example.com".into()],
			headers:                BTreeMap::from([("Authorization".into(), b"top-secret".to_vec())]),
			expires_at_unix_millis: None,
			requests_per_minute:    1,
			version:                "v1".into(),
		};
		let mut body = b"echo: top-secret".to_vec();
		redact_secrets(&mut body, &credential);
		assert_eq!(body, b"echo: **********");
		credential.headers.insert("X-Short".into(), b"A".to_vec());
		let mut short = b"A [REDACTED]".to_vec();
		redact_secrets(&mut short, &credential);
		assert_eq!(short, b"* [RED*CTED]");
		credential.headers.clear();
	}

	#[test]
	fn protected_address_filter_blocks_internal_networks() {
		assert!(is_protected("127.0.0.1".parse().unwrap()));
		assert!(is_protected("169.254.169.254".parse().unwrap()));
		assert!(is_protected("0.0.0.1".parse().unwrap()));
		assert!(is_protected("::ffff:127.0.0.1".parse().unwrap()));
		assert!(is_protected("64:ff9b::7f00:1".parse().unwrap()));
		assert!(!is_protected("1.1.1.1".parse().unwrap()));
	}

	#[test]
	fn resolver_rejects_every_protected_or_empty_answer_set() {
		let public = ["1.1.1.1:0".parse().unwrap(), "[2606:4700:4700::1111]:0".parse().unwrap()];
		assert!(validate_resolved_addresses("public.example", &public).is_ok());

		let mixed = ["1.1.1.1:0".parse().unwrap(), "127.0.0.1:0".parse().unwrap()];
		let error = validate_resolved_addresses("rebound.example", &mixed).unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
		assert!(validate_resolved_addresses("empty.example", &[]).is_err());
		assert!(validate_public_host("169.254.169.254").is_err());
	}

	#[test]
	fn credential_provider_fixture_stays_host_side() {
		let temp = TempDir::new().unwrap();
		let home = Home::new(temp.path());
		let keys = Arc::new(Keyring::open(&home).unwrap());
		let store = CredentialStore::open(&home, keys).unwrap();
		store
			.put("default", "default", Credential {
				name:                   "api".into(),
				allowed_domains:        vec!["api.example.com".into()],
				headers:                BTreeMap::from([("Authorization".into(), b"token".to_vec())]),
				expires_at_unix_millis: None,
				requests_per_minute:    1,
				version:                String::new(),
			})
			.unwrap();
		assert_eq!(store.list("default").unwrap().len(), 1);
	}
}
