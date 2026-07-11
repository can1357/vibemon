use std::{fmt::Write as _, sync::Arc};

use axum::{
	Json, Router,
	body::{Body, Bytes, to_bytes},
	extract::{Path, Request, State, WebSocketUpgrade},
	http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header},
	middleware,
	response::{IntoResponse, Response},
	routing::{any, get},
};
use include_dir::{Dir, File, include_dir};
use serde::Serialize;
use serde_json::Value;

use super::{
	error::{ApiError, ApiResult, join_error},
	state::{ApiState, auth_middleware},
	ws,
};
use crate::{
	engine::EngineApi,
	mesh::{routes::MeshRouteState, transfer},
};

const BODY_LIMIT: usize = 64 * 1024 * 1024;
static WEB_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/web");
const WEB_INDEX: &str = "index.html";

#[derive(Serialize)]
pub struct HealthBody {
	ok: bool,
}

pub fn router(state: ApiState) -> Router {
	let mut app = Router::new()
		.route("/healthz", get(healthz))
		.route("/metrics", get(metrics))
		.route("/grpc", get(bridge_ws))
		.route("/v1/sandboxes/{id}/ports/{port}", any(proxy_http_empty))
		.route("/v1/sandboxes/{id}/ports/{port}/{*rest}", any(proxy_http))
		.route("/v1/sandboxes/{id}/ports/{port}/ws", get(proxy_ws_empty))
		.route("/v1/sandboxes/{id}/ports/{port}/ws/{*rest}", get(proxy_ws))
		.with_state(state.clone());

	app = app.merge(super::grpc::routes(state.clone()));
	app = app.merge(crate::function::gateway::router(state.functions.clone()));

	if let Some(mesh) = &state.mesh {
		let mesh_state = mesh.route_state();
		app = app
			.merge(crate::mesh::routes::router(mesh_state.clone()))
			.merge(template_router(mesh_state));
	}

	app.fallback(static_guard)
		.layer(middleware::from_fn_with_state(state, auth_middleware))
}

fn template_router(state: MeshRouteState) -> Router {
	Router::new()
		.route("/v1/templates/{digest}", get(template_archive))
		.route("/v1/templates/{digest}/metadata", get(template_metadata_archive))
		.route("/v1/templates/{digest}/pages/{page}", get(template_page))
		.with_state(state)
}

async fn healthz() -> Json<HealthBody> {
	Json(HealthBody { ok: true })
}

async fn metrics(State(state): State<ApiState>) -> Response {
	let mut text = state.engine.prometheus_metrics();
	if !text.ends_with('\n') {
		text.push('\n');
	}
	text.push_str("# HELP vmon_api_auth_failures_total API authentication failures.\n");
	text.push_str("# TYPE vmon_api_auth_failures_total counter\n");
	let _ = writeln!(text, "vmon_api_auth_failures_total {}", state.auth_failure_count());
	let function = state.functions.metrics();
	text.push_str("# TYPE vmon_function_inputs_started_total counter\n");
	let _ = writeln!(text, "vmon_function_inputs_started_total {}", function.inputs_started);
	text.push_str("# TYPE vmon_function_inputs_succeeded_total counter\n");
	let _ = writeln!(text, "vmon_function_inputs_succeeded_total {}", function.inputs_succeeded);
	text.push_str("# TYPE vmon_function_user_failures_total counter\n");
	let _ = writeln!(text, "vmon_function_user_failures_total {}", function.user_failures);
	text.push_str("# TYPE vmon_function_infrastructure_failures_total counter\n");
	let _ = writeln!(
		text,
		"vmon_function_infrastructure_failures_total {}",
		function.infrastructure_failures
	);
	text.push_str("# TYPE vmon_function_workers_started_total counter\n");
	let _ = writeln!(text, "vmon_function_workers_started_total {}", function.workers_started);
	text.push_str("# TYPE vmon_function_workers_retired_total counter\n");
	let _ = writeln!(text, "vmon_function_workers_retired_total {}", function.workers_retired);
	text.push_str("# TYPE vmon_function_queue_milliseconds_total counter\n");
	let _ = writeln!(text, "vmon_function_queue_milliseconds_total {}", function.queue_millis);
	text.push_str("# TYPE vmon_function_startup_milliseconds_total counter\n");
	let _ = writeln!(text, "vmon_function_startup_milliseconds_total {}", function.startup_millis);
	text.push_str("# TYPE vmon_function_execution_milliseconds_total counter\n");
	let _ =
		writeln!(text, "vmon_function_execution_milliseconds_total {}", function.execution_millis);
	Response::builder()
		.header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
		.body(Body::from(text))
		.unwrap_or_else(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
				.into_response()
		})
}

/// gRPC-over-WebSocket bridge endpoint (`GET /grpc`). Auth happens at the
/// upgrade (bearer header or `?token=` query, UDS peer-cred bypass); the
/// bridge itself enforces admin-only methods for client tokens.
async fn bridge_ws(
	State(state): State<ApiState>,
	headers: HeaderMap,
	uri: Uri,
	ws: WebSocketUpgrade,
) -> impl IntoResponse {
	let restricted = super::state::bridge_restricted(&state, &headers, uri.query());
	let routes = super::grpc::service(state);
	ws.max_message_size(BODY_LIMIT + 1024)
		.max_frame_size(BODY_LIMIT + 1024)
		.on_upgrade(move |socket| super::bridge::serve_bridge(routes, restricted, socket))
}

async fn template_archive(
	State(state): State<MeshRouteState>,
	Path(digest): Path<String>,
	headers: HeaderMap,
) -> ApiResult<Response> {
	require_template_auth(&state, &headers)?;
	let bundle = tokio::task::spawn_blocking(move || transfer::bundle_for_digest(&digest))
		.await
		.map_err(join_error)?
		.map_err(ApiError::from)?;
	stream_bundle_response(bundle)
}

async fn template_metadata_archive(
	State(state): State<MeshRouteState>,
	Path(digest): Path<String>,
	headers: HeaderMap,
) -> ApiResult<Response> {
	require_template_auth(&state, &headers)?;
	let bundle = tokio::task::spawn_blocking(move || transfer::metadata_bundle_for_digest(&digest))
		.await
		.map_err(join_error)?
		.map_err(ApiError::from)?;
	stream_bundle_response(bundle)
}

async fn template_page(
	State(state): State<MeshRouteState>,
	Path((digest, page)): Path<(String, u64)>,
	headers: HeaderMap,
) -> ApiResult<Response> {
	require_template_auth(&state, &headers)?;
	let bytes = tokio::task::spawn_blocking(move || transfer::page_for_digest(&digest, page))
		.await
		.map_err(join_error)?
		.map_err(ApiError::from)?;
	Response::builder()
		.header(header::CONTENT_TYPE, "application/octet-stream")
		.body(Body::from(bytes))
		.map_err(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
		})
}

/// Stream a template bundle as the response body: compression runs on a
/// blocking thread and overlaps the transfer, so multi-GiB checkpoints never
/// touch server memory or disk.
fn stream_bundle_response(bundle: transfer::TemplateBundle) -> ApiResult<Response> {
	let (tx, rx) = tokio::sync::mpsc::channel(16);
	let disposition = format!("attachment; filename=\"{}\"", bundle.filename);
	let media_type = bundle.media_type;
	tokio::task::spawn_blocking(move || transfer::stream_bundle(&bundle, tx));
	Response::builder()
		.header(header::CONTENT_TYPE, media_type)
		.header(header::CONTENT_DISPOSITION, disposition)
		.body(Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)))
		.map_err(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
		})
}

fn require_template_auth(state: &MeshRouteState, headers: &HeaderMap) -> ApiResult<()> {
	let expected = state.inbound_token.trim();
	if !expected.is_empty() {
		let supplied = super::state::bearer_token(headers.get(header::AUTHORIZATION));
		let ok = supplied.as_deref().is_some_and(|token| {
			expected
				.split(',')
				.map(str::trim)
				.any(|expected| expected == token)
		});
		if !ok {
			return Err(ApiError::unauthorized("unauthorized"));
		}
	}
	let hop = headers
		.get("x-vmon-mesh-hop")
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value == "1");
	if !hop {
		return Err(ApiError::invalid("missing mesh hop header"));
	}
	Ok(())
}

async fn proxy_http(
	State(state): State<ApiState>,
	Path((id, port, rest)): Path<(String, u16, String)>,
	uri: Uri,
	request: Request,
) -> ApiResult<Response> {
	proxy_http_inner(state, id, port, rest, uri, request).await
}

async fn proxy_http_empty(
	State(state): State<ApiState>,
	Path((id, port)): Path<(String, u16)>,
	uri: Uri,
	request: Request,
) -> ApiResult<Response> {
	proxy_http_inner(state, id, port, String::new(), uri, request).await
}

async fn proxy_ws(
	State(state): State<ApiState>,
	Path((id, port, rest)): Path<(String, u16, String)>,
	uri: Uri,
	ws_upgrade: WebSocketUpgrade,
) -> ApiResult<impl IntoResponse> {
	proxy_ws_inner(state, id, port, rest, uri, ws_upgrade).await
}

async fn proxy_ws_empty(
	State(state): State<ApiState>,
	Path((id, port)): Path<(String, u16)>,
	uri: Uri,
	ws_upgrade: WebSocketUpgrade,
) -> ApiResult<impl IntoResponse> {
	proxy_ws_inner(state, id, port, String::new(), uri, ws_upgrade).await
}

async fn static_guard(request: Request) -> Response {
	if request.method() != Method::GET || is_api_guard_path(request.uri().path()) {
		return not_found_response();
	}
	let Some(index) = WEB_DIR.get_file(WEB_INDEX) else {
		return not_found_response();
	};
	let Some(path) = web_relative_path(request.uri().path()) else {
		return not_found_response();
	};
	if let Some(file) = WEB_DIR.get_file(path.as_str()) {
		return static_file_response(file);
	}
	if path.starts_with("assets/") {
		return not_found_response();
	}
	static_file_response(index)
}

fn static_file_response(file: &'static File<'static>) -> Response {
	Response::builder()
		.header(header::CONTENT_TYPE, static_content_type(file))
		.body(Body::from(Bytes::from_static(file.contents())))
		.unwrap_or_else(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
				.into_response()
		})
}

fn web_relative_path(path: &str) -> Option<String> {
	let path = path.trim_start_matches('/');
	if path.is_empty() {
		return Some(WEB_INDEX.to_owned());
	}
	let mut parts = Vec::new();
	for part in path.split('/') {
		if part.is_empty() || matches!(part, "." | "..") {
			return None;
		}
		parts.push(part);
	}
	Some(parts.join("/"))
}

fn is_api_guard_path(path: &str) -> bool {
	path.starts_with("/v1")
		|| path.starts_with("/healthz")
		|| path.starts_with("/metrics")
		|| path.starts_with("/vmon.v1.")
		|| path == "/grpc"
}

fn static_content_type(file: &File<'_>) -> &'static str {
	match file
		.path()
		.extension()
		.and_then(|ext| ext.to_str())
		.unwrap_or_default()
	{
		"html" => "text/html; charset=utf-8",
		"js" | "mjs" => "text/javascript; charset=utf-8",
		"css" => "text/css; charset=utf-8",
		"json" => "application/json",
		"svg" => "image/svg+xml",
		"png" => "image/png",
		"jpg" | "jpeg" => "image/jpeg",
		"webp" => "image/webp",
		"ico" => "image/x-icon",
		"wasm" => "application/wasm",
		"txt" => "text/plain; charset=utf-8",
		_ => "application/octet-stream",
	}
}

fn not_found_response() -> Response {
	ApiError::new(StatusCode::NOT_FOUND, "not_found", "not found").into_response()
}

async fn proxy_http_inner(
	state: ApiState,
	id: String,
	port: u16,
	rest: String,
	uri: Uri,
	request: Request,
) -> ApiResult<Response> {
	require_connect_token(&state, &id, &uri).await?;
	let target =
		engine_call(state.engine.clone(), move |engine| engine.tunnel_target(&id, port)).await?;
	let method = request.method().clone();
	let headers = request.headers().clone();
	let body = body_bytes(request).await?;
	let url = proxy_url(&target.0, target.1, &rest, uri.query());
	let client = reqwest::Client::new();
	let req_method = reqwest::Method::from_bytes(method.as_str().as_bytes())
		.map_err(|err| ApiError::invalid(format!("invalid proxy method: {err}")))?;
	let mut builder = client.request(req_method, url).body(body);
	for (name, value) in filtered_request_headers(&headers) {
		builder = builder.header(name, value);
	}
	let response = builder.send().await.map_err(|err| {
		ApiError::bad_gateway("unreachable", format!("tunnel connect failed: {err}"))
	})?;
	let status = response.status();
	let headers = response.headers().clone();
	let bytes = response.bytes().await.map_err(|err| {
		ApiError::bad_gateway("unreachable", format!("tunnel response failed: {err}"))
	})?;
	let mut builder = Response::builder().status(status);
	for (name, value) in filtered_response_headers(&headers) {
		builder = builder.header(name, value);
	}
	builder
		.body(Body::from(bytes))
		.map_err(|err| ApiError::new(StatusCode::BAD_GATEWAY, "engine_error", err.to_string()))
}

async fn proxy_ws_inner(
	state: ApiState,
	id: String,
	port: u16,
	rest: String,
	uri: Uri,
	ws_upgrade: WebSocketUpgrade,
) -> ApiResult<impl IntoResponse> {
	require_connect_token(&state, &id, &uri).await?;
	let target =
		engine_call(state.engine.clone(), move |engine| engine.tunnel_target(&id, port)).await?;
	let query = filtered_query(uri.query()).unwrap_or_default();
	Ok(ws_upgrade.on_upgrade(move |socket| ws::proxy_websocket(socket, target, rest, query)))
}

async fn require_connect_token(state: &ApiState, id: &str, uri: &Uri) -> ApiResult<()> {
	let id = id.to_owned();
	let tunnels = engine_call(state.engine.clone(), move |engine| engine.tunnels(&id)).await?;
	let expected = tunnels
		.get("connect_token")
		.and_then(Value::as_str)
		.ok_or_else(|| ApiError::unauthorized("invalid connect token"))?;
	let supplied = connect_token_from_query(uri.query());
	if supplied.as_deref() == Some(expected) {
		Ok(())
	} else {
		state.count_auth_failure();
		Err(ApiError::unauthorized("invalid connect token"))
	}
}

async fn engine_call<T, F>(engine: Arc<dyn EngineApi>, call: F) -> ApiResult<T>
where
	T: Send + 'static,
	F: FnOnce(Arc<dyn EngineApi>) -> crate::Result<T> + Send + 'static,
{
	tokio::task::spawn_blocking(move || call(engine))
		.await
		.map_err(join_error)?
		.map_err(Into::into)
}

async fn body_bytes(request: Request) -> ApiResult<Bytes> {
	to_bytes(request.into_body(), BODY_LIMIT)
		.await
		.map_err(|_| ApiError::invalid("invalid request"))
}

pub(super) fn compact_json(value: &Value) -> String {
	serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned())
}

fn proxy_url(host: &str, port: u16, rest: &str, query: Option<&str>) -> String {
	let mut url = format!("http://{host}:{port}/{}", rest.trim_start_matches('/'));
	if let Some(query) = filtered_query(query) {
		url.push('?');
		url.push_str(&query);
	}
	url
}

fn filtered_query(query: Option<&str>) -> Option<String> {
	let query = query?;
	let filtered = query
		.split('&')
		.filter(|part| {
			let key = part.split_once('=').map_or(*part, |(key, _)| key);
			!matches!(key, "connect_token" | "token" | "access_token")
		})
		.collect::<Vec<_>>()
		.join("&");
	if filtered.is_empty() {
		None
	} else {
		Some(filtered)
	}
}

fn connect_token_from_query(query: Option<&str>) -> Option<String> {
	query?.split('&').find_map(|part| {
		let (key, value) = part.split_once('=')?;
		if matches!(key, "connect_token" | "token" | "access_token") && !value.is_empty() {
			Some(value.to_owned())
		} else {
			None
		}
	})
}

fn filtered_request_headers(headers: &HeaderMap) -> Vec<(String, String)> {
	headers
		.iter()
		.filter_map(|(name, value)| {
			let lower = name.as_str().to_ascii_lowercase();
			if matches!(lower.as_str(), "host" | "connection" | "content-length" | "authorization") {
				return None;
			}
			Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned()))
		})
		.collect()
}

fn filtered_response_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
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
