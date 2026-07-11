use std::{convert::Infallible, fmt::Write as _, sync::Arc, time::Duration};

use axum::{
	Json, Router,
	body::{Body, Bytes, to_bytes},
	extract::{Path, Query, Request, State, WebSocketUpgrade},
	http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header},
	middleware,
	response::{IntoResponse, Response},
	routing::{any, get, post, put},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use futures_util::stream;
use include_dir::{Dir, File, include_dir};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Value, json};
use utoipa::{OpenApi, ToSchema};

use super::{
	error::{ApiError, ApiResult, ErrorBody, join_error},
	state::{ApiState, auth_middleware},
	validation, ws,
};
use crate::{
	EngineError,
	engine::EngineApi,
	mesh::{routes::MeshRouteState, transfer},
	models::{
		ExecBody, ExtendBody, ForkBody, MigrateBody, NetworkBody, PoolPutBody, RestoreBody,
		SandboxCreate, SnapshotBody, SnapshotFsBody, StopBody,
	},
};

const BODY_LIMIT: usize = 64 * 1024 * 1024;
static WEB_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/web");
const WEB_INDEX: &str = "index.html";

#[derive(Serialize, ToSchema)]
pub struct HealthBody {
	ok: bool,
}

#[derive(Serialize, ToSchema)]
pub struct SandboxesBody {
	sandboxes: Vec<Value>,
}

#[derive(Serialize, ToSchema)]
pub struct SnapshotsBody {
	snapshots: Vec<String>,
}

#[derive(Serialize, ToSchema)]
pub struct VolumesBody {
	volumes: Vec<String>,
}

#[derive(Serialize, ToSchema)]
pub struct ExecCaptureBody {
	exit:       i64,
	stdout_b64: String,
	stderr_b64: String,
}

#[derive(Serialize, ToSchema)]
pub struct OkBody {
	ok: bool,
}

#[derive(Deserialize)]
struct ListQuery {
	#[serde(default, deserialize_with = "deserialize_tag_query")]
	tag: Vec<String>,
}

fn deserialize_tag_query<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
	D: Deserializer<'de>,
{
	struct TagVisitor;

	impl<'de> de::Visitor<'de> for TagVisitor {
		type Value = Vec<String>;

		fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			formatter.write_str("a tag filter string or a list of tag filter strings")
		}

		fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
		where
			E: de::Error,
		{
			Ok(vec![value.to_owned()])
		}

		fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
		where
			E: de::Error,
		{
			Ok(vec![value])
		}

		fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
		where
			A: de::SeqAccess<'de>,
		{
			let mut values = Vec::new();
			while let Some(value) = seq.next_element()? {
				values.push(value);
			}
			Ok(values)
		}
	}

	deserializer.deserialize_any(TagVisitor)
}

#[derive(Deserialize)]
struct LogsQuery {
	#[serde(default)]
	follow: bool,
}

#[derive(Deserialize)]
struct FileQuery {
	path:      String,
	#[serde(default)]
	recursive: bool,
}

#[derive(Deserialize)]
struct PathQuery {
	path: String,
}

pub fn router(state: ApiState) -> Router {
	let mut app = Router::new()
		.route("/healthz", get(healthz))
		.route("/v1/openapi.json", get(openapi_json_route))
		.route("/v1/info", get(info))
		.route("/metrics", get(metrics))
		.route("/v1/events", get(events))
		.route("/v1/sandboxes", post(create_sandbox).get(list_sandboxes))
		.route("/v1/sandboxes/{id}", get(get_sandbox).delete(remove_sandbox))
		.route("/v1/sandboxes/{id}/stop", post(stop_sandbox))
		.route("/v1/sandboxes/{id}/terminate", post(terminate_sandbox))
		.route("/v1/sandboxes/{id}/pause", post(pause_sandbox))
		.route("/v1/sandboxes/{id}/resume", post(resume_sandbox))
		.route("/v1/sandboxes/{id}/extend", post(extend_sandbox))
		.route("/v1/sandboxes/{id}/metrics", get(sandbox_metrics))
		.route("/v1/sandboxes/{id}/logs", get(logs_sandbox))
		.route("/v1/sandboxes/{id}/exec", post(exec_capture).get(exec_ws))
		.route("/v1/sandboxes/{id}/attach", get(attach_ws))
		.route("/v1/sandboxes/{id}/files", get(file_read).put(file_write).delete(file_delete))
		.route("/v1/sandboxes/{id}/files/list", get(file_list))
		.route("/v1/sandboxes/{id}/files/stat", get(file_stat))
		.route("/v1/sandboxes/{id}/network", get(network_get).put(network_set))
		.route("/v1/sandboxes/{id}/tunnels", get(tunnels))
		.route("/v1/sandboxes/{id}/migrate", post(migrate))
		.route("/v1/sandboxes/{id}/snapshots", post(snapshot))
		.route("/v1/sandboxes/{id}/snapshots/fs", post(snapshot_fs))
		.route("/v1/sandboxes/{id}/ports/{port}", any(proxy_http_empty))
		.route("/v1/sandboxes/{id}/ports/{port}/{*rest}", any(proxy_http))
		.route("/v1/sandboxes/{id}/ports/{port}/ws", get(proxy_ws_empty))
		.route("/v1/sandboxes/{id}/ports/{port}/ws/{*rest}", get(proxy_ws))
		.route("/v1/snapshots", get(list_snapshots))
		.route("/v1/snapshots/{name}/restore", post(restore_snapshot))
		.route("/v1/snapshots/{name}/fork", post(fork_snapshot))
		.route("/v1/volumes", get(volume_list))
		.route("/v1/volumes/{name}", put(volume_create).delete(volume_delete))
		.route("/v1/pools", get(pool_list))
		.route("/v1/pools/{reference}", put(pool_set).delete(pool_delete))
		.route("/v1/shell", get(shell_ws))
		.with_state(state.clone());

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

#[utoipa::path(get, path = "/healthz", responses((status = 200, description = "healthy", body = HealthBody)))]
async fn healthz() -> Json<HealthBody> {
	Json(HealthBody { ok: true })
}

#[utoipa::path(get, path = "/v1/openapi.json", responses((status = 200, description = "OpenAPI JSON")))]
async fn openapi_json_route() -> Response {
	Response::builder()
		.header(header::CONTENT_TYPE, "application/json")
		.body(Body::from(super::openapi_json()))
		.unwrap_or_else(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
				.into_response()
		})
}

#[utoipa::path(get, path = "/v1/info", responses((status = 200, description = "server info"), (status = 503, body = ErrorBody)))]
async fn info(State(state): State<ApiState>) -> ApiResult<Json<Value>> {
	let value = engine_call(state.engine.clone(), |engine| engine.info()).await?;
	Ok(Json(value))
}

#[utoipa::path(get, path = "/metrics", responses((status = 200, description = "Prometheus metrics")))]
async fn metrics(State(state): State<ApiState>) -> Response {
	let mut text = state.engine.prometheus_metrics();
	if !text.ends_with('\n') {
		text.push('\n');
	}
	text.push_str("# HELP vmon_api_auth_failures_total API authentication failures.\n");
	text.push_str("# TYPE vmon_api_auth_failures_total counter\n");
	let _ = writeln!(text, "vmon_api_auth_failures_total {}", state.auth_failure_count());
	Response::builder()
		.header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
		.body(Body::from(text))
		.unwrap_or_else(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
				.into_response()
		})
}

#[utoipa::path(get, path = "/v1/events", responses((status = 200, description = "SSE events")))]
async fn events(State(state): State<ApiState>) -> Response {
	let rx = state.engine.subscribe_events();
	let rx = flume_to_unbounded(rx);
	let interval = tokio::time::interval(Duration::from_secs(15));
	let stream = stream::unfold((rx, interval, true), |(mut rx, mut interval, first)| async move {
		if first {
			return Some((
				Ok::<Bytes, Infallible>(Bytes::from_static(b": keepalive\n\n")),
				(rx, interval, false),
			));
		}
		tokio::select! {
			_ = interval.tick() => Some((Ok(Bytes::from_static(b": keepalive\n\n")), (rx, interval, false))),
			payload = rx.recv() => match payload {
				Some(payload) => {
					let data = format!("data: {}\n\n", compact_json(&payload));
					Some((Ok(Bytes::from(data)), (rx, interval, false)))
				},
				None => None,
			},
		}
	});
	Response::builder()
		.header(header::CONTENT_TYPE, "text/event-stream")
		.body(Body::from_stream(stream))
		.unwrap_or_else(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
				.into_response()
		})
}

#[utoipa::path(post, path = "/v1/sandboxes", request_body = SandboxCreate, responses((status = 201, description = "created"), (status = 400, body = ErrorBody)))]
async fn create_sandbox(State(state): State<ApiState>, request: Request) -> ApiResult<Response> {
	let headers = request.headers().clone();
	let value = json_value(request).await?;
	validation::validate_create_value(&value)?;
	let body: SandboxCreate = validation::from_value(value.clone())?;
	validation::validate_create(&body)?;
	let view = if let Some(mesh) = state.mesh.clone().filter(|mesh| mesh.mesh_enabled())
		&& !crate::mesh::proxy::is_mesh_hop(&headers)
	{
		mesh
			.create_sandbox(value.as_object().cloned().unwrap_or_default())
			.await
			.map_err(ApiError::from)?
	} else {
		engine_call(state.engine.clone(), |engine| engine.create(body)).await?
	};
	let view = annotate_local_mesh_node(&state, view);
	Ok((StatusCode::CREATED, Json(view)).into_response())
}

#[utoipa::path(get, path = "/v1/sandboxes", responses((status = 200, body = SandboxesBody)))]
async fn list_sandboxes(
	State(state): State<ApiState>,
	Query(query): Query<ListQuery>,
) -> ApiResult<Json<SandboxesBody>> {
	let tags = validation::parse_tag_filters(&query.tag)?;
	let rows = engine_call(state.engine.clone(), |engine| engine.list(tags)).await?;
	let sandboxes = rows
		.into_iter()
		.map(|row| annotate_local_mesh_node(&state, row))
		.collect();
	Ok(Json(SandboxesBody { sandboxes }))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}", params(("id" = String, Path)), responses((status = 200), (status = 404, body = ErrorBody)))]
async fn get_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
	let view = engine_call(state.engine.clone(), move |engine| engine.get(&id)).await?;
	Ok(Json(annotate_local_mesh_node(&state, view)))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/stop", params(("id" = String, Path)), request_body = Option<StopBody>, responses((status = 200)))]
async fn stop_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let raw = body_bytes(request).await?;
	let body = if raw.is_empty() {
		None
	} else {
		Some(
			serde_json::from_slice::<StopBody>(&raw)
				.map_err(|err| ApiError::invalid(format!("invalid JSON body: {err}")))?,
		)
	};
	let view = engine_call(state.engine.clone(), move |engine| {
		engine.stop_with_returncode(&id, body.and_then(|body| body.returncode))
	})
	.await?;
	Ok(Json(view))
}

#[utoipa::path(delete, path = "/v1/sandboxes/{id}", params(("id" = String, Path)), responses((status = 200)))]
async fn remove_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
	let view = engine_call(state.engine.clone(), move |engine| engine.remove(&id)).await?;
	Ok(Json(view))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/terminate", params(("id" = String, Path)), responses((status = 200)))]
async fn terminate_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
	let view = engine_call(state.engine.clone(), move |engine| engine.terminate(&id, "api")).await?;
	Ok(Json(view))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/pause", params(("id" = String, Path)), responses((status = 200)))]
async fn pause_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
	let view = engine_call(state.engine.clone(), move |engine| engine.pause(&id)).await?;
	Ok(Json(view))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/resume", params(("id" = String, Path)), responses((status = 200)))]
async fn resume_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
	let view = engine_call(state.engine.clone(), move |engine| engine.resume(&id)).await?;
	Ok(Json(view))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/extend", params(("id" = String, Path)), request_body = ExtendBody, responses((status = 200), (status = 400, body = ErrorBody)))]
async fn extend_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let body: ExtendBody = json_body(request).await?;
	validation::validate_extend(&body)?;
	let view =
		engine_call(state.engine.clone(), move |engine| engine.extend(&id, body.secs)).await?;
	Ok(Json(view))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/metrics", params(("id" = String, Path)), responses((status = 200)))]
async fn sandbox_metrics(
	State(state): State<ApiState>,
	Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
	let view = engine_call(state.engine.clone(), move |engine| engine.metrics(&id)).await?;
	Ok(Json(view))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/logs", params(("id" = String, Path)), responses((status = 200)))]
async fn logs_sandbox(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	Query(query): Query<LogsQuery>,
) -> ApiResult<Response> {
	if query.follow {
		let rx = engine_call(state.engine.clone(), move |engine| engine.logs_follow(&id)).await?;
		let rx = flume_to_unbounded(rx);
		let stream = stream::unfold(rx, |mut rx| async move {
			match rx.recv().await {
				Some(chunk) => {
					let payload = json!({"stream":"console","data":String::from_utf8_lossy(&chunk)});
					let data = format!("data: {}\n\n", compact_json(&payload));
					Some((Ok::<Bytes, Infallible>(Bytes::from(data)), rx))
				},
				None => None,
			}
		});
		return Ok(Response::builder()
			.header(header::CONTENT_TYPE, "text/event-stream")
			.body(Body::from_stream(stream))
			.unwrap_or_else(|err| {
				ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
					.into_response()
			}));
	}
	let data = engine_call(state.engine.clone(), move |engine| engine.logs(&id)).await?;
	Ok(Response::builder()
		.header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
		.body(Body::from(data))
		.unwrap_or_else(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
				.into_response()
		}))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/exec", params(("id" = String, Path)), request_body = ExecBody, responses((status = 200, body = ExecCaptureBody)))]
async fn exec_capture(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	request: Request,
) -> ApiResult<Json<ExecCaptureBody>> {
	let body: ExecBody = json_body(request).await?;
	let req = validation::validate_exec(&body)?;
	let capture =
		engine_call(state.engine.clone(), move |engine| engine.exec_capture(&id, req)).await?;
	Ok(Json(ExecCaptureBody {
		exit:       capture.exit,
		stdout_b64: B64.encode(capture.stdout),
		stderr_b64: B64.encode(capture.stderr),
	}))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/exec", params(("id" = String, Path)), responses((status = 101, description = "exec websocket")))]
async fn exec_ws(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	ws: WebSocketUpgrade,
) -> impl IntoResponse {
	let engine = state.engine.clone();
	ws.on_upgrade(move |socket| ws::exec_socket(engine, id, socket))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/attach", params(("id" = String, Path)), responses((status = 101, description = "attach websocket")))]
async fn attach_ws(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	ws: WebSocketUpgrade,
) -> impl IntoResponse {
	let engine = state.engine.clone();
	ws.on_upgrade(move |socket| ws::attach_socket(engine, id, socket))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/files", params(("id" = String, Path), ("path" = String, Query)), responses((status = 200)))]
async fn file_read(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	Query(query): Query<PathQuery>,
) -> ApiResult<Response> {
	let path = query.path;
	let data = engine_call(state.engine.clone(), move |engine| engine.file_read(&id, &path)).await?;
	Ok(Response::builder()
		.header(header::CONTENT_TYPE, "application/octet-stream")
		.body(Body::from(data))
		.unwrap_or_else(|err| {
			ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", err.to_string())
				.into_response()
		}))
}

#[utoipa::path(put, path = "/v1/sandboxes/{id}/files", params(("id" = String, Path), ("path" = String, Query)), responses((status = 200, body = OkBody)))]
async fn file_write(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	Query(query): Query<PathQuery>,
	request: Request,
) -> ApiResult<Json<OkBody>> {
	let path = query.path;
	let data = body_bytes(request).await?;
	engine_call(state.engine.clone(), move |engine| engine.file_write(&id, &path, &data)).await?;
	Ok(Json(OkBody { ok: true }))
}

#[utoipa::path(delete, path = "/v1/sandboxes/{id}/files", params(("id" = String, Path), ("path" = String, Query)), responses((status = 200, body = OkBody)))]
async fn file_delete(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	Query(query): Query<FileQuery>,
) -> ApiResult<Json<OkBody>> {
	engine_call(state.engine.clone(), move |engine| {
		engine.file_delete(&id, &query.path, query.recursive)
	})
	.await?;
	Ok(Json(OkBody { ok: true }))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/files/list", params(("id" = String, Path), ("path" = String, Query)), responses((status = 200)))]
async fn file_list(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	Query(query): Query<PathQuery>,
) -> ApiResult<Json<Value>> {
	let value =
		engine_call(state.engine.clone(), move |engine| engine.file_list(&id, &query.path)).await?;
	Ok(Json(value))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/files/stat", params(("id" = String, Path), ("path" = String, Query)), responses((status = 200)))]
async fn file_stat(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	Query(query): Query<PathQuery>,
) -> ApiResult<Json<Value>> {
	let value =
		engine_call(state.engine.clone(), move |engine| engine.file_stat(&id, &query.path)).await?;
	Ok(Json(value))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/network", params(("id" = String, Path)), responses((status = 200)))]
async fn network_get(
	State(state): State<ApiState>,
	Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
	let value = engine_call(state.engine.clone(), move |engine| engine.network_get(&id)).await?;
	Ok(Json(value))
}

#[utoipa::path(put, path = "/v1/sandboxes/{id}/network", params(("id" = String, Path)), request_body = NetworkBody, responses((status = 200)))]
async fn network_set(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let body: NetworkBody = json_body(request).await?;
	validation::validate_network(&body)?;
	let value =
		engine_call(state.engine.clone(), move |engine| engine.network_set(&id, body)).await?;
	Ok(Json(value))
}

#[utoipa::path(get, path = "/v1/sandboxes/{id}/tunnels", params(("id" = String, Path)), responses((status = 200)))]
async fn tunnels(State(state): State<ApiState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
	let value = engine_call(state.engine.clone(), move |engine| engine.tunnels(&id)).await?;
	Ok(Json(value))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/migrate", params(("id" = String, Path)), request_body = MigrateBody, responses((status = 200)))]
async fn migrate(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let body: MigrateBody = json_body(request).await?;
	if body.target.is_empty() {
		return Err(ApiError::invalid("target node id is required"));
	}
	let value = if let Some(mesh) = state.mesh.clone() {
		mesh
			.migrate_sandbox(id, body.target)
			.await
			.map_err(ApiError::from)?
	} else {
		engine_call(state.engine.clone(), move |engine| engine.migrate(&id, &body.target)).await?
	};
	Ok(Json(value))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/snapshots", params(("id" = String, Path)), request_body = SnapshotBody, responses((status = 200)))]
async fn snapshot(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let body: SnapshotBody = json_body(request).await?;
	let value =
		engine_call(state.engine.clone(), move |engine| engine.snapshot(&id, body.name, body.stop))
			.await?;
	Ok(Json(value))
}

#[utoipa::path(post, path = "/v1/sandboxes/{id}/snapshots/fs", params(("id" = String, Path)), request_body = SnapshotFsBody, responses((status = 200)))]
async fn snapshot_fs(
	State(state): State<ApiState>,
	Path(id): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let body: SnapshotFsBody = json_body(request).await?;
	let value =
		engine_call(state.engine.clone(), move |engine| engine.snapshot_fs(&id, body.name)).await?;
	Ok(Json(value))
}

#[utoipa::path(get, path = "/v1/snapshots", responses((status = 200, body = SnapshotsBody)))]
async fn list_snapshots(State(state): State<ApiState>) -> ApiResult<Json<SnapshotsBody>> {
	let snapshots = engine_call(state.engine.clone(), |engine| engine.snapshots()).await?;
	Ok(Json(SnapshotsBody { snapshots }))
}

#[utoipa::path(post, path = "/v1/snapshots/{name}/restore", params(("name" = String, Path)), request_body = RestoreBody, responses((status = 201)))]
async fn restore_snapshot(
	State(state): State<ApiState>,
	Path(name): Path<String>,
	request: Request,
) -> ApiResult<Response> {
	let body: RestoreBody = json_body(request).await?;
	let value = engine_call(state.engine.clone(), move |engine| engine.restore(&name, body)).await?;
	Ok((StatusCode::CREATED, Json(value)).into_response())
}

#[utoipa::path(post, path = "/v1/snapshots/{name}/fork", params(("name" = String, Path)), request_body = ForkBody, responses((status = 200)))]
async fn fork_snapshot(
	State(state): State<ApiState>,
	Path(name): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let body: ForkBody = json_body(request).await?;
	validation::validate_fork(&body)?;
	let value = engine_call(state.engine.clone(), move |engine| engine.fork(&name, body)).await?;
	Ok(Json(value))
}

#[utoipa::path(get, path = "/v1/volumes", responses((status = 200, body = VolumesBody)))]
async fn volume_list(State(state): State<ApiState>) -> ApiResult<Json<VolumesBody>> {
	let volumes = engine_call(state.engine.clone(), |engine| engine.volume_list()).await?;
	Ok(Json(VolumesBody { volumes }))
}

#[utoipa::path(put, path = "/v1/volumes/{name}", params(("name" = String, Path)), responses((status = 200, body = OkBody)))]
async fn volume_create(
	State(state): State<ApiState>,
	Path(name): Path<String>,
) -> ApiResult<Json<OkBody>> {
	engine_call(state.engine.clone(), move |engine| engine.volume_create(&name)).await?;
	Ok(Json(OkBody { ok: true }))
}

#[utoipa::path(delete, path = "/v1/volumes/{name}", params(("name" = String, Path)), responses((status = 200, body = OkBody)))]
async fn volume_delete(
	State(state): State<ApiState>,
	Path(name): Path<String>,
) -> ApiResult<Json<OkBody>> {
	engine_call(state.engine.clone(), move |engine| engine.volume_delete(&name)).await?;
	Ok(Json(OkBody { ok: true }))
}

#[utoipa::path(get, path = "/v1/pools", responses((status = 200)))]
async fn pool_list(State(state): State<ApiState>) -> ApiResult<Json<Value>> {
	let value = engine_call(state.engine.clone(), |engine| engine.pool_list()).await?;
	Ok(Json(value))
}

#[utoipa::path(put, path = "/v1/pools/{reference}", params(("reference" = String, Path)), request_body = PoolPutBody, responses((status = 200)))]
async fn pool_set(
	State(state): State<ApiState>,
	Path(reference): Path<String>,
	request: Request,
) -> ApiResult<Json<Value>> {
	let body: PoolPutBody = json_body(request).await?;
	let value =
		engine_call(state.engine.clone(), move |engine| engine.pool_set(&reference, body)).await?;
	Ok(Json(value))
}

#[utoipa::path(delete, path = "/v1/pools/{reference}", params(("reference" = String, Path)), responses((status = 200, body = OkBody)))]
async fn pool_delete(
	State(state): State<ApiState>,
	Path(reference): Path<String>,
) -> ApiResult<Json<OkBody>> {
	engine_call(state.engine.clone(), move |engine| engine.pool_delete(&reference)).await?;
	Ok(Json(OkBody { ok: true }))
}

#[utoipa::path(get, path = "/v1/shell", responses((status = 101, description = "shell websocket")))]
async fn shell_ws(
	State(state): State<ApiState>,
	Query(params): Query<std::collections::HashMap<String, String>>,
	ws: WebSocketUpgrade,
) -> impl IntoResponse {
	let engine = state.engine.clone();
	let params = ws::shell_params_from_query(&params);
	ws.on_upgrade(move |socket| ws::shell_socket(engine, params, socket))
}

async fn template_archive(
	State(state): State<MeshRouteState>,
	Path(digest): Path<String>,
	headers: HeaderMap,
) -> ApiResult<Response> {
	require_template_auth(&state, &headers)?;
	let archive = tokio::task::spawn_blocking(move || transfer::archive_for_digest(&digest))
		.await
		.map_err(join_error)?
		.map_err(ApiError::from)?;
	template_archive_response(archive).await
}

async fn template_metadata_archive(
	State(state): State<MeshRouteState>,
	Path(digest): Path<String>,
	headers: HeaderMap,
) -> ApiResult<Response> {
	require_template_auth(&state, &headers)?;
	let archive =
		tokio::task::spawn_blocking(move || transfer::metadata_archive_for_digest(&digest))
			.await
			.map_err(join_error)?
			.map_err(ApiError::from)?;
	template_archive_response(archive).await
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

async fn template_archive_response(archive: transfer::TemplateArchive) -> ApiResult<Response> {
	let bytes = tokio::fs::read(&archive.path)
		.await
		.map_err(EngineError::from)?;
	let _ = tokio::fs::remove_file(&archive.path).await;
	Response::builder()
		.header(header::CONTENT_TYPE, archive.media_type)
		.header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", archive.filename))
		.body(Body::from(bytes))
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

#[utoipa::path(method(get, post, put, delete, patch, head, options), path = "/v1/sandboxes/{id}/ports/{port}/{rest}", params(("id" = String, Path), ("port" = u16, Path), ("rest" = String, Path)), responses((status = 200)))]
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

#[utoipa::path(get, path = "/v1/sandboxes/{id}/ports/{port}/ws/{rest}", params(("id" = String, Path), ("port" = u16, Path), ("rest" = String, Path)), responses((status = 101, description = "port websocket")))]
async fn proxy_ws(
	State(state): State<ApiState>,
	Path((id, port, rest)): Path<(String, u16, String)>,
	uri: Uri,
	ws_upgrade: WebSocketUpgrade,
) -> ApiResult<impl IntoResponse> {
	proxy_ws_inner(state, id, port, rest, uri, ws_upgrade).await
}

fn annotate_local_mesh_node(state: &ApiState, view: Value) -> Value {
	let Some(mesh) = state.mesh.as_ref().filter(|mesh| mesh.mesh_enabled()) else {
		return view;
	};
	let mut object = match view {
		Value::Object(object) => object,
		other => return other,
	};
	object
		.entry("node".to_owned())
		.or_insert_with(|| Value::String(mesh.local_node_id()));
	Value::Object(object)
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
	path.starts_with("/v1") || path.starts_with("/healthz") || path.starts_with("/metrics")
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

async fn json_body<T>(request: Request) -> ApiResult<T>
where
	T: serde::de::DeserializeOwned,
{
	let value = json_value(request).await?;
	validation::from_value(value)
}

async fn json_value(request: Request) -> ApiResult<Value> {
	let bytes = body_bytes(request).await?;
	serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid("invalid request"))
}

async fn body_bytes(request: Request) -> ApiResult<Bytes> {
	to_bytes(request.into_body(), BODY_LIMIT)
		.await
		.map_err(|_| ApiError::invalid("invalid request"))
}

fn compact_json(value: &Value) -> String {
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
fn flume_to_unbounded<T: Send + 'static>(
	rx: flume::Receiver<T>,
) -> tokio::sync::mpsc::UnboundedReceiver<T> {
	let (tx, rx_out) = tokio::sync::mpsc::unbounded_channel();
	std::thread::spawn(move || {
		while let Ok(item) = rx.recv() {
			if tx.send(item).is_err() {
				break;
			}
		}
	});
	rx_out
}

#[derive(OpenApi)]
#[openapi(
	paths(
		healthz,
		openapi_json_route,
		info,
		metrics,
		events,
		create_sandbox,
		list_sandboxes,
		get_sandbox,
		stop_sandbox,
		remove_sandbox,
		terminate_sandbox,
		pause_sandbox,
		resume_sandbox,
		extend_sandbox,
		sandbox_metrics,
		logs_sandbox,
		exec_capture,
		exec_ws,
		attach_ws,
		file_read,
		file_write,
		file_delete,
		file_list,
		file_stat,
		network_get,
		network_set,
		tunnels,
		migrate,
		snapshot,
		snapshot_fs,
		list_snapshots,
		restore_snapshot,
		fork_snapshot,
		volume_list,
		volume_create,
		volume_delete,
		pool_list,
		pool_set,
		pool_delete,
		shell_ws,
		proxy_http,
		proxy_ws
	),
	components(schemas(
		ErrorBody,
		HealthBody,
		SandboxesBody,
		SnapshotsBody,
		VolumesBody,
		ExecCaptureBody,
		OkBody,
		ExecBody,
		ExtendBody,
		NetworkBody,
		MigrateBody,
		SnapshotBody,
		SnapshotFsBody,
		RestoreBody,
		ForkBody,
		PoolPutBody
	)),
	tags((name = "vmon", description = "Vibemon v1 API"))
)]
struct ApiDoc;

pub fn openapi_json() -> String {
	ApiDoc::openapi()
		.to_pretty_json()
		.unwrap_or_else(|err| format!(r#"{{"openapi":"3.1.0","error":"{err}"}}"#))
}
