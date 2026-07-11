//! Peer-facing mesh HTTP routes.
//!
//! These handlers preserve the Python mesh wire shape for node-to-node routes:
//! FastAPI-style `{"detail": ...}` error bodies, bearer authentication, and
//! `X-Vmon-Mesh-Hop: 1` on peer-only calls.  The concrete state/lease/record
//! stores are provided by sibling modules through the traits below.

use std::{
	sync::Arc,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
	Json, Router,
	body::{Body, to_bytes},
	extract::{Path, Request, State},
	http::{HeaderMap, HeaderValue, StatusCode, header},
	response::{IntoResponse, Response},
	routing::{get, post},
};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::gossip::{self, JsonObject, MeshError, MeshResult, PeerHttpClient};

const BODY_LIMIT: usize = 16 * 1024 * 1024;
const ALLOWED_HA: &[&str] = &["async", "async+rerun", "off", "rerun"];

pub type RouteResult<T> = std::result::Result<T, MeshRouteError>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeCapsWire {
	pub vcpus:   u32,
	pub mem_mib: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeshSetupResult {
	pub blob:      String,
	pub node_id:   String,
	pub advertise: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerMember {
	pub node_id:   String,
	pub advertise: String,
	pub healthy:   bool,
}

#[derive(Clone, Debug)]
pub struct MetadataPull {
	pub template:        String,
	pub remote_page_url: String,
	pub digest:          String,
}

#[derive(Clone, Debug)]
pub struct MigratePrepareWire {
	pub digest:       String,
	pub snapshot_dir: String,
	pub params:       JsonObject,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateRecordWire {
	pub sid:             String,
	pub params:          JsonObject,
	pub owner:           String,
	pub epoch:           i64,
	pub idempotency_key: String,
	pub ha:              String,
	pub restart_policy:  String,
	pub created_at:      f64,
}

impl CreateRecordWire {
	pub fn from_value(value: Value) -> MeshResult<Self> {
		let Value::Object(mut data) = value else {
			return Err(MeshError::invalid("record body must be an object"));
		};
		let sid = take_string(&mut data, "sid");
		if sid.is_empty() {
			return Err(MeshError::invalid("record sid is required"));
		}
		let Some(Value::Object(params)) = data.remove("params") else {
			return Err(MeshError::invalid("record params must be an object"));
		};
		let owner = take_string(&mut data, "owner");
		if owner.is_empty() {
			return Err(MeshError::invalid("record owner is required"));
		}
		let idempotency_key = take_string(&mut data, "idempotency_key");
		if idempotency_key.is_empty() {
			return Err(MeshError::invalid("record idempotency_key is required"));
		}
		let ha = nonempty_string(data.remove("ha")).unwrap_or_else(|| "off".to_owned());
		if !ALLOWED_HA.contains(&ha.as_str()) {
			return Err(MeshError::invalid(format!("invalid ha tier {ha:?}")));
		}
		let restart_policy = nonempty_string(data.remove("restart_policy"))
			.unwrap_or_else(|| restart_policy_for_ha(&ha).to_owned());
		Ok(Self {
			sid,
			params,
			owner,
			epoch: to_i64(data.remove("epoch")).unwrap_or(0),
			idempotency_key,
			ha,
			restart_policy,
			created_at: to_f64(data.remove("created_at")).unwrap_or_else(unix_now),
		})
	}

	pub fn to_value(&self) -> Value {
		json!({
			"sid": self.sid,
			"params": self.params,
			"owner": self.owner,
			"epoch": self.epoch,
			"idempotency_key": self.idempotency_key,
			"ha": self.ha,
			"restart_policy": self.restart_policy,
			"created_at": self.created_at,
		})
	}
}

#[derive(Clone, Debug)]
pub struct MeshRouteConfig {
	pub replicas:          usize,
	pub create_timeout:    Duration,
	pub default_ha_meshed: String,
	pub default_ha_local:  String,
}

impl Default for MeshRouteConfig {
	fn default() -> Self {
		Self {
			replicas:          1,
			create_timeout:    Duration::from_mins(2),
			default_ha_meshed: "async".to_owned(),
			default_ha_local:  "off".to_owned(),
		}
	}
}

#[derive(Clone)]
pub struct MeshRouteState {
	pub inbound_token:  Arc<str>,
	pub outbound_token: Arc<str>,
	pub transport:      PeerHttpClient,
	pub mesh:           Arc<dyn MeshControl>,
	pub engine:         Arc<dyn MeshEngine>,
	pub leases:         Arc<dyn MeshLeaseManager>,
	pub records:        Arc<dyn MeshRecordStore>,
	pub replicas:       Arc<dyn MeshReplicaStore>,
	pub transfer:       Arc<dyn MeshTemplateTransfer>,
	pub config:         MeshRouteConfig,
}

impl MeshRouteState {
	#[allow(clippy::too_many_arguments, reason = "explicit service wiring keeps mesh seams visible")]
	pub fn new(
		inbound_token: impl Into<String>,
		outbound_token: impl Into<String>,
		transport: PeerHttpClient,
		mesh: Arc<dyn MeshControl>,
		engine: Arc<dyn MeshEngine>,
		leases: Arc<dyn MeshLeaseManager>,
		records: Arc<dyn MeshRecordStore>,
		replicas: Arc<dyn MeshReplicaStore>,
		transfer: Arc<dyn MeshTemplateTransfer>,
		config: MeshRouteConfig,
	) -> Self {
		Self {
			inbound_token: Arc::from(inbound_token.into()),
			outbound_token: Arc::from(outbound_token.into()),
			transport,
			mesh,
			engine,
			leases,
			records,
			replicas,
			transfer,
			config,
		}
	}

	pub async fn idempotent_sandbox_create(
		&self,
		key: String,
		params: JsonObject,
	) -> MeshResult<Value> {
		idempotent_create(self, CreateKind::Sandbox, key, params).await
	}

	pub async fn idempotent_run_create(
		&self,
		key: String,
		mut params: JsonObject,
	) -> MeshResult<Value> {
		params.insert("detach".to_owned(), Value::Bool(true));
		idempotent_create(self, CreateKind::Run, key, params).await
	}

	pub async fn idempotent_restore_create(
		&self,
		key: String,
		mut params: JsonObject,
	) -> MeshResult<Value> {
		params.insert("detach".to_owned(), Value::Bool(true));
		idempotent_create(self, CreateKind::Restore, key, params).await
	}
}

/// Membership/state seam supplied by `mesh::state` + `mesh::place`.
pub trait MeshControl: Send + Sync {
	fn node_id(&self) -> String;
	fn advertise(&self) -> String;
	fn default_advertise(&self) -> String;
	fn caps(&self) -> NodeCapsWire;
	fn enabled(&self) -> bool;
	fn peers(&self) -> Vec<PeerMember>;
	fn expected_members(&self) -> usize;
	fn quorum_needed(&self) -> usize;

	fn setup(
		&self,
		advertise: String,
		region: String,
		caps: Option<NodeCapsWire>,
	) -> MeshResult<MeshSetupResult>;
	fn join(&self, blob: String, advertise: Option<String>, region: String) -> MeshResult<Value>;
	fn leave(&self) -> MeshResult<()>;
	fn status(&self) -> MeshResult<Value>;
	fn register(&self, state: JsonObject) -> MeshResult<Value>;
	fn heartbeat(&self, state: JsonObject, known: Vec<Value>) -> MeshResult<Value>;
	fn depart(&self, node_id: &str);
	fn ensure_heartbeat(&self) {}
	fn request_replication(&self) {}

	fn is_peer_healthy(&self, node_id: &str) -> bool;
	fn migration_target(&self, node_id: &str) -> MeshResult<PeerMember>;
	fn replica_targets(&self, sid: &str, k: usize) -> Vec<String>;
	fn peer_url(&self, node_id: &str) -> Option<String>;
	fn find_template_provider(&self, key: &str) -> Option<(String, String)>;
	fn request_template_key(&self, params: &JsonObject) -> Option<String>;
	fn has_local_template_key(&self, key: &str) -> bool;

	fn authoritative_owner(&self, sid: &str) -> Option<(String, i64)>;
	fn local_epoch(&self, sid: &str) -> i64;
	fn owner_of(&self, sid: &str) -> Option<String>;
	fn record_owner(&self, sid: &str, node_id: &str, epoch: i64);
	fn broadcast_owner(&self, sid: &str, node_id: &str, epoch: i64);
	fn forget_owner(&self, sid: &str);
	fn mark_unhealthy(&self, node_id: &str);

	fn pinned_local(&self, params: &JsonObject) -> bool;
	fn place(&self, params: &JsonObject) -> MeshResult<String>;
	fn coordinator_for(&self, key: &str) -> String;
	fn idem_owner(&self, key: &str) -> Option<String>;
	fn idem_pin(&self, key: &str, owner: &str) -> String;
	fn idem_unpin(&self, key: &str);
	fn worker_begin(&self, key: &str) -> bool;
	fn worker_end(&self, key: &str);
}

/// Engine seam used by peer routes.  Implementations may block internally;
/// route code awaits the returned futures so callers can wrap sync work in
/// `spawn_blocking`.
pub trait MeshEngine: Send + Sync {
	fn owned_ids(&self) -> MeshResult<Vec<String>>;
	fn list_views(&self) -> MeshResult<Vec<Value>>;
	fn has_sandbox(&self, sid: &str) -> bool;
	fn get_view(&self, sid: &str) -> MeshResult<Value>;
	fn checkpoint_age_sec(&self, sid: &str) -> Option<f64>;
	fn find_by_idempotency_key(&self, key: &str) -> MeshResult<Option<Value>>;
	fn record_idempotency(&self, sid: &str, key: &str) -> MeshResult<()>;
	fn record_volume_leases(&self, sid: &str, leases: Vec<Value>) -> MeshResult<()>;

	fn create_sandbox(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>>;
	fn run_detached(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>>;
	fn restore_detached(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>>;
	fn restore_from_template(
		&self,
		params: JsonObject,
		template_dir: String,
		quorum_ok: bool,
	) -> BoxFuture<'_, MeshResult<Value>>;
	fn migrate_prepare(&self, sid: String) -> BoxFuture<'_, MeshResult<MigratePrepareWire>>;
	fn migrate_abort(
		&self,
		sid: String,
		snapshot_dir: String,
		digest: String,
		params: JsonObject,
	) -> BoxFuture<'_, MeshResult<()>>;
	fn migrate_commit(
		&self,
		sid: String,
		snapshot_dir: String,
		digest: String,
	) -> BoxFuture<'_, MeshResult<()>>;
}

pub trait MeshLeaseManager: Send + Sync {
	fn vote_grant(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<JsonObject>;
	fn vote_renew(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<JsonObject>;
	fn vote_release(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
	) -> MeshResult<JsonObject>;

	fn acquire_writable_volume_leases(
		&self,
		params: JsonObject,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<Vec<Value>>>;
	fn release_leases(&self, leases: Vec<Value>) -> BoxFuture<'_, MeshResult<()>>;
	fn release_record_volume_leases(&self, sid: String) -> BoxFuture<'_, MeshResult<()>>;
}

pub trait MeshRecordStore: Send + Sync {
	fn get(&self, sid: &str) -> MeshResult<Option<CreateRecordWire>>;
	fn put(&self, record: CreateRecordWire) -> MeshResult<()>;
	fn remove(&self, sid: &str) -> MeshResult<()>;

	fn put_if_newer(&self, record: CreateRecordWire) -> MeshResult<bool> {
		let should_put = match self.get(&record.sid)? {
			Some(existing) => record.epoch >= existing.epoch,
			None => true,
		};
		if should_put {
			self.put(record)?;
		}
		Ok(should_put)
	}
}

#[derive(Clone, Debug)]
pub struct ReplicaRecordWire {
	pub sid:           String,
	pub digest:        String,
	pub source_node:   String,
	pub snapshot_dir:  String,
	pub params:        JsonObject,
	pub needs_secrets: bool,
}

pub trait MeshReplicaStore: Send + Sync {
	fn get(&self, sid: &str) -> MeshResult<Option<ReplicaRecordWire>>;
	fn put(
		&self,
		sid: String,
		digest: String,
		source_node: String,
		snapshot_dir: String,
		params: JsonObject,
	) -> MeshResult<()>;
	fn list(&self) -> MeshResult<Vec<String>>;
	fn remove(&self, sid: &str) -> MeshResult<()>;
}

pub trait MeshTemplateTransfer: Send + Sync {
	fn pull_template<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>>;

	fn pull_template_metadata<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<MetadataPull>>;
}

#[derive(Debug)]
pub struct MeshRouteError {
	status:       StatusCode,
	detail:       Value,
	authenticate: bool,
}

impl MeshRouteError {
	pub fn detail(status: StatusCode, detail: impl Into<String>) -> Self {
		Self { status, detail: Value::String(detail.into()), authenticate: false }
	}

	pub fn coded(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
		Self {
			status,
			detail: json!({"code": code.into(), "message": message.into()}),
			authenticate: false,
		}
	}

	pub fn invalid(message: impl Into<String>) -> Self {
		Self::coded(StatusCode::BAD_REQUEST, "invalid", message)
	}

	pub fn unauthorized() -> Self {
		Self {
			status:       StatusCode::UNAUTHORIZED,
			detail:       Value::String("unauthorized".to_owned()),
			authenticate: true,
		}
	}
}

impl From<MeshError> for MeshRouteError {
	fn from(value: MeshError) -> Self {
		Self::coded(value.http_status(), value.code, value.message)
	}
}

impl IntoResponse for MeshRouteError {
	fn into_response(self) -> Response {
		let mut response = (self.status, Json(json!({"detail": self.detail}))).into_response();
		if self.authenticate {
			response
				.headers_mut()
				.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
		}
		response
	}
}

pub fn router(state: MeshRouteState) -> Router {
	Router::new()
		.route("/v1/mesh/setup", post(mesh_setup))
		.route("/v1/mesh/join", post(mesh_join))
		.route("/v1/mesh/leave", post(mesh_leave))
		.route("/v1/mesh/status", get(mesh_status))
		.route("/v1/mesh/members", post(mesh_members))
		.route("/v1/mesh/heartbeat", post(mesh_heartbeat))
		.route("/v1/mesh/depart", post(mesh_depart))
		.route("/v1/mesh/reachable/{node_id}", get(mesh_reachable))
		.route("/v1/mesh/locate/{sandbox_id}", get(mesh_locate))
		.route("/v1/mesh/owner", post(mesh_owner))
		.route("/v1/mesh/lease/grant", post(mesh_lease_grant))
		.route("/v1/mesh/lease/renew", post(mesh_lease_renew))
		.route("/v1/mesh/lease/release", post(mesh_lease_release))
		.route("/v1/mesh/migrate/receive", post(mesh_migrate_receive))
		.route("/v1/mesh/replica/receive", post(mesh_replica_receive))
		.route("/v1/mesh/replica/list", get(mesh_replica_list))
		.route("/v1/mesh/record/put", post(mesh_record_put))
		.route("/v1/mesh/record/remove", post(mesh_record_remove))
		.route("/v1/mesh/idem/sandboxes/coordinate", post(mesh_idem_sandbox_coordinate))
		.route("/v1/mesh/idem/sandboxes/work", post(mesh_idem_sandbox_work))
		.route("/v1/mesh/idem/run/coordinate", post(mesh_idem_run_coordinate))
		.route("/v1/mesh/idem/run/work", post(mesh_idem_run_work))
		.route("/v1/mesh/idem/restore/coordinate", post(mesh_idem_restore_coordinate))
		.route("/v1/mesh/idem/restore/work", post(mesh_idem_restore_work))
		.with_state(state)
}

async fn mesh_setup(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, false)?;
	let body = json_object(request).await?;
	let current_caps = state.mesh.caps();
	let caps = if body.contains_key("max_vcpus") || body.contains_key("max_mem_mib") {
		Some(NodeCapsWire {
			vcpus:   to_u32(body.get("max_vcpus").cloned()).unwrap_or(current_caps.vcpus),
			mem_mib: to_u64(body.get("max_mem_mib").cloned()).unwrap_or(current_caps.mem_mib),
		})
	} else {
		None
	};
	let advertise = string_or(body.get("advertise"), state.mesh.default_advertise());
	let region = string_or(body.get("region"), String::new());
	let setup = map_mesh(state.mesh.setup(advertise, region, caps))?;
	state.mesh.ensure_heartbeat();
	Ok(Json(json!({
		"blob": setup.blob,
		"node_id": setup.node_id,
		"advertise": setup.advertise,
	})))
}

async fn mesh_join(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, false)?;
	let body = json_object(request).await?;
	let blob = string_or(body.get("blob"), String::new());
	let advertise = body
		.get("advertise")
		.and_then(value_string)
		.filter(|value| !value.is_empty());
	let region = string_or(body.get("region"), String::new());
	let status = map_mesh(state.mesh.join(blob, advertise, region))?;
	state.mesh.ensure_heartbeat();
	Ok(Json(status))
}

async fn mesh_leave(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, false)?;
	let body = json_object_or_empty(request).await?;
	let mut drained = Vec::new();
	if py_truthy(body.get("drain")) && state.mesh.enabled() {
		for sid in map_mesh(state.engine.owned_ids())? {
			let target = drain_target(&state);
			let Some(target) = target else {
				drained
					.push(json!({"sandbox": sid, "status": "skipped", "reason": "no compatible peer"}));
				continue;
			};
			match migrate_sandbox_to(&state, sid.clone(), target.clone()).await {
				Ok(_) => drained.push(json!({"sandbox": sid, "status": "migrated", "target": target})),
				Err(err) => drained.push(json!({
					"sandbox": sid,
					"status": "failed",
					"reason": route_error_detail(&err),
				})),
			}
		}
	}
	map_mesh(state.mesh.leave())?;
	Ok(Json(json!({"ok": true, "drained": drained})))
}

async fn mesh_status(
	State(state): State<MeshRouteState>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, false)?;
	let mut status = value_object(map_mesh(state.mesh.status())?)?;
	let mut self_status = status
		.remove("self")
		.and_then(|value| match value {
			Value::Object(object) => Some(object),
			_ => None,
		})
		.unwrap_or_default();
	self_status.insert("sandboxes".to_owned(), Value::Array(sandbox_status_rows(&state)?));
	status.insert("self".to_owned(), Value::Object(self_status));
	status.insert("replicas_held".to_owned(), json!(map_mesh(state.replicas.list())?.len()));
	Ok(Json(Value::Object(status)))
}

async fn mesh_members(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	Ok(Json(map_mesh(state.mesh.register(body))?))
}

async fn mesh_heartbeat(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let state_body = match body.remove("state") {
		Some(Value::Object(object)) => object,
		_ => JsonObject::new(),
	};
	let known = match body.remove("known") {
		Some(Value::Array(items)) => items,
		_ => Vec::new(),
	};
	Ok(Json(map_mesh(state.mesh.heartbeat(state_body, known))?))
}

async fn mesh_depart(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	state
		.mesh
		.depart(&string_or(body.get("node_id"), String::new()));
	Ok(Json(json!({"ok": true})))
}

async fn mesh_reachable(
	State(state): State<MeshRouteState>,
	Path(node_id): Path<String>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, true)?;
	Ok(Json(json!({"reachable": state.mesh.is_peer_healthy(&node_id)})))
}

async fn mesh_locate(
	State(state): State<MeshRouteState>,
	Path(sandbox_id): Path<String>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, true)?;
	if state.engine.has_sandbox(&sandbox_id) {
		let (owner, epoch) = state
			.mesh
			.authoritative_owner(&sandbox_id)
			.unwrap_or_else(|| (state.mesh.node_id(), state.mesh.local_epoch(&sandbox_id)));
		return Ok(Json(json!({"owner": owner, "epoch": epoch})));
	}
	if let Some((owner, epoch)) = state.mesh.authoritative_owner(&sandbox_id) {
		return Ok(Json(json!({"owner": owner, "epoch": epoch})));
	}
	if let Some(record) = map_mesh(state.records.get(&sandbox_id))? {
		state
			.mesh
			.record_owner(&sandbox_id, &record.owner, record.epoch);
		return Ok(Json(json!({"owner": record.owner, "epoch": record.epoch})));
	}
	Err(MeshRouteError::detail(StatusCode::NOT_FOUND, "unknown sandbox"))
}

async fn mesh_owner(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let sid = string_or(body.get("sid"), String::new());
	let node_id = string_or(body.get("node_id"), String::new());
	let epoch = to_i64(body.get("epoch").cloned()).unwrap_or(0);
	if !sid.is_empty() && !node_id.is_empty() {
		state.mesh.record_owner(&sid, &node_id, epoch);
	}
	Ok(Json(json!({"ok": true})))
}

async fn mesh_lease_grant(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	lease_vote(state, request, LeaseOp::Grant).await
}

async fn mesh_lease_renew(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	lease_vote(state, request, LeaseOp::Renew).await
}

async fn mesh_lease_release(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	lease_vote(state, request, LeaseOp::Release).await
}

async fn mesh_migrate_receive(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let digest = take_string(&mut body, "digest");
	let source_url = take_string(&mut body, "source_url");
	let params = match body.remove("params") {
		Some(Value::Object(params)) => params,
		_ => JsonObject::new(),
	};
	let name = string_or(params.get("name"), String::new());
	if digest.is_empty() || source_url.is_empty() || name.is_empty() {
		return Err(MeshRouteError::detail(
			StatusCode::UNPROCESSABLE_ENTITY,
			"digest, source_url, and params.name are required",
		));
	}
	if state.engine.has_sandbox(&name) {
		return Err(MeshRouteError::coded(
			StatusCode::CONFLICT,
			"conflict",
			format!("sandbox {} already exists on the target node", py_repr(&name)),
		));
	}
	let installed = state
		.transfer
		.pull_template(state.transport.client(), source_url, digest, state.outbound_token.to_string())
		.await
		.map_err(|err| {
			MeshRouteError::coded(
				StatusCode::BAD_GATEWAY,
				"unreachable",
				format!("failed to pull migration checkpoint: {err}"),
			)
		})?;
	let epoch = to_i64(body.remove("epoch")).unwrap_or(0);
	let lease_records = map_mesh(
		state
			.leases
			.acquire_writable_volume_leases(params.clone(), epoch)
			.await,
	)?;
	let restored = match state
		.engine
		.restore_from_template(params, installed, true)
		.await
	{
		Ok(view) => view,
		Err(err) => {
			let _ = state.leases.release_leases(lease_records).await;
			return Err(MeshRouteError::from(err));
		},
	};
	let sid = sid_from_view(&restored).unwrap_or_else(|| name.clone());
	map_mesh(state.engine.record_volume_leases(&sid, lease_records))?;
	state.mesh.record_owner(&name, &state.mesh.node_id(), epoch);
	Ok(Json(restored))
}

async fn mesh_replica_receive(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let digest = take_string(&mut body, "digest");
	let source_url = take_string(&mut body, "source_url");
	let source_node = take_string(&mut body, "source_node");
	let params = match body.remove("params") {
		Some(Value::Object(params)) => params,
		_ => JsonObject::new(),
	};
	let name = string_or(params.get("name"), String::new());
	if digest.is_empty() || source_url.is_empty() || source_node.is_empty() || name.is_empty() {
		return Err(MeshRouteError::detail(
			StatusCode::UNPROCESSABLE_ENTITY,
			"digest, source_url, source_node, and params.name are required",
		));
	}
	if state.engine.has_sandbox(&name) {
		return Ok(Json(json!({"ok": true, "skipped": "owner"})));
	}
	if let Some(current) = map_mesh(state.replicas.get(&name))?
		&& current.digest == digest
	{
		return Ok(Json(json!({"ok": true, "skipped": "current"})));
	}
	let installed = state
		.transfer
		.pull_template(
			state.transport.client(),
			source_url,
			digest.clone(),
			state.outbound_token.to_string(),
		)
		.await
		.map_err(|err| {
			MeshRouteError::coded(
				StatusCode::BAD_GATEWAY,
				"unreachable",
				format!("failed to pull replica checkpoint: {err}"),
			)
		})?;
	map_mesh(
		state
			.replicas
			.put(name, digest, source_node, installed, params),
	)?;
	Ok(Json(json!({"ok": true})))
}

async fn mesh_replica_list(
	State(state): State<MeshRouteState>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, false)?;
	Ok(Json(json!({"sids": map_mesh(state.replicas.list())?})))
}

async fn mesh_record_put(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let record = CreateRecordWire::from_value(Value::Object(body)).map_err(MeshRouteError::from)?;
	let accepted = map_mesh(state.records.put_if_newer(record.clone()))?;
	if accepted {
		state
			.mesh
			.record_owner(&record.sid, &record.owner, record.epoch);
	}
	Ok(Json(json!({"ok": true})))
}

async fn mesh_record_remove(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let sid = string_or(body.get("sid"), String::new());
	if !sid.is_empty() {
		map_mesh(state.records.remove(&sid))?;
		state.mesh.forget_owner(&sid);
	}
	Ok(Json(json!({"ok": true})))
}

async fn mesh_idem_sandbox_coordinate(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Sandbox, IdemRole::Coordinate).await
}

async fn mesh_idem_sandbox_work(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Sandbox, IdemRole::Work).await
}

async fn mesh_idem_run_coordinate(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Run, IdemRole::Coordinate).await
}

async fn mesh_idem_run_work(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Run, IdemRole::Work).await
}

async fn mesh_idem_restore_coordinate(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Restore, IdemRole::Coordinate).await
}

async fn mesh_idem_restore_work(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Restore, IdemRole::Work).await
}

#[derive(Clone, Copy)]
enum LeaseOp {
	Grant,
	Renew,
	Release,
}

async fn lease_vote(
	state: MeshRouteState,
	request: Request<Body>,
	op: LeaseOp,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let volume = string_or(body.get("volume"), String::new());
	let holder_node = string_or(body.get("holder_node"), String::new());
	let epoch = to_i64(body.get("epoch").cloned()).unwrap_or(0);
	let ttl = to_f64(body.get("ttl").cloned()).unwrap_or(0.0);
	let mut decision = match op {
		LeaseOp::Grant => state.leases.vote_grant(volume, holder_node, epoch, ttl),
		LeaseOp::Renew => state.leases.vote_renew(volume, holder_node, epoch, ttl),
		LeaseOp::Release => state.leases.vote_release(volume, holder_node, epoch),
	}
	.map_err(|err| {
		MeshRouteError::coded(StatusCode::UNPROCESSABLE_ENTITY, "invalid", err.message)
	})?;
	if matches!(op, LeaseOp::Release) {
		let released = decision
			.get("granted")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		decision.insert("released".to_owned(), Value::Bool(released));
	}
	Ok(Json(Value::Object(decision)))
}

#[derive(Clone, Copy)]
enum IdemRole {
	Coordinate,
	Work,
}

async fn idem_route(
	state: MeshRouteState,
	request: Request<Body>,
	kind: CreateKind,
	role: IdemRole,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let (key, mut params) = idem_payload(body).map_err(MeshRouteError::from)?;
	if matches!(kind, CreateKind::Run | CreateKind::Restore) {
		params.insert("detach".to_owned(), Value::Bool(true));
	}
	let value = match role {
		IdemRole::Coordinate => coordinate_create(&state, kind, &key, params).await,
		IdemRole::Work => worker_create(&state, kind, &key, params).await,
	};
	Ok(Json(map_mesh(value)?))
}

#[derive(Clone, Copy)]
enum CreateKind {
	Sandbox,
	Run,
	Restore,
}

impl CreateKind {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Sandbox => "sandbox",
			Self::Run => "run",
			Self::Restore => "restore",
		}
	}

	const fn work_path(self) -> &'static str {
		match self {
			Self::Sandbox => "/v1/mesh/idem/sandboxes/work",
			Self::Run => "/v1/mesh/idem/run/work",
			Self::Restore => "/v1/mesh/idem/restore/work",
		}
	}

	const fn coordinate_path(self) -> &'static str {
		match self {
			Self::Sandbox => "/v1/mesh/idem/sandboxes/coordinate",
			Self::Run => "/v1/mesh/idem/run/coordinate",
			Self::Restore => "/v1/mesh/idem/restore/coordinate",
		}
	}

	const fn no_owner_message(self) -> &'static str {
		match self {
			Self::Sandbox => "no reachable owner for idempotent create",
			Self::Run => "no reachable owner for idempotent run",
			Self::Restore => "no reachable owner for idempotent restore",
		}
	}
}

async fn idempotent_create(
	state: &MeshRouteState,
	kind: CreateKind,
	idem_key: String,
	params: JsonObject,
) -> MeshResult<Value> {
	if !state.mesh.enabled() {
		return worker_create(state, kind, &idem_key, params).await;
	}
	if state.mesh.pinned_local(&params) {
		let prepared = prepare_create_params(state, params, kind)?;
		let view = worker_create(state, kind, &idem_key, prepared.clone()).await?;
		let sid = sid_from_view(&view).unwrap_or_default();
		return commit_create_record(
			state,
			kind,
			view,
			state.mesh.node_id(),
			state.mesh.local_epoch(&sid),
			idem_key,
			prepared,
		)
		.await;
	}
	let coordinator = state.mesh.coordinator_for(&idem_key);
	if coordinator == state.mesh.node_id() {
		return coordinate_create(state, kind, &idem_key, params).await;
	}
	let Some(peer_url) = state.mesh.peer_url(&coordinator) else {
		state.mesh.mark_unhealthy(&coordinator);
		return coordinate_create(state, kind, &idem_key, params).await;
	};
	let payload = json!({"key": idem_key, "params": params});
	match state
		.transport
		.post(&peer_url, kind.coordinate_path(), &payload, Some(state.config.create_timeout))
		.await
	{
		Ok(view) => {
			let value = Value::Object(view);
			if let Some(sid) = sid_from_view(&value)
				&& scatter_locate(state, &sid).await?.is_none()
			{
				state.mesh.record_owner(&sid, &coordinator, 0);
			}
			Ok(value)
		},
		Err(err) if err.code == "unreachable" => {
			state.mesh.mark_unhealthy(&coordinator);
			coordinate_create(state, kind, &idem_key, value_object(payload["params"].clone())?).await
		},
		Err(err) => Err(err),
	}
}

async fn coordinate_create(
	state: &MeshRouteState,
	kind: CreateKind,
	idem_key: &str,
	params: JsonObject,
) -> MeshResult<Value> {
	let params = prepare_create_params(state, params, kind)?;
	if let Some(existing) = state.engine.find_by_idempotency_key(idem_key)? {
		let sid = sid_from_view(&existing).unwrap_or_default();
		return commit_create_record(
			state,
			kind,
			view_with_ha(existing, &params),
			state.mesh.node_id(),
			state.mesh.local_epoch(&sid),
			idem_key.to_owned(),
			params,
		)
		.await;
	}
	for _ in 0..std::cmp::max(1, state.mesh.peers().len() + 1) {
		let pinned = state.mesh.idem_owner(idem_key);
		let owner = match pinned {
			Some(owner) => owner,
			None => state.mesh.place(&params)?,
		};
		let owner = state.mesh.idem_pin(idem_key, &owner);
		if owner == state.mesh.node_id() {
			let view = worker_create(state, kind, idem_key, params.clone()).await?;
			return commit_create_record(state, kind, view, owner, 0, idem_key.to_owned(), params)
				.await;
		}
		let Some(peer_url) = state.mesh.peer_url(&owner) else {
			state.mesh.mark_unhealthy(&owner);
			state.mesh.idem_unpin(idem_key);
			continue;
		};
		let payload = json!({"key": idem_key, "params": params});
		match state
			.transport
			.post(&peer_url, kind.work_path(), &payload, Some(state.config.create_timeout))
			.await
		{
			Ok(view) => {
				return commit_create_record(
					state,
					kind,
					Value::Object(view),
					owner,
					0,
					idem_key.to_owned(),
					params,
				)
				.await;
			},
			Err(err) if err.code == "unreachable" => {
				state.mesh.mark_unhealthy(&owner);
				state.mesh.idem_unpin(idem_key);
			},
			Err(err) => return Err(err),
		}
	}
	Err(MeshError::unreachable(kind.no_owner_message()))
}

async fn worker_create(
	state: &MeshRouteState,
	kind: CreateKind,
	idem_key: &str,
	params: JsonObject,
) -> MeshResult<Value> {
	let params = prepare_create_params(state, params, kind)?;
	if let Some(existing) = state.engine.find_by_idempotency_key(idem_key)? {
		return Ok(existing);
	}
	if !state.mesh.worker_begin(idem_key) {
		let deadline =
			Instant::now() + std::cmp::min(Duration::from_secs(5), state.config.create_timeout);
		while Instant::now() < deadline {
			tokio::time::sleep(Duration::from_millis(50)).await;
			if let Some(existing) = state.engine.find_by_idempotency_key(idem_key)? {
				return Ok(existing);
			}
		}
		return Err(MeshError::conflict("idempotent create already in progress"));
	}
	let result = async {
		if let Some(existing) = state.engine.find_by_idempotency_key(idem_key)? {
			return Ok(existing);
		}
		let name = string_or(params.get("name"), String::new());
		if !name.is_empty()
			&& (state.engine.has_sandbox(&name)
				|| state.mesh.owner_of(&name).is_some()
				|| scatter_locate(state, &name).await?.is_some())
		{
			return Err(MeshError::conflict(format!("sandbox {} already exists", py_repr(&name))));
		}
		let view = local_create_view(state, kind, params.clone()).await?;
		if let Some(sid) = sid_from_view(&view) {
			state.engine.record_idempotency(&sid, idem_key)?;
			return Ok(view_with_ha(state.engine.get_view(&sid)?, &params));
		}
		Ok(view)
	}
	.await;
	state.mesh.worker_end(idem_key);
	result
}

async fn local_create_view(
	state: &MeshRouteState,
	kind: CreateKind,
	mut params: JsonObject,
) -> MeshResult<Value> {
	match kind {
		CreateKind::Sandbox => {
			if state.mesh.enabled()
				&& let Some(key) = state.mesh.request_template_key(&params)
				&& !state.mesh.has_local_template_key(&key)
				&& let Some((peer_url, digest)) = state.mesh.find_template_provider(&key)
			{
				match state
					.transfer
					.pull_template_metadata(
						state.transport.client(),
						peer_url.clone(),
						digest.clone(),
						state.outbound_token.to_string(),
					)
					.await
				{
					Ok(installed) => {
						params.insert("template".to_owned(), Value::String(installed.template));
						params.insert(
							"remote_page_url".to_owned(),
							Value::String(installed.remote_page_url),
						);
						params.insert(
							"remote_page_token".to_owned(),
							Value::String(state.outbound_token.to_string()),
						);
						params.insert("remote_page_digest".to_owned(), Value::String(installed.digest));
					},
					Err(_) => {
						if let Ok(installed) = state
							.transfer
							.pull_template(
								state.transport.client(),
								peer_url,
								digest,
								state.outbound_token.to_string(),
							)
							.await
						{
							params.insert("template".to_owned(), Value::String(installed));
						}
					},
				}
			}
			let lease_records = state
				.leases
				.acquire_writable_volume_leases(params.clone(), 0)
				.await?;
			let record = match state.engine.create_sandbox(params).await {
				Ok(view) => view,
				Err(err) => {
					let _ = state.leases.release_leases(lease_records).await;
					return Err(err);
				},
			};
			if let Some(sid) = sid_from_view(&record) {
				state.engine.record_volume_leases(&sid, lease_records)?;
				state.mesh.request_replication();
			}
			Ok(record)
		},
		CreateKind::Run => {
			params.insert("detach".to_owned(), Value::Bool(true));
			let view = state.engine.run_detached(params.clone()).await?;
			Ok(view_with_ha(view, &params))
		},
		CreateKind::Restore => {
			params.insert("detach".to_owned(), Value::Bool(true));
			let view = state.engine.restore_detached(params.clone()).await?;
			Ok(view_with_ha(view, &params))
		},
	}
}

async fn commit_create_record(
	state: &MeshRouteState,
	kind: CreateKind,
	view: Value,
	owner: String,
	epoch: i64,
	idem_key: String,
	params: JsonObject,
) -> MeshResult<Value> {
	let Some(sid) = sid_from_view(&view) else {
		return Ok(view);
	};
	if !state.mesh.enabled() {
		return Ok(view_with_ha(view, &params));
	}
	let record = make_create_record(sid, owner, epoch, idem_key, params, kind);
	replicate_record(state, record.clone(), true).await?;
	state.mesh.request_replication();
	Ok(apply_view_detail(view, &record))
}

async fn replicate_record(
	state: &MeshRouteState,
	record: CreateRecordWire,
	require_quorum: bool,
) -> MeshResult<()> {
	state.records.put(record.clone())?;
	state
		.mesh
		.record_owner(&record.sid, &record.owner, record.epoch);
	let mut acks = 1usize;
	let mut live_peer_acks_needed = 0usize;
	for peer in state.mesh.peers() {
		if peer.healthy {
			live_peer_acks_needed += 1;
		}
		let posted = state
			.transport
			.post(
				&peer.advertise,
				"/v1/mesh/record/put",
				&record.to_value(),
				Some(state.config.create_timeout),
			)
			.await;
		if posted.is_ok() {
			acks += 1;
		}
	}
	let expected = state.mesh.expected_members();
	let needed = if expected <= 2 {
		1 + live_peer_acks_needed
	} else {
		state.mesh.quorum_needed()
	};
	if require_quorum && acks < needed {
		return Err(MeshError::with_code(
			format!(
				"create record for {} replicated to {acks}/{needed} required members",
				py_repr(&record.sid)
			),
			"record_unreplicated",
		));
	}
	Ok(())
}

pub(crate) async fn migrate_sandbox_to(
	state: &MeshRouteState,
	sandbox_id: String,
	target: String,
) -> RouteResult<Value> {
	let peer = map_mesh(state.mesh.migration_target(&target))?;
	let prep = map_mesh(state.engine.migrate_prepare(sandbox_id.clone()).await)?;
	let epoch = state
		.mesh
		.authoritative_owner(&sandbox_id)
		.map_or(0, |(_, epoch)| epoch)
		+ 1;
	let digest = prep.digest.clone();
	let snapshot_dir = prep.snapshot_dir.clone();
	let params = prep.params.clone();
	let receive = json!({
		"digest": digest,
		"source_url": state.mesh.advertise(),
		"params": params,
		"epoch": epoch,
	});
	let view = match state
		.transport
		.post(
			&peer.advertise,
			"/v1/mesh/migrate/receive",
			&receive,
			Some(state.config.create_timeout),
		)
		.await
	{
		Ok(view) => Value::Object(view),
		Err(err) => {
			let _ = state
				.engine
				.migrate_abort(sandbox_id.clone(), snapshot_dir.clone(), digest.clone(), params.clone())
				.await;
			return Err(MeshRouteError::coded(
				StatusCode::BAD_GATEWAY,
				"unreachable",
				format!("migration to {target} failed (source restored): {}", err.message),
			));
		},
	};
	state.mesh.broadcast_owner(&sandbox_id, &target, epoch);
	if let Some(mut record) = map_mesh(state.records.get(&sandbox_id))? {
		record.owner = target.clone();
		record.epoch = epoch;
		map_mesh(replicate_record(state, record, false).await)?;
	}
	map_mesh(
		state
			.leases
			.release_record_volume_leases(sandbox_id.clone())
			.await,
	)?;
	map_mesh(
		state
			.engine
			.migrate_commit(sandbox_id, snapshot_dir, digest)
			.await,
	)?;
	Ok(view)
}

fn drain_target(state: &MeshRouteState) -> Option<String> {
	for peer in state.mesh.peers() {
		if state.mesh.migration_target(&peer.node_id).is_ok() {
			return Some(peer.node_id);
		}
	}
	None
}

async fn scatter_locate(state: &MeshRouteState, sandbox_id: &str) -> MeshResult<Option<String>> {
	for peer in state.mesh.peers() {
		let path = format!("/v1/mesh/locate/{sandbox_id}");
		let Ok(response) = state.transport.get(&peer.advertise, &path, None).await else {
			continue;
		};
		let owner = response
			.get("owner")
			.and_then(value_string)
			.filter(|owner| !owner.is_empty());
		if let Some(owner) = owner {
			let epoch = to_i64(response.get("epoch").cloned()).unwrap_or(0);
			state.mesh.record_owner(sandbox_id, &owner, epoch);
			return Ok(Some(owner));
		}
	}
	if let Some(record) = state.records.get(sandbox_id)? {
		state
			.mesh
			.record_owner(sandbox_id, &record.owner, record.epoch);
		return Ok(Some(record.owner));
	}
	Ok(None)
}

fn make_create_record(
	sid: String,
	owner: String,
	epoch: i64,
	idempotency_key: String,
	mut params: JsonObject,
	kind: CreateKind,
) -> CreateRecordWire {
	params.remove("idempotency_key");
	params.insert("_kind".to_owned(), Value::String(kind.as_str().to_owned()));
	let ha = string_or(params.get("ha"), "off".to_owned());
	let restart_policy =
		string_or(params.get("restart_policy"), restart_policy_for_ha(&ha).to_owned());
	params.insert("restart_policy".to_owned(), Value::String(restart_policy.clone()));
	CreateRecordWire {
		sid,
		params,
		owner,
		epoch,
		idempotency_key,
		ha,
		restart_policy,
		created_at: unix_now(),
	}
}

fn apply_view_detail(view: Value, record: &CreateRecordWire) -> Value {
	let mut object = match view {
		Value::Object(object) => object,
		other => return other,
	};
	object.insert("ha".to_owned(), Value::String(record.ha.clone()));
	object.insert("node".to_owned(), Value::String(record.owner.clone()));
	object.insert("restart_policy".to_owned(), Value::String(record.restart_policy.clone()));
	Value::Object(object)
}

fn view_with_ha(view: Value, params: &JsonObject) -> Value {
	let mut object = match view {
		Value::Object(object) => object,
		other => return other,
	};
	object.insert("ha".to_owned(), Value::String(string_or(params.get("ha"), "off".to_owned())));
	object.insert(
		"restart_policy".to_owned(),
		Value::String(string_or(params.get("restart_policy"), "none".to_owned())),
	);
	Value::Object(object)
}

fn prepare_create_params(
	state: &MeshRouteState,
	mut params: JsonObject,
	kind: CreateKind,
) -> MeshResult<JsonObject> {
	params.remove("idempotency_key");
	if state.mesh.enabled() && py_truthy(params.get("fs_dir")) {
		return Err(MeshError::invalid(
			"host-local fs_dir cannot be placed or protected; use a volume",
		));
	}
	apply_ha_defaults(state, &mut params)?;
	params.insert("_kind".to_owned(), Value::String(kind.as_str().to_owned()));
	Ok(params)
}

fn apply_ha_defaults(state: &MeshRouteState, params: &mut JsonObject) -> MeshResult<()> {
	let raw = params.get("ha").and_then(value_string).unwrap_or_default();
	let ha = if raw.is_empty() {
		if state.mesh.enabled() {
			state.config.default_ha_meshed.clone()
		} else {
			state.config.default_ha_local.clone()
		}
	} else {
		raw
	};
	if !ALLOWED_HA.contains(&ha.as_str()) {
		return Err(MeshError::invalid("ha must be one of: async, async+rerun, off, rerun"));
	}
	params.insert("ha".to_owned(), Value::String(ha.clone()));
	params.insert("restart_policy".to_owned(), Value::String(restart_policy_for_ha(&ha).to_owned()));
	Ok(())
}

fn restart_policy_for_ha(ha: &str) -> &'static str {
	if ha.contains("rerun") {
		"rerun"
	} else {
		"none"
	}
}

fn idem_payload(mut body: JsonObject) -> MeshResult<(String, JsonObject)> {
	let key = take_string(&mut body, "key").trim().to_owned();
	if key.is_empty() {
		return Err(MeshError::new("missing idempotency key"));
	}
	let params = match body.remove("params") {
		Some(Value::Object(mut params)) => {
			params.remove("idempotency_key");
			params
		},
		_ => return Err(MeshError::new("missing idempotency params")),
	};
	Ok((key, params))
}

fn sandbox_status_rows(state: &MeshRouteState) -> MeshResult<Vec<Value>> {
	let mut rows = Vec::new();
	for view in state.engine.list_views()? {
		let Value::Object(object) = view else {
			continue;
		};
		if object.get("status").and_then(value_string).as_deref() == Some("terminated") {
			continue;
		}
		let sid = object
			.get("id")
			.or_else(|| object.get("name"))
			.and_then(value_string)
			.unwrap_or_default();
		let ha = object
			.get("ha")
			.and_then(value_string)
			.unwrap_or_else(|| "off".to_owned());
		let restart_policy = object
			.get("restart_policy")
			.and_then(value_string)
			.unwrap_or_else(|| "none".to_owned());
		let checkpoint_age = if ha == "off" {
			None
		} else {
			state.engine.checkpoint_age_sec(&sid)
		};
		let replicas = if ha == "off" {
			Vec::new()
		} else {
			state.mesh.replica_targets(&sid, state.config.replicas)
		};
		rows.push(json!({
			"id": sid,
			"ha": ha,
			"restart_policy": restart_policy,
			"checkpoint_age_sec": checkpoint_age,
			"replicas": replicas,
		}));
	}
	Ok(rows)
}

fn require_mesh_auth(
	state: &MeshRouteState,
	headers: &HeaderMap,
	require_hop: bool,
) -> RouteResult<()> {
	let expected = state.inbound_token.trim();
	if !expected.is_empty() {
		let supplied = gossip::bearer_token(headers).unwrap_or_default();
		if !token_matches(supplied, expected) {
			return Err(MeshRouteError::unauthorized());
		}
	}
	if require_hop && !gossip::has_mesh_hop(headers) {
		return Err(MeshRouteError::invalid("missing mesh hop header"));
	}
	Ok(())
}

fn token_matches(supplied: &str, expected: &str) -> bool {
	expected
		.split(',')
		.map(str::trim)
		.filter(|token| !token.is_empty())
		.any(|token| token == supplied)
}

async fn json_object(request: Request<Body>) -> RouteResult<JsonObject> {
	let bytes = to_bytes(request.into_body(), BODY_LIMIT)
		.await
		.map_err(|err| MeshRouteError::invalid(format!("failed to read request body: {err}")))?;
	if bytes.is_empty() {
		return Err(MeshRouteError::invalid("request body must be a JSON object"));
	}
	match serde_json::from_slice::<Value>(&bytes) {
		Ok(Value::Object(object)) => Ok(object),
		Ok(_) => Err(MeshRouteError::invalid("request body must be a JSON object")),
		Err(err) => Err(MeshRouteError::invalid(format!("invalid JSON body: {err}"))),
	}
}

async fn json_object_or_empty(request: Request<Body>) -> RouteResult<JsonObject> {
	let bytes = to_bytes(request.into_body(), BODY_LIMIT)
		.await
		.map_err(|err| MeshRouteError::invalid(format!("failed to read request body: {err}")))?;
	if bytes.is_empty() {
		return Ok(JsonObject::new());
	}
	match serde_json::from_slice::<Value>(&bytes) {
		Ok(Value::Object(object)) => Ok(object),
		Ok(_) => Err(MeshRouteError::invalid("request body must be a JSON object")),
		Err(err) => Err(MeshRouteError::invalid(format!("invalid JSON body: {err}"))),
	}
}

fn value_object(value: Value) -> MeshResult<JsonObject> {
	match value {
		Value::Object(object) => Ok(object),
		_ => Err(MeshError::invalid("expected JSON object")),
	}
}

fn map_mesh<T>(value: MeshResult<T>) -> RouteResult<T> {
	value.map_err(MeshRouteError::from)
}

fn route_error_detail(error: &MeshRouteError) -> String {
	match &error.detail {
		Value::String(text) => text.clone(),
		Value::Object(object) => object
			.get("message")
			.and_then(Value::as_str)
			.unwrap_or_else(|| error.status.canonical_reason().unwrap_or("mesh error"))
			.to_owned(),
		_ => error
			.status
			.canonical_reason()
			.unwrap_or("mesh error")
			.to_owned(),
	}
}

fn sid_from_view(value: &Value) -> Option<String> {
	let object = value.as_object()?;
	object
		.get("id")
		.or_else(|| object.get("name"))
		.and_then(value_string)
		.filter(|sid| !sid.is_empty())
}

fn take_string(object: &mut JsonObject, key: &str) -> String {
	nonempty_string(object.remove(key)).unwrap_or_default()
}

fn string_or(value: Option<&Value>, default: String) -> String {
	value.and_then(value_string).unwrap_or(default)
}

fn value_string(value: &Value) -> Option<String> {
	match value {
		Value::String(text) => Some(text.clone()),
		Value::Number(number) => Some(number.to_string()),
		Value::Bool(value) => Some(value.to_string()),
		_ => None,
	}
}

fn nonempty_string(value: Option<Value>) -> Option<String> {
	value
		.as_ref()
		.and_then(value_string)
		.filter(|text| !text.is_empty())
}

fn to_i64(value: Option<Value>) -> Option<i64> {
	match value? {
		Value::Number(number) => number
			.as_i64()
			.or_else(|| number.as_u64().and_then(|n| i64::try_from(n).ok())),
		Value::String(text) => text.parse().ok(),
		Value::Bool(value) => Some(i64::from(value)),
		_ => None,
	}
}

fn to_u32(value: Option<Value>) -> Option<u32> {
	to_u64(value).and_then(|value| u32::try_from(value).ok())
}

fn to_u64(value: Option<Value>) -> Option<u64> {
	match value? {
		Value::Number(number) => number
			.as_u64()
			.or_else(|| number.as_i64().and_then(|n| u64::try_from(n).ok())),
		Value::String(text) => text.parse().ok(),
		Value::Bool(value) => Some(u64::from(value)),
		_ => None,
	}
}

fn to_f64(value: Option<Value>) -> Option<f64> {
	match value? {
		Value::Number(number) => number.as_f64(),
		Value::String(text) => text.parse().ok(),
		Value::Bool(value) => Some(if value { 1.0 } else { 0.0 }),
		_ => None,
	}
}

fn py_truthy(value: Option<&Value>) -> bool {
	match value {
		None | Some(Value::Null) => false,
		Some(Value::Bool(value)) => *value,
		Some(Value::Number(number)) => number.as_f64().is_some_and(|value| value != 0.0),
		Some(Value::String(text)) => !text.is_empty(),
		Some(Value::Array(items)) => !items.is_empty(),
		Some(Value::Object(object)) => !object.is_empty(),
	}
}

fn py_repr(value: &str) -> String {
	format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn unix_now() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}
