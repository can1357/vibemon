//! gRPC surface of the v1 API (proto/vmon/v1/api.proto).
//!
//! Five tonic services implemented against [`EngineApi`], mounted into the
//! axum router next to the kept HTTP routes (healthz, metrics, SSE, WS, ports
//! proxy, static web). Every RPC failure is a gRPC status carrying the stable
//! vmond error code in `vmon-code` response metadata. Requests for sandboxes
//! owned by a mesh peer are re-issued over a tonic channel to the owner.

use std::{
	collections::{HashMap, HashSet},
	io::{Read as _, Seek as _, SeekFrom},
	pin::Pin,
	sync::Arc,
	thread,
};

use axum::http::HeaderMap;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt as _, wrappers::ReceiverStream};
use tonic::{
	Code, Request, Response, Status, Streaming,
	metadata::{MetadataMap, MetadataValue},
	service::Routes,
	transport::{Channel, Endpoint},
};
use vmon_proto::v1 as pb;

use super::{
	error::{ApiError, ApiResult, join_error},
	routes::compact_json,
	state::ApiState,
	validation,
};
use crate::{
	engine::{EngineApi, ExecExit, ExecStream as EngineExecStream},
	image::normalize_oci_arch,
	mesh::{
		proxy::{self, MeshError, MeshPeer, OwnerProxyDecision, OwnerRecord},
		routes::{CreateRecordWire, MeshControl, MeshRecordStore, MeshRouteState, apply_view_detail},
	},
	models::{ExecBody, ExtendBody, ForkBody, NetworkBody, PoolPutBody, RestoreBody, SandboxCreate},
};

/// Mirror of the old REST `BODY_LIMIT`, applied to encode and decode on the
/// server, on every forwarding client, and on `/grpc` bridge frames.
pub(super) const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const MESH_HOP_KEY: &str = "x-vmon-mesh-hop";

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const ARTIFACT_CHUNK_SIZE: usize = 64 * 1024;

fn stream_verified_artifact(
	mut reader: crate::function::artifact::VerifiedArtifactReader,
	offset: u64,
	length: u64,
) -> BoxStream<pb::ArtifactChunk> {
	let (tx, rx) = mpsc::channel(8);
	tokio::task::spawn_blocking(move || {
		if let Err(error) = reader.seek(SeekFrom::Start(offset)) {
			let _ = tx.blocking_send(Err(status_from(&ApiError::from(crate::EngineError::engine(
				error.to_string(),
			)))));
			return;
		}
		if length == 0 {
			let _ = tx.blocking_send(Ok(pb::ArtifactChunk { offset, data: Vec::new(), eof: true }));
			return;
		}
		let mut remaining = length;
		let mut frame_offset = offset;
		while remaining != 0 {
			let frame_len = remaining.min(ARTIFACT_CHUNK_SIZE as u64) as usize;
			let mut data = vec![0_u8; frame_len];
			if let Err(error) = reader.read_exact(&mut data) {
				let _ = tx.blocking_send(Err(status_from(&ApiError::from(
					crate::EngineError::engine(error.to_string()),
				))));
				return;
			}
			remaining -= frame_len as u64;
			let frame = pb::ArtifactChunk { offset: frame_offset, data, eof: remaining == 0 };
			frame_offset += frame_len as u64;
			if tx.blocking_send(Ok(frame)).is_err() {
				return;
			}
		}
	});
	Box::pin(ReceiverStream::new(rx))
}

fn stream_call_event_pages<F>(mut load_page: F, after_sequence: u64) -> BoxStream<pb::CallEvent>
where
	F: FnMut(u64) -> crate::Result<Vec<pb::CallEvent>> + Send + 'static,
{
	const PAGE_SIZE: usize = 10_000;
	let (tx, rx) = mpsc::channel(32);
	tokio::task::spawn_blocking(move || {
		let mut cursor = after_sequence;
		loop {
			let events = match load_page(cursor) {
				Ok(events) => events,
				Err(error) => {
					let _ = tx.blocking_send(Err(status_from(&ApiError::from(error))));
					return;
				},
			};
			let page_len = events.len();
			for event in events {
				if event.sequence <= cursor {
					let _ = tx.blocking_send(Err(Status::internal("call event replay did not advance")));
					return;
				}
				cursor = event.sequence;
				if tx.blocking_send(Ok(event)).is_err() {
					return;
				}
			}
			if page_len < PAGE_SIZE {
				return;
			}
		}
	});
	Box::pin(ReceiverStream::new(rx))
}

fn stream_persisted_call_events(
	domain: Arc<crate::function::FunctionDomain>,
	call_id: String,
	after_sequence: u64,
) -> BoxStream<pb::CallEvent> {
	stream_call_event_pages(
		move |cursor| domain.store().events_after(&call_id, cursor, 10_000),
		after_sequence,
	)
}

enum ArtifactUploadFrame {
	Chunk(Vec<u8>),
	Finish,
	Abort,
}

/// Map an [`ApiError`] onto the contract's gRPC status table with stable code,
/// retryability, and recovery guidance metadata.
pub fn status_from(err: &ApiError) -> Status {
	let code = match err.code() {
		"not_found" => Code::NotFound,
		"invalid" => Code::InvalidArgument,
		"unauthorized" => Code::Unauthenticated,
		"not_running" | "actor_lost" | "unavailable_secret" | "ha_unavailable" => {
			Code::FailedPrecondition
		},
		"busy" | "conflict" => Code::Aborted,
		"checksum" => Code::DataLoss,
		"deadline" => Code::DeadlineExceeded,
		"unsupported" => Code::Unimplemented,
		// engine_error plus mesh transport codes (unreachable, ambiguous, …).
		_ => Code::Unavailable,
	};
	let mut status = Status::new(code, err.message().to_owned());
	if let Ok(value) = MetadataValue::try_from(err.code()) {
		status.metadata_mut().insert("vmon-code", value);
	}
	status.metadata_mut().insert(
		"vmon-retryable",
		MetadataValue::from_static(if err.retryable() { "true" } else { "false" }),
	);
	if let Ok(value) = MetadataValue::try_from(err.action()) {
		status.metadata_mut().insert("vmon-action", value);
	}
	status
}

impl From<ApiError> for Status {
	fn from(err: ApiError) -> Self {
		status_from(&err)
	}
}

/// Build every v1 service with uniform 64 MiB message limits. The same
/// [`Routes`] instance shape serves native h2c and the `/grpc` bridge.
pub(super) fn service(state: ApiState) -> Routes {
	let api = GrpcApi::new(state);
	Routes::default()
		.add_service(
			pb::sandbox_service_server::SandboxServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::snapshot_service_server::SnapshotServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::volume_service_server::VolumeServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::pool_service_server::PoolServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::artifact_service_server::ArtifactServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::function_service_server::FunctionServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::call_service_server::CallServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::actor_service_server::ActorServiceServer::new(api.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::system_service_server::SystemServiceServer::new(api)
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
}

/// The gRPC sub-router mounted into axum for native h2c clients.
pub(super) fn routes(state: ApiState) -> axum::Router {
	service(state).prepare().into_axum_router()
}

#[derive(Clone)]
pub struct GrpcApi {
	state: ApiState,
}

/// Resolved mesh owner hop: a lazy tonic channel plus the peer credential.
struct ForwardTarget {
	channel: Channel,
	token:   Arc<str>,
}

impl ForwardTarget {
	fn request<T>(&self, message: T) -> Request<T> {
		let mut request = Request::new(message);
		forward_metadata(request.metadata_mut(), &self.token);
		request
	}

	fn sandbox_client(&self) -> pb::sandbox_service_client::SandboxServiceClient<Channel> {
		pb::sandbox_service_client::SandboxServiceClient::new(self.channel.clone())
			.max_decoding_message_size(MAX_MESSAGE_SIZE)
			.max_encoding_message_size(MAX_MESSAGE_SIZE)
	}
}

fn forward_metadata(metadata: &mut MetadataMap, token: &str) {
	if !token.is_empty()
		&& let Ok(value) = MetadataValue::try_from(format!("Bearer {token}"))
	{
		metadata.insert("authorization", value);
	}
	metadata.insert(MESH_HOP_KEY, MetadataValue::from_static("1"));
}

fn is_mesh_hop(metadata: &MetadataMap) -> bool {
	metadata.contains_key(MESH_HOP_KEY)
}

/// Adapter exposing [`MeshRouteState`] through the owner-routing traits so the
/// gRPC layer reuses the exact REST forwarding decision (locate scatter,
/// durable record fallback, hop and presence early exits).
struct MeshView {
	state:   MeshRouteState,
	node_id: String,
}

impl proxy::OwnerRouter for MeshView {
	fn mesh_enabled(&self) -> bool {
		self.state.mesh.enabled()
	}

	fn local_node_id(&self) -> &str {
		&self.node_id
	}

	fn owner_of(&self, sandbox_id: &str) -> proxy::MeshResult<Option<String>> {
		self
			.state
			.mesh
			.owner_of(sandbox_id)
			.map_err(|err| proxy::MeshError::new(err.code, err.message))
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		self.state.mesh.peer_url(node_id)
	}

	fn peers(&self) -> Vec<MeshPeer> {
		self
			.state
			.mesh
			.peers()
			.into_iter()
			.map(|peer| MeshPeer { node_id: peer.node_id, advertise: peer.advertise })
			.collect()
	}

	fn record_owner(&self, sandbox_id: &str, owner: &str, epoch: u64) {
		self
			.state
			.mesh
			.record_owner(sandbox_id, owner, i64::try_from(epoch).unwrap_or(i64::MAX));
	}
}

impl proxy::SandboxPresence for MeshView {
	fn has_sandbox(&self, sandbox_id: &str) -> bool {
		self.state.engine.has_sandbox(sandbox_id)
	}
}

impl proxy::RecordOwnerLookup for MeshView {
	fn owner_record(&self, sandbox_id: &str) -> proxy::MeshResult<Option<OwnerRecord>> {
		self
			.state
			.records
			.get(sandbox_id)
			.map_err(|error| proxy::MeshError::new("internal", error.to_string()))
			.map(|record| {
				record.map(|record| OwnerRecord {
					owner: record.owner,
					epoch: u64::try_from(record.epoch).unwrap_or(0),
				})
			})
	}
}

fn mesh_api_error(err: MeshError) -> ApiError {
	ApiError::new(err.status(), err.code, err.message)
}

fn function_ha_admission(
	spec: &pb::FunctionSpec,
	nodes: &[crate::mesh::state::NodeState],
	remote_execution_available: bool,
) -> ApiResult<()> {
	let resources = spec.resources.as_ref();
	let policy = resources
		.and_then(|resources| pb::HighAvailabilityPolicy::try_from(resources.high_availability).ok())
		.unwrap_or(pb::HighAvailabilityPolicy::Unspecified);
	let scope = match policy {
		pb::HighAvailabilityPolicy::Host => "host",
		pb::HighAvailabilityPolicy::Zone => "zone",
		pb::HighAvailabilityPolicy::Unspecified | pb::HighAvailabilityPolicy::None => return Ok(()),
	};
	let required_arch = resources
		.and_then(|resources| pb::CpuArchitecture::try_from(resources.architecture).ok())
		.and_then(|arch| match arch {
			pb::CpuArchitecture::Amd64 => Some("x86_64"),
			pb::CpuArchitecture::Arm64 => Some("aarch64"),
			pb::CpuArchitecture::Unspecified => None,
		});
	let compatible = nodes
		.iter()
		.filter(|node| {
			required_arch.is_none_or(|required| {
				normalize_oci_arch(Some(&node.arch)).as_deref() == Some(required)
			})
		})
		.collect::<Vec<_>>();
	let domains = match policy {
		pb::HighAvailabilityPolicy::Host => compatible
			.iter()
			.map(|node| node.node_id.as_str())
			.filter(|host| !host.is_empty())
			.collect::<HashSet<_>>(),
		pb::HighAvailabilityPolicy::Zone => compatible
			.iter()
			.map(|node| node.region.as_str())
			.filter(|zone| !zone.is_empty())
			.collect::<HashSet<_>>(),
		pb::HighAvailabilityPolicy::Unspecified | pb::HighAvailabilityPolicy::None => {
			unreachable!("non-HA policies returned above")
		},
	};
	if domains.len() < 2 {
		return Err(ApiError::function(
			"ha_unavailable",
			format!(
				"function HA {scope} requires at least 2 compatible mesh {scope}s; found {}; \
				 cross-node function execution is unavailable",
				domains.len()
			),
		));
	}
	if !remote_execution_available {
		return Err(ApiError::function(
			"ha_unavailable",
			format!(
				"function HA {scope} cannot be admitted: cross-node function execution is \
				 unavailable; use HIGH_AVAILABILITY_POLICY_NONE"
			),
		));
	}
	Ok(())
}

/// Relay a forwarded server-stream response as this server's boxed stream,
/// preserving response metadata; per-item statuses (incl. `vmon-code`) flow
/// through untouched.
fn relay_stream<T: Send + 'static>(response: Response<Streaming<T>>) -> Response<BoxStream<T>> {
	let (metadata, stream, extensions) = response.into_parts();
	let stream: BoxStream<T> = Box::pin(stream);
	Response::from_parts(metadata, stream, extensions)
}

/// Forward a sandbox-scoped RPC to the mesh owner when one is resolved. The
/// borrow of `$message` for the id ends before the message is moved into the
/// forwarded request.
macro_rules! forward_to_owner {
	($self:ident, $metadata:ident, $id:expr, $message:ident, $method:ident) => {
		if let Some(target) = $self.forward_target(&$metadata, $id).await? {
			return target
				.sandbox_client()
				.$method(target.request($message))
				.await;
		}
	};
	($self:ident, $metadata:ident, $id:expr, $message:ident, $method:ident,stream) => {
		if let Some(target) = $self.forward_target(&$metadata, $id).await? {
			return target
				.sandbox_client()
				.$method(target.request($message))
				.await
				.map(relay_stream);
		}
	};
}

impl GrpcApi {
	pub const fn new(state: ApiState) -> Self {
		Self { state }
	}

	async fn engine_call<T, F>(&self, call: F) -> Result<T, Status>
	where
		T: Send + 'static,
		F: FnOnce(Arc<dyn EngineApi>) -> crate::Result<T> + Send + 'static,
	{
		let engine = self.state.engine.clone();
		tokio::task::spawn_blocking(move || call(engine))
			.await
			.map_err(|err| status_from(&join_error(err)))?
			.map_err(|err| status_from(&ApiError::from(err)))
	}

	fn admit_function_ha(&self, spec: &pb::FunctionSpec) -> Result<(), Status> {
		let nodes = self
			.state
			.mesh
			.as_ref()
			.map_or_else(Vec::new, |mesh| mesh.function_placement_nodes());
		// Sandbox forwarding has no function-worker equivalent: peer function
		// stores, artifacts, call leases, and result streams are process-local.
		function_ha_admission(spec, &nodes, false).map_err(|error| status_from(&error))
	}

	async fn function_call<T, F>(&self, call: F) -> Result<T, Status>
	where
		T: Send + 'static,
		F: FnOnce(Arc<crate::function::FunctionDomain>) -> crate::Result<T> + Send + 'static,
	{
		let domain = self.state.functions.clone();
		tokio::task::spawn_blocking(move || call(domain))
			.await
			.map_err(|err| status_from(&join_error(err)))?
			.map_err(|err| status_from(&ApiError::from(err)))
	}

	/// Resolve the mesh owner hop for a sandbox-scoped RPC. `None` means serve
	/// locally: mesh disabled, request already a hop, sandbox present here, or
	/// no owner known anywhere.
	async fn forward_target(
		&self,
		metadata: &MetadataMap,
		sandbox_id: &str,
	) -> Result<Option<ForwardTarget>, Status> {
		let Some(mesh) = self.state.mesh.as_ref() else {
			return Ok(None);
		};
		if sandbox_id.is_empty() || !mesh.mesh_enabled() || is_mesh_hop(metadata) {
			return Ok(None);
		}
		let state = mesh.route_state();
		let view = MeshView { node_id: state.mesh.node_id(), state };
		let path = format!("/v1/sandboxes/{sandbox_id}");
		let decision = proxy::owner_proxy_decision(
			&view,
			&view,
			Some(&view),
			view.state.transport.client(),
			&path,
			&HeaderMap::new(),
			&view.state.outbound_token,
		)
		.await
		.map_err(|err| status_from(&mesh_api_error(err)))?;
		match decision {
			OwnerProxyDecision::ServeLocal => Ok(None),
			OwnerProxyDecision::Forward { peer_url, .. } => {
				let endpoint = Endpoint::from_shared(peer_url).map_err(|err| {
					status_from(&mesh_api_error(MeshError::unreachable(format!(
						"invalid peer URL: {err}"
					))))
				})?;
				Ok(Some(ForwardTarget {
					channel: endpoint.connect_lazy(),
					token:   view.state.outbound_token.clone(),
				}))
			},
		}
	}
}

fn function_now_millis() -> u64 {
	use std::time::{SystemTime, UNIX_EPOCH};
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

fn artifact_digest(reference: Option<&pb::ArtifactRef>) -> Result<(Vec<u8>, String), Status> {
	let digest = reference
		.and_then(|reference| reference.digest.as_ref())
		.ok_or_else(|| status_from(&ApiError::invalid("artifact digest is required")))?;
	if digest.algorithm != pb::DigestAlgorithm::Sha256 as i32 || digest.value.len() != 32 {
		return Err(status_from(&ApiError::invalid(
			"artifact digest must be a 32-byte SHA-256 value",
		)));
	}
	Ok((digest.value.clone(), hex::encode(&digest.value)))
}

fn terminal_call_status(status: i32) -> bool {
	matches!(
		pb::CallStatus::try_from(status).unwrap_or(pb::CallStatus::Unspecified),
		pb::CallStatus::Succeeded | pb::CallStatus::Failed | pb::CallStatus::Cancelled
	)
}

fn json_view(value: &Value) -> pb::JsonView {
	pb::JsonView { json: compact_json(value) }
}

/// Convert a proto `ExecStart` into the REST-era `ExecBody` so
/// [`validation::validate_exec`] applies identical rules (empty-cmd checks,
/// timeout finiteness and the 60s cap). The proto surface has no `cwd` alias
/// and an empty env map means "inherit" exactly like the omitted REST field.
fn exec_body(start: pb::ExecStart) -> ExecBody {
	ExecBody {
		cmd:     start.cmd,
		workdir: start.workdir,
		cwd:     None,
		env:     (!start.env.is_empty()).then_some(start.env),
		timeout: start.timeout,
		tty:     start.tty,
	}
}

/// The mandatory first `ExecInput` of `Exec`: a `start` payload naming the
/// sandbox.
fn require_exec_start(frame: Option<pb::ExecInput>) -> Result<pb::ExecStart, Status> {
	match frame.and_then(|frame| frame.input) {
		Some(pb::exec_input::Input::Start(start)) => {
			if start.sandbox_id.is_empty() {
				Err(ApiError::invalid("exec start requires sandbox_id").into())
			} else {
				Ok(start)
			}
		},
		_ => Err(ApiError::invalid("first exec frame must be start").into()),
	}
}

/// The mandatory first `ExecInput` of `Shell`: the shell params JSON document
/// (an object; empty text counts as `{}`).
fn require_shell_params(frame: Option<pb::ExecInput>) -> Result<Value, Status> {
	let Some(pb::exec_input::Input::ShellParamsJson(text)) = frame.and_then(|frame| frame.input)
	else {
		return Err(ApiError::invalid("first shell frame must be shell_params_json").into());
	};
	if text.trim().is_empty() {
		return Ok(json!({}));
	}
	let value: Value = serde_json::from_str(&text)
		.map_err(|err| ApiError::invalid(format!("invalid shell request: {err}")))?;
	if value.is_object() {
		Ok(value)
	} else {
		Err(ApiError::invalid("invalid shell request").into())
	}
}

fn resize_dims(resize: pb::Resize) -> ApiResult<(u16, u16)> {
	Ok((resize_dim(resize.rows)?, resize_dim(resize.cols)?))
}

fn resize_dim(raw: u32) -> ApiResult<u16> {
	u16::try_from(raw)
		.ok()
		.filter(|dim| *dim > 0)
		.ok_or_else(|| ApiError::invalid("resize must be [rows, cols]"))
}

/// Parse a schemaless `body_json` document (`RestoreBody` / `ForkBody` /
/// `PoolPutBody`). Empty text means the empty document, mirroring `"{}"`.
fn parse_body_json<T>(text: &str) -> Result<T, Status>
where
	T: serde::de::DeserializeOwned,
{
	let text = if text.trim().is_empty() { "{}" } else { text };
	let value: Value =
		serde_json::from_str(text).map_err(|_| Status::from(ApiError::invalid("invalid request")))?;
	Ok(validation::from_value(value)?)
}

const fn chunk_output(stream: pb::Stream, data: Vec<u8>) -> pb::ExecOutput {
	pb::ExecOutput {
		output: Some(pb::exec_output::Output::Chunk(pb::Output { stream: stream as i32, data })),
	}
}

const fn exit_output(exit: ExecExit) -> pb::ExecOutput {
	pb::ExecOutput {
		output: Some(pb::exec_output::Output::Exit(pb::Exit {
			code:   exit.code,
			signal: exit.signal,
		})),
	}
}

const fn ready_output(sandbox_id: String) -> pb::ExecOutput {
	pb::ExecOutput { output: Some(pb::exec_output::Output::Ready(pb::Ready { sandbox_id })) }
}

/// Keep the last `tail` newline-separated lines of a console buffer.
fn tail_bytes(mut data: Vec<u8>, tail: u64) -> Vec<u8> {
	if tail == 0 {
		return Vec::new();
	}
	let mut lines = 0_u64;
	let mut start = 0_usize;
	for (index, byte) in data.iter().enumerate().rev() {
		// A trailing newline terminates the last line rather than opening one.
		if *byte == b'\n' && index + 1 != data.len() {
			lines += 1;
			if lines == tail {
				start = index + 1;
				break;
			}
		}
	}
	if lines < tail || start == 0 {
		data
	} else {
		data.split_off(start)
	}
}

fn spawn_chunk_forward(
	rx: flume::Receiver<Vec<u8>>,
	stream: pb::Stream,
	tx: mpsc::Sender<pb::ExecOutput>,
) -> thread::JoinHandle<()> {
	thread::spawn(move || {
		while let Ok(chunk) = rx.recv() {
			if tx.blocking_send(chunk_output(stream, chunk)).is_err() {
				break;
			}
		}
	})
}

fn spawn_exit_forward(
	rx: flume::Receiver<ExecExit>,
	streams: [thread::JoinHandle<()>; 2],
	tx: mpsc::Sender<pb::ExecOutput>,
) {
	thread::spawn(move || {
		let exit = rx.recv();
		for stream in streams {
			let _ = stream.join();
		}
		if let Ok(exit) = exit {
			let _ = tx.blocking_send(exit_output(exit));
		}
	});
}

async fn shell_cleanup(engine: &Arc<dyn EngineApi>, cleanup: Option<String>) {
	if let Some(name) = cleanup {
		let engine = engine.clone();
		let _ = tokio::task::spawn_blocking(move || engine.shell_cleanup(&name)).await;
	}
}

/// Bridge a live engine exec onto the gRPC bidi stream, mirroring the WS pump
/// semantics: Output/Exit frames out, stdin/eof/resize in, `Exit` then clean
/// stream end, and `kill(15)` when the client stream ends or the RPC is
/// cancelled before the exit was observed. Control errors terminate the
/// stream as a status.
fn pump_exec(
	engine: Arc<dyn EngineApi>,
	stream: EngineExecStream,
	mut inbound: Streaming<pb::ExecInput>,
	ready: Option<String>,
	cleanup: Option<String>,
) -> BoxStream<pb::ExecOutput> {
	let EngineExecStream { mut control, stdout, stderr, exit } = stream;
	let (out_tx, out_rx) = mpsc::channel::<Result<pb::ExecOutput, Status>>(32);
	let (events_tx, mut events_rx) = mpsc::channel::<pb::ExecOutput>(32);
	let stdout_forward = spawn_chunk_forward(stdout, pb::Stream::Stdout, events_tx.clone());
	let stderr_forward = spawn_chunk_forward(stderr, pb::Stream::Stderr, events_tx.clone());
	spawn_exit_forward(exit, [stdout_forward, stderr_forward], events_tx);
	tokio::spawn(async move {
		if let Some(name) = ready
			&& out_tx.send(Ok(ready_output(name))).await.is_err()
		{
			shell_cleanup(&engine, cleanup).await;
			return;
		}
		let output_tx = out_tx.clone();
		let output = async move {
			while let Some(event) = events_rx.recv().await {
				let is_exit = matches!(event.output, Some(pb::exec_output::Output::Exit(_)));
				if output_tx.send(Ok(event)).await.is_err() || is_exit {
					break;
				}
			}
		};
		let input = async move {
			loop {
				let Ok(Some(frame)) = inbound.message().await else {
					let _ = control.kill(15);
					break;
				};
				let result = match frame.input {
					Some(pb::exec_input::Input::Stdin(data)) => {
						control.write_stdin(&data).map_err(ApiError::from)
					},
					Some(pb::exec_input::Input::Eof(_)) => control.close_stdin().map_err(ApiError::from),
					Some(pb::exec_input::Input::Resize(resize)) => resize_dims(resize)
						.and_then(|(rows, cols)| control.resize(rows, cols).map_err(ApiError::from)),
					// Mid-stream request payloads are ignored, like the WS pump.
					Some(
						pb::exec_input::Input::Start(_) | pb::exec_input::Input::ShellParamsJson(_),
					)
					| None => Ok(()),
				};
				if let Err(err) = result {
					let _ = out_tx.send(Err(status_from(&err))).await;
					break;
				}
			}
		};
		tokio::select! {
			() = output => {},
			() = input => {},
		}
		shell_cleanup(&engine, cleanup).await;
	});
	Box::pin(ReceiverStream::new(out_rx))
}

#[tonic::async_trait]
impl pb::artifact_service_server::ArtifactService for GrpcApi {
	type GetStream = BoxStream<pb::ArtifactChunk>;

	async fn put(
		&self,
		request: Request<Streaming<pb::PutArtifactRequest>>,
	) -> Result<Response<pb::ArtifactRecord>, Status> {
		let mut stream = request.into_inner();
		let first = stream
			.message()
			.await?
			.ok_or_else(|| status_from(&ApiError::invalid("artifact upload requires a header")))?;
		let Some(pb::put_artifact_request::Frame::Header(header)) = first.frame else {
			return Err(status_from(&ApiError::invalid(
				"artifact upload first frame must be a header",
			)));
		};
		let digest = header
			.expected_digest
			.as_ref()
			.ok_or_else(|| status_from(&ApiError::invalid("expected digest is required")))?;
		if digest.algorithm != pb::DigestAlgorithm::Sha256 as i32 || digest.value.len() != 32 {
			return Err(status_from(&ApiError::invalid(
				"expected digest must be a 32-byte SHA-256 value",
			)));
		}
		let artifact_quota = self.state.config.function_artifact_max_bytes;
		if header.expected_size_bytes > artifact_quota {
			let mut status = Status::resource_exhausted(format!(
				"artifact size {} exceeds configured quota {artifact_quota}",
				header.expected_size_bytes
			));
			status
				.metadata_mut()
				.insert("vmon-code", MetadataValue::from_static("artifact_quota"));
			return Err(status);
		}
		let expected = digest.value.clone();
		let returned_digest = digest.clone();
		let media_type = header.media_type_presence.map(|presence| match presence {
			pb::put_artifact_header::MediaTypePresence::MediaType(value) => value,
		});
		let ttl_millis = header.ttl_millis_presence.map(|presence| match presence {
			pb::put_artifact_header::TtlMillisPresence::TtlMillis(value) => value,
		});
		let domain = self.state.functions.clone();
		let artifacts = domain.artifacts().clone();
		let expected_size = header.expected_size_bytes;
		let writer = tokio::task::spawn_blocking(move || {
			artifacts.begin_put(&expected, expected_size, artifact_quota)
		})
		.await
		.map_err(|error| status_from(&join_error(error)))?
		.map_err(|error| status_from(&ApiError::from(error)))?;
		let (tx, mut rx) = mpsc::channel::<ArtifactUploadFrame>(8);
		let commit = tokio::task::spawn_blocking(move || {
			let mut writer = writer;
			loop {
				match rx.blocking_recv() {
					Some(ArtifactUploadFrame::Chunk(chunk)) => writer.write_chunk(&chunk)?,
					Some(ArtifactUploadFrame::Finish) => break,
					Some(ArtifactUploadFrame::Abort) | None => {
						return Err(crate::EngineError::invalid("artifact upload interrupted"));
					},
				}
			}
			let stored = writer.finalize()?;
			let path = stored
				.path
				.to_str()
				.ok_or_else(|| crate::EngineError::engine("artifact path is not valid UTF-8"))?;
			let created_at = function_now_millis();
			let expires_at = ttl_millis.map(|ttl| created_at.saturating_add(ttl));
			domain.store().record_artifact(
				&stored.digest,
				stored.size,
				media_type.as_deref(),
				path,
				created_at,
				expires_at,
			)?;
			let (size, canonical_media, canonical_created, canonical_expires, _) =
				domain.store().stat_artifact(&stored.digest)?;
			Ok::<_, crate::EngineError>(pb::ArtifactRecord {
				r#ref: Some(pb::ArtifactRef { digest: Some(returned_digest) }),
				size_bytes: size,
				stored_size_bytes: size,
				media_type_presence: canonical_media
					.map(pb::artifact_record::MediaTypePresence::MediaType),
				created_at_unix_millis: canonical_created,
				expires_at_unix_millis_presence: canonical_expires
					.map(pb::artifact_record::ExpiresAtUnixMillisPresence::ExpiresAtUnixMillis),
			})
		});
		loop {
			let frame = match stream.message().await {
				Ok(Some(frame)) => frame,
				Ok(None) => {
					tx.send(ArtifactUploadFrame::Finish)
						.await
						.map_err(|_| Status::cancelled("artifact writer stopped"))?;
					break;
				},
				Err(error) => {
					let _ = tx.send(ArtifactUploadFrame::Abort).await;
					let _ = commit.await;
					return Err(error);
				},
			};
			if let Some(pb::put_artifact_request::Frame::Data(data)) = frame.frame {
				tx.send(ArtifactUploadFrame::Chunk(data))
					.await
					.map_err(|_| Status::cancelled("artifact writer stopped"))?;
			} else {
				let _ = tx.send(ArtifactUploadFrame::Abort).await;
				let _ = commit.await;
				return Err(status_from(&ApiError::invalid(
					"artifact upload frames after the header must contain data",
				)));
			}
		}
		let record = commit
			.await
			.map_err(|error| status_from(&join_error(error)))?
			.map_err(|error| {
				if error.message.contains("digest mismatch") || error.message.contains("size mismatch")
				{
					status_from(&ApiError::function("checksum", error.message))
				} else {
					status_from(&ApiError::from(error))
				}
			})?;
		Ok(Response::new(record))
	}

	async fn get(
		&self,
		request: Request<pb::GetArtifactRequest>,
	) -> Result<Response<Self::GetStream>, Status> {
		let request = request.into_inner();
		let (_, digest) = artifact_digest(request.artifact.as_ref())?;
		let artifacts = self.state.functions.artifacts().clone();
		let reader = tokio::task::spawn_blocking(move || artifacts.open_verified(&digest, None))
			.await
			.map_err(|error| status_from(&join_error(error)))?
			.map_err(|error| status_from(&ApiError::from(error)))?;
		let size = reader.len();
		let (offset, length) = if let Some(pb::get_artifact_request::RangePresence::Range(range)) =
			request.range_presence
		{
			let end = range
				.offset
				.checked_add(range.length)
				.filter(|end| *end <= size)
				.ok_or_else(|| status_from(&ApiError::invalid("artifact range is out of bounds")))?;
			(range.offset, end - range.offset)
		} else {
			(0, size)
		};
		Ok(Response::new(stream_verified_artifact(reader, offset, length)))
	}

	async fn stat(
		&self,
		request: Request<pb::ArtifactRef>,
	) -> Result<Response<pb::ArtifactRecord>, Status> {
		let reference = request.into_inner();
		let (_, digest) = artifact_digest(Some(&reference))?;
		let (size, media_type, created_at, expires_at, _path) = self
			.function_call(move |domain| domain.store().stat_artifact(&digest))
			.await?;
		Ok(Response::new(pb::ArtifactRecord {
			r#ref: Some(reference),
			size_bytes: size,
			stored_size_bytes: size,
			media_type_presence: media_type.map(pb::artifact_record::MediaTypePresence::MediaType),
			created_at_unix_millis: created_at,
			expires_at_unix_millis_presence: expires_at
				.map(pb::artifact_record::ExpiresAtUnixMillisPresence::ExpiresAtUnixMillis),
		}))
	}
}

#[tonic::async_trait]
impl pb::function_service_server::FunctionService for GrpcApi {
	async fn register(
		&self,
		request: Request<pb::RegisterFunctionRequest>,
	) -> Result<Response<pb::FunctionRevision>, Status> {
		let request = request.into_inner();
		let spec = request
			.spec
			.as_ref()
			.ok_or_else(|| status_from(&ApiError::invalid("function spec is required")))?;
		self.admit_function_ha(spec)?;
		let revision = self
			.function_call(move |domain| {
				let mut spec = request
					.spec
					.clone()
					.ok_or_else(|| crate::EngineError::invalid("function spec is required"))?;
				let architecture = spec
					.resources
					.as_ref()
					.and_then(|resources| pb::CpuArchitecture::try_from(resources.architecture).ok())
					.unwrap_or(pb::CpuArchitecture::Unspecified);
				let image = spec
					.image
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("function image is required"))?;
				spec.image = Some(domain.realize_image(image, architecture)?);
				let revision = domain.store().register_function(
					&spec,
					&request.request_id,
					function_now_millis(),
				)?;
				let revision_id = revision
					.r#ref
					.as_ref()
					.map(|reference| reference.revision_id.clone())
					.ok_or_else(|| crate::EngineError::engine("registered revision has no identity"))?;
				for material in request.transient_secrets {
					let secret = material.secret.ok_or_else(|| {
						crate::EngineError::invalid("transient secret reference is required")
					})?;
					domain.set_secret(revision_id.clone(), &secret, material.value);
				}
				domain.refresh_revision_availability(&revision_id)
			})
			.await?;
		Ok(Response::new(revision))
	}

	async fn get(
		&self,
		request: Request<pb::GetFunctionRequest>,
	) -> Result<Response<pb::FunctionRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| {
				match request.function.and_then(|selector| selector.selection) {
					Some(pb::function_selector::Selection::Current(function)) => {
						domain.store().get_active_revision(&function)
					},
					Some(pb::function_selector::Selection::Pinned(requested)) => {
						let requested_function = requested.function.as_ref().ok_or_else(|| {
							crate::EngineError::invalid("pinned revision function is required")
						})?;
						let loaded = domain.store().get_revision(&requested.revision_id)?;
						if loaded
							.r#ref
							.as_ref()
							.and_then(|reference| reference.function.as_ref())
							!= Some(requested_function)
						{
							return Err(crate::EngineError::not_found(
								"function revision does not belong to the requested function",
							));
						}
						Ok(loaded)
					},
					None => Err(crate::EngineError::invalid("function selector is required")),
				}
			})
			.await?;
		Ok(Response::new(revision))
	}

	async fn list(
		&self,
		request: Request<pb::ListFunctionsRequest>,
	) -> Result<Response<pb::ListFunctionsResponse>, Status> {
		let request = request.into_inner();
		let page = self
			.function_call(move |domain| {
				let namespace = request
					.namespace_presence
					.as_ref()
					.map(|presence| match presence {
						pb::list_functions_request::NamespacePresence::Namespace(value) => value.as_str(),
					});
				let mut page =
					domain
						.store()
						.list_revisions(namespace, request.page_size, &request.page_token)?;
				if let Some(pb::list_functions_request::FunctionPresence::Function(function)) =
					request.function_presence
				{
					page.items.retain(|revision| {
						revision
							.r#ref
							.as_ref()
							.and_then(|reference| reference.function.as_ref())
							== Some(&function)
					});
				}
				Ok(page)
			})
			.await?;
		Ok(Response::new(pb::ListFunctionsResponse {
			revisions:       page.items,
			next_page_token: page.next_page_token,
		}))
	}

	async fn activate(
		&self,
		request: Request<pb::ActivateFunctionRequest>,
	) -> Result<Response<pb::FunctionRecord>, Status> {
		let request = request.into_inner();
		let record = self
			.function_call(move |domain| {
				let revision = request
					.revision
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("revision is required"))?;
				let expected =
					request
						.expected_current_presence
						.as_ref()
						.map(|presence| match presence {
							pb::activate_function_request::ExpectedCurrentPresence::ExpectedCurrent(
								value,
							) => value,
						});
				domain
					.store()
					.activate_function(revision, expected, function_now_millis())
			})
			.await?;
		Ok(Response::new(record))
	}

	async fn delete(
		&self,
		request: Request<pb::DeleteFunctionRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let request = request.into_inner();
		self
			.function_call(move |domain| {
				let revision = request
					.revision
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("revision is required"))?;
				domain.store().delete_revision(revision)
			})
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn activate_app(
		&self,
		request: Request<pb::ActivateAppRequest>,
	) -> Result<Response<pb::AppRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| domain.store().activate_app(&request, function_now_millis()))
			.await?;
		Ok(Response::new(revision))
	}

	async fn get_app(
		&self,
		request: Request<pb::GetAppRequest>,
	) -> Result<Response<pb::AppRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| match request.app.and_then(|selector| selector.selection) {
				Some(pb::app_selector::Selection::Current(app)) => domain.store().get_active_app(&app),
				Some(pb::app_selector::Selection::Pinned(requested)) => {
					let requested_app = requested.app.as_ref().ok_or_else(|| {
						crate::EngineError::invalid("pinned app revision app is required")
					})?;
					let loaded = domain.store().get_app_revision(&requested.revision_id)?;
					if loaded
						.r#ref
						.as_ref()
						.and_then(|reference| reference.app.as_ref())
						!= Some(requested_app)
					{
						return Err(crate::EngineError::not_found(
							"app revision does not belong to the requested app",
						));
					}
					Ok(loaded)
				},
				None => Err(crate::EngineError::invalid("app selector is required")),
			})
			.await?;
		Ok(Response::new(revision))
	}

	async fn rollback_app(
		&self,
		request: Request<pb::RollbackAppRequest>,
	) -> Result<Response<pb::AppRevision>, Status> {
		let request = request.into_inner();
		let revision = self
			.function_call(move |domain| domain.store().rollback_app(&request, function_now_millis()))
			.await?;
		Ok(Response::new(revision))
	}

	async fn create_schedule(
		&self,
		request: Request<pb::CreateScheduleRequest>,
	) -> Result<Response<pb::ScheduleRecord>, Status> {
		let request = request.into_inner();
		let domain = self.state.functions.clone();
		let schedule = tokio::task::spawn_blocking(move || {
			domain.create_schedule(&request, function_now_millis())
		})
		.await
		.map_err(|error| status_from(&join_error(error)))?
		.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(schedule))
	}

	async fn get_schedule(
		&self,
		request: Request<pb::ScheduleRef>,
	) -> Result<Response<pb::ScheduleRecord>, Status> {
		let reference = request.into_inner();
		let schedule = self
			.function_call(move |domain| domain.store().get_schedule(&reference.schedule_id))
			.await?;
		Ok(Response::new(schedule))
	}

	async fn list_schedules(
		&self,
		request: Request<pb::ListSchedulesRequest>,
	) -> Result<Response<pb::ListSchedulesResponse>, Status> {
		let request = request.into_inner();
		let page = self
			.function_call(move |domain| domain.store().list_schedules(&request))
			.await?;
		Ok(Response::new(pb::ListSchedulesResponse {
			schedules:       page.items,
			next_page_token: page.next_page_token,
		}))
	}

	async fn delete_schedule(
		&self,
		request: Request<pb::ScheduleRef>,
	) -> Result<Response<pb::Ok>, Status> {
		let reference = request.into_inner();
		self
			.function_call(move |domain| domain.store().delete_schedule(&reference.schedule_id))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::call_service_server::CallService for GrpcApi {
	type StreamInputsStream = BoxStream<pb::StreamCallInputsResponse>;
	type WatchStream = BoxStream<pb::CallEvent>;

	async fn create(
		&self,
		request: Request<pb::CreateCallRequest>,
	) -> Result<Response<pb::CallRecord>, Status> {
		let request = request.into_inner();
		if let Some(revision_id) = request
			.target
			.as_ref()
			.and_then(|target| target.function.as_ref())
			.map(|revision| revision.revision_id.clone())
			.filter(|revision_id| !revision_id.is_empty())
		{
			let revision = self
				.function_call(move |domain| domain.store().get_revision(&revision_id))
				.await?;
			let spec = revision
				.spec
				.as_ref()
				.ok_or_else(|| Status::internal("function revision is missing its spec"))?;
			self.admit_function_ha(spec)?;
		}
		let record = self
			.function_call(move |domain| {
				let record = domain
					.store()
					.create_call(&request, function_now_millis())?;
				domain.notify_work();
				Ok(record)
			})
			.await?;
		Ok(Response::new(record))
	}

	async fn stream_inputs(
		&self,
		request: Request<Streaming<pb::StreamCallInputsRequest>>,
	) -> Result<Response<Self::StreamInputsStream>, Status> {
		const INPUT_WINDOW: u32 = 8;
		let mut stream = request.into_inner();
		let first = stream
			.message()
			.await?
			.ok_or_else(|| status_from(&ApiError::invalid("input stream requires a call frame")))?;
		let call = match first.frame {
			Some(pb::stream_call_inputs_request::Frame::Call(call)) if !call.call_id.is_empty() => {
				call
			},
			_ => {
				return Err(status_from(&ApiError::invalid(
					"input stream first frame must be a call reference",
				)));
			},
		};
		let committed = self
			.function_call({
				let call_id = call.call_id.clone();
				move |domain| Ok(domain.store().get_call(&call_id)?.input_count)
			})
			.await?;
		let (tx, rx) = mpsc::channel(INPUT_WINDOW as usize);
		tx.send(Ok(pb::StreamCallInputsResponse {
			call:                   Some(call.clone()),
			committed_input_count:  committed,
			last_input_presence:    None,
			max_inputs_outstanding: INPUT_WINDOW,
		}))
		.await
		.map_err(|_| Status::cancelled("input response stream closed"))?;
		let domain = self.state.functions.clone();
		tokio::spawn(async move {
			loop {
				let frame = match stream.message().await {
					Ok(Some(frame)) => frame,
					Ok(None) => break,
					Err(error) => {
						let _ = tx.send(Err(error)).await;
						break;
					},
				};
				let Some(pb::stream_call_inputs_request::Frame::Input(input)) = frame.frame else {
					let _ = tx
						.send(Err(status_from(&ApiError::invalid(
							"input frames after the opener must contain an input",
						))))
						.await;
					break;
				};
				let input_ref =
					pb::InputRef { input_id: input.input_id.clone(), input_index: input.index };
				let call_id = call.call_id.clone();
				let commit_domain = domain.clone();
				let result = tokio::task::spawn_blocking(move || {
					let frontier = commit_domain.store().event_frontier(&call_id)?;
					let committed =
						commit_domain
							.store()
							.append_input(&call_id, &input, function_now_millis())?;
					for event in commit_domain
						.store()
						.events_after(&call_id, frontier, 10_000)?
					{
						commit_domain.publish_call_event(event);
					}
					commit_domain.notify_work();
					Ok::<_, crate::EngineError>(committed)
				})
				.await;
				let committed = match result {
					Ok(Ok(committed)) => committed,
					Ok(Err(error)) => {
						let _ = tx.send(Err(status_from(&ApiError::from(error)))).await;
						break;
					},
					Err(error) => {
						let _ = tx.send(Err(status_from(&join_error(error)))).await;
						break;
					},
				};
				if tx
					.send(Ok(pb::StreamCallInputsResponse {
						call:                   Some(call.clone()),
						committed_input_count:  committed,
						last_input_presence:    Some(
							pb::stream_call_inputs_response::LastInputPresence::LastInput(input_ref),
						),
						max_inputs_outstanding: INPUT_WINDOW,
					}))
					.await
					.is_err()
				{
					break;
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
	}

	async fn close_inputs(
		&self,
		request: Request<pb::CloseCallInputsRequest>,
	) -> Result<Response<pb::CallRecord>, Status> {
		let request = request.into_inner();
		let record = self
			.function_call(move |domain| {
				let call = request
					.call
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("call is required"))?;
				let frontier = domain.store().event_frontier(&call.call_id)?;
				let record = domain.store().close_inputs(
					&call.call_id,
					request.expected_input_count,
					function_now_millis(),
				)?;
				for event in domain
					.store()
					.events_after(&call.call_id, frontier, 10_000)?
				{
					domain.publish_call_event(event);
				}
				domain.notify_work();
				Ok(record)
			})
			.await?;
		Ok(Response::new(record))
	}

	async fn get(&self, request: Request<pb::CallRef>) -> Result<Response<pb::CallRecord>, Status> {
		let call = request.into_inner();
		let record = self
			.function_call(move |domain| domain.store().get_call(&call.call_id))
			.await?;
		Ok(Response::new(record))
	}

	async fn list(
		&self,
		request: Request<pb::ListCallsRequest>,
	) -> Result<Response<pb::ListCallsResponse>, Status> {
		let request = request.into_inner();
		let page = self
			.function_call(move |domain| domain.store().list_calls(&request))
			.await?;
		Ok(Response::new(pb::ListCallsResponse {
			calls:           page.items,
			next_page_token: page.next_page_token,
		}))
	}

	async fn get_result(
		&self,
		request: Request<pb::GetCallResultRequest>,
	) -> Result<Response<pb::CallResult>, Status> {
		let request = request.into_inner();
		let result = self
			.function_call(move |domain| {
				let call = request
					.call
					.as_ref()
					.ok_or_else(|| crate::EngineError::invalid("call is required"))?;
				domain.store().get_result(&call.call_id, request.index)
			})
			.await?;
		Ok(Response::new(result))
	}

	async fn list_results(
		&self,
		request: Request<pb::ListCallResultsRequest>,
	) -> Result<Response<pb::ListCallResultsResponse>, Status> {
		let request = request.into_inner();
		let cursor = request
			.cursor
			.ok_or_else(|| status_from(&ApiError::invalid("result cursor is required")))?;
		let call = cursor
			.call
			.ok_or_else(|| status_from(&ApiError::invalid("cursor call is required")))?;
		let page_size = if request.page_size == 0 {
			100
		} else {
			request.page_size.min(1000)
		};
		let call_id = call.call_id.clone();
		let after = cursor.after_sequence;
		let (results, end) = self
			.function_call(move |domain| {
				let results = domain.store().results_after(&call_id, after, page_size)?;
				let last = results.last().map_or(after, |result| result.sequence);
				let end = domain.store().results_after(&call_id, last, 1)?.is_empty();
				Ok((results, end))
			})
			.await?;
		let after_sequence = results.last().map_or(after, |result| result.sequence);
		Ok(Response::new(pb::ListCallResultsResponse {
			results,
			next_cursor: Some(pb::ResultCursor { call: Some(call), after_sequence }),
			end,
		}))
	}

	async fn watch(
		&self,
		request: Request<pb::WatchCallRequest>,
	) -> Result<Response<Self::WatchStream>, Status> {
		let request = request.into_inner();
		let cursor = request
			.cursor
			.ok_or_else(|| status_from(&ApiError::invalid("event cursor is required")))?;
		let call = cursor
			.call
			.ok_or_else(|| status_from(&ApiError::invalid("cursor call is required")))?;
		if call.call_id.is_empty() {
			return Err(status_from(&ApiError::invalid("call id is required")));
		}
		let current = self
			.function_call({
				let call_id = call.call_id.clone();
				move |domain| domain.store().get_call(&call_id)
			})
			.await?;
		if !request.follow || terminal_call_status(current.status) {
			return Ok(Response::new(stream_persisted_call_events(
				Arc::clone(&self.state.functions),
				call.call_id,
				cursor.after_sequence,
			)));
		}
		let domain = Arc::clone(&self.state.functions);
		let call_id = call.call_id;
		let mut watch = domain
			.watch_call(&call_id, cursor.after_sequence)
			.map_err(|error| status_from(&ApiError::from(error)))?;
		let (tx, rx) = mpsc::channel(32);
		tokio::spawn(async move {
			let mut last_sequence = cursor.after_sequence;
			loop {
				match watch.recv().await {
					Ok(event) => {
						let is_terminal = matches!(
							event.payload.as_ref(),
							Some(pb::call_event::Payload::Status(status))
								if terminal_call_status(status.status)
						);
						let sequence = event.sequence;
						if tx.send(Ok(event)).await.is_err() {
							break;
						}
						last_sequence = sequence;
						if is_terminal {
							break;
						}
					},
					Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
						match domain.watch_call(&call_id, last_sequence) {
							Ok(recovered) => watch = recovered,
							Err(error) => {
								let _ = tx.send(Err(status_from(&ApiError::from(error)))).await;
								break;
							},
						}
					},
					Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
	}

	async fn cancel(
		&self,
		request: Request<pb::CancelCallRequest>,
	) -> Result<Response<pb::CallRecord>, Status> {
		let request = request.into_inner();
		let call = request
			.call
			.as_ref()
			.ok_or_else(|| status_from(&ApiError::invalid("call is required")))?;
		let record = self
			.state
			.functions
			.cancel_call(&call.call_id, &request.reason, &request.request_id)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(record))
	}
}

#[tonic::async_trait]
impl pb::actor_service_server::ActorService for GrpcApi {
	async fn create(
		&self,
		request: Request<pb::CreateActorRequest>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let request = request.into_inner();
		let actor = self
			.state
			.functions
			.create_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(actor))
	}

	async fn get(
		&self,
		request: Request<pb::ActorRef>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let actor = request.into_inner();
		let record = self
			.function_call(move |domain| domain.store().get_actor(&actor.actor_id))
			.await?;
		Ok(Response::new(record))
	}

	async fn checkpoint(
		&self,
		request: Request<pb::CheckpointActorRequest>,
	) -> Result<Response<pb::ActorCheckpoint>, Status> {
		let request = request.into_inner();
		let checkpoint = self
			.state
			.functions
			.checkpoint_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(checkpoint))
	}

	async fn restore(
		&self,
		request: Request<pb::RestoreActorRequest>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let request = request.into_inner();
		let actor = self
			.state
			.functions
			.restore_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(actor))
	}

	async fn fork(
		&self,
		request: Request<pb::ForkActorRequest>,
	) -> Result<Response<pb::ActorRecord>, Status> {
		let request = request.into_inner();
		let actor = self
			.state
			.functions
			.fork_actor(&request)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(actor))
	}

	async fn delete(&self, request: Request<pb::ActorRef>) -> Result<Response<pb::Ok>, Status> {
		let actor = request.into_inner();
		self
			.state
			.functions
			.delete_actor(&actor.actor_id)
			.await
			.map_err(|error| status_from(&ApiError::from(error)))?;
		Ok(Response::new(pb::Ok {}))
	}
}

fn gossip_mesh_api_error(err: crate::mesh::gossip::MeshError) -> ApiError {
	ApiError::new(err.http_status(), err.code, err.message)
}

fn record_matches_tags(record: &CreateRecordWire, tags: Option<&HashMap<String, String>>) -> bool {
	let Some(tags) = tags else {
		return true;
	};
	let Some(record_tags) = record.params.get("tags").and_then(|v| v.as_object()) else {
		return tags.is_empty();
	};
	for (k, v) in tags {
		let Some(record_v) = record_tags.get(k).and_then(|v| v.as_str()) else {
			return false;
		};
		if record_v != v {
			return false;
		}
	}
	true
}

#[tonic::async_trait]
impl pb::sandbox_service_server::SandboxService for GrpcApi {
	type AttachStream = BoxStream<pb::ExecOutput>;
	type ExecStream = BoxStream<pb::ExecOutput>;
	type LogsStream = BoxStream<pb::LogChunk>;
	type ShellStream = BoxStream<pb::ExecOutput>;

	async fn create(
		&self,
		request: Request<pb::CreateSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		let value: Value = serde_json::from_str(&message.spec_json)
			.map_err(|_| Status::from(ApiError::invalid("invalid request")))?;
		validation::validate_create_value(&value)?;
		let body: SandboxCreate = validation::from_value(value.clone())?;
		validation::validate_create(&body)?;
		let view = if let Some(mesh) = self.state.mesh.clone().filter(|mesh| mesh.mesh_enabled())
			&& !is_mesh_hop(&metadata)
		{
			mesh
				.create_sandbox(value.as_object().cloned().unwrap_or_default())
				.await
				.map_err(|err| status_from(&ApiError::from(err)))?
		} else {
			self.engine_call(move |engine| engine.create(body)).await?
		};
		Ok(Response::new(json_view(&view)))
	}

	async fn list(
		&self,
		request: Request<pb::ListSandboxesRequest>,
	) -> Result<Response<pb::ListSandboxesResponse>, Status> {
		let message = request.into_inner();
		let tags = validation::parse_tag_filters(&message.tags)?;
		let rows = if let Some(mesh) = self.state.mesh.clone()
			&& mesh.mesh_enabled()
		{
			let records = MeshRecordStore::list(&*mesh)
				.map_err(|err| status_from(&gossip_mesh_api_error(err)))?;
			let mut views = Vec::new();
			let local_node = MeshControl::node_id(&*mesh);
			for rec in records {
				if !record_matches_tags(&rec, tags.as_ref()) {
					continue;
				}
				let sid = rec.sid.clone();
				if rec.owner == local_node {
					let engine = self.state.engine.clone();
					let view_res = tokio::task::spawn_blocking(move || engine.get(&sid)).await;
					if let Ok(Ok(view)) = view_res {
						views.push(apply_view_detail(view, &rec));
					} else {
						views.push(serde_json::json!({
							"id": rec.sid,
							"name": rec.sid,
							"node": rec.owner,
							"ha": rec.ha,
							"restart_policy": rec.restart_policy,
							"created_at": rec.created_at,
							"status": "running",
						}));
					}
				} else {
					views.push(serde_json::json!({
						"id": rec.sid,
						"name": rec.sid,
						"node": rec.owner,
						"ha": rec.ha,
						"restart_policy": rec.restart_policy,
						"created_at": rec.created_at,
						"status": "running",
					}));
				}
			}
			views
		} else {
			self.engine_call(move |engine| engine.list(tags)).await?
		};
		let sandboxes_json = rows.into_iter().map(|row| compact_json(&row)).collect();
		Ok(Response::new(pb::ListSandboxesResponse { sandboxes_json }))
	}

	async fn get(&self, request: Request<pb::SandboxRef>) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, get);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.get(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn stop(
		&self,
		request: Request<pb::StopSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, stop);
		let pb::StopSandboxRequest { id, returncode } = message;
		let view = self
			.engine_call(move |engine| engine.stop_with_returncode(&id, returncode))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn remove(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, remove);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.remove(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn terminate(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, terminate);
		let id = message.id;
		let view = self
			.engine_call(move |engine| engine.terminate(&id, "api"))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn pause(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, pause);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.pause(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn resume(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, resume);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.resume(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn extend(
		&self,
		request: Request<pb::ExtendSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, extend);
		validation::validate_extend(&ExtendBody { secs: message.secs })?;
		let pb::ExtendSandboxRequest { id, secs } = message;
		let view = self
			.engine_call(move |engine| engine.extend(&id, secs))
			.await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn metrics(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, metrics);
		let id = message.id;
		let view = self.engine_call(move |engine| engine.metrics(&id)).await?;
		Ok(Response::new(json_view(&view)))
	}

	async fn logs(
		&self,
		request: Request<pb::LogsRequest>,
	) -> Result<Response<Self::LogsStream>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, logs, stream);
		let pb::LogsRequest { id, follow, tail } = message;
		if follow {
			let history = match tail {
				Some(tail) => {
					let history_id = id.clone();
					Some(tail_bytes(
						self
							.engine_call(move |engine| engine.logs(&history_id))
							.await?,
						tail,
					))
				},
				None => None,
			};
			let rx = self
				.engine_call(move |engine| engine.logs_follow(&id))
				.await?;
			let (tx, out) = mpsc::channel::<Result<pb::LogChunk, Status>>(32);
			if let Some(history) = history.filter(|history| !history.is_empty()) {
				let _ = tx.try_send(Ok(pb::LogChunk { data: history }));
			}
			thread::spawn(move || {
				while let Ok(chunk) = rx.recv() {
					if tx.blocking_send(Ok(pb::LogChunk { data: chunk })).is_err() {
						break;
					}
				}
			});
			return Ok(Response::new(Box::pin(ReceiverStream::new(out))));
		}
		let data = self.engine_call(move |engine| engine.logs(&id)).await?;
		let data = match tail {
			Some(tail) => tail_bytes(data, tail),
			None => data,
		};
		Ok(Response::new(Box::pin(tokio_stream::once(Ok(pb::LogChunk { data })))))
	}

	async fn exec_capture(
		&self,
		request: Request<pb::ExecCaptureRequest>,
	) -> Result<Response<pb::ExecCaptureResponse>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, exec_capture);
		let pb::ExecCaptureRequest { id, exec } = message;
		let exec = exec.ok_or_else(|| Status::from(ApiError::invalid("exec start is required")))?;
		// The wrapper's `id` is authoritative; `ExecStart.sandbox_id` is ignored.
		let exec_request = validation::validate_exec(&exec_body(exec))?;
		let capture = self
			.engine_call(move |engine| engine.exec_capture(&id, exec_request))
			.await?;
		Ok(Response::new(pb::ExecCaptureResponse {
			code:   capture.exit,
			signal: None,
			stdout: capture.stdout,
			stderr: capture.stderr,
		}))
	}

	async fn exec(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ExecStream>, Status> {
		let (metadata, _, mut inbound) = request.into_parts();
		let start = require_exec_start(inbound.message().await?)?;
		if let Some(target) = self.forward_target(&metadata, &start.sandbox_id).await? {
			let first = pb::ExecInput { input: Some(pb::exec_input::Input::Start(start)) };
			let outbound = tokio_stream::once(first).chain(inbound.filter_map(|frame| frame.ok()));
			return target
				.sandbox_client()
				.exec(target.request(outbound))
				.await
				.map(relay_stream);
		}
		let id = start.sandbox_id.clone();
		let exec_request = validation::validate_exec(&exec_body(start))?;
		let stream = self
			.engine_call(move |engine| engine.exec_stream(&id, exec_request))
			.await?;
		Ok(Response::new(pump_exec(self.state.engine.clone(), stream, inbound, None, None)))
	}

	async fn shell(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ShellStream>, Status> {
		let mut inbound = request.into_inner();
		let params = require_shell_params(inbound.message().await?)?;
		let engine = self.state.engine.clone();
		let session = tokio::task::spawn_blocking(move || engine.shell_start(params))
			.await
			.map_err(|err| status_from(&join_error(err)))?
			.map_err(|err| status_from(&ApiError::from(err)))?;
		let ready = session.name.clone();
		let cleanup = session.ephemeral.then(|| session.name.clone());
		Ok(Response::new(pump_exec(
			self.state.engine.clone(),
			session.stream,
			inbound,
			Some(ready),
			cleanup,
		)))
	}

	async fn attach(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<Self::AttachStream>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, attach, stream);
		let id = message.id;
		let logs = self
			.engine_call(move |engine| engine.logs_follow(&id))
			.await?;
		let (tx, rx) = mpsc::channel::<Result<pb::ExecOutput, Status>>(32);
		thread::spawn(move || {
			while let Ok(chunk) = logs.recv() {
				if tx
					.blocking_send(Ok(chunk_output(pb::Stream::Console, chunk)))
					.is_err()
				{
					break;
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
	}

	async fn file_read(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::FileContent>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_read);
		let pb::FilePathRequest { id, path } = message;
		let data = self
			.engine_call(move |engine| engine.file_read(&id, &path))
			.await?;
		Ok(Response::new(pb::FileContent { data }))
	}

	async fn file_write(
		&self,
		request: Request<pb::FileWriteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_write);
		let pb::FileWriteRequest { id, path, data } = message;
		self
			.engine_call(move |engine| engine.file_write(&id, &path, &data))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn file_delete(
		&self,
		request: Request<pb::FileDeleteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_delete);
		let pb::FileDeleteRequest { id, path, recursive } = message;
		self
			.engine_call(move |engine| engine.file_delete(&id, &path, recursive))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn file_list(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_list);
		let pb::FilePathRequest { id, path } = message;
		let value = self
			.engine_call(move |engine| engine.file_list(&id, &path))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn file_stat(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, file_stat);
		let pb::FilePathRequest { id, path } = message;
		let value = self
			.engine_call(move |engine| engine.file_stat(&id, &path))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn network_get(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, network_get);
		let id = message.id;
		let value = self
			.engine_call(move |engine| engine.network_get(&id))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn network_set(
		&self,
		request: Request<pb::NetworkSetRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, network_set);
		let pb::NetworkSetRequest { id, block_network, cidr_allow, domain_allow } = message;
		let body = NetworkBody {
			block_network,
			cidr_allow: cidr_allow.map(|list| list.values),
			domain_allow: domain_allow.map(|list| list.values),
		};
		validation::validate_network(&body)?;
		let value = self
			.engine_call(move |engine| engine.network_set(&id, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn tunnels(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, tunnels);
		let id = message.id;
		let value = self.engine_call(move |engine| engine.tunnels(&id)).await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn migrate(
		&self,
		request: Request<pb::MigrateRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		if message.target.is_empty() {
			return Err(ApiError::invalid("target node id is required").into());
		}
		forward_to_owner!(self, metadata, &message.id, message, migrate);
		let pb::MigrateRequest { id, target } = message;
		let value = if let Some(mesh) = self.state.mesh.clone() {
			mesh
				.migrate_sandbox(id, target)
				.await
				.map_err(|err| status_from(&ApiError::from(err)))?
		} else {
			self
				.engine_call(move |engine| engine.migrate(&id, &target))
				.await?
		};
		Ok(Response::new(json_view(&value)))
	}

	async fn snapshot(
		&self,
		request: Request<pb::SnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, snapshot);
		let pb::SnapshotRequest { id, name, stop } = message;
		let value = self
			.engine_call(move |engine| engine.snapshot(&id, name, stop))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn snapshot_fs(
		&self,
		request: Request<pb::SnapshotFsRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		forward_to_owner!(self, metadata, &message.id, message, snapshot_fs);
		let pb::SnapshotFsRequest { id, name } = message;
		let value = self
			.engine_call(move |engine| engine.snapshot_fs(&id, name))
			.await?;
		Ok(Response::new(json_view(&value)))
	}
}

#[tonic::async_trait]
impl pb::snapshot_service_server::SnapshotService for GrpcApi {
	async fn list(
		&self,
		_request: Request<pb::ListSnapshotsRequest>,
	) -> Result<Response<pb::SnapshotList>, Status> {
		let snapshots = self.engine_call(|engine| engine.snapshots()).await?;
		Ok(Response::new(pb::SnapshotList { snapshots }))
	}

	async fn restore(
		&self,
		request: Request<pb::RestoreSnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let pb::RestoreSnapshotRequest { name, body_json } = request.into_inner();
		let body: RestoreBody = parse_body_json(&body_json)?;
		let value = self
			.engine_call(move |engine| engine.restore(&name, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn fork(
		&self,
		request: Request<pb::ForkSnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let pb::ForkSnapshotRequest { name, body_json } = request.into_inner();
		let body: ForkBody = parse_body_json(&body_json)?;
		validation::validate_fork(&body)?;
		let value = self
			.engine_call(move |engine| engine.fork(&name, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}
}

#[tonic::async_trait]
impl pb::volume_service_server::VolumeService for GrpcApi {
	async fn list(
		&self,
		_request: Request<pb::ListVolumesRequest>,
	) -> Result<Response<pb::VolumeList>, Status> {
		let volumes = self.engine_call(|engine| engine.volume_list()).await?;
		Ok(Response::new(pb::VolumeList { volumes }))
	}

	async fn create(&self, request: Request<pb::VolumeRef>) -> Result<Response<pb::Ok>, Status> {
		let name = request.into_inner().name;
		self
			.engine_call(move |engine| engine.volume_create(&name))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}

	async fn delete(&self, request: Request<pb::VolumeRef>) -> Result<Response<pb::Ok>, Status> {
		let name = request.into_inner().name;
		self
			.engine_call(move |engine| engine.volume_delete(&name))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::pool_service_server::PoolService for GrpcApi {
	async fn list(
		&self,
		_request: Request<pb::ListPoolsRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let value = self.engine_call(|engine| engine.pool_list()).await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn set(
		&self,
		request: Request<pb::PoolSetRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let pb::PoolSetRequest { reference, body_json } = request.into_inner();
		let body: PoolPutBody = parse_body_json(&body_json)?;
		let value = self
			.engine_call(move |engine| engine.pool_set(&reference, body))
			.await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn delete(&self, request: Request<pb::PoolRef>) -> Result<Response<pb::Ok>, Status> {
		let reference = request.into_inner().reference;
		self
			.engine_call(move |engine| engine.pool_delete(&reference))
			.await?;
		Ok(Response::new(pb::Ok {}))
	}
}

#[tonic::async_trait]
impl pb::system_service_server::SystemService for GrpcApi {
	type EventsStream = BoxStream<pb::JsonView>;

	async fn info(
		&self,
		_request: Request<pb::InfoRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let value = self.engine_call(|engine| engine.info()).await?;
		Ok(Response::new(json_view(&value)))
	}

	async fn events(
		&self,
		_request: Request<pb::EventsRequest>,
	) -> Result<Response<Self::EventsStream>, Status> {
		let rx = self.state.engine.subscribe_events();
		let (tx, out) = mpsc::channel::<Result<pb::JsonView, Status>>(32);
		thread::spawn(move || {
			while let Ok(payload) = rx.recv() {
				if tx.blocking_send(Ok(json_view(&payload))).is_err() {
					break;
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(out))))
	}

	async fn mesh_status(
		&self,
		_request: Request<pb::MeshStatusRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let Some(mesh) = self.state.mesh.as_ref() else {
			return Err(
				ApiError::from(crate::EngineError::unsupported("mesh is not configured")).into(),
			);
		};
		let state = mesh.route_state();
		let value =
			tokio::task::spawn_blocking(move || crate::mesh::routes::mesh_status_view(&state))
				.await
				.map_err(|err| status_from(&join_error(err)))?
				.map_err(|err| status_from(&ApiError::new(err.http_status(), err.code, err.message)))?;
		Ok(Response::new(json_view(&value)))
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::{EngineError, ErrorCode};

	#[test]
	fn status_mapping_covers_every_error_code_and_carries_vmon_code() {
		let cases = [
			(ErrorCode::NotFound, Code::NotFound),
			(ErrorCode::Invalid, Code::InvalidArgument),
			(ErrorCode::Unauthorized, Code::Unauthenticated),
			(ErrorCode::NotRunning, Code::FailedPrecondition),
			(ErrorCode::Busy, Code::Aborted),
			(ErrorCode::Unsupported, Code::Unimplemented),
			(ErrorCode::Engine, Code::Unavailable),
		];
		for (code, expected) in cases {
			let status = status_from(&ApiError::from(EngineError::new(code, "boom")));
			assert_eq!(status.code(), expected, "grpc code for {code:?}");
			assert_eq!(status.message(), "boom");
			let carried = status
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok())
				.map(str::to_owned);
			assert_eq!(carried.as_deref(), Some(code.as_str()));
		}
	}

	#[test]
	fn status_metadata_explains_retryability_and_recovery() {
		let invalid = status_from(&ApiError::invalid("bad request"));
		assert_eq!(
			invalid
				.metadata()
				.get("vmon-retryable")
				.and_then(|value| value.to_str().ok()),
			Some("false")
		);
		assert_eq!(
			invalid
				.metadata()
				.get("vmon-action")
				.and_then(|value| value.to_str().ok()),
			Some("correct the request parameters")
		);

		let engine = status_from(&ApiError::from(EngineError::engine("host unavailable")));
		assert_eq!(
			engine
				.metadata()
				.get("vmon-retryable")
				.and_then(|value| value.to_str().ok()),
			Some("true")
		);
	}

	#[test]
	fn function_errors_have_stable_grpc_codes_and_metadata() {
		for (stable, expected) in [
			("actor_lost", Code::FailedPrecondition),
			("unavailable_secret", Code::FailedPrecondition),
			("ha_unavailable", Code::FailedPrecondition),
			("checksum", Code::DataLoss),
			("conflict", Code::Aborted),
			("deadline", Code::DeadlineExceeded),
		] {
			let status = status_from(&ApiError::new(axum::http::StatusCode::CONFLICT, stable, "boom"));
			assert_eq!(status.code(), expected, "{stable}");
			assert_eq!(
				status
					.metadata()
					.get("vmon-code")
					.and_then(|value| value.to_str().ok()),
				Some(stable)
			);
		}
	}

	fn ha_spec(policy: pb::HighAvailabilityPolicy, arch: pb::CpuArchitecture) -> pb::FunctionSpec {
		pb::FunctionSpec {
			resources: Some(pb::ResourceSpec {
				high_availability: policy.into(),
				architecture: arch.into(),
				..Default::default()
			}),
			..Default::default()
		}
	}

	fn topology_node(id: &str, zone: &str, arch: &str) -> crate::mesh::state::NodeState {
		let mut node = crate::mesh::state::NodeState::new(id, format!("https://{id}"));
		node.region = zone.into();
		node.arch = arch.into();
		node
	}

	#[test]
	fn none_is_admitted_without_mesh_or_remote_executor() {
		function_ha_admission(
			&ha_spec(pb::HighAvailabilityPolicy::None, pb::CpuArchitecture::Arm64),
			&[],
			false,
		)
		.unwrap();
	}

	#[test]
	fn host_and_zone_never_degrade_to_local_execution() {
		let nodes = [
			topology_node("host-a", "zone-a", "aarch64"),
			topology_node("host-b", "zone-b", "aarch64"),
		];
		for policy in [pb::HighAvailabilityPolicy::Host, pb::HighAvailabilityPolicy::Zone] {
			let error =
				function_ha_admission(&ha_spec(policy, pb::CpuArchitecture::Arm64), &nodes, false)
					.unwrap_err();
			assert_eq!(error.code(), "ha_unavailable");
			assert!(
				error
					.message()
					.contains("cross-node function execution is unavailable")
			);
			assert_eq!(status_from(&error).code(), Code::FailedPrecondition);
		}
	}

	#[test]
	fn insufficient_and_arch_incompatible_topology_are_actionable() {
		let nodes =
			[topology_node("host-a", "zone-a", "x86_64"), topology_node("host-b", "zone-b", "x86_64")];
		let error = function_ha_admission(
			&ha_spec(pb::HighAvailabilityPolicy::Zone, pb::CpuArchitecture::Arm64),
			&nodes,
			false,
		)
		.unwrap_err();
		assert_eq!(error.code(), "ha_unavailable");
		assert!(
			error
				.message()
				.contains("requires at least 2 compatible mesh zones; found 0")
		);
		assert!(
			error
				.message()
				.contains("cross-node function execution is unavailable")
		);
	}
	#[test]
	fn mesh_transport_codes_map_to_unavailable_or_aborted() {
		let unreachable = status_from(&mesh_api_error(MeshError::unreachable("gone")));
		assert_eq!(unreachable.code(), Code::Unavailable);
		assert_eq!(
			unreachable
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("unreachable")
		);
		let conflict = status_from(&mesh_api_error(MeshError::conflict("taken")));
		assert_eq!(conflict.code(), Code::Aborted);
	}

	#[test]
	fn exec_start_converts_to_engine_request_with_timeout_cap() {
		let start = pb::ExecStart {
			cmd:        vec!["sh".to_owned(), "-c".to_owned(), "ls".to_owned()],
			workdir:    Some("/tmp".to_owned()),
			env:        HashMap::from([("A".to_owned(), "1".to_owned())]),
			timeout:    Some(120.0),
			tty:        true,
			sandbox_id: "sb".to_owned(),
		};
		let request = validation::validate_exec(&exec_body(start)).expect("valid exec start");
		assert_eq!(request.cmd, ["sh", "-c", "ls"]);
		assert!(request.tty);
		assert_eq!(request.workdir.as_deref(), Some("/tmp"));
		assert_eq!(
			request
				.env
				.as_ref()
				.and_then(|env| env.get("A"))
				.map(String::as_str),
			Some("1")
		);
		assert_eq!(request.timeout, Some(60.0));
	}

	#[test]
	fn exec_start_empty_env_means_inherit_and_empty_cmd_is_rejected() {
		let start = pb::ExecStart { cmd: vec!["true".to_owned()], ..Default::default() };
		let request = validation::validate_exec(&exec_body(start)).expect("valid exec start");
		assert_eq!(request.env, None);
		assert_eq!(request.workdir, None);

		let empty = pb::ExecStart::default();
		let err = validation::validate_exec(&exec_body(empty)).expect_err("empty cmd rejected");
		assert_eq!(status_from(&err).code(), Code::InvalidArgument);
	}

	fn start_input(start: pb::ExecStart) -> pb::ExecInput {
		pb::ExecInput { input: Some(pb::exec_input::Input::Start(start)) }
	}

	#[test]
	fn first_exec_frame_must_be_start_naming_a_sandbox() {
		let missing = require_exec_start(None).expect_err("stream end rejected");
		assert_eq!(missing.code(), Code::InvalidArgument);
		assert_eq!(
			missing
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("invalid")
		);

		let stdin = require_exec_start(Some(pb::ExecInput {
			input: Some(pb::exec_input::Input::Stdin(b"hi".to_vec())),
		}))
		.expect_err("stdin-first rejected");
		assert_eq!(stdin.code(), Code::InvalidArgument);

		let unnamed = require_exec_start(Some(start_input(pb::ExecStart::default())))
			.expect_err("start without sandbox_id rejected");
		assert_eq!(unnamed.code(), Code::InvalidArgument);
		assert!(unnamed.message().contains("sandbox_id"));

		let start = require_exec_start(Some(start_input(pb::ExecStart {
			cmd: vec!["true".to_owned()],
			sandbox_id: "sb-1".to_owned(),
			..Default::default()
		})))
		.expect("valid start accepted");
		assert_eq!(start.sandbox_id, "sb-1");
	}

	#[test]
	fn first_shell_frame_must_be_params_json_object() {
		let missing = require_shell_params(None).expect_err("stream end rejected");
		assert_eq!(missing.code(), Code::InvalidArgument);

		let wrong = require_shell_params(Some(start_input(pb::ExecStart::default())))
			.expect_err("start-first rejected");
		assert_eq!(wrong.code(), Code::InvalidArgument);

		let params_frame = |text: &str| {
			Some(pb::ExecInput {
				input: Some(pb::exec_input::Input::ShellParamsJson(text.to_owned())),
			})
		};
		assert_eq!(require_shell_params(params_frame("")).expect("empty defaults"), json!({}));
		assert_eq!(
			require_shell_params(params_frame(r#"{"image":"alpine"}"#)).expect("object accepted"),
			json!({"image": "alpine"})
		);
		let not_object = require_shell_params(params_frame("[1,2]")).expect_err("array rejected");
		assert_eq!(not_object.code(), Code::InvalidArgument);
		let garbage = require_shell_params(params_frame("{nope")).expect_err("garbage rejected");
		assert_eq!(garbage.code(), Code::InvalidArgument);
	}

	#[test]
	fn resize_bounds_match_the_ws_contract() {
		for (rows, cols) in [(1, 1), (24, 80), (65_535, 65_535)] {
			let (parsed_rows, parsed_cols) =
				resize_dims(pb::Resize { rows, cols }).expect("in-range resize accepted");
			assert_eq!(u32::from(parsed_rows), rows);
			assert_eq!(u32::from(parsed_cols), cols);
		}
		for (rows, cols) in [(0, 80), (24, 0), (65_536, 80), (24, 100_000)] {
			assert!(
				resize_dims(pb::Resize { rows, cols }).is_err(),
				"resize ({rows}, {cols}) must be rejected"
			);
		}
	}

	#[test]
	fn tail_bytes_keeps_the_last_lines() {
		let data = b"one\ntwo\nthree\n".to_vec();
		assert_eq!(tail_bytes(data.clone(), 0), b"");
		assert_eq!(tail_bytes(data.clone(), 1), b"three\n");
		assert_eq!(tail_bytes(data.clone(), 2), b"two\nthree\n");
		assert_eq!(tail_bytes(data.clone(), 3), data.as_slice());
		assert_eq!(tail_bytes(data.clone(), 10), data.as_slice());
		assert_eq!(tail_bytes(b"no-newline".to_vec(), 1), b"no-newline");
	}

	#[test]
	fn body_json_defaults_to_the_empty_document() {
		let restore: RestoreBody = parse_body_json("").expect("empty body defaults");
		assert_eq!(restore.name, None);
		let restore: RestoreBody =
			parse_body_json(r#"{"name":"copy"}"#).expect("named restore parses");
		assert_eq!(restore.name.as_deref(), Some("copy"));
		let fork: Result<ForkBody, Status> = parse_body_json("");
		assert!(fork.is_err(), "fork requires count");
		let garbage: Result<RestoreBody, Status> = parse_body_json("{nope");
		assert!(garbage.is_err());
	}
	#[tokio::test]
	async fn verified_artifact_range_streams_in_bounded_chunks() {
		let temp = tempfile::tempdir().expect("temp dir");
		let store = crate::function::artifact::ArtifactStore::open(temp.path()).expect("store");
		let bytes = (0..(ARTIFACT_CHUNK_SIZE * 3 + 17))
			.map(|index| (index % 251) as u8)
			.collect::<Vec<_>>();
		let stored = store.put(&bytes).expect("artifact");
		let reader = store
			.open_verified(&stored.digest, Some(stored.size))
			.expect("verified reader");
		let offset = 31_u64;
		let length = (ARTIFACT_CHUNK_SIZE * 2 + 7) as u64;
		let mut stream = stream_verified_artifact(reader, offset, length);
		let mut chunks = Vec::new();
		while let Some(chunk) = stream.next().await {
			chunks.push(chunk.expect("chunk"));
		}
		assert_eq!(chunks.len(), 3);
		assert!(
			chunks
				.iter()
				.all(|chunk| chunk.data.len() <= ARTIFACT_CHUNK_SIZE)
		);
		assert_eq!(chunks[0].offset, offset);
		assert_eq!(chunks[1].offset, offset + ARTIFACT_CHUNK_SIZE as u64);
		assert_eq!(chunks[2].offset, offset + (ARTIFACT_CHUNK_SIZE * 2) as u64);
		assert!(chunks[..2].iter().all(|chunk| !chunk.eof));
		assert!(chunks[2].eof);
		let streamed = chunks
			.into_iter()
			.flat_map(|chunk| chunk.data)
			.collect::<Vec<_>>();
		assert_eq!(streamed, bytes[offset as usize..(offset + length) as usize],);
	}

	#[tokio::test]
	async fn persisted_event_stream_pages_past_ten_thousand() {
		const EVENT_COUNT: u64 = 10_017;
		let page_loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let loads = Arc::clone(&page_loads);
		let mut stream = stream_call_event_pages(
			move |cursor| {
				loads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				let end = (cursor + 10_000).min(EVENT_COUNT);
				Ok((cursor + 1..=end)
					.map(|sequence| pb::CallEvent { sequence, ..Default::default() })
					.collect())
			},
			0,
		);
		let mut sequences = Vec::new();
		while let Some(event) = stream.next().await {
			sequences.push(event.expect("event").sequence);
		}
		assert_eq!(sequences.len(), EVENT_COUNT as usize);
		assert_eq!(sequences.first(), Some(&1));
		assert_eq!(sequences.last(), Some(&EVENT_COUNT));
		assert_eq!(page_loads.load(std::sync::atomic::Ordering::Relaxed), 2);
	}
}
