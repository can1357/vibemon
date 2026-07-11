//! Engine-backed persistent function workers.
//!
//! The daemon owns worker admission and durability. The guest only executes one
//! immutable revision over protocol v2; it never owns durable queues or
//! results.

use std::{
	collections::HashMap,
	fs::File,
	future::Future,
	io::Read,
	path::{Component, Path},
	pin::Pin,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use parking_lot::{Mutex, RwLock};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use vmon_proto::v1 as pb;

use super::{
	artifact::ArtifactStore,
	image,
	protocol::{
		DEFAULT_EVENT_CAPACITY, Frame, ProtocolError, ProtocolRequirements, ProtocolSession,
	},
	store::Store,
};
use crate::{
	engine::{EngineApi, ExecRequest},
	home::Home,
	models::{RestoreBody, SandboxCreate},
};

const RUNNER: &[u8] = include_bytes!("runner.py");
const RUNNER_PATH: &str = "/opt/vmon/runner.py";
const PACKAGE_PATH: &str = "/opt/vmon/functions/package.zip";
const SOCKET_PATH: &str = "/run/vmon/function-v2.sock";
const DETACHED_RUNNER: &str = concat!(
	"import os,sys\n",
	"pid=os.fork()\n",
	"if pid:\n os._exit(0)\n",
	"os.setsid()\n",
	"null=os.open('/dev/null',os.O_RDWR)\n",
	"log=os.open('/tmp/vmon-function-runner.log',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)\n",
	"os.dup2(null,0)\n",
	"os.dup2(log,1)\n",
	"os.dup2(log,2)\n",
	"os.closerange(3,256)\n",
	"os.execv(sys.executable,[sys.executable,sys.argv[1],'--socket',sys.argv[2]])"
);
const DEFAULT_STARTUP: Duration = Duration::from_mins(1);
const DEFAULT_EXECUTION: Duration = Duration::from_mins(5);
const DEFAULT_SHUTDOWN: Duration = Duration::from_secs(10);
static RESTORE_SERIAL: AtomicU64 = AtomicU64::new(1);

/// A boxed asynchronous operation used by object-safe executor traits.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Ephemeral secret material. Values are consumed by worker creation and are
/// never included in errors, snapshots, provenance, or persisted metadata.
#[derive(Default)]
pub struct SecretValues(HashMap<String, Vec<u8>>);

impl SecretValues {
	/// Construct an empty secret set.
	pub fn new() -> Self {
		Self::default()
	}

	/// Insert or replace one secret.
	pub fn insert(&mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) {
		if let Some(mut previous) = self.0.insert(name.into(), value.into()) {
			previous.fill(0);
		}
	}

	/// Borrow one secret value without copying it.
	pub fn get(&self, name: &str) -> Option<&[u8]> {
		self.0.get(name).map(Vec::as_slice)
	}

	/// Whether material exists for a persisted secret name.
	pub fn contains_key(&self, name: &str) -> bool {
		self.0.contains_key(name)
	}

	/// Iterate safe-to-log secret names. Values are never exposed here.
	pub fn names(&self) -> impl Iterator<Item = &str> {
		self.0.keys().map(String::as_str)
	}

	/// Whether the set contains no material.
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	fn sandbox_wire(&self) -> Result<Vec<Value>, WorkerError> {
		self
			.0
			.iter()
			.map(|(name, bytes)| {
				let value = std::str::from_utf8(bytes).map_err(|_| {
					WorkerError::platform(
						"invalid_secret",
						format!("secret {name} is not valid UTF-8 environment data"),
					)
				})?;
				if value.contains('\0') {
					return Err(WorkerError::platform(
						"invalid_secret",
						format!("secret {name} contains a NUL byte"),
					));
				}
				Ok(json!({"name":name,"values":{name:value}}))
			})
			.collect()
	}

	fn protocol_wire(&self) -> Value {
		Value::Object(
			self
				.0
				.iter()
				.filter_map(|(name, bytes)| {
					std::str::from_utf8(bytes)
						.ok()
						.map(|value| (name.clone(), Value::String(value.to_owned())))
				})
				.collect(),
		)
	}

	fn redact_error(&self, mut error: WorkerError) -> WorkerError {
		for bytes in self.0.values() {
			if let Ok(secret) = std::str::from_utf8(bytes)
				&& !secret.is_empty()
			{
				error.message = error.message.replace(secret, "[REDACTED]");
			}
		}
		error
	}
}

impl Drop for SecretValues {
	fn drop(&mut self) {
		for value in self.0.values_mut() {
			value.fill(0);
		}
	}
}

/// Stable classification used by retry and actor-loss policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerErrorKind {
	User,
	Platform,
	Infrastructure,
	ActorLost,
	Cancelled,
	Timeout,
}

/// A non-secret worker failure.
#[derive(Clone, Debug)]
pub struct WorkerError {
	/// Retry classification.
	pub kind:       WorkerErrorKind,
	/// Stable machine-readable code.
	pub code:       String,
	/// Sanitized human-readable message.
	pub message:    String,
	/// Whether user retry policy may retry this error.
	pub retryable:  bool,
	/// Producer exception type, bounded for durable conversion.
	pub error_type: String,
	/// Structured frames supplied by the guest.
	pub frames:     Vec<pb::ErrorFrame>,
	/// One bounded structured cause.
	pub cause:      Option<Box<Self>>,
	/// Non-secret diagnostic attributes.
	pub details:    Box<HashMap<String, String>>,
}

impl WorkerError {
	/// Construct an application error reported by user code.
	pub fn user(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
		Self::new(WorkerErrorKind::User, code, message, retryable)
	}

	/// Construct a deterministic platform/specification error.
	pub fn platform(code: impl Into<String>, message: impl Into<String>) -> Self {
		Self::new(WorkerErrorKind::Platform, code, message, false)
	}

	/// Construct a retryable engine, transport, or worker-loss error.
	pub fn infrastructure(code: impl Into<String>, message: impl Into<String>) -> Self {
		Self::new(WorkerErrorKind::Infrastructure, code, message, true)
	}

	/// Construct explicit actor loss; callers must never initialize fresh state.
	pub fn actor_lost(message: impl Into<String>) -> Self {
		Self::new(WorkerErrorKind::ActorLost, "actor_lost", message, false)
	}

	/// Construct cancellation.
	pub fn cancelled(message: impl Into<String>) -> Self {
		Self::new(WorkerErrorKind::Cancelled, "cancelled", message, false)
	}

	/// Construct a host-enforced deadline failure.
	pub fn timeout(phase: &'static str) -> Self {
		Self::new(
			WorkerErrorKind::Timeout,
			format!("{phase}_timeout"),
			format!("worker {phase} deadline exceeded"),
			phase == "startup",
		)
	}

	fn new(
		kind: WorkerErrorKind,
		code: impl Into<String>,
		message: impl Into<String>,
		retryable: bool,
	) -> Self {
		let message = message.into();
		Self {
			kind,
			code: code.into(),
			message: message.chars().take(16 * 1024).collect(),
			retryable,
			error_type: String::new(),
			frames: Vec::new(),
			cause: None,
			details: Box::default(),
		}
	}
}

impl std::fmt::Display for WorkerError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.message)
	}
}
impl std::error::Error for WorkerError {}
impl From<ProtocolError> for WorkerError {
	fn from(error: ProtocolError) -> Self {
		match error {
			ProtocolError::Timeout => Self::timeout("execution"),
			ProtocolError::Version { .. }
			| ProtocolError::EnvelopeVersion { .. }
			| ProtocolError::InvalidCapabilities { .. }
			| ProtocolError::MissingCapability { .. }
			| ProtocolError::InvalidFrame(_) => Self::platform("runner_protocol", error.to_string()),
			ProtocolError::Backpressure(_) | ProtocolError::Disconnected(_) => {
				Self::infrastructure("worker_lost", error.to_string())
			},
		}
	}
}

/// Execution mode sent to the guest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionMode {
	Unary,
	Generator,
	Batch,
}
impl ExecutionMode {
	const fn wire(self) -> &'static str {
		match self {
			Self::Unary => "unary",
			Self::Generator => "generator",
			Self::Batch => "batch",
		}
	}
}

/// Whether cooperative cancellation can stop this request without killing its
/// VM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Interruptibility {
	Async,
	Sync,
}

/// One attempt admitted by the durable scheduler.
#[derive(Clone, Debug)]
pub struct ExecuteRequest {
	pub request_id:        String,
	pub function_id:       String,
	pub call_id:           String,
	pub input_id:          String,
	pub input_index:       u64,
	pub attempt:           u32,
	pub mode:              ExecutionMode,
	/// Typed scalar or positional/named invocation payload.
	pub input:             pb::call_input::Payload,
	pub deadline:          Option<SystemTime>,
	pub parent_call_id:    Option<String>,
	pub parent_request_id: Option<String>,
	pub interruptibility:  Interruptibility,
	pub service:           Option<ServiceDispatch>,
}

/// Deterministic service receiver and constructor arguments.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceDispatch {
	pub service_key: String,
	pub method:      String,
	pub constructor: Option<pb::InvocationArguments>,
}

/// Actor operation executed on a pinned worker.
#[derive(Clone, Debug)]
pub enum ActorOperation {
	Create,
	Method { name: String },
	Checkpoint { checkpoint_id: String },
	Restore { checkpoint_id: String, state: Box<pb::ValueEnvelope> },
	Fork { child_actor_id: String },
	Shutdown,
}

/// One durable actor request.
#[derive(Clone, Debug)]
pub struct ActorRequest {
	pub request_id: String,
	pub call_id:    Option<String>,
	pub actor_id:   String,
	pub operation:  ActorOperation,
	pub input:      Option<pb::call_input::Payload>,
	pub deadline:   Option<SystemTime>,
}

/// Typed event emitted while an attempt is running.
#[derive(Clone, Debug)]
pub enum WorkerEvent {
	Log { stream: String, message: String },
	Yield { index: u64, value: pb::ValueEnvelope },
	Status { status: String },
}

/// Per-attempt timing and resource measurements.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptStats {
	pub attempt:           u32,
	pub startup_millis:    u64,
	pub execution_millis:  u64,
	pub cpu_millis:        u64,
	pub peak_memory_bytes: u64,
	pub snapshot_restore:  bool,
}

/// Terminal worker result.
#[derive(Clone, Debug)]
pub enum WorkerOutcome {
	Success { value: pb::ValueEnvelope, stats: AttemptStats },
	Cancelled { reason: String, stats: AttemptStats },
	UserError { error: WorkerError, stats: AttemptStats },
	PlatformError { error: WorkerError, stats: AttemptStats },
	InfrastructureError { error: WorkerError, stats: AttemptStats },
	ActorLost { error: WorkerError, stats: AttemptStats },
}

/// Split event/completion handles for one attempt.
pub struct Execution {
	pub events:     flume::Receiver<WorkerEvent>,
	pub completion: BoxFuture<Result<WorkerOutcome, WorkerError>>,
}
/// Ordered inputs admitted as one bounded dynamic invocation.
#[derive(Clone, Debug)]
pub struct BatchExecuteRequest {
	pub request_id: String,
	pub inputs:     Vec<ExecuteRequest>,
}
/// One ordered terminal result from a grouped invocation.
#[derive(Clone, Debug)]
pub struct BatchItemOutcome {
	pub request_id:  String,
	pub input_id:    String,
	pub input_index: u64,
	pub outcome:     WorkerOutcome,
}
/// Terminal grouped result with exactly one item per admitted input.
#[derive(Clone, Debug)]
pub struct BatchOutcome {
	pub items: Vec<BatchItemOutcome>,
	pub stats: AttemptStats,
}
/// An event attributed to one grouped input.
#[derive(Clone, Debug)]
pub struct BatchWorkerEvent {
	pub input_index: Option<u64>,
	pub event:       WorkerEvent,
}
/// Split event/completion handles for a grouped invocation.
pub struct BatchExecution {
	pub events:     flume::Receiver<BatchWorkerEvent>,
	pub completion: BoxFuture<Result<BatchOutcome, WorkerError>>,
}

/// Why the daemon captures a VM snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotReason {
	PostInitialize,
	WorkerRetire,
	ActorCheckpoint,
}

/// Immutable snapshot provenance used to reject stale worker state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotProvenance {
	pub revision_id:     String,
	pub function:        Option<pb::FunctionRef>,
	pub image_digest:    Vec<u8>,
	pub spec_digest:     Vec<u8>,
	pub package_digest:  Vec<u8>,
	pub initialize_hook: Option<(String, String)>,
	pub runner_digest:   Vec<u8>,
	pub runner_protocol: u64,
}

impl SnapshotProvenance {
	/// Build the expected provenance directly from an immutable revision.
	pub fn for_revision(revision: &pb::FunctionRevision) -> Result<Self, WorkerError> {
		let revision_ref = revision
			.r#ref
			.as_ref()
			.ok_or_else(|| WorkerError::platform("invalid_revision", "revision ref is required"))?;
		let revision_id = revision_ref.revision_id.clone();
		if revision_id.is_empty() {
			return Err(WorkerError::platform("invalid_revision", "revision_id is required"));
		}
		let function = revision_ref.function.clone();
		let spec = revision
			.spec
			.as_ref()
			.ok_or_else(|| WorkerError::platform("invalid_revision", "function spec is required"))?;
		let image_digest = spec
			.image
			.as_ref()
			.and_then(|i| i.resolved_oci_digest.as_ref())
			.map(|d| d.value.clone())
			.unwrap_or_default();
		let package_digest = spec
			.package
			.as_ref()
			.and_then(|p| p.content_digest.as_ref())
			.map(|d| d.value.clone())
			.unwrap_or_default();
		let initialize_hook = spec.lifecycle_hooks.as_ref().and_then(|hooks| {
			hooks.initialize_presence.as_ref().map(
				|pb::lifecycle_hooks::InitializePresence::Initialize(hook)| {
					(hook.module.clone(), hook.qualname.clone())
				},
			)
		});
		let spec_digest = revision
			.spec_digest
			.as_ref()
			.map(|d| d.value.clone())
			.ok_or_else(|| {
				WorkerError::platform(
					"invalid_revision",
					"spec digest is required for snapshot provenance",
				)
			})?;
		Ok(Self {
			revision_id,
			function,
			image_digest,
			package_digest,
			initialize_hook,
			spec_digest,
			runner_protocol: 2,
			runner_digest: Sha256::digest(RUNNER).to_vec(),
		})
	}

	/// Return true only when every state-bearing immutable input matches.
	pub fn matches(&self, revision: &pb::FunctionRevision) -> bool {
		Self::for_revision(revision).is_ok_and(|expected| expected == *self)
	}
}

/// Engine snapshot plus verified function provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerSnapshot {
	pub engine_snapshot:        String,
	pub provenance:             SnapshotProvenance,
	pub reason:                 SnapshotReason,
	pub created_at_unix_millis: u64,
}

impl WorkerSnapshot {
	/// Convert this captured snapshot to durable metadata after its engine
	/// snapshot name has been committed as an artifact.
	pub fn to_record(
		&self,
		snapshot_id: String,
		artifact: pb::ArtifactRef,
	) -> pb::FunctionSnapshotRecord {
		pb::FunctionSnapshotRecord {
			r#ref:                    Some(pb::FunctionSnapshotRef { snapshot_id }),
			revision:                 Some(pb::RevisionRef {
				function:    self.provenance.function.clone(),
				revision_id: self.provenance.revision_id.clone(),
			}),
			artifact:                 Some(artifact),
			protocol_version:         self.provenance.runner_protocol as u32,
			runner_digest:            Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     self.provenance.runner_digest.clone(),
			}),
			image_digest:             Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     self.provenance.image_digest.clone(),
			}),
			package_digest:           Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     self.provenance.package_digest.clone(),
			}),
			created_at_unix_millis:   self.created_at_unix_millis,
			initialize_hook_presence: self.provenance.initialize_hook.as_ref().map(
				|(module, qualname)| {
					pb::function_snapshot_record::InitializeHookPresence::InitializeHook(
						pb::LifecycleHookRef { module: module.clone(), qualname: qualname.clone() },
					)
				},
			),
		}
	}

	/// Rebuild a worker snapshot from durable metadata after the caller reads
	/// the artifact as the engine snapshot name.
	pub fn from_record(
		record: &pb::FunctionSnapshotRecord,
		revision: &pb::FunctionRevision,
		engine_snapshot: String,
		reason: SnapshotReason,
	) -> Result<Self, WorkerError> {
		let expected = SnapshotProvenance::for_revision(revision)?;
		let revision_id = record.revision.as_ref().map(|r| r.revision_id.as_str());
		let runner = record.runner_digest.as_ref().map(|d| d.value.as_slice());
		let image = record.image_digest.as_ref().map(|d| d.value.as_slice());
		let package = record.package_digest.as_ref().map(|d| d.value.as_slice());
		let hook = record.initialize_hook_presence.as_ref().map(
			|pb::function_snapshot_record::InitializeHookPresence::InitializeHook(hook)| {
				(hook.module.clone(), hook.qualname.clone())
			},
		);
		if revision_id != Some(expected.revision_id.as_str())
			|| record.protocol_version as u64 != expected.runner_protocol
			|| runner != Some(expected.runner_digest.as_slice())
			|| image != Some(expected.image_digest.as_slice())
			|| package != Some(expected.package_digest.as_slice())
			|| hook != expected.initialize_hook
		{
			return Err(WorkerError::platform(
				"snapshot_provenance_mismatch",
				"persisted snapshot provenance does not match revision",
			));
		}
		Ok(Self {
			engine_snapshot,
			provenance: expected,
			reason,
			created_at_unix_millis: record.created_at_unix_millis,
		})
	}
}

/// Narrow execution seam used by the scheduler and its fakes.
pub trait Executor: Send + Sync {
	fn spawn(
		&self,
		revision: Arc<pb::FunctionRevision>,
		secrets: SecretValues,
	) -> BoxFuture<Result<Arc<dyn Worker>, WorkerError>>;
	fn restore(
		&self,
		revision: Arc<pb::FunctionRevision>,
		snapshot: WorkerSnapshot,
		secrets: SecretValues,
	) -> BoxFuture<Result<Arc<dyn Worker>, WorkerError>>;
}

/// Narrow persistent-worker seam used by scheduler and actor tests.
pub trait Worker: Send + Sync {
	fn id(&self) -> &str;
	fn revision_id(&self) -> &str;
	fn capacity(&self) -> usize;
	fn interruptibility(&self) -> Interruptibility;
	/// Whether this worker can be returned to a pool after the current attempt.
	///
	/// Test workers and external implementations are reusable unless they
	/// explicitly report otherwise.
	fn is_reusable(&self) -> bool {
		true
	}
	fn execute_batch(
		&self,
		request: BatchExecuteRequest,
	) -> BoxFuture<Result<BatchExecution, WorkerError>> {
		let _ = request;
		Box::pin(async {
			Err(WorkerError::platform(
				"batching_unsupported",
				"executor does not support grouped batching",
			))
		})
	}
	fn execute(&self, request: ExecuteRequest) -> BoxFuture<Result<Execution, WorkerError>>;
	fn cancel(&self, request_id: &str) -> BoxFuture<Result<(), WorkerError>>;
	fn snapshot(&self, reason: SnapshotReason) -> BoxFuture<Result<WorkerSnapshot, WorkerError>>;
	fn actor(&self, request: ActorRequest) -> BoxFuture<Result<Execution, WorkerError>>;
	fn retire(&self) -> BoxFuture<Result<(), WorkerError>>;
	fn initial_snapshot(&self) -> Option<WorkerSnapshot>;
}

/// Real EngineApi-backed executor.
pub struct EngineExecutor {
	home:   Home,
	engine: Arc<dyn EngineApi>,
	serial: AtomicU64,
}

impl EngineExecutor {
	/// Construct a shared executor rooted in one daemon home.
	pub fn new(home: Home, engine: Arc<dyn EngineApi>) -> Arc<Self> {
		Arc::new(Self { home, engine, serial: AtomicU64::new(1) })
	}
}

impl Executor for EngineExecutor {
	fn spawn(
		&self,
		revision: Arc<pb::FunctionRevision>,
		secrets: SecretValues,
	) -> BoxFuture<Result<Arc<dyn Worker>, WorkerError>> {
		let engine = Arc::clone(&self.engine);
		let home = self.home.clone();
		let serial = self.serial.fetch_add(1, Ordering::Relaxed);
		Box::pin(async move {
			EngineWorker::spawn(home, engine, revision, secrets, serial)
				.await
				.map(|w| w as Arc<dyn Worker>)
		})
	}

	fn restore(
		&self,
		revision: Arc<pb::FunctionRevision>,
		snapshot: WorkerSnapshot,
		secrets: SecretValues,
	) -> BoxFuture<Result<Arc<dyn Worker>, WorkerError>> {
		let engine = Arc::clone(&self.engine);
		let home = self.home.clone();
		Box::pin(async move {
			EngineWorker::restore(home, engine, revision, snapshot, secrets)
				.await
				.map(|w| w as Arc<dyn Worker>)
		})
	}
}

struct EngineWorker {
	id:                   String,
	revision_id:          String,
	revision:             Arc<pb::FunctionRevision>,
	home:                 Home,
	engine:               Arc<dyn EngineApi>,
	session:              ProtocolSession,
	capacity:             usize,
	active:               Arc<Mutex<HashMap<String, Interruptibility>>>,
	closed:               Arc<AtomicBool>,
	startup_millis:       u64,
	restored:             bool,
	async_interruptible:  AtomicBool,
	secrets:              SecretValues,
	package_sha256:       String,
	package_size:         u64,
	output_format:        String,
	output_compression:   String,
	allow_trusted_python: bool,
	cloudpickle_version:  Option<String>,
	max_batch_size:       usize,
	initial_snapshot:     RwLock<Option<WorkerSnapshot>>,
}

impl EngineWorker {
	async fn spawn(
		home: Home,
		engine: Arc<dyn EngineApi>,
		revision: Arc<pb::FunctionRevision>,
		secrets: SecretValues,
		serial: u64,
	) -> Result<Arc<Self>, WorkerError> {
		let started = Instant::now();
		let expected = SnapshotProvenance::for_revision(&revision)?;
		let spec = revision
			.spec
			.as_ref()
			.ok_or_else(|| WorkerError::platform("invalid_revision", "function spec is required"))?;
		let startup =
			duration_ms(spec.timeouts.as_ref().map_or(0, |t| t.startup_millis), DEFAULT_STARTUP);
		let (output_format, output_compression, allow_trusted_python, cloudpickle_version) =
			execution_policy(spec)?;
		let protocol_requirements = protocol_requirements(spec)?;
		let startup_deadline = started + startup;
		let name = worker_name(&expected.revision_id, serial);
		let create = sandbox_create(&home, spec, &name, &secrets)?;
		let view = blocking(remaining(startup_deadline)?, {
			let engine = Arc::clone(&engine);
			move || engine.create(create)
		})
		.await
		.map_err(|error| secrets.redact_error(error))?;
		let id = sandbox_id(&view).unwrap_or(name);
		let artifact = {
			let package = spec
				.package
				.as_ref()
				.ok_or_else(|| WorkerError::platform("invalid_revision", "package is required"))?;
			let source = package.source.as_ref().ok_or_else(|| {
				WorkerError::platform("invalid_revision", "package source is required")
			})?;
			read_artifact_at(&home, source)?
		};
		let package_sha256 = hex::encode(Sha256::digest(&artifact));
		let package_size = artifact.len() as u64;
		let prepared = async {
			blocking(remaining(startup_deadline)?, {
				let engine = Arc::clone(&engine);
				let id = id.clone();
				move || {
					engine.exec_capture(&id, ExecRequest {
						cmd: vec![
							"mkdir".into(),
							"-p".into(),
							"/opt/vmon/functions".into(),
							"/opt/vmon/values".into(),
							"/run/vmon".into(),
						],
						..ExecRequest::default()
					})
				}
			})
			.await?;
			blocking(remaining(startup_deadline)?, {
				let engine = Arc::clone(&engine);
				let id = id.clone();
				move || engine.file_write(&id, RUNNER_PATH, RUNNER)
			})
			.await?;
			blocking(remaining(startup_deadline)?, {
				let engine = Arc::clone(&engine);
				let id = id.clone();
				move || engine.file_write(&id, PACKAGE_PATH, &artifact)
			})
			.await?;
			let session = start_and_connect(
				Arc::clone(&engine),
				&id,
				remaining(startup_deadline)?,
				protocol_requirements,
			)
			.await?;
			Ok::<_, WorkerError>(session)
		}
		.await;
		let session = match prepared {
			Ok(session) => session,
			Err(error) => {
				let e = Arc::clone(&engine);
				let n = id.clone();
				let _ = tokio::task::spawn_blocking(move || e.remove(&n)).await;
				return Err(error);
			},
		};
		let worker = Arc::new(Self {
			id,
			revision_id: expected.revision_id.clone(),
			revision: Arc::clone(&revision),
			home,
			engine,
			session,
			capacity: worker_capacity(spec),
			max_batch_size: batch_capacity(spec),
			async_interruptible: AtomicBool::new(false),
			secrets,
			package_sha256,
			package_size,
			output_format,
			output_compression,
			allow_trusted_python,
			cloudpickle_version,
			active: Arc::new(Mutex::new(HashMap::new())),
			closed: Arc::new(AtomicBool::new(false)),
			startup_millis: millis(started.elapsed()),
			restored: false,
			initial_snapshot: RwLock::new(None),
		});
		let initialized = async {
			worker.define(remaining(startup_deadline)?).await?;
			if let Some(hook) = initialize_hook(spec) {
				worker
					.run_named_hook("initialize", hook, remaining(startup_deadline)?)
					.await?;
			}
			if spec
				.lifecycle_hooks
				.as_ref()
				.is_some_and(|h| h.snapshot_after_initialize)
			{
				let snapshot = tokio::time::timeout(
					remaining(startup_deadline)?,
					worker.capture_snapshot(SnapshotReason::PostInitialize),
				)
				.await
				.map_err(|_| WorkerError::timeout("startup"))??;
				*worker.initial_snapshot.write() = Some(snapshot);
			}
			Ok::<(), WorkerError>(())
		}
		.await;
		if let Err(error) = initialized {
			let engine = Arc::clone(&worker.engine);
			let id = worker.id.clone();
			let _ = tokio::task::spawn_blocking(move || engine.remove(&id)).await;
			return Err(error);
		}
		Ok(worker)
	}

	async fn restore(
		home: Home,
		engine: Arc<dyn EngineApi>,
		revision: Arc<pb::FunctionRevision>,
		snapshot: WorkerSnapshot,
		secrets: SecretValues,
	) -> Result<Arc<Self>, WorkerError> {
		ensure_snapshot_safe(revision.spec.as_ref())?;
		if !snapshot.provenance.matches(&revision) {
			return Err(WorkerError::platform(
				"snapshot_provenance_mismatch",
				"snapshot immutable inputs no longer match revision",
			));
		}
		let spec = revision
			.spec
			.as_ref()
			.ok_or_else(|| WorkerError::platform("invalid_revision", "function spec is required"))?;
		let startup =
			duration_ms(spec.timeouts.as_ref().map_or(0, |t| t.startup_millis), DEFAULT_STARTUP);
		let (output_format, output_compression, allow_trusted_python, cloudpickle_version) =
			execution_policy(spec)?;
		let protocol_requirements = protocol_requirements(spec)?;
		let restore_hook = restore_hook(Some(spec)).cloned();
		let mut extra = HashMap::new();
		extra.insert("secrets".into(), Value::Array(secrets.sandbox_wire()?));
		let restore_name = worker_name(
			&snapshot.provenance.revision_id,
			RESTORE_SERIAL.fetch_add(1, Ordering::Relaxed),
		);
		let started = Instant::now();
		let startup_deadline = started + startup;
		let view = blocking(remaining(startup_deadline)?, {
			let engine = Arc::clone(&engine);
			let name = snapshot.engine_snapshot.clone();
			move || {
				engine.restore(&name, RestoreBody {
					name: Some(restore_name),
					agent: Some(true),
					extra,
				})
			}
		})
		.await
		.map_err(|error| secrets.redact_error(error))?;
		let id = sandbox_id(&view).ok_or_else(|| {
			WorkerError::infrastructure("restore_failed", "restored sandbox view omitted identity")
		})?;
		let session = connect_existing(
			Arc::clone(&engine),
			&id,
			remaining(startup_deadline)?,
			protocol_requirements,
		)
		.await?;
		let package_bytes = engine.file_read(&id, PACKAGE_PATH).map_err(engine_error)?;
		let package_sha256 = hex::encode(Sha256::digest(&package_bytes));
		let expected_package = spec
			.package
			.as_ref()
			.and_then(|package| package.source.as_ref())
			.and_then(|source| source.digest.as_ref())
			.ok_or_else(|| {
				WorkerError::platform("invalid_revision", "package source digest is required")
			})?;
		if package_sha256 != hex::encode(&expected_package.value) {
			return Err(WorkerError::platform(
				"snapshot_package_mismatch",
				"restored package bytes do not match revision",
			));
		}
		let package_size = package_bytes.len() as u64;
		let capacity = worker_capacity(spec);
		let worker = Arc::new(Self {
			id,
			revision_id: snapshot.provenance.revision_id.clone(),
			revision: Arc::clone(&revision),
			home,
			engine,
			session,
			capacity,
			max_batch_size: batch_capacity(spec),
			async_interruptible: AtomicBool::new(false),
			secrets,
			package_sha256,
			package_size,
			output_format,
			output_compression,
			allow_trusted_python,
			cloudpickle_version,
			active: Arc::new(Mutex::new(HashMap::new())),
			closed: Arc::new(AtomicBool::new(false)),
			startup_millis: millis(started.elapsed()),
			restored: true,
			initial_snapshot: RwLock::new(Some(snapshot)),
		});
		let restored = async {
			worker.define(remaining(startup_deadline)?).await?;
			worker
				.control("after_restore", remaining(startup_deadline)?)
				.await?;
			if let Some(hook) = restore_hook.as_ref() {
				worker
					.run_named_hook("restore", hook, remaining(startup_deadline)?)
					.await?;
			}
			Ok::<(), WorkerError>(())
		}
		.await;
		if let Err(error) = restored {
			let engine = Arc::clone(&worker.engine);
			let id = worker.id.clone();
			let _ = tokio::task::spawn_blocking(move || engine.remove(&id)).await;
			return Err(error);
		}
		Ok(worker)
	}

	async fn define(&self, timeout: Duration) -> Result<(), WorkerError> {
		let spec =
			self.revision.spec.as_ref().ok_or_else(|| {
				WorkerError::platform("invalid_revision", "function spec is required")
			})?;
		let package = spec
			.package
			.as_ref()
			.ok_or_else(|| WorkerError::platform("invalid_revision", "package is required"))?;
		let mode = pb::PackageMode::try_from(package.mode).unwrap_or(pb::PackageMode::Unspecified);
		if mode == pb::PackageMode::TrustedSerialized && !self.allow_trusted_python {
			return Err(WorkerError::platform(
				"trusted_python_disabled",
				"trusted serialized package rejected by revision serializer policy",
			));
		}
		let definition = match mode {
			pb::PackageMode::Module | pb::PackageMode::Package => package_definition(
				&format!("{}:{}", package.module, package.qualname),
				&self.package_sha256,
				self.package_size,
			),
			pb::PackageMode::TrustedSerialized => {
				let abi = package
					.python
					.as_ref()
					.map(|python| python.abi_tag.as_str())
					.filter(|abi| !abi.is_empty())
					.ok_or_else(|| {
						WorkerError::platform(
							"python_abi_mismatch",
							"trusted serialized packages require an exact Python ABI tag",
						)
					})?;
				json!({"mode":"serialized","trusted":self.allow_trusted_python,"value":path_envelope(PACKAGE_PATH,&self.package_sha256,self.package_size,"cloudpickle",abi,self.cloudpickle_version.as_deref())})
			},
			pb::PackageMode::Unspecified => {
				return Err(WorkerError::platform(
					"invalid_package_mode",
					"package mode is unspecified",
				));
			},
		};
		let frame = define_frame(
			&self.revision_id,
			definition,
			self.secrets.protocol_wire(),
			self.allow_trusted_python,
			self.cloudpickle_version.as_deref(),
		);
		let terminal = self.session.request(frame)?.complete(timeout).await?;
		terminal_error(&terminal)?;
		let callable_kind = terminal
			.0
			.get("callable_kind")
			.and_then(Value::as_str)
			.unwrap_or("sync");
		self.async_interruptible.store(
			callable_interruptibility(callable_kind) == Interruptibility::Async,
			Ordering::Release,
		);
		Ok(())
	}

	async fn run_named_hook(
		&self,
		phase: &str,
		hook: &pb::LifecycleHookRef,
		timeout: Duration,
	) -> Result<(), WorkerError> {
		if !worker_hook_allowed(self.revision.spec.as_ref(), hook)? {
			return Ok(());
		}
		let define_id = format!("hook:{phase}");
		let target = format!("{}:{}", hook.module, hook.qualname);
		let define = json!({"type":"define","request_id":format!("define:{define_id}"),"definition_id":define_id,"revision":self.revision_id,"definition":{"mode":"package","archive_path":PACKAGE_PATH,"archive_sha256":self.package_sha256,"archive_size":self.package_size,"target":target},"secrets":self.secrets.protocol_wire()});
		terminal_error(&self.session.request(define)?.complete(timeout).await?)?;
		let call = json!({"type":"call","request_id":format!("hook:{phase}"),"definition_id":define_id,"revision":self.revision_id,"output_codec":"json"});
		terminal_error(&self.session.request(call)?.complete(timeout).await?)?;
		Ok(())
	}

	async fn control(&self, operation: &str, timeout: Duration) -> Result<(), WorkerError> {
		let frame = json!({"type":operation,"request_id":format!("lifecycle:{operation}")});
		terminal_error(&self.session.request(frame)?.complete(timeout).await?)?;
		Ok(())
	}

	async fn capture_snapshot(&self, reason: SnapshotReason) -> Result<WorkerSnapshot, WorkerError> {
		ensure_snapshot_safe(self.revision.spec.as_ref())?;
		let timeout = execution_timeout(&self.revision);
		if let Some(hook) = snapshot_hook(self.revision.spec.as_ref()).cloned() {
			self.run_named_hook("snapshot", &hook, timeout).await?;
		}
		self.control("before_snapshot", timeout).await?;
		let view = blocking_phase(timeout, "snapshot", {
			let engine = Arc::clone(&self.engine);
			let id = self.id.clone();
			move || engine.snapshot(&id, None, false)
		})
		.await?;
		let name = view
			.get("snapshot")
			.and_then(Value::as_str)
			.ok_or_else(|| {
				WorkerError::infrastructure("snapshot_failed", "engine omitted snapshot name")
			})?
			.to_owned();
		Ok(WorkerSnapshot {
			engine_snapshot: name,
			provenance: SnapshotProvenance::for_revision(&self.revision)?,
			reason,
			created_at_unix_millis: now_ms(),
		})
	}

	fn wire_input(
		&self,
		envelope: &pb::ValueEnvelope,
	) -> Result<(Value, Option<String>), WorkerError> {
		if pb::ValueSerializer::try_from(envelope.serializer)
			.unwrap_or(pb::ValueSerializer::Unspecified)
			== pb::ValueSerializer::Cloudpickle
		{
			if !self.allow_trusted_python {
				return Err(WorkerError::platform(
					"trusted_python_disabled",
					"cloudpickle input rejected by revision serializer policy",
				));
			}
			let supplied = envelope.python_presence.as_ref().map(
				|pb::value_envelope::PythonPresence::Python(python)| {
					python.cloudpickle_version.as_str()
				},
			);
			if supplied != self.cloudpickle_version.as_deref() {
				return Err(WorkerError::platform(
					"cloudpickle_version_mismatch",
					"cloudpickle input version does not match immutable package metadata",
				));
			}
		}
		let guest_path =
			if let Some(pb::value_envelope::Storage::Artifact(reference)) = &envelope.storage {
				let bytes = read_artifact_at(&self.home, reference)?;
				let path = guest_value_path();
				self
					.engine
					.file_write(&self.id, &path, &bytes)
					.map_err(engine_error)?;
				Some(path)
			} else {
				None
			};
		Ok((envelope_to_wire(envelope, guest_path.as_deref())?, guest_path))
	}

	fn actor_state_wire_input(
		&self,
		envelope: &pb::ValueEnvelope,
	) -> Result<(Value, Option<String>), WorkerError> {
		if pb::ValueSerializer::try_from(envelope.serializer).ok()
			!= Some(pb::ValueSerializer::Cloudpickle)
		{
			return Err(WorkerError::platform(
				"invalid_actor_state",
				"actor state must use the daemon cloudpickle envelope",
			));
		}
		if pb::ValueCompression::try_from(envelope.compression).ok()
			!= Some(pb::ValueCompression::None)
		{
			return Err(WorkerError::platform(
				"invalid_actor_state",
				"actor state must use uncompressed checkpoint bytes",
			));
		}
		let Some(pb::value_envelope::PythonPresence::Python(python)) =
			envelope.python_presence.as_ref()
		else {
			return Err(WorkerError::platform(
				"invalid_actor_state",
				"actor state is missing Python ABI metadata",
			));
		};
		let expected_abi = self
			.revision
			.spec
			.as_ref()
			.and_then(|spec| spec.package.as_ref())
			.and_then(|package| package.python.as_ref())
			.map(|metadata| metadata.abi_tag.as_str())
			.filter(|abi| !abi.is_empty());
		if Some(python.abi_tag.as_str()) != expected_abi
			|| Some(python.cloudpickle_version.as_str()) != self.cloudpickle_version.as_deref()
		{
			return Err(WorkerError::platform(
				"cloudpickle_version_mismatch",
				"actor state ABI/version does not match immutable package metadata",
			));
		}
		let bytes = match envelope.storage.as_ref() {
			Some(pb::value_envelope::Storage::InlineData(bytes)) => bytes.clone(),
			Some(pb::value_envelope::Storage::Artifact(reference)) => {
				read_artifact_at(&self.home, reference)?
			},
			None => {
				return Err(WorkerError::platform(
					"invalid_actor_state",
					"actor state has no checkpoint bytes",
				));
			},
		};
		if envelope.uncompressed_size_bytes != bytes.len() as u64
			|| envelope
				.checksum
				.as_ref()
				.is_none_or(|checksum| checksum.value.as_slice() != Sha256::digest(&bytes).as_slice())
		{
			return Err(WorkerError::platform(
				"invalid_actor_state",
				"actor state checksum or size does not match checkpoint bytes",
			));
		}
		let guest_path = if matches!(envelope.storage, Some(pb::value_envelope::Storage::Artifact(_)))
		{
			let path = guest_value_path();
			self
				.engine
				.file_write(&self.id, &path, &bytes)
				.map_err(engine_error)?;
			Some(path)
		} else {
			None
		};
		let mut wire = envelope_to_wire(envelope, guest_path.as_deref())?;
		wire["internal_purpose"] = json!("daemon_actor_state");
		Ok((wire, guest_path))
	}

	fn wire_payload(
		&self,
		payload: &pb::call_input::Payload,
	) -> Result<(serde_json::Map<String, Value>, Vec<String>), WorkerError> {
		let mut fields = serde_json::Map::new();
		let mut cleanup = Vec::new();
		match payload {
			pb::call_input::Payload::Value(value) => {
				let (value, path) = self.wire_input(value)?;
				cleanup.extend(path);
				fields.insert("input".into(), value);
			},
			pb::call_input::Payload::Arguments(arguments) => {
				let mut positional = Vec::with_capacity(arguments.positional.len());
				for value in &arguments.positional {
					let (value, path) = self.wire_input(value)?;
					cleanup.extend(path);
					positional.push(value);
				}
				let mut named = serde_json::Map::new();
				for (key, value) in &arguments.named {
					let (value, path) = self.wire_input(value)?;
					cleanup.extend(path);
					named.insert(key.clone(), value);
				}
				fields.insert("positional".into(), Value::Array(positional));
				fields.insert("named".into(), Value::Object(named));
			},
		}
		Ok((fields, cleanup))
	}

	fn wire_arguments(
		&self,
		arguments: &pb::InvocationArguments,
	) -> Result<(Value, Vec<String>), WorkerError> {
		let (payload, cleanup) =
			self.wire_payload(&pb::call_input::Payload::Arguments(arguments.clone()))?;
		Ok((Value::Object(payload), cleanup))
	}

	fn execute_frame(
		&self,
		frame: Value,
		request_id: String,
		interruptibility: Interruptibility,
		attempt: u32,
		cleanup_paths: Vec<String>,
	) -> Result<Execution, WorkerError> {
		if self.closed.load(Ordering::Acquire) {
			cleanup_guest_paths(self.engine.as_ref(), &self.id, &cleanup_paths);
			return Err(WorkerError::infrastructure("worker_retired", "worker is retired"));
		}
		{
			let mut active = self.active.lock();
			if active.len() >= self.capacity {
				cleanup_guest_paths(self.engine.as_ref(), &self.id, &cleanup_paths);
				return Err(WorkerError::infrastructure(
					"worker_busy",
					"worker concurrency is exhausted",
				));
			}
			if active
				.insert(request_id.clone(), interruptibility)
				.is_some()
			{
				cleanup_guest_paths(self.engine.as_ref(), &self.id, &cleanup_paths);
				return Err(WorkerError::platform("duplicate_request", "request is already active"));
			}
		}
		let protocol = match self.session.request(frame) {
			Ok(request) => request,
			Err(error) => {
				self.active.lock().remove(&request_id);
				cleanup_guest_paths(self.engine.as_ref(), &self.id, &cleanup_paths);
				return Err(error.into());
			},
		};
		let (events_tx, events) = flume::bounded(DEFAULT_EVENT_CAPACITY);
		let raw_events = protocol.events.clone();
		let event_error = Arc::new(Mutex::new(None));
		let event_failure = Arc::clone(&event_error);
		let event_engine = Arc::clone(&self.engine);
		let event_worker_id = self.id.clone();
		let event_artifact_root = self.home.root().join("functions/artifacts");
		let event_worker = match std::thread::Builder::new()
			.name(format!("vmon-events-{request_id}"))
			.spawn(move || {
				while let Ok(mut frame) = raw_events.recv() {
					if frame.event() == Some("yield")
						&& let Err(error) = materialize_output(
							event_engine.as_ref(),
							&event_worker_id,
							&event_artifact_root,
							&mut frame,
						) {
						*event_failure.lock() = Some(error);
						break;
					}
					if let Some(event) = typed_event(&frame)
						&& events_tx.try_send(event).is_err()
					{
						*event_failure.lock() = Some(WorkerError::infrastructure(
							"event_backpressure",
							"worker event consumer did not keep up",
						));
						break;
					}
				}
			}) {
			Ok(worker) => worker,
			Err(error) => {
				self.active.lock().remove(&request_id);
				let _ = self.session.cancel(&request_id);
				cleanup_guest_paths(self.engine.as_ref(), &self.id, &cleanup_paths);
				return Err(WorkerError::infrastructure("event_dispatch", error.to_string()));
			},
		};
		let active = Arc::clone(&self.active);
		let engine = Arc::clone(&self.engine);
		let id = self.id.clone();
		let startup = self.startup_millis;
		let restored = self.restored;
		let timeout = execution_timeout(&self.revision);
		let session = self.session.clone();
		let closed = Arc::clone(&self.closed);
		let artifact_root = self.home.root().join("functions/artifacts");
		let completion = Box::pin(async move {
			let started = Instant::now();
			let mut result = protocol.complete(timeout).await;
			active.lock().remove(&request_id);
			if matches!(&result, Err(ProtocolError::Timeout)) {
				match interruptibility {
					Interruptibility::Async => {
						let _ = session.cancel(&request_id);
					},
					Interruptibility::Sync => {
						closed.store(true, Ordering::Release);
						let _ = session.kill();
						let terminate = Arc::clone(&engine);
						let worker = id.clone();
						let _ = tokio::task::spawn_blocking(move || {
							terminate.terminate(&worker, "synchronous function deadline")
						})
						.await;
					},
				}
			}
			let _ = tokio::task::spawn_blocking(move || event_worker.join()).await;
			let cleanup_engine = Arc::clone(&engine);
			let cleanup_id = id.clone();
			let _ = tokio::task::spawn_blocking(move || {
				cleanup_guest_paths(cleanup_engine.as_ref(), &cleanup_id, &cleanup_paths);
			})
			.await;
			let event_failure = event_error.lock().take();
			if let Some(error) = event_failure {
				result = Err(ProtocolError::Disconnected(error.to_string()));
			}
			if let Ok(mut frame) = result {
				let output_engine = Arc::clone(&engine);
				let output_id = id.clone();
				result = match tokio::task::spawn_blocking(move || {
					materialize_output(output_engine.as_ref(), &output_id, &artifact_root, &mut frame)?;
					Ok::<Frame, WorkerError>(frame)
				})
				.await
				{
					Ok(Ok(frame)) => Ok(frame),
					Ok(Err(error)) => {
						return Ok(WorkerOutcome::PlatformError {
							error,
							stats: AttemptStats {
								attempt,
								startup_millis: startup,
								execution_millis: millis(started.elapsed()),
								snapshot_restore: restored,
								..AttemptStats::default()
							},
						});
					},
					Err(error) => {
						return Err(WorkerError::infrastructure(
							"result_materialization",
							error.to_string(),
						));
					},
				};
			}
			if result.as_ref().ok().is_some_and(|frame| {
				frame.0.get("worker_kill_required").and_then(Value::as_bool) == Some(true)
			}) {
				closed.store(true, Ordering::Release);
				let _ = session.kill();
				let terminate = Arc::clone(&engine);
				let worker = id.clone();
				let _ = tokio::task::spawn_blocking(move || {
					terminate.terminate(&worker, "runner required worker termination")
				})
				.await;
			}
			let mut stats = AttemptStats {
				attempt,
				startup_millis: startup,
				execution_millis: millis(started.elapsed()),
				snapshot_restore: restored,
				..AttemptStats::default()
			};
			if let Ok(metrics) = tokio::task::spawn_blocking(move || engine.metrics(&id)).await
				&& let Ok(metrics) = metrics
			{
				stats.cpu_millis = metrics
					.get("cpu_millis")
					.and_then(Value::as_u64)
					.unwrap_or(0);
				stats.peak_memory_bytes = metrics
					.get("peak_memory_bytes")
					.or_else(|| metrics.get("memory_bytes"))
					.and_then(Value::as_u64)
					.unwrap_or(0);
			}
			match result {
				Ok(frame) => Ok(outcome(frame, stats)),
				Err(error) => {
					let error: WorkerError = error.into();
					Ok(match error.kind {
						WorkerErrorKind::Infrastructure => {
							WorkerOutcome::InfrastructureError { error, stats }
						},
						WorkerErrorKind::ActorLost => WorkerOutcome::ActorLost { error, stats },
						_ => WorkerOutcome::PlatformError { error, stats },
					})
				},
			}
		});
		Ok(Execution { events, completion })
	}
}

impl Worker for EngineWorker {
	fn id(&self) -> &str {
		&self.id
	}

	fn revision_id(&self) -> &str {
		&self.revision_id
	}

	fn interruptibility(&self) -> Interruptibility {
		if self.async_interruptible.load(Ordering::Acquire) {
			Interruptibility::Async
		} else {
			Interruptibility::Sync
		}
	}

	fn is_reusable(&self) -> bool {
		!self.closed.load(Ordering::Acquire)
	}

	fn capacity(&self) -> usize {
		self.capacity
	}

	fn execute(&self, request: ExecuteRequest) -> BoxFuture<Result<Execution, WorkerError>> {
		let result = (|| {
			let (payload, mut cleanup_paths) = self.wire_payload(&request.input)?;
			let deadline = request.deadline.map(system_ms);
			let mut frame = json!({"type":if request.service.is_some(){"service_call"}else{"call"},"request_id":request.request_id,"function_id":request.function_id,"call_id":request.call_id,"input_id":request.input_id,"input_index":request.input_index,"attempt":request.attempt,"definition_id":"main","revision":self.revision_id,"execution_mode":request.mode.wire(),"deadline_unix_ms":deadline,"parent_request_id":request.parent_request_id,"parent_call_id":request.parent_call_id,"output_format":self.output_format,"output_compression":self.output_compression,"trusted":self.allow_trusted_python,"cloudpickle_version":self.cloudpickle_version,"secrets":self.secrets.protocol_wire()});
			if let Some(object) = frame.as_object_mut() {
				object.extend(payload);
			}
			if let Some(service) = request.service {
				frame["service_key"] = json!(service.service_key);
				frame["method"] = json!(service.method);
				if let Some(constructor) = service.constructor {
					let (value, paths) = self.wire_arguments(&constructor)?;
					cleanup_paths.extend(paths);
					frame["constructor"] = value;
				}
			}
			self.execute_frame(
				frame,
				request.request_id,
				self.interruptibility(),
				request.attempt,
				cleanup_paths,
			)
		})();
		Box::pin(async move { result })
	}

	fn execute_batch(
		&self,
		request: BatchExecuteRequest,
	) -> BoxFuture<Result<BatchExecution, WorkerError>> {
		let result = (|| {
			validate_batch_inputs(&request.inputs, self.max_batch_size)?;
			if self.closed.load(Ordering::Acquire) {
				return Err(WorkerError::infrastructure("worker_retired", "worker is retired"));
			}
			{
				let mut active = self.active.lock();
				if active.len() >= self.capacity {
					return Err(WorkerError::infrastructure(
						"worker_busy",
						"worker concurrency is exhausted",
					));
				}
				active.insert(request.request_id.clone(), self.interruptibility());
			}
			let mut cleanup_paths = Vec::new();
			let mut items = Vec::with_capacity(request.inputs.len());
			for input in &request.inputs {
				let (payload, paths) = match self.wire_payload(&input.input) {
					Ok(value) => value,
					Err(error) => {
						self.active.lock().remove(&request.request_id);
						cleanup_guest_paths(self.engine.as_ref(), &self.id, &cleanup_paths);
						return Err(error);
					},
				};
				cleanup_paths.extend(paths);
				let mut item = json!({"request_id":input.request_id,"function_id":input.function_id,"call_id":input.call_id,"input_id":input.input_id,"input_index":input.input_index,"attempt":input.attempt,"parent_request_id":input.parent_request_id,"parent_call_id":input.parent_call_id,"deadline_unix_ms":input.deadline.map(system_ms)});
				if let Some(object) = item.as_object_mut() {
					object.extend(payload);
				}
				if let Some(service) = &input.service {
					item["service_key"] = json!(service.service_key);
					item["method"] = json!(service.method);
					if let Some(constructor) = &service.constructor {
						let (value, paths) = self.wire_arguments(constructor)?;
						cleanup_paths.extend(paths);
						item["constructor"] = value;
					}
				}
				items.push(item);
			}
			let frame = json!({"type":"batch","request_id":request.request_id,"definition_id":"main","revision":self.revision_id,"trusted":self.allow_trusted_python,"output_format":self.output_format,"output_compression":self.output_compression,"cloudpickle_version":self.cloudpickle_version,"inputs":items,"secrets":self.secrets.protocol_wire()});
			let protocol = match self.session.request(frame) {
				Ok(protocol) => protocol,
				Err(error) => {
					self.active.lock().remove(&request.request_id);
					cleanup_guest_paths(self.engine.as_ref(), &self.id, &cleanup_paths);
					return Err(error.into());
				},
			};
			let (events_tx, events) = flume::bounded(DEFAULT_EVENT_CAPACITY);
			let raw_events = protocol.events.clone();
			let event_error = Arc::new(Mutex::new(None));
			let event_failure = Arc::clone(&event_error);
			let event_worker = std::thread::Builder::new()
				.name(format!("vmon-batch-events-{}", request.request_id))
				.spawn(move || {
					while let Ok(frame) = raw_events.recv() {
						if let Err(error) = dispatch_batch_event(&events_tx, &frame) {
							*event_failure.lock() = Some(error);
							break;
						}
					}
				})
				.map_err(|error| WorkerError::infrastructure("event_dispatch", error.to_string()))?;
			let active = Arc::clone(&self.active);
			let engine = Arc::clone(&self.engine);
			let worker_id = self.id.clone();
			let batch_id = request.request_id.clone();
			let inputs = request.inputs;
			let timeout = execution_timeout(&self.revision);
			let startup = self.startup_millis;
			let restored = self.restored;
			let artifact_root = self.home.root().join("functions/artifacts");
			let session = self.session.clone();
			let closed = Arc::clone(&self.closed);
			let interruptibility = self.interruptibility();
			let completion = Box::pin(async move {
				let started = Instant::now();
				let terminal = protocol.complete(timeout).await;
				if matches!(&terminal, Err(ProtocolError::Timeout)) {
					match interruptibility {
						Interruptibility::Async => {
							if session.cancel(&batch_id).is_err() {
								closed.store(true, Ordering::Release);
								let _ = session.kill();
							}
						},
						Interruptibility::Sync => {
							closed.store(true, Ordering::Release);
							let _ = session.kill();
							let terminate = Arc::clone(&engine);
							let worker = worker_id.clone();
							let _ = tokio::task::spawn_blocking(move || {
								terminate.terminate(&worker, "synchronous batch deadline")
							})
							.await;
						},
					}
				}
				active.lock().remove(&batch_id);
				let _ = tokio::task::spawn_blocking(move || event_worker.join()).await;
				let cleanup_engine = Arc::clone(&engine);
				let cleanup_id = worker_id.clone();
				let _ = tokio::task::spawn_blocking(move || {
					cleanup_guest_paths(cleanup_engine.as_ref(), &cleanup_id, &cleanup_paths);
				})
				.await;
				let event_error = event_error.lock().take();
				if let Some(error) = event_error {
					return Err(error);
				}
				let stats = AttemptStats {
					attempt: 0,
					startup_millis: startup,
					execution_millis: millis(started.elapsed()),
					snapshot_restore: restored,
					..AttemptStats::default()
				};
				let mut frame = terminal.map_err(WorkerError::from)?;
				if frame.event() == Some("error") {
					let error = terminal_error(&frame).unwrap_err();
					let items = inputs
						.into_iter()
						.map(|input| BatchItemOutcome {
							request_id:  input.request_id,
							input_id:    input.input_id,
							input_index: input.input_index,
							outcome:     WorkerOutcome::UserError { error: error.clone(), stats },
						})
						.collect();
					return Ok(BatchOutcome { items, stats });
				}
				if frame.event() == Some("cancelled") {
					return Ok(cancelled_batch_outcome(frame, inputs, stats));
				}
				if frame.event() != Some("batch_result") {
					return Err(WorkerError::platform(
						"invalid_batch_result",
						"runner omitted batch_result terminal",
					));
				}
				let results = frame
					.0
					.get_mut("results")
					.and_then(Value::as_array_mut)
					.ok_or_else(|| {
						WorkerError::platform("invalid_batch_result", "runner batch results are missing")
					})?;
				if results.len() != inputs.len() {
					return Err(WorkerError::platform(
						"batch_cardinality",
						"runner batch result cardinality mismatch",
					));
				}
				let mut outcomes = Vec::with_capacity(inputs.len());
				for (input, result) in inputs.into_iter().zip(results.iter_mut()) {
					if result.get("request_id").and_then(Value::as_str)
						!= Some(input.request_id.as_str())
						|| result.get("input_id").and_then(Value::as_str) != Some(input.input_id.as_str())
						|| result.get("input_index").and_then(Value::as_u64) != Some(input.input_index)
					{
						return Err(WorkerError::platform(
							"batch_order",
							"runner batch result metadata/order mismatch",
						));
					}
					let mut value_frame =
						Frame(json!({"value":result.get("value").cloned().unwrap_or(Value::Null)}));
					materialize_output(engine.as_ref(), &worker_id, &artifact_root, &mut value_frame)?;
					let value = wire_to_envelope(value_frame.0.get("value").unwrap())?;
					outcomes.push(BatchItemOutcome {
						request_id:  input.request_id,
						input_id:    input.input_id,
						input_index: input.input_index,
						outcome:     WorkerOutcome::Success { value, stats },
					});
				}
				Ok(BatchOutcome { items: outcomes, stats })
			});
			Ok(BatchExecution { events, completion })
		})();
		Box::pin(async move { result })
	}

	fn cancel(&self, request_id: &str) -> BoxFuture<Result<(), WorkerError>> {
		let mode = self.active.lock().get(request_id).copied();
		let session = self.session.clone();
		let engine = Arc::clone(&self.engine);
		let id = self.id.clone();
		let closed = Arc::clone(&self.closed);
		let request_id = request_id.to_owned();
		Box::pin(async move {
			match mode {
				Some(Interruptibility::Async) => session.cancel(&request_id).map_err(Into::into),
				Some(Interruptibility::Sync) => {
					closed.store(true, Ordering::Release);
					session.kill()?;
					let _ = tokio::task::spawn_blocking(move || {
						engine.terminate(&id, "synchronous function cancellation")
					})
					.await;
					Ok(())
				},
				None => Ok(()),
			}
		})
	}

	fn snapshot(&self, reason: SnapshotReason) -> BoxFuture<Result<WorkerSnapshot, WorkerError>> {
		if let Err(error) = ensure_snapshot_safe(self.revision.spec.as_ref()) {
			return Box::pin(async move { Err(error) });
		}
		let engine = Arc::clone(&self.engine);
		let id = self.id.clone();
		let session = self.session.clone();
		let revision = Arc::clone(&self.revision);
		let hook = snapshot_hook(revision.spec.as_ref()).cloned();
		let hook = match filter_worker_hook(revision.spec.as_ref(), hook) {
			Ok(hook) => hook,
			Err(error) => return Box::pin(async move { Err(error) }),
		};
		let package_sha256 = self.package_sha256.clone();
		let package_size = self.package_size;
		let secrets = self.secrets.protocol_wire();
		Box::pin(async move {
			let timeout = execution_timeout(&revision);
			if let Some(hook) = hook {
				let definition_id = "hook:snapshot";
				let define = json!({"type":"define","request_id":"define:hook:snapshot","definition_id":definition_id,"revision":revision.r#ref.as_ref().map(|r|r.revision_id.as_str()).unwrap_or_default(),"definition":{"mode":"package","archive_path":PACKAGE_PATH,"archive_sha256":package_sha256,"archive_size":package_size,"target":format!("{}:{}",hook.module,hook.qualname)},"secrets":secrets});
				terminal_error(&session.request(define)?.complete(timeout).await?)?;
				terminal_error(&session.request(json!({"type":"call","request_id":"hook:snapshot","definition_id":definition_id,"output_codec":"json"}))?.complete(timeout).await?)?;
			}
			terminal_error(
				&session
					.request(json!({"type":"before_snapshot","request_id":"lifecycle:before_snapshot"}))?
					.complete(timeout)
					.await?,
			)?;
			let view = blocking_phase(timeout, "snapshot", {
				let engine = Arc::clone(&engine);
				let id = id.clone();
				move || engine.snapshot(&id, None, false)
			})
			.await?;
			let name = view
				.get("snapshot")
				.and_then(Value::as_str)
				.ok_or_else(|| {
					WorkerError::infrastructure("snapshot_failed", "engine omitted snapshot name")
				})?
				.to_owned();
			Ok(WorkerSnapshot {
				engine_snapshot: name,
				provenance: SnapshotProvenance::for_revision(&revision)?,
				reason,
				created_at_unix_millis: now_ms(),
			})
		})
	}

	fn actor(&self, request: ActorRequest) -> BoxFuture<Result<Execution, WorkerError>> {
		let result = (|| {
			let request_id = request.request_id;
			let mut cleanup_paths = Vec::new();
			let (kind, extra) = match request.operation {
				ActorOperation::Create => ("actor_create", json!({})),
				ActorOperation::Method { name } => ("actor_call", json!({"method":name})),
				ActorOperation::Checkpoint { checkpoint_id } => {
					("actor_checkpoint", json!({"checkpoint_id":checkpoint_id}))
				},
				ActorOperation::Restore { checkpoint_id, state } => {
					let (state, path) = self.actor_state_wire_input(&state)?;
					cleanup_paths.extend(path);
					("actor_restore", json!({"checkpoint_id":checkpoint_id,"state":state}))
				},
				ActorOperation::Fork { child_actor_id } => {
					("actor_fork", json!({"child_actor_id":child_actor_id}))
				},
				ActorOperation::Shutdown => ("actor_shutdown", json!({})),
			};
			let mut frame = json!({"type":kind,"request_id":request_id,"call_id":request.call_id,"actor_id":request.actor_id,"definition_id":"main","revision":self.revision_id,"deadline_unix_ms":request.deadline.map(system_ms),"output_format":self.output_format,"output_compression":self.output_compression,"trusted":self.allow_trusted_python,"cloudpickle_version":self.cloudpickle_version,"secrets":self.secrets.protocol_wire()});
			if let Some(input) = request.input {
				let (payload, paths) = self.wire_payload(&input)?;
				cleanup_paths.extend(paths);
				if let Some(object) = frame.as_object_mut() {
					object.extend(payload);
				}
			}
			if let (Some(target), Some(source)) = (frame.as_object_mut(), extra.as_object()) {
				target.extend(source.clone());
			}
			self.execute_frame(frame, request_id, Interruptibility::Async, 1, cleanup_paths)
		})();
		Box::pin(async move { result })
	}

	fn retire(&self) -> BoxFuture<Result<(), WorkerError>> {
		let engine = Arc::clone(&self.engine);
		let id = self.id.clone();
		let session = self.session.clone();
		let revision = Arc::clone(&self.revision);
		let closed = Arc::clone(&self.closed);
		let hook = shutdown_hook(revision.spec.as_ref()).cloned();
		let hook = match filter_worker_hook(revision.spec.as_ref(), hook) {
			Ok(hook) => hook,
			Err(error) => return Box::pin(async move { Err(error) }),
		};
		let package_sha256 = self.package_sha256.clone();
		let package_size = self.package_size;
		let secrets = self.secrets.protocol_wire();
		Box::pin(async move {
			if closed.swap(true, Ordering::AcqRel) {
				return Ok(());
			}
			let timeout = shutdown_timeout(&revision);
			if let Some(hook) = hook {
				let definition_id = "hook:shutdown";
				let define = json!({"type":"define","request_id":"define:hook:shutdown","definition_id":definition_id,"revision":revision.r#ref.as_ref().map(|r|r.revision_id.as_str()).unwrap_or_default(),"definition":{"mode":"package","archive_path":PACKAGE_PATH,"archive_sha256":package_sha256,"archive_size":package_size,"target":format!("{}:{}",hook.module,hook.qualname)},"secrets":secrets});
				terminal_error(&session.request(define)?.complete(timeout).await?)?;
				let call = json!({"type":"call","request_id":"hook:shutdown","definition_id":definition_id,"output_codec":"json"});
				terminal_error(&session.request(call)?.complete(timeout).await?)?;
			}
			let _ = session.shutdown();
			let result =
				tokio::time::timeout(timeout, tokio::task::spawn_blocking(move || engine.remove(&id)))
					.await;
			match result {
				Ok(Ok(Ok(_))) => Ok(()),
				Ok(Ok(Err(e))) => Err(engine_error(e)),
				Ok(Err(e)) => Err(WorkerError::infrastructure("worker_teardown", e.to_string())),
				Err(_) => Err(WorkerError::timeout("shutdown")),
			}
		})
	}

	fn initial_snapshot(&self) -> Option<WorkerSnapshot> {
		self.initial_snapshot.read().clone()
	}
}

const fn engine_architecture(architecture: pb::CpuArchitecture) -> Option<&'static str> {
	match architecture {
		pb::CpuArchitecture::Amd64 => Some("x86_64"),
		pb::CpuArchitecture::Arm64 => Some("aarch64"),
		pb::CpuArchitecture::Unspecified => None,
	}
}
fn apply_sandbox_resources(create: &mut SandboxCreate, resources: &pb::ResourceSpec) {
	create.cpus = resources.cpus.max(1);
	create.memory = bytes_to_mib(resources.memory_bytes, 512);
	create.disk_mb = bytes_to_mib(resources.ephemeral_disk_bytes, 1024);
	create.arch = engine_architecture(
		pb::CpuArchitecture::try_from(resources.architecture)
			.unwrap_or(pb::CpuArchitecture::Unspecified),
	)
	.map(str::to_owned);
	if let Some(network) = &resources.network {
		create.block_network = network.block_network;
		create.egress_allow = Some(network.egress_cidrs.clone()).filter(|v| !v.is_empty());
		create.egress_allow_domains = Some(network.egress_domains.clone()).filter(|v| !v.is_empty());
		create.inbound_cidr_allowlist = Some(network.inbound_cidrs.clone()).filter(|v| !v.is_empty());
	}
}

fn sandbox_create(
	home: &Home,
	spec: &pb::FunctionSpec,
	name: &str,
	secrets: &SecretValues,
) -> Result<SandboxCreate, WorkerError> {
	let image_spec = spec
		.image
		.as_ref()
		.ok_or_else(|| WorkerError::platform("invalid_image", "image is required"))?;
	let architecture = spec
		.resources
		.as_ref()
		.map_or(pb::CpuArchitecture::Unspecified, |r| {
			pb::CpuArchitecture::try_from(r.architecture).unwrap_or(pb::CpuArchitecture::Unspecified)
		});
	let image = image::realize(home, image_spec, architecture).map_err(engine_error)?;
	sandbox_create_from_image(spec, name, secrets, image)
}

fn sandbox_create_from_image(
	spec: &pb::FunctionSpec,
	name: &str,
	secrets: &SecretValues,
	image: image::RealizedImage,
) -> Result<SandboxCreate, WorkerError> {
	let mut create = SandboxCreate {
		name: Some(name.to_owned()),
		secrets: Some(secrets.sandbox_wire()?),
		image: image.image,
		template: image.template,
		dockerfile: image.dockerfile,
		context: image.context.unwrap_or_default(),
		env: Some(image.environment.into_iter().collect()),
		// Function HOST/ZONE placement is a mesh concern. A local function VM
		// must use the engine's non-HA sandbox tier.
		ha: Some("off".into()),
		..SandboxCreate::default()
	};
	if let Some(resources) = &spec.resources {
		apply_sandbox_resources(&mut create, resources);
	}
	create.timeout_secs = spec.timeouts.as_ref().map(|t| {
		(t.execution_millis + t.startup_millis)
			.div_ceil(1000)
			.max(1)
	});
	Ok(create)
}

async fn start_and_connect(
	engine: Arc<dyn EngineApi>,
	id: &str,
	timeout: Duration,
	requirements: ProtocolRequirements,
) -> Result<ProtocolSession, WorkerError> {
	blocking(timeout, {
		let engine = Arc::clone(&engine);
		let id = id.to_owned();
		move || {
			engine.exec_capture(&id, ExecRequest {
				cmd: vec![
					"python3".into(),
					"-c".into(),
					DETACHED_RUNNER.into(),
					RUNNER_PATH.into(),
					SOCKET_PATH.into(),
				],
				..ExecRequest::default()
			})
		}
	})
	.await?;
	connect_existing(engine, id, timeout, requirements).await
}
async fn connect_existing(
	engine: Arc<dyn EngineApi>,
	id: &str,
	timeout: Duration,
	requirements: ProtocolRequirements,
) -> Result<ProtocolSession, WorkerError> {
	let relay_code = "import os,socket,select,sys;s=socket.socket(socket.AF_UNIX);s.connect(sys.\
	                  argv[1]);fds=[0,s.fileno()];\nwhile True:\n r,_,_=select.select(fds,[],[])\n \
	                  if 0 in r:\n  b=os.read(0,65536)\n  if not b: break\n  s.sendall(b)\n if \
	                  s.fileno() in r:\n  b=s.recv(65536)\n  if not b: break\n  os.write(1,b)";
	let deadline = Instant::now() + timeout;
	loop {
		let stream = blocking(remaining(deadline)?.min(Duration::from_secs(5)), {
			let engine = Arc::clone(&engine);
			let id = id.to_owned();
			let code = relay_code.to_owned();
			move || {
				engine.exec_stream(&id, ExecRequest {
					cmd: vec!["python3".into(), "-c".into(), code, SOCKET_PATH.into()],
					..ExecRequest::default()
				})
			}
		})
		.await?;
		match ProtocolSession::connect(
			stream,
			deadline.saturating_duration_since(Instant::now()),
			DEFAULT_EVENT_CAPACITY,
			requirements.clone(),
		)
		.await
		{
			Ok(session) => return Ok(session),
			Err(ProtocolError::Disconnected(_) | ProtocolError::Timeout)
				if Instant::now() < deadline =>
			{
				tokio::time::sleep(Duration::from_millis(25)).await;
			},
			Err(e) => return Err(e.into()),
		}
	}
}

fn ensure_snapshot_safe(spec: Option<&pb::FunctionSpec>) -> Result<(), WorkerError> {
	if spec.is_some_and(|spec| !spec.secrets.is_empty()) {
		Err(WorkerError::platform(
			"snapshot_with_secrets_unsupported",
			"VM memory snapshots are disabled for revisions with transient secrets",
		))
	} else {
		Ok(())
	}
}

async fn blocking<T: Send + 'static>(
	timeout: Duration,
	operation: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> Result<T, WorkerError> {
	blocking_phase(timeout, "startup", operation).await
}
async fn blocking_phase<T: Send + 'static>(
	timeout: Duration,
	phase: &'static str,
	operation: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> Result<T, WorkerError> {
	match tokio::time::timeout(timeout, tokio::task::spawn_blocking(operation)).await {
		Ok(Ok(Ok(value))) => Ok(value),
		Ok(Ok(Err(error))) => Err(engine_error(error)),
		Ok(Err(error)) => Err(WorkerError::infrastructure("engine_task", error.to_string())),
		Err(_) => Err(WorkerError::timeout(phase)),
	}
}
fn engine_error(error: crate::EngineError) -> WorkerError {
	WorkerError::infrastructure(error.code.as_str(), error.to_string())
}

fn path_envelope(
	path: &str,
	sha256: &str,
	size: u64,
	format: &str,
	python_abi: &str,
	cloudpickle_version: Option<&str>,
) -> Value {
	json!({"version":1,"format":format,"path":path,"remove_after_read":false,"sha256":format!("sha256:{sha256}"),"uncompressed_size":size,"compression":"none","python_abi":python_abi,"cloudpickle_version":cloudpickle_version})
}
fn package_definition(target: &str, sha256: &str, size: u64) -> Value {
	json!({"mode":"package","archive_path":PACKAGE_PATH,"archive_sha256":sha256,"archive_size":size,"target":target})
}
fn guest_value_path() -> String {
	format!("/opt/vmon/values/{}", hex::encode(rand::random::<[u8; 16]>()))
}
fn cleanup_guest_paths(engine: &dyn EngineApi, worker_id: &str, paths: &[String]) {
	for path in paths {
		let _ = engine.file_delete(worker_id, path, false);
	}
}
fn protocol_requirements(spec: &pb::FunctionSpec) -> Result<ProtocolRequirements, WorkerError> {
	let serializer = spec.serializer.as_ref().ok_or_else(|| {
		WorkerError::platform("invalid_serializer", "serializer policy is required")
	})?;
	let format = |value| match pb::ValueSerializer::try_from(value)
		.unwrap_or(pb::ValueSerializer::Unspecified)
	{
		pb::ValueSerializer::Json => Ok("json"),
		pb::ValueSerializer::Cbor => Ok("cbor"),
		pb::ValueSerializer::Cloudpickle => Ok("cloudpickle"),
		pb::ValueSerializer::Unspecified => {
			Err(WorkerError::platform("invalid_serializer", "serializer policy is unspecified"))
		},
	};
	let mut requirements = ProtocolRequirements::default()
		.require_format(format(serializer.input_serializer)?)
		.require_format(format(serializer.result_serializer)?);
	let compression =
		pb::ValueCompression::try_from(serializer.compression).unwrap_or(pb::ValueCompression::None);
	if compression == pb::ValueCompression::Zstd {
		requirements = requirements.require_compression("zstd");
	}
	let trusted_package = spec.package.as_ref().is_some_and(|package| {
		pb::PackageMode::try_from(package.mode).unwrap_or(pb::PackageMode::Unspecified)
			== pb::PackageMode::TrustedSerialized
	});
	if trusted_package || serializer.allow_trusted_python {
		requirements = requirements.require_format("cloudpickle");
	}
	Ok(requirements)
}

fn execution_policy(
	spec: &pb::FunctionSpec,
) -> Result<(String, String, bool, Option<String>), WorkerError> {
	let serializer = spec.serializer.as_ref().ok_or_else(|| {
		WorkerError::platform("invalid_serializer", "serializer policy is required")
	})?;
	let output = match pb::ValueSerializer::try_from(serializer.result_serializer)
		.unwrap_or(pb::ValueSerializer::Unspecified)
	{
		pb::ValueSerializer::Json => "json",
		pb::ValueSerializer::Cbor => "cbor",
		pb::ValueSerializer::Cloudpickle => "cloudpickle",
		pb::ValueSerializer::Unspecified => {
			return Err(WorkerError::platform(
				"invalid_serializer",
				"result serializer is unspecified",
			));
		},
	};
	let compression = match pb::ValueCompression::try_from(serializer.compression)
		.unwrap_or(pb::ValueCompression::None)
	{
		pb::ValueCompression::None => "none",
		pb::ValueCompression::Gzip => "gzip",
		pb::ValueCompression::Zstd => "zstd",
	};
	if output == "cloudpickle" && !serializer.allow_trusted_python {
		return Err(WorkerError::platform(
			"trusted_python_disabled",
			"cloudpickle result serializer requires trusted Python policy",
		));
	}
	let cloudpickle = spec
		.package
		.as_ref()
		.and_then(|package| package.python.as_ref())
		.and_then(|python| {
			python.cloudpickle_version_presence.as_ref().map(
				|pb::python_code_metadata::CloudpickleVersionPresence::CloudpickleVersion(version)| {
					version.clone()
				},
			)
		});
	if (output == "cloudpickle" || serializer.allow_trusted_python) && cloudpickle.is_none() {
		return Err(WorkerError::platform(
			"cloudpickle_version_missing",
			"trusted Python policy requires exact cloudpickle version metadata",
		));
	}
	Ok((output.into(), compression.into(), serializer.allow_trusted_python, cloudpickle))
}
fn cancelled_batch_outcome(
	frame: Frame,
	inputs: Vec<ExecuteRequest>,
	stats: AttemptStats,
) -> BatchOutcome {
	let reason = frame
		.0
		.get("reason")
		.and_then(Value::as_str)
		.unwrap_or("cancelled")
		.to_owned();
	let items = inputs
		.into_iter()
		.map(|input| BatchItemOutcome {
			request_id:  input.request_id,
			input_id:    input.input_id,
			input_index: input.input_index,
			outcome:     WorkerOutcome::Cancelled { reason: reason.clone(), stats },
		})
		.collect();
	BatchOutcome { items, stats }
}

fn validate_batch_inputs(
	inputs: &[ExecuteRequest],
	max_batch_size: usize,
) -> Result<(), WorkerError> {
	if max_batch_size == 0 {
		return Err(WorkerError::platform("batching_disabled", "revision batching is disabled"));
	}
	if inputs.is_empty() || inputs.len() > max_batch_size {
		return Err(WorkerError::platform(
			"invalid_batch_size",
			"grouped input count is outside revision bounds",
		));
	}
	let function = &inputs[0].function_id;
	let service = &inputs[0].service;
	let mut requests = std::collections::HashSet::new();
	let mut ids = std::collections::HashSet::new();
	let mut indices = std::collections::HashSet::new();
	for input in inputs {
		if &input.function_id != function || &input.service != service {
			return Err(WorkerError::platform(
				"mixed_batch",
				"grouped inputs must target one function and identical service authority",
			));
		}
		if !requests.insert(&input.request_id)
			|| !ids.insert(&input.input_id)
			|| !indices.insert(input.input_index)
		{
			return Err(WorkerError::platform(
				"duplicate_batch_input",
				"grouped request, input ID, and index must be unique",
			));
		}
	}
	Ok(())
}
fn define_frame(
	revision_id: &str,
	definition: Value,
	secrets: Value,
	trusted: bool,
	cloudpickle_version: Option<&str>,
) -> Value {
	json!({
		"type":"define",
		"request_id":"define:main",
		"definition_id":"main",
		"revision":revision_id,
		"definition":definition,
		"secrets":secrets,
		"trusted":trusted,
		"cloudpickle_version":cloudpickle_version,
	})
}

fn materialize_output(
	engine: &dyn EngineApi,
	worker_id: &str,
	artifact_root: &Path,
	frame: &mut Frame,
) -> Result<(), WorkerError> {
	let Some(value) = frame.0.get_mut("value") else {
		return Ok(());
	};
	let Some(path) = value.get("path").and_then(Value::as_str).map(str::to_owned) else {
		return Ok(());
	};
	let allowed = path.starts_with("/run/vmon/results/") || path.starts_with("/run/vmon/values/");
	if !allowed
		|| Path::new(&path)
			.components()
			.any(|part| matches!(part, Component::ParentDir))
	{
		return Err(WorkerError::platform(
			"invalid_spill_path",
			"runner returned an unsafe spill path",
		));
	}
	let read = (|| {
		let stat = engine.file_stat(worker_id, &path).map_err(engine_error)?;
		if stat.get("size").and_then(Value::as_u64).unwrap_or(u64::MAX) > 64 * 1024 * 1024 {
			return Err(WorkerError::platform("result_too_large", "runner spill exceeds 64 MiB"));
		}
		engine.file_read(worker_id, &path).map_err(engine_error)
	})();
	let delete = engine
		.file_delete(worker_id, &path, false)
		.map_err(engine_error);
	let bytes = read?;
	delete?;
	materialize_output_bytes(artifact_root, value, bytes)?;
	if let Some(object) = value.as_object_mut() {
		object.remove("path");
		object.remove("remove_after_read");
	}
	Ok(())
}

fn materialize_output_bytes(
	artifact_root: &Path,
	value: &mut Value,
	bytes: Vec<u8>,
) -> Result<(), WorkerError> {
	if bytes.len() > 64 * 1024 * 1024 {
		return Err(WorkerError::platform("result_too_large", "runner spill exceeds 64 MiB"));
	}
	let checksum = value
		.get("sha256")
		.and_then(Value::as_str)
		.and_then(|text| hex::decode(text.strip_prefix("sha256:").unwrap_or(text)).ok())
		.filter(|digest| digest.len() == 32)
		.ok_or_else(|| {
			WorkerError::platform("invalid_runner_value", "runner spill checksum is invalid")
		})?;
	let compression = value
		.get("compression")
		.and_then(Value::as_str)
		.unwrap_or("none");
	if !matches!(compression, "none" | "gzip" | "zstd") {
		return Err(WorkerError::platform(
			"invalid_runner_value",
			format!("unsupported runner compression {compression}"),
		));
	}
	let uncompressed_size = value
		.get("uncompressed_size")
		.and_then(Value::as_u64)
		.ok_or_else(|| {
			WorkerError::platform("invalid_runner_value", "runner result size is missing")
		})?;
	if uncompressed_size > 64 * 1024 * 1024 {
		return Err(WorkerError::platform("result_too_large", "runner result exceeds 64 MiB"));
	}
	if compression == "none"
		&& (uncompressed_size != bytes.len() as u64
			|| checksum.as_slice() != Sha256::digest(&bytes).as_slice())
	{
		return Err(WorkerError::platform(
			"invalid_runner_value",
			"runner spill checksum or size mismatch",
		));
	}
	if bytes.len() <= 512 * 1024 {
		value["inline_data"] = json!(BASE64.encode(bytes));
	} else {
		let stored_digest = Sha256::digest(&bytes);
		let artifacts = ArtifactStore::open(artifact_root.to_owned()).map_err(engine_error)?;
		let stored = artifacts
			.put_verified(&stored_digest, bytes.len() as u64, &bytes)
			.map_err(engine_error)?;
		let home_root = artifact_root
			.parent()
			.and_then(Path::parent)
			.ok_or_else(|| WorkerError::infrastructure("artifact_store", "invalid artifact root"))?;
		let store = Store::open(&Home::new(home_root)).map_err(engine_error)?;
		store
			.record_artifact(
				&stored.digest,
				stored.size,
				None,
				stored.path.to_str().ok_or_else(|| {
					WorkerError::infrastructure("artifact_store", "artifact path is not UTF-8")
				})?,
				now_ms(),
				None,
			)
			.map_err(engine_error)?;
		value["artifact_digest"] = json!(stored.digest);
	}
	Ok(())
}

/// Convert the protobuf value contract to the runner's protocol-v2 envelope.
///
/// `artifact_path` is required for artifact-backed storage and must name the
/// already materialized absolute guest path.
pub fn envelope_to_wire(
	envelope: &pb::ValueEnvelope,
	artifact_path: Option<&str>,
) -> Result<Value, WorkerError> {
	let format = match pb::ValueSerializer::try_from(envelope.serializer)
		.unwrap_or(pb::ValueSerializer::Unspecified)
	{
		pb::ValueSerializer::Json => "json",
		pb::ValueSerializer::Cbor => "cbor",
		pb::ValueSerializer::Cloudpickle => "cloudpickle",
		pb::ValueSerializer::Unspecified => {
			return Err(WorkerError::platform("invalid_value", "value serializer is unspecified"));
		},
	};
	let compression = match pb::ValueCompression::try_from(envelope.compression)
		.unwrap_or(pb::ValueCompression::None)
	{
		pb::ValueCompression::None => "none",
		pb::ValueCompression::Gzip => "gzip",
		pb::ValueCompression::Zstd => "zstd",
	};
	let checksum = envelope
		.checksum
		.as_ref()
		.ok_or_else(|| WorkerError::platform("invalid_value", "value checksum is required"))?;
	if checksum.value.len() != 32 || checksum.algorithm != pb::DigestAlgorithm::Sha256 as i32 {
		return Err(WorkerError::platform(
			"invalid_value",
			"protocol-v2 values require SHA-256 checksums",
		));
	}
	let mut wire = json!({"version":envelope.schema_version,"format":format,"compression":compression,"sha256":format!("sha256:{}",hex::encode(&checksum.value)),"uncompressed_size":envelope.uncompressed_size_bytes});
	match &envelope.storage {
		Some(pb::value_envelope::Storage::InlineData(bytes)) => {
			wire["inline_data"] = json!(BASE64.encode(bytes));
		},
		Some(pb::value_envelope::Storage::Artifact(_)) => {
			let path = artifact_path.ok_or_else(|| {
				WorkerError::platform(
					"artifact_unavailable",
					"artifact value was not materialized in the guest",
				)
			})?;
			validate_absolute(path)?;
			wire["path"] = json!(path);
			wire["remove_after_read"] = json!(true);
		},
		None => return Err(WorkerError::platform("invalid_value", "value storage is required")),
	}
	if let Some(pb::value_envelope::PythonPresence::Python(python)) = &envelope.python_presence {
		wire["python_abi"] = json!(python.abi_tag);
		wire["cloudpickle_version"] = json!(python.cloudpickle_version);
	}
	Ok(wire)
}

/// Convert a runner protocol-v2 envelope back into the exact protobuf value
/// contract.
pub fn wire_to_envelope(wire: &Value) -> Result<pb::ValueEnvelope, WorkerError> {
	let format = wire.get("format").and_then(Value::as_str).ok_or_else(|| {
		WorkerError::platform("invalid_runner_value", "runner value format is missing")
	})?;
	let serializer = match format {
		"json" => pb::ValueSerializer::Json,
		"cbor" => pb::ValueSerializer::Cbor,
		"cloudpickle" => pb::ValueSerializer::Cloudpickle,
		_ => {
			return Err(WorkerError::platform(
				"invalid_runner_value",
				format!("unsupported runner value format {format}"),
			));
		},
	};
	let compression = match wire
		.get("compression")
		.and_then(Value::as_str)
		.unwrap_or("none")
	{
		"none" => pb::ValueCompression::None,
		"gzip" => pb::ValueCompression::Gzip,
		"zstd" => pb::ValueCompression::Zstd,
		other => {
			return Err(WorkerError::platform(
				"invalid_runner_value",
				format!("unsupported runner compression {other}"),
			));
		},
	};
	let checksum_text = wire.get("sha256").and_then(Value::as_str).ok_or_else(|| {
		WorkerError::platform("invalid_runner_value", "runner result checksum is missing")
	})?;
	let checksum = hex::decode(
		checksum_text
			.strip_prefix("sha256:")
			.unwrap_or(checksum_text),
	)
	.map_err(|_| {
		WorkerError::platform("invalid_runner_value", "runner result checksum is invalid")
	})?;
	if checksum.len() != 32 {
		return Err(WorkerError::platform(
			"invalid_runner_value",
			"runner result checksum is not SHA-256",
		));
	}
	let expected_size = wire
		.get("uncompressed_size")
		.and_then(Value::as_u64)
		.ok_or_else(|| {
			WorkerError::platform("invalid_runner_value", "runner result size is missing")
		})?;
	let storage = if let Some(encoded) = wire.get("inline_data").and_then(Value::as_str) {
		let bytes = BASE64.decode(encoded).map_err(|_| {
			WorkerError::platform("invalid_runner_value", "runner result contains invalid base64")
		})?;
		if compression == pb::ValueCompression::None
			&& (expected_size != bytes.len() as u64
				|| Sha256::digest(&bytes).as_slice() != checksum.as_slice())
		{
			return Err(WorkerError::platform(
				"invalid_runner_value",
				"runner result checksum or size mismatch",
			));
		}
		pb::value_envelope::Storage::InlineData(bytes)
	} else if let Some(digest) = wire.get("artifact_digest").and_then(Value::as_str) {
		let artifact_digest = hex::decode(digest).map_err(|_| {
			WorkerError::platform("invalid_runner_value", "runner artifact digest is invalid")
		})?;
		if compression == pb::ValueCompression::None && artifact_digest != checksum {
			return Err(WorkerError::platform(
				"invalid_runner_value",
				"runner artifact digest does not match value checksum",
			));
		}
		pb::value_envelope::Storage::Artifact(pb::ArtifactRef {
			digest: Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     artifact_digest,
			}),
		})
	} else {
		return Err(WorkerError::platform("invalid_runner_value", "runner result has no storage"));
	};
	let python_presence = if serializer == pb::ValueSerializer::Cloudpickle {
		Some(pb::value_envelope::PythonPresence::Python(pb::PythonCodecMetadata {
			implementation:      "CPython".into(),
			abi_tag:             wire
				.get("python_abi")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_owned(),
			python_version:      String::new(),
			cloudpickle_version: wire
				.get("cloudpickle_version")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_owned(),
		}))
	} else {
		None
	};
	Ok(pb::ValueEnvelope {
		schema_version: wire.get("version").and_then(Value::as_u64).unwrap_or(1) as u32,
		serializer: serializer as i32,
		compression: compression as i32,
		checksum: Some(pb::Digest {
			algorithm: pb::DigestAlgorithm::Sha256 as i32,
			value:     checksum,
		}),
		uncompressed_size_bytes: expected_size,
		storage: Some(storage),
		python_presence,
		type_name_presence: None,
	})
}
fn batch_capacity(spec: &pb::FunctionSpec) -> usize {
	spec
		.batching
		.as_ref()
		.filter(|batch| batch.enabled)
		.map_or(0, |batch| batch.max_batch_size.max(1) as usize)
}

fn outcome(frame: Frame, stats: AttemptStats) -> WorkerOutcome {
	match frame.event() {
		Some("result") => match frame
			.0
			.get("value")
			.ok_or_else(|| {
				WorkerError::platform("invalid_runner_value", "runner result omitted value")
			})
			.and_then(wire_to_envelope)
		{
			Ok(value) => WorkerOutcome::Success { value, stats },
			Err(error) => WorkerOutcome::PlatformError { error, stats },
		},
		Some("cancelled") => WorkerOutcome::Cancelled {
			reason: frame
				.0
				.get("reason")
				.and_then(Value::as_str)
				.unwrap_or("cancelled")
				.to_owned(),
			stats,
		},
		Some("error") => {
			let e = terminal_error(&frame).unwrap_err();
			match e.kind {
				WorkerErrorKind::User => WorkerOutcome::UserError { error: e, stats },
				WorkerErrorKind::ActorLost => WorkerOutcome::ActorLost { error: e, stats },
				WorkerErrorKind::Infrastructure => {
					WorkerOutcome::InfrastructureError { error: e, stats }
				},

				_ => WorkerOutcome::PlatformError { error: e, stats },
			}
		},
		_ => WorkerOutcome::PlatformError {
			error: WorkerError::platform("runner_terminal", "unexpected terminal frame"),
			stats,
		},
	}
}
fn typed_event(frame: &Frame) -> Option<WorkerEvent> {
	match frame.event()? {
		"log" => Some(WorkerEvent::Log {
			stream:  frame
				.0
				.get("stream")
				.and_then(Value::as_str)
				.unwrap_or("stdout")
				.to_owned(),
			message: frame
				.0
				.get("message")
				.and_then(Value::as_str)
				.unwrap_or("")
				.to_owned(),
		}),
		"yield" => wire_to_envelope(frame.0.get("value")?)
			.ok()
			.map(|value| WorkerEvent::Yield {
				index: frame.0.get("index").and_then(Value::as_u64).unwrap_or(0),
				value,
			}),
		"status" => Some(WorkerEvent::Status {
			status: frame
				.0
				.get("status")
				.and_then(Value::as_str)
				.unwrap_or("")
				.to_owned(),
		}),
		_ => None,
	}
}
fn dispatch_batch_event(
	events: &flume::Sender<BatchWorkerEvent>,
	frame: &Frame,
) -> Result<(), WorkerError> {
	let Some(event) = typed_event(frame) else {
		return Ok(());
	};
	let input_index = frame.0.get("input_index").and_then(Value::as_u64);
	events
		.try_send(BatchWorkerEvent { input_index, event })
		.map_err(|_| {
			WorkerError::infrastructure("event_backpressure", "worker event consumer did not keep up")
		})
}
fn callable_interruptibility(kind: &str) -> Interruptibility {
	if matches!(kind, "async" | "async_generator") {
		Interruptibility::Async
	} else {
		Interruptibility::Sync
	}
}
fn runner_failure(error: &Value, kind: WorkerErrorKind, depth: usize) -> WorkerError {
	let message = error
		.get("message")
		.and_then(Value::as_str)
		.unwrap_or("runner error");
	let error_type = error
		.get("type")
		.and_then(Value::as_str)
		.unwrap_or("runner_error");
	let code = error
		.get("code")
		.and_then(Value::as_str)
		.unwrap_or(error_type);
	let mut failure = match kind {
		WorkerErrorKind::User => WorkerError::user(code, message, true),
		WorkerErrorKind::ActorLost => WorkerError::actor_lost(message),
		_ => WorkerError::platform("runner_error", message),
	};
	failure.error_type = error_type.chars().take(256).collect();
	if let Some(phase) = error.get("phase").and_then(Value::as_str) {
		failure
			.details
			.insert("phase".into(), phase.chars().take(128).collect());
	}
	if let Some(module) = error.get("module").and_then(Value::as_str) {
		failure
			.details
			.insert("module".into(), module.chars().take(512).collect());
	}
	if let Some(frames) = error.get("frames").and_then(Value::as_array) {
		failure.frames = frames
			.iter()
			.take(128)
			.map(|frame| pb::ErrorFrame {
				file:          frame
					.get("file")
					.and_then(Value::as_str)
					.unwrap_or("")
					.chars()
					.take(1024)
					.collect(),
				line:          frame
					.get("line")
					.and_then(Value::as_u64)
					.unwrap_or(0)
					.min(u32::MAX as u64) as u32,
				function:      frame
					.get("function")
					.and_then(Value::as_str)
					.unwrap_or("")
					.chars()
					.take(512)
					.collect(),
				code_presence: frame
					.get("code")
					.and_then(Value::as_str)
					.map(|code| pb::error_frame::CodePresence::Code(code.chars().take(2048).collect())),
			})
			.collect();
	}
	if depth < 4
		&& let Some(cause) = error.get("cause").filter(|cause| cause.is_object())
	{
		failure.cause = Some(Box::new(runner_failure(cause, kind, depth + 1)));
	}
	failure
}

fn terminal_error(frame: &Frame) -> Result<(), WorkerError> {
	if frame.event() != Some("error") {
		return Ok(());
	}
	let error = &frame.0["error"];
	let phase = error.get("phase").and_then(Value::as_str).unwrap_or("user");
	let message = error
		.get("message")
		.and_then(Value::as_str)
		.unwrap_or("runner error");
	let kind = if phase == "actor"
		&& (message.contains("unavailable")
			|| message.contains("explicit restore")
			|| message.contains("checkpoint is unavailable"))
	{
		WorkerErrorKind::ActorLost
	} else if matches!(phase, "call" | "batch" | "actor") {
		WorkerErrorKind::User
	} else {
		WorkerErrorKind::Platform
	};
	Err(runner_failure(error, kind, 0))
}
fn read_artifact_at(home: &Home, reference: &pb::ArtifactRef) -> Result<Vec<u8>, WorkerError> {
	let digest = reference
		.digest
		.as_ref()
		.ok_or_else(|| WorkerError::platform("invalid_artifact", "artifact digest required"))?;
	if digest.value.len() != 32 {
		return Err(WorkerError::platform("invalid_artifact", "artifact digest must be SHA-256"));
	}
	let hex = hex::encode(&digest.value);
	let path = home
		.root()
		.join("functions/artifacts")
		.join(&hex[..2])
		.join(&hex[2..]);
	let bytes = read_bounded_file(&path, 64 * 1024 * 1024).map_err(|error| {
		if error.kind() == std::io::ErrorKind::InvalidData {
			WorkerError::platform("artifact_too_large", "artifact exceeds 64 MiB worker limit")
		} else {
			WorkerError::platform("artifact_unavailable", format!("artifact {hex} is unavailable"))
		}
	})?;
	if Sha256::digest(&bytes).as_slice() != digest.value.as_slice() {
		return Err(WorkerError::platform("artifact_corrupt", "artifact digest mismatch"));
	}
	Ok(bytes)
}
fn read_bounded_file(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
	let file = File::open(path)?;
	if file.metadata()?.len() > limit {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"file exceeds bounded read limit",
		));
	}
	let mut bytes = Vec::with_capacity(usize::try_from(limit.min(1024 * 1024)).unwrap_or(0));
	file.take(limit + 1).read_to_end(&mut bytes)?;
	if bytes.len() as u64 > limit {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"file grew beyond bounded read limit",
		));
	}
	Ok(bytes)
}
fn worker_capacity(spec: &pb::FunctionSpec) -> usize {
	let concurrency = spec
		.concurrency
		.as_ref()
		.map_or(1, |c| c.max_concurrent_calls.max(1));
	let outstanding = spec
		.workers
		.as_ref()
		.map_or(concurrency, |w| w.max_outstanding_inputs.max(concurrency));
	outstanding.min(concurrency) as usize
}
const fn duration_ms(value: u64, default: Duration) -> Duration {
	if value == 0 {
		default
	} else {
		Duration::from_millis(value)
	}
}
fn remaining(deadline: Instant) -> Result<Duration, WorkerError> {
	let duration = deadline.saturating_duration_since(Instant::now());
	if duration.is_zero() {
		Err(WorkerError::timeout("startup"))
	} else {
		Ok(duration)
	}
}
fn execution_timeout(revision: &pb::FunctionRevision) -> Duration {
	duration_ms(
		revision
			.spec
			.as_ref()
			.and_then(|s| s.timeouts.as_ref())
			.map_or(0, |t| t.execution_millis),
		DEFAULT_EXECUTION,
	)
}
fn shutdown_timeout(revision: &pb::FunctionRevision) -> Duration {
	duration_ms(
		revision
			.spec
			.as_ref()
			.and_then(|s| s.timeouts.as_ref())
			.map_or(0, |t| t.graceful_shutdown_millis),
		DEFAULT_SHUTDOWN,
	)
}
fn initialize_hook(spec: &pb::FunctionSpec) -> Option<&pb::LifecycleHookRef> {
	spec.lifecycle_hooks.as_ref().and_then(|h| {
		h.initialize_presence
			.as_ref()
			.map(|pb::lifecycle_hooks::InitializePresence::Initialize(h)| h)
	})
}
fn shutdown_hook(spec: Option<&pb::FunctionSpec>) -> Option<&pb::LifecycleHookRef> {
	spec.and_then(|s| s.lifecycle_hooks.as_ref()).and_then(|h| {
		h.shutdown_presence
			.as_ref()
			.map(|pb::lifecycle_hooks::ShutdownPresence::Shutdown(h)| h)
	})
}
fn restore_hook(spec: Option<&pb::FunctionSpec>) -> Option<&pb::LifecycleHookRef> {
	spec.and_then(|s| s.lifecycle_hooks.as_ref()).and_then(|h| {
		h.restore_presence
			.as_ref()
			.map(|pb::lifecycle_hooks::RestorePresence::Restore(h)| h)
	})
}
fn snapshot_hook(spec: Option<&pb::FunctionSpec>) -> Option<&pb::LifecycleHookRef> {
	spec.and_then(|s| s.lifecycle_hooks.as_ref()).and_then(|h| {
		h.snapshot_presence
			.as_ref()
			.map(|pb::lifecycle_hooks::SnapshotPresence::Snapshot(h)| h)
	})
}
fn worker_hook_allowed(
	spec: Option<&pb::FunctionSpec>,
	hook: &pb::LifecycleHookRef,
) -> Result<bool, WorkerError> {
	if hook.module.is_empty() || hook.qualname.is_empty() {
		return Err(WorkerError::platform(
			"invalid_lifecycle_hook",
			"lifecycle hook module and qualname are required",
		));
	}
	if !hook.qualname.contains('.') && !hook.qualname.contains("<locals>") {
		return Ok(true);
	}
	let lifecycle = spec.map_or(pb::FunctionLifecycle::Unspecified, |spec| {
		pb::FunctionLifecycle::try_from(spec.lifecycle).unwrap_or(pb::FunctionLifecycle::Unspecified)
	});
	if matches!(lifecycle, pb::FunctionLifecycle::Actor | pb::FunctionLifecycle::Instance) {
		Ok(false)
	} else {
		Err(WorkerError::platform(
			"invalid_lifecycle_hook",
			"worker-level lifecycle hooks must be module functions",
		))
	}
}
fn filter_worker_hook(
	spec: Option<&pb::FunctionSpec>,
	hook: Option<pb::LifecycleHookRef>,
) -> Result<Option<pb::LifecycleHookRef>, WorkerError> {
	match hook {
		Some(hook) if worker_hook_allowed(spec, &hook)? => Ok(Some(hook)),
		_ => Ok(None),
	}
}
fn bytes_to_mib(bytes: u64, default: u32) -> u32 {
	if bytes == 0 {
		return default;
	}
	((bytes.saturating_add(1024 * 1024 - 1) / (1024 * 1024)).min(u32::MAX as u64)) as u32
}
fn sandbox_id(view: &Value) -> Option<String> {
	view
		.get("name")
		.or_else(|| view.get("id"))
		.and_then(Value::as_str)
		.map(str::to_owned)
}
fn worker_name(revision_id: &str, serial: u64) -> String {
	let digest = hex::encode(Sha256::digest(revision_id.as_bytes()));
	format!("f-{}-{serial:x}", &digest[..12])
}
fn validate_absolute(path: &str) -> Result<(), WorkerError> {
	let path = Path::new(path);
	if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
		return Err(WorkerError::platform(
			"invalid_path",
			format!("guest mount path {} must be absolute and normalized", path.display()),
		));
	}
	Ok(())
}
fn millis(duration: Duration) -> u64 {
	duration.as_millis().min(u64::MAX as u128) as u64
}
fn system_ms(time: SystemTime) -> u64 {
	time
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.min(u64::MAX as u128) as u64
}
fn now_ms() -> u64 {
	system_ms(SystemTime::now())
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;

	fn digest(byte: u8) -> pb::Digest {
		pb::Digest { algorithm: pb::DigestAlgorithm::Sha256 as i32, value: vec![byte; 32] }
	}

	fn revision() -> pb::FunctionRevision {
		pb::FunctionRevision {
			r#ref: Some(pb::RevisionRef { function: None, revision_id: "revision-1".into() }),
			spec: Some(pb::FunctionSpec {
				package: Some(pb::PackageSpec {
					content_digest: Some(digest(1)),
					..pb::PackageSpec::default()
				}),
				image: Some(pb::ImageSpec {
					resolved_oci_digest: Some(digest(2)),
					source: Some(pb::image_spec::Source::Registry(pb::RegistryImageSource {
						reference: format!("example@sha256:{}", "02".repeat(32)),
					})),
					..pb::ImageSpec::default()
				}),
				resources: Some(pb::ResourceSpec {
					cpus: 1,
					architecture: pb::CpuArchitecture::Arm64 as i32,
					..pb::ResourceSpec::default()
				}),
				lifecycle_hooks: Some(pb::LifecycleHooks {
					initialize_presence: Some(pb::lifecycle_hooks::InitializePresence::Initialize(
						pb::LifecycleHookRef { module: "hooks".into(), qualname: "initialize".into() },
					)),
					..pb::LifecycleHooks::default()
				}),
				..pb::FunctionSpec::default()
			}),
			spec_digest: Some(digest(3)),
			..pb::FunctionRevision::default()
		}
	}

	#[test]
	fn protocol_requirements_follow_immutable_serializer_policy() {
		let spec = pb::FunctionSpec {
			package: Some(pb::PackageSpec {
				mode: pb::PackageMode::TrustedSerialized as i32,
				..pb::PackageSpec::default()
			}),
			serializer: Some(pb::SerializerSpec {
				input_serializer:     pb::ValueSerializer::Cbor as i32,
				result_serializer:    pb::ValueSerializer::Cloudpickle as i32,
				compression:          pb::ValueCompression::Zstd as i32,
				allow_trusted_python: true,
			}),
			..pb::FunctionSpec::default()
		};
		assert_eq!(
			protocol_requirements(&spec).unwrap(),
			ProtocolRequirements::default()
				.require_format("cbor")
				.require_format("cloudpickle")
				.require_compression("zstd")
		);
	}

	#[test]
	fn protobuf_envelope_round_trips_protocol_wire() {
		let bytes = br#"{\"answer\":42}"#.to_vec();
		let envelope = pb::ValueEnvelope {
			schema_version:          1,
			serializer:              pb::ValueSerializer::Json as i32,
			compression:             pb::ValueCompression::None as i32,
			checksum:                Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     Sha256::digest(&bytes).to_vec(),
			}),
			uncompressed_size_bytes: bytes.len() as u64,
			storage:                 Some(pb::value_envelope::Storage::InlineData(bytes)),
			python_presence:         None,
			type_name_presence:      None,
		};
		let wire = envelope_to_wire(&envelope, None).unwrap();
		assert_eq!(wire_to_envelope(&wire).unwrap(), envelope);
	}

	#[test]
	fn snapshot_provenance_invalidates_on_immutable_input_change() {
		let original = revision();
		let provenance = SnapshotProvenance::for_revision(&original).unwrap();
		assert!(provenance.matches(&original));
		let mut changed = original;
		changed
			.spec
			.as_mut()
			.unwrap()
			.image
			.as_mut()
			.unwrap()
			.resolved_oci_digest = Some(digest(9));
		assert!(!provenance.matches(&changed));
	}

	#[test]
	fn none_worker_request_builds_a_single_node_local_sandbox() {
		let mut revision = revision();
		let spec = revision.spec.as_mut().unwrap();
		spec.resources.as_mut().unwrap().high_availability = pb::HighAvailabilityPolicy::None as i32;
		let image = image::RealizedImage {
			image:             Some("local-test-image".into()),
			template:          None,
			dockerfile:        None,
			context:           None,
			environment:       BTreeMap::new(),
			resolved_spec:     spec.image.clone().unwrap(),
			provenance_digest: "07".repeat(32),
		};
		let create =
			sandbox_create_from_image(spec, "fn-revision-1-1", &SecretValues::new(), image).unwrap();
		assert_eq!(create.name.as_deref(), Some("fn-revision-1-1"));
		assert_eq!(create.ha.as_deref(), Some("off"));
		assert_eq!(create.arch.as_deref(), Some("aarch64"));
	}

	#[test]
	fn worker_names_are_short_stable_and_collision_resistant() {
		let first = worker_name("revision-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", u64::MAX);
		assert_eq!(first, worker_name("revision-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", u64::MAX));
		assert_ne!(first, worker_name("revision-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", u64::MAX));
		assert_ne!(first, worker_name("revision-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", u64::MAX - 1));
		assert!(first.len() <= 31);
		assert!(
			first
				.bytes()
				.all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
		);
	}

	#[test]
	fn sandbox_mapping_is_typed_and_secret_errors_are_redacted() {
		let mut secrets = SecretValues::new();
		secrets.insert("TOKEN", b"sensitive-value".to_vec());
		let mut create = SandboxCreate { ha: Some("off".into()), ..SandboxCreate::default() };
		apply_sandbox_resources(
			&mut create,
			revision()
				.spec
				.as_ref()
				.unwrap()
				.resources
				.as_ref()
				.unwrap(),
		);
		assert_eq!(create.arch.as_deref(), Some("aarch64"));
		assert_eq!(create.ha.as_deref(), Some("off"));
		assert_eq!(engine_architecture(pb::CpuArchitecture::Amd64), Some("x86_64"));
		assert_eq!(engine_architecture(pb::CpuArchitecture::Arm64), Some("aarch64"));
		assert_eq!(engine_architecture(pb::CpuArchitecture::Unspecified), None);
		assert_eq!(secrets.sandbox_wire().unwrap()[0]["values"]["TOKEN"], "sensitive-value");
		let redacted = secrets
			.redact_error(WorkerError::infrastructure("engine", "create failed with sensitive-value"));
		assert_eq!(redacted.message, "create failed with [REDACTED]");
		let mut invalid = SecretValues::new();
		invalid.insert("TOKEN", vec![0xff, 0xfe]);
		let error = invalid.sandbox_wire().unwrap_err();
		assert!(!error.to_string().contains("ff"));
		assert!(!error.to_string().contains("fe"));
	}

	#[test]
	fn package_definition_is_constant_size_for_large_archives() {
		let definition = package_definition("module:call", &"a".repeat(64), 2 * 1024 * 1024);
		assert_eq!(definition["archive_path"], "/opt/vmon/functions/package.zip");
		assert_eq!(definition["archive_size"], 2 * 1024 * 1024);
		assert!(serde_json::to_vec(&definition).unwrap().len() < 512);
		let serialized = path_envelope(
			PACKAGE_PATH,
			&"b".repeat(64),
			64 * 1024 * 1024,
			"cloudpickle",
			"cp312",
			Some("3.1.1"),
		);
		assert!(serialized.get("inline_data").is_none());
		assert_eq!(serialized["remove_after_read"], false);
		assert!(serde_json::to_vec(&serialized).unwrap().len() < 512);
	}

	#[test]
	fn concurrent_artifact_inputs_get_unique_confined_paths() {
		let paths: std::collections::HashSet<_> = (0..32).map(|_| guest_value_path()).collect();
		assert_eq!(paths.len(), 32);
		assert!(
			paths
				.iter()
				.all(|path| path.starts_with("/opt/vmon/values/")
					&& path.len() == "/opt/vmon/values/".len() + 32)
		);
		let artifact = pb::ValueEnvelope {
			schema_version:          1,
			serializer:              pb::ValueSerializer::Json as i32,
			compression:             pb::ValueCompression::None as i32,
			checksum:                Some(digest(4)),
			uncompressed_size_bytes: 2 * 1024 * 1024,
			storage:                 Some(pb::value_envelope::Storage::Artifact(pb::ArtifactRef {
				digest: Some(digest(4)),
			})),
			python_presence:         None,
			type_name_presence:      None,
		};
		for path in &paths {
			let wire = envelope_to_wire(&artifact, Some(path)).unwrap();
			assert_eq!(wire["remove_after_read"], true);
			assert_eq!(wire["path"], path.as_str());
		}
	}

	#[test]
	fn callable_kind_is_conservative_for_sync_work() {
		assert_eq!(callable_interruptibility("async"), Interruptibility::Async);
		assert_eq!(callable_interruptibility("async_generator"), Interruptibility::Async);
		assert_eq!(callable_interruptibility("sync"), Interruptibility::Sync);
		assert_eq!(callable_interruptibility("generator"), Interruptibility::Sync);
		assert_eq!(callable_interruptibility("unknown"), Interruptibility::Sync);
	}

	#[test]
	fn actor_class_hooks_are_runner_instance_owned() {
		let mut revision = revision();
		let spec = revision.spec.as_mut().unwrap();
		spec.lifecycle = pb::FunctionLifecycle::Actor as i32;
		let class_hook =
			pb::LifecycleHookRef { module: "service".into(), qualname: "Counter.enter".into() };
		assert!(!worker_hook_allowed(Some(spec), &class_hook).unwrap());
		let module_hook =
			pb::LifecycleHookRef { module: "service".into(), qualname: "initialize".into() };
		assert!(worker_hook_allowed(Some(spec), &module_hook).unwrap());
		spec.lifecycle = pb::FunctionLifecycle::Stateless as i32;
		assert_eq!(
			worker_hook_allowed(Some(spec), &class_hook)
				.unwrap_err()
				.code,
			"invalid_lifecycle_hook"
		);
	}

	#[test]
	fn compressed_runner_frames_preserve_gzip_and_zstd_storage() {
		let data = br#"{"answer":42}"#;
		let checksum = hex::encode(Sha256::digest(data));
		let gzip = vec![
			31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 171, 86, 74, 204, 43, 46, 79, 45, 82, 178, 50, 49, 170,
			5, 0, 245, 101, 217, 204, 13, 0, 0, 0,
		];
		let zstd = zstd::encode_all(data.as_slice(), 0).unwrap();
		for (compression, stored, expected) in
			[("gzip", gzip, pb::ValueCompression::Gzip), ("zstd", zstd, pb::ValueCompression::Zstd)]
		{
			let wire = json!({
				"version":1,
				"format":"json",
				"compression":compression,
				"sha256":format!("sha256:{checksum}"),
				"uncompressed_size":data.len(),
				"inline_data":BASE64.encode(&stored),
			});
			let envelope = wire_to_envelope(&wire).unwrap();
			assert_eq!(envelope.compression, expected as i32);
			assert_eq!(envelope.storage, Some(pb::value_envelope::Storage::InlineData(stored)));
		}
	}

	#[test]
	fn large_materialized_result_records_retrievable_artifact_metadata() {
		let temp = tempfile::tempdir().unwrap();
		let home = Home::new(temp.path());
		let artifact_root = home.function_artifacts_dir();
		let bytes = vec![7u8; 600 * 1024];
		let checksum = hex::encode(Sha256::digest(&bytes));
		let mut value = json!({
			"version":1,
			"format":"json",
			"compression":"none",
			"sha256":format!("sha256:{checksum}"),
			"uncompressed_size":bytes.len(),
		});
		materialize_output_bytes(&artifact_root, &mut value, bytes.clone()).unwrap();
		let digest = value["artifact_digest"].as_str().unwrap();
		let store = Store::open(&home).unwrap();
		let metadata = store.stat_artifact(digest).unwrap();
		assert_eq!(metadata.0, bytes.len() as u64);
		assert_eq!(
			ArtifactStore::open(artifact_root)
				.unwrap()
				.read(digest, Some(bytes.len() as u64))
				.unwrap(),
			bytes
		);
		let envelope = wire_to_envelope(&value).unwrap();
		assert!(matches!(envelope.storage, Some(pb::value_envelope::Storage::Artifact(_))));
	}

	#[test]
	fn trusted_serialized_define_uses_runner_top_level_authority() {
		let definition = json!({
			"mode":"serialized",
			"trusted":true,
			"value":{"format":"cloudpickle","cloudpickle_version":"3.1.1"},
		});
		let frame = define_frame(
			"revision-1",
			definition.clone(),
			json!({"TOKEN":"secret"}),
			true,
			Some("3.1.1"),
		);
		assert_eq!(frame["trusted"], true);
		assert_eq!(frame["cloudpickle_version"], "3.1.1");
		assert_eq!(frame["definition"], definition);
		assert_eq!(frame["definition"]["trusted"], true);
		assert_eq!(frame["definition"]["value"]["cloudpickle_version"], "3.1.1");
	}

	#[test]
	fn bounded_artifact_read_rejects_oversized_file_before_allocation() {
		let temp = tempfile::tempdir().unwrap();
		let path = temp.path().join("oversized");
		let file = File::create(&path).unwrap();
		file.set_len(1025).unwrap();
		assert_eq!(
			read_bounded_file(&path, 1024).unwrap_err().kind(),
			std::io::ErrorKind::InvalidData
		);
	}

	#[test]
	fn batch_phase_errors_are_retryable_with_runner_codes() {
		let frame = Frame(json!({
			"type":"error",
			"request_id":"batch-1",
			"error":{"phase":"batch","type":"ValueError","code":"fail_once","message":"try again"},
		}));
		let error = terminal_error(&frame).unwrap_err();
		assert_eq!(error.kind, WorkerErrorKind::User);
		assert_eq!(error.code, "fail_once");
		assert_eq!(error.error_type, "ValueError");
		assert!(error.retryable);
	}

	#[test]
	fn cancelled_grouped_batch_finishes_every_active_input() {
		let input = ExecuteRequest {
			request_id:        "request-1".into(),
			function_id:       "function-1".into(),
			call_id:           "call-1".into(),
			input_id:          "input-1".into(),
			input_index:       3,
			attempt:           1,
			mode:              ExecutionMode::Unary,
			input:             pb::call_input::Payload::Value(pb::ValueEnvelope::default()),
			deadline:          None,
			parent_call_id:    None,
			parent_request_id: None,
			interruptibility:  Interruptibility::Async,
			service:           None,
		};
		let outcome = cancelled_batch_outcome(
			Frame(json!({"type":"cancelled","request_id":"batch-1","reason":"client request"})),
			vec![input],
			AttemptStats::default(),
		);
		assert_eq!(outcome.items.len(), 1);
		assert_eq!(outcome.items[0].input_index, 3);
		assert!(matches!(
			&outcome.items[0].outcome,
			WorkerOutcome::Cancelled { reason, .. } if reason == "client request"
		));
	}

	#[test]
	fn grouped_batch_event_capacity_is_fail_closed() {
		let (events, receiver) = flume::bounded(1);
		let frame = Frame(json!({
			"type":"log",
			"request_id":"batch-1",
			"input_index":2,
			"stream":"stdout",
			"message":"line",
		}));
		dispatch_batch_event(&events, &frame).unwrap();
		let error = dispatch_batch_event(&events, &frame).unwrap_err();
		assert_eq!(error.code, "event_backpressure");
		assert_eq!(error.kind, WorkerErrorKind::Infrastructure);
		assert!(error.retryable);
		assert_eq!(receiver.recv().unwrap().input_index, Some(2));
		let unscoped = Frame(json!({
			"type":"log",
			"request_id":"batch-1",
			"stream":"stderr",
			"message":"batch-level",
		}));
		dispatch_batch_event(&events, &unscoped).unwrap();
		assert_eq!(receiver.recv().unwrap().input_index, None);
	}

	#[test]
	fn unavailable_actor_is_explicit_actor_lost() {
		let frame = Frame(
			json!({"protocol":2,"type":"error","request_id":"actor","error":{"phase":"actor","type":"KeyError","message":"actor is unavailable; explicit restore is required"}}),
		);
		let outcome = outcome(frame, AttemptStats::default());
		assert!(matches!(outcome, WorkerOutcome::ActorLost { .. }));
	}

	#[test]
	fn snapshots_reject_transient_secret_revisions() {
		let mut revision = revision();
		revision
			.spec
			.as_mut()
			.unwrap()
			.secrets
			.push(pb::SecretRef { name: "TOKEN".into(), version_presence: None });
		let error = ensure_snapshot_safe(revision.spec.as_ref()).unwrap_err();
		assert_eq!(error.code, "snapshot_with_secrets_unsupported");
	}

	#[test]
	fn artifact_result_envelope_stays_out_of_line() {
		let bytes = vec![7u8; 600 * 1024];
		let digest = Sha256::digest(&bytes).to_vec();
		let wire = json!({"version":1,"format":"json","compression":"none","sha256":format!("sha256:{}",hex::encode(&digest)),"uncompressed_size":bytes.len(),"artifact_digest":hex::encode(&digest)});
		let envelope = wire_to_envelope(&wire).unwrap();
		assert!(matches!(envelope.storage, Some(pb::value_envelope::Storage::Artifact(_))));
	}

	#[test]
	fn structured_runner_errors_are_bounded() {
		let frame = Frame(
			json!({"protocol":2,"type":"error","request_id":"x","error":{"phase":"call","type":"ValueError","module":"user","message":"bad","frames":[{"file":"app.py","line":7,"function":"call","code":"raise"}],"cause":{"type":"KeyError","message":"missing"}}}),
		);
		let error = terminal_error(&frame).unwrap_err();
		assert_eq!(error.error_type, "ValueError");
		assert_eq!(error.frames.len(), 1);
		assert_eq!(error.cause.unwrap().error_type, "KeyError");
	}
}
