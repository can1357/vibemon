//! Server-native durable function runtime.
//!
//! [`FunctionDomain`] owns the shared durable store, content-addressed
//! artifacts, executor, actor pins, secret material, reconnectable event
//! broadcasts, and every background task. Dropping an RPC handle does not drop
//! any of these resources or cancel durable work.

pub mod actor;
pub mod artifact;
pub mod gateway;
pub mod image;
pub mod metrics;
pub mod protocol;
pub mod scheduler;
pub mod store;
pub mod worker;

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
	time::{Duration, UNIX_EPOCH},
};

use parking_lot::{Mutex, RwLock};
use tokio::{sync::broadcast, task::JoinHandle};
use vmon_proto::v1 as pb;

use self::{
	actor::ActorManager,
	artifact::ArtifactStore,
	metrics::{FunctionMetrics, MetricsSnapshot, ReproducibilityDescription},
	scheduler::{PoolPolicy, SchedulerControl, WorkerPool},
	store::{LeasedInput, QueuedRevision, Store},
	worker::{
		ActorOperation, ActorRequest, AttemptStats as WorkerAttemptStats, BatchExecuteRequest,
		BatchExecution, BatchWorkerEvent, EngineExecutor, ExecuteRequest, ExecutionMode, Executor,
		SecretValues, ServiceDispatch, SnapshotProvenance, SnapshotReason, Worker, WorkerError,
		WorkerEvent, WorkerOutcome, WorkerSnapshot,
	},
};
use crate::{EngineError, Result, config::ServeConfig, engine::EngineApi, home::Home};

const EVENT_REPLAY_LIMIT: u32 = 10_000;
const SNAPSHOT_ARTIFACT_LIMIT: u64 = 64 * 1024 * 1024;
const LEASE_MILLIS: u64 = 30_000;
const GLOBAL_WORKER_LIMIT: usize = 64;

struct ActiveExecution {
	worker:     Arc<dyn Worker>,
	request_id: String,
}
type SecretKey = (String, String, Option<String>);
type ActiveKey = (String, String);

/// Replay plus live subscription returned by [`FunctionDomain::watch_call`].
pub struct CallWatch {
	replay:        std::collections::VecDeque<pb::CallEvent>,
	receiver:      broadcast::Receiver<pb::CallEvent>,
	last_sequence: u64,
}

impl CallWatch {
	/// Receive the next event, deduplicating the subscribe-before-replay race.
	pub async fn recv(&mut self) -> std::result::Result<pb::CallEvent, broadcast::error::RecvError> {
		loop {
			let event = if let Some(event) = self.replay.pop_front() {
				event
			} else {
				self.receiver.recv().await?
			};
			if event.sequence <= self.last_sequence {
				continue;
			}
			self.last_sequence = event.sequence;
			return Ok(event);
		}
	}
}

/// Shared ownership root for the durable function runtime.
pub struct FunctionDomain {
	home:           Home,
	store:          Arc<Store>,
	artifacts:      ArtifactStore,
	executor:       Arc<dyn Executor>,
	actors:         ActorManager,
	metrics:        Arc<FunctionMetrics>,
	secrets:        RwLock<HashMap<SecretKey, Vec<u8>>>,
	snapshots:      RwLock<HashMap<String, WorkerSnapshot>>,
	published:      Mutex<HashMap<String, u64>>,
	watchers:       Mutex<HashMap<String, broadcast::Sender<pb::CallEvent>>>,
	pools:          Mutex<HashMap<String, WorkerPool>>,
	workers:        Mutex<HashMap<String, Arc<dyn Worker>>>,
	active:         Mutex<HashMap<ActiveKey, ActiveExecution>>,
	worker_startup: Mutex<HashMap<String, pb::StartupKind>>,
	control:        Arc<SchedulerControl>,
	shutdown:       broadcast::Sender<()>,
	tasks:          Mutex<Vec<JoinHandle<()>>>,
}

impl FunctionDomain {
	/// Open durable state and start lease-recovery, scheduler, schedule, and GC
	/// tasks.
	pub fn open(home: Home, engine: Arc<dyn EngineApi>, config: &ServeConfig) -> Result<Arc<Self>> {
		let _runtime = tokio::runtime::Handle::try_current()
			.map_err(|_| EngineError::engine("FunctionDomain::open requires a Tokio runtime"))?;
		let store = Arc::new(Store::open_with_config(&home, config)?);
		let now = unix_millis();
		store.recover_startup(now)?;
		let artifacts = ArtifactStore::open_with_config(home.function_artifacts_dir(), config)?;
		let executor: Arc<dyn Executor> = EngineExecutor::new(home.clone(), engine, config.clone());
		let metrics = Arc::new(FunctionMetrics::default());
		let control = Arc::new(SchedulerControl::new(Arc::clone(&metrics)));
		let (shutdown, _) = broadcast::channel(4);
		let domain = Arc::new(Self {
			home,
			store,
			artifacts,
			executor,
			actors: ActorManager::new(),
			metrics,
			secrets: RwLock::new(HashMap::new()),
			snapshots: RwLock::new(HashMap::new()),
			published: Mutex::new(HashMap::new()),
			watchers: Mutex::new(HashMap::new()),
			pools: Mutex::new(HashMap::new()),
			workers: Mutex::new(HashMap::new()),
			active: Mutex::new(HashMap::new()),
			worker_startup: Mutex::new(HashMap::new()),
			control,
			shutdown,
			tasks: Mutex::new(Vec::new()),
		});
		domain.refresh_all_revision_availability()?;
		domain.reload_function_snapshots()?;
		domain.start_background_tasks();
		Ok(domain)
	}

	/// Durable metadata store. API methods remain thin wrappers over this store.
	pub fn store(&self) -> &Store {
		&self.store
	}

	/// Immutable content-addressed payload store.
	pub const fn artifacts(&self) -> &ArtifactStore {
		&self.artifacts
	}

	/// Process-local actor worker pins.
	pub const fn actors(&self) -> &ActorManager {
		&self.actors
	}

	/// Current process-local scheduler metric values.
	pub fn metrics(&self) -> MetricsSnapshot {
		self.metrics.snapshot()
	}

	/// Return the non-secret reproducibility description for a pinned revision.
	pub fn reproducibility(&self, revision_id: &str) -> Result<ReproducibilityDescription> {
		let revision = self.store.get_revision(revision_id)?;
		Ok(metrics::describe_reproducibility(&revision))
	}

	/// Resolve and verify an image at registration time, returning its pinned
	/// spec.
	pub fn realize_image(
		&self,
		spec: &pb::ImageSpec,
		architecture: pb::CpuArchitecture,
	) -> Result<pb::ImageSpec> {
		Ok(image::realize(&self.home, &self.artifacts, spec, architecture)?.resolved_spec)
	}

	/// Wake the scheduler after a transaction creates or requeues work.
	pub fn notify_work(&self) {
		self.control.notify.notify_one();
	}

	/// Install transient secret material for one immutable revision.
	///
	/// The bytes are keyed by revision, name, and optional provider version,
	/// never passed to the store, and overwritten in memory on replacement.
	pub fn set_secret(
		&self,
		revision_id: impl Into<String>,
		secret: &pb::SecretRef,
		value: Vec<u8>,
	) {
		let revision_id = revision_id.into();
		let key = (revision_id.clone(), secret.name.clone(), secret_version(secret));
		let replaced = self.secrets.write().insert(key, value);
		if let Some(mut old) = replaced {
			old.fill(0);
		}
		if let Err(error) = self.refresh_revision_availability(&revision_id) {
			tracing::warn!(%error, %revision_id, "revision availability refresh failed");
		}
	}

	/// Remove all transient secret bytes from memory.
	pub fn clear_secrets(&self) {
		{
			let mut secrets = self.secrets.write();
			for value in secrets.values_mut() {
				value.fill(0);
			}
			secrets.clear();
		}
		if let Err(error) = self.refresh_all_revision_availability() {
			tracing::warn!(%error, "revision availability refresh failed after clearing secrets");
		}
	}

	/// Recompute one revision's persisted availability from the memory-only
	/// registry.
	pub fn refresh_revision_availability(&self, revision_id: &str) -> Result<pb::FunctionRevision> {
		let revision = self.store.get_revision(revision_id)?;
		let registry = self.secrets.read();
		let unavailable = revision
			.spec
			.as_ref()
			.map(|spec| {
				spec
					.secrets
					.iter()
					.filter(|secret| {
						let key = (revision_id.to_owned(), secret.name.clone(), secret_version(secret));
						!registry.contains_key(&key)
					})
					.cloned()
					.collect()
			})
			.unwrap_or_default();
		drop(registry);
		self
			.store
			.set_revision_availability(revision_id, unavailable)
	}

	fn refresh_all_revision_availability(&self) -> Result<()> {
		let mut page_token = String::new();
		loop {
			let page = self.store.list_revisions(None, 1_000, &page_token)?;
			for revision in page.items {
				if let Some(revision_id) = revision
					.r#ref
					.as_ref()
					.map(|reference| reference.revision_id.as_str())
				{
					self.refresh_revision_availability(revision_id)?;
				}
			}
			if page.next_page_token.is_empty() {
				return Ok(());
			}
			page_token = page.next_page_token;
		}
	}

	/// Record a reusable snapshot after its engine snapshot and provenance
	/// exist.
	pub fn record_snapshot(&self, snapshot: WorkerSnapshot) {
		self
			.snapshots
			.write()
			.insert(snapshot.provenance.revision_id.clone(), snapshot);
	}

	/// Persist a captured worker snapshot and attach its verified record to the
	/// revision.
	pub fn persist_snapshot(
		&self,
		snapshot: WorkerSnapshot,
		revision: &pb::FunctionRevision,
	) -> Result<pb::FunctionSnapshotRecord> {
		if !snapshot.provenance.matches(revision) {
			return Err(EngineError::invalid("snapshot provenance does not match revision"));
		}
		if revision
			.spec
			.as_ref()
			.is_some_and(|spec| !spec.secrets.is_empty())
		{
			return Err(EngineError::unsupported(
				"snapshots with transient secret material are unsupported",
			));
		}
		let artifact = self.artifacts.put(snapshot.engine_snapshot.as_bytes())?;
		self.store.record_stored_artifact(
			&artifact,
			Some("application/vnd.vmon.engine-snapshot-name"),
			snapshot.created_at_unix_millis,
			None,
		)?;
		let artifact_ref = pb::ArtifactRef {
			digest: Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     hex::decode(&artifact.digest)
					.map_err(|error| EngineError::engine(error.to_string()))?,
			}),
		};
		let snapshot_id =
			format!("{}:{}", snapshot.provenance.revision_id, snapshot.created_at_unix_millis);
		let record = snapshot.to_record(snapshot_id, artifact_ref);
		let record = self.store.put_function_snapshot(&record)?;
		self.record_snapshot(snapshot);
		Ok(record)
	}

	fn reload_function_snapshots(&self) -> Result<()> {
		let mut page_token = String::new();
		loop {
			let page = self.store.list_revisions(None, 1_000, &page_token)?;
			for revision in page.items {
				let Some(pb::function_revision::SnapshotPresence::Snapshot(record)) =
					revision.snapshot_presence.as_ref()
				else {
					continue;
				};
				let Some(revision_id) = revision
					.r#ref
					.as_ref()
					.map(|reference| reference.revision_id.clone())
				else {
					continue;
				};
				let expected = match SnapshotProvenance::for_revision(&revision) {
					Ok(expected) => expected,
					Err(error) => {
						tracing::warn!(%error, %revision_id, "detaching invalid function snapshot");
						self.store.detach_function_snapshot(&revision_id)?;
						self.remove_snapshot(&revision_id);
						continue;
					},
				};
				if record.revision.as_ref() != revision.r#ref.as_ref()
					|| record.protocol_version != expected.runner_protocol as u32
					|| record
						.runner_digest
						.as_ref()
						.map(|digest| digest.value.as_slice())
						!= Some(expected.runner_digest.as_slice())
					|| record
						.image_digest
						.as_ref()
						.map(|digest| digest.value.as_slice())
						!= Some(expected.image_digest.as_slice())
					|| record
						.package_digest
						.as_ref()
						.map(|digest| digest.value.as_slice())
						!= Some(expected.package_digest.as_slice())
				{
					self.store.detach_function_snapshot(&revision_id)?;
					self.remove_snapshot(&revision_id);
					continue;
				}
				let Some(digest) = record
					.artifact
					.as_ref()
					.and_then(|artifact| artifact.digest.as_ref())
				else {
					self.store.detach_function_snapshot(&revision_id)?;
					self.remove_snapshot(&revision_id);
					continue;
				};
				let bytes = match self.artifacts.read_bounded(
					&hex::encode(&digest.value),
					None,
					SNAPSHOT_ARTIFACT_LIMIT,
				) {
					Ok(bytes) => bytes,
					Err(error) => {
						tracing::warn!(%error, %revision_id, "detaching unreadable function snapshot");
						self.store.detach_function_snapshot(&revision_id)?;
						self.remove_snapshot(&revision_id);
						continue;
					},
				};
				let engine_snapshot = match String::from_utf8(bytes) {
					Ok(snapshot) => snapshot,
					Err(error) => {
						tracing::warn!(%error, %revision_id, "detaching corrupt function snapshot");
						self.store.detach_function_snapshot(&revision_id)?;
						self.remove_snapshot(&revision_id);
						continue;
					},
				};
				self.record_snapshot(WorkerSnapshot {
					engine_snapshot,
					provenance: expected,
					reason: SnapshotReason::PostInitialize,
					created_at_unix_millis: record.created_at_unix_millis,
				});
			}
			if page.next_page_token.is_empty() {
				return Ok(());
			}
			page_token = page.next_page_token;
		}
	}

	/// Return a reusable snapshot only for the exact immutable revision.
	pub fn snapshot_for(&self, revision: &pb::FunctionRevision) -> Option<WorkerSnapshot> {
		let revision_id = revision.r#ref.as_ref()?.revision_id.as_str();
		self
			.snapshots
			.read()
			.get(revision_id)
			.filter(|snapshot| snapshot.provenance.matches(revision))
			.cloned()
	}

	/// Forget a reusable snapshot after explicit invalidation or deletion.
	pub fn remove_snapshot(&self, revision_id: &str) -> Option<WorkerSnapshot> {
		self.snapshots.write().remove(revision_id)
	}

	/// Resolve all required revision-scoped secrets into executor-owned memory.
	pub(crate) fn secrets_for(&self, revision: &pb::FunctionRevision) -> Result<SecretValues> {
		let revision_id = revision
			.r#ref
			.as_ref()
			.map(|reference| reference.revision_id.as_str())
			.ok_or_else(|| EngineError::invalid("revision id is required"))?;
		let required = revision
			.spec
			.as_ref()
			.map(|spec| spec.secrets.as_slice())
			.unwrap_or_default();
		let registry = self.secrets.read();
		let mut values = SecretValues::new();
		for secret in required {
			let key = (revision_id.to_owned(), secret.name.clone(), secret_version(secret));
			let value = registry.get(&key).ok_or_else(|| {
				EngineError::not_running(format!("secret_unavailable: {}", secret.name))
			})?;
			values.insert(secret.name.clone(), value.clone());
		}
		Ok(values)
	}

	/// Subscribe before replaying durable events, eliminating the reconnect
	/// race.
	pub fn watch_call(&self, call_id: &str, after_sequence: u64) -> Result<CallWatch> {
		let receiver = self.subscribe_call(call_id);
		let frontier = self.store.event_frontier(call_id)?;
		let mut cursor = after_sequence;
		let mut replay = std::collections::VecDeque::new();
		while cursor < frontier {
			let page = self
				.store
				.events_after(call_id, cursor, EVENT_REPLAY_LIMIT)?;
			let previous = cursor;
			for event in page
				.into_iter()
				.take_while(|event| event.sequence <= frontier)
			{
				cursor = event.sequence;
				replay.push_back(event);
			}
			if cursor == previous {
				return Err(EngineError::engine(format!(
					"call {call_id} event replay did not reach subscribed frontier {frontier}"
				)));
			}
		}
		Ok(CallWatch { replay, receiver, last_sequence: after_sequence })
	}

	/// Subscribe to newly published events for one call.
	pub fn subscribe_call(&self, call_id: &str) -> broadcast::Receiver<pb::CallEvent> {
		let mut watchers = self.watchers.lock();
		let receiver = watchers
			.entry(call_id.to_owned())
			.or_insert_with(|| broadcast::channel(256).0)
			.subscribe();
		let frontier = self.store.event_frontier(call_id).unwrap_or_default();
		self
			.published
			.lock()
			.entry(call_id.to_owned())
			.and_modify(|published| *published = (*published).max(frontier))
			.or_insert(frontier);
		receiver
	}

	/// Publish an event after its store transaction commits.
	pub fn publish_call_event(&self, event: pb::CallEvent) {
		let Some(call_id) = event.call.as_ref().map(|call| call.call_id.clone()) else {
			return;
		};
		let mut watchers = self.watchers.lock();
		self
			.published
			.lock()
			.entry(call_id.clone())
			.and_modify(|published| *published = (*published).max(event.sequence))
			.or_insert(event.sequence);
		if let Some(sender) = watchers.get(&call_id) {
			let _ = sender.send(event);
			if sender.receiver_count() == 0 {
				watchers.remove(&call_id);
				self.published.lock().remove(&call_id);
			}
		}
	}

	fn publish_committed_events(&self, call_id: &str) {
		if !self.watchers.lock().contains_key(call_id) {
			return;
		}
		let mut cursor = self
			.published
			.lock()
			.get(call_id)
			.copied()
			.unwrap_or_default();
		loop {
			match self.store.events_after(call_id, cursor, EVENT_REPLAY_LIMIT) {
				Ok(events) if events.is_empty() => return,
				Ok(events) => {
					let previous = cursor;
					for event in events {
						cursor = cursor.max(event.sequence);
						self.publish_call_event(event);
					}
					if cursor == previous {
						tracing::warn!(%call_id, cursor, "call event publication made no progress");
						return;
					}
				},
				Err(error) => {
					tracing::warn!(%error, %call_id, "call event publication failed");
					return;
				},
			}
		}
	}

	/// Create or replace a typed schedule with its first durable firing
	/// frontier.
	pub fn create_schedule(
		&self,
		request: &pb::CreateScheduleRequest,
		now_ms: u64,
	) -> Result<pb::ScheduleRecord> {
		let spec = request
			.spec
			.clone()
			.ok_or_else(|| EngineError::invalid("schedule spec is required"))?;
		let provisional = pb::ScheduleRecord {
			r#ref:                  None,
			spec:                   Some(spec),
			created_at_unix_millis: now_ms,
			updated_at_unix_millis: now_ms,
			next_run_presence:      None,
		};
		let next_run = store::next_schedule_run(&provisional, now_ms)?;
		let schedule = self.store.put_schedule(request, next_run, now_ms)?;
		self.notify_work();
		Ok(schedule)
	}

	/// Durably create and initialize an actor before publishing it as ready.
	pub async fn create_actor(&self, request: &pb::CreateActorRequest) -> Result<pb::ActorRecord> {
		let now = unix_millis();
		let function = request
			.function
			.as_ref()
			.ok_or_else(|| EngineError::invalid("actor function is required"))?;
		let admitted_revision = self.store.get_revision(&function.revision_id)?;
		let lifecycle = admitted_revision
			.spec
			.as_ref()
			.and_then(|spec| pb::FunctionLifecycle::try_from(spec.lifecycle).ok())
			.unwrap_or(pb::FunctionLifecycle::Stateless);
		if lifecycle != pb::FunctionLifecycle::Actor {
			return Err(EngineError::invalid(
				"durable actor creation requires ACTOR function lifecycle",
			));
		}
		let actor = self.store.create_actor(request, now)?;
		let actor_id = actor
			.r#ref
			.as_ref()
			.ok_or_else(|| EngineError::engine("created actor missing ref"))?
			.actor_id
			.clone();
		self.actors.register(actor_id.clone(), None);
		let create_gate = self.actors.acquire_gate(&actor_id).await?;
		let actor = self.store.get_actor(&actor_id)?;
		let status = pb::ActorStatus::try_from(actor.status).unwrap_or(pb::ActorStatus::Failed);
		if status != pb::ActorStatus::Creating {
			if status == pb::ActorStatus::Ready && self.actors.gated_worker(&create_gate).is_some() {
				return Ok(actor);
			}
			let checkpoint_id =
				actor
					.latest_checkpoint_presence
					.as_ref()
					.map(|presence| match presence {
						pb::actor_record::LatestCheckpointPresence::LatestCheckpoint(checkpoint) => {
							checkpoint.checkpoint_id.clone()
						},
					});
			if matches!(status, pb::ActorStatus::Ready | pb::ActorStatus::Stopped)
				&& let Some(checkpoint_id) = checkpoint_id
			{
				drop(create_gate);
				return self
					.restore_actor_checkpoint(
						&actor_id,
						&checkpoint_id,
						&format!("recover:{}", request.request_id),
					)
					.await;
			}
			let _ =
				self
					.store
					.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
			return Err(EngineError::not_running(format!(
				"actor {actor_id} has no recoverable live state"
			)));
		}
		let function = actor
			.function
			.as_ref()
			.ok_or_else(|| EngineError::engine("created actor missing function"))?;
		let revision = Arc::new(self.store.get_revision(&function.revision_id)?);
		let secrets = match self.secrets_for(&revision) {
			Ok(secrets) => secrets,
			Err(error) => {
				let _ =
					self
						.store
						.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
				return Err(error);
			},
		};
		let worker = match self.executor.spawn(revision, secrets).await {
			Ok(worker) => worker,
			Err(error) => {
				let _ =
					self
						.store
						.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
				return Err(EngineError::engine(error.to_string()));
			},
		};
		let input = request
			.initial_payload
			.as_ref()
			.map(|payload| match payload {
				pb::create_actor_request::InitialPayload::InitialValue(value) => {
					pb::call_input::Payload::Value(value.clone())
				},
				pb::create_actor_request::InitialPayload::InitialArguments(arguments) => {
					pb::call_input::Payload::Arguments(arguments.clone())
				},
			});
		let execution = worker
			.actor(ActorRequest {
				request_id: request.request_id.clone(),
				call_id: None,
				actor_id: actor_id.clone(),
				operation: ActorOperation::Create,
				input,
				deadline: None,
			})
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?;
		match execution
			.completion
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?
		{
			WorkerOutcome::Success { .. } => {
				self.actors.register(actor_id.clone(), None);
				self.actors.pin(&actor_id, Arc::clone(&worker), None)?;
				self.store.set_actor_status(
					&actor_id,
					pb::ActorStatus::Ready,
					Some(worker.id()),
					unix_millis(),
				)
			},
			outcome => {
				let _ =
					self
						.store
						.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
				Err(EngineError::engine(actor_outcome_message(&outcome)))
			},
		}
	}

	/// Capture and durably commit an immutable checkpoint from the pinned actor
	/// worker.
	pub async fn checkpoint_actor(
		&self,
		request: &pb::CheckpointActorRequest,
	) -> Result<pb::ActorCheckpoint> {
		let actor_id = request
			.actor
			.as_ref()
			.ok_or_else(|| EngineError::invalid("actor is required"))?
			.actor_id
			.clone();
		if let Some(checkpoint) = self.store.checkpoint_for_request(&request.request_id)? {
			if checkpoint
				.actor
				.as_ref()
				.map(|actor| actor.actor_id.as_str())
				!= Some(actor_id.as_str())
			{
				return Err(EngineError::invalid(
					"request_id was already used for a different actor checkpoint",
				));
			}
			return Ok(checkpoint);
		}
		let actor = self.store.get_actor(&actor_id)?;
		let permit = self.actors.acquire(&actor_id).await?;
		if let Some(checkpoint) = self.store.checkpoint_for_request(&request.request_id)? {
			if checkpoint
				.actor
				.as_ref()
				.map(|actor| actor.actor_id.as_str())
				!= Some(actor_id.as_str())
			{
				return Err(EngineError::invalid(
					"request_id was already used for a different actor checkpoint",
				));
			}
			return Ok(checkpoint);
		}
		let sequence = self
			.store
			.allocate_actor_operation(&actor_id, unix_millis())?;
		let checkpoint_id = format!("{}:{}", actor_id, request.request_id);
		let execution = match permit
			.worker()
			.actor(ActorRequest {
				request_id: request.request_id.clone(),
				call_id:    None,
				actor_id:   actor_id.clone(),
				operation:  ActorOperation::Checkpoint { checkpoint_id: checkpoint_id.clone() },
				input:      None,
				deadline:   None,
			})
			.await
		{
			Ok(execution) => execution,
			Err(error) => {
				self.complete_actor_operation(&actor_id, sequence);
				return Err(EngineError::engine(error.to_string()));
			},
		};
		let state = match execution.completion.await {
			Ok(WorkerOutcome::Success { value, .. }) => value,
			Ok(outcome) => {
				self.complete_actor_operation(&actor_id, sequence);
				return Err(EngineError::engine(actor_outcome_message(&outcome)));
			},
			Err(error) => {
				self.mark_actor_worker_lost(&actor_id);
				return Err(EngineError::engine(error.to_string()));
			},
		};
		let now = unix_millis();
		let checkpoint = pb::ActorCheckpoint {
			r#ref: Some(pb::ActorCheckpointRef { checkpoint_id: checkpoint_id.clone() }),
			actor: actor.r#ref,
			function: actor.function,
			state: Some(state),
			sequence,
			created_at_unix_millis: now,
		};
		let persisted = self
			.store
			.put_checkpoint(&checkpoint, &request.request_id, now);
		self.complete_actor_operation(&actor_id, sequence);
		let checkpoint = persisted?;
		let committed_id = checkpoint
			.r#ref
			.as_ref()
			.ok_or_else(|| EngineError::engine("persisted checkpoint missing ref"))?
			.checkpoint_id
			.clone();
		self.actors.checkpoint_committed(&actor_id, committed_id)?;
		Ok(checkpoint)
	}

	/// Restore an actor only after its compatible checkpoint is loaded by a
	/// worker.
	pub async fn restore_actor(&self, request: &pb::RestoreActorRequest) -> Result<pb::ActorRecord> {
		let actor_id = request
			.actor
			.as_ref()
			.ok_or_else(|| EngineError::invalid("actor is required"))?
			.actor_id
			.clone();
		let checkpoint_id = request
			.checkpoint
			.as_ref()
			.ok_or_else(|| EngineError::invalid("checkpoint is required"))?
			.checkpoint_id
			.clone();
		self
			.restore_actor_checkpoint(&actor_id, &checkpoint_id, &request.request_id)
			.await
	}

	/// Fork a checkpoint into a new actor with an independent worker and
	/// checkpoint frontier.
	pub async fn fork_actor(&self, request: &pb::ForkActorRequest) -> Result<pb::ActorRecord> {
		let checkpoint_id = request
			.checkpoint
			.as_ref()
			.ok_or_else(|| EngineError::invalid("checkpoint is required"))?
			.checkpoint_id
			.clone();
		let actor = self
			.store
			.fork_actor_from_checkpoint(request, unix_millis())?;
		let actor_id = actor
			.r#ref
			.as_ref()
			.ok_or_else(|| EngineError::engine("forked actor missing ref"))?
			.actor_id
			.clone();
		self
			.actors
			.register(actor_id.clone(), Some(checkpoint_id.clone()));
		self
			.restore_actor_checkpoint(&actor_id, &checkpoint_id, &request.request_id)
			.await
	}

	/// Durably delete an actor, retire its live worker, and remove its
	/// process-local pin.
	pub async fn delete_actor(&self, actor_id: &str) -> Result<pb::ActorRecord> {
		let deleted = self.store.delete_actor(actor_id, unix_millis())?;
		if let Ok(permit) = self.actors.acquire(actor_id).await {
			let worker = Arc::clone(permit.worker());
			drop(permit);
			let _ = worker.retire().await;
		}
		self.actors.remove(actor_id);
		Ok(deleted)
	}

	async fn restore_actor_checkpoint(
		&self,
		actor_id: &str,
		checkpoint_id: &str,
		request_id: &str,
	) -> Result<pb::ActorRecord> {
		let checkpoint = self.store.get_checkpoint(checkpoint_id)?;
		let actor = self.store.get_actor(actor_id)?;
		if actor.function != checkpoint.function {
			return Err(EngineError::invalid("checkpoint is incompatible with actor function"));
		}
		let function = actor
			.function
			.as_ref()
			.ok_or_else(|| EngineError::engine("actor missing function"))?;
		let actor_gate = self.actors.acquire_gate(actor_id).await?;
		let current = self.store.get_actor(actor_id)?;
		if self.actors.gated_worker(&actor_gate).is_some()
			&& current.status == pb::ActorStatus::Ready as i32
			&& current
				.latest_checkpoint_presence
				.as_ref()
				.is_some_and(|presence| match presence {
					pb::actor_record::LatestCheckpointPresence::LatestCheckpoint(checkpoint) => {
						checkpoint.checkpoint_id == checkpoint_id
					},
				}) {
			return Ok(current);
		}
		let old_worker = self.actors.gated_worker(&actor_gate);
		let revision = Arc::new(self.store.get_revision(&function.revision_id)?);
		let worker = self
			.executor
			.spawn(Arc::clone(&revision), self.secrets_for(&revision)?)
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?;
		let state = checkpoint
			.state
			.clone()
			.ok_or_else(|| EngineError::engine("checkpoint missing state"))?;
		let execution = worker
			.actor(ActorRequest {
				request_id: request_id.to_owned(),
				call_id:    None,
				actor_id:   actor_id.to_owned(),
				operation:  ActorOperation::Restore {
					checkpoint_id: checkpoint_id.to_owned(),
					state:         Box::new(state),
				},
				input:      None,
				deadline:   None,
			})
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?;
		match execution
			.completion
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?
		{
			WorkerOutcome::Success { .. } => {
				self
					.store
					.restore_actor(actor_id, checkpoint_id, request_id, unix_millis())?;
				let worker_id = worker.id().to_owned();
				self
					.actors
					.install_checkpoint_frontier(actor_id, checkpoint_id.to_owned())?;
				self
					.actors
					.pin(actor_id, worker, Some(checkpoint_id.to_owned()))?;
				let ready = self.store.set_actor_status_if(
					actor_id,
					pb::ActorStatus::Stopped,
					pb::ActorStatus::Ready,
					Some(&worker_id),
					unix_millis(),
				)?;
				drop(actor_gate);
				if let Some(old_worker) = old_worker {
					let _ = old_worker.retire().await;
				}
				Ok(ready)
			},
			outcome => Err(EngineError::engine(actor_outcome_message(&outcome))),
		}
	}

	/// Persist a cancellation request before signaling an active worker.
	pub async fn cancel_call(
		&self,
		call_id: &str,
		reason: &str,
		request_id: &str,
	) -> Result<pb::CallRecord> {
		let call = self
			.store
			.cancel_call(call_id, reason, request_id, unix_millis())?;
		self.publish_committed_events(call_id);
		let active = {
			let executions = self.active.lock();
			let mut seen = HashSet::new();
			executions
				.iter()
				.filter(|((active_call_id, _), _)| active_call_id == call_id)
				.filter(|&(_, execution)| seen.insert(execution.request_id.clone()))
				.map(|(_, execution)| (Arc::clone(&execution.worker), execution.request_id.clone()))
				.collect::<Vec<_>>()
		};
		for (worker, execution_request_id) in active {
			worker
				.cancel(&execution_request_id)
				.await
				.map_err(|error| EngineError::engine(error.to_string()))?;
		}
		self.notify_work();
		Ok(call)
	}

	async fn retire_worker(&self, worker: Arc<dyn Worker>) {
		let revision = self.store.get_revision(worker.revision_id()).ok();
		let snapshot_on_retire = revision
			.as_ref()
			.and_then(|revision| revision.spec.as_ref())
			.and_then(|spec| spec.lifecycle_hooks.as_ref())
			.is_some_and(|hooks| hooks.snapshot_on_worker_retire);
		retire_worker_ordered(worker, snapshot_on_retire, |snapshot| {
			let revision = revision
				.as_ref()
				.ok_or_else(|| EngineError::not_found("worker revision not found"))?;
			self.persist_snapshot(snapshot, revision).map(|_| ())
		})
		.await;
	}

	/// Ask every background task to stop and wait for clean termination.
	pub async fn shutdown(&self) {
		let _ = self.shutdown.send(());
		let tasks = std::mem::take(&mut *self.tasks.lock());
		for task in tasks {
			let _ = task.await;
		}
		let mut retiring = std::mem::take(&mut *self.workers.lock());
		self.pools.lock().clear();
		for worker in self.actors.drain_workers() {
			retiring.entry(worker.id().to_owned()).or_insert(worker);
		}
		for (_, worker) in retiring {
			self.retire_worker(worker).await;
		}
		self.clear_secrets();
	}

	fn start_background_tasks(self: &Arc<Self>) {
		let domain = Arc::clone(self);
		let mut shutdown = self.shutdown.subscribe();
		let task = tokio::spawn(async move {
			let mut interval = tokio::time::interval(Duration::from_millis(100));
			interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
			loop {
				tokio::select! {
					_ = shutdown.recv() => break,
					() = domain.control.notify.notified() => domain.scheduler_tick().await,
					_ = interval.tick() => domain.scheduler_tick().await,
				}
			}
		});
		self.tasks.lock().push(task);
	}

	fn track_task(&self, task: JoinHandle<()>) {
		let mut tasks = self.tasks.lock();
		reap_finished_tasks(&mut tasks);
		tasks.push(task);
	}

	async fn scheduler_tick(self: &Arc<Self>) {
		let now = unix_millis();
		if let Err(error) = self.store.expire_leases(now) {
			tracing::warn!(%error, "function lease expiry failed");
		}
		if let Err(error) = self.store.complete_ready_calls(now) {
			tracing::warn!(%error, "function completion scan failed");
		}
		if let Err(error) = self.store.fire_due_schedules(now, 128) {
			tracing::warn!(%error, "function schedule scan failed");
		}
		if let Err(error) = self.store.expire_results(now) {
			tracing::warn!(%error, "function result expiry failed");
		}
		if let Err(error) = self.artifacts.gc_expired(&self.store, now, 128) {
			tracing::warn!(%error, "function artifact GC failed");
		}
		let mut revisions = match self.store.queued_revisions(now) {
			Ok(revisions) => revisions,
			Err(error) => {
				tracing::warn!(%error, "function queue scan failed");
				return;
			},
		};
		let mut page_token = String::new();
		loop {
			let page = match self.store.list_revisions(None, 1_000, &page_token) {
				Ok(page) => page,
				Err(error) => {
					tracing::warn!(%error, "function revision scan failed");
					break;
				},
			};
			for revision in page.items {
				let Some(revision_id) = revision
					.r#ref
					.as_ref()
					.map(|reference| &reference.revision_id)
				else {
					continue;
				};
				if !revisions
					.iter()
					.any(|queued| queued.revision_id == *revision_id)
				{
					revisions.push(QueuedRevision {
						revision_id:         revision_id.clone(),
						oldest_available_ms: now,
						queued_inputs:       0,
					});
				}
			}
			if page.next_page_token.is_empty() {
				break;
			}
			page_token = page.next_page_token;
		}
		for queued in revisions {
			let revision = match self.store.get_revision(&queued.revision_id) {
				Ok(revision) => Arc::new(revision),
				Err(error) => {
					tracing::warn!(%error, revision_id = %queued.revision_id, "queued revision is unavailable");
					continue;
				},
			};
			let policy = pool_policy(&revision);
			self
				.pools
				.lock()
				.entry(queued.revision_id.clone())
				.or_insert_with(|| WorkerPool::new(policy));

			let mut worker_id = self
				.pools
				.lock()
				.get_mut(&queued.revision_id)
				.and_then(WorkerPool::admit);
			if worker_id.is_none() {
				let should_spawn = {
					let pools = self.pools.lock();
					let pool = pools.get(&queued.revision_id).expect("pool inserted");
					pool.worker_count() < pool.desired_workers(queued.queued_inputs as usize)
						&& self.workers.lock().len() < GLOBAL_WORKER_LIMIT
				};
				if should_spawn {
					let secrets = match self.secrets_for(&revision) {
						Ok(secrets) => secrets,
						Err(error) => {
							tracing::warn!(%error, revision_id = %queued.revision_id, "revision secrets unavailable");
							let unavailable_worker = format!("unavailable:{}", queued.revision_id);
							if let Ok(Some(leased)) = self.store.lease_next_for_revision(
								&queued.revision_id,
								&unavailable_worker,
								now,
								LEASE_MILLIS,
							) {
								let _ = self.store.mark_running(
									&leased.lease,
									now,
									pb::StartupKind::Unspecified,
								);
								let unavailable = WorkerError::platform(
									"secret_unavailable",
									"required transient secret material is unavailable",
								);
								let _ = self.store.fail_user(
									&leased.lease,
									&call_error(&unavailable),
									None,
									now,
								);
							}
							continue;
						},
					};
					let started = std::time::Instant::now();
					let snapshot = self.snapshot_for(&revision);
					let mut startup = initial_startup(snapshot.is_some());
					let spawned = if let Some(snapshot) = snapshot {
						match self
							.executor
							.restore(Arc::clone(&revision), snapshot, secrets)
							.await
						{
							Err(error) if !error.retryable => {
								tracing::warn!(%error, revision_id = %queued.revision_id, "function snapshot restore rejected; falling back to cold start");
								let revision_id = revision
									.r#ref
									.as_ref()
									.map(|reference| reference.revision_id.as_str())
									.ok_or_else(|| EngineError::engine("revision missing ref"));
								if let Ok(revision_id) = revision_id {
									if let Err(detach_error) =
										self.store.detach_function_snapshot(revision_id)
									{
										tracing::warn!(%detach_error, %revision_id, "failed to detach rejected function snapshot");
										Err(error)
									} else {
										self.remove_snapshot(revision_id);
										startup = pb::StartupKind::Cold;
										self
											.executor
											.spawn(
												Arc::clone(&revision),
												self.secrets_for(&revision).unwrap_or_default(),
											)
											.await
									}
								} else {
									Err(error)
								}
							},
							result => result,
						}
					} else {
						self.executor.spawn(Arc::clone(&revision), secrets).await
					};
					match spawned {
						Ok(worker) => {
							self
								.metrics
								.worker_started(started.elapsed().as_millis() as u64);
							if let Some(snapshot) = worker.initial_snapshot()
								&& let Err(error) = self.persist_snapshot(snapshot, &revision)
							{
								tracing::warn!(%error, revision_id = %queued.revision_id, "initial function snapshot persistence failed");
							}
							let id = worker.id().to_owned();
							self.worker_startup.lock().insert(id.clone(), startup);
							self.workers.lock().insert(id.clone(), worker);
							self
								.pools
								.lock()
								.get_mut(&queued.revision_id)
								.expect("pool inserted")
								.add_worker(id, now);
							worker_id = self
								.pools
								.lock()
								.get_mut(&queued.revision_id)
								.and_then(WorkerPool::admit);
						},
						Err(error) => {
							tracing::warn!(%error, revision_id = %queued.revision_id, "worker startup failed");
							self.metrics.infrastructure_failure();
							continue;
						},
					}
				}
			}
			let Some(worker_id) = worker_id else { continue };
			let worker = self.workers.lock().get(&worker_id).cloned();
			let Some(worker) = worker else {
				let mut pools = self.pools.lock();
				if let Some(pool) = pools.get_mut(&queued.revision_id) {
					pool.abandon(&worker_id, now);
				}
				continue;
			};
			if !worker.is_reusable() {
				let mut pools = self.pools.lock();
				if let Some(pool) = pools.get_mut(&queued.revision_id) {
					pool.remove_worker(&worker_id);
				}
				drop(pools);
				self.workers.lock().remove(&worker_id);
				self.worker_startup.lock().remove(&worker_id);
				self.metrics.worker_retired();
				tokio::spawn(async move {
					let _ = worker.retire().await;
				});
				continue;
			}
			// Image builds and snapshot restores may outlive one lease interval. Lease
			// against the current clock, not the scheduler scan that started the worker.
			let now = unix_millis();
			let batching = revision
				.spec
				.as_ref()
				.and_then(|spec| spec.batching.as_ref())
				.filter(|batching| batching.enabled);
			let max_batch_size = effective_batch_size(policy);
			if let Some(batching) = batching {
				let frontier = self
					.store
					.queued_batch_for_revision(&queued.revision_id, now);
				let ready = frontier
					.as_ref()
					.ok()
					.and_then(Option::as_ref)
					.is_some_and(|frontier| {
						scheduler::dynamic_batch_ready(
							frontier.queued_inputs,
							frontier.oldest_available_ms,
							now,
							max_batch_size,
							batching.max_wait_millis,
						)
					});
				if ready {
					match self.store.lease_batch_for_revision(
						&queued.revision_id,
						&worker_id,
						now,
						LEASE_MILLIS,
						max_batch_size,
					) {
						Ok(leased) if !leased.is_empty() => {
							self.start_batch_execution(leased, Arc::clone(&worker));
							continue;
						},
						Ok(_) => {},
						Err(error) => {
							tracing::warn!(%error, "dynamic batch lease failed");
						},
					}
				}
			}
			let leased = if batching.is_some() {
				self.store.lease_next_non_batch_for_revision(
					&queued.revision_id,
					&worker_id,
					now,
					LEASE_MILLIS,
				)
			} else {
				self
					.store
					.lease_next_for_revision(&queued.revision_id, &worker_id, now, LEASE_MILLIS)
			};
			let leased = match leased {
				Ok(Some(leased)) => leased,
				Ok(None) => {
					self
						.pools
						.lock()
						.get_mut(&queued.revision_id)
						.expect("pool inserted")
						.abandon(&worker_id, now);
					continue;
				},
				Err(error) => {
					self
						.pools
						.lock()
						.get_mut(&queued.revision_id)
						.expect("pool inserted")
						.abandon(&worker_id, now);
					tracing::warn!(%error, "input lease failed");
					continue;
				},
			};
			self.start_execution(leased, worker);
		}
		self.retire_idle(now);
	}

	fn start_execution(self: &Arc<Self>, leased: LeasedInput, worker: Arc<dyn Worker>) {
		let domain = Arc::clone(self);
		let request_id = format!(
			"{}:{}:{}",
			leased.lease.call_id, leased.lease.input_index, leased.lease.lease_generation
		);
		self.active.lock().insert(
			(leased.lease.call_id.clone(), leased.input.input_id.clone()),
			ActiveExecution { worker: Arc::clone(&worker), request_id: request_id.clone() },
		);
		let task = tokio::spawn(async move {
			domain.execute_leased(leased, worker, request_id).await;
		});
		self.track_task(task);
	}

	fn start_batch_execution(self: &Arc<Self>, leased: Vec<LeasedInput>, worker: Arc<dyn Worker>) {
		let Some(first) = leased.first() else { return };
		let request_id = format!(
			"batch:{}:{}:{}",
			first.lease.call_id, first.lease.input_index, first.lease.lease_generation
		);
		for item in &leased {
			self.active.lock().insert(
				(item.lease.call_id.clone(), item.input.input_id.clone()),
				ActiveExecution { worker: Arc::clone(&worker), request_id: request_id.clone() },
			);
		}
		let domain = Arc::clone(self);
		let task = tokio::spawn(async move {
			domain
				.execute_batch_leased(leased, worker, request_id)
				.await;
		});
		self.track_task(task);
	}

	async fn execute_leased(
		&self,
		leased: LeasedInput,
		worker: Arc<dyn Worker>,
		request_id: String,
	) {
		let now = unix_millis();
		let startup = self.worker_startup(worker.id());
		if let Err(error) = self.store.mark_running(&leased.lease, now, startup) {
			tracing::warn!(%error, "leased input could not enter running state");
			self.finish_worker_slot(&leased, worker.id(), true);
			return;
		}
		self.publish_committed_events(&leased.lease.call_id);
		self
			.metrics
			.input_started(now.saturating_sub(leased.available_ms));
		let call_type = pb::CallType::try_from(leased.call.r#type).unwrap_or(pb::CallType::Unary);
		if !lifecycle_accepts(&leased.revision, call_type) {
			self.commit_worker_error(
				&leased,
				&WorkerError::platform(
					"lifecycle_mismatch",
					"call type is incompatible with function lifecycle",
				),
				None,
			);
			self.finish_worker_slot(&leased, worker.id(), true);
			return;
		}
		let mode = scheduler::input_execution_mode(call_type);
		let Some(input) = leased.input.payload.clone() else {
			let _ = self.store.fail_user(
				&leased.lease,
				&call_error(&WorkerError::platform("invalid_input", "input payload is required")),
				None,
				unix_millis(),
			);
			self.finish_worker_slot(&leased, worker.id(), true);
			return;
		};
		let deadline = leased
			.execution_deadline_ms
			.map(|millis| UNIX_EPOCH + Duration::from_millis(millis));
		let mut actor_permit = None;
		let mut actor_operation: Option<(String, u64)> = None;
		let execution_result = if call_type == pb::CallType::Actor {
			let actor_target = leased
				.call
				.target
				.as_ref()
				.and_then(|target| target.receiver.as_ref())
				.and_then(|receiver| match receiver {
					pb::call_target::Receiver::Actor(actor) => Some(actor),
					pb::call_target::Receiver::Service(_) => None,
				});
			let actor_id = actor_target
				.and_then(|target| target.actor.as_ref())
				.map(|actor| actor.actor_id.clone());
			let method = actor_target
				.map(|target| target.method.clone())
				.filter(|method| !method.is_empty());
			let (Some(actor_id), Some(method)) = (actor_id, method) else {
				let error = WorkerError::actor_lost("actor call target is incomplete");
				self.commit_worker_error(&leased, &error, None);
				self.finish_worker_slot(&leased, worker.id(), true);
				return;
			};
			let permit = if let Ok(permit) = self.actors.acquire(&actor_id).await {
				permit
			} else {
				let record = self.store.get_actor(&actor_id);
				let checkpoint_id = record.ok().and_then(|record| {
					record
						.latest_checkpoint_presence
						.map(|presence| match presence {
							pb::actor_record::LatestCheckpointPresence::LatestCheckpoint(checkpoint) => {
								checkpoint.checkpoint_id
							},
						})
				});
				if let Some(checkpoint_id) = checkpoint_id {
					if self
						.restore_actor_checkpoint(
							&actor_id,
							&checkpoint_id,
							&format!("recover:{request_id}"),
						)
						.await
						.is_ok()
					{
						match self.actors.acquire(&actor_id).await {
							Ok(permit) => permit,
							Err(error) => {
								self.commit_worker_error(
									&leased,
									&WorkerError::actor_lost(error.to_string()),
									None,
								);
								self.finish_worker_slot(&leased, worker.id(), true);
								return;
							},
						}
					} else {
						self.commit_worker_error(
							&leased,
							&WorkerError::actor_lost("actor checkpoint restore failed"),
							None,
						);
						self.finish_worker_slot(&leased, worker.id(), true);
						return;
					}
				} else {
					let _ = self.store.set_actor_status(
						&actor_id,
						pb::ActorStatus::Failed,
						None,
						unix_millis(),
					);
					self.commit_worker_error(
						&leased,
						&WorkerError::actor_lost("actor worker was lost without a checkpoint"),
						None,
					);
					self.finish_worker_slot(&leased, worker.id(), true);
					return;
				}
			};
			let sequence = match self
				.store
				.allocate_actor_operation(&actor_id, unix_millis())
			{
				Ok(sequence) => sequence,
				Err(error) => {
					self.commit_worker_error(
						&leased,
						&WorkerError::platform("actor_busy", error.to_string()),
						None,
					);
					self.finish_worker_slot(&leased, worker.id(), true);
					return;
				},
			};
			actor_operation = Some((actor_id.clone(), sequence));
			let actor_worker = Arc::clone(permit.worker());
			self.active.lock().insert(
				(leased.lease.call_id.clone(), leased.input.input_id.clone()),
				ActiveExecution {
					worker:     Arc::clone(&actor_worker),
					request_id: request_id.clone(),
				},
			);
			let result = actor_worker
				.actor(ActorRequest {
					request_id: request_id.clone(),
					call_id: Some(leased.lease.call_id.clone()),
					actor_id,
					operation: ActorOperation::Method { name: method },
					input: Some(input.clone()),
					deadline,
				})
				.await;
			actor_permit = Some(permit);
			result
		} else {
			worker
				.execute(ExecuteRequest {
					request_id: request_id.clone(),
					function_id: leased
						.revision
						.r#ref
						.as_ref()
						.map(|reference| reference.revision_id.clone())
						.unwrap_or_default(),
					call_id: leased.lease.call_id.clone(),
					input_id: leased.input.input_id.clone(),
					input_index: leased.lease.input_index,
					attempt: leased.user_attempts.max(1),
					mode,
					input,
					deadline,
					parent_call_id: parent_edge(&leased.call).map(|parent| parent.call_id.clone()),
					parent_request_id: parent_edge(&leased.call).map(|parent| parent.input_id.clone()),
					interruptibility: scheduler::execution_interruptibility(worker.as_ref()),
					service: service_dispatch(&leased.call),
				})
				.await
		};
		let execution = match execution_result {
			Ok(execution) => execution,
			Err(error) => {
				self.commit_worker_error(&leased, &error, None);
				self.finish_worker_slot(&leased, worker.id(), false);
				if let Some((actor_id, _)) = &actor_operation {
					self.mark_actor_worker_lost(actor_id);
				}
				return;
			},
		};
		let events = execution.events;
		let mut completion = execution.completion;
		let mut heartbeat = tokio::time::interval(Duration::from_millis(LEASE_MILLIS / 3));
		let outcome = loop {
			tokio::select! {
				outcome = &mut completion => {
					for event in events.try_iter() {
						self.commit_worker_event(&leased, event);
					}
					break outcome;
				}
				() = tokio::time::sleep(Duration::from_millis(5)) => {
					for event in events.try_iter() {
						self.commit_worker_event(&leased, event);
					}
				}
				_ = heartbeat.tick() => {
					if let Err(error) = self.store.heartbeat(&leased.lease, unix_millis(), LEASE_MILLIS) {
						tracing::warn!(%error, "worker lease heartbeat failed");
					}
				}
			}
		};
		let mut reusable = true;
		match outcome {
			Ok(WorkerOutcome::Success { value, stats }) => {
				let result = pb::CallResult {
					call:                   None,
					index:                  leased.lease.input_index,
					outcome:                Some(pb::call_result::Outcome::Value(value)),
					created_at_unix_millis: 0,
					sequence:               0,
					input_id:               leased.input.input_id.clone(),
					input_index:            leased.lease.input_index,
					yield_index_presence:   None,
				};
				let proto_stats =
					attempt_stats(stats, startup, &leased, now.saturating_sub(leased.available_ms));
				if let Err(error) =
					self
						.store
						.succeed(&leased.lease, &result, Some(&proto_stats), unix_millis())
				{
					tracing::warn!(%error, "result commit failed");
				} else {
					self.metrics.input_succeeded(stats.execution_millis);
				}
				self.publish_committed_events(&leased.lease.call_id);
				if let Some((actor_id, sequence)) = &actor_operation {
					self.complete_actor_operation(actor_id, *sequence);
				}
			},
			Ok(WorkerOutcome::Cancelled { reason, .. }) => {
				if worker.interruptibility() == self::worker::Interruptibility::Sync {
					reusable = false;
				}
				if let Err(error) = self
					.store
					.finish_cancelled(&leased.lease, &reason, unix_millis())
				{
					tracing::warn!(%error, "cancellation completion commit failed");
				}
				self.publish_committed_events(&leased.lease.call_id);
				if let Some((actor_id, sequence)) = &actor_operation {
					self.complete_actor_operation(actor_id, *sequence);
				}
			},
			Ok(
				WorkerOutcome::UserError { error, stats }
				| WorkerOutcome::PlatformError { error, stats },
			) => {
				self.metrics.user_failure(stats.execution_millis);
				let proto_stats = failed_attempt_stats(
					stats,
					startup,
					&leased,
					now.saturating_sub(leased.available_ms),
					pb::AttemptFailureKind::User,
				);
				self.commit_worker_error(&leased, &error, Some(&proto_stats));
				if let Some((actor_id, sequence)) = &actor_operation {
					self.complete_actor_operation(actor_id, *sequence);
				}
			},
			Ok(WorkerOutcome::ActorLost { error, stats }) => {
				reusable = false;
				self.metrics.user_failure(stats.execution_millis);
				let proto_stats = failed_attempt_stats(
					stats,
					startup,
					&leased,
					now.saturating_sub(leased.available_ms),
					pb::AttemptFailureKind::User,
				);
				if let Some((actor_id, _)) = &actor_operation {
					self.mark_actor_worker_lost(actor_id);
				}
				self.commit_worker_error(&leased, &error, Some(&proto_stats));
			},
			Ok(WorkerOutcome::InfrastructureError { error, stats }) => {
				reusable = false;
				self.metrics.infrastructure_failure();
				let proto_stats = failed_attempt_stats(
					stats,
					startup,
					&leased,
					now.saturating_sub(leased.available_ms),
					pb::AttemptFailureKind::Infrastructure,
				);
				self.commit_worker_error(&leased, &error, Some(&proto_stats));
				if let Some((actor_id, _)) = &actor_operation {
					self.mark_actor_worker_lost(actor_id);
				}
			},
			Err(error) => {
				reusable = false;
				self.metrics.infrastructure_failure();
				self.commit_worker_error(&leased, &error, None);
				if let Some((actor_id, _)) = &actor_operation {
					self.mark_actor_worker_lost(actor_id);
				}
			},
		}
		drop(actor_permit);
		self.finish_worker_slot(&leased, worker.id(), reusable);
		self.notify_work();
	}

	async fn execute_batch_leased(
		&self,
		leased: Vec<LeasedInput>,
		worker: Arc<dyn Worker>,
		request_id: String,
	) {
		let Some(first) = leased.first() else { return };
		let startup = self.worker_startup(worker.id());
		if !lifecycle_accepts(&first.revision, pb::CallType::Batch) {
			for item in &leased {
				self.commit_worker_error(
					item,
					&WorkerError::platform(
						"lifecycle_mismatch",
						"batch call is incompatible with actor lifecycle",
					),
					None,
				);
			}
			self.finish_worker_slot(first, worker.id(), true);
			return;
		}
		let now = unix_millis();
		for item in &leased {
			if let Err(error) = self.store.mark_running(&item.lease, now, startup) {
				tracing::warn!(%error, input_index = item.lease.input_index, "batch input could not enter running state");
				for leased_item in &leased {
					let failure =
						WorkerError::infrastructure("batch_admission", "grouped batch admission failed");
					self.commit_worker_error(leased_item, &failure, None);
				}
				self.finish_worker_slot(first, worker.id(), true);
				return;
			}
			self
				.metrics
				.input_started(now.saturating_sub(item.available_ms));
			self.publish_committed_events(&item.lease.call_id);
		}
		let mut inputs = Vec::with_capacity(leased.len());
		for item in &leased {
			let Some(input) = item.input.payload.clone() else {
				for leased_item in &leased {
					self.commit_worker_error(
						leased_item,
						&WorkerError::platform("invalid_input", "input payload is required"),
						None,
					);
				}
				self.finish_worker_slot(first, worker.id(), true);
				return;
			};
			inputs.push(ExecuteRequest {
				request_id: format!(
					"{}:{}:{}",
					item.lease.call_id, item.lease.input_index, item.lease.lease_generation
				),
				function_id: item
					.revision
					.r#ref
					.as_ref()
					.map(|reference| reference.revision_id.clone())
					.unwrap_or_default(),
				call_id: item.lease.call_id.clone(),
				input_id: item.input.input_id.clone(),
				input_index: item.lease.input_index,
				attempt: item.user_attempts.max(1),
				mode: ExecutionMode::Batch,
				input,
				deadline: item
					.execution_deadline_ms
					.map(|millis| UNIX_EPOCH + Duration::from_millis(millis)),
				parent_call_id: parent_edge(&item.call).map(|parent| parent.call_id.clone()),
				parent_request_id: parent_edge(&item.call).map(|parent| parent.input_id.clone()),
				interruptibility: scheduler::execution_interruptibility(worker.as_ref()),
				service: service_dispatch(&item.call),
			});
		}
		let BatchExecution { events, mut completion } = match worker
			.execute_batch(BatchExecuteRequest { request_id: request_id.clone(), inputs })
			.await
		{
			Ok(execution) => execution,
			Err(error) => {
				for item in &leased {
					self.commit_worker_error(item, &error, None);
				}
				self.finish_worker_slot(first, worker.id(), false);
				self.notify_work();
				return;
			},
		};
		let mut heartbeat = tokio::time::interval(Duration::from_millis(LEASE_MILLIS / 3));
		let outcome = loop {
			tokio::select! {
				outcome = &mut completion => {
					for event in events.try_iter() {
						self.commit_batch_worker_event(&leased, event);
					}
					break outcome;
				}
				() = tokio::time::sleep(Duration::from_millis(5)) => {
					for event in events.try_iter() {
						self.commit_batch_worker_event(&leased, event);
					}
				}
				_ = heartbeat.tick() => {
					for item in &leased {
						if let Err(error) = self.store.heartbeat(&item.lease, unix_millis(), LEASE_MILLIS) {
							tracing::warn!(%error, input_index = item.lease.input_index, "batch lease heartbeat failed");
						}
					}
				}
			}
		};
		let mut reusable = true;
		match outcome {
			Ok(batch) => {
				for (item, terminal) in leased.iter().zip(batch.items) {
					match terminal.outcome {
						WorkerOutcome::Success { value, stats } => {
							let result = pb::CallResult {
								call:                   None,
								index:                  item.lease.input_index,
								outcome:                Some(pb::call_result::Outcome::Value(value)),
								created_at_unix_millis: 0,
								sequence:               0,
								input_id:               item.input.input_id.clone(),
								input_index:            item.lease.input_index,
								yield_index_presence:   None,
							};
							let proto_stats =
								attempt_stats(stats, startup, item, now.saturating_sub(item.available_ms));
							if self
								.store
								.succeed(&item.lease, &result, Some(&proto_stats), unix_millis())
								.is_ok()
							{
								self.metrics.input_succeeded(stats.execution_millis);
							}
						},
						WorkerOutcome::Cancelled { reason, .. } => {
							if worker.interruptibility() == self::worker::Interruptibility::Sync {
								reusable = false;
							}
							let _ = self
								.store
								.finish_cancelled(&item.lease, &reason, unix_millis());
						},
						WorkerOutcome::UserError { error, stats }
						| WorkerOutcome::PlatformError { error, stats } => {
							self.metrics.user_failure(stats.execution_millis);
							let proto_stats = failed_attempt_stats(
								stats,
								startup,
								item,
								now.saturating_sub(item.available_ms),
								pb::AttemptFailureKind::User,
							);
							self.commit_worker_error(item, &error, Some(&proto_stats));
						},
						WorkerOutcome::ActorLost { error, stats } => {
							reusable = false;
							self.metrics.user_failure(stats.execution_millis);
							let proto_stats = failed_attempt_stats(
								stats,
								startup,
								item,
								now.saturating_sub(item.available_ms),
								pb::AttemptFailureKind::User,
							);
							self.commit_worker_error(item, &error, Some(&proto_stats));
						},
						WorkerOutcome::InfrastructureError { error, stats } => {
							reusable = false;
							self.metrics.infrastructure_failure();
							let proto_stats = failed_attempt_stats(
								stats,
								startup,
								item,
								now.saturating_sub(item.available_ms),
								pb::AttemptFailureKind::Infrastructure,
							);
							self.commit_worker_error(item, &error, Some(&proto_stats));
						},
					}
					self.publish_committed_events(&item.lease.call_id);
				}
			},
			Err(error) => {
				reusable = false;
				for item in &leased {
					self.metrics.infrastructure_failure();
					self.commit_worker_error(item, &error, None);
				}
			},
		}
		for item in &leased {
			self
				.active
				.lock()
				.remove(&(item.lease.call_id.clone(), item.input.input_id.clone()));
		}
		self.finish_worker_slot(first, worker.id(), reusable);
		self.notify_work();
	}

	fn complete_actor_operation(&self, actor_id: &str, sequence: u64) {
		if let Err(error) = self
			.store
			.complete_actor_operation(actor_id, sequence, unix_millis())
		{
			tracing::warn!(%error, %actor_id, sequence, "actor READY transition failed");
		}
	}

	fn mark_actor_worker_lost(&self, actor_id: &str) {
		let _ = self.actors.worker_lost(actor_id);
		if let Err(error) = self.store.mark_actor_worker_lost(actor_id, unix_millis()) {
			tracing::warn!(%error, %actor_id, "actor loss transition failed");
		}
	}

	fn commit_batch_worker_event(&self, leased: &[LeasedInput], event: BatchWorkerEvent) {
		match event.input_index {
			Some(input_index) => {
				let Some(item) = leased
					.iter()
					.find(|item| item.lease.input_index == input_index)
				else {
					tracing::warn!(input_index, "worker event references input outside leased batch");
					return;
				};
				self.commit_worker_event(item, event.event);
			},
			None => match event.event {
				WorkerEvent::Status { .. } => {},
				WorkerEvent::Yield { .. } => {
					tracing::warn!("unindexed batch yield cannot be attributed to an input");
				},
				WorkerEvent::Log { stream, message } => {
					let Some(call_id) = leased.first().map(|item| item.lease.call_id.as_str()) else {
						return;
					};
					if leased.iter().any(|item| item.lease.call_id != call_id) {
						tracing::warn!("unindexed batch log spans more than one call");
						return;
					}
					match self.store.append_call_log(
						call_id,
						log_stream(&stream),
						message.into_bytes(),
						unix_millis(),
					) {
						Ok(event) => self.publish_call_event(event),
						Err(error) => {
							tracing::warn!(%error, %call_id, "unindexed batch log commit failed");
						},
					}
				},
			},
		}
	}

	fn commit_worker_event(&self, leased: &LeasedInput, event: WorkerEvent) {
		let committed = match event {
			WorkerEvent::Yield { index, value } => {
				self
					.store
					.commit_yield(&leased.lease, index, value, unix_millis())
			},
			WorkerEvent::Log { stream, message } => {
				let stream = log_stream(&stream);
				self
					.store
					.append_log(&leased.lease, stream, message.into_bytes(), unix_millis())
			},
			WorkerEvent::Status { .. } => return,
		};
		match committed {
			Ok(event) => self.publish_call_event(event),
			Err(error) => {
				tracing::warn!(%error, "worker event commit failed");
			},
		}
	}

	fn commit_worker_error(
		&self,
		leased: &LeasedInput,
		error: &WorkerError,
		stats: Option<&pb::AttemptStats>,
	) {
		let now = unix_millis();
		let proto = call_error(error);
		if error.kind == self::worker::WorkerErrorKind::Infrastructure {
			let retry_at = now.saturating_add(
				scheduler::retry_backoff(100, 30_000, 2.0, leased.infra_attempts).as_millis() as u64,
			);
			let _ = self
				.store
				.fail_infra(&leased.lease, &proto, retry_at, stats, now);
			self.publish_committed_events(&leased.lease.call_id);
			return;
		}
		let retry = leased
			.revision
			.spec
			.as_ref()
			.and_then(|spec| spec.retry.as_ref());
		let attempt = leased.user_attempts.max(1);
		let allowed = retry.is_some_and(|policy| {
			error.retryable
				&& attempt < policy.max_attempts.max(1)
				&& (policy.retryable_codes.is_empty() || policy.retryable_codes.contains(&error.code))
		});
		if allowed {
			let policy = retry.expect("checked above");
			let retry_at = now.saturating_add(
				scheduler::retry_backoff(
					policy.initial_backoff_millis,
					policy.max_backoff_millis,
					policy.backoff_multiplier,
					attempt,
				)
				.as_millis() as u64,
			);
			let _ = self
				.store
				.retry_user(&leased.lease, &proto, retry_at, stats, now);
		} else {
			let _ = self.store.fail_user(&leased.lease, &proto, stats, now);
		}
		self.publish_committed_events(&leased.lease.call_id);
	}

	fn worker_startup(&self, worker_id: &str) -> pb::StartupKind {
		let mut startups = self.worker_startup.lock();
		let startup = startups
			.get(worker_id)
			.copied()
			.unwrap_or(pb::StartupKind::Warm);
		startups.insert(worker_id.to_owned(), pb::StartupKind::Warm);
		startup
	}

	fn finish_worker_slot(&self, leased: &LeasedInput, worker_id: &str, reusable: bool) {
		self
			.active
			.lock()
			.remove(&(leased.lease.call_id.clone(), leased.input.input_id.clone()));
		let reusable = reusable
			&& self
				.workers
				.lock()
				.get(worker_id)
				.is_some_and(|worker| worker.is_reusable());
		let now = unix_millis();
		let revision_id = leased
			.revision
			.r#ref
			.as_ref()
			.map(|reference| reference.revision_id.as_str());
		if let Some(revision_id) = revision_id {
			let mut pools = self.pools.lock();
			if let Some(pool) = pools.get_mut(revision_id) {
				pool.complete(worker_id, now);
				if !reusable {
					pool.remove_worker(worker_id);
				}
			}
		}
		if reusable {
			return;
		}
		self.worker_startup.lock().remove(worker_id);
		let worker = self.workers.lock().remove(worker_id);
		if let Some(worker) = worker {
			self.metrics.worker_retired();
			tokio::spawn(async move {
				let _ = worker.retire().await;
			});
		}
	}

	fn retire_idle(&self, now: u64) {
		let mut retire = Vec::new();
		for pool in self.pools.lock().values_mut() {
			retire.extend(pool.retire_ready(now));
		}
		for worker_id in retire {
			let worker = self.workers.lock().remove(&worker_id);
			if let Some(worker) = worker {
				for pool in self.pools.lock().values_mut() {
					pool.remove_worker(&worker_id);
				}
				self.metrics.worker_retired();
				tokio::spawn(async move {
					let _ = worker.retire().await;
				});
			}
		}
	}

	/// Vibemon home that owns this domain.
	pub const fn home(&self) -> &Home {
		&self.home
	}
}

fn parent_edge(call: &pb::CallRecord) -> Option<&pb::ParentEdge> {
	call.graph.as_ref()?.parents.first()
}

fn log_stream(stream: &str) -> pb::LogStream {
	match stream {
		"stderr" => pb::LogStream::Stderr,
		"structured" => pb::LogStream::Structured,
		_ => pb::LogStream::Stdout,
	}
}

fn service_dispatch(call: &pb::CallRecord) -> Option<ServiceDispatch> {
	let receiver = call.target.as_ref()?.receiver.as_ref()?;
	match receiver {
		pb::call_target::Receiver::Actor(_) => None,
		pb::call_target::Receiver::Service(service) => Some(ServiceDispatch {
			service_key: service.service_key.clone(),
			method:      service.method.clone(),
			constructor: service.constructor.clone(),
		}),
	}
}

fn reap_finished_tasks(tasks: &mut Vec<JoinHandle<()>>) {
	tasks.retain(|task| !task.is_finished());
}

const fn initial_startup(restored: bool) -> pb::StartupKind {
	if restored {
		pb::StartupKind::Snapshot
	} else {
		pb::StartupKind::Cold
	}
}

fn pool_policy(revision: &pb::FunctionRevision) -> PoolPolicy {
	let spec = revision.spec.as_ref();
	let workers = spec.and_then(|spec| spec.workers.as_ref());
	let concurrency = spec.and_then(|spec| spec.concurrency.as_ref());
	let batching = spec.and_then(|spec| spec.batching.as_ref());
	let capacity = concurrency.map_or(1, |value| value.max_concurrent_calls.max(1)) as usize;
	PoolPolicy {
		min_workers: workers.map_or(0, |value| value.min_workers) as usize,
		max_workers: workers.map_or(1, |value| value.max_workers.max(1)) as usize,
		buffer_workers: workers.map_or(0, |value| value.buffer_workers) as usize,
		capacity,

		max_outstanding: workers
			.map_or(capacity, |value| value.max_outstanding_inputs.max(capacity as u32) as usize),
		idle_timeout: Duration::from_millis(workers.map_or(60_000, |value| {
			if value.idle_timeout_millis == 0 {
				60_000
			} else {
				value.idle_timeout_millis
			}
		})),
		max_calls: workers.map_or(0, |value| value.max_calls_per_worker),
		max_batch_size: batching
			.filter(|value| value.enabled)
			.map_or(1, |value| value.max_batch_size.max(1)) as usize,
		batch_wait: Duration::from_millis(batching.map_or(0, |value| value.max_wait_millis)),
	}
}

fn effective_batch_size(policy: PoolPolicy) -> u32 {
	policy
		.max_batch_size
		.min(policy.max_outstanding.saturating_sub(1).max(1))
		.try_into()
		.unwrap_or(u32::MAX)
}

fn secret_version(secret: &pb::SecretRef) -> Option<String> {
	secret
		.version_presence
		.as_ref()
		.map(|version| match version {
			pb::secret_ref::VersionPresence::Version(value) => value.clone(),
		})
}

fn lifecycle_accepts(revision: &pb::FunctionRevision, call_type: pb::CallType) -> bool {
	let lifecycle = revision
		.spec
		.as_ref()
		.and_then(|spec| pb::FunctionLifecycle::try_from(spec.lifecycle).ok())
		.unwrap_or(pb::FunctionLifecycle::Stateless);
	(lifecycle == pb::FunctionLifecycle::Actor) == (call_type == pb::CallType::Actor)
}

fn attempt_stats(
	stats: WorkerAttemptStats,
	startup: pb::StartupKind,
	leased: &LeasedInput,
	queued_millis: u64,
) -> pb::AttemptStats {
	let startup_millis = if startup == pb::StartupKind::Warm {
		0
	} else {
		stats.startup_millis
	};
	pb::AttemptStats {
		attempt_id: leased.attempt_id.clone(),
		user_attempt: leased.user_attempts.max(1),
		infra_attempt: leased.infra_attempts,
		startup: startup as i32,
		failure_kind: pb::AttemptFailureKind::Unspecified as i32,
		queued_millis,
		startup_millis,
		execution_millis: stats.execution_millis,
		cpu_millis: stats.cpu_millis,
		peak_memory_bytes: stats.peak_memory_bytes,
	}
}

fn failed_attempt_stats(
	stats: WorkerAttemptStats,
	startup: pb::StartupKind,
	leased: &LeasedInput,
	queued_millis: u64,
	failure_kind: pb::AttemptFailureKind,
) -> pb::AttemptStats {
	let mut stats = attempt_stats(stats, startup, leased, queued_millis);
	stats.failure_kind = failure_kind as i32;
	stats
}

fn call_error(error: &WorkerError) -> pb::CallError {
	pb::CallError {
		code:           error.code.clone(),
		message:        error.message.clone(),
		r#type:         if error.error_type.is_empty() {
			match error.kind {
				self::worker::WorkerErrorKind::User => "UserError",
				self::worker::WorkerErrorKind::Platform => "PlatformError",
				self::worker::WorkerErrorKind::Infrastructure => "InfrastructureError",
				self::worker::WorkerErrorKind::ActorLost => "ActorLost",
				self::worker::WorkerErrorKind::Cancelled => "Cancelled",
				self::worker::WorkerErrorKind::Timeout => "Timeout",
			}
			.into()
		} else {
			error.error_type.clone()
		},
		retryable:      error.retryable,
		frames:         error.frames.iter().take(128).cloned().collect(),
		cause_presence: error
			.cause
			.as_ref()
			.map(|cause| pb::call_error::CausePresence::Cause(Box::new(call_error(cause)))),
		details:        error
			.details
			.iter()
			.take(128)
			.map(|(key, value)| (key.clone(), value.clone()))
			.collect(),
	}
}

fn actor_outcome_message(outcome: &WorkerOutcome) -> String {
	match outcome {
		WorkerOutcome::Success { .. } => "actor operation succeeded unexpectedly".into(),
		WorkerOutcome::Cancelled { reason, .. } => format!("actor operation cancelled: {reason}"),
		WorkerOutcome::UserError { error, .. }
		| WorkerOutcome::PlatformError { error, .. }
		| WorkerOutcome::InfrastructureError { error, .. }
		| WorkerOutcome::ActorLost { error, .. } => error.to_string(),
	}
}

impl Drop for FunctionDomain {
	fn drop(&mut self) {
		let _ = self.shutdown.send(());
		for value in self.secrets.get_mut().values_mut() {
			value.fill(0);
		}
	}
}
async fn retire_worker_ordered(
	worker: Arc<dyn Worker>,
	snapshot_on_retire: bool,
	persist: impl FnOnce(WorkerSnapshot) -> Result<()>,
) {
	if snapshot_on_retire {
		match worker.snapshot(SnapshotReason::WorkerRetire).await {
			Ok(snapshot) => {
				if let Err(error) = persist(snapshot) {
					tracing::warn!(%error, worker_id = worker.id(), "worker-retire snapshot persistence failed");
				}
			},
			Err(error) => {
				tracing::warn!(%error, worker_id = worker.id(), "worker-retire snapshot capture failed");
			},
		}
	}
	let _ = worker.retire().await;
}

fn unix_millis() -> u64 {
	use std::time::{SystemTime, UNIX_EPOCH};
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
	use self::store::LeaseToken;
	use super::*;

	#[test]
	fn ordinary_function_policy_honors_pool_and_concurrency_settings() {
		let revision = pb::FunctionRevision {
			spec: Some(pb::FunctionSpec {
				workers: Some(pb::WorkerSpec {
					min_workers:            2,
					max_workers:            7,
					buffer_workers:         3,
					max_outstanding_inputs: 11,
					max_calls_per_worker:   23,
					idle_timeout_millis:    456,
				}),
				concurrency: Some(pb::ConcurrencySpec {
					max_concurrent_calls: 5,
					..Default::default()
				}),
				batching: Some(pb::BatchingSpec {
					enabled:         true,
					max_batch_size:  10,
					max_wait_millis: 12,
				}),
				..Default::default()
			}),
			..Default::default()
		};
		let policy = pool_policy(&revision);
		assert_eq!(policy.min_workers, 2);
		assert_eq!(policy.max_workers, 7);
		assert_eq!(policy.buffer_workers, 3);
		assert_eq!(policy.capacity, 5);
		assert_eq!(policy.max_outstanding, 11);
		assert_eq!(policy.max_calls, 23);
		assert_eq!(effective_batch_size(policy), 10);
	}

	#[test]
	fn grouped_batch_leaves_the_reserved_outstanding_slot_bounded() {
		let policy = PoolPolicy { max_outstanding: 4, max_batch_size: 20, ..PoolPolicy::default() };
		assert_eq!(effective_batch_size(policy), 3);
		let single = PoolPolicy { max_outstanding: 1, max_batch_size: 20, ..policy };
		assert_eq!(effective_batch_size(single), 1);
	}

	#[test]
	fn attempt_stats_correlate_with_persisted_lease_identity() {
		let leased = LeasedInput {
			lease:                 LeaseToken {
				call_id:          "call".into(),
				input_index:      4,
				worker_id:        "worker".into(),
				lease_generation: 9,
			},
			revision:              Default::default(),
			input:                 Default::default(),
			call:                  Default::default(),
			user_attempts:         2,
			infra_attempts:        3,
			execution_deadline_ms: None,
			attempt_id:            "persisted-attempt-uuid".into(),
			available_ms:          100,
		};
		let stats = attempt_stats(
			WorkerAttemptStats {
				startup_millis: 7,
				execution_millis: 8,
				cpu_millis: 9,
				peak_memory_bytes: 10,
				..Default::default()
			},
			pb::StartupKind::Cold,
			&leased,
			55,
		);
		assert_eq!(stats.attempt_id, "persisted-attempt-uuid");
		assert_eq!(stats.user_attempt, 2);
		assert_eq!(stats.infra_attempt, 3);
		assert_eq!(stats.queued_millis, 55);
		assert_eq!(stats.startup_millis, 7);
		let warm = attempt_stats(
			WorkerAttemptStats { startup_millis: 7, ..Default::default() },
			pb::StartupKind::Warm,
			&leased,
			0,
		);
		assert_eq!(warm.startup_millis, 0);
		let failed = failed_attempt_stats(
			WorkerAttemptStats { execution_millis: 11, ..Default::default() },
			pb::StartupKind::Warm,
			&leased,
			2,
			pb::AttemptFailureKind::Infrastructure,
		);
		assert_eq!(failed.failure_kind, pb::AttemptFailureKind::Infrastructure as i32);
	}

	#[test]
	fn startup_lifecycle_is_cold_or_snapshot_then_warm() {
		assert_eq!(initial_startup(false), pb::StartupKind::Cold);
		assert_eq!(initial_startup(true), pb::StartupKind::Snapshot);
		let mut lifecycle = HashMap::from([("worker".to_owned(), pb::StartupKind::Cold)]);
		let first = lifecycle.get("worker").copied().unwrap();
		lifecycle.insert("worker".to_owned(), pb::StartupKind::Warm);
		assert_eq!(first, pb::StartupKind::Cold);
		assert_eq!(lifecycle["worker"], pb::StartupKind::Warm);
	}

	#[tokio::test]
	async fn completed_execution_tasks_are_reaped() {
		let complete = tokio::spawn(async {});
		complete.await.unwrap();
		let pending = tokio::spawn(std::future::pending());
		let mut tasks = vec![tokio::spawn(async {}), pending];
		tokio::task::yield_now().await;
		reap_finished_tasks(&mut tasks);
		assert_eq!(tasks.len(), 1);
		tasks.pop().unwrap().abort();
	}

	struct OrderedRetireWorker {
		events: Mutex<Vec<&'static str>>,
	}

	impl Worker for OrderedRetireWorker {
		fn id(&self) -> &'static str {
			"ordered-retire"
		}

		fn revision_id(&self) -> &'static str {
			"revision"
		}

		fn capacity(&self) -> usize {
			1
		}

		fn interruptibility(&self) -> worker::Interruptibility {
			worker::Interruptibility::Async
		}

		fn execute(
			&self,
			_request: ExecuteRequest,
		) -> worker::BoxFuture<Result<worker::Execution, WorkerError>> {
			Box::pin(async { Err(WorkerError::platform("unused", "unused")) })
		}

		fn cancel(&self, _request_id: &str) -> worker::BoxFuture<Result<(), WorkerError>> {
			Box::pin(async { Ok(()) })
		}

		fn snapshot(
			&self,
			reason: SnapshotReason,
		) -> worker::BoxFuture<Result<WorkerSnapshot, WorkerError>> {
			self.events.lock().push("snapshot");
			Box::pin(async move {
				Ok(WorkerSnapshot {
					engine_snapshot: "snapshot".into(),
					provenance: SnapshotProvenance {
						revision_id:     "revision".into(),
						function:        None,
						image_digest:    vec![],
						spec_digest:     vec![],
						package_digest:  vec![],
						initialize_hook: None,
						runner_digest:   vec![],
						runner_protocol: 2,
					},
					reason,
					created_at_unix_millis: 1,
				})
			})
		}

		fn actor(
			&self,
			_request: ActorRequest,
		) -> worker::BoxFuture<Result<worker::Execution, WorkerError>> {
			Box::pin(async { Err(WorkerError::platform("unused", "unused")) })
		}

		fn retire(&self) -> worker::BoxFuture<Result<(), WorkerError>> {
			self.events.lock().push("retire");
			Box::pin(async { Ok(()) })
		}

		fn initial_snapshot(&self) -> Option<WorkerSnapshot> {
			None
		}
	}

	#[tokio::test]
	async fn worker_retire_snapshot_is_persisted_before_shutdown_hook() {
		let worker = Arc::new(OrderedRetireWorker { events: Mutex::new(Vec::new()) });
		let erased: Arc<dyn Worker> = worker.clone();
		retire_worker_ordered(erased, true, |_| {
			worker.events.lock().push("persist");
			Ok(())
		})
		.await;
		assert_eq!(*worker.events.lock(), vec!["snapshot", "persist", "retire"]);
	}
}
