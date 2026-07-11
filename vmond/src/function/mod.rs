//! Server-native durable function runtime.
//!
//! [`FunctionDomain`] owns the shared durable store, content-addressed
//! artifacts, executor, actor pins, secret material, reconnectable event
//! broadcasts, and every background task. Dropping an RPC handle does not drop
//! any of these resources or cancel durable work.

pub mod actor;
pub mod artifact;
pub mod gateway;
pub mod metrics;
pub mod protocol;
pub mod scheduler;
pub mod store;
pub mod worker;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use vmon_proto::v1 as pb;

use crate::engine::EngineApi;
use crate::home::Home;
use crate::{EngineError, Result};

use self::actor::ActorManager;
use self::artifact::ArtifactStore;
use self::metrics::{FunctionMetrics, MetricsSnapshot, ReproducibilityDescription};
use self::scheduler::{PoolPolicy, SchedulerControl, WorkerPool};
use self::store::{LeasedInput, Store};
use self::worker::{
	ActorOperation, ActorRequest, AttemptStats as WorkerAttemptStats, EngineExecutor, ExecuteRequest,
	ExecutionMode, Executor, SecretValues, Worker, WorkerError, WorkerEvent,
	WorkerOutcome, WorkerSnapshot,
};

const EVENT_REPLAY_LIMIT: u32 = 10_000;
const LEASE_MILLIS: u64 = 30_000;
const GLOBAL_WORKER_LIMIT: usize = 64;

struct ActiveExecution {
	worker: Arc<dyn Worker>,
	request_id: String,
}

/// Replay plus live subscription returned by [`FunctionDomain::watch_call`].
pub struct CallWatch {
	replay: std::collections::VecDeque<pb::CallEvent>,
	receiver: broadcast::Receiver<pb::CallEvent>,
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
	home: Home,
	store: Arc<Store>,
	artifacts: ArtifactStore,
	executor: Arc<dyn Executor>,
	actors: ActorManager,
	metrics: Arc<FunctionMetrics>,
	secrets: RwLock<HashMap<String, Vec<u8>>>,
	snapshots: RwLock<HashMap<String, WorkerSnapshot>>,
	watchers: Mutex<HashMap<String, broadcast::Sender<pb::CallEvent>>>,
	pools: Mutex<HashMap<String, WorkerPool>>,
	workers: Mutex<HashMap<String, Arc<dyn Worker>>>,
	active: Mutex<HashMap<String, ActiveExecution>>,
	control: Arc<SchedulerControl>,
	shutdown: broadcast::Sender<()>,
	tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl FunctionDomain {
	/// Open durable state and start lease-recovery, scheduler, schedule, and GC tasks.
	pub fn open(home: Home, engine: Arc<dyn EngineApi>) -> Result<Arc<Self>> {
		let _runtime = tokio::runtime::Handle::try_current()
			.map_err(|_| EngineError::engine("FunctionDomain::open requires a Tokio runtime"))?;
		let store = Arc::new(Store::open(&home)?);
		let now = unix_millis();
		store.recover_startup(now)?;
		let artifacts = ArtifactStore::open(home.function_artifacts_dir())?;
		let executor: Arc<dyn Executor> = EngineExecutor::new(home.clone(), engine);
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
			watchers: Mutex::new(HashMap::new()),
			pools: Mutex::new(HashMap::new()),
			workers: Mutex::new(HashMap::new()),
			active: Mutex::new(HashMap::new()),
			control,
			shutdown,
			tasks: Mutex::new(Vec::new()),
		});
		domain.start_background_tasks();
		Ok(domain)
	}

	/// Durable metadata store. API methods remain thin wrappers over this store.
	pub fn store(&self) -> &Store {
		&self.store
	}

	/// Immutable content-addressed payload store.
	pub fn artifacts(&self) -> &ArtifactStore {
		&self.artifacts
	}

	/// Process-local actor worker pins.
	pub fn actors(&self) -> &ActorManager {
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

	/// Wake the scheduler after a transaction creates or requeues work.
	pub fn notify_work(&self) {
		self.control.notify.notify_one();
	}

	/// Install transient secret material by reference name.
	///
	/// The bytes are never passed to the store and are overwritten in memory on
	/// replacement. A restart begins with an empty registry.
	pub fn set_secret(&self, name: impl Into<String>, value: Vec<u8>) {
		let name = name.into();
		if let Some(mut old) = self.secrets.write().insert(name, value) {
			old.fill(0);
		}
	}

	/// Remove all transient secret bytes from memory.
	pub fn clear_secrets(&self) {
		let mut secrets = self.secrets.write();
		for value in secrets.values_mut() {
			value.fill(0);
		}
		secrets.clear();
	}

	/// Record a reusable snapshot after its engine snapshot and provenance exist.
	pub fn record_snapshot(&self, snapshot: WorkerSnapshot) {
		self.snapshots.write().insert(snapshot.provenance.revision_id.clone(), snapshot);
	}

	/// Return a reusable snapshot only for the exact immutable revision.
	pub fn snapshot_for(&self, revision: &pb::FunctionRevision) -> Option<WorkerSnapshot> {
		let revision_id = revision.r#ref.as_ref()?.revision_id.as_str();
		self.snapshots
			.read()
			.get(revision_id)
			.filter(|snapshot| snapshot.provenance.matches(revision))
			.cloned()
	}

	/// Forget a reusable snapshot after explicit invalidation or deletion.
	pub fn remove_snapshot(&self, revision_id: &str) -> Option<WorkerSnapshot> {
		self.snapshots.write().remove(revision_id)
	}

	/// Resolve all required secret names into a fresh, executor-owned registry.
	pub(crate) fn secrets_for(&self, revision: &pb::FunctionRevision) -> Result<SecretValues> {
		let required = revision.spec.as_ref().map(|spec| spec.secrets.as_slice()).unwrap_or_default();
		let registry = self.secrets.read();
		let mut values = SecretValues::new();
		for secret in required {
			let value = registry.get(&secret.name).ok_or_else(|| {
				EngineError::not_running(format!("secret_unavailable: {}", secret.name))
			})?;
			values.insert(secret.name.clone(), value.clone());
		}
		Ok(values)
	}

	/// Subscribe before replaying durable events, eliminating the reconnect race.
	pub fn watch_call(&self, call_id: &str, after_sequence: u64) -> Result<CallWatch> {
		let receiver = self.subscribe_call(call_id);
		let replay = self.store.events_after(call_id, after_sequence, EVENT_REPLAY_LIMIT)?;
		Ok(CallWatch {
			replay: replay.into(),
			receiver,
			last_sequence: after_sequence,
		})
	}

	/// Subscribe to newly published events for one call.
	pub fn subscribe_call(&self, call_id: &str) -> broadcast::Receiver<pb::CallEvent> {
		let mut watchers = self.watchers.lock();
		watchers
			.entry(call_id.to_owned())
			.or_insert_with(|| broadcast::channel(256).0)
			.subscribe()
	}

	/// Publish an event after its store transaction commits.
	pub fn publish_call_event(&self, event: pb::CallEvent) {
		let Some(call_id) = event.call.as_ref().map(|call| call.call_id.clone()) else { return };
		let mut watchers = self.watchers.lock();
		if let Some(sender) = watchers.get(&call_id) {
			let _ = sender.send(event);
			if sender.receiver_count() == 0 {
				watchers.remove(&call_id);
			}
		}
	}

	fn publish_committed_events(&self, call_id: &str) {
		if !self.watchers.lock().contains_key(call_id) {
			return;
		}
		match self.store.events_after(call_id, 0, EVENT_REPLAY_LIMIT) {
			Ok(events) => {
				for event in events {
					self.publish_call_event(event);
				}
			}
			Err(error) => tracing::warn!(%error, %call_id, "call event publication failed"),
		}
	}

	/// Create or replace a typed schedule with its first durable firing frontier.
	pub fn create_schedule(&self, request: &pb::CreateScheduleRequest, now_ms: u64) -> Result<pb::ScheduleRecord> {
		let spec = request
			.spec
			.clone()
			.ok_or_else(|| EngineError::invalid("schedule spec is required"))?;
		let provisional = pb::ScheduleRecord {
			r#ref: None,
			spec: Some(spec),
			created_at_unix_millis: now_ms,
			updated_at_unix_millis: now_ms,
			next_run_presence: None,
		};
		let next_run = store::next_schedule_run(&provisional, now_ms)?;
		let schedule = self.store.put_schedule(request, next_run, now_ms)?;
		self.notify_work();
		Ok(schedule)
	}

	/// Durably create and initialize an actor before publishing it as ready.
	pub async fn create_actor(&self, request: &pb::CreateActorRequest) -> Result<pb::ActorRecord> {
		let now = unix_millis();
		let actor = self.store.create_actor(request, now)?;
		let actor_id = actor
			.r#ref
			.as_ref()
			.ok_or_else(|| EngineError::engine("created actor missing ref"))?
			.actor_id
			.clone();
		let status = pb::ActorStatus::try_from(actor.status).unwrap_or(pb::ActorStatus::Failed);
		if status != pb::ActorStatus::Creating {
			if status == pb::ActorStatus::Ready && self.actors.acquire(&actor_id).await.is_ok() {
				return Ok(actor);
			}
			let checkpoint_id = actor.latest_checkpoint_presence.as_ref().map(|presence| match presence {
				pb::actor_record::LatestCheckpointPresence::LatestCheckpoint(checkpoint) => {
					checkpoint.checkpoint_id.clone()
				}
			});
			if matches!(status, pb::ActorStatus::Ready | pb::ActorStatus::Stopped) {
				if let Some(checkpoint_id) = checkpoint_id {
					return self
						.restore_actor_checkpoint(&actor_id, &checkpoint_id, &format!("recover:{}", request.request_id))
						.await;
				}
			}
			let _ = self.store.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
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
				let _ = self.store.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
				return Err(error);
			}
		};
		let worker = match self.executor.spawn(revision, secrets).await {
			Ok(worker) => worker,
			Err(error) => {
				let _ = self.store.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
				return Err(EngineError::engine(error.to_string()));
			}
		};
		let input = request.initial_state_presence.as_ref().map(|state| match state {
			pb::create_actor_request::InitialStatePresence::InitialState(value) => value.clone(),
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
		match execution.completion.await.map_err(|error| EngineError::engine(error.to_string()))? {
			WorkerOutcome::Success { .. } => {
				self.actors.register(actor_id.clone(), None);
				self.actors.pin(&actor_id, Arc::clone(&worker), None)?;
				self.store.set_actor_status(&actor_id, pb::ActorStatus::Ready, Some(worker.id()), unix_millis())
			}
			outcome => {
				let _ = self.store.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
				Err(EngineError::engine(actor_outcome_message(&outcome)))
			}
		}
	}

	/// Capture and durably commit an immutable checkpoint from the pinned actor worker.
	pub async fn checkpoint_actor(&self, request: &pb::CheckpointActorRequest) -> Result<pb::ActorCheckpoint> {
		let actor_id = request
			.actor
			.as_ref()
			.ok_or_else(|| EngineError::invalid("actor is required"))?
			.actor_id
			.clone();
		let actor = self.store.get_actor(&actor_id)?;
		let permit = self.actors.acquire(&actor_id).await?;
		let checkpoint_id = format!("{}:{}", actor_id, request.request_id);
		let execution = permit
			.worker()
			.actor(ActorRequest {
				request_id: request.request_id.clone(),
				call_id: None,
				actor_id: actor_id.clone(),
				operation: ActorOperation::Checkpoint { checkpoint_id: checkpoint_id.clone() },
				input: None,
				deadline: None,
			})
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?;
		let state = match execution.completion.await.map_err(|error| EngineError::engine(error.to_string()))? {
			WorkerOutcome::Success { value, .. } => value,
			outcome => return Err(EngineError::engine(actor_outcome_message(&outcome))),
		};
		let now = unix_millis();
		let checkpoint = pb::ActorCheckpoint {
			r#ref: Some(pb::ActorCheckpointRef { checkpoint_id: checkpoint_id.clone() }),
			actor: actor.r#ref,
			function: actor.function,
			state: Some(state),
			sequence: now,
			created_at_unix_millis: now,
		};
		let checkpoint = self.store.put_checkpoint(&checkpoint, &request.request_id, now)?;
		self.actors.checkpoint_committed(&actor_id, checkpoint_id)?;
		Ok(checkpoint)
	}

	/// Restore an actor only after its compatible checkpoint is loaded by a worker.
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
		if let Ok(permit) = self.actors.acquire(&actor_id).await {
			let old_worker = Arc::clone(permit.worker());
			drop(permit);
			let _ = old_worker.retire().await;
			let _ = self.actors.worker_lost(&actor_id);
		}
		let restored = self.restore_actor_checkpoint(&actor_id, &checkpoint_id, &request.request_id).await;
		if restored.is_err() {
			let _ = self.store.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
		}
		restored
	}

	/// Fork a checkpoint into a new actor with an independent worker and checkpoint frontier.
	pub async fn fork_actor(&self, request: &pb::ForkActorRequest) -> Result<pb::ActorRecord> {
		let checkpoint_id = request
			.checkpoint
			.as_ref()
			.ok_or_else(|| EngineError::invalid("checkpoint is required"))?
			.checkpoint_id
			.clone();
		let checkpoint = self.store.get_checkpoint(&checkpoint_id)?;
		let create = pb::CreateActorRequest {
			function: checkpoint.function.clone(),
			initial_state_presence: checkpoint.state.clone().map(
				pb::create_actor_request::InitialStatePresence::InitialState,
			),
			request_id: request.request_id.clone(),
			labels: request.labels.clone(),
		};
		let actor = self.create_actor(&create).await?;
		let actor_id = actor
			.r#ref
			.as_ref()
			.ok_or_else(|| EngineError::engine("forked actor missing ref"))?
			.actor_id
			.clone();
		if let Ok(permit) = self.actors.acquire(&actor_id).await {
			let old_worker = Arc::clone(permit.worker());
			drop(permit);
			let _ = old_worker.retire().await;
		}
		self.restore_actor_checkpoint(&actor_id, &checkpoint_id, &request.request_id).await
	}

	/// Durably delete an actor, retire its live worker, and remove its process-local pin.
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

	async fn restore_actor_checkpoint(&self, actor_id: &str, checkpoint_id: &str, request_id: &str) -> Result<pb::ActorRecord> {
		let checkpoint = self.store.get_checkpoint(checkpoint_id)?;
		let actor = self.store.get_actor(actor_id)?;
		if actor.function != checkpoint.function {
			return Err(EngineError::invalid("checkpoint is incompatible with actor function"));
		}
		let function = actor.function.as_ref().ok_or_else(|| EngineError::engine("actor missing function"))?;
		let revision = Arc::new(self.store.get_revision(&function.revision_id)?);
		let worker = self
			.executor
			.spawn(Arc::clone(&revision), self.secrets_for(&revision)?)
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?;
		let state = checkpoint.state.clone().ok_or_else(|| EngineError::engine("checkpoint missing state"))?;
		let execution = worker
			.actor(ActorRequest {
				request_id: request_id.to_owned(),
				call_id: None,
				actor_id: actor_id.to_owned(),
				operation: ActorOperation::Restore { checkpoint_id: checkpoint_id.to_owned(), state },
				input: None,
				deadline: None,
			})
			.await
			.map_err(|error| EngineError::engine(error.to_string()))?;
		match execution.completion.await.map_err(|error| EngineError::engine(error.to_string()))? {
			WorkerOutcome::Success { .. } => {
				let restored = self.store.restore_actor(actor_id, checkpoint_id, request_id, unix_millis())?;
				self.actors.register(actor_id.to_owned(), Some(checkpoint_id.to_owned()));
				self.actors.pin(actor_id, worker, Some(checkpoint_id.to_owned()))?;
				Ok(restored)
			}
			outcome => Err(EngineError::engine(actor_outcome_message(&outcome))),
		}
	}

	/// Persist a cancellation request before signaling an active worker.
	pub async fn cancel_call(&self, call_id: &str, reason: &str, request_id: &str) -> Result<pb::CallRecord> {
		let call = self.store.cancel_call(call_id, reason, request_id, unix_millis())?;
		let active = self
			.active
			.lock()
			.get(call_id)
			.map(|execution| (Arc::clone(&execution.worker), execution.request_id.clone()));
		if let Some((worker, execution_request_id)) = active {
			worker
				.cancel(&execution_request_id)
				.await
				.map_err(|error| EngineError::engine(error.to_string()))?;
		}
		self.notify_work();
		Ok(call)
	}


	/// Ask every background task to stop and wait for clean termination.
	pub async fn shutdown(&self) {
		let _ = self.shutdown.send(());
		let tasks = std::mem::take(&mut *self.tasks.lock());
		for task in tasks {
			let _ = task.await;
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
					_ = domain.control.notify.notified() => domain.scheduler_tick().await,
					_ = interval.tick() => domain.scheduler_tick().await,
				}
			}
		});
		self.tasks.lock().push(task);
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
		let revisions = match self.store.queued_revisions(now) {
			Ok(revisions) => revisions,
			Err(error) => {
				tracing::warn!(%error, "function queue scan failed");
				return;
			}
		};
		for queued in revisions {
			let revision = match self.store.get_revision(&queued.revision_id) {
				Ok(revision) => Arc::new(revision),
				Err(error) => {
					tracing::warn!(%error, revision_id = %queued.revision_id, "queued revision is unavailable");
					continue;
				}
			};
			let policy = pool_policy(&revision);
			self.pools
				.lock()
				.entry(queued.revision_id.clone())
				.or_insert_with(|| WorkerPool::new(policy));

			let mut worker_id = self.pools.lock().get_mut(&queued.revision_id).and_then(WorkerPool::admit);
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
								let _ = self.store.mark_running(&leased.lease, now, pb::StartupKind::Unspecified);
								let unavailable = WorkerError::platform(
									"secret_unavailable",
									"required transient secret material is unavailable",
								);
								let _ = self.store.fail_user(&leased.lease, &call_error(&unavailable), now);
							}
							continue;
						}
					};
					let started = std::time::Instant::now();
					let spawned = if let Some(snapshot) = self.snapshot_for(&revision) {
						self.executor.restore(Arc::clone(&revision), snapshot, secrets).await
					} else {
						self.executor.spawn(Arc::clone(&revision), secrets).await
					};
					match spawned {
						Ok(worker) => {
							self.metrics.worker_started(started.elapsed().as_millis() as u64);
							if let Some(snapshot) = worker.initial_snapshot() {
								self.record_snapshot(snapshot);
							}
							let id = worker.id().to_owned();
							self.workers.lock().insert(id.clone(), worker);
							self.pools.lock().get_mut(&queued.revision_id).expect("pool inserted").add_worker(id, now);
							worker_id = self.pools.lock().get_mut(&queued.revision_id).and_then(WorkerPool::admit);
						}
						Err(error) => {
							tracing::warn!(%error, revision_id = %queued.revision_id, "worker startup failed");
							self.metrics.infrastructure_failure();
							continue;
						}
					}
				}
			}
			let Some(worker_id) = worker_id else { continue };
			let leased = match self.store.lease_next_for_revision(&queued.revision_id, &worker_id, now, LEASE_MILLIS) {
				Ok(Some(leased)) => leased,
				Ok(None) => {
					self.pools.lock().get_mut(&queued.revision_id).expect("pool inserted").abandon(&worker_id, now);
					continue;
				}
				Err(error) => {
					self.pools.lock().get_mut(&queued.revision_id).expect("pool inserted").abandon(&worker_id, now);
					tracing::warn!(%error, "input lease failed");
					continue;
				}
			};
			let Some(worker) = self.workers.lock().get(&worker_id).cloned() else {
				let _ = self.store.fail_infra(
					&leased.lease,
					&call_error(&WorkerError::infrastructure("worker_lost", "worker disappeared before execution")),
					now,
					now,
				);
				if let Some(pool) = self.pools.lock().get_mut(&queued.revision_id) {
					pool.abandon(&worker_id, now);
				}
				continue;
			};
			self.start_execution(leased, worker);
		}
		self.retire_idle(now);
	}

	fn start_execution(self: &Arc<Self>, leased: LeasedInput, worker: Arc<dyn Worker>) {
		let domain = Arc::clone(self);
		let request_id = format!("{}:{}:{}", leased.lease.call_id, leased.lease.input_index, leased.lease.lease_generation);
		self.active.lock().insert(
			leased.lease.call_id.clone(),
			ActiveExecution { worker: Arc::clone(&worker), request_id: request_id.clone() },
		);
		let task = tokio::spawn(async move {
			domain.execute_leased(leased, worker, request_id).await;
		});
		self.tasks.lock().push(task);
	}

	async fn execute_leased(&self, leased: LeasedInput, worker: Arc<dyn Worker>, request_id: String) {
		let now = unix_millis();
		let startup = if worker.initial_snapshot().is_some() { pb::StartupKind::Snapshot } else { pb::StartupKind::Warm };
		if let Err(error) = self.store.mark_running(&leased.lease, now, startup) {
			tracing::warn!(%error, "leased input could not enter running state");
			self.finish_worker_slot(&leased, worker.id());
			return;
		}
		self.publish_committed_events(&leased.lease.call_id);
		self.metrics.input_started(now.saturating_sub(leased.call.created_at_unix_millis));
		let call_type = pb::CallType::try_from(leased.call.r#type).unwrap_or(pb::CallType::Unary);
		let mode = match call_type {
			pb::CallType::Generator => ExecutionMode::Generator,
			pb::CallType::Batch => ExecutionMode::Batch,
			_ => ExecutionMode::Unary,
		};
		let Some(input) = leased.input.value.clone() else {
			let _ = self.store.fail_user(&leased.lease, &call_error(&WorkerError::platform("invalid_input", "input value is required")), unix_millis());
			self.finish_worker_slot(&leased, worker.id());
			return;
		};
		let deadline = leased.execution_deadline_ms.map(|millis| UNIX_EPOCH + Duration::from_millis(millis));
		let mut _actor_permit = None;
		let execution_result = if call_type == pb::CallType::Actor {
			let target = leased.call.target.as_ref();
			let actor_id = target
				.and_then(|target| target.actor_presence.as_ref())
				.map(|presence| match presence {
					pb::call_target::ActorPresence::Actor(actor) => actor.actor_id.clone(),
				});
			let method = target
				.and_then(|target| target.actor_method_presence.as_ref())
				.map(|presence| match presence {
					pb::call_target::ActorMethodPresence::ActorMethod(method) => method.clone(),
				});
			let (Some(actor_id), Some(method)) = (actor_id, method) else {
				let error = WorkerError::actor_lost("actor call target is incomplete");
				self.commit_worker_error(&leased, &error);
				self.finish_worker_slot(&leased, worker.id());
				return;
			};
			let permit = match self.actors.acquire(&actor_id).await {
				Ok(permit) => permit,
				Err(_) => {
					let record = self.store.get_actor(&actor_id);
					let checkpoint_id = record.ok().and_then(|record| {
						record.latest_checkpoint_presence.map(|presence| match presence {
							pb::actor_record::LatestCheckpointPresence::LatestCheckpoint(checkpoint) => checkpoint.checkpoint_id,
						})
					});
					if let Some(checkpoint_id) = checkpoint_id {
						if self
							.restore_actor_checkpoint(&actor_id, &checkpoint_id, &format!("recover:{request_id}"))
							.await
							.is_ok()
						{
							match self.actors.acquire(&actor_id).await {
								Ok(permit) => permit,
								Err(error) => {
									self.commit_worker_error(&leased, &WorkerError::actor_lost(error.to_string()));
									self.finish_worker_slot(&leased, worker.id());
									return;
								}
							}
						} else {
							self.commit_worker_error(&leased, &WorkerError::actor_lost("actor checkpoint restore failed"));
							self.finish_worker_slot(&leased, worker.id());
							return;
						}
					} else {
						let _ = self.store.set_actor_status(&actor_id, pb::ActorStatus::Failed, None, unix_millis());
						self.commit_worker_error(&leased, &WorkerError::actor_lost("actor worker was lost without a checkpoint"));
						self.finish_worker_slot(&leased, worker.id());
						return;
					}
				}
			};
			let actor_worker = Arc::clone(permit.worker());
			self.active.lock().insert(
				leased.lease.call_id.clone(),
				ActiveExecution { worker: Arc::clone(&actor_worker), request_id: request_id.clone() },
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
			_actor_permit = Some(permit);
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
					input_id: format!("{}:{}", leased.lease.call_id, leased.lease.input_index),
					input_index: leased.lease.input_index,
					attempt: leased.user_attempts.saturating_add(1),
					mode,
					input,
					deadline,
					parent_call_id: leased.call.graph.as_ref().and_then(|graph| graph.parent_call_ids.first()).cloned(),
					parent_request_id: None,
					interruptibility: scheduler::execution_interruptibility(worker.as_ref()),
				})
				.await
		};
		let execution = match execution_result {
			Ok(execution) => execution,
			Err(error) => {
				self.commit_worker_error(&leased, &error);
				self.finish_worker_slot(&leased, worker.id());
				return;
			}
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
				_ = tokio::time::sleep(Duration::from_millis(5)) => {
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
		match outcome {
			Ok(WorkerOutcome::Success { value, stats }) => {
				let result = pb::CallResult {
					call: None,
					index: leased.lease.input_index,
					outcome: Some(pb::call_result::Outcome::Value(value)),
					created_at_unix_millis: 0,
					sequence: 0,
					input_id: format!("{}:{}", leased.lease.call_id, leased.lease.input_index),
					input_index: leased.lease.input_index,
					yield_index_presence: None,
				};
				let proto_stats = attempt_stats(stats, startup);
				if let Err(error) = self.store.succeed(&leased.lease, &result, Some(&proto_stats), unix_millis()) {
					tracing::warn!(%error, "result commit failed");
				} else {
					self.metrics.input_succeeded(stats.execution_millis);
				}
				self.publish_committed_events(&leased.lease.call_id);
			}
			Ok(WorkerOutcome::Cancelled { reason, .. }) => {
				if let Err(error) = self.store.finish_cancelled(&leased.lease, &reason, unix_millis()) {
					tracing::warn!(%error, "cancellation completion commit failed");
				}
				self.publish_committed_events(&leased.lease.call_id);
			}
			Ok(WorkerOutcome::UserError { error, stats } | WorkerOutcome::PlatformError { error, stats }) => {
				self.metrics.user_failure(stats.execution_millis);
				self.commit_worker_error(&leased, &error);
			}
			Ok(WorkerOutcome::ActorLost { error, stats }) => {
				self.metrics.user_failure(stats.execution_millis);
				if let Some(actor_id) = call_actor_id(&leased.call) {
					let recovery = self.actors.worker_lost(&actor_id).ok();
					let status = if matches!(recovery, Some(actor::ActorRecovery::Restore { .. })) {
						pb::ActorStatus::Stopped
					} else {
						pb::ActorStatus::Failed
					};
					let _ = self.store.set_actor_status(&actor_id, status, None, unix_millis());
				}
				self.commit_worker_error(&leased, &error);
			}
			Ok(WorkerOutcome::InfrastructureError { error, .. }) | Err(error) => {
				self.metrics.infrastructure_failure();
				self.commit_worker_error(&leased, &error);
			}
		}
		self.active.lock().remove(&leased.lease.call_id);
		self.finish_worker_slot(&leased, worker.id());
		self.notify_work();
	}

	fn commit_worker_event(&self, leased: &LeasedInput, event: WorkerEvent) {
		let committed = match event {
			WorkerEvent::Yield { index, value } => self.store.commit_yield(&leased.lease, index, value, unix_millis()),
			WorkerEvent::Log { stream, message } => {
				let stream = match stream.as_str() {
					"stderr" => pb::LogStream::Stderr,
					"structured" => pb::LogStream::Structured,
					_ => pb::LogStream::Stdout,
				};
				self.store.append_log(&leased.lease, stream, message.into_bytes(), unix_millis())
			}
			WorkerEvent::Status { .. } => return,
		};
		match committed {
			Ok(event) => self.publish_call_event(event),
			Err(error) => tracing::warn!(%error, "worker event commit failed"),
		}
	}

	fn commit_worker_error(&self, leased: &LeasedInput, error: &WorkerError) {
		let now = unix_millis();
		let proto = call_error(error);
		if error.kind == self::worker::WorkerErrorKind::Infrastructure {
			let retry_at = now.saturating_add(scheduler::retry_backoff(100, 30_000, 2.0, leased.infra_attempts).as_millis() as u64);
			let _ = self.store.fail_infra(&leased.lease, &proto, retry_at, now);
			return;
		}
		let retry = leased.revision.spec.as_ref().and_then(|spec| spec.retry.as_ref());
		let attempt = leased.user_attempts.saturating_add(1);
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
			let _ = self.store.retry_user(&leased.lease, &proto, retry_at, now);
		} else {
			let _ = self.store.fail_user(&leased.lease, &proto, now);
		}
		self.publish_committed_events(&leased.lease.call_id);
	}

	fn finish_worker_slot(&self, leased: &LeasedInput, worker_id: &str) {
		self.active.lock().remove(&leased.lease.call_id);
		if let Some(revision_id) = leased.revision.r#ref.as_ref().map(|reference| reference.revision_id.as_str()) {
			if let Some(pool) = self.pools.lock().get_mut(revision_id) {
				pool.complete(worker_id, unix_millis());
			}
		}
	}

	fn retire_idle(&self, now: u64) {
		let mut retire = Vec::new();
		for pool in self.pools.lock().values_mut() {
			retire.extend(pool.retire_ready(now));
		}
		for worker_id in retire {
			if let Some(worker) = self.workers.lock().remove(&worker_id) {
				for pool in self.pools.lock().values_mut() {
					pool.remove_worker(&worker_id);
				}
				self.metrics.worker_retired();
				tokio::spawn(async move { let _ = worker.retire().await; });
			}
		}
	}


	/// Vibemon home that owns this domain.
	pub fn home(&self) -> &Home {
		&self.home
	}
}

fn call_actor_id(call: &pb::CallRecord) -> Option<String> {
	call.target
		.as_ref()?
		.actor_presence
		.as_ref()
		.map(|presence| match presence {
			pb::call_target::ActorPresence::Actor(actor) => actor.actor_id.clone(),
		})
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
			if value.idle_timeout_millis == 0 { 60_000 } else { value.idle_timeout_millis }
		})),
		max_calls: workers.map_or(0, |value| value.max_calls_per_worker),
		max_batch_size: batching.filter(|value| value.enabled).map_or(1, |value| value.max_batch_size.max(1)) as usize,
		batch_wait: Duration::from_millis(batching.map_or(0, |value| value.max_wait_millis)),
	}
}

fn attempt_stats(stats: WorkerAttemptStats, startup: pb::StartupKind) -> pb::AttemptStats {
	pb::AttemptStats {
		attempt: stats.attempt,
		startup: startup as i32,
		queued_millis: 0,
		startup_millis: stats.startup_millis,
		execution_millis: stats.execution_millis,
		cpu_millis: stats.cpu_millis,
		peak_memory_bytes: stats.peak_memory_bytes,
	}
}

fn call_error(error: &WorkerError) -> pb::CallError {
	pb::CallError {
		code: error.code.clone(),
		message: error.message.clone(),
		r#type: match error.kind {
			self::worker::WorkerErrorKind::User => "UserError",
			self::worker::WorkerErrorKind::Platform => "PlatformError",
			self::worker::WorkerErrorKind::Infrastructure => "InfrastructureError",
			self::worker::WorkerErrorKind::ActorLost => "ActorLost",
			self::worker::WorkerErrorKind::Cancelled => "Cancelled",
			self::worker::WorkerErrorKind::Timeout => "Timeout",
		}
		.into(),
		retryable: error.retryable,
		frames: Vec::new(),
		cause_presence: None,
		details: HashMap::new(),
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

fn unix_millis() -> u64 {
	use std::time::{SystemTime, UNIX_EPOCH};
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}
