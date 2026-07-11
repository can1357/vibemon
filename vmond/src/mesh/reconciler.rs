//! Background orphan reconciliation and replication loops.
//!
//! Port of `python/vmon/server/reconciler.py`.  This module intentionally keeps
//! the neighboring state/lease/record/replica implementations behind narrow
//! traits so the route/state agents can wire their concrete stores without this
//! file editing them.  The ordering, quorum checks, HA tiers, worker keys, and
//! peer wire payloads mirror the Python implementation.

use std::{
	collections::{HashMap, HashSet},
	path::{Path, PathBuf},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use parking_lot::Mutex;
use reqwest::Client;
use serde_json::{Map, Value, json};
use tokio::{sync::Semaphore, time::sleep};
use tracing::{error, info, warn};

use crate::{EngineError, Result, config::ServeConfig, mesh::lease::LeaseRecord};

#[derive(Clone, Debug)]
pub struct ReconcileConfig {
	pub replicate_sec:         f64,
	pub replicas:              usize,
	pub replicate_concurrency: usize,
	pub restore_quorum:        Option<bool>,
	pub outbound_token:        String,
}

impl ReconcileConfig {
	pub fn from_serve_config(
		config: &ServeConfig,
		mesh_enabled: bool,
		outbound_token: String,
	) -> Self {
		Self {
			replicate_sec: config.effective_replicate_sec(mesh_enabled),
			replicas: config.replicas,
			replicate_concurrency: config.replicate_concurrency,
			restore_quorum: config.restore_quorum,
			outbound_token,
		}
	}

	pub fn restore_quorum_enabled(&self, expected_members: usize) -> bool {
		self.restore_quorum.unwrap_or(expected_members >= 3)
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateRecord {
	pub sid:             String,
	pub params:          Value,
	pub owner:           String,
	pub epoch:           u64,
	pub idempotency_key: String,
	pub ha:              String,
	pub restart_policy:  String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRecord {
	pub sid:           String,
	pub digest:        String,
	pub source_node:   String,
	pub snapshot_dir:  PathBuf,
	pub params:        Value,
	pub needs_secrets: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicatePreparation {
	pub digest:       String,
	pub snapshot_dir: PathBuf,
	pub params:       Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxRecord {
	pub name:   String,
	pub detail: Map<String, Value>,
}

pub trait MeshReconcileState: Send + Sync {
	fn enabled(&self) -> bool;
	fn interval(&self) -> f64;
	fn node_id(&self) -> &str;
	fn advertise(&self) -> String;
	fn create_timeout(&self) -> Duration;
	fn expected_members(&self) -> usize;
	fn live_member_ids(&self) -> Vec<String>;
	fn peer_url(&self, node_id: &str) -> Option<String>;
	fn is_peer_healthy(&self, node_id: &str) -> bool;
	fn owner_of(&self, sid: &str) -> Option<String>;
	fn restore_owner(&self, sid: &str, exclude: &HashSet<String>) -> Option<String>;
	fn restore_quorum_met(&self, confirmations: usize) -> bool;
	fn quorum_needed(&self) -> usize;
	fn drain_orphans(&self) -> Vec<(String, String)>;
	fn requeue_orphan(&self, sid: &str, dead: &str);
	fn next_epoch(&self, sid: &str) -> u64;
	fn broadcast_owner(&self, sid: &str, node_id: &str, epoch: u64) -> Result<()>;
	fn update_record_owner(
		&self,
		sid: &str,
		node_id: &str,
		epoch: u64,
		require_quorum: bool,
	) -> Result<()>;
	fn worker_begin(&self, key: &str) -> bool;
	fn worker_end(&self, key: &str);
	fn note_event(&self, event: &str);
	fn replica_targets(&self, sid: &str, replicas: usize) -> Vec<String>;
	fn fenced_local_ids(&self, owned_ids: &[String]) -> Vec<String>;
	fn forget_local_owner(&self, sid: &str);
}

pub trait ReconcileEngine: Send + Sync {
	fn owned_ids(&self) -> Result<Vec<String>>;
	fn get(&self, sid: &str) -> Result<SandboxRecord>;
	fn remove(&self, sid: &str) -> Result<()>;
	fn replicate_prepare<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<ReplicatePreparation>>;
	fn replicate_cleanup(&self, digest: &str, snapshot_dir: &Path) -> Result<()>;
	fn restore_from_template(
		&self,
		params: &Value,
		snapshot_dir: &Path,
		quorum_ok: bool,
	) -> Result<SandboxRecord>;
	fn record_volume_leases(&self, sid: &str, leases: &[LeaseRecord]) -> Result<()>;
	fn record_idempotency(&self, sid: &str, key: &str) -> Result<()>;
	fn set_ha_metadata(&self, sid: &str, ha: &str, restart_policy: &str) -> Result<()>;
	fn create_from_record(&self, params: &Value) -> Result<SandboxRecord>;
	fn run_from_record(&self, params: &Value) -> Result<Value>;
	fn restore_from_record(&self, params: &Value) -> Result<Value>;
}

pub trait CreateRecordStore: Send + Sync {
	fn get(&self, sid: &str) -> Option<CreateRecord>;
	fn reconcile_records(&self) -> Result<()>;
}

pub trait ReplicaStore: Send + Sync {
	fn get(&self, sid: &str) -> Option<ReplicaRecord>;
	fn holds(&self, sid: &str) -> bool;
	fn secrets_ready(&self, sid: &str) -> bool;
	fn drop_replica(&self, sid: &str) -> Result<()>;
}

pub trait LeaseManager: Send + Sync {
	/// Renew every local writable-volume lease by strict majority, stopping
	/// writers that miss their renew deadline.
	fn renew_writable_volume_leases_once(&self) -> BoxFuture<'_, Result<()>>;
	/// Acquire strict-majority leases for all writable volumes in `params`.
	fn acquire_writable_volume_leases<'a>(
		&'a self,
		params: &'a Value,
		epoch: u64,
	) -> BoxFuture<'a, Result<Vec<LeaseRecord>>>;
	/// Best-effort release for already-granted lease records.
	fn release_leases<'a>(&'a self, leases: &'a [LeaseRecord]) -> BoxFuture<'a, Result<()>>;
	/// Release every lease recorded for a sandbox and clear its metadata.
	fn release_record_volume_leases<'a>(
		&'a self,
		record: &'a SandboxRecord,
	) -> BoxFuture<'a, Result<()>>;
}

pub struct Reconciler<'a> {
	pub config:           ReconcileConfig,
	pub mesh:             &'a dyn MeshReconcileState,
	pub engine:           &'a dyn ReconcileEngine,
	pub records:          &'a dyn CreateRecordStore,
	pub replicas:         &'a dyn ReplicaStore,
	pub leases:           &'a dyn LeaseManager,
	pub client:           &'a Client,
	pub checkpoint_times: Mutex<HashMap<String, f64>>,
}

impl<'a> Reconciler<'a> {
	pub fn new(
		config: ReconcileConfig,
		mesh: &'a dyn MeshReconcileState,
		engine: &'a dyn ReconcileEngine,
		records: &'a dyn CreateRecordStore,
		replicas: &'a dyn ReplicaStore,
		leases: &'a dyn LeaseManager,
		client: &'a Client,
	) -> Self {
		Self {
			config,
			mesh,
			engine,
			records,
			replicas,
			leases,
			client,
			checkpoint_times: Mutex::new(HashMap::new()),
		}
	}

	/// One replication cadence.  The caller owns `last` so `replicate_forever`
	/// can suppress redundant transfers across sleeps without storing the digest
	/// in durable state.
	pub async fn replicate_once(&self, last: &mut HashMap<String, String>) -> Result<()> {
		if !self.mesh.enabled() {
			return Ok(());
		}
		let owned_ids = self.engine.owned_ids()?;
		let owned = owned_ids.iter().cloned().collect::<HashSet<_>>();
		self.prune_replication_bookkeeping(last, &owned);
		for sid in owned_ids {
			self.replicate_sid(&sid, last).await;
		}
		Ok(())
	}

	/// One HA reconciliation cadence: renew leases, reconcile durable records,
	/// drain orphan queue, then fence local VMs superseded by higher-epoch
	/// owners.
	pub async fn ha_reconcile_once(&self) -> Result<()> {
		if !self.mesh.enabled() {
			return Ok(());
		}
		if let Err(err) = self.leases.renew_writable_volume_leases_once().await {
			error!("failed to renew writable-volume leases: {err}");
		}
		if let Err(err) = self.records.reconcile_records() {
			error!("failed to reconcile create records: {err}");
		}
		for (sid, dead) in self.mesh.drain_orphans() {
			self.reconcile_orphan(&sid, &dead).await;
		}
		let owned_ids = self.engine.owned_ids()?;
		for sid in self.mesh.fenced_local_ids(&owned_ids) {
			self.fence_local(&sid).await;
		}
		Ok(())
	}

	async fn replicate_sid(&self, sid: &str, last: &mut HashMap<String, String>) {
		let mut digest = String::new();
		let mut snapshot_dir = PathBuf::new();
		let result: Result<()> = async {
			let ha = self.ha_for_sid(sid);
			if ha == "off" {
				last.remove(sid);
				self.checkpoint_times.lock().remove(sid);
				return Ok(());
			}
			let now = unix_time();
			{
				let mut checkpoint_times = self.checkpoint_times.lock();
				if let Some(last_attempt) = checkpoint_times.get(sid)
					&& now - *last_attempt < self.config.replicate_sec.max(1.0)
				{
					return Ok(());
				}
				checkpoint_times.insert(sid.to_owned(), now);
			}
			let prep = match self.engine.replicate_prepare(sid).await {
				Ok(prep) => prep,
				Err(err) => {
					warn!("replicate skip {sid}: {err}");
					return Ok(());
				},
			};
			digest.clone_from(&prep.digest);
			snapshot_dir.clone_from(&prep.snapshot_dir);
			if Some(&prep.digest) == last.get(sid) {
				return Ok(());
			}
			let targets = self.mesh.replica_targets(sid, self.config.replicas);
			info!("replicate {sid} digest={} targets={targets:?}", digest_prefix(&prep.digest));
			if targets.is_empty() {
				return Ok(());
			}
			let any_success = self.push_replica_to_targets(targets, &prep).await;
			if any_success.any {
				self.mesh.note_event("replication");
				self
					.checkpoint_times
					.lock()
					.insert(sid.to_owned(), unix_time());
			}
			if any_success.all {
				last.insert(sid.to_owned(), prep.digest);
			}
			Ok(())
		}
		.await;
		if let Err(err) = result {
			error!("replication cycle failed for {sid}: {err}");
		}
		if !digest.is_empty()
			&& !snapshot_dir.as_os_str().is_empty()
			&& let Err(err) = self.engine.replicate_cleanup(&digest, &snapshot_dir)
		{
			warn!("replication cleanup failed for {sid}: {err}");
		}
	}

	async fn push_replica_to_targets(
		&self,
		targets: Vec<String>,
		prep: &ReplicatePreparation,
	) -> PushSummary {
		let concurrency = self.config.replicate_concurrency.max(1);
		let semaphore = std::sync::Arc::new(Semaphore::new(concurrency));
		let mut futures = FuturesUnordered::new();
		for peer_id in targets {
			futures.push(push_replica(
				self.client,
				self.mesh,
				semaphore.clone(),
				peer_id,
				prep.digest.clone(),
				prep.params.clone(),
				self.config.outbound_token.clone(),
			));
		}
		let mut any = false;
		let mut all = true;
		while let Some(ok) = futures.next().await {
			any |= ok;
			all &= ok;
		}
		PushSummary { any, all }
	}

	fn prune_replication_bookkeeping(
		&self,
		last: &mut HashMap<String, String>,
		owned: &HashSet<String>,
	) {
		let mut checkpoint_times = self.checkpoint_times.lock();
		let stale = last
			.keys()
			.chain(checkpoint_times.keys())
			.filter(|sid| !owned.contains(*sid))
			.cloned()
			.collect::<HashSet<_>>();
		for sid in stale {
			last.remove(&sid);
			checkpoint_times.remove(&sid);
		}
	}

	fn ha_for_sid(&self, sid: &str) -> String {
		if let Some(record) = self.records.get(sid) {
			return record.ha;
		}
		self
			.engine
			.get(sid)
			.ok()
			.and_then(|record| {
				record
					.detail
					.get("ha")
					.and_then(Value::as_str)
					.map(str::to_owned)
			})
			.unwrap_or_else(|| "async".to_owned())
	}

	async fn reconcile_orphan(&self, sid: &str, dead: &str) {
		let result = async {
			if let Some(owner) = self.mesh.owner_of(sid)
				&& owner != dead
				&& (owner == self.mesh.node_id() || self.mesh.is_peer_healthy(&owner))
			{
				return Ok(false);
			}
			if self.mesh.is_peer_healthy(dead) {
				warn!("deferring restore of {sid}: prior owner {dead} is still reachable");
				return Ok(true);
			}
			let exclude = HashSet::from([dead.to_owned()]);
			if self.mesh.restore_owner(sid, &exclude).as_deref() != Some(self.mesh.node_id()) {
				return Ok(true);
			}
			let create_record = self.records.get(sid);
			let ha = create_record
				.as_ref()
				.map_or("async", |record| record.ha.as_str());
			let can_rerun = create_record.as_ref().is_some_and(record_can_rerun);
			if ha == "off" {
				return Ok(false);
			}
			let restore_quorum = self
				.config
				.restore_quorum_enabled(self.mesh.expected_members());
			if restore_quorum && !self.restore_quorum_confirmed(dead).await {
				warn!(
					"deferring restore of {sid}: quorum not met ({}/{})",
					self.last_quorum_confirmations(dead).await,
					self.mesh.quorum_needed()
				);
				return Ok(true);
			}
			if self.engine.get(sid).is_ok() {
				return Ok(false);
			}
			let allow_checkpoint = matches!(ha, "async" | "async+rerun");
			let replica_ready =
				allow_checkpoint && self.replicas.holds(sid) && self.replicas.secrets_ready(sid);
			if replica_ready {
				return self
					.restore_from_replica(sid, dead, create_record.as_ref(), restore_quorum)
					.await;
			}
			if can_rerun && self.rerun_from_record(sid, dead)? {
				return Ok(false);
			}
			if !self.replicas.holds(sid) {
				warn!("cannot auto-restore {sid}: no local replica (will retry)");
			} else if !self.replicas.secrets_ready(sid) {
				warn!("cannot auto-restore {sid}: replica secrets unavailable after restart");
			}
			Ok(true)
		}
		.await;
		let retry = match result {
			Ok(should_retry) => should_retry,
			Err(err) => {
				error!("failed to restore orphaned sandbox {sid} from {dead}: {err}");
				true
			},
		};
		if retry {
			self.mesh.requeue_orphan(sid, dead);
		}
	}

	async fn restore_quorum_confirmed(&self, dead: &str) -> bool {
		let confirmations = self.last_quorum_confirmations(dead).await;
		self.mesh.restore_quorum_met(confirmations)
	}

	async fn last_quorum_confirmations(&self, dead: &str) -> usize {
		let mut confirmations = 1;
		for member_id in self.mesh.live_member_ids() {
			if member_id == self.mesh.node_id() {
				continue;
			}
			let Some(url) = self.mesh.peer_url(&member_id) else {
				continue;
			};
			let target = format!("{}/v1/mesh/reachable/{dead}", url.trim_end_matches('/'));
			let Ok(response) = self
				.client
				.get(target)
				.bearer_auth(&self.config.outbound_token)
				.header("X-Vmon-Mesh-Hop", "1")
				.timeout(self.mesh.create_timeout())
				.send()
				.await
			else {
				continue;
			};
			let Ok(payload) = response.json::<Value>().await else {
				continue;
			};
			if payload.get("reachable").and_then(Value::as_bool) == Some(false) {
				confirmations += 1;
			}
		}
		confirmations
	}

	async fn restore_from_replica(
		&self,
		sid: &str,
		dead: &str,
		create_record: Option<&CreateRecord>,
		restore_quorum: bool,
	) -> Result<bool> {
		let key = format!("restore:{sid}");
		if !self.mesh.worker_begin(&key) {
			return Ok(true);
		}
		let result = async {
			let rec = self
				.replicas
				.get(sid)
				.ok_or_else(|| EngineError::not_found("no local replica"))?;
			let epoch = self.mesh.next_epoch(sid);
			let lease_records = self
				.leases
				.acquire_writable_volume_leases(&rec.params, epoch)
				.await?;
			let restored =
				match self
					.engine
					.restore_from_template(&rec.params, &rec.snapshot_dir, restore_quorum)
				{
					Ok(restored) => restored,
					Err(err) => {
						let _ = self.leases.release_leases(&lease_records).await;
						return Err(err);
					},
				};
			if let Some(record) = create_record {
				self
					.engine
					.set_ha_metadata(&restored.name, &record.ha, &record.restart_policy)?;
			}
			self
				.engine
				.record_volume_leases(&restored.name, &lease_records)?;
			self.mesh.broadcast_owner(sid, self.mesh.node_id(), epoch)?;
			self
				.mesh
				.update_record_owner(sid, self.mesh.node_id(), epoch, false)?;
			self.replicas.drop_replica(sid)?;
			self.mesh.note_event("restore");
			info!("restored orphaned sandbox {sid} from replica after {dead} died");
			Ok(false)
		}
		.await;
		self.mesh.worker_end(&key);
		result
	}

	fn rerun_from_record(&self, sid: &str, dead: &str) -> Result<bool> {
		let Some(record) = self.records.get(sid) else {
			return Ok(false);
		};
		if !record_can_rerun(&record) {
			return Ok(false);
		}
		let key = format!("rerun:{sid}");
		if !self.mesh.worker_begin(&key) {
			return Ok(true);
		}
		let result = (|| {
			let epoch = self.mesh.next_epoch(sid);
			self.mesh.broadcast_owner(sid, self.mesh.node_id(), epoch)?;
			let mut params = params_object(record.params.clone())?;
			let kind = params
				.remove("_kind")
				.and_then(|value| value.as_str().map(str::to_owned))
				.unwrap_or_else(|| "sandbox".to_owned());
			params.insert("name".to_owned(), Value::String(sid.to_owned()));
			params.insert("ha".to_owned(), Value::String(record.ha.clone()));
			params.insert("restart_policy".to_owned(), Value::String(record.restart_policy.clone()));
			if kind == "run" || kind == "restore" {
				params.insert("detach".to_owned(), Value::Bool(true));
			}
			let params = Value::Object(params);
			let created_sid = match kind.as_str() {
				"run" => created_name_from_value(&self.engine.run_from_record(&params)?, sid),
				"restore" => created_name_from_value(&self.engine.restore_from_record(&params)?, sid),
				_ => self.engine.create_from_record(&params)?.name,
			};
			self
				.engine
				.set_ha_metadata(&created_sid, &record.ha, &record.restart_policy)?;
			self
				.engine
				.record_idempotency(&created_sid, &record.idempotency_key)?;
			self
				.mesh
				.update_record_owner(&created_sid, self.mesh.node_id(), epoch, false)?;
			self.mesh.note_event("restore");
			info!("re-ran orphaned sandbox {sid} from create record after {dead} died");
			Ok(true)
		})();
		self.mesh.worker_end(&key);
		result
	}

	async fn fence_local(&self, sid: &str) {
		let result: Result<()> = async {
			let record = self.engine.get(sid)?;
			self.engine.remove(sid)?;
			self.leases.release_record_volume_leases(&record).await?;
			self.mesh.forget_local_owner(sid);
			self.mesh.note_event("fence");
			warn!("fenced {sid}: superseded by a higher-epoch owner");
			Ok(())
		}
		.await;
		if let Err(err) = result {
			error!("failed to fence superseded sandbox {sid}: {err}");
		}
	}
}

#[derive(Clone, Copy, Debug)]
struct PushSummary {
	any: bool,
	all: bool,
}

pub async fn replicate_forever(ctx: &Reconciler<'_>) {
	let mut last = HashMap::new();
	if ctx.config.replicate_sec <= 0.0 {
		return;
	}
	loop {
		sleep(Duration::from_secs_f64(ctx.config.replicate_sec)).await;
		if let Err(err) = ctx.replicate_once(&mut last).await {
			error!("replication pass failed: {err}");
		}
	}
}

pub async fn ha_reconcile_forever(ctx: &Reconciler<'_>) {
	loop {
		sleep(Duration::from_secs_f64(ctx.mesh.interval().max(1.0))).await;
		if let Err(err) = ctx.ha_reconcile_once().await {
			error!("HA reconciliation pass failed: {err}");
		}
	}
}

async fn push_replica(
	client: &Client,
	mesh: &dyn MeshReconcileState,
	semaphore: std::sync::Arc<Semaphore>,
	peer_id: String,
	digest: String,
	params: Value,
	outbound_token: String,
) -> bool {
	let Some(url) = mesh.peer_url(&peer_id) else {
		return false;
	};
	let Ok(_permit) = semaphore.acquire_owned().await else {
		return false;
	};
	let target = format!("{}/v1/mesh/replica/receive", url.trim_end_matches('/'));
	let body = json!({
		"digest": digest,
		"source_url": mesh.advertise(),
		"source_node": mesh.node_id(),
		"params": params,
	});
	let result = client
		.post(target)
		.bearer_auth(outbound_token)
		.header("X-Vmon-Mesh-Hop", "1")
		.timeout(mesh.create_timeout())
		.json(&body)
		.send()
		.await
		.and_then(reqwest::Response::error_for_status);
	if let Err(err) = result {
		error!("failed to replicate sandbox to peer {peer_id}: {err}");
		false
	} else {
		true
	}
}

fn record_can_rerun(record: &CreateRecord) -> bool {
	record.restart_policy == "rerun" || record.ha.contains("rerun")
}

fn params_object(value: Value) -> Result<Map<String, Value>> {
	match value {
		Value::Object(map) => Ok(map),
		_ => Err(EngineError::invalid("record params must be an object")),
	}
}

fn created_name_from_value(value: &Value, fallback: &str) -> String {
	value
		.get("name")
		.and_then(Value::as_str)
		.filter(|name| !name.is_empty())
		.unwrap_or(fallback)
		.to_owned()
}

fn digest_prefix(digest: &str) -> &str {
	digest.get(..12).unwrap_or(digest)
}

fn unix_time() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}
