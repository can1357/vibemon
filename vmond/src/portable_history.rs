//! Transactional portable recovery-point publication for production clusters.
//!
//! Object bytes are content-addressed and deliberately invisible until the
//! corresponding immutable manifest has been verified and committed to
//! `PostgreSQL`, which is therefore the sole authority for which remote
//! recovery points are restorable; object listings are only a GC input.

use std::{
	collections::BTreeSet,
	fs::{self, File},
	io::{Read, Write},
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ::postgres::{Transaction, types::ToSql};
use chrono::{Duration as ChronoDuration, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
	EngineError, Result,
	config::{ClusterMode, ServeConfig},
	home::Home,
	mesh::{
		cluster_store::{
			ProductionStore, RollbackDisposition, RollbackMarker, SuspensionMarker,
			replica_object_lock_key,
		},
		record::CreateRecord,
		state::{self, MembershipState},
	},
	postgres::{self as pg, Client},
	s3::{ObjKind, S3Auth, S3Client, S3Credentials, S3Error, S3MountConfig},
	security::Keyring,
};

/// Fence a restore before the database lease becomes claimable by another
/// node. The same margin is used by every portable restore heartbeat.
const RESTORE_LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(5);
const COPY_CHUNK_BYTES: usize = 1024 * 1024;
/// Leave enough time for a stalled publisher to acquire its object locks and
/// commit after a retry before GC considers its completed objects orphaned.
const GC_MIN_AGE_SECS: u64 = 60 * 60;

/// A caller-owned archive ready to become a portable recovery point.
#[derive(Clone, Debug)]
pub struct PortablePointInput {
	pub sid:                    String,
	pub name:                   String,
	pub kind:                   String,
	pub created_at_unix_millis: u64,
	pub owner_node:             String,
	pub owner_epoch:            i64,
	pub incarnation_epoch:      i64,
	pub lifecycle_generation:   u64,
	pub archive:                PathBuf,
}

/// Exact ownership fence to atomically expose a recovery point as a pending
/// production suspension. All fields are checked against the point itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableSuspendIntent {
	pub sid:                  String,
	pub owner:                String,
	pub epoch:                i64,
	pub point:                String,
	pub lifecycle_generation: u64,
}

/// Retention is independently enforced for `disk` and `checkpoint` tiers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionPolicy {
	pub per_kind:            usize,
	pub max_age_unix_millis: Option<u64>,
}

impl RetentionPolicy {
	pub fn from_config(config: &ServeConfig) -> Self {
		let max_age_unix_millis =
			(config.history_max_age_sec > 0.0).then_some((config.history_max_age_sec * 1000.0) as u64);
		Self { per_kind: config.history_retention.max(1), max_age_unix_millis }
	}
}

/// Immutable, non-secret description of the two objects needed to restore a
/// point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PortableManifest {
	pub version:                u8,
	pub sid:                    String,
	pub name:                   String,
	pub kind:                   String,
	pub created_at_unix_millis: u64,
	pub owner_node:             String,
	pub owner_epoch:            i64,
	#[serde(default)]
	pub incarnation_epoch:      i64,
	pub lifecycle_generation:   u64,
	pub archive_sha256:         String,
	pub archive_size_bytes:     u64,
}

/// A locally prepared point.  Preparation has no remote side effects.
#[derive(Clone, Debug)]
pub struct PreparedPortablePoint {
	manifest:       PortableManifest,
	archive:        PathBuf,
	archive_key:    String,
	manifest_key:   String,
	manifest_bytes: Vec<u8>,
}

/// A fully uploaded and verified point which remains unlisted until
/// [`PortableHistory::commit`].
pub struct PublishedPortablePoint {
	prepared: PreparedPortablePoint,
	guard:    Mutex<Option<Client>>,
}

/// A committed portable recovery point, addressable from every production node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableRecoveryPoint {
	pub sid:                    String,
	pub name:                   String,
	pub kind:                   String,
	pub created_at_unix_millis: u64,
	pub owner_node:             String,
	pub owner_epoch:            i64,
	pub incarnation_epoch:      i64,
	pub lifecycle_generation:   u64,
	pub size_bytes:             u64,
	pub archive_key:            String,
	pub archive_sha256:         String,
	pub manifest_key:           String,
	pub manifest_sha256:        String,
}

/// Durable rollback retention pin, retained until lifecycle reconciliation
/// confirms the matching operation generation has converged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableRollbackPin {
	pub sid:                  String,
	pub point_name:           String,
	pub operation_generation: u64,
	pub owner:                String,
	pub epoch:                i64,
}

#[derive(Clone, Debug)]
pub struct PortableRestoreLease {
	pub sid:                  String,
	pub owner_node:           String,
	pub epoch:                i64,
	previous_owner:           String,
	previous_epoch:           i64,
	pub lifecycle_generation: u64,
}

/// Independently renews a pre-launch portable restore lease.
///
/// Dropping the guard stops only this heartbeat; it never purports to cancel a
/// caller's launch task. Callers must await launch, then call [`Self::finish`]
/// and tear down the completed launch output when it returns `false`.
pub struct PortableRestoreHeartbeat {
	ownership: Arc<PortableOwnership>,
	lease:     PortableRestoreLease,
	lost:      Arc<AtomicBool>,
	stopped:   Arc<AtomicBool>,
	stop:      Option<flume::Sender<()>>,
	joins:     Vec<thread::JoinHandle<()>>,
}
/// Fenced ownership identity returned before a fresh node materializes a
/// portable recovery point.
#[derive(Clone, Debug)]
pub struct ClaimedPortableIdentity {
	pub node_id: String,
	pub epoch:   i64,
	pub record:  CreateRecord,
}

/// Exact non-serving marker claim. The lease retains the former marker
/// generation so an abort can restore it without weakening fencing.
#[derive(Clone, Debug)]
pub(crate) struct ClaimedSuspendMarker {
	pub record: CreateRecord,
	pub lease:  PortableRestoreLease,
}

/// Narrow bridge from lifecycle restore to production mesh ownership.
#[derive(Clone)]
pub struct PortableOwnership {
	store:   Arc<ProductionStore>,
	home:    Home,
	node_id: Option<String>,
}

impl PortableOwnership {
	/// Connect to the production ownership authority and recover this node's
	/// durable mesh identity. Single-node callers receive no bridge.
	pub fn connect(config: &ServeConfig) -> Result<Option<Self>> {
		if config.cluster_mode != ClusterMode::Production {
			return Ok(None);
		}
		let home = Home::new(config.home.clone());
		let node_id = MembershipState::load(
			&state::mesh_state_path(&home),
			state::probe_caps(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.map_err(|error| {
					EngineError::engine(format!("system clock precedes Unix epoch: {error}"))
				})?
				.as_secs_f64(),
		)
		.map_err(|error| EngineError::invalid(error.to_string()))?
		.map(|membership| membership.node_id);
		let url = required_config(config.postgres_url.as_deref(), "postgres_url")?;
		Ok(Some(Self { store: Arc::new(ProductionStore::connect(url)?), home, node_id }))
	}

	pub fn local_node_id(&self) -> Option<&str> {
		self.node_id.as_deref()
	}

	/// Atomically fence all previous owners and return the authoritative
	/// non-secret identity at the newly assigned generation.  A new daemon may
	/// connect before mesh setup; node identity is therefore bound lazily here.
	pub fn claim_next(&self, sid: &str) -> Result<ClaimedPortableIdentity> {
		let node_id = self
			.node_id
			.clone()
			.or_else(|| {
				MembershipState::load(
					&state::mesh_state_path(&self.home),
					state::probe_caps(),
					SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.ok()?
						.as_secs_f64(),
				)
				.ok()
				.flatten()
				.map(|membership| membership.node_id)
			})
			.ok_or_else(|| {
				EngineError::invalid("portable restore requires enabled mesh membership")
			})?;
		let record = self.store.claim_next(sid, &node_id)?;
		Ok(ClaimedPortableIdentity { node_id, epoch: record.epoch, record })
	}

	/// Claim only an unchanged, expired non-serving suspension marker. This
	/// serializes marker enumeration with ownership fencing.
	pub(crate) fn claim_suspend_marker(
		&self,
		marker: &SuspensionMarker,
	) -> Result<ClaimedSuspendMarker> {
		let node_id = self
			.node_id
			.clone()
			.or_else(|| {
				MembershipState::load(
					&state::mesh_state_path(&self.home),
					state::probe_caps(),
					SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.ok()?
						.as_secs_f64(),
				)
				.ok()
				.flatten()
				.map(|membership| membership.node_id)
			})
			.ok_or_else(|| {
				EngineError::invalid("portable restore requires enabled mesh membership")
			})?;
		let record = self.store.claim_suspend_marker(
			&marker.sid,
			&marker.owner,
			marker.epoch,
			&marker.state,
			&marker.point,
			marker.generation,
			&node_id,
		)?;
		let lease = PortableRestoreLease {
			sid:                  marker.sid.clone(),
			owner_node:           node_id,
			epoch:                record.epoch,
			previous_owner:       marker.owner.clone(),
			previous_epoch:       marker.epoch,
			lifecycle_generation: marker.generation,
		};
		Ok(ClaimedSuspendMarker { record, lease })
	}

	/// Re-adopt an exact locally owned `resuming` marker after a process crash
	/// without claiming a new epoch. This returns the fenced lease needed to
	/// continue the existing materialization and heartbeat.
	pub(crate) fn lease_for_resuming_marker(
		&self,
		marker: &SuspensionMarker,
	) -> Result<ClaimedSuspendMarker> {
		let node_id = self
			.node_id
			.clone()
			.or_else(|| {
				MembershipState::load(
					&state::mesh_state_path(&self.home),
					state::probe_caps(),
					SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.ok()?
						.as_secs_f64(),
				)
				.ok()
				.flatten()
				.map(|membership| membership.node_id)
			})
			.ok_or_else(|| {
				EngineError::invalid("portable restore requires enabled mesh membership")
			})?;
		if marker.state != "resuming" || marker.owner != node_id {
			return Err(EngineError::busy(format!(
				"resuming marker for {} is not locally owned",
				marker.sid
			)));
		}
		let record = self.store.adopt_live_lifecycle(
			&marker.sid,
			&node_id,
			marker.epoch,
			&marker.state,
			&marker.point,
			marker.generation,
		)?;
		Ok(ClaimedSuspendMarker {
			record,
			lease: PortableRestoreLease {
				sid:                  marker.sid.clone(),
				owner_node:           node_id,
				epoch:                marker.epoch,
				previous_owner:       marker.owner.clone(),
				previous_epoch:       marker.epoch,
				lifecycle_generation: marker.generation,
			},
		})
	}

	/// Return `PostgreSQL`'s current authoritative ownership generation.
	pub fn current(&self, sid: &str) -> Result<(String, i64)> {
		let record = self
			.store
			.resolve(sid)?
			.ok_or_else(|| EngineError::not_found(format!("sandbox {sid} has no ownership record")))?;
		Ok((record.owner, record.epoch))
	}

	/// Resolve the immutable sandbox incarnation only while this caller still
	/// owns the exact live generation it observed.
	pub fn current_incarnation(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
	) -> Result<i64> {
		self
			.store
			.current_incarnation(sid, expected_owner, expected_epoch)
	}

	fn marker_lineage_is_current(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		incarnation_epoch: i64,
	) -> Result<bool> {
		Ok(self.store.resolve(sid)?.is_some_and(|record| {
			record.owner == owner
				&& record.epoch == epoch
				&& record.incarnation_epoch == incarnation_epoch
		}))
	}

	/// Return a suspended marker only when it belongs to the current
	/// non-deleting ownership lineage.
	pub(crate) fn suspend_marker(&self, sid: &str) -> Result<Option<SuspensionMarker>> {
		let Some(marker) = self.store.suspend_marker(sid)? else {
			return Ok(None);
		};
		Ok(self
			.marker_lineage_is_current(
				&marker.sid,
				&marker.owner,
				marker.epoch,
				marker.incarnation_epoch,
			)?
			.then_some(marker))
	}

	/// List only non-serving markers from the current ownership lineage.
	pub(crate) fn suspend_markers(&self) -> Result<Vec<SuspensionMarker>> {
		let mut current = Vec::new();
		for marker in self.store.list_suspend_markers()? {
			if self.marker_lineage_is_current(
				&marker.sid,
				&marker.owner,
				marker.epoch,
				marker.incarnation_epoch,
			)? {
				current.push(marker);
			}
		}
		Ok(current)
	}

	/// Return a rollback marker only when it belongs to the current
	/// non-deleting ownership lineage.
	pub(crate) fn rollback_marker(&self, sid: &str) -> Result<Option<RollbackMarker>> {
		let Some(marker) = self.store.rollback_marker(sid)? else {
			return Ok(None);
		};
		Ok(self
			.marker_lineage_is_current(
				&marker.sid,
				&marker.owner,
				marker.epoch,
				marker.incarnation_epoch,
			)?
			.then_some(marker))
	}

	/// List only rollback markers from the current ownership lineage.
	pub(crate) fn rollback_markers(&self) -> Result<Vec<RollbackMarker>> {
		let mut current = Vec::new();
		for marker in self.store.list_rollback_markers()? {
			if self.marker_lineage_is_current(
				&marker.sid,
				&marker.owner,
				marker.epoch,
				marker.incarnation_epoch,
			)? {
				current.push(marker);
			}
		}
		Ok(current)
	}

	/// Query exact rollback replay state for the journal's fenced source
	/// generation. Unknown or non-local state is deliberately `Other`.
	pub(crate) fn rollback_disposition(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
	) -> Result<RollbackDisposition> {
		self.store.rollback_disposition(sid, owner, epoch)
	}

	/// Allocate the next durable checkpoint sequence only while this exact
	/// production owner generation remains authoritative. Checkpoint captures
	/// must use this value instead of any sequence restored from local state.
	pub fn allocate_checkpoint_generation(&self, sid: &str, owner: &str, epoch: i64) -> Result<u64> {
		self.store.allocate_checkpoint_generation(sid, owner, epoch)
	}

	/// Acquire the next generation only if the caller's observed ownership is
	/// still authoritative.  A competing restorer with the same observation is
	/// fenced before it can launch.
	pub fn claim_expected(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		lifecycle_generation: u64,
	) -> Result<PortableRestoreLease> {
		let node_id = self
			.node_id
			.clone()
			.or_else(|| {
				MembershipState::load(
					&state::mesh_state_path(&self.home),
					state::probe_caps(),
					SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.ok()?
						.as_secs_f64(),
				)
				.ok()
				.flatten()
				.map(|membership| membership.node_id)
			})
			.ok_or_else(|| {
				EngineError::invalid("portable restore requires enabled mesh membership")
			})?;
		let record = self
			.store
			.claim_expected(sid, expected_owner, expected_epoch, &node_id)?;
		Ok(PortableRestoreLease {
			sid: sid.to_owned(),
			owner_node: node_id,
			epoch: record.epoch,
			previous_owner: expected_owner.to_owned(),
			previous_epoch: expected_epoch,
			lifecycle_generation,
		})
	}

	/// Claim a fenced expired owner generation for rollback while atomically
	/// recording the exact target and checkpoint generations before restore.
	pub fn claim_expected_rollback(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		target: &str,
		operation_generation: u64,
		checkpoint_generation: u64,
	) -> Result<PortableRestoreLease> {
		let node_id = self
			.node_id
			.clone()
			.or_else(|| {
				MembershipState::load(
					&state::mesh_state_path(&self.home),
					state::probe_caps(),
					SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.ok()?
						.as_secs_f64(),
				)
				.ok()
				.flatten()
				.map(|membership| membership.node_id)
			})
			.ok_or_else(|| {
				EngineError::invalid("portable restore requires enabled mesh membership")
			})?;
		let record = self.store.claim_expected_rollback(
			sid,
			expected_owner,
			expected_epoch,
			&node_id,
			target,
			operation_generation,
			checkpoint_generation,
		)?;
		Ok(PortableRestoreLease {
			sid:                  sid.to_owned(),
			owner_node:           node_id,
			epoch:                record.epoch,
			previous_owner:       expected_owner.to_owned(),
			previous_epoch:       expected_epoch,
			lifecycle_generation: operation_generation,
		})
	}

	/// Re-adopt this node's exact live rollback marker after restart without
	/// advancing ownership. The durable marker remains non-serving until finish.
	pub fn adopt_live_rollback(
		&self,
		sid: &str,
		epoch: i64,
		lifecycle_generation: u64,
	) -> Result<PortableRestoreLease> {
		let node_id = self
			.node_id
			.clone()
			.or_else(|| {
				MembershipState::load(
					&state::mesh_state_path(&self.home),
					state::probe_caps(),
					SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.ok()?
						.as_secs_f64(),
				)
				.ok()
				.flatten()
				.map(|membership| membership.node_id)
			})
			.ok_or_else(|| {
				EngineError::invalid("portable restore requires enabled mesh membership")
			})?;
		let record = self.store.adopt_live_rollback(sid, &node_id, epoch)?;
		Ok(PortableRestoreLease {
			sid: sid.to_owned(),
			owner_node: node_id.clone(),
			epoch: record.epoch,
			previous_owner: node_id,
			previous_epoch: epoch,
			lifecycle_generation,
		})
	}

	/// Re-adopt this node's exact live suspend/resume lifecycle marker after
	/// restart without advancing ownership.
	pub fn adopt_live_lifecycle(
		&self,
		sid: &str,
		epoch: i64,
		state: &str,
		point: &str,
		lifecycle_generation: u64,
	) -> Result<PortableRestoreLease> {
		let node_id = self
			.node_id
			.clone()
			.or_else(|| {
				MembershipState::load(
					&state::mesh_state_path(&self.home),
					state::probe_caps(),
					SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.ok()?
						.as_secs_f64(),
				)
				.ok()
				.flatten()
				.map(|membership| membership.node_id)
			})
			.ok_or_else(|| {
				EngineError::invalid("portable restore requires enabled mesh membership")
			})?;
		let record = self.store.adopt_live_lifecycle(
			sid,
			&node_id,
			epoch,
			state,
			point,
			lifecycle_generation,
		)?;
		Ok(PortableRestoreLease {
			sid: sid.to_owned(),
			owner_node: node_id.clone(),
			epoch: record.epoch,
			previous_owner: node_id,
			previous_epoch: epoch,
			lifecycle_generation,
		})
	}

	/// Renew the exact pre-launch restore lease. `None` means the lease is
	/// expired or no longer belongs to this owner generation.
	pub fn renew_restore(&self, lease: &PortableRestoreLease) -> Result<Option<Duration>> {
		self
			.store
			.renew_owner_lease(&lease.sid, &lease.owner_node, lease.epoch)?
			.map(ttl_duration)
			.transpose()
	}

	/// Begin an independent five-second heartbeat for a slow restore. The
	/// caller must always await its launch work, then use
	/// [`PortableRestoreHeartbeat::finish`] to obtain authoritative completion
	/// status.
	pub fn start_restore_heartbeat(
		&self,
		lease: &PortableRestoreLease,
	) -> Result<PortableRestoreHeartbeat> {
		let started = Instant::now();
		let ttl = self
			.renew_restore(lease)?
			.ok_or_else(|| restore_lease_lost(&lease.sid))?;
		if ttl <= RESTORE_LEASE_SAFETY_MARGIN {
			return Err(restore_lease_lost(&lease.sid));
		}
		let ownership = Arc::new(self.clone());
		let lease = lease.clone();
		let lost = Arc::new(AtomicBool::new(false));
		let stopped = Arc::new(AtomicBool::new(false));
		let deadline = Arc::new(Mutex::new(restore_deadline(started, ttl)));
		let (stop, receiver) = flume::bounded(1);
		let task_owner = Arc::clone(&ownership);
		let task_lease = lease.clone();
		let task_lost = Arc::clone(&lost);
		let task_stopped = Arc::clone(&stopped);
		let task_deadline = Arc::clone(&deadline);
		let renew = thread::spawn(move || {
			loop {
				match receiver.recv_timeout(Duration::from_secs(5)) {
					Ok(()) | Err(flume::RecvTimeoutError::Disconnected) => break,
					Err(flume::RecvTimeoutError::Timeout) => {},
				}
				if task_stopped.load(Ordering::Acquire) {
					break;
				}
				let renewed_at = Instant::now();
				match task_owner.renew_restore(&task_lease) {
					Ok(Some(ttl)) => *task_deadline.lock() = restore_deadline(renewed_at, ttl),
					Ok(None) => {
						task_lost.store(true, Ordering::Release);
						break;
					},
					Err(_) if Instant::now() >= *task_deadline.lock() => {
						task_lost.store(true, Ordering::Release);
						break;
					},
					Err(_) => {},
				}
			}
		});
		let watchdog_lost = Arc::clone(&lost);
		let watchdog_stopped = Arc::clone(&stopped);
		let watchdog_deadline = Arc::clone(&deadline);
		let watchdog = thread::spawn(move || {
			watchdog_until_stopped(&watchdog_deadline, &watchdog_stopped, &watchdog_lost);
		});
		Ok(PortableRestoreHeartbeat {
			ownership,
			lease,
			lost,
			stopped,
			stop: Some(stop),
			joins: vec![renew, watchdog],
		})
	}

	/// Confirm the lease still fences every other owner before reporting a
	/// restored VM as usable. An expired lease cannot be revived at finalize.
	pub fn finalize(&self, lease: &PortableRestoreLease) -> Result<()> {
		if self.renew_restore(lease)?.is_none()
			|| !self.verify(&lease.sid, &lease.owner_node, lease.epoch)?
		{
			return Err(restore_lease_lost(&lease.sid));
		}
		Ok(())
	}

	/// Release only this unlaunched claim.  The prior owner is restored at a
	/// fresh generation, so delayed operations from either prior epoch remain
	/// fenced. `None` means another owner already superseded this lease.
	pub fn abort(&self, lease: &PortableRestoreLease) -> Result<Option<ClaimedPortableIdentity>> {
		self
			.store
			.release_claim(
				&lease.sid,
				&lease.owner_node,
				lease.epoch,
				&lease.previous_owner,
				lease.previous_epoch,
			)
			.map(|record| {
				record.map(|record| ClaimedPortableIdentity {
					node_id: record.owner.clone(),
					epoch: record.epoch,
					record,
				})
			})
	}

	/// Verify the caller still owns the exact generation immediately before
	/// and after local launch; a fenced restore must tear itself down.
	pub fn verify(&self, sid: &str, node_id: &str, epoch: i64) -> Result<bool> {
		self.store.owns_epoch(sid, node_id, epoch)
	}
}

impl PortableRestoreHeartbeat {
	/// A shareable cancellation signal for launch readiness/progress loops.
	/// Once set, the candidate must abort promptly and await its own teardown.
	pub fn lost_signal(&self) -> Arc<AtomicBool> {
		Arc::clone(&self.lost)
	}

	/// Return whether the watchdog or heartbeat has already fenced this lease.
	pub fn lost(&self) -> bool {
		self.lost.load(Ordering::Acquire)
	}

	/// Stop and join the heartbeat, then renew and verify the exact unexpired
	/// lease. `false` means the caller must destroy its already-completed launch
	/// output before exposing any restored state.
	pub async fn finish(mut self) -> Result<bool> {
		self.stopped.store(true, Ordering::Release);
		if let Some(stop) = self.stop.take() {
			let _ = stop.send(());
		}
		let joins = std::mem::take(&mut self.joins);
		let joined = tokio::task::spawn_blocking(move || {
			for join in joins {
				join
					.join()
					.map_err(|_| EngineError::engine("portable restore heartbeat panicked"))?;
			}
			Ok::<(), EngineError>(())
		});
		if let Ok(result) = tokio::time::timeout(Duration::from_secs(1), joined).await {
			result.map_err(|error| {
				EngineError::engine(format!("joining portable restore heartbeat: {error}"))
			})??;
		} else {
			self.lost.store(true, Ordering::Release);
			return Ok(false);
		}
		if self.lost() {
			return Ok(false);
		}
		match self.ownership.renew_restore(&self.lease)? {
			Some(_) => {
				self
					.ownership
					.verify(&self.lease.sid, &self.lease.owner_node, self.lease.epoch)
			},
			None => Ok(false),
		}
	}
}

impl Drop for PortableRestoreHeartbeat {
	fn drop(&mut self) {
		self.stopped.store(true, Ordering::Release);
		if let Some(stop) = self.stop.take() {
			let _ = stop.send(());
		}
	}
}

/// `PostgreSQL` metadata and S3 objects for production portable recovery
/// history.
pub struct PortableHistory {
	postgres_url:        String,
	client:              Mutex<Option<Client>>,
	s3:                  Arc<S3Client>,
	retention:           RetentionPolicy,
	namespace:           String,
	multipart_stale_sec: f64,
}

/// Owns a dedicated `PostgreSQL` session while an S3 operation holds its
/// distributed object lock. Dropping the session releases `pg_advisory_lock`.
struct PortableObjectSessionGuard {
	client: Option<Client>,
}

impl PortableHistory {
	/// Connect only in production mode.  Production setup validates the shared
	/// substrates immediately, so a suspend cannot silently fall back to local
	/// history.
	pub fn connect(config: &ServeConfig, keyring: &Keyring) -> Result<Option<Arc<Self>>> {
		if config.cluster_mode != ClusterMode::Production {
			return Ok(None);
		}
		let postgres_url = required_config(config.postgres_url.as_deref(), "postgres_url")?;
		let bucket = required_config(config.s3_bucket.as_deref(), "s3_bucket")?;
		let region = required_config(config.s3_region.as_deref(), "s3_region")?;
		let access_key = required_config(config.s3_access_key.as_deref(), "s3_access_key")?;
		let secret_key = required_config(config.s3_secret_key.as_deref(), "s3_secret_key")?;
		let shared_key_id =
			required_config(config.portable_history_key_id.as_deref(), "portable_history_key_id")?;
		if shared_key_id == "default" {
			return Err(EngineError::invalid(
				"production portable history requires a non-default shared key ID",
			));
		}
		let store = ProductionStore::connect(postgres_url)?;
		for key_id in std::iter::once(shared_key_id)
			.chain(config.tenant_keys.values().map(String::as_str))
			.filter(|key_id| *key_id != "default")
		{
			let key = keyring.load(key_id)?;
			store.verify_or_register_key(key_id, &portable_key_verifier(key.as_slice()))?;
		}
		let namespace = store.object_namespace().to_owned();
		let s3 = S3Client::new(S3MountConfig {
			bucket:    bucket.to_owned(),
			prefix:    config.s3_prefix.clone().unwrap_or_default(),
			region:    region.to_owned(),
			endpoint:  config.s3_endpoint.clone(),
			read_only: false,
			creds:     Some(S3Credentials {
				access_key:    access_key.to_owned(),
				secret_key:    secret_key.to_owned(),
				session_token: None,
			}),
			auth:      S3Auth::Inline,
		})
		.map_err(s3_error)?;
		let mut client = pg::connect(postgres_url, "portable-history PostgreSQL store")?;
		pg::blocking(|| migrate(&mut client, &namespace))?;
		Ok(Some(Arc::new(Self {
			postgres_url: postgres_url.to_owned(),
			client: Mutex::new(Some(client)),
			s3: Arc::new(s3),
			namespace,
			multipart_stale_sec: config.s3_multipart_stale_sec,
			retention: RetentionPolicy::from_config(config),
		})))
	}

	/// Hash a regular archive and construct its immutable manifest.  The
	/// manifest contains no sandbox parameters, environment, or secrets.
	pub fn prepare(&self, input: PortablePointInput) -> Result<PreparedPortablePoint> {
		validate_input(&input)?;
		let (archive_sha256, archive_size_bytes) = hash_file(&input.archive)?;
		let manifest = PortableManifest {
			version: 2,
			sid: input.sid,
			name: input.name,
			kind: input.kind,
			created_at_unix_millis: input.created_at_unix_millis,
			owner_node: input.owner_node,
			owner_epoch: input.owner_epoch,
			incarnation_epoch: input.incarnation_epoch,
			lifecycle_generation: input.lifecycle_generation,
			archive_sha256: archive_sha256.clone(),
			archive_size_bytes,
		};
		let manifest_bytes = serde_json::to_vec(&manifest)?;
		let manifest_sha256 = digest_bytes(&manifest_bytes);
		Ok(PreparedPortablePoint {
			archive: input.archive,
			archive_key: self.blob_key(&archive_sha256),
			manifest_key: self.manifest_key(&manifest_sha256),
			manifest,
			manifest_bytes,
		})
	}

	/// Upload and re-read both content-addressed objects.  This does not create
	/// a recovery-point row and is safe to repeat after a process crash.
	pub fn publish(&self, prepared: &PreparedPortablePoint) -> Result<PublishedPortablePoint> {
		let published = PublishedPortablePoint {
			prepared: prepared.clone(),
			guard:    Mutex::new(Some(self.acquire_publication_guard(prepared)?)),
		};
		self.ensure_file(
			&prepared.archive_key,
			&prepared.archive,
			&prepared.manifest.archive_sha256,
		)?;
		let manifest_sha256 = digest_bytes(&prepared.manifest_bytes);
		self.ensure_bytes(&prepared.manifest_key, &prepared.manifest_bytes, &manifest_sha256)?;
		Ok(published)
	}

	fn acquire_publication_guard(&self, prepared: &PreparedPortablePoint) -> Result<Client> {
		let mut client = pg::connect(&self.postgres_url, "portable-history publication guard")?;
		for object in BTreeSet::from([prepared.archive_key.clone(), prepared.manifest_key.clone()]) {
			session_advisory_lock(&mut client, &object_lock_key(&object))?;
		}
		Ok(client)
	}

	/// Atomically expose an already verified point. Rows are inserted only
	/// after both objects exist and hash-match; repeat commits are idempotent.
	pub fn commit(
		&self,
		published: &PublishedPortablePoint,
		retention: RetentionPolicy,
	) -> Result<PortableRecoveryPoint> {
		self.commit_with_suspend_intent(published, retention, None)
	}

	/// Atomically expose a verified point and its exact `suspending` ownership
	/// marker under one `PostgreSQL` transaction. On any ownership, marker, or
	/// immutable-point mismatch, neither a new point nor marker is visible.
	pub fn commit_suspend(
		&self,
		published: &PublishedPortablePoint,
		retention: RetentionPolicy,
		intent: &PortableSuspendIntent,
	) -> Result<PortableRecoveryPoint> {
		self.commit_with_suspend_intent(published, retention, Some(intent))
	}

	/// Retention deletes metadata transactionally, separately by tier; object
	/// cleanup happens only after the committing transaction has completed.
	fn commit_with_suspend_intent(
		&self,
		published: &PublishedPortablePoint,
		retention: RetentionPolicy,
		intent: Option<&PortableSuspendIntent>,
	) -> Result<PortableRecoveryPoint> {
		let point = point_from_prepared(&published.prepared);
		if let Some(intent) = intent {
			validate_suspend_intent(&point, intent)?;
		}
		let mut guard = published.guard.lock();
		if guard.is_none() {
			*guard = Some(self.acquire_publication_guard(&published.prepared)?);
		}
		let client = guard
			.as_mut()
			.ok_or_else(|| EngineError::engine("portable publication guard was released"))?;
		let commit_result = pg::blocking(|| {
			(|| -> Result<Vec<String>> {
				let mut transaction = client.transaction().map_err(database_error)?;
				advisory_lock(&mut transaction, &format!("portable-history:{}", point.sid))?;
				// Ownership mutations take this same transaction-scoped lock before
				// changing owner/epoch, so validation and publication are one fence.
				advisory_lock(&mut transaction, &format!("sandbox:{}", point.sid))?;
				let ownership = transaction
					.query_opt(
						"SELECT incarnation_epoch FROM sandbox_ownership WHERE sid = $1 AND owner = $2 \
						 AND epoch = $3 AND deleting = FALSE AND owner_lease_owner = $2 AND \
						 owner_lease_epoch = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM \
						 clock_timestamp()) FOR UPDATE",
						&[&point.sid as &(dyn ToSql + Sync), &point.owner_node, &point.owner_epoch],
					)
					.map_err(database_error)?;
				let Some(ownership) = ownership else {
					return Err(EngineError::busy(format!(
						"portable publication for {} was fenced by ownership change",
						point.sid
					)));
				};
				let incarnation_epoch = ownership.try_get::<_, i64>(0).map_err(database_error)?;
				if incarnation_epoch != point.incarnation_epoch {
					return Err(EngineError::busy(format!(
						"portable publication for {} was fenced by incarnation change",
						point.sid
					)));
				}
				let existing = transaction
					.query_opt(
						"SELECT sid, name, kind, created_at_unix_millis, archive_size_bytes, \
						 archive_key, archive_sha256, manifest_key, manifest_sha256, owner_node, \
						 owner_epoch, incarnation_epoch, lifecycle_generation FROM \
						 portable_recovery_points WHERE sid = $1 AND name = $2",
						&[&point.sid, &point.name],
					)
					.map_err(database_error)?;
				if let Some(row) = existing {
					if point_from_row(&row)? != point {
						return Err(EngineError::invalid(format!(
							"portable recovery point {} for sandbox {} has conflicting immutable contents",
							point.name, point.sid
						)));
					}
				} else {
					transaction
						.execute(
							"INSERT INTO portable_recovery_points (sid, name, kind, \
							 created_at_unix_millis, archive_size_bytes, archive_key, archive_sha256, \
							 manifest_key, manifest_sha256, owner_node, owner_epoch, incarnation_epoch, \
							 lifecycle_generation, committed_at_unix_millis) VALUES ($1, $2, $3, $4, $5, \
							 $6, $7, $8, $9, $10, $11, $12, $13, FLOOR(EXTRACT(EPOCH FROM \
							 clock_timestamp()) * 1000)::BIGINT)",
							&[
								&point.sid as &(dyn ToSql + Sync),
								&point.name,
								&point.kind,
								&i64::try_from(point.created_at_unix_millis).map_err(|_| {
									EngineError::invalid("recovery timestamp exceeds PostgreSQL range")
								})?,
								&i64::try_from(point.size_bytes).map_err(|_| {
									EngineError::invalid("recovery archive exceeds PostgreSQL range")
								})?,
								&point.archive_key,
								&point.archive_sha256,
								&point.manifest_key,
								&point.manifest_sha256,
								&point.owner_node,
								&point.owner_epoch,
								&point.incarnation_epoch,
								&i64::try_from(point.lifecycle_generation).map_err(|_| {
									EngineError::invalid("lifecycle generation exceeds PostgreSQL range")
								})?,
							],
						)
						.map_err(database_error)?;
				}
				if let Some(intent) = intent {
					write_suspending_marker(&mut transaction, intent)?;
				}
				let orphaned = Vec::new();
				transaction.commit().map_err(database_error)?;
				Ok(orphaned)
			})()
		});
		let client = guard.take();
		drop(guard);
		drop(client);
		let _commit_stale_objects = commit_result?;
		match self.prune_retained_rows_with(retention) {
			Ok(objects) => {
				for object in objects {
					if let Err(error) = self.remove_if_unreferenced(&object) {
						tracing::warn!(%error, object, "portable recovery object cleanup deferred");
					}
				}
			},
			Err(error) => tracing::warn!(%error, "portable recovery retention deferred"),
		}
		Ok(point)
	}

	/// Persistently protect a committed rollback target from tier retention.
	pub fn pin_rollback_target(
		&self,
		sid: &str,
		name: &str,
		operation_generation: u64,
		owner: &str,
		epoch: i64,
	) -> Result<()> {
		if owner.is_empty() || epoch < 0 {
			return Err(EngineError::invalid("rollback pin requires an owner and non-negative epoch"));
		}
		let generation = i64::try_from(operation_generation).map_err(|_| {
			EngineError::invalid("rollback operation generation exceeds PostgreSQL range")
		})?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("portable-history:{sid}"))?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let exists = transaction
				.query_opt(
					"SELECT 1 FROM portable_recovery_points p JOIN sandbox_ownership o ON o.sid = \
					 p.sid AND o.incarnation_epoch = p.incarnation_epoch AND o.deleting = FALSE WHERE \
					 p.sid = $1 AND p.name = $2 AND o.owner = $3 AND o.epoch = $4 AND \
					 o.owner_lease_owner = $3 AND o.owner_lease_epoch = $4 AND \
					 o.owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) FOR UPDATE",
					&[&sid, &name, &owner, &epoch],
				)
				.map_err(database_error)?;
			if exists.is_none() {
				return Err(EngineError::not_found(format!(
					"portable recovery point {name} not found"
				)));
			}
			transaction
				.execute(
					"INSERT INTO portable_rollback_pins (sid, point_name, operation_generation, owner, \
					 epoch) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
					&[&sid as &(dyn ToSql + Sync), &name, &generation, &owner, &epoch],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)
		})
	}

	/// Release a durable rollback-target pin after the rollback journal
	/// converges.
	pub fn release_rollback_target(
		&self,
		sid: &str,
		name: &str,
		operation_generation: u64,
		owner: &str,
		epoch: i64,
	) -> Result<()> {
		if owner.is_empty() || epoch < 0 {
			return Err(EngineError::invalid(
				"rollback pin release requires an owner and non-negative epoch",
			));
		}
		let generation = i64::try_from(operation_generation).map_err(|_| {
			EngineError::invalid("rollback operation generation exceeds PostgreSQL range")
		})?;
		self.with_client(|client| {
			client
				.execute(
					"DELETE FROM portable_rollback_pins WHERE sid = $1 AND point_name = $2 AND \
					 operation_generation = $3 AND owner = $4 AND epoch = $5",
					&[&sid as &(dyn ToSql + Sync), &name, &generation, &owner, &epoch],
				)
				.map_err(database_error)?;
			Ok(())
		})
	}

	/// List durable pins for lifecycle startup reconciliation. This
	/// intentionally has no implicit release path: unresolved journals retain
	/// their target.
	pub fn list_rollback_pins(&self) -> Result<Vec<PortableRollbackPin>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid, point_name, operation_generation, owner, epoch FROM \
					 portable_rollback_pins ORDER BY sid, point_name, operation_generation",
					&[],
				)
				.map_err(database_error)?
				.iter()
				.map(|row| {
					Ok(PortableRollbackPin {
						sid:                  row.try_get(0).map_err(database_error)?,
						point_name:           row.try_get(1).map_err(database_error)?,
						operation_generation: i64_to_u64(
							row.try_get(2).map_err(database_error)?,
							"rollback operation generation",
						)?,
						owner:                row.try_get(3).map_err(database_error)?,
						epoch:                row.try_get(4).map_err(database_error)?,
					})
				})
				.collect()
		})
	}

	/// Remove every portable point for an exact, durable deletion tombstone.
	///
	/// The caller must run this after local teardown and before the matching
	/// ownership row is finally deleted.  Rows are the reachability authority:
	/// object deletion happens only after their transaction commits and then
	/// revalidates each candidate under its own object advisory lock.
	pub fn delete_sandbox_history(
		&self,
		sid: &str,
		deletion_owner: &str,
		deletion_epoch: i64,
	) -> Result<usize> {
		let sid = sid.to_owned();
		let deletion_owner = deletion_owner.to_owned();
		let deletion_token = format!("delete:{deletion_owner}:{deletion_epoch}");
		let objects = self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("portable-history:{sid}"))?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let tombstone = transaction
				.query_opt(
					"SELECT deleting FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = \
					 $3 AND delete_token = $4 FOR UPDATE",
					&[&sid as &(dyn ToSql + Sync), &deletion_owner, &deletion_epoch, &deletion_token],
				)
				.map_err(database_error)?;
			if let Some(row) = tombstone {
				if !row.try_get::<_, bool>(0).map_err(database_error)? {
					return Err(EngineError::busy(format!(
						"portable history deletion for {sid} requires an active ownership tombstone"
					)));
				}
			} else {
				let remaining = transaction
					.query_one("SELECT COUNT(*) FROM portable_recovery_points WHERE sid = $1", &[&sid])
					.map_err(database_error)?
					.try_get::<_, i64>(0)
					.map_err(database_error)?;
				if remaining != 0 {
					return Err(EngineError::busy(format!(
						"portable history deletion for {sid} lost its ownership tombstone"
					)));
				}
				transaction.commit().map_err(database_error)?;
				return Ok(BTreeSet::new());
			}
			transaction
				.execute("DELETE FROM portable_rollback_pins WHERE sid = $1", &[&sid])
				.map_err(database_error)?;
			let rows = transaction
				.query(
					"DELETE FROM portable_recovery_points WHERE sid = $1 RETURNING archive_key, \
					 manifest_key",
					&[&sid],
				)
				.map_err(database_error)?;
			let mut objects = BTreeSet::new();
			for row in &rows {
				objects.insert(row.try_get::<_, String>(0).map_err(database_error)?);
				objects.insert(row.try_get::<_, String>(1).map_err(database_error)?);
			}
			transaction.commit().map_err(database_error)?;
			Ok(objects)
		})?;
		let mut removed = 0;
		for object in objects {
			match self.remove_if_unreferenced(&object) {
				Ok(true) => removed += 1,
				Ok(false) => {},
				Err(error) => {
					tracing::warn!(%error, object, "portable deletion object cleanup deferred");
				},
			}
		}
		Ok(removed)
	}

	/// content-addressed objects are intentionally left for [`Self::gc`]: an
	/// immediate delete could race a separate publisher of the same digest.
	pub fn abort(&self, _published: PublishedPortablePoint) -> Result<()> {
		Ok(())
	}

	/// Look up one exact committed recovery point.  Object-store listings never
	/// participate in lookup, so partial uploads are not restorable.
	pub fn lookup(&self, sid: &str, name: &str) -> Result<Option<PortableRecoveryPoint>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT p.sid, p.name, p.kind, p.created_at_unix_millis, p.archive_size_bytes, \
					 p.archive_key, p.archive_sha256, p.manifest_key, p.manifest_sha256, p.owner_node, \
					 p.owner_epoch, p.incarnation_epoch, p.lifecycle_generation FROM \
					 portable_recovery_points p JOIN sandbox_ownership o ON o.sid = p.sid AND \
					 o.incarnation_epoch = p.incarnation_epoch AND o.deleting = FALSE WHERE p.sid = $1 \
					 AND p.name = $2",
					&[&sid, &name],
				)
				.map_err(database_error)?
				.as_ref()
				.map(point_from_row)
				.transpose()
		})
	}

	/// List only committed portable points in chronological order.
	pub fn history(&self, sid: &str) -> Result<Vec<PortableRecoveryPoint>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT p.sid, p.name, p.kind, p.created_at_unix_millis, p.archive_size_bytes, \
					 p.archive_key, p.archive_sha256, p.manifest_key, p.manifest_sha256, p.owner_node, \
					 p.owner_epoch, p.incarnation_epoch, p.lifecycle_generation FROM \
					 portable_recovery_points p JOIN sandbox_ownership o ON o.sid = p.sid AND \
					 o.incarnation_epoch = p.incarnation_epoch AND o.deleting = FALSE WHERE p.sid = $1 \
					 ORDER BY p.capture_sequence, p.name",
					&[&sid],
				)
				.map_err(database_error)?
				.iter()
				.map(point_from_row)
				.collect()
		})
	}

	/// Fetch a committed archive to a unique temporary path, hash it while
	/// reading, then atomically rename it into `destination`.
	pub fn download(&self, point: &PortableRecoveryPoint, destination: &Path) -> Result<()> {
		let requested = point.clone();
		let parent = destination
			.parent()
			.ok_or_else(|| EngineError::invalid("portable recovery destination has no parent"))?;
		fs::create_dir_all(parent)?;
		let temporary = parent.join(format!(
			".{}.portable-download-{}",
			file_name(destination)?,
			uuid::Uuid::new_v4()
		));
		let result = (|| {
			let mut client = pg::connect(&self.postgres_url, "portable-history download guard")?;
			for object in
				BTreeSet::from([requested.archive_key.clone(), requested.manifest_key.clone()])
			{
				session_advisory_lock(&mut client, &object_lock_key(&object))?;
			}
			let committed = pg::blocking(|| {
				let mut transaction = client.transaction().map_err(database_error)?;
				let row = transaction
					.query_opt(
						"SELECT p.sid, p.name, p.kind, p.created_at_unix_millis, p.archive_size_bytes, \
						 p.archive_key, p.archive_sha256, p.manifest_key, p.manifest_sha256, \
						 p.owner_node, p.owner_epoch, p.incarnation_epoch, p.lifecycle_generation FROM \
						 portable_recovery_points p JOIN sandbox_ownership o ON o.sid = p.sid AND \
						 o.incarnation_epoch = p.incarnation_epoch AND o.deleting = FALSE WHERE p.sid = \
						 $1 AND p.name = $2",
						&[&requested.sid, &requested.name],
					)
					.map_err(database_error)?
					.ok_or_else(|| {
						EngineError::not_found(format!(
							"portable recovery point {} not found",
							requested.name
						))
					})?;
				let committed = point_from_row(&row)?;
				if committed != requested {
					return Err(EngineError::invalid(
						"portable recovery metadata changed before download",
					));
				}
				transaction.commit().map_err(database_error)?;
				Ok(committed)
			})?;
			self.verify_manifest(&committed)?;
			self.download_verified(
				&committed.archive_key,
				&temporary,
				&committed.archive_sha256,
				committed.size_bytes,
			)?;
			Ok(fs::rename(&temporary, destination)?)
		})();
		if let Err(error) = result {
			let _ = fs::remove_file(&temporary);
			return Err(error);
		}
		Ok(())
	}

	/// Prune expired metadata and delete old, unreferenced completed objects.
	/// `PostgreSQL` remains authoritative; S3 listings provide candidates only.
	pub fn gc(&self) -> Result<usize> {
		let mut removed = 0;
		for object in self.prune_retained_rows()? {
			if self.remove_if_unreferenced(&object)? {
				removed += 1;
			}
		}
		for root in [
			format!("{}/portable-history/v1/blobs/sha256", self.namespace),
			format!("{}/portable-history/v1/manifests/sha256", self.namespace),
		] {
			let mut continuation = None;
			loop {
				let page = self.scan_page(&root, continuation.as_deref())?;
				for entry in page.entries {
					if Self::listed_old_enough(entry.mtime) && self.remove_if_unreferenced(&entry.key)? {
						removed += 1;
					}
				}
				let Some(next) = page.next_continuation_token else {
					break;
				};
				continuation = Some(next);
			}
		}
		let replica_directory = format!("{}/replicas", self.namespace);
		let mut continuation = None;
		loop {
			let page = self.scan_page(&replica_directory, continuation.as_deref())?;
			for entry in page.entries {
				if !Self::listed_old_enough(entry.mtime) {
					continue;
				}
				if let Some(final_object) = replica_staging_final_object(&self.namespace, &entry.key) {
					if self.remove_staged_replica_if_old(&entry.key, &final_object)? {
						removed += 1;
					}
					continue;
				}
				if let Some(object_key) = scoped_replica_object_key(&self.namespace, &entry.key)
					&& self.remove_replica_if_unreferenced(&object_key)?
				{
					removed += 1;
				}
			}
			let Some(next) = page.next_continuation_token else {
				break;
			};
			continuation = Some(next);
		}
		if self.multipart_stale_sec > 0.0 {
			let older_than = Utc::now()
				- ChronoDuration::from_std(Duration::from_secs_f64(self.multipart_stale_sec))
					.unwrap_or_else(|_| ChronoDuration::days(365));
			if let Err(error) = Self::s3_block({
				let s3 = Arc::clone(&self.s3);
				let prefix = self.namespace.clone();
				let namespace = self.namespace.clone();
				let postgres_url = self.postgres_url.clone();
				async move {
					s3.abort_stale_multipart_uploads_guarded(&prefix, older_than, |object| {
						let lock_key = multipart_lock_key(&namespace, object);
						portable_object_session_guard(&postgres_url, &lock_key)
					})
					.await
				}
			}) {
				tracing::warn!(error = %error, "portable incomplete multipart cleanup deferred");
			}
		}
		Ok(removed)
	}

	fn scan_page(&self, prefix: &str, continuation: Option<&str>) -> Result<crate::s3::S3ScanPage> {
		Self::s3_block({
			let s3 = Arc::clone(&self.s3);
			let prefix = prefix.to_owned();
			let continuation = continuation.map(str::to_owned);
			async move {
				s3.scan_keys_page(&prefix, continuation.as_deref(), 1000)
					.await
			}
		})
	}

	fn listed_old_enough(mtime: u64) -> bool {
		unix_millis().is_ok_and(|now| (now / 1000).saturating_sub(mtime) >= GC_MIN_AGE_SECS)
	}

	fn blob_key(&self, digest: &str) -> String {
		let (shard, rest) = digest.split_at(2);
		format!("{}/portable-history/v1/blobs/sha256/{shard}/{rest}", self.namespace)
	}

	fn manifest_key(&self, digest: &str) -> String {
		manifest_key(&self.namespace, digest)
	}

	fn ensure_file(&self, key: &str, source: &Path, digest: &str) -> Result<()> {
		let expected_size = fs::metadata(source)?.len();
		match self.object_matches(key, digest, expected_size) {
			Ok(true) => return Ok(()),
			Ok(false) => {},
			Err(error) => return Err(error),
		}
		// A mismatching content-address key is corruption/collision, never overwrite
		// it.
		match self.stat(key) {
			Ok(_) => {
				return Err(EngineError::engine(format!("portable object {key} has unexpected bytes")));
			},
			Err(error) if error.code == crate::ErrorCode::NotFound => {},
			Err(error) => return Err(error),
		}
		self.upload_file(key, source)?;
		self.verify_object(key, digest, expected_size)
	}

	fn ensure_bytes(&self, key: &str, bytes: &[u8], digest: &str) -> Result<()> {
		let expected_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
		match self.object_matches(key, digest, expected_size) {
			Ok(true) => return Ok(()),
			Ok(false) => {},
			Err(error) => return Err(error),
		}
		match self.stat(key) {
			Ok(_) => {
				return Err(EngineError::engine(format!("portable object {key} has unexpected bytes")));
			},
			Err(error) if error.code == crate::ErrorCode::NotFound => {},
			Err(error) => return Err(error),
		}
		Self::s3_block({
			let s3 = Arc::clone(&self.s3);
			let key = key.to_owned();
			let bytes = bytes.to_vec();
			async move { s3.put(&key, bytes).await }
		})?;
		self.verify_object(key, digest, expected_size)
	}

	fn object_matches(&self, key: &str, digest: &str, size: u64) -> Result<bool> {
		match self.stat(key) {
			Ok(stat) if stat.kind == ObjKind::File && stat.size == size => {
				Ok(self.hash_object(key, &stat)? == digest)
			},
			Ok(_) => Ok(false),
			Err(error) if error.code == crate::ErrorCode::NotFound => Ok(false),
			Err(error) => Err(error),
		}
	}

	fn verify_object(&self, key: &str, digest: &str, size: u64) -> Result<()> {
		let stat = self.stat(key)?;
		if stat.kind != ObjKind::File || stat.size != size || self.hash_object(key, &stat)? != digest
		{
			return Err(EngineError::engine(format!("portable object verification failed for {key}")));
		}
		Ok(())
	}

	fn stat(&self, key: &str) -> Result<crate::s3::ObjStat> {
		Self::s3_block({
			let s3 = Arc::clone(&self.s3);
			let key = key.to_owned();
			async move { s3.head_object(&key).await }
		})
	}

	fn hash_object(&self, key: &str, stat: &crate::s3::ObjStat) -> Result<String> {
		let mut hasher = Sha256::new();
		let mut offset = 0;
		while offset < stat.size {
			let wanted =
				u32::try_from((stat.size - offset).min(COPY_CHUNK_BYTES as u64)).unwrap_or(u32::MAX);
			let bytes = self.read_range(key, stat, offset, wanted)?;
			if bytes.is_empty() {
				return Err(EngineError::engine(format!("portable object {key} ended before EOF")));
			}
			offset += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
			hasher.update(&bytes);
		}
		Ok(hex::encode(hasher.finalize()))
	}

	fn read_object(&self, key: &str, stat: &crate::s3::ObjStat) -> Result<Vec<u8>> {
		let mut object = Vec::with_capacity(usize::try_from(stat.size).unwrap_or(0));
		let mut offset = 0;
		while offset < stat.size {
			let wanted =
				u32::try_from((stat.size - offset).min(COPY_CHUNK_BYTES as u64)).unwrap_or(u32::MAX);
			let bytes = self.read_range(key, stat, offset, wanted)?;
			if bytes.is_empty() {
				return Err(EngineError::engine(format!("portable object {key} ended before EOF")));
			}
			offset += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
			object.extend_from_slice(&bytes);
		}
		Ok(object)
	}

	fn read_range(
		&self,
		key: &str,
		stat: &crate::s3::ObjStat,
		offset: u64,
		length: u32,
	) -> Result<bytes::Bytes> {
		Self::s3_block({
			let s3 = Arc::clone(&self.s3);
			let key = key.to_owned();
			let stat = stat.clone();
			async move { s3.read_range_if_match(&key, &stat, offset, length).await }
		})
	}

	fn verify_manifest(&self, point: &PortableRecoveryPoint) -> Result<()> {
		let stat = self.stat(&point.manifest_key)?;
		if stat.kind != ObjKind::File || stat.size > 64 * 1024 {
			return Err(EngineError::engine("portable recovery manifest has invalid size"));
		}
		let bytes = self.read_object(&point.manifest_key, &stat)?;
		if digest_bytes(&bytes) != point.manifest_sha256 {
			return Err(EngineError::engine("portable recovery manifest digest verification failed"));
		}
		let manifest: PortableManifest = serde_json::from_slice(&bytes)?;
		if !(1..=2).contains(&manifest.version)
			|| manifest.sid != point.sid
			|| manifest.name != point.name
			|| manifest.kind != point.kind
			|| manifest.created_at_unix_millis != point.created_at_unix_millis
			|| manifest.owner_node != point.owner_node
			|| manifest.owner_epoch != point.owner_epoch
			|| (manifest.version >= 2 && manifest.incarnation_epoch != point.incarnation_epoch)
			|| manifest.lifecycle_generation != point.lifecycle_generation
			|| manifest.archive_size_bytes.ne(&point.size_bytes)
			|| manifest.archive_sha256 != point.archive_sha256
		{
			return Err(EngineError::invalid(
				"portable recovery manifest does not match committed point",
			));
		}
		Ok(())
	}

	fn upload_file(&self, key: &str, source: &Path) -> Result<()> {
		let (sender, receiver) = tokio::sync::mpsc::channel(4);
		let source = source.to_owned();
		let producer = std::thread::spawn(move || -> std::io::Result<()> {
			let mut file = match File::open(source) {
				Ok(file) => file,
				Err(error) => {
					let _ =
						sender.blocking_send(Err(std::io::Error::new(error.kind(), error.to_string())));
					return Err(error);
				},
			};
			let mut buffer = vec![0_u8; COPY_CHUNK_BYTES];
			loop {
				let read = match file.read(&mut buffer) {
					Ok(read) => read,
					Err(error) => {
						let _ = sender
							.blocking_send(Err(std::io::Error::new(error.kind(), error.to_string())));
						return Err(error);
					},
				};
				if read == 0 {
					return Ok(());
				}
				if sender
					.blocking_send(Ok(bytes::Bytes::copy_from_slice(&buffer[..read])))
					.is_err()
				{
					return Ok(());
				}
			}
		});
		let key = key.to_owned();
		let uploaded = Self::s3_block({
			let s3 = Arc::clone(&self.s3);
			async move { s3.put_multipart(&key, receiver).await }
		});
		let produced = producer
			.join()
			.map_err(|_| EngineError::engine("portable upload producer panicked"))?;
		produced?;
		uploaded
	}

	fn download_verified(
		&self,
		key: &str,
		destination: &Path,
		digest: &str,
		size: u64,
	) -> Result<()> {
		let stat = self.stat(key)?;
		if stat.kind != ObjKind::File || stat.size != size {
			return Err(EngineError::engine("portable archive size changed before download"));
		}
		let mut output = File::create(destination)?;
		let mut hasher = Sha256::new();
		let mut offset = 0;
		while offset < size {
			let wanted =
				u32::try_from((size - offset).min(COPY_CHUNK_BYTES as u64)).unwrap_or(u32::MAX);
			let bytes = Self::s3_block({
				let s3 = Arc::clone(&self.s3);
				let key = key.to_owned();
				let stat = stat.clone();
				async move { s3.read_range_if_match(&key, &stat, offset, wanted).await }
			})?;
			if bytes.is_empty() {
				return Err(EngineError::engine("portable archive ended before EOF"));
			}
			output.write_all(&bytes)?;
			offset += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
			hasher.update(&bytes);
		}
		output.sync_all()?;
		if hex::encode(hasher.finalize()) != digest {
			return Err(EngineError::engine("portable archive digest verification failed"));
		}
		Ok(())
	}

	fn remove_if_unreferenced(&self, object: &str) -> Result<bool> {
		let object = object.to_owned();
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &object_lock_key(&object))?;
			let referenced = history_object_referenced(&mut transaction, &object)?;
			if !referenced && self.object_is_old_enough(&object)? {
				Self::s3_block({
					let s3 = Arc::clone(&self.s3);
					let object = object.clone();
					async move { s3.remove(&object).await }
				})?;
				transaction.commit().map_err(database_error)?;
				return Ok(true);
			}
			transaction.commit().map_err(database_error)?;
			Ok(false)
		})
	}

	fn remove_replica_if_unreferenced(&self, object: &str) -> Result<bool> {
		let object = object.to_owned();
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &replica_object_lock_key(&object))?;
			// The replica migration belongs to ProductionStore. Until it has
			// established its authority table, conservatively retain every bundle.
			let replicas_ready = transaction
				.query_one("SELECT to_regclass('sandbox_replicas') IS NOT NULL", &[])
				.map_err(database_error)?
				.try_get::<_, bool>(0)
				.map_err(database_error)?;
			let referenced = replicas_ready
				&& transaction
					.query_one(
						"SELECT EXISTS (SELECT 1 FROM sandbox_replicas WHERE object_key = $1)",
						&[&object],
					)
					.map_err(database_error)?
					.try_get::<_, bool>(0)
					.map_err(database_error)?;
			if replica_is_collectible(replicas_ready, referenced, self.object_is_old_enough(&object)?)
			{
				Self::s3_block({
					let s3 = Arc::clone(&self.s3);
					let object = object.clone();
					async move { s3.remove(&object).await }
				})?;
				transaction.commit().map_err(database_error)?;
				return Ok(true);
			}
			transaction.commit().map_err(database_error)?;
			Ok(false)
		})
	}

	/// Completed staging bundles are never referenced metadata.  The final
	/// bundle lock fences a publisher which may still be verifying this staged
	/// object, then exact HEAD rechecks its age under that lock.
	fn remove_staged_replica_if_old(&self, staging: &str, final_object: &str) -> Result<bool> {
		let staging = staging.to_owned();
		let final_object = final_object.to_owned();
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &replica_object_lock_key(&final_object))?;
			if self.object_is_old_enough(&staging)? {
				Self::s3_block({
					let s3 = Arc::clone(&self.s3);
					async move { s3.remove(&staging).await }
				})?;
				transaction.commit().map_err(database_error)?;
				return Ok(true);
			}
			transaction.commit().map_err(database_error)?;
			Ok(false)
		})
	}

	fn object_is_old_enough(&self, object: &str) -> Result<bool> {
		match self.stat(object) {
			Ok(stat) => Ok(stat.kind == ObjKind::File
				&& unix_millis()
					.is_ok_and(|now| (now / 1000).saturating_sub(stat.mtime) >= GC_MIN_AGE_SECS)),
			Err(error) if error.code == crate::ErrorCode::NotFound => Ok(false),
			Err(error) => Err(error),
		}
	}

	fn prune_retained_rows(&self) -> Result<BTreeSet<String>> {
		self.prune_retained_rows_with(self.retention)
	}

	fn prune_retained_rows_with(&self, retention: RetentionPolicy) -> Result<BTreeSet<String>> {
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			let sandboxes = transaction
				.query("SELECT DISTINCT sid FROM portable_recovery_points ORDER BY sid", &[])
				.map_err(database_error)?;
			let mut objects = BTreeSet::new();
			for row in sandboxes {
				let sid = row.try_get::<_, String>(0).map_err(database_error)?;
				advisory_lock(&mut transaction, &format!("portable-history:{sid}"))?;
				objects.extend(prune_transaction(&mut transaction, &sid, None, retention)?);
			}
			transaction.commit().map_err(database_error)?;
			Ok(objects)
		})
	}

	fn with_client<T: Send>(
		&self,
		operation: impl FnOnce(&mut Client) -> Result<T> + Send,
	) -> Result<T> {
		let mut guard = self.client.lock();
		let client = guard
			.as_mut()
			.ok_or_else(|| EngineError::engine("portable-history PostgreSQL store is closed"))?;
		pg::blocking(|| operation(client))
	}

	fn s3_block<T: Send + 'static>(
		future: impl Future<Output = std::result::Result<T, S3Error>> + Send + 'static,
	) -> Result<T> {
		let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
			if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
				tokio::task::block_in_place(|| handle.block_on(future))
			} else {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.map_err(|error| {
						EngineError::engine(format!("creating portable S3 runtime: {error}"))
					})?;
				std::thread::spawn(move || runtime.block_on(future))
					.join()
					.map_err(|_| EngineError::engine("portable S3 worker panicked"))?
			}
		} else {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.map_err(|error| EngineError::engine(format!("creating portable S3 runtime: {error}")))?
				.block_on(future)
		};
		result.map_err(s3_error)
	}
}

fn prune_transaction(
	transaction: &mut Transaction<'_>,
	sid: &str,
	protected_name: Option<&str>,
	retention: RetentionPolicy,
) -> Result<Vec<String>> {
	// Caller holds `portable-history:{sid}`; take the ownership lock second so
	// a marker cannot be committed between candidate selection and deletion.
	advisory_lock(transaction, &format!("sandbox:{sid}"))?;
	let now = transaction
		.query_one("SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT", &[])
		.map_err(database_error)?
		.try_get::<_, i64>(0)
		.map_err(database_error)
		.and_then(|value| i64_to_u64(value, "recovery retention timestamp"))?;
	let mut candidates = Vec::new();
	for kind in ["disk", "checkpoint"] {
		let rows = transaction
			.query(
				"SELECT name, committed_at_unix_millis, archive_key, manifest_key FROM \
				 portable_recovery_points p WHERE sid = $1 AND kind = $2 AND NOT EXISTS (SELECT 1 \
				 FROM portable_rollback_pins pin WHERE pin.sid = p.sid AND pin.point_name = p.name) \
				 AND NOT EXISTS (SELECT 1 FROM sandbox_ownership ownership WHERE ownership.sid = \
				 p.sid AND ownership.deleting = FALSE AND ( (ownership.suspend_point = p.name AND \
				 ownership.lifecycle_state <> 'running') OR (ownership.lifecycle_state = \
				 'rolling_back' AND (ownership.rollback_target = p.name OR ownership.rollback_safety \
				 = p.name)) )) ORDER BY capture_sequence, name",
				&[&sid, &kind],
			)
			.map_err(database_error)?;
		for (index, row) in rows.iter().enumerate() {
			let name = row.try_get::<_, String>(0).map_err(database_error)?;
			let committed = i64_to_u64(
				row.try_get::<_, i64>(1).map_err(database_error)?,
				"recovery commit timestamp",
			)?;
			if retention_prunes_row(
				index,
				rows.len(),
				&name,
				committed,
				now,
				protected_name,
				retention,
			) {
				candidates.push((
					name,
					row.try_get::<_, String>(2).map_err(database_error)?,
					row.try_get::<_, String>(3).map_err(database_error)?,
				));
			}
		}
	}
	let mut objects = BTreeSet::new();
	for (name, archive_key, manifest_key) in candidates {
		transaction
			.execute("DELETE FROM portable_recovery_points WHERE sid = $1 AND name = $2", &[
				&sid as &(dyn ToSql + Sync),
				&name,
			])
			.map_err(database_error)?;
		objects.insert(archive_key);
		objects.insert(manifest_key);
	}
	Ok(objects.into_iter().collect())
}

fn portable_key_verifier(key: &[u8]) -> String {
	let mut verifier_hasher = Sha256::new();
	verifier_hasher.update(b"vibemon portable-history key verifier v1\0");
	verifier_hasher.update(key);
	hex::encode(verifier_hasher.finalize())
}

fn retention_prunes_row(
	index: usize,
	total: usize,
	name: &str,
	created_at_unix_millis: u64,
	now_unix_millis: u64,
	protected_name: Option<&str>,
	retention: RetentionPolicy,
) -> bool {
	let newest = index + 1 == total;
	let over_count = total.saturating_sub(index) > retention.per_kind.max(1);
	let expired = retention
		.max_age_unix_millis
		.is_some_and(|max_age| now_unix_millis.saturating_sub(created_at_unix_millis) > max_age);
	!newest && protected_name != Some(name) && (over_count || expired)
}

fn history_object_referenced(transaction: &mut Transaction<'_>, object: &str) -> Result<bool> {
	transaction
		.query_one(
			"SELECT EXISTS (SELECT 1 FROM portable_recovery_points WHERE archive_key = $1 OR \
			 manifest_key = $1)",
			&[&object],
		)
		.map_err(database_error)?
		.try_get::<_, bool>(0)
		.map_err(database_error)
}

/// Map only a fully validated scoped final bundle key to itself. Scope is
/// derived from the tenant key snapshot, so equal plaintext digests in two
/// scopes are distinct immutable objects.
fn scoped_replica_object_key(namespace: &str, object: &str) -> Option<String> {
	let scoped = object.strip_prefix(&format!("{namespace}/replicas/by-key/"))?;
	let (scope, name) = scoped.split_once('/')?;
	if scope.len() != 64 || !is_sha256_digest(scope) || name.contains('/') {
		return None;
	}
	let digest = name.strip_suffix(".vbundle")?;
	if !is_sha256_digest(digest) {
		return None;
	}
	Some(object.to_owned())
}

/// Replica publication holds the scoped final bundle lock while writing a
/// staging object. Map only a fully validated staging name to that final key.
fn multipart_lock_object(namespace: &str, object: &str) -> String {
	replica_staging_final_object(namespace, object).unwrap_or_else(|| object.to_owned())
}

/// Pick the same lock as replica publication/readers for a valid final or
/// staging bundle. Other multipart objects retain their portable-history key.
fn multipart_lock_key(namespace: &str, object: &str) -> String {
	let object = multipart_lock_object(namespace, object);
	if scoped_replica_object_key(namespace, &object).is_some() {
		replica_object_lock_key(&object)
	} else {
		object_lock_key(&object)
	}
}

fn replica_staging_final_object(namespace: &str, object: &str) -> Option<String> {
	let staged = object.strip_prefix(&format!("{namespace}/replicas/.staging/by-key/"))?;
	let (scope, name) = staged.split_once('/')?;
	if scope.len() != 64 || !is_sha256_digest(scope) || name.contains('/') {
		return None;
	}
	let name = name.strip_suffix(".vbundle")?;
	let digest = name.get(..64)?;
	let operation = name.get(64..)?.strip_prefix('-')?;
	if !is_sha256_digest(digest) || uuid::Uuid::parse_str(operation).is_err() {
		return None;
	}
	Some(format!("{namespace}/replicas/by-key/{scope}/{digest}.vbundle"))
}
fn is_sha256_digest(value: &str) -> bool {
	value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn object_lock_key(object: &str) -> String {
	format!("portable-history-object:{object}")
}

const fn replica_is_collectible(replicas_ready: bool, referenced: bool, old_enough: bool) -> bool {
	replicas_ready && !referenced && old_enough
}

fn manifest_key(namespace: &str, digest: &str) -> String {
	let (shard, rest) = digest.split_at(2);
	format!("{namespace}/portable-history/v1/manifests/sha256/{shard}/{rest}.json")
}

/// Install the portable-history schema for production-store acceptance tests.
#[cfg(test)]
pub(crate) fn migrate_for_test(client: &mut Client) -> Result<()> {
	migrate(client, "test")
}

fn migrate(client: &mut Client, namespace: &str) -> Result<()> {
	client
		.batch_execute(
			r"
			CREATE TABLE IF NOT EXISTS portable_recovery_points (
				sid TEXT NOT NULL,
				name TEXT NOT NULL,
				kind TEXT NOT NULL CHECK (kind IN ('disk', 'checkpoint')),
				created_at_unix_millis BIGINT NOT NULL,
				archive_size_bytes BIGINT NOT NULL,
				archive_key TEXT NOT NULL,
				archive_sha256 TEXT NOT NULL,
				manifest_key TEXT NOT NULL,
				manifest_sha256 TEXT NOT NULL,
				owner_node TEXT NOT NULL DEFAULT '',
				owner_epoch BIGINT NOT NULL DEFAULT 0,
				incarnation_epoch BIGINT NOT NULL DEFAULT 0,
				lifecycle_generation BIGINT NOT NULL DEFAULT 0,
				capture_sequence BIGSERIAL,
				committed_at_unix_millis BIGINT NOT NULL DEFAULT 0,
				PRIMARY KEY (sid, name)
			);
			ALTER TABLE portable_recovery_points ADD COLUMN IF NOT EXISTS owner_node TEXT NOT NULL DEFAULT '';
			ALTER TABLE portable_recovery_points ADD COLUMN IF NOT EXISTS owner_epoch BIGINT NOT NULL DEFAULT 0;
			ALTER TABLE portable_recovery_points ADD COLUMN IF NOT EXISTS incarnation_epoch BIGINT NOT NULL DEFAULT 0;
			ALTER TABLE portable_recovery_points ADD COLUMN IF NOT EXISTS lifecycle_generation BIGINT NOT NULL DEFAULT 0;
			ALTER TABLE portable_recovery_points ADD COLUMN IF NOT EXISTS capture_sequence BIGSERIAL;
			ALTER TABLE portable_recovery_points ADD COLUMN IF NOT EXISTS manifest_key TEXT;
			ALTER TABLE portable_recovery_points ADD COLUMN IF NOT EXISTS committed_at_unix_millis BIGINT;
			UPDATE portable_recovery_points points SET incarnation_epoch = ownership.incarnation_epoch
			FROM sandbox_ownership ownership
			WHERE points.sid = ownership.sid AND points.incarnation_epoch = 0
				AND ownership.deleting = FALSE
				AND points.created_at_unix_millis >= FLOOR(ownership.created_at * 1000)::BIGINT;
			UPDATE portable_recovery_points
			SET committed_at_unix_millis = created_at_unix_millis
			WHERE committed_at_unix_millis IS NULL OR committed_at_unix_millis = 0;
			ALTER TABLE portable_recovery_points ALTER COLUMN committed_at_unix_millis SET NOT NULL;
			CREATE INDEX IF NOT EXISTS portable_recovery_points_history
				ON portable_recovery_points (sid, kind, capture_sequence, name);
			CREATE TABLE IF NOT EXISTS portable_rollback_pins (
				sid TEXT NOT NULL,
				point_name TEXT NOT NULL,
				operation_generation BIGINT NOT NULL,
				PRIMARY KEY (sid, point_name, operation_generation)
			);
			ALTER TABLE portable_rollback_pins ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT '';
			ALTER TABLE portable_rollback_pins ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;
			",
		)
		.map_err(database_error)?;
	let rows = client
		.query(
			"SELECT sid, name, manifest_sha256 FROM portable_recovery_points WHERE manifest_key IS \
			 NULL",
			&[],
		)
		.map_err(database_error)?;
	for row in rows {
		let sid = row.try_get::<_, String>(0).map_err(database_error)?;
		let name = row.try_get::<_, String>(1).map_err(database_error)?;
		let digest = row.try_get::<_, String>(2).map_err(database_error)?;
		if !is_sha256_digest(&digest) {
			return Err(EngineError::engine(
				"portable recovery migration found an invalid manifest digest",
			));
		}
		let key = manifest_key(namespace, &digest);
		client
			.execute(
				"UPDATE portable_recovery_points SET manifest_key = $1 WHERE sid = $2 AND name = $3",
				&[&key, &sid, &name],
			)
			.map_err(database_error)?;
	}
	client
		.batch_execute("ALTER TABLE portable_recovery_points ALTER COLUMN manifest_key SET NOT NULL;")
		.map_err(database_error)
}

fn point_from_prepared(prepared: &PreparedPortablePoint) -> PortableRecoveryPoint {
	PortableRecoveryPoint {
		sid:                    prepared.manifest.sid.clone(),
		incarnation_epoch:      prepared.manifest.incarnation_epoch,
		name:                   prepared.manifest.name.clone(),
		kind:                   prepared.manifest.kind.clone(),
		created_at_unix_millis: prepared.manifest.created_at_unix_millis,
		owner_node:             prepared.manifest.owner_node.clone(),
		owner_epoch:            prepared.manifest.owner_epoch,
		lifecycle_generation:   prepared.manifest.lifecycle_generation,
		size_bytes:             prepared.manifest.archive_size_bytes,
		archive_key:            prepared.archive_key.clone(),
		archive_sha256:         prepared.manifest.archive_sha256.clone(),
		manifest_key:           prepared.manifest_key.clone(),
		manifest_sha256:        digest_bytes(&prepared.manifest_bytes),
	}
}

fn point_from_row(row: &::postgres::Row) -> Result<PortableRecoveryPoint> {
	let kind = row.try_get::<_, String>(2).map_err(database_error)?;
	if kind != "disk" && kind != "checkpoint" {
		return Err(EngineError::engine("portable recovery point has invalid kind"));
	}
	Ok(PortableRecoveryPoint {
		sid: row.try_get(0).map_err(database_error)?,
		name: row.try_get(1).map_err(database_error)?,
		kind,
		created_at_unix_millis: i64_to_u64(
			row.try_get(3).map_err(database_error)?,
			"recovery timestamp",
		)?,
		size_bytes: i64_to_u64(row.try_get(4).map_err(database_error)?, "recovery archive size")?,
		archive_key: row.try_get(5).map_err(database_error)?,
		archive_sha256: row.try_get(6).map_err(database_error)?,
		manifest_key: row.try_get(7).map_err(database_error)?,
		manifest_sha256: row.try_get(8).map_err(database_error)?,
		owner_node: row.try_get(9).unwrap_or_default(),
		owner_epoch: row.try_get(10).unwrap_or_default(),
		incarnation_epoch: row.try_get(11).unwrap_or_default(),
		lifecycle_generation: i64_to_u64(
			row.try_get(12).unwrap_or_default(),
			"lifecycle generation",
		)?,
	})
}

fn watchdog_until_stopped(deadline: &Mutex<Instant>, stopped: &AtomicBool, lost: &AtomicBool) {
	while !stopped.load(Ordering::Acquire) {
		if Instant::now() >= *deadline.lock() {
			lost.store(true, Ordering::Release);
			break;
		}
		thread::sleep(Duration::from_millis(50));
	}
}

fn validate_suspend_intent(
	point: &PortableRecoveryPoint,
	intent: &PortableSuspendIntent,
) -> Result<()> {
	if intent.sid != point.sid
		|| intent.point.ne(&point.name)
		|| intent.owner != point.owner_node
		|| intent.epoch != point.owner_epoch
		|| intent.lifecycle_generation != point.lifecycle_generation
		|| intent.owner.is_empty()
		|| intent.epoch < 0
	{
		return Err(EngineError::invalid(
			"portable suspend intent does not exactly match the published recovery point",
		));
	}
	Ok(())
}

fn write_suspending_marker(
	transaction: &mut Transaction<'_>,
	intent: &PortableSuspendIntent,
) -> Result<()> {
	let generation = i64::try_from(intent.lifecycle_generation)
		.map_err(|_| EngineError::invalid("suspend lifecycle generation exceeds PostgreSQL range"))?;
	let changed = transaction
		.execute(
			"UPDATE sandbox_ownership SET lifecycle_state = 'suspending', suspend_point = $4, \
			 suspend_generation = $5 WHERE sid = $1 AND owner = $2 AND epoch = $3 AND deleting = \
			 FALSE AND lifecycle_state = 'running' AND owner_lease_owner = $2 AND owner_lease_epoch \
			 = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp())",
			&[
				&intent.sid as &(dyn ToSql + Sync),
				&intent.owner,
				&intent.epoch,
				&intent.point,
				&generation,
			],
		)
		.map_err(database_error)?;
	if changed == 1 {
		return Ok(());
	}
	let row = transaction
		.query_opt(
			"SELECT lifecycle_state, suspend_point, suspend_generation FROM sandbox_ownership WHERE \
			 sid = $1 AND owner = $2 AND epoch = $3 AND deleting = FALSE AND owner_lease_owner = $2 \
			 AND owner_lease_epoch = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM \
			 clock_timestamp()) FOR UPDATE",
			&[&intent.sid as &(dyn ToSql + Sync), &intent.owner, &intent.epoch],
		)
		.map_err(database_error)?
		.ok_or_else(|| {
			EngineError::busy(format!("suspend marker for {} lost ownership", intent.sid))
		})?;
	let state = row.try_get::<_, String>(0).map_err(database_error)?;
	let point = row
		.try_get::<_, Option<String>>(1)
		.map_err(database_error)?;
	let marker_generation = row.try_get::<_, Option<i64>>(2).map_err(database_error)?;
	if state == "suspending"
		&& point.as_deref() == Some(intent.point.as_str())
		&& marker_generation == Some(generation)
	{
		return Ok(());
	}
	Err(EngineError::busy(format!(
		"sandbox {} lifecycle {state} conflicts with requested suspension",
		intent.sid
	)))
}

fn validate_input(input: &PortablePointInput) -> Result<()> {
	if input.sid.is_empty()
		|| input.name.is_empty()
		|| input.sid.contains('/')
		|| input.name.contains('/')
	{
		return Err(EngineError::invalid(
			"portable recovery sid and name must be non-empty local names",
		));
	}
	if input.kind != "disk" && input.kind != "checkpoint" {
		return Err(EngineError::invalid("portable recovery kind must be disk or checkpoint"));
	}
	if input.owner_node.is_empty() || input.owner_epoch < 0 || input.incarnation_epoch < 0 {
		return Err(EngineError::invalid(
			"portable recovery requires an authoritative owner, epoch, and incarnation",
		));
	}
	Ok(())
}

fn ttl_duration(ttl_seconds: f64) -> Result<Duration> {
	if !ttl_seconds.is_finite() || ttl_seconds <= 0.0 {
		return Err(EngineError::busy("portable restore lease expired"));
	}
	Duration::try_from_secs_f64(ttl_seconds)
		.map_err(|_| EngineError::busy("portable restore lease TTL is invalid"))
}
fn restore_deadline(started: Instant, ttl: Duration) -> Instant {
	// PostgreSQL reports a duration while handling the request. Anchor the
	// deadline at request start and subtract a shared margin so ownership loss
	// is detected before another node may legally claim this VM.
	started
		.checked_add(ttl.saturating_sub(RESTORE_LEASE_SAFETY_MARGIN))
		.unwrap_or(started)
}

fn restore_lease_lost(sid: &str) -> EngineError {
	EngineError::busy(format!("portable restore lease for {sid} was fenced or expired"))
}
fn hash_file(path: &Path) -> Result<(String, u64)> {
	let mut file = File::open(path)?;
	let mut hasher = Sha256::new();
	let mut size = 0_u64;
	let mut buffer = vec![0_u8; COPY_CHUNK_BYTES];
	loop {
		let read = file.read(&mut buffer)?;
		if read == 0 {
			break;
		}
		hasher.update(&buffer[..read]);
		size = size.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
	}
	Ok((hex::encode(hasher.finalize()), size))
}

fn digest_bytes(bytes: &[u8]) -> String {
	hex::encode(Sha256::digest(bytes))
}

fn file_name(path: &Path) -> Result<&str> {
	path
		.file_name()
		.and_then(|name| name.to_str())
		.ok_or_else(|| EngineError::invalid("portable recovery destination has invalid filename"))
}

fn required_config<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str> {
	value
		.filter(|value| !value.is_empty())
		.ok_or_else(|| EngineError::invalid(format!("production portable history requires {name}")))
}

fn i64_to_u64(value: i64, field: &str) -> Result<u64> {
	u64::try_from(value).map_err(|_| EngineError::engine(format!("portable {field} is negative")))
}

fn unix_millis() -> Result<u64> {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
		.map_err(|error| EngineError::engine(format!("system clock precedes Unix epoch: {error}")))
}

fn session_advisory_lock(client: &mut Client, key: &str) -> Result<()> {
	pg::blocking(|| {
		client
			.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key])
			.map_err(database_error)?;
		Ok(())
	})
}

fn portable_object_session_guard(
	postgres_url: &str,
	lock_key: &str,
) -> Result<PortableObjectSessionGuard> {
	let mut guard = PortableObjectSessionGuard {
		client: Some(pg::connect(postgres_url, "portable-history multipart cleanup guard")?),
	};
	let client = guard
		.client
		.as_mut()
		.ok_or_else(|| EngineError::engine("portable object session guard was released"))?;
	session_advisory_lock(client, lock_key)?;
	Ok(guard)
}
fn advisory_lock(transaction: &mut Transaction<'_>, key: &str) -> Result<()> {
	transaction
		.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&key])
		.map_err(database_error)?;
	Ok(())
}

fn database_error(error: ::postgres::Error) -> EngineError {
	EngineError::engine(format!("portable-history PostgreSQL metadata store: {error}"))
}

fn s3_error(error: S3Error) -> EngineError {
	match error {
		S3Error::NotFound => EngineError::not_found("portable-history S3 object was not found"),
		error => EngineError::engine(format!("portable-history S3: {error}")),
	}
}

#[cfg(test)]
mod tests {
	use std::{
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
		thread,
		time::{Duration, Instant},
	};

	use parking_lot::Mutex;

	use super::{
		RetentionPolicy, multipart_lock_key, multipart_lock_object, object_lock_key,
		replica_is_collectible, replica_object_lock_key, replica_staging_final_object,
		restore_deadline, retention_prunes_row, scoped_replica_object_key, ttl_duration,
		watchdog_until_stopped,
	};
	fn pruned_names<'a>(
		rows: &'a [(&'a str, u64)],
		now: u64,
		protected_name: Option<&str>,
		retention: RetentionPolicy,
	) -> Vec<&'a str> {
		rows
			.iter()
			.enumerate()
			.filter_map(|(index, (name, created))| {
				retention_prunes_row(index, rows.len(), name, *created, now, protected_name, retention)
					.then_some(*name)
			})
			.collect()
	}

	#[test]
	fn replica_publisher_reader_and_multipart_gc_share_the_final_object_lock() {
		let namespace = "cluster-a";
		let scope = "b".repeat(64);
		let digest = "a".repeat(64);
		let staged = format!(
			"{namespace}/replicas/.staging/by-key/{scope}/\
			 {digest}-550e8400-e29b-41d4-a716-446655440000.vbundle"
		);
		let final_key = format!("{namespace}/replicas/by-key/{scope}/{digest}.vbundle");
		assert_eq!(replica_object_lock_key(&final_key), format!("replica-object:{final_key}"));
		assert_eq!(multipart_lock_object(namespace, &staged), final_key);
		assert_eq!(multipart_lock_key(namespace, &staged), replica_object_lock_key(&final_key));
		assert_eq!(multipart_lock_key(namespace, &final_key), replica_object_lock_key(&final_key));
		assert!(
			replica_staging_final_object(
				namespace,
				&format!("{namespace}/replicas/.staging/by-key/{scope}/not-a-stage.vbundle")
			)
			.is_none()
		);
		assert_eq!(
			multipart_lock_object(
				namespace,
				&format!("{namespace}/replicas/.staging/by-key/not-a-scope/nope"),
			),
			format!("{namespace}/replicas/.staging/by-key/not-a-scope/nope")
		);
	}

	#[test]
	fn scheduled_expiry_without_new_capture_preserves_the_newest_point() {
		let policy = RetentionPolicy { per_kind: 24, max_age_unix_millis: Some(100) };
		assert_eq!(pruned_names(&[("only", 1)], 1_000, None, policy), Vec::<&str>::new());
		assert_eq!(pruned_names(&[("old", 1), ("newest", 2)], 1_000, None, policy), vec!["old"]);
	}

	#[test]
	fn retention_keeps_newest_point_in_each_tier() {
		let policy = RetentionPolicy { per_kind: 1, max_age_unix_millis: Some(1) };
		assert_eq!(pruned_names(&[("disk-old", 1), ("disk-newest", 2)], 1_000, None, policy), vec![
			"disk-old"
		]);
		assert_eq!(
			pruned_names(&[("checkpoint-old", 1), ("checkpoint-newest", 2)], 1_000, None, policy,),
			vec!["checkpoint-old"]
		);
	}
	#[test]
	fn lowered_count_prunes_without_a_new_capture() {
		let policy = RetentionPolicy { per_kind: 2, max_age_unix_millis: None };
		assert_eq!(pruned_names(&[("one", 1), ("two", 2), ("three", 3)], 3, None, policy), vec![
			"one"
		]);
	}

	#[test]
	fn history_and_replica_objects_use_distinct_canonical_lock_domains() {
		let object = "portable-history/v1/blobs/sha256/ab/cd";
		assert_eq!(
			object_lock_key(object),
			"portable-history-object:portable-history/v1/blobs/sha256/ab/cd"
		);
		assert_eq!(
			replica_object_lock_key(object),
			"replica-object:portable-history/v1/blobs/sha256/ab/cd"
		);
		assert_ne!(object_lock_key(object), replica_object_lock_key(object));
	}

	#[test]
	fn concurrent_publication_and_gc_share_the_exact_object_lock() {
		let object = "portable-history/v1/blobs/sha256/ab/cd";
		assert_eq!(
			object_lock_key(object),
			"portable-history-object:portable-history/v1/blobs/sha256/ab/cd"
		);
	}

	#[test]
	fn referenced_replica_bundle_is_preserved() {
		assert!(!replica_is_collectible(true, true, true));
		assert!(!replica_is_collectible(false, false, true));
	}

	#[test]
	fn blocked_renew_watchdog_fences_before_database_expiry() {
		let started = Instant::now();
		let ttl = Duration::from_secs(5);
		let raw_expiry = started + ttl;
		let deadline = Arc::new(Mutex::new(restore_deadline(started, ttl)));
		let stopped = Arc::new(AtomicBool::new(false));
		let lost = Arc::new(AtomicBool::new(false));
		let worker_deadline = Arc::clone(&deadline);
		let worker_stopped = Arc::clone(&stopped);
		let worker_lost = Arc::clone(&lost);
		let worker = thread::spawn(move || {
			watchdog_until_stopped(&worker_deadline, &worker_stopped, &worker_lost);
		});
		worker.join().unwrap();
		assert!(lost.load(Ordering::Acquire));
		assert!(Instant::now() < raw_expiry, "watchdog must fence before DB claimability");
	}

	#[test]
	fn lease_deadline_is_anchored_at_request_start_with_margin() {
		let started = Instant::now();
		let ttl = ttl_duration(6.0).unwrap();
		assert_eq!(restore_deadline(started, ttl), started + Duration::from_secs(1));
		assert!(ttl_duration(0.0).is_err());
	}
	#[test]
	fn scoped_replica_keys_keep_same_digest_scopes_separate() {
		let namespace = "cluster-a";
		let digest = "a".repeat(64);
		let scope_a = "b".repeat(64);
		let scope_b = "c".repeat(64);
		let first = format!("{namespace}/replicas/by-key/{scope_a}/{digest}.vbundle");
		let second = format!("{namespace}/replicas/by-key/{scope_b}/{digest}.vbundle");
		assert_eq!(scoped_replica_object_key(namespace, &first), Some(first.clone()));
		assert_eq!(scoped_replica_object_key(namespace, &second), Some(second.clone()));
		assert_ne!(replica_object_lock_key(&first), replica_object_lock_key(&second));
		assert!(
			scoped_replica_object_key(
				namespace,
				&format!("{namespace}/replicas/by-key/{scope_a}/{digest}/extra.vbundle")
			)
			.is_none()
		);
		assert!(replica_is_collectible(true, false, true));
		assert!(!replica_is_collectible(true, false, false));
	}
}
