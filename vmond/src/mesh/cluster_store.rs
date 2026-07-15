//! `PostgreSQL` ownership and lease metadata for production clusters.

use std::time::{SystemTime, UNIX_EPOCH};

use ::postgres::{Client, Row, Transaction, types::ToSql};
use parking_lot::Mutex;

use super::{lease::LeaseRecord, record::CreateRecord, replica::ReplicaRecord};
use crate::{EngineError, Result, postgres as pg};

/// Synchronous `PostgreSQL` store used by the blocking engine layer.
pub(crate) struct ProductionStore {
	client: Mutex<Option<Client>>,
}

impl ProductionStore {
	/// Connect to `PostgreSQL` and fail unless the cluster schema is usable.
	pub(crate) fn connect(url: &str) -> Result<Self> {
		let mut client = pg::connect(url, "cluster PostgreSQL store")?;
		pg::blocking(|| migrate(&mut client))?;
		Ok(Self { client: Mutex::new(Some(client)) })
	}

	/// Run a complete synchronous `PostgreSQL` operation outside Tokio's worker
	/// runtime. The client remains serialized while the operation executes.
	fn with_client<T>(&self, operation: impl FnOnce(&mut Client) -> Result<T>) -> Result<T> {
		let mut client = self.client.lock();
		let client = client
			.as_mut()
			.ok_or_else(|| EngineError::engine("PostgreSQL metadata store is closed"))?;
		pg::blocking(|| operation(client))
	}

	/// Insert or advance one sandbox ownership record transactionally.
	pub(crate) fn record(&self, record: &CreateRecord) -> Result<CreateRecord> {
		let record = record_without_secrets(record);
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{}", record.sid))?;
			if !record.idempotency_key.is_empty() {
				advisory_lock(
					&mut transaction,
					&format!("sandbox-idempotency:{}", record.idempotency_key),
				)?;
				if let Some(row) = transaction
					.query_opt(
						"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
						 created_at FROM sandbox_ownership WHERE idempotency_key = $1",
						&[&record.idempotency_key],
					)
					.map_err(database_error)?
				{
					let original = record_from_row(&row)?;
					transaction.commit().map_err(database_error)?;
					return Ok(original);
				}
			}

			if let Some(row) = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at \
					 FROM sandbox_ownership WHERE sid = $1 FOR UPDATE",
					&[&record.sid],
				)
				.map_err(database_error)?
			{
				let current = record_from_row(&row)?;
				if record.epoch < current.epoch
					|| (record.epoch == current.epoch && record.owner != current.owner)
				{
					return Err(fencing_error(
						&record.sid,
						&record.owner,
						record.epoch,
						&current.owner,
						current.epoch,
					));
				}
				if record.epoch == current.epoch {
					transaction.commit().map_err(database_error)?;
					return Ok(current);
				}
				let params = serde_json::to_string(&record.params)?;
				let idempotency_key = optional_key(&record.idempotency_key);
				transaction
					.execute(
						"UPDATE sandbox_ownership SET owner = $2, epoch = $3, idempotency_key = $4, \
						 params = $5, ha = $6, restart_policy = $7, created_at = $8 WHERE sid = $1",
						&[
							&record.sid as &(dyn ToSql + Sync),
							&record.owner,
							&record.epoch,
							&idempotency_key,
							&params,
							&record.ha,
							&record.restart_policy,
							&record.created_at,
						],
					)
					.map_err(database_error)?;
			} else {
				let params = serde_json::to_string(&record.params)?;
				let idempotency_key = optional_key(&record.idempotency_key);
				transaction
					.execute(
						"INSERT INTO sandbox_ownership (sid, owner, epoch, idempotency_key, params, ha, \
						 restart_policy, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
						&[
							&record.sid as &(dyn ToSql + Sync),
							&record.owner,
							&record.epoch,
							&idempotency_key,
							&params,
							&record.ha,
							&record.restart_policy,
							&record.created_at,
						],
					)
					.map_err(database_error)?;
			}
			transaction.commit().map_err(database_error)?;
			Ok(record.clone())
		})
	}

	/// Resolve the authoritative owner and create parameters for a sandbox.
	pub(crate) fn resolve(&self, sid: &str) -> Result<Option<CreateRecord>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at \
					 FROM sandbox_ownership WHERE sid = $1",
					&[&sid],
				)
				.map_err(database_error)?
				.as_ref()
				.map(record_from_row)
				.transpose()
		})
	}

	/// List every authoritative sandbox record in stable identifier order.
	pub(crate) fn list(&self) -> Result<Vec<CreateRecord>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at \
					 FROM sandbox_ownership ORDER BY sid",
					&[],
				)
				.map_err(database_error)?
				.iter()
				.map(record_from_row)
				.collect()
		})
	}

	/// List sandbox identifiers currently fenced to one owner.
	pub(crate) fn owned_by(&self, owner: &str) -> Result<Vec<String>> {
		self.with_client(|client| {
			client
				.query("SELECT sid FROM sandbox_ownership WHERE owner = $1 ORDER BY sid", &[&owner])
				.map_err(database_error)?
				.iter()
				.map(|row| row.try_get::<_, String>(0).map_err(database_error))
				.collect()
		})
	}

	/// Persist replica metadata whose checkpoint bytes live in shared object
	/// storage.
	pub(crate) fn put_replica(&self, record: &ReplicaRecord) -> Result<()> {
		let params = serde_json::to_string(&record.params)?;
		let updated_at = unix_now()?;
		self.with_client(|client| {
			client
				.execute(
					"INSERT INTO sandbox_replicas (sid, digest, source_node, snapshot_dir, params, \
					 needs_secrets, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (sid) \
					 DO UPDATE SET digest = EXCLUDED.digest, source_node = EXCLUDED.source_node, \
					 snapshot_dir = EXCLUDED.snapshot_dir, params = EXCLUDED.params, needs_secrets = \
					 EXCLUDED.needs_secrets, updated_at = EXCLUDED.updated_at",
					&[
						&record.sid as &(dyn ToSql + Sync),
						&record.digest,
						&record.source_node,
						&record.snapshot_dir,
						&params,
						&record.needs_secrets,
						&updated_at,
					],
				)
				.map_err(database_error)?;
			Ok(())
		})
	}

	/// Resolve shared replica metadata for a sandbox.
	pub(crate) fn replica(&self, sid: &str) -> Result<Option<ReplicaRecord>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT sid, digest, source_node, snapshot_dir, params, needs_secrets FROM \
					 sandbox_replicas WHERE sid = $1",
					&[&sid],
				)
				.map_err(database_error)?
				.as_ref()
				.map(replica_from_row)
				.transpose()
		})
	}

	/// List every sandbox with a checkpoint in shared object storage.
	pub(crate) fn list_replicas(&self) -> Result<Vec<String>> {
		self.with_client(|client| {
			client
				.query("SELECT sid FROM sandbox_replicas ORDER BY sid", &[])
				.map_err(database_error)?
				.iter()
				.map(|row| row.try_get::<_, String>(0).map_err(database_error))
				.collect()
		})
	}

	/// Remove shared replica metadata after a successful restore.
	pub(crate) fn remove_replica(&self, sid: &str) -> Result<()> {
		self.with_client(|client| {
			client
				.execute("DELETE FROM sandbox_replicas WHERE sid = $1", &[&sid])
				.map_err(database_error)?;
			Ok(())
		})
	}

	/// Move ownership only when the supplied epoch is newer or idempotent.
	pub(crate) fn claim(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		if owner.is_empty() || epoch < 0 {
			return Err(EngineError::invalid(
				"ownership claims require an owner and non-negative epoch",
			));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let Some(row) = transaction
				.query_opt("SELECT owner, epoch FROM sandbox_ownership WHERE sid = $1 FOR UPDATE", &[
					&sid,
				])
				.map_err(database_error)?
			else {
				return Err(EngineError::not_found(format!("sandbox {sid} has no ownership record")));
			};
			let current_owner = row.try_get::<_, String>(0).map_err(database_error)?;
			let current_epoch = row.try_get::<_, i64>(1).map_err(database_error)?;
			if epoch < current_epoch || (epoch == current_epoch && owner != current_owner) {
				return Err(fencing_error(sid, owner, epoch, &current_owner, current_epoch));
			}
			if epoch > current_epoch {
				transaction
					.execute("UPDATE sandbox_ownership SET owner = $2, epoch = $3 WHERE sid = $1", &[
						&sid as &(dyn ToSql + Sync),
						&owner,
						&epoch,
					])
					.map_err(database_error)?;
			}
			transaction.commit().map_err(database_error)
		})
	}

	/// Delete exactly the ownership generation that authorized the removal.
	pub(crate) fn remove(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		self.with_client(|client| {
			let deleted = client
				.execute(
					"DELETE FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)?;
			if deleted == 1 {
				return Ok(());
			}
			match Self::resolve_unlocked(client, sid)? {
				Some(current) => Err(fencing_error(sid, owner, epoch, &current.owner, current.epoch)),
				None => Ok(()),
			}
		})
	}

	/// Acquire or renew a cluster-wide writable-volume lease.
	pub(crate) fn acquire_lease(
		&self,
		volume: &str,
		holder: &str,
		epoch: u64,
		ttl: f64,
	) -> Result<LeaseRecord> {
		let epoch = i64::try_from(epoch)
			.map_err(|_| EngineError::invalid("lease epoch exceeds PostgreSQL range"))?;
		let now = unix_now()?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("volume:{volume}"))?;
			let row = transaction
				.query_opt(
					"SELECT holder, epoch, granted_at, ttl FROM volume_leases WHERE volume = $1 FOR \
					 UPDATE",
					&[&volume],
				)
				.map_err(database_error)?;
			if let Some(row) = row {
				let current_holder = row.try_get::<_, String>(0).map_err(database_error)?;
				let current_epoch = row.try_get::<_, i64>(1).map_err(database_error)?;
				let granted_at = row.try_get::<_, f64>(2).map_err(database_error)?;
				let current_ttl = row.try_get::<_, f64>(3).map_err(database_error)?;
				let expired = granted_at + current_ttl <= now;
				let renewal = current_holder == holder && epoch >= current_epoch;
				if !expired && !renewal {
					return Err(EngineError::busy(format!(
						"Lease collision: volume {volume} is leased by {current_holder} at epoch \
						 {current_epoch}"
					)));
				}
				if renewal && epoch < current_epoch {
					return Err(EngineError::invalid(format!(
						"stale lease epoch {epoch} for volume {volume}; current epoch is {current_epoch}"
					)));
				}
				transaction
					.execute(
						"UPDATE volume_leases SET holder = $2, epoch = $3, granted_at = $4, ttl = $5 \
						 WHERE volume = $1",
						&[&volume as &(dyn ToSql + Sync), &holder, &epoch, &now, &ttl],
					)
					.map_err(database_error)?;
			} else {
				transaction
					.execute(
						"INSERT INTO volume_leases (volume, holder, epoch, granted_at, ttl) VALUES ($1, \
						 $2, $3, $4, $5)",
						&[&volume as &(dyn ToSql + Sync), &holder, &epoch, &now, &ttl],
					)
					.map_err(database_error)?;
			}
			let record = LeaseRecord::new(volume, holder, epoch as u64, now, ttl)?;
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Release the exact writable-volume lease generation held by a node.
	pub(crate) fn release_lease(&self, volume: &str, holder: &str, epoch: u64) -> Result<()> {
		let epoch = i64::try_from(epoch)
			.map_err(|_| EngineError::invalid("lease epoch exceeds PostgreSQL range"))?;
		self.with_client(|client| {
			client
				.execute(
					"DELETE FROM volume_leases WHERE volume = $1 AND holder = $2 AND epoch = $3",
					&[&volume as &(dyn ToSql + Sync), &holder, &epoch],
				)
				.map_err(database_error)?;
			Ok(())
		})
	}

	fn resolve_unlocked(client: &mut Client, sid: &str) -> Result<Option<CreateRecord>> {
		client
			.query_opt(
				"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at \
				 FROM sandbox_ownership WHERE sid = $1",
				&[&sid],
			)
			.map_err(database_error)?
			.as_ref()
			.map(record_from_row)
			.transpose()
	}

	#[cfg(test)]
	pub(crate) fn clear_for_test(&self) -> Result<()> {
		self.with_client(|client| {
			client
				.batch_execute("TRUNCATE sandbox_ownership, sandbox_replicas, volume_leases")
				.map_err(database_error)
		})
	}
}

impl Drop for ProductionStore {
	fn drop(&mut self) {
		if let Some(client) = self.client.get_mut().take() {
			pg::blocking(move || drop(client));
		}
	}
}

fn migrate(client: &mut Client) -> Result<()> {
	client
		.batch_execute(
			r"
			CREATE TABLE IF NOT EXISTS sandbox_ownership (
				sid TEXT PRIMARY KEY,
				owner TEXT NOT NULL,
				epoch BIGINT NOT NULL,
				idempotency_key TEXT,
				params TEXT NOT NULL,
				ha TEXT NOT NULL,
				restart_policy TEXT NOT NULL,
				created_at DOUBLE PRECISION NOT NULL
			);
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS params TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS ha TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS restart_policy TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS created_at DOUBLE PRECISION;
			UPDATE sandbox_ownership SET params = '{}' WHERE params IS NULL;
			UPDATE sandbox_ownership SET ha = 'off' WHERE ha IS NULL;
			UPDATE sandbox_ownership SET restart_policy = 'none' WHERE restart_policy IS NULL;
			UPDATE sandbox_ownership SET created_at = 0 WHERE created_at IS NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN params SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN ha SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN restart_policy SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN created_at SET NOT NULL;
			CREATE UNIQUE INDEX IF NOT EXISTS sandbox_ownership_idempotency_key
				ON sandbox_ownership (idempotency_key)
				WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';

			CREATE TABLE IF NOT EXISTS sandbox_replicas (
				sid TEXT PRIMARY KEY,
				digest TEXT NOT NULL,
				source_node TEXT NOT NULL,
				snapshot_dir TEXT NOT NULL,
				params TEXT NOT NULL,
				needs_secrets BOOLEAN NOT NULL,
				updated_at DOUBLE PRECISION NOT NULL
			);

			CREATE TABLE IF NOT EXISTS volume_leases (
				volume TEXT PRIMARY KEY,
				holder TEXT NOT NULL,
				epoch BIGINT NOT NULL,
				granted_at DOUBLE PRECISION NOT NULL,
				ttl DOUBLE PRECISION NOT NULL
			);
			",
		)
		.map_err(database_error)
}

fn advisory_lock(transaction: &mut Transaction<'_>, key: &str) -> Result<()> {
	transaction
		.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&key])
		.map_err(database_error)?;
	Ok(())
}

fn record_from_row(row: &Row) -> Result<CreateRecord> {
	let sid = row.try_get::<_, String>(0).map_err(database_error)?;
	let params = row.try_get::<_, String>(1).map_err(database_error)?;
	let params = serde_json::from_str(&params).map_err(|error| {
		EngineError::engine(format!("invalid sandbox metadata for {sid}: {error}"))
	})?;
	let owner = row.try_get::<_, String>(2).map_err(database_error)?;
	let epoch = row.try_get::<_, i64>(3).map_err(database_error)?;
	let idempotency_key = row
		.try_get::<_, Option<String>>(4)
		.map_err(database_error)?
		.unwrap_or_default();
	let ha = row.try_get::<_, String>(5).map_err(database_error)?;
	let restart_policy = row.try_get::<_, String>(6).map_err(database_error)?;
	let created_at = row.try_get::<_, f64>(7).map_err(database_error)?;
	CreateRecord::new(sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at)
}

fn record_without_secrets(record: &CreateRecord) -> CreateRecord {
	let mut durable = record.clone();
	durable.params.remove("secrets");
	durable
}

fn replica_from_row(row: &Row) -> Result<ReplicaRecord> {
	let sid = row.try_get::<_, String>(0).map_err(database_error)?;
	let digest = row.try_get::<_, String>(1).map_err(database_error)?;
	let source_node = row.try_get::<_, String>(2).map_err(database_error)?;
	let snapshot_dir = row.try_get::<_, String>(3).map_err(database_error)?;
	let params = row.try_get::<_, String>(4).map_err(database_error)?;
	let params = serde_json::from_str(&params).map_err(|error| {
		EngineError::engine(format!("invalid replica metadata for {sid}: {error}"))
	})?;
	let needs_secrets = row.try_get::<_, bool>(5).map_err(database_error)?;
	ReplicaRecord::new(sid, digest, source_node, snapshot_dir, params, needs_secrets)
}

fn optional_key(key: &str) -> Option<&str> {
	(!key.is_empty()).then_some(key)
}

fn fencing_error(
	sid: &str,
	owner: &str,
	epoch: i64,
	current_owner: &str,
	current_epoch: i64,
) -> EngineError {
	EngineError::invalid(format!(
		"Fencing error: ownership claim for {sid} by {owner} at epoch {epoch} was fenced by \
		 {current_owner} at epoch {current_epoch}"
	))
}

fn unix_now() -> Result<f64> {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_secs_f64())
		.map_err(|error| EngineError::engine(format!("system clock precedes Unix epoch: {error}")))
}

fn database_error(error: ::postgres::Error) -> EngineError {
	EngineError::engine(format!("PostgreSQL metadata store: {error}"))
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn durable_records_exclude_secrets() {
		let params = json!({
			"image": "alpine",
			"secrets": [{"name": "migration", "values": {"TOKEN": "sensitive"}}],
		})
		.as_object()
		.expect("record parameters must be an object")
		.clone();
		let record = CreateRecord::new("sandbox", params, "node-a", 1, "request", "off", "none", 1.0)
			.expect("record must be valid");

		let durable = record_without_secrets(&record);

		assert_eq!(durable.params.get("image"), Some(&json!("alpine")));
		assert!(!durable.params.contains_key("secrets"));
		assert!(record.params.contains_key("secrets"));
	}
}
