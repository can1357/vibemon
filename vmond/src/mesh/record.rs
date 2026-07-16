//! Durable mesh create-record replication.
//!
//! Python stores one JSON file per acknowledged create at
//! `$VMON_HOME/records/<sid>.json`.  The `params.secrets` field is never
//! written: it is split into a memory-only side map and reattached only while
//! this process still has it.  That invariant is load-bearing for HA rerun and
//! record anti-entropy, so this module keeps the same split at the type
//! boundary.

use std::{
	collections::BTreeMap,
	fmt, fs,
	io::ErrorKind,
	os::unix::fs::DirBuilderExt,
	path::{Path, PathBuf},
	process,
	time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::RwLock;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::{EngineError, Result, home::Home};

/// Mesh HA tiers accepted by Python's `CreateRecord.from_wire`.
pub const ALLOWED_HA: &[&str] = &["off", "async", "rerun", "async+rerun"];

const RECORD_DIR_MODE: u32 = 0o700;

pub type Params = JsonMap<String, JsonValue>;

/// Return the restart policy implied by a durability tier.
pub fn restart_policy_for_ha(ha: &str) -> &'static str {
	if ha.contains("rerun") {
		"rerun"
	} else {
		"none"
	}
}

/// Required create-record acknowledgements for an expected mesh size.
///
/// The caller supplies `live_peer_count` because the two-node rule is weaker
/// than quorum by design: expected <= 2 requires local + every currently-live
/// peer; expected >= 3 requires strict majority.
pub const fn required_record_acks(expected_members: usize, live_peer_count: usize) -> usize {
	if expected_members <= 2 {
		1 + live_peer_count
	} else {
		expected_members / 2 + 1
	}
}

/// Error code used by routes when synchronous create-record replication misses
/// its required acknowledgement count.
pub const RECORD_UNREPLICATED_CODE: &str = "record_unreplicated";

/// Synchronous mesh metadata for an acknowledged create.
#[derive(Clone, Debug, PartialEq)]
pub struct CreateRecord {
	pub sid:               String,
	pub params:            Params,
	pub owner:             String,
	pub epoch:             i64,
	/// Stable sandbox incarnation; owner epoch may advance during handoff.
	pub incarnation_epoch: i64,
	pub idempotency_key:   String,
	pub ha:                String,
	pub restart_policy:    String,
	pub created_at:        f64,
}

impl CreateRecord {
	#[allow(clippy::too_many_arguments, reason = "record construction mirrors the mesh wire schema")]
	pub fn new(
		sid: impl Into<String>,
		params: Params,
		owner: impl Into<String>,
		epoch: i64,
		idempotency_key: impl Into<String>,
		ha: impl Into<String>,
		restart_policy: impl Into<String>,
		created_at: f64,
	) -> Result<Self> {
		let sid = non_empty_string("record sid is required", sid.into())?;
		let owner = non_empty_string("record owner is required", owner.into())?;
		let idempotency_key =
			non_empty_string("record idempotency_key is required", idempotency_key.into())?;
		let ha = ha.into();
		if !ALLOWED_HA.contains(&ha.as_str()) {
			return Err(EngineError::invalid(format!("invalid ha tier {ha:?}")));
		}
		let restart_policy = restart_policy.into();
		let created_at = if created_at.is_finite() {
			created_at
		} else {
			unix_now()
		};
		Ok(Self {
			sid,
			params,
			owner,
			epoch,
			incarnation_epoch: epoch,
			idempotency_key,
			ha,
			restart_policy,
			created_at,
		})
	}

	/// Return a JSON-ready representation for disk and peer replication.
	pub fn to_wire(&self) -> JsonValue {
		JsonValue::Object(self.to_wire_map())
	}

	pub fn to_wire_map(&self) -> Params {
		let mut out = Params::new();
		out.insert("sid".to_owned(), JsonValue::String(self.sid.clone()));
		out.insert("params".to_owned(), JsonValue::Object(self.params.clone()));
		out.insert("owner".to_owned(), JsonValue::String(self.owner.clone()));
		out.insert("epoch".to_owned(), JsonValue::Number(JsonNumber::from(self.epoch)));
		out.insert(
			"incarnation_epoch".to_owned(),
			JsonValue::Number(JsonNumber::from(self.incarnation_epoch)),
		);
		out.insert("idempotency_key".to_owned(), JsonValue::String(self.idempotency_key.clone()));
		out.insert("ha".to_owned(), JsonValue::String(self.ha.clone()));
		out.insert("restart_policy".to_owned(), JsonValue::String(self.restart_policy.clone()));
		out.insert("created_at".to_owned(), json_f64(self.created_at));
		out
	}

	/// Build and validate a record from persisted or peer-provided data.
	pub fn from_wire(data: &JsonValue) -> Result<Self> {
		let object = data
			.as_object()
			.ok_or_else(|| EngineError::invalid("record must be an object"))?;
		Self::from_wire_map(object)
	}

	pub fn from_wire_map(data: &Params) -> Result<Self> {
		let sid = required_string(data, "sid", "record sid is required")?;
		let params = data
			.get("params")
			.and_then(JsonValue::as_object)
			.cloned()
			.ok_or_else(|| EngineError::invalid("record params must be an object"))?;
		let owner = required_string(data, "owner", "record owner is required")?;
		let idempotency_key =
			required_string(data, "idempotency_key", "record idempotency_key is required")?;
		let ha = optional_string(data.get("ha"), "off");
		let restart_policy = optional_string(data.get("restart_policy"), restart_policy_for_ha(&ha));
		let epoch = data.get("epoch").and_then(JsonValue::as_i64).unwrap_or(0);
		let mut record = Self::new(
			sid,
			params,
			owner,
			epoch,
			idempotency_key,
			ha,
			restart_policy,
			data
				.get("created_at")
				.and_then(JsonValue::as_f64)
				.unwrap_or_default(),
		)?;
		record.incarnation_epoch = data
			.get("incarnation_epoch")
			.and_then(JsonValue::as_i64)
			.unwrap_or(epoch);
		Ok(record)
	}
}

/// Build a durable create record from normalized request params.
pub fn make_create_record(
	sid: impl Into<String>,
	owner: impl Into<String>,
	epoch: i64,
	idempotency_key: impl Into<String>,
	mut params: Params,
	kind: impl Into<String>,
) -> Result<CreateRecord> {
	params.remove("idempotency_key");
	params.insert("_kind".to_owned(), JsonValue::String(kind.into()));
	let ha = optional_string(params.get("ha"), "off");
	let restart_policy = optional_string(params.get("restart_policy"), restart_policy_for_ha(&ha));
	params.insert("restart_policy".to_owned(), JsonValue::String(restart_policy.clone()));
	CreateRecord::new(sid, params, owner, epoch, idempotency_key, ha, restart_policy, unix_now())
}

/// Persist create records while keeping secret env material memory-only.
pub struct RecordStore {
	root:  PathBuf,
	inner: RwLock<RecordInner>,
}

#[derive(Default)]
struct RecordInner {
	meta:    BTreeMap<String, CreateRecord>,
	secrets: BTreeMap<String, JsonValue>,
}

impl fmt::Debug for RecordStore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RecordStore")
			.field("root", &self.root)
			.finish_non_exhaustive()
	}
}

impl Default for RecordStore {
	fn default() -> Self {
		Self::for_home(Home::default())
	}
}

impl RecordStore {
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self { root: root.into(), inner: RwLock::new(RecordInner::default()) }
	}

	pub fn for_home(home: Home) -> Self {
		Self::new(home.records_dir())
	}

	pub fn root(&self) -> &Path {
		&self.root
	}

	/// Load persisted records, ignoring corrupt or incomplete files.
	pub fn load(&self) {
		let entries = fs::read_dir(&self.root);
		let mut inner = self.inner.write();
		inner.meta.clear();
		inner.secrets.clear();
		let Ok(entries) = entries else {
			return;
		};
		for entry in entries.flatten() {
			let path = entry.path();
			if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
				continue;
			}
			let Ok(text) = fs::read_to_string(&path) else {
				continue;
			};
			let Ok(value) = serde_json::from_str::<JsonValue>(&text) else {
				continue;
			};
			let Ok(mut record) = CreateRecord::from_wire(&value) else {
				continue;
			};
			let (clean, _) = split_secrets(&record.params);
			record.params = clean;
			inner.meta.insert(record.sid.clone(), record);
		}
	}

	/// Persist or replace a create record atomically.
	pub fn put(&self, mut record: CreateRecord) -> Result<()> {
		let (clean, secrets) = split_secrets(&record.params);
		record.params = clean;
		let mut inner = self.inner.write();
		store_secrets(&mut inner, &record.sid, secrets);
		inner.meta.insert(record.sid.clone(), record.clone());
		write_record(&self.root, &record.sid, &record.to_wire())
	}

	/// Apply the `/v1/mesh/record/put` acceptance rule: accept iff no local
	/// record exists or the incoming epoch is at least the existing epoch.
	pub fn put_if_newer(&self, record: CreateRecord) -> Result<bool> {
		let accept = self
			.get(&record.sid)
			.is_none_or(|existing| record.epoch >= existing.epoch);
		if accept {
			self.put(record)?;
		}
		Ok(accept)
	}

	/// Return a create record, reattaching in-memory secrets if still present.
	pub fn get(&self, sid: &str) -> Option<CreateRecord> {
		let inner = self.inner.read();
		let mut record = inner.meta.get(sid)?.clone();
		if let Some(secrets) = inner.secrets.get(sid) {
			record.params.insert("secrets".to_owned(), secrets.clone());
		}
		Some(record)
	}

	/// Remember secret material for a durable record without writing it to disk.
	pub(crate) fn remember_secrets(&self, sid: &str, secrets: Option<JsonValue>) {
		store_secrets(&mut self.inner.write(), sid, secrets);
	}

	/// Reattach process-local secret material to a record loaded from durable
	/// storage.
	pub(crate) fn attach_secrets(&self, record: &mut CreateRecord) {
		if let Some(secrets) = self.inner.read().secrets.get(&record.sid) {
			record.params.insert("secrets".to_owned(), secrets.clone());
		}
	}

	/// Forget process-local secret material after its durable record is removed.
	pub(crate) fn forget_secrets(&self, sid: &str) {
		self.inner.write().secrets.remove(sid);
	}

	/// Update a record's authoritative owner and persist the replacement.
	pub fn update_owner(
		&self,
		sid: &str,
		owner: impl Into<String>,
		epoch: i64,
	) -> Result<Option<CreateRecord>> {
		let Some(mut record) = self.get(sid) else {
			return Ok(None);
		};
		record.owner = owner.into();
		record.epoch = epoch;
		self.put(record.clone())?;
		Ok(Some(record))
	}

	/// Return all valid records in stable sandbox-id order.
	#[allow(
		clippy::needless_collect,
		reason = "collecting keys releases the metadata read lock before each record load"
	)]
	pub fn list(&self) -> Vec<CreateRecord> {
		let keys = self.inner.read().meta.keys().cloned().collect::<Vec<_>>();
		keys.into_iter().filter_map(|sid| self.get(&sid)).collect()
	}

	/// Remove a create record and any memory-only secret material.
	pub fn remove(&self, sid: &str) -> Result<()> {
		{
			let mut inner = self.inner.write();
			inner.meta.remove(sid);
			inner.secrets.remove(sid);
		}
		match fs::remove_file(self.root.join(format!("{sid}.json"))) {
			Ok(()) => {},
			Err(err) if err.kind() == ErrorKind::NotFound => {},
			Err(err) => return Err(err.into()),
		}
		Ok(())
	}

	pub fn drop_record(&self, sid: &str) -> Result<()> {
		self.remove(sid)
	}

	pub fn contains(&self, sid: &str) -> bool {
		self.inner.read().meta.contains_key(sid)
	}
}

fn store_secrets(inner: &mut RecordInner, sid: &str, secrets: Option<JsonValue>) {
	if let Some(secrets) = secrets {
		inner.secrets.insert(sid.to_owned(), secrets);
	} else {
		inner.secrets.remove(sid);
	}
}

/// Remove `params.secrets` for durable storage and return the memory-only
/// value.
pub fn split_secrets(params: &Params) -> (Params, Option<JsonValue>) {
	let mut clean = params.clone();
	let secrets = match clean.remove("secrets") {
		Some(JsonValue::Null) | None => None,
		Some(value) => Some(value),
	};
	(clean, secrets)
}

fn write_record(root: &Path, sid: &str, value: &JsonValue) -> Result<()> {
	ensure_private_dir(root)?;
	let path = root.join(format!("{sid}.json"));
	let tmp = temp_path_for(&path);
	let bytes = serde_json::to_vec(value)?;
	fs::write(&tmp, [&bytes[..], b"\n"].concat())?;
	fs::rename(&tmp, path)?;
	Ok(())
}

fn required_string(data: &Params, key: &str, message: &'static str) -> Result<String> {
	let value = data
		.get(key)
		.and_then(JsonValue::as_str)
		.unwrap_or_default();
	non_empty_string(message, value.to_owned())
}

fn non_empty_string(message: &'static str, value: String) -> Result<String> {
	if value.is_empty() {
		Err(EngineError::invalid(message))
	} else {
		Ok(value)
	}
}

fn optional_string(value: Option<&JsonValue>, default: &str) -> String {
	match value {
		Some(JsonValue::String(text)) if !text.is_empty() => text.clone(),
		Some(JsonValue::Number(number)) => number.to_string(),
		Some(JsonValue::Bool(flag)) if *flag => "true".to_owned(),
		_ => default.to_owned(),
	}
}

fn json_f64(value: f64) -> JsonValue {
	JsonValue::Number(JsonNumber::from_f64(value).unwrap_or_else(|| JsonNumber::from(0)))
}

fn unix_now() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| {
			duration.as_secs() as f64 + f64::from(duration.subsec_nanos()) / 1_000_000_000.0
		})
}

fn ensure_private_dir(path: &Path) -> Result<()> {
	let mut builder = fs::DirBuilder::new();
	builder.recursive(true).mode(RECORD_DIR_MODE);
	builder.create(path).or_else(|err| {
		if err.kind() == ErrorKind::AlreadyExists {
			Ok(())
		} else {
			Err(err)
		}
	})?;
	Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
	let file_name = path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("record.json");
	path.with_file_name(format!(".{file_name}.{}.tmp", process::id()))
}
