//! Replica metadata and restore prerequisites.
//!
//! Cold HA replicas persist only checkpoint metadata at
//! `$VMON_HOME/replicas/<sid>.json`.  Restore secrets are process memory only;
//! after a restart, `needs_secrets == true` plus a missing memory entry makes
//! `secrets_ready` false so the reconciler refuses a degraded restore instead
//! of silently dropping secret environment.

use std::{
	collections::BTreeMap,
	fmt, fs,
	io::ErrorKind,
	os::unix::fs::DirBuilderExt,
	path::{Path, PathBuf},
	process,
};

use parking_lot::RwLock;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::record::{Params, split_secrets};
use crate::{EngineError, Result, home::Home};

const REPLICA_DIR_MODE: u32 = 0o700;

/// Metadata for one held replica checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRecord {
	pub sid:           String,
	pub digest:        String,
	pub source_node:   String,
	pub snapshot_dir:  String,
	pub params:        Params,
	pub needs_secrets: bool,
}

impl ReplicaRecord {
	pub fn new(
		sid: impl Into<String>,
		digest: impl Into<String>,
		source_node: impl Into<String>,
		snapshot_dir: impl Into<String>,
		params: Params,
		needs_secrets: bool,
	) -> Result<Self> {
		let sid = non_empty_string("replica sid is required", sid.into())?;
		let digest = non_empty_string("replica digest is required", digest.into())?;
		let source_node = non_empty_string("replica source_node is required", source_node.into())?;
		let snapshot_dir = non_empty_string("replica snapshot_dir is required", snapshot_dir.into())?;
		Ok(Self { sid, digest, source_node, snapshot_dir, params, needs_secrets })
	}

	pub fn to_wire(&self) -> JsonValue {
		JsonValue::Object(self.to_wire_map())
	}

	pub fn to_wire_map(&self) -> JsonMap<String, JsonValue> {
		let mut out = JsonMap::new();
		out.insert("sid".to_owned(), JsonValue::String(self.sid.clone()));
		out.insert("digest".to_owned(), JsonValue::String(self.digest.clone()));
		out.insert("source_node".to_owned(), JsonValue::String(self.source_node.clone()));
		out.insert("snapshot_dir".to_owned(), JsonValue::String(self.snapshot_dir.clone()));
		out.insert("params".to_owned(), JsonValue::Object(self.params.clone()));
		out.insert("needs_secrets".to_owned(), JsonValue::Bool(self.needs_secrets));
		out
	}

	pub fn from_wire(data: &JsonValue) -> Result<Self> {
		let object = data
			.as_object()
			.ok_or_else(|| EngineError::invalid("replica record must be an object"))?;
		let sid = required_string(object, "sid", "replica sid is required")?;
		let digest = required_string(object, "digest", "replica digest is required")?;
		let source_node = required_string(object, "source_node", "replica source_node is required")?;
		let snapshot_dir =
			required_string(object, "snapshot_dir", "replica snapshot_dir is required")?;
		let params = object
			.get("params")
			.and_then(JsonValue::as_object)
			.cloned()
			.ok_or_else(|| EngineError::invalid("replica params must be an object"))?;
		let needs_secrets = object
			.get("needs_secrets")
			.and_then(JsonValue::as_bool)
			.unwrap_or(false);
		Self::new(sid, digest, source_node, snapshot_dir, params, needs_secrets)
	}
}

/// Persist replica metadata while keeping restore secrets in memory only.
pub struct ReplicaStore {
	root:  PathBuf,
	inner: RwLock<ReplicaInner>,
}

#[derive(Default)]
struct ReplicaInner {
	meta:    BTreeMap<String, ReplicaRecord>,
	secrets: BTreeMap<String, JsonValue>,
}

impl fmt::Debug for ReplicaStore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ReplicaStore")
			.field("root", &self.root)
			.finish_non_exhaustive()
	}
}

impl Default for ReplicaStore {
	fn default() -> Self {
		Self::for_home(Home::default())
	}
}

impl ReplicaStore {
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self { root: root.into(), inner: RwLock::new(ReplicaInner::default()) }
	}

	pub fn for_home(home: Home) -> Self {
		Self::new(home.replicas_dir())
	}

	pub fn root(&self) -> &Path {
		&self.root
	}

	/// Load persisted replica metadata, skipping missing or corrupt files.
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
			let Ok(mut record) = ReplicaRecord::from_wire(&value) else {
				continue;
			};
			let (clean, _) = split_secrets(&record.params);
			record.params = clean;
			inner.meta.insert(record.sid.clone(), record);
		}
	}

	/// Persist replica metadata for `sid` without writing secrets to disk.
	pub fn put(
		&self,
		sid: impl Into<String>,
		digest: impl Into<String>,
		source_node: impl Into<String>,
		snapshot_dir: impl Into<String>,
		params: Params,
	) -> Result<()> {
		let sid = sid.into();
		let digest = digest.into();
		let source_node = source_node.into();
		let snapshot_dir = snapshot_dir.into();
		let (clean, secrets) = split_secrets(&params);
		let needs_secrets = secrets.is_some();
		let record =
			ReplicaRecord::new(sid, digest, source_node, snapshot_dir, clean, needs_secrets)?;
		self.put_clean(record, secrets)
	}

	pub fn put_record(&self, record: ReplicaRecord) -> Result<()> {
		let (clean, secrets) = split_secrets(&record.params);
		let mut clean_record = record;
		clean_record.params = clean;
		clean_record.needs_secrets = clean_record.needs_secrets || secrets.is_some();
		self.put_clean(clean_record, secrets)
	}

	fn put_clean(&self, record: ReplicaRecord, secrets: Option<JsonValue>) -> Result<()> {
		let mut inner = self.inner.write();
		if let Some(secrets) = secrets {
			inner.secrets.insert(record.sid.clone(), secrets);
		} else {
			inner.secrets.remove(&record.sid);
		}
		inner.meta.insert(record.sid.clone(), record.clone());
		write_record(&self.root, &record.sid, &record.to_wire())
	}

	/// Return the record for `sid`, reattaching in-memory secrets if any.
	pub fn get(&self, sid: &str) -> Option<ReplicaRecord> {
		let inner = self.inner.read();
		let mut record = inner.meta.get(sid)?.clone();
		if let Some(secrets) = inner.secrets.get(sid) {
			record.params.insert("secrets".to_owned(), secrets.clone());
		}
		Some(record)
	}

	/// Return whether this store has metadata for `sid`.
	pub fn holds(&self, sid: &str) -> bool {
		self.inner.read().meta.contains_key(sid)
	}

	/// True when the replica carries no secrets, or its secrets are still in
	/// memory.
	pub fn secrets_ready(&self, sid: &str) -> bool {
		let inner = self.inner.read();
		let Some(record) = inner.meta.get(sid) else {
			return false;
		};
		!record.needs_secrets || inner.secrets.contains_key(sid)
	}

	/// Return held replica sandbox ids in stable sorted order.
	pub fn list(&self) -> Vec<String> {
		self.inner.read().meta.keys().cloned().collect()
	}

	/// Remove held metadata for `sid` while leaving its snapshot directory.
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

	pub fn drop_replica(&self, sid: &str) -> Result<()> {
		self.remove(sid)
	}
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

fn required_string(
	data: &JsonMap<String, JsonValue>,
	key: &str,
	message: &'static str,
) -> Result<String> {
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

fn ensure_private_dir(path: &Path) -> Result<()> {
	let mut builder = fs::DirBuilder::new();
	builder.recursive(true).mode(REPLICA_DIR_MODE);
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
		.unwrap_or("replica.json");
	path.with_file_name(format!(".{file_name}.{}.tmp", process::id()))
}
