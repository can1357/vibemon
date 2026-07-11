//! Sandbox registry: `VmRecord` + rehydrate. Port of python/vmon/core.py
//! registry.

use std::{
	collections::HashMap,
	fs, io,
	path::Path,
	time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{EngineError, Result, home::Home};

/// Engine seam for re-acquiring writable volume locks after registry rehydrate.
pub type VolumeLockRequests = Vec<(String, Vec<String>)>;

/// One sandbox registry entry shared by API and engine layers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VmRecord {
	/// Stable sandbox id.
	pub id:            String,
	/// Human-facing sandbox name.
	pub name:          String,
	/// Lifecycle status.
	pub status:        String,
	/// Per-VM monitor process id.
	pub pid:           Option<i32>,
	/// Image, template, fork, or restore source.
	pub source:        Option<String>,
	/// Unix creation timestamp.
	pub created_at:    f64,
	/// Timeout in seconds from last activity.
	pub timeout:       Option<f64>,
	/// Raw metadata detail spread into API views.
	pub detail:        Value,
	/// String tags used for filtering.
	pub tags:          HashMap<String, String>,
	/// Unix timestamp of last activity.
	pub last_active:   f64,
	/// Unix termination timestamp.
	pub terminated_at: Option<f64>,
	/// Terminal error string.
	pub error:         Option<String>,
}

impl VmRecord {
	/// Build a record with Python's timestamp defaults.
	pub fn new(id: impl Into<String>, name: impl Into<String>, status: impl Into<String>) -> Self {
		let now = unix_time();
		Self {
			id:            id.into(),
			name:          name.into(),
			status:        status.into(),
			pid:           None,
			source:        None,
			created_at:    now,
			timeout:       None,
			detail:        Value::Object(Map::new()),
			tags:          HashMap::new(),
			last_active:   now,
			terminated_at: None,
			error:         None,
		}
	}

	/// Return the timeout deadline, if one is armed.
	pub fn expires_at(&self) -> Option<f64> {
		let timeout = self.timeout?;
		(timeout > 0.0).then_some(self.last_active + timeout)
	}

	/// Refresh `last_active` to the current Unix time.
	pub fn touch(&mut self) {
		self.last_active = unix_time();
	}

	/// Return the API view, with canonical keys overriding raw detail.
	pub fn view(&self) -> Value {
		let mut out = self.detail.as_object().cloned().unwrap_or_default();
		let returncode = out.get("returncode").cloned().unwrap_or(Value::Null);
		out.remove("idempotency_key");
		out.insert("id".to_owned(), json!(self.id));
		out.insert("name".to_owned(), json!(self.name));
		out.insert("status".to_owned(), json!(self.status));
		out.insert("created_at".to_owned(), json!(self.created_at));
		out.insert("last_active".to_owned(), json!(self.last_active));
		out.insert("expires_at".to_owned(), json!(self.expires_at()));
		out.insert("terminated_at".to_owned(), json!(self.terminated_at));
		out.insert("error".to_owned(), json!(self.error));
		out.insert("tags".to_owned(), json!(self.tags));
		out.insert("returncode".to_owned(), returncode);
		Value::Object(out)
	}
}

/// In-memory sandbox registry plus create idempotency index.
#[derive(Debug, Default)]
pub struct Registry {
	records:     RwLock<HashMap<String, VmRecord>>,
	idempotency: RwLock<HashMap<String, String>>,
}

impl Registry {
	/// Create an empty registry.
	pub fn new() -> Self {
		Self::default()
	}

	/// Rebuild records from `$VMON_HOME/vms/<name>/meta.json`.
	///
	/// The returned `(record_name, volume_names)` entries are the volume locks
	/// the engine must re-acquire for records still backed by a live VMM
	/// process.
	pub fn rehydrate(&self, home: &Home) -> Result<VolumeLockRequests> {
		let vms_dir = home.vms_dir();
		if !vms_dir.is_dir() {
			self.replace(HashMap::new(), HashMap::new());
			return Ok(Vec::new());
		}

		let mut vm_dirs = Vec::new();
		for entry in fs::read_dir(&vms_dir)? {
			let entry = entry?;
			if entry.file_type()?.is_dir() {
				vm_dirs.push(entry.path());
			}
		}
		vm_dirs.sort_by(|left, right| left.file_name().cmp(&right.file_name()));

		let mut records = HashMap::new();
		let mut lock_requests = Vec::new();
		for dir in vm_dirs {
			let Some(name) = dir
				.file_name()
				.map(|name| name.to_string_lossy().into_owned())
			else {
				continue;
			};
			let meta_path = home.meta_path(&name);
			let mut meta = read_meta_map(&meta_path)?;
			let pid = pid_of(&meta);
			let running = pid.is_some_and(pid_lives);
			let saved_status = meta
				.get("status")
				.and_then(Value::as_str)
				.unwrap_or("")
				.to_owned();
			let status = if running {
				"running".to_owned()
			} else if matches!(saved_status.as_str(), "stopped" | "terminated") {
				saved_status.clone()
			} else {
				"stopped".to_owned()
			};
			let now = unix_time();
			let mut record = VmRecord {
				id: name.clone(),
				name: name.clone(),
				status: status.clone(),
				pid,
				source: source_of(&meta),
				created_at: now,
				timeout: timeout_of(&meta),
				detail: Value::Object(meta.clone()),
				tags: tags_of(&meta),
				last_active: now,
				terminated_at: None,
				error: None,
			};

			if running {
				let volume_names = volume_names(&meta);
				if !volume_names.is_empty() {
					lock_requests.push((name.clone(), volume_names));
				}
				if meta.get("tap").is_some_and(json_truthy) {
					detail_object_mut(&mut record.detail)
						.insert("tunnels_lost".to_owned(), Value::Bool(true));
				}
			} else if saved_status != status {
				detail_object_mut(&mut record.detail).insert("status".to_owned(), json!(status));
				meta.insert("status".to_owned(), json!(status));
				if let Err(_err) = write_meta_map(&meta_path, &meta) {
					// Python treats terminal-state persistence during rehydrate as
					// best effort.
				}
			}
			records.insert(name, record);
		}

		let mut idempotency = HashMap::new();
		for (name, record) in &records {
			if record.status == "terminated" {
				continue;
			}
			if let Some(key) = record
				.detail
				.get("idempotency_key")
				.and_then(Value::as_str)
				.filter(|key| !key.is_empty())
			{
				idempotency.insert(key.to_owned(), name.clone());
			}
		}
		self.replace(records, idempotency);
		Ok(lock_requests)
	}

	/// Return a cloned record by name.
	pub fn get(&self, name: &str) -> Option<VmRecord> {
		self.records.read().get(name).cloned()
	}

	/// Return all records sorted by name for deterministic callers.
	pub fn list(&self) -> Vec<VmRecord> {
		let mut records = self.records.read().values().cloned().collect::<Vec<_>>();
		records.sort_by(|left, right| left.name.cmp(&right.name));
		records
	}

	/// Return the sandbox name previously recorded for an idempotency key.
	pub fn find_by_idempotency_key(&self, key: &str) -> Option<String> {
		self.idempotency.read().get(key).cloned()
	}

	/// Record an idempotency key mapping for a sandbox name.
	pub fn record_idempotency(&self, name: &str, key: &str) {
		if !key.is_empty() {
			self
				.idempotency
				.write()
				.insert(key.to_owned(), name.to_owned());
		}
	}

	/// Insert or replace one record.
	pub fn insert(&self, record: VmRecord) {
		self.records.write().insert(record.name.clone(), record);
	}

	/// Remove one record and any idempotency entries pointing at it.
	pub fn remove(&self, name: &str) -> Option<VmRecord> {
		let removed = self.records.write().remove(name);
		if removed.is_some() {
			self.idempotency.write().retain(|_, sid| sid != name);
		}
		removed
	}

	/// Mutate one record in place and return a clone of the updated record.
	pub fn update<F>(&self, name: &str, update: F) -> Option<VmRecord>
	where
		F: FnOnce(&mut VmRecord),
	{
		let mut records = self.records.write();
		let record = records.get_mut(name)?;
		update(record);
		Some(record.clone())
	}

	/// Drop a stale idempotency mapping if it still points at `name`.
	pub fn remove_idempotency_for(&self, key: &str, name: &str) {
		let mut idempotency = self.idempotency.write();
		if idempotency.get(key).is_some_and(|sid| sid == name) {
			idempotency.remove(key);
		}
	}

	/// Replace or add one field in the durable detail object.
	pub fn set_detail_field(&self, name: &str, key: &str, value: Value) -> Option<VmRecord> {
		self.update(name, |record| {
			detail_object_mut(&mut record.detail).insert(key.to_owned(), value);
		})
	}

	/// Rebuild the idempotency index from current records.
	pub fn rebuild_idempotency_index(&self) {
		let records = self.records.read();
		let mut idempotency = HashMap::new();
		for (name, record) in records.iter() {
			if record.status == "terminated" {
				continue;
			}
			if let Some(key) = record
				.detail
				.get("idempotency_key")
				.and_then(Value::as_str)
				.filter(|key| !key.is_empty())
			{
				idempotency.insert(key.to_owned(), name.clone());
			}
		}
		*self.idempotency.write() = idempotency;
	}

	/// Update a record and persist terminal status into `meta.json`.
	pub fn persist_record_status(
		&self,
		home: &Home,
		name: &str,
		status: &str,
		returncode: Option<i64>,
		terminated_at: Option<f64>,
	) -> Result<()> {
		if let Some(record) = self.records.write().get_mut(name) {
			status.clone_into(&mut record.status);
			let detail = detail_object_mut(&mut record.detail);
			detail.insert("status".to_owned(), json!(status));
			if let Some(code) = returncode {
				detail.insert("returncode".to_owned(), json!(code));
			}
			if let Some(ts) = terminated_at {
				record.terminated_at = Some(ts);
				detail.insert("terminated_at".to_owned(), json!(ts));
			}
		}
		let mut meta = read_meta_map(&home.meta_path(name))?;
		meta.insert("status".to_owned(), json!(status));
		if let Some(code) = returncode {
			meta.insert("returncode".to_owned(), json!(code));
		}
		if let Some(ts) = terminated_at {
			meta.insert("terminated_at".to_owned(), json!(ts));
		}
		write_meta_map(&home.meta_path(name), &meta)
	}

	fn replace(&self, records: HashMap<String, VmRecord>, idempotency: HashMap<String, String>) {
		*self.records.write() = records;
		*self.idempotency.write() = idempotency;
	}
}

fn read_meta_map(path: &Path) -> Result<Map<String, Value>> {
	let text = match fs::read_to_string(path) {
		Ok(text) => text,
		Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Map::new()),
		Err(err) => return Err(err.into()),
	};
	let value = serde_json::from_str::<Value>(&text)
		.map_err(|err| EngineError::invalid(format!("could not parse {}: {err}", path.display())))?;
	value
		.as_object()
		.cloned()
		.ok_or_else(|| EngineError::invalid(format!("{} must contain a JSON object", path.display())))
}

fn write_meta_map(path: &Path, meta: &Map<String, Value>) -> Result<()> {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent)?;
	}
	let text = serde_json::to_string_pretty(&Value::Object(meta.clone()))?;
	fs::write(path, text)?;
	Ok(())
}

fn pid_of(meta: &Map<String, Value>) -> Option<i32> {
	let pid = meta.get("pid")?.as_i64()?;
	i32::try_from(pid).ok()
}

fn timeout_of(meta: &Map<String, Value>) -> Option<f64> {
	meta.get("timeout_secs")?.as_f64()
}

fn tags_of(meta: &Map<String, Value>) -> HashMap<String, String> {
	let Some(tags) = meta.get("tags").and_then(Value::as_object) else {
		return HashMap::new();
	};
	tags
		.iter()
		.filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_owned())))
		.collect()
}

fn source_of(meta: &Map<String, Value>) -> Option<String> {
	for key in ["image", "restored_from", "forked_from", "restored_snapshot"] {
		if let Some(source) = meta
			.get(key)
			.and_then(Value::as_str)
			.filter(|source| !source.is_empty())
		{
			return Some(source.to_owned());
		}
	}
	None
}

fn volume_names(meta: &Map<String, Value>) -> Vec<String> {
	let Some(volumes) = meta.get("volumes").and_then(Value::as_object) else {
		return Vec::new();
	};
	let mut writable = HashMap::<String, bool>::new();
	for info in volumes.values() {
		let Some(info) = info.as_object() else {
			continue;
		};
		let Some(name) = info
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
		else {
			continue;
		};
		let read_only = info
			.get("read_only")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		writable.insert(name.to_owned(), writable.get(name).copied().unwrap_or(false) || !read_only);
	}
	let mut out = writable
		.into_iter()
		.filter_map(|(name, writable)| writable.then_some(name))
		.collect::<Vec<_>>();
	out.sort();
	out
}

fn detail_object_mut(detail: &mut Value) -> &mut Map<String, Value> {
	if !detail.is_object() {
		*detail = Value::Object(Map::new());
	}
	detail
		.as_object_mut()
		.expect("detail was just initialized as an object")
}

fn json_truthy(value: &Value) -> bool {
	match value {
		Value::Null => false,
		Value::Bool(value) => *value,
		Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
		Value::String(value) => !value.is_empty(),
		Value::Array(value) => !value.is_empty(),
		Value::Object(value) => !value.is_empty(),
	}
}

fn pid_lives(pid: i32) -> bool {
	if pid <= 0 {
		return false;
	}
	// SAFETY: `kill(pid, 0)` probes liveness without delivering a signal and does
	// not retain pointers into Rust memory.
	unsafe { libc::kill(pid, 0) == 0 }
}

fn unix_time() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn view_spreads_detail_then_overrides_canonical_keys() {
		let mut record = VmRecord::new("canonical-id", "canonical-name", "running");
		record.created_at = 10.0;
		record.last_active = 20.0;
		record.timeout = Some(5.0);
		record.tags = HashMap::from([("role".to_owned(), "api".to_owned())]);
		record.detail = json!({
			"id": "bad-id",
			"name": "bad-name",
			"status": "bad-status",
			"created_at": -1,
			"last_active": -1,
			"expires_at": -1,
			"terminated_at": 123,
			"error": "bad-error",
			"tags": {"bad": "tag"},
			"returncode": 7,
			"idempotency_key": "secret",
			"extra": "kept"
		});

		let view = record.view();
		assert_eq!(view["id"], "canonical-id");
		assert_eq!(view["name"], "canonical-name");
		assert_eq!(view["status"], "running");
		assert_eq!(view["created_at"], 10.0);
		assert_eq!(view["last_active"], 20.0);
		assert_eq!(view["expires_at"], 25.0);
		assert_eq!(view["terminated_at"], Value::Null);
		assert_eq!(view["error"], Value::Null);
		assert_eq!(view["tags"], json!({"role": "api"}));
		assert_eq!(view["returncode"], 7);
		assert_eq!(view["extra"], "kept");
		assert!(view.get("idempotency_key").is_none());
	}

	#[test]
	fn rehydrate_reads_python_meta_and_rebuilds_indexes() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		write_fixture(
			&home,
			"dead",
			json!({
				"status": "running",
				"pid": i32::MAX,
				"image": "alpine",
				"timeout_secs": 42,
				"tags": {"role": "test", "ignored": 5},
				"idempotency_key": "idem-dead",
				"volumes": {"/data": {"name": "data"}}
			}),
		);
		write_fixture(
			&home,
			"terminated",
			json!({
				"status": "terminated",
				"pid": i32::MAX,
				"idempotency_key": "idem-terminated"
			}),
		);
		write_fixture(
			&home,
			"live",
			json!({
				"status": "stopped",
				"pid": i32::try_from(std::process::id()).expect("pid fits i32"),
				"forked_from": "base-snap",
				"tap": "tap0",
				"idempotency_key": "idem-live",
				"volumes": {
					"/data": {"name": "data"},
					"/logs": {"name": "logs", "read_only": true}
				}
			}),
		);

		let registry = Registry::new();
		let locks = registry.rehydrate(&home).expect("rehydrate");
		assert_eq!(locks, vec![("live".to_owned(), vec!["data".to_owned()])]);

		let dead = registry.get("dead").expect("dead record");
		assert_eq!(dead.status, "stopped");
		assert_eq!(dead.pid, Some(i32::MAX));
		assert_eq!(dead.source.as_deref(), Some("alpine"));
		assert_eq!(dead.timeout, Some(42.0));
		assert_eq!(dead.tags, HashMap::from([("role".to_owned(), "test".to_owned())]));
		assert_eq!(registry.find_by_idempotency_key("idem-dead"), Some("dead".to_owned()));
		let dead_meta = read_meta_map(&home.meta_path("dead")).expect("dead meta");
		assert_eq!(dead_meta.get("status"), Some(&json!("stopped")));

		let terminated = registry.get("terminated").expect("terminated record");
		assert_eq!(terminated.status, "terminated");
		assert_eq!(registry.find_by_idempotency_key("idem-terminated"), None);

		let live = registry.get("live").expect("live record");
		assert_eq!(live.status, "running");
		assert_eq!(live.source.as_deref(), Some("base-snap"));
		assert_eq!(live.detail["tunnels_lost"], true);
		assert_eq!(registry.find_by_idempotency_key("idem-live"), Some("live".to_owned()));
	}

	fn write_fixture(home: &Home, name: &str, value: Value) {
		let path = home.meta_path(name);
		fs::create_dir_all(path.parent().expect("meta parent")).expect("mkdir");
		fs::write(path, serde_json::to_string_pretty(&value).expect("fixture json"))
			.expect("write fixture");
	}
}
