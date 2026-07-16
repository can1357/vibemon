//! Append-only, non-secret security audit records.

use std::{
	collections::BTreeMap,
	fs::{self, OpenOptions},
	io::Write,
	os::unix::fs::{OpenOptionsExt, PermissionsExt},
	path::PathBuf,
	sync::Arc,
};

use parking_lot::Mutex;
use serde::Serialize;

use crate::{EngineError, Result, home::Home};

/// One credential, authorization, lifecycle, or key-management audit event.
#[derive(Clone, Debug, Serialize)]
pub struct AuditEvent {
	/// Unix timestamp in milliseconds.
	pub timestamp_unix_millis: u64,
	/// Authenticated tenant that initiated the operation.
	pub tenant:                String,
	/// Stable actor identity, never a bearer token.
	pub actor:                 String,
	/// Stable operation name.
	pub action:                String,
	/// Non-secret resource identifier.
	pub resource:              String,
	/// `attempted`, `allowed`, `denied`, `succeeded`, or `failed`.
	pub outcome:               String,
	/// Additional non-secret structured attributes.
	pub attributes:            BTreeMap<String, String>,
}

impl AuditEvent {
	/// Build an event using the current wall-clock timestamp.
	pub fn new(
		tenant: impl Into<String>,
		actor: impl Into<String>,
		action: impl Into<String>,
		resource: impl Into<String>,
		outcome: impl Into<String>,
	) -> Self {
		Self {
			timestamp_unix_millis: unix_millis(),
			tenant:                tenant.into(),
			actor:                 actor.into(),
			action:                action.into(),
			resource:              resource.into(),
			outcome:               outcome.into(),
			attributes:            BTreeMap::new(),
		}
	}

	/// Attach a non-secret attribute.
	pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
		self.attributes.insert(key.into(), value.into());
		self
	}
}

/// Serialized writer for `$VMON_HOME/security/audit.jsonl`.
#[derive(Clone, Debug)]
pub struct AuditLog {
	path: Arc<PathBuf>,
	lock: Arc<Mutex<()>>,
}

impl AuditLog {
	/// Open an audit sink, creating its private parent directory.
	pub fn open(home: &Home) -> Result<Self> {
		let path = home.audit_log();
		let parent = path
			.parent()
			.ok_or_else(|| EngineError::invalid("audit log path has no parent"))?;
		fs::create_dir_all(parent)?;
		fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
		if let Ok(metadata) = fs::symlink_metadata(&path)
			&& (metadata.file_type().is_symlink() || !metadata.is_file())
		{
			return Err(EngineError::invalid(format!(
				"audit log {} must be a regular file",
				path.display()
			)));
		}
		Ok(Self { path: Arc::new(path), lock: Arc::new(Mutex::new(())) })
	}

	/// Durably append one JSON event without logging secret material.
	pub fn record(&self, event: &AuditEvent) -> Result<()> {
		let encoded = serde_json::to_vec(event)?;
		let _guard = self.lock.lock();
		let mut file = OpenOptions::new()
			.create(true)
			.append(true)
			.mode(0o600)
			.open(self.path.as_ref())?;
		file.write_all(&encoded)?;
		file.write_all(b"\n")?;
		file.sync_data()?;
		Ok(())
	}
}

fn unix_millis() -> u64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
	use std::fs;

	use tempfile::TempDir;

	use super::{AuditEvent, AuditLog};
	use crate::home::Home;

	#[test]
	fn audit_log_appends_structured_non_secret_events() {
		let temp = TempDir::new().unwrap();
		let home = Home::new(temp.path());
		let log = AuditLog::open(&home).unwrap();
		log.record(
			&AuditEvent::new("tenant-a", "client", "credential.use", "github", "allowed")
				.with_attribute("domain", "api.github.com"),
		)
		.unwrap();
		let text = fs::read_to_string(home.audit_log()).unwrap();
		assert!(text.contains("credential.use"));
		assert!(text.contains("api.github.com"));
	}
}
