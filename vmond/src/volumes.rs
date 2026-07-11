//! Named volumes and request-scoped secrets. Port of python/vmon/volume.py +
//! secret.py.

use std::{
	collections::BTreeMap,
	ffi::CString,
	fmt,
	fs::{self, File, OpenOptions, Permissions},
	io,
	os::{
		fd::{AsRawFd, FromRawFd},
		unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
	},
	path::{Path, PathBuf},
};

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::error::{EngineError, Result};

const VOLUME_NAME_PATTERN: &str = "^[a-z0-9_][a-z0-9_.-]{0,63}$";
const VOLUME_DIR_MODE: u32 = 0o700;
const LOCK_FILE_MODE: u32 = 0o600;
const LOCK_FILE_NAME: &str = ".lock";

/// A persistent named host directory shared into guests by the engine.
#[derive(Clone, Debug)]
pub struct Volume {
	name:     String,
	host_dir: PathBuf,
}

impl Volume {
	/// Validate a name and ensure its private host directory exists.
	pub fn new(name: &str) -> Result<Self> {
		Self::new_in_home(crate::home::state_dir(), name)
	}

	/// Validate a name and ensure its private host directory exists under
	/// `home`.
	pub fn new_in_home(home: impl AsRef<Path>, name: &str) -> Result<Self> {
		validate_volume_name(name)?;
		let volumes_dir = home.as_ref().join("volumes");
		let host_dir = volumes_dir.join(name);
		ensure_private_dir(&volumes_dir, true)?;
		ensure_private_dir(&host_dir, false)?;
		Ok(Self { name: name.to_owned(), host_dir })
	}

	/// The stable volume name.
	pub fn name(&self) -> &str {
		&self.name
	}

	/// The private host directory backing this volume.
	pub fn path(&self) -> PathBuf {
		self.host_dir.clone()
	}

	/// Take the host-local single-writer lock for a writable mount.
	pub fn acquire_write_lock(&self) -> Result<VolumeLock> {
		ensure_private_dir(
			self.host_dir.parent().ok_or_else(|| {
				EngineError::engine(format!("volume path {} has no parent", self.host_dir.display()))
			})?,
			true,
		)?;
		ensure_private_dir(&self.host_dir, false)?;
		let dir = open_volume_dir(&self.host_dir)?;
		let lock_file = open_lock_file(&dir, &self.name)?;
		lock_exclusive_nonblocking(&lock_file, &self.name)?;
		Ok(VolumeLock { file: lock_file })
	}
}

/// An exclusive writable-volume flock released automatically on drop.
#[derive(Debug)]
pub struct VolumeLock {
	file: File,
}

impl Drop for VolumeLock {
	fn drop(&mut self) {
		// SAFETY: `self.file` owns a live file descriptor; `flock` does not retain
		// pointers and unlocking during drop is best-effort cleanup.
		let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
	}
}

/// Names of existing safe volume directories under `home/volumes`.
pub fn list_volumes(home: impl AsRef<Path>) -> Vec<String> {
	let volume_dir = home.as_ref().join("volumes");
	let Ok(meta) = fs::symlink_metadata(&volume_dir) else {
		return Vec::new();
	};
	if meta.file_type().is_symlink() || !meta.is_dir() {
		return Vec::new();
	}

	let Ok(entries) = fs::read_dir(volume_dir) else {
		return Vec::new();
	};
	let mut names = Vec::new();
	for entry in entries.flatten() {
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		if file_type.is_symlink() || !file_type.is_dir() {
			continue;
		}
		let Ok(name) = entry.file_name().into_string() else {
			continue;
		};
		if is_valid_volume_name(&name) {
			names.push(name);
		}
	}
	names.sort();
	names
}

/// Remove an unlocked volume directory from the default `$VMON_HOME`.
pub fn remove_volume(name: &str) -> Result<()> {
	remove_volume_in_home(crate::home::state_dir(), name)
}

/// Remove an unlocked volume directory from an explicit Vibemon home.
pub fn remove_volume_in_home(home: impl AsRef<Path>, name: &str) -> Result<()> {
	validate_volume_name(name)?;
	let host_dir = home.as_ref().join("volumes").join(name);
	match fs::symlink_metadata(&host_dir) {
		Ok(_) => {},
		Err(err) if err.kind() == io::ErrorKind::NotFound => {
			return Err(EngineError::not_found(format!("unknown volume '{name}'")));
		},
		Err(err) => return Err(err.into()),
	}

	let dir = open_volume_dir(&host_dir)?;
	let lock_file = open_lock_file(&dir, name)?;
	lock_exclusive_nonblocking(&lock_file, name)?;
	fs::remove_dir_all(&host_dir)?;
	Ok(())
}

/// Memory-only secret bundle for per-exec environment injection.
///
/// Secret values must never be serialized into registry or replica state. Use
/// [`Secret::redacted_json`] for durable records and [`Secret::env_pairs`] only
/// at the point where values are injected into a guest exec request.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret {
	/// Human-readable in-memory bundle name.
	pub name:   String,
	/// Environment variables, sorted by key; values are memory-only.
	pub values: BTreeMap<String, String>,
}

impl Secret {
	/// Build a memory-only secret bundle after Python-compatible validation.
	pub fn new(name: impl Into<String>, values: BTreeMap<String, String>) -> Result<Self> {
		let name = name.into();
		validate_secret_env_name(&name)?;
		let mut checked = BTreeMap::new();
		for (key, value) in values {
			validate_secret_env_name(&key)?;
			validate_secret_env_value(&value)?;
			checked.insert(key, value);
		}
		Ok(Self { name, values: checked })
	}

	/// Parse the SDK wire shape `{ "name": string, "values": { env: value } }`.
	pub fn from_wire(value: JsonValue) -> Result<Self> {
		let JsonValue::Object(mut object) = value else {
			return Err(EngineError::invalid("secret must be an object"));
		};
		let name = object
			.remove("name")
			.and_then(|value| match value {
				JsonValue::String(text) => Some(text),
				_ => None,
			})
			.ok_or_else(|| EngineError::invalid("secret name must be a string"))?;
		let values = object
			.remove("values")
			.ok_or_else(|| EngineError::invalid("secret values are required"))?;
		let JsonValue::Object(values) = values else {
			return Err(EngineError::invalid("secret values must be an object"));
		};

		let mut env = BTreeMap::new();
		for (key, value) in values {
			validate_secret_env_name(&key)?;
			let value = json_env_value_to_string(value);
			validate_secret_env_value(&value)?;
			env.insert(key, value);
		}
		Self::new(name, env)
	}

	/// Safe record JSON: bundle name and env keys only, never secret values.
	pub fn redacted_json(&self) -> JsonValue {
		let values = self
			.values
			.keys()
			.map(|key| (key.clone(), JsonValue::String("<redacted>".to_owned())))
			.collect::<JsonMap<_, _>>();
		let mut object = JsonMap::new();
		object.insert("name".to_owned(), JsonValue::String(self.name.clone()));
		object.insert("values".to_owned(), JsonValue::Object(values));
		JsonValue::Object(object)
	}

	/// Sorted environment pairs for explicit guest exec injection.
	pub fn env_pairs(&self) -> Vec<(String, String)> {
		self
			.values
			.iter()
			.map(|(key, value)| (key.clone(), value.clone()))
			.collect()
	}
}

impl fmt::Debug for Secret {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let names = self.values.keys().collect::<Vec<_>>();
		f.debug_struct("Secret")
			.field("name", &self.name)
			.field("env_names", &names)
			.finish()
	}
}

fn validate_volume_name(name: &str) -> Result<()> {
	if is_valid_volume_name(name) {
		Ok(())
	} else {
		Err(EngineError::invalid(format!(
			"invalid volume name '{name}': must match {VOLUME_NAME_PATTERN}"
		)))
	}
}

fn is_valid_volume_name(name: &str) -> bool {
	let bytes = name.as_bytes();
	if bytes.is_empty() || bytes.len() > 64 {
		return false;
	}
	is_volume_name_start(bytes[0]) && bytes[1..].iter().all(|byte| is_volume_name_rest(*byte))
}

const fn is_volume_name_start(byte: u8) -> bool {
	byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
}

const fn is_volume_name_rest(byte: u8) -> bool {
	is_volume_name_start(byte) || byte == b'.' || byte == b'-'
}

fn ensure_private_dir(path: &Path, parents: bool) -> Result<()> {
	let mut builder = fs::DirBuilder::new();
	builder.mode(VOLUME_DIR_MODE);
	builder.recursive(parents);
	match builder.create(path) {
		Ok(()) => {},
		Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
			if !path.metadata().is_ok_and(|meta| meta.is_dir()) {
				return Err(EngineError::invalid(format!(
					"unsafe volume path {}: not a directory",
					path.display()
				)));
			}
		},
		Err(err) => return Err(err.into()),
	}

	let dir = open_dir_no_follow(path).map_err(|_| {
		EngineError::invalid(format!("unsafe volume path {}: not a real directory", path.display()))
	})?;
	dir.set_permissions(Permissions::from_mode(VOLUME_DIR_MODE))?;
	Ok(())
}

fn open_volume_dir(path: &Path) -> Result<File> {
	open_dir_no_follow(path).map_err(|_| {
		EngineError::invalid(format!("unsafe volume path {}: not a real directory", path.display()))
	})
}

fn open_dir_no_follow(path: &Path) -> io::Result<File> {
	OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
		.open(path)
}

fn open_lock_file(dir: &File, name: &str) -> Result<File> {
	let lock_name = CString::new(LOCK_FILE_NAME).expect("static lock filename contains no NUL");
	// SAFETY: `dir` is an open directory descriptor and `lock_name` is a valid
	// NUL-terminated C string. `openat` returns a new owned descriptor on success.
	let fd = unsafe {
		libc::openat(
			dir.as_raw_fd(),
			lock_name.as_ptr(),
			libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
			LOCK_FILE_MODE,
		)
	};
	if fd < 0 {
		return Err(unsafe_volume_lock(name));
	}
	// SAFETY: `fd` was returned by `openat` above and is uniquely owned here.
	let file = unsafe { File::from_raw_fd(fd) };
	if !file.metadata()?.file_type().is_file() {
		return Err(unsafe_volume_lock(name));
	}
	file.set_permissions(Permissions::from_mode(LOCK_FILE_MODE))?;
	Ok(file)
}

fn lock_exclusive_nonblocking(file: &File, name: &str) -> Result<()> {
	// SAFETY: `file` owns a live file descriptor; `flock` takes no Rust pointers
	// and only changes the kernel lock state for that descriptor.
	let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
	if rc == 0 {
		return Ok(());
	}
	let err = io::Error::last_os_error();
	match err.raw_os_error() {
		Some(code) if code == libc::EACCES || code == libc::EAGAIN || code == libc::EWOULDBLOCK => {
			Err(EngineError::busy(format!("volume '{name}' is already in use by another VM")))
		},
		_ => Err(err.into()),
	}
}

fn unsafe_volume_lock(name: &str) -> EngineError {
	EngineError::invalid(format!("unsafe volume lock for '{name}': not a regular file"))
}

fn validate_secret_env_name(name: &str) -> Result<()> {
	if name.is_empty() || name.contains('=') || name.contains('\0') {
		Err(EngineError::invalid(
			"secret environment names must be non-empty and contain no '=' or NUL",
		))
	} else {
		Ok(())
	}
}

fn validate_secret_env_value(value: &str) -> Result<()> {
	if value.contains('\0') {
		Err(EngineError::invalid("secret environment values must contain no NUL bytes"))
	} else {
		Ok(())
	}
}

fn json_env_value_to_string(value: JsonValue) -> String {
	match value {
		JsonValue::String(text) => text,
		other => other.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use std::os::unix::fs::{PermissionsExt, symlink};

	use serde_json::json;
	use tempfile::TempDir;

	use super::*;
	use crate::error::ErrorCode;

	#[test]
	fn volumes_name_validation_matrix() {
		let temp = TempDir::new().expect("create temp home");
		let max = "a".repeat(64);
		for name in ["a", "z9_", "a.b-c", max.as_str()] {
			let volume = Volume::new_in_home(temp.path(), name).expect("valid volume name");
			assert_eq!(volume.name(), name);
			assert_eq!(volume.path(), temp.path().join("volumes").join(name));
		}

		for name in ["", "-bad", ".bad", "Upper", "../x", "a/b"] {
			let err = Volume::new_in_home(temp.path(), name).expect_err("invalid volume name");
			assert_eq!(err.code, ErrorCode::Invalid);
			assert!(err.message.contains("must match"));
		}
		let too_long = "a".repeat(65);
		let err = Volume::new_in_home(temp.path(), &too_long).expect_err("too long");
		assert_eq!(err.code, ErrorCode::Invalid);
	}

	#[test]
	fn volumes_create_private_directories_0700() {
		let temp = TempDir::new().expect("create temp home");
		let volume = Volume::new_in_home(temp.path(), "data").expect("create volume");

		let root_mode = fs::metadata(temp.path().join("volumes"))
			.expect("volumes root metadata")
			.permissions()
			.mode()
			& 0o777;
		let volume_mode = fs::metadata(volume.path())
			.expect("volume metadata")
			.permissions()
			.mode()
			& 0o777;
		assert_eq!(root_mode, 0o700);
		assert_eq!(volume_mode, 0o700);
		assert_eq!(list_volumes(temp.path()), vec!["data".to_owned()]);
	}

	#[test]
	fn volumes_reject_symlink_volume_components() {
		let temp = TempDir::new().expect("create temp home");
		let real = temp.path().join("real-volumes");
		fs::create_dir(&real).expect("create symlink target");
		symlink(&real, temp.path().join("volumes")).expect("symlink volumes root");

		let err = Volume::new_in_home(temp.path(), "data").expect_err("symlink root refused");
		assert_eq!(err.code, ErrorCode::Invalid);
		assert!(err.message.contains("not a real directory"));
	}

	#[test]
	fn volumes_write_lock_exclusive_across_instances() {
		let temp = TempDir::new().expect("create temp home");
		let first = Volume::new_in_home(temp.path(), "shared").expect("first volume");
		let second = Volume::new_in_home(temp.path(), "shared").expect("second volume");

		let lock = first.acquire_write_lock().expect("first lock");
		let err = second
			.acquire_write_lock()
			.expect_err("second lock refused");
		assert_eq!(err.code, ErrorCode::Busy);
		assert_eq!(err.message, "volume 'shared' is already in use by another VM");

		drop(lock);
		let _lock = second.acquire_write_lock().expect("lock after drop");
	}

	#[test]
	fn volumes_remove_refuses_locked_volume() {
		let temp = TempDir::new().expect("create temp home");
		let volume = Volume::new_in_home(temp.path(), "data").expect("create volume");
		fs::write(volume.path().join("payload.txt"), "kept").expect("write payload");
		let lock = volume.acquire_write_lock().expect("lock volume");

		let err = remove_volume_in_home(temp.path(), "data").expect_err("locked remove refused");
		assert_eq!(err.code, ErrorCode::Busy);
		assert_eq!(err.message, "volume 'data' is already in use by another VM");
		assert!(volume.path().exists());

		drop(lock);
		remove_volume_in_home(temp.path(), "data").expect("remove unlocked volume");
		assert!(!volume.path().exists());
	}

	#[test]
	fn volumes_secret_wire_redaction_and_sorted_env_pairs() {
		let secret = Secret::from_wire(json!({
			"name": "bundle",
			"values": {
				"B": "2",
				"A": "1"
			}
		}))
		.expect("parse secret");

		assert_eq!(secret.name, "bundle");
		assert_eq!(secret.env_pairs(), vec![
			("A".to_owned(), "1".to_owned()),
			("B".to_owned(), "2".to_owned())
		]);
		let redacted = secret.redacted_json();
		assert_eq!(
			redacted,
			json!({
				"name": "bundle",
				"values": {
					"A": "<redacted>",
					"B": "<redacted>"
				}
			})
		);
		assert!(!redacted.to_string().contains('1'));
		assert!(!redacted.to_string().contains('2'));
	}
}
