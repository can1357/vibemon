//! `$VMON_HOME` layout and single-owner lock. Port of python/vmon/daemon.py
//! paths.

use std::{
	collections::HashSet,
	env,
	fs::{self, File, OpenOptions},
	io,
	os::unix::{fs::OpenOptionsExt, io::AsRawFd},
	path::{Path, PathBuf},
	sync::{LazyLock, Mutex},
};

use crate::{EngineError, Result};

static HELD_OWNER_LOCKS: LazyLock<Mutex<HashSet<PathBuf>>> =
	LazyLock::new(|| Mutex::new(HashSet::new()));

/// Return `$VMON_HOME`, or `$HOME/.vmon` when it is unset.
pub fn state_dir() -> PathBuf {
	if let Some(home) = env::var_os("VMON_HOME") {
		return PathBuf::from(home);
	}
	env::var_os("HOME")
		.map_or_else(|| PathBuf::from(".vmon"), |home| PathBuf::from(home).join(".vmon"))
}

/// Filesystem layout rooted at one Vibemon home directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Home {
	root: PathBuf,
}

impl Home {
	/// Build path helpers for a specific Vibemon home directory.
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self { root: root.into() }
	}

	/// Return the root directory for this home.
	pub fn root(&self) -> &Path {
		&self.root
	}

	/// Return `$VMON_HOME/vms`.
	pub fn vms_dir(&self) -> PathBuf {
		self.root.join("vms")
	}

	/// Return `$VMON_HOME/vms/<name>`.
	pub fn vm_dir(&self, name: &str) -> PathBuf {
		self.vms_dir().join(name)
	}

	/// Return `$VMON_HOME/vms/<name>/meta.json`.
	pub fn meta_path(&self, name: &str) -> PathBuf {
		self.vm_dir(name).join("meta.json")
	}

	/// Return `$VMON_HOME/volumes`.
	pub fn volumes_dir(&self) -> PathBuf {
		self.root.join("volumes")
	}

	/// Return `$VMON_HOME/assets`.
	pub fn assets_dir(&self) -> PathBuf {
		self.root.join("assets")
	}

	/// Return `$VMON_HOME/images`.
	pub fn images_dir(&self) -> PathBuf {
		self.root.join("images")
	}

	/// Return `$VMON_HOME/templates`.
	pub fn templates_dir(&self) -> PathBuf {
		self.root.join("templates")
	}

	/// Return `$VMON_HOME/cas`.
	pub fn cas_dir(&self) -> PathBuf {
		self.root.join("cas")
	}

	/// Return `$VMON_HOME/functions`, the durable function runtime root.
	pub fn functions_dir(&self) -> PathBuf {
		self.root.join("functions")
	}

	/// Return the durable function metadata database.
	pub fn functions_db(&self) -> PathBuf {
		self.functions_dir().join("state.sqlite3")
	}

	/// Return the content-addressed function artifact directory.
	pub fn function_artifacts_dir(&self) -> PathBuf {
		self.functions_dir().join("artifacts")
	}

	/// Return `$VMON_HOME/records`.
	pub fn records_dir(&self) -> PathBuf {
		self.root.join("records")
	}

	/// Return `$VMON_HOME/replicas`.
	pub fn replicas_dir(&self) -> PathBuf {
		self.root.join("replicas")
	}

	/// Return `$VMON_HOME/leases`.
	pub fn leases_dir(&self) -> PathBuf {
		self.root.join("leases")
	}

	/// Return `$VMON_HOME/network`.
	pub fn network_dir(&self) -> PathBuf {
		self.root.join("network")
	}

	/// Return `$VMON_HOME/security`.
	pub fn security_dir(&self) -> PathBuf {
		self.root.join("security")
	}

	/// Return the customer-managed encryption-key directory.
	pub fn keys_dir(&self) -> PathBuf {
		self.security_dir().join("keys")
	}

	/// Return the encrypted credential-record directory.
	pub fn credentials_dir(&self) -> PathBuf {
		self.security_dir().join("credentials")
	}

	/// Return the append-only security audit log.
	pub fn audit_log(&self) -> PathBuf {
		self.security_dir().join("audit.jsonl")
	}

	/// Return `$VMON_HOME/vmond.sock`.
	pub fn vmond_sock(&self) -> PathBuf {
		self.root.join("vmond.sock")
	}

	/// Return `$VMON_HOME/vmond.lock`.
	pub fn vmond_lock(&self) -> PathBuf {
		self.root.join("vmond.lock")
	}

	/// Return `$VMON_HOME/vmond.pid`.
	pub fn vmond_pid(&self) -> PathBuf {
		self.root.join("vmond.pid")
	}

	/// Return `$VMON_HOME/vmond.log`.
	pub fn vmond_log(&self) -> PathBuf {
		self.root.join("vmond.log")
	}
}

impl Default for Home {
	fn default() -> Self {
		Self::new(state_dir())
	}
}

/// Held exclusive flock proving this process owns a Vibemon home.
#[derive(Debug)]
pub struct OwnerLock {
	file: File,
	key:  PathBuf,
	path: PathBuf,
}

impl OwnerLock {
	/// Acquire `vmond.lock` with a non-blocking exclusive flock.
	pub fn acquire(home: &Home) -> Result<Self> {
		fs::create_dir_all(home.root())?;
		let path = home.vmond_lock();
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.mode(0o600)
			.open(&path)?;
		let key = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
		let mut held = HELD_OWNER_LOCKS
			.lock()
			.map_err(|_| EngineError::engine("owner-lock registry poisoned"))?;
		if held.contains(&key) {
			return Err(Self::busy_error(home));
		}
		// SAFETY: `file.as_raw_fd()` is an open file descriptor and `flock` does not
		// retain pointers into Rust memory.
		let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		if rc != 0 {
			let err = io::Error::last_os_error();
			if matches!(err.raw_os_error(), Some(code) if lock_would_block(code)) {
				return Err(Self::busy_error(home));
			}
			return Err(err.into());
		}
		held.insert(key.clone());
		Ok(Self { file, key, path })
	}

	/// Return the lock-file path backing this owner lock.
	pub fn path(&self) -> &Path {
		&self.path
	}

	fn busy_error(home: &Home) -> EngineError {
		EngineError::busy(format!("another vmon server already owns {}", home.root().display()))
	}
}

impl Drop for OwnerLock {
	fn drop(&mut self) {
		// SAFETY: `self.file.as_raw_fd()` remains open for the duration of `drop` and
		// `flock` does not retain pointers into Rust memory.
		let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
		if let Ok(mut held) = HELD_OWNER_LOCKS.lock() {
			held.remove(&self.key);
		}
	}
}

const fn lock_would_block(code: i32) -> bool {
	code == libc::EACCES || code == libc::EAGAIN
}

/// Serialize and scope `VMON_HOME` mutation across test modules.
///
/// Every test that touches env-resolved state (CAS pointers, template
/// installs) MUST hold this guard; an unsynchronized `std::env::set_var`
/// races the whole test process.
#[cfg(test)]
pub(crate) mod test_home {
	use std::{
		ffi::OsString,
		path::Path,
		sync::{Mutex, MutexGuard, PoisonError},
	};

	static VMON_HOME_LOCK: Mutex<()> = Mutex::new(());

	/// Hold the process-wide env-mutation lock without changing `VMON_HOME`.
	/// Test harnesses that mutate other env keys (config resolution) share
	/// this lock so no two harnesses race the same process environment.
	pub fn lock() -> MutexGuard<'static, ()> {
		VMON_HOME_LOCK
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}

	/// RAII guard holding the process-wide lock with `VMON_HOME` pointed at a
	/// test home; restores the previous value on drop.
	pub struct HomeGuard {
		_lock:    MutexGuard<'static, ()>,
		previous: Option<OsString>,
	}

	/// Point `VMON_HOME` at `home` for the lifetime of the returned guard.
	pub fn set(home: &Path) -> HomeGuard {
		let lock = lock();
		let previous = std::env::var_os("VMON_HOME");
		// SAFETY: VMON_HOME mutation is serialized by VMON_HOME_LOCK and the
		// previous value is restored before the lock is released.
		unsafe { std::env::set_var("VMON_HOME", home) };
		HomeGuard { _lock: lock, previous }
	}

	impl Drop for HomeGuard {
		fn drop(&mut self) {
			match &self.previous {
				// SAFETY: still serialized by the held VMON_HOME_LOCK.
				Some(value) => unsafe { std::env::set_var("VMON_HOME", value) },
				// SAFETY: still serialized by the held VMON_HOME_LOCK.
				None => unsafe { std::env::remove_var("VMON_HOME") },
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ErrorCode;

	#[test]
	fn owner_lock_is_exclusive() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let first = OwnerLock::acquire(&home).expect("first lock");
		let err = OwnerLock::acquire(&home).expect_err("second lock must fail");
		assert_eq!(err.code, ErrorCode::Busy);
		assert!(err.message.contains("already owns"));
		drop(first);
		let third = OwnerLock::acquire(&home).expect("lock after drop");
		assert_eq!(third.path(), home.vmond_lock());
	}

	#[test]
	fn home_paths_match_python_daemon_names() {
		let home = Home::new("/tmp/vmon-home");
		assert_eq!(home.vms_dir(), PathBuf::from("/tmp/vmon-home/vms"));
		assert_eq!(home.vm_dir("box"), PathBuf::from("/tmp/vmon-home/vms/box"));
		assert_eq!(home.meta_path("box"), PathBuf::from("/tmp/vmon-home/vms/box/meta.json"));
		assert_eq!(home.vmond_sock(), PathBuf::from("/tmp/vmon-home/vmond.sock"));
		assert_eq!(home.vmond_lock(), PathBuf::from("/tmp/vmon-home/vmond.lock"));
		assert_eq!(home.vmond_pid(), PathBuf::from("/tmp/vmon-home/vmond.pid"));
		assert_eq!(home.vmond_log(), PathBuf::from("/tmp/vmon-home/vmond.log"));
	}
}
