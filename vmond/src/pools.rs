//! Warm pools: ready clones + background refiller. Port of python/vmon/pool.py.

use std::{
	collections::{BTreeMap, VecDeque},
	fs,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;

use crate::{
	Result,
	engine::{
		agent::AgentConn,
		spawn::{LaunchSpec, MicroVm},
	},
};

const REFILL_POLL: Duration = Duration::from_millis(100);
const DEFAULT_PING_TIMEOUT: Duration = Duration::from_secs(30);
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// Format the portable template key used by placement and pool APIs.
pub fn template_key(
	reference: &str,
	disk_mb: u64,
	memory: u64,
	cpus: u64,
	fs_slots: u64,
	host_slot: bool,
	nic_slot: bool,
	tap_slot: bool,
) -> String {
	format!(
		"{reference}|d{disk_mb}|m{memory}|c{cpus}|s{fs_slots}|h{}|n{}|t{}",
		u8::from(host_slot),
		u8::from(nic_slot),
		u8::from(tap_slot),
	)
}

/// One warm-pool statistics snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolStats {
	pub ready:  usize,
	pub hits:   u64,
	pub misses: u64,
	pub size:   usize,
}

/// A background-filled deque of restored/forked, agent-pinged VMs.
pub struct WarmPool {
	template:     PathBuf,
	size:         AtomicUsize,
	fork:         bool,
	agent:        bool,
	ping_timeout: Duration,
	ready:        Mutex<VecDeque<MicroVm>>,
	stop:         AtomicBool,
	hits:         AtomicU64,
	misses:       AtomicU64,
	refiller:     Mutex<Option<JoinHandle<()>>>,
}

impl WarmPool {
	/// Start a refiller for `template` with `size` parked VMs.
	pub fn new(template: impl Into<PathBuf>, size: usize) -> Result<Arc<Self>> {
		Self::with_options(template, size, true, true, DEFAULT_PING_TIMEOUT)
	}

	/// Start a configurable refiller, mostly used by tests and restore-only
	/// pools.
	pub fn with_options(
		template: impl Into<PathBuf>,
		size: usize,
		fork: bool,
		agent: bool,
		ping_timeout: Duration,
	) -> Result<Arc<Self>> {
		let pool = Arc::new(Self {
			template: template.into(),
			size: AtomicUsize::new(size),
			fork,
			agent,
			ping_timeout,
			ready: Mutex::new(VecDeque::new()),
			stop: AtomicBool::new(false),
			hits: AtomicU64::new(0),
			misses: AtomicU64::new(0),
			refiller: Mutex::new(None),
		});
		let thread_pool = Arc::clone(&pool);
		let handle = thread::Builder::new()
			.name("vmon-warmpool-refiller".to_owned())
			.spawn(move || thread_pool.refill_loop())?;
		*pool.refiller.lock() = Some(handle);
		Ok(pool)
	}

	/// Template snapshot directory backing this pool.
	pub fn template(&self) -> &Path {
		&self.template
	}

	/// Resize the target ready depth. The refiller observes this immediately.
	pub fn resize(&self, size: usize) {
		self.size.store(size, Ordering::Relaxed);
		let mut ready = self.ready.lock();
		while ready.len() > size {
			if let Some(vm) = ready.pop_back() {
				Self::teardown(vm);
			}
		}
	}

	/// Pop a ready clone in O(1), optionally renaming it to the requested
	/// sandbox name.
	pub fn claim(&self, name: Option<&str>) -> Result<Option<MicroVm>> {
		let Some(vm) = self.ready.lock().pop_front() else {
			self.misses.fetch_add(1, Ordering::Relaxed);
			return Ok(None);
		};
		self.hits.fetch_add(1, Ordering::Relaxed);
		if let Some(name) = name
			&& vm.name() != name
		{
			return rename_vm(vm, name).map(Some);
		}
		Ok(Some(vm))
	}

	/// Snapshot hit/miss counters and ready depth.
	pub fn stats(&self) -> PoolStats {
		PoolStats {
			ready:  self.ready.lock().len(),
			hits:   self.hits.load(Ordering::Relaxed),
			misses: self.misses.load(Ordering::Relaxed),
			size:   self.size.load(Ordering::Relaxed),
		}
	}

	/// Stop the refiller and tear down still-parked clones. Claimed VMs are
	/// untouched.
	pub fn shutdown(&self) {
		self.stop.store(true, Ordering::Relaxed);
		let handle = self.refiller.lock().take();
		if let Some(handle) = handle {
			let _ = handle.join();
		}
		let parked = self.ready.lock().drain(..).collect::<Vec<_>>();
		for vm in parked {
			Self::teardown(vm);
		}
	}

	fn refill_loop(self: Arc<Self>) {
		while !self.stop.load(Ordering::Relaxed) {
			let short = self.ready.lock().len() < self.size.load(Ordering::Relaxed);
			if short && let Some(vm) = self.spawn_one() {
				if self.stop.load(Ordering::Relaxed) {
					Self::teardown(vm);
				} else {
					self.ready.lock().push_back(vm);
				}
			}
			thread::sleep(REFILL_POLL);
		}
	}

	fn spawn_one(&self) -> Option<MicroVm> {
		let vm_name = pooled_name(&self.template);
		let vm = MicroVm::new(vm_name);
		let mut spec = if self.fork {
			LaunchSpec::fork_from(vm.api_sock(), &self.template)
		} else {
			LaunchSpec::restore(vm.api_sock(), &self.template)
		};
		if self.agent {
			spec = spec.with_agent_sock(vm.dir().join("agent.sock"));
		}
		if let Err(_err) = vm.launch(&spec) {
			let _ = vm.remove();
			return None;
		}
		if self.agent {
			let readiness =
				AgentConn::connect(&vm.agent_sock().ok()?, self.ping_timeout).and_then(|agent| {
					let result = agent.ping(self.ping_timeout).map(|_| ());
					agent.close();
					result
				});
			if let Err(_err) = readiness {
				Self::teardown(vm);
				return None;
			}
		}
		Some(vm)
	}

	fn teardown(vm: MicroVm) {
		let _ = vm.stop(true);
		let _ = vm.remove();
	}
}

impl Drop for WarmPool {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::Relaxed);
	}
}

/// Server-owned registry of warm pools keyed by portable template reference.
#[derive(Default)]
pub struct PoolRegistry {
	pools: Mutex<BTreeMap<String, Arc<WarmPool>>>,
}

impl PoolRegistry {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn get(&self, reference: &str) -> Option<Arc<WarmPool>> {
		self.pools.lock().get(reference).cloned()
	}

	pub fn set(&self, reference: impl Into<String>, pool: Arc<WarmPool>) -> Option<Arc<WarmPool>> {
		self.pools.lock().insert(reference.into(), pool)
	}

	pub fn delete(&self, reference: &str) -> Option<Arc<WarmPool>> {
		self.pools.lock().remove(reference)
	}

	pub fn list(&self) -> BTreeMap<String, PoolStats> {
		self
			.pools
			.lock()
			.iter()
			.map(|(reference, pool)| (reference.clone(), pool.stats()))
			.collect()
	}

	pub fn shutdown(&self) {
		let pools = self.pools.lock().values().cloned().collect::<Vec<_>>();
		for pool in pools {
			pool.shutdown();
		}
	}

	pub fn total_hits_misses(&self) -> (u64, u64) {
		self
			.pools
			.lock()
			.values()
			.map(|pool| {
				let stats = pool.stats();
				(stats.hits, stats.misses)
			})
			.fold((0, 0), |(hits, misses), (pool_hits, pool_misses)| {
				(hits + pool_hits, misses + pool_misses)
			})
	}
}

fn rename_vm(vm: MicroVm, name: &str) -> Result<MicroVm> {
	let old_meta = vm.meta()?;
	let old_dir = vm.dir().to_path_buf();
	let parent = old_dir
		.parent()
		.ok_or_else(|| crate::EngineError::invalid("VM directory must have a parent"))?;
	let new_dir = parent.join(name);
	if new_dir.exists() {
		return Err(crate::EngineError::busy(format!("sandbox '{name}' already exists")));
	}
	fs::create_dir_all(parent)?;
	fs::rename(old_dir, &new_dir)?;
	let renamed = MicroVm::from_dir(name.to_owned(), new_dir);
	let mut meta = serde_json::Map::new();
	meta.insert("sock".to_owned(), serde_json::json!(renamed.api_sock().to_string_lossy()));
	if let Some(value) = old_meta.get("agent_sock") {
		meta.insert(
			"agent_sock".to_owned(),
			renamed_path_meta(value, &renamed.dir().join("agent.sock"))?,
		);
	}
	if let Some(value) = old_meta.get("rootfs") {
		meta
			.insert("rootfs".to_owned(), renamed_path_meta(value, &renamed.dir().join("rootfs.img"))?);
	}
	renamed.save_meta(meta)?;
	Ok(renamed)
}

fn renamed_path_meta(value: &serde_json::Value, replacement: &Path) -> Result<serde_json::Value> {
	match value {
		serde_json::Value::Null | serde_json::Value::Bool(false) => Ok(serde_json::Value::Null),
		serde_json::Value::String(raw) if raw.is_empty() => Ok(serde_json::Value::Null),
		serde_json::Value::String(_) => Ok(serde_json::json!(replacement.to_string_lossy())),
		_ => Err(crate::EngineError::invalid("metadata path must be str or null")),
	}
}

fn pooled_name(template: &Path) -> String {
	let stem = template
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("template")
		.chars()
		.map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
		.take(32)
		.collect::<String>();
	let seq = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_nanos() as u64);
	format!("_pool-{stem}-{seq:08x}{:08x}", nanos & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn template_key_uses_locked_format() {
		assert_eq!(
			template_key("debian:stable", 1024, 512, 2, 3, true, false, true),
			"debian:stable|d1024|m512|c2|s3|h1|n0|t1",
		);
	}

	#[test]
	fn renamed_path_meta_preserves_nullish_values() -> Result<()> {
		let replacement = Path::new("/tmp/new-rootfs.img");
		assert!(renamed_path_meta(&serde_json::Value::Null, replacement)?.is_null());
		assert!(renamed_path_meta(&serde_json::Value::Bool(false), replacement)?.is_null());
		assert!(renamed_path_meta(&serde_json::Value::String(String::new()), replacement)?.is_null());
		assert_eq!(
			renamed_path_meta(&serde_json::Value::String("/old".to_owned()), replacement)?.as_str(),
			Some("/tmp/new-rootfs.img"),
		);
		Ok(())
	}

	#[test]
	fn rename_rewrites_present_paths_and_preserves_nulls() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let old_dir = tmp.path().join("vms").join("old");
		fs::create_dir_all(&old_dir)?;
		let vm = MicroVm::from_dir("old", &old_dir);
		let mut meta = serde_json::Map::new();
		meta.insert(
			"agent_sock".to_owned(),
			serde_json::json!(old_dir.join("agent.sock").to_string_lossy()),
		);
		meta.insert(
			"rootfs".to_owned(),
			serde_json::json!(old_dir.join("rootfs.img").to_string_lossy()),
		);
		meta.insert("remote_page_url".to_owned(), serde_json::Value::Null);
		vm.save_meta(meta)?;

		let renamed = rename_vm(vm, "new")?;
		let meta = renamed.meta()?;
		assert_eq!(
			meta.get("sock").and_then(serde_json::Value::as_str),
			Some(
				tmp.path()
					.join("vms/new/api.sock")
					.to_string_lossy()
					.as_ref()
			),
		);
		assert_eq!(
			meta.get("agent_sock").and_then(serde_json::Value::as_str),
			Some(
				tmp.path()
					.join("vms/new/agent.sock")
					.to_string_lossy()
					.as_ref()
			),
		);
		assert_eq!(
			meta.get("rootfs").and_then(serde_json::Value::as_str),
			Some(
				tmp.path()
					.join("vms/new/rootfs.img")
					.to_string_lossy()
					.as_ref()
			),
		);
		assert!(
			meta
				.get("remote_page_url")
				.is_some_and(serde_json::Value::is_null)
		);
		Ok(())
	}

	#[test]
	fn registry_stats_are_keyed_by_reference() {
		let registry = PoolRegistry::new();
		let pool = WarmPool::with_options("/no/template", 0, false, false, Duration::ZERO)
			.expect("pool starts");
		registry.set("ref", Arc::clone(&pool));
		let stats = registry.list();
		assert_eq!(stats["ref"], PoolStats { ready: 0, hits: 0, misses: 0, size: 0 });
		registry.shutdown();
	}
}
