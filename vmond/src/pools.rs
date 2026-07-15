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
		spawn::{LaunchSpec, SandboxRuntime, SandboxVm, VmonRuntime},
	},
};

const REFILL_POLL: Duration = Duration::from_millis(100);
const DEFAULT_PING_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REFILL_BATCH: usize = 32;
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
	has_rootfs:   bool,
	size:         AtomicUsize,
	fork:         bool,
	agent:        bool,
	ping_timeout: Duration,
	runtime:      Arc<dyn SandboxRuntime>,
	ready:        Mutex<VecDeque<SandboxVm>>,
	stop:         AtomicBool,
	hits:         AtomicU64,
	misses:       AtomicU64,
	refiller:     Mutex<Option<JoinHandle<()>>>,
}

impl WarmPool {
	/// Start a refiller for `template` with `size` parked VMs.
	pub fn new(template: impl Into<PathBuf>, size: usize) -> Result<Arc<Self>> {
		Self::with_runtime(template, size, Arc::new(VmonRuntime))
	}

	/// Start a refiller using the selected sandbox runtime.
	pub fn with_runtime(
		template: impl Into<PathBuf>,
		size: usize,
		runtime: Arc<dyn SandboxRuntime>,
	) -> Result<Arc<Self>> {
		Self::with_runtime_options(template, size, true, true, DEFAULT_PING_TIMEOUT, runtime)
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
		Self::with_runtime_options(template, size, fork, agent, ping_timeout, Arc::new(VmonRuntime))
	}

	fn with_runtime_options(
		template: impl Into<PathBuf>,
		size: usize,
		fork: bool,
		agent: bool,
		ping_timeout: Duration,
		runtime: Arc<dyn SandboxRuntime>,
	) -> Result<Arc<Self>> {
		let template = template.into();
		let has_rootfs = template.join("rootfs.img").is_file();
		let pool = Arc::new(Self {
			template,
			has_rootfs,
			size: AtomicUsize::new(size),
			fork,
			agent,
			ping_timeout,
			runtime,
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
				self.teardown(vm);
			}
		}
	}

	/// Pop a ready clone in O(1), optionally renaming it to the requested
	/// sandbox name.
	pub fn claim(&self, name: Option<&str>) -> Result<Option<SandboxVm>> {
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
			self.teardown(vm);
		}
	}

	fn refill_loop(self: Arc<Self>) {
		while !self.stop.load(Ordering::Relaxed) {
			let ready = self.ready.lock().len();
			let target = self.size.load(Ordering::Relaxed);
			if ready < target {
				self.refill_batch(target - ready);
			}
			thread::sleep(REFILL_POLL);
		}
	}

	/// Refill independent pool slots concurrently. Only this background thread
	/// produces ready VMs, so launches do not race; capacity is checked again
	/// before publishing results to make a concurrent resize safe.
	fn refill_batch(&self, shortfall: usize) {
		let count = shortfall.min(MAX_REFILL_BATCH);
		if count == 0 {
			return;
		}
		let spawned = thread::scope(|scope| {
			let workers = (0..count)
				.map(|_| scope.spawn(|| self.spawn_one()))
				.collect::<Vec<_>>();
			workers
				.into_iter()
				.filter_map(|worker| worker.join().ok().flatten())
				.collect::<Vec<_>>()
		});

		let mut overflow = Vec::new();
		{
			let mut ready = self.ready.lock();
			let target = self.size.load(Ordering::Relaxed);
			for vm in spawned {
				if self.stop.load(Ordering::Relaxed) || ready.len() >= target {
					overflow.push(vm);
				} else {
					ready.push_back(vm);
				}
			}
		}
		for vm in overflow {
			self.teardown(vm);
		}
	}

	fn spawn_one(&self) -> Option<SandboxVm> {
		let vm_name = pooled_name(&self.template);
		let vm = self.runtime.sandbox(&vm_name);
		let mut spec = if self.fork {
			LaunchSpec::fork_from(vm.api_sock(), &self.template)
		} else {
			LaunchSpec::restore(vm.api_sock(), &self.template)
		};
		// Pool VMs must not share the template's writable image: overlay it so
		// each claimed sandbox owns a checkpointable per-VM disk.
		if self.has_rootfs {
			let base_disk = self.template.join("rootfs.img");
			spec = spec.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"));
		}
		if self.agent {
			spec = spec.with_agent_sock(vm.dir().join("agent.sock"));
		}
		if let Err(_err) = self.runtime.launch(&vm, &spec) {
			let _ = self.runtime.remove(&vm);
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
				self.teardown(vm);
				return None;
			}
		}
		Some(vm)
	}

	fn teardown(&self, vm: SandboxVm) {
		let _ = self.runtime.stop(&vm, true);
		let _ = self.runtime.remove(&vm);
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

fn rename_vm(vm: SandboxVm, name: &str) -> Result<SandboxVm> {
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
	let renamed = SandboxVm::from_dir(name.to_owned(), new_dir);
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
	use std::time::Instant;

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
		let vm = SandboxVm::from_dir("old", &old_dir);
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
	#[derive(Debug)]
	struct RecordingRuntime {
		root:   PathBuf,
		events: Mutex<Vec<&'static str>>,
	}

	impl SandboxRuntime for RecordingRuntime {
		fn name(&self) -> &'static str {
			"recording"
		}

		fn sandbox(&self, name: &str) -> SandboxVm {
			SandboxVm::from_dir(name, self.root.join(name))
		}

		fn launch(&self, vm: &SandboxVm, _spec: &LaunchSpec) -> Result<()> {
			fs::create_dir_all(vm.dir())?;
			self.events.lock().push("launch");
			Ok(())
		}

		fn stop(&self, _vm: &SandboxVm, _wait: bool) -> Result<()> {
			self.events.lock().push("stop");
			Ok(())
		}

		fn remove(&self, vm: &SandboxVm) -> Result<()> {
			self.events.lock().push("remove");
			fs::remove_dir_all(vm.dir())?;
			Ok(())
		}

		fn is_running(&self, _vm: &SandboxVm) -> Result<bool> {
			Ok(true)
		}
	}

	#[test]
	fn warm_pool_delegates_lifecycle_to_selected_runtime() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let runtime = Arc::new(RecordingRuntime {
			root:   tmp.path().join("vms"),
			events: Mutex::new(Vec::new()),
		});
		let backend: Arc<dyn SandboxRuntime> = runtime.clone();
		let pool = WarmPool::with_runtime_options(
			tmp.path().join("template"),
			1,
			false,
			false,
			Duration::ZERO,
			backend,
		)?;
		let deadline = Instant::now() + Duration::from_secs(1);
		while pool.stats().ready == 0 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(1));
		}
		assert_eq!(pool.stats().ready, 1);

		pool.shutdown();
		assert_eq!(*runtime.events.lock(), ["launch", "stop", "remove"]);
		Ok(())
	}
	#[derive(Debug)]
	struct DelayedRuntime {
		root:             PathBuf,
		delay:            Duration,
		concurrency_peak: Mutex<usize>,
		concurrency_now:  Mutex<usize>,
	}

	impl SandboxRuntime for DelayedRuntime {
		fn name(&self) -> &'static str {
			"delayed"
		}

		fn sandbox(&self, name: &str) -> SandboxVm {
			SandboxVm::from_dir(name, self.root.join(name))
		}

		fn launch(&self, vm: &SandboxVm, _spec: &LaunchSpec) -> Result<()> {
			fs::create_dir_all(vm.dir())?;
			{
				let mut now = self.concurrency_now.lock();
				*now += 1;
				let mut peak = self.concurrency_peak.lock();
				*peak = (*peak).max(*now);
			}
			thread::sleep(self.delay);
			{
				let mut now = self.concurrency_now.lock();
				*now -= 1;
			}
			Ok(())
		}

		fn stop(&self, _vm: &SandboxVm, _wait: bool) -> Result<()> {
			Ok(())
		}

		fn remove(&self, vm: &SandboxVm) -> Result<()> {
			let _ = fs::remove_dir_all(vm.dir());
			Ok(())
		}

		fn is_running(&self, _vm: &SandboxVm) -> Result<bool> {
			Ok(true)
		}
	}

	#[test]
	fn warm_pool_refills_concurrently_and_respects_target_bounds() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let delay = Duration::from_millis(50);
		let runtime = Arc::new(DelayedRuntime {
			root: tmp.path().join("vms"),
			delay,
			concurrency_peak: Mutex::new(0),
			concurrency_now: Mutex::new(0),
		});

		// Execute 32 launches sequentially as a baseline for the maximum batch.
		let t_seq_start = Instant::now();
		for i in 0..32 {
			let vm = runtime.sandbox(&format!("serial-{i}"));
			let spec = LaunchSpec::restore(vm.api_sock(), tmp.path().join("template"));
			runtime.launch(&vm, &spec)?;
		}
		let elapsed_seq = t_seq_start.elapsed();

		// Execute the same 32 launches through the bounded concurrent refiller.
		let backend: Arc<dyn SandboxRuntime> = runtime.clone();
		let t_concurrent_start = Instant::now();
		let pool = WarmPool::with_runtime_options(
			tmp.path().join("template"),
			32,
			false,
			false,
			Duration::ZERO,
			backend,
		)?;

		let deadline = Instant::now() + Duration::from_secs(4);
		while pool.stats().ready < 32 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(1));
		}
		let elapsed_concurrent = t_concurrent_start.elapsed();

		assert_eq!(pool.stats().ready, 32, "Concurrent pool failed to reach full size");
		pool.shutdown();

		let peak_concurrency = *runtime.concurrency_peak.lock();
		println!(
			"{}",
			serde_json::json!({
				"kind": "warm_pool_refill",
				"clones": 32,
				"baseline_serial_ms": elapsed_seq.as_millis(),
				"optimized_parallel_ms": elapsed_concurrent.as_millis(),
				"peak_parallel_spawns": peak_concurrency,
			})
		);

		// Assert parallel execution and bounds checks
		assert!(
			peak_concurrency > 1,
			"WarmPool refill did not run launches in parallel: peak={peak_concurrency}"
		);
		assert!(
			elapsed_concurrent < elapsed_seq,
			"Parallel pool refill took longer than sequential baseline"
		);
		Ok(())
	}
}
