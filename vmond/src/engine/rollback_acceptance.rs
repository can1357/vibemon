use std::{
	fs, io,
	os::unix::net::UnixListener,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use futures_util::future::BoxFuture;
use serde_json::{Map, Value};
use tempfile::TempDir;

use super::*;

struct AcceptanceRuntime {
	launches:            AtomicUsize,
	fail_launches:       usize,
	cancel_after_launch: Mutex<Option<Arc<AtomicBool>>>,
}

struct NoopOwnershipHandoff;

impl OwnershipHandoff for NoopOwnershipHandoff {
	fn begin_restore(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn commit_restore(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn abort_restore(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn acquire_restore_volume_leases<'a>(
		&'a self,
		_sid: &'a str,
		_owner: &'a str,
		_epoch: i64,
		_params: &'a mut Map<String, Value>,
	) -> BoxFuture<'a, Result<Vec<Value>>> {
		Box::pin(async { Ok(Vec::new()) })
	}

	fn persist_restore_volume_leases<'a>(
		&'a self,
		_sid: &'a str,
		_leases: &'a [Value],
	) -> BoxFuture<'a, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn release_restore_volume_leases(&self, _leases: Vec<Value>) -> BoxFuture<'_, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn release_source_volume_leases<'a>(&'a self, _sid: &'a str) -> BoxFuture<'a, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn prepare_suspend(
		&self,
		_sid: &str,
		_owner: &str,
		_epoch: i64,
		_point: &str,
		_generation: u64,
	) -> Result<()> {
		Ok(())
	}

	fn commit_suspend(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn abort_suspend(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn prepare_rollback(
		&self,
		_sid: &str,
		_owner: &str,
		_epoch: i64,
		_target: &str,
		_safety: &str,
		_operation_generation: u64,
		_checkpoint_generation: u64,
	) -> Result<()> {
		Ok(())
	}

	fn commit_rollback(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn abort_rollback(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn begin_delete(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}

	fn commit_delete(&self, _sid: &str, _owner: &str, _epoch: i64) -> Result<()> {
		Ok(())
	}
}

impl AcceptanceRuntime {
	fn new(fail_launches: usize) -> Arc<Self> {
		Arc::new(Self {
			launches: AtomicUsize::new(0),
			fail_launches,
			cancel_after_launch: Mutex::new(None),
		})
	}

	fn cancel_after_launch(&self, cancellation: Arc<AtomicBool>) {
		*self.cancel_after_launch.lock() = Some(cancellation);
	}
}

impl SandboxRuntime for AcceptanceRuntime {
	fn name(&self) -> &'static str {
		"rollback-acceptance"
	}

	fn launch(&self, vm: &SandboxVm, _spec: &LaunchSpec) -> Result<()> {
		let attempt = self.launches.fetch_add(1, Ordering::SeqCst) + 1;
		fs::create_dir_all(vm.dir())?;
		if attempt <= self.fail_launches {
			return Err(EngineError::engine("injected replacement launch failure"));
		}
		let socket = vm.dir().join("agent.sock");
		let listener = UnixListener::bind(&socket)?;
		thread::spawn(move || {
			let Ok((mut stream, _)) = listener.accept() else {
				return;
			};
			if vmon_agent::proto::resync_to_marker(&mut stream).ok() != Some(true) {
				return;
			}
			while let Ok(Some(frame)) = vmon_agent::proto::read_frame(&mut stream) {
				if frame.ty == vmon_agent::proto::FRAME_REQ {
					let response = serde_json::to_vec(&json!({"ok": true})).expect("agent response");
					let _ = vmon_agent::proto::write_frame(
						&mut stream,
						vmon_agent::proto::FRAME_RESP,
						frame.id,
						&response,
					);
				}
			}
		});
		vm.save_meta(Map::from_iter([("pid".to_owned(), json!(10_000 + attempt))]))?;
		if let Some(cancelled) = self.cancel_after_launch.lock().as_ref() {
			cancelled.store(true, Ordering::Release);
		}
		Ok(())
	}

	fn stop(&self, _vm: &SandboxVm, _wait: bool) -> Result<()> {
		Ok(())
	}

	fn remove(&self, vm: &SandboxVm) -> Result<()> {
		match fs::remove_dir_all(vm.dir()) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error.into()),
		}
	}

	fn is_running(&self, vm: &SandboxVm) -> Result<bool> {
		Ok(vm.dir().is_dir())
	}
}

fn config_for(temp: &TempDir) -> ServeConfig {
	let mut config = ServeConfig::default();
	config.home = temp.path().to_path_buf();
	config.warm_images = Vec::new();
	config
}

fn engine_for(
	temp: &TempDir,
	runtime: Arc<AcceptanceRuntime>,
) -> (Engine, crate::home::test_home::HomeGuard) {
	let home = crate::home::test_home::set(temp.path());
	let engine = Engine::with_runtime(config_for(temp), runtime.clone())
		.expect("acceptance engine constructs");
	engine.test_set_restore_executor(Arc::new(
		move |engine, params, _template, _replace_volumes, cancellation| {
			if cancellation
				.as_ref()
				.is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
			{
				return Err(EngineError::engine("restore cancelled before readiness"));
			}
			let attempt = runtime.launches.fetch_add(1, Ordering::SeqCst) + 1;
			if attempt <= runtime.fail_launches {
				return Err(EngineError::engine("injected replacement launch failure"));
			}
			let id = params
				.get("name")
				.and_then(Value::as_str)
				.expect("restore identity")
				.to_owned();
			let vm = engine.sandbox(&id);
			fs::create_dir_all(vm.dir())?;
			vm.save_meta(params.clone())?;
			let mut record = VmRecord::new(&id, &id, "running");
			record.detail = Value::Object(params);
			engine.insert_test_record(record);
			Ok(json!({"name": id}))
		},
	));
	(engine, home)
}

fn seal_recovery(engine: &Engine, id: &str, point: &str) {
	let source = engine.snapshot_dir(&format!(".{id}-{point}-fixture"));
	let state = source.join("state");
	fs::create_dir_all(&state).expect("recovery fixture state");
	fs::write(state.join("rootfs.img"), b"bootable").expect("fixture rootfs");
	fs::write(state.join("agent-ready.json"), b"{}").expect("fixture readiness marker");
	fs::write(
		source.join("recovery.json"),
		serde_json::to_vec(&json!({
			"version": 1,
			"sandbox_id": id,
			"kind": "checkpoint",
			"created_at_unix_millis": 1,
			"params": {"name": id, "block_network": true, "fixture_point": point},
		}))
		.expect("recovery manifest"),
	)
	.expect("write recovery manifest");
	let archive = engine
		.recovery_archive(id, point)
		.expect("recovery archive path");
	fs::create_dir_all(archive.parent().expect("recovery root")).expect("recovery root");
	EncryptedArchive::seal(&source, &archive, &engine.inner.keyring, "default")
		.expect("seal recovery");
	fs::remove_dir_all(source).expect("remove recovery fixture");
}

fn journal(id: &str, target: &str, safety: &str, generation: u64) -> RollbackJournal {
	RollbackJournal {
		sandbox_id: id.to_owned(),
		target_recovery_point: target.to_owned(),
		safety_recovery_point: safety.to_owned(),
		generation,
		checkpoint_generation: 41,
		portable_owner: String::new(),
		portable_owner_epoch: 0,
		source_token: "source-token-before-rollback".to_owned(),
	}
}

fn runtime_root_is_empty(home: &Home) -> bool {
	let root = home.security_dir().join("runtime");
	fs::read_dir(root).map_or(true, |mut entries| entries.next().is_none())
}

#[test]
fn journal_survives_restart_after_durable_write_and_replays_target() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	seal_recovery(&engine, "rollback", "target");
	seal_recovery(&engine, "rollback", "safety");
	engine
		.write_rollback_journal(&journal("rollback", "target", "safety", 7))
		.expect("durable journal");
	let path = engine
		.rollback_journal_path("rollback")
		.expect("journal path");
	assert!(path.is_file(), "the crash boundary must retain the durable journal");

	engine
		.test_recover_rollback_journals_after_durable()
		.expect("restart recovery");
	let record = engine
		.inner
		.registry
		.get("rollback")
		.expect("target restored after restart");
	assert_eq!(record.id, "rollback");
	assert_eq!(record.lifecycle.generation, StateGeneration(7));
	assert!(record.lifecycle.is_converged());
	assert!(!path.exists(), "journal is removed only after convergence");
	assert!(
		!engine
			.recovery_archive("rollback", "safety")
			.expect("safety archive")
			.exists()
	);
}

#[test]
fn no_record_post_delete_restart_restores_selected_target() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	seal_recovery(&engine, "deleted", "selected");
	seal_recovery(&engine, "deleted", "safety");
	engine
		.write_rollback_journal(&journal("deleted", "selected", "safety", 9))
		.expect("durable journal");
	assert!(
		engine.inner.registry.get("deleted").is_none(),
		"delete boundary has no registry record"
	);

	engine
		.test_recover_rollback_journals_after_durable()
		.expect("recover selected target");
	let record = engine
		.inner
		.registry
		.get("deleted")
		.expect("selected target restored");
	assert_eq!(record.id, "deleted");
	assert_eq!(record.lifecycle.generation, StateGeneration(9));
	assert!(record.lifecycle.is_converged());
}

#[test]
fn same_pid_with_new_source_token_restores_exact_safety_checkpoint() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	seal_recovery(&engine, "token-fence", "target");
	seal_recovery(&engine, "token-fence", "safety");
	let mut candidate = VmRecord::new("token-fence", "token-fence", "running");
	candidate.pid = Some(42);
	candidate.detail = json!({"rollback_source_token": "replacement-token-with-the-same-pid"});
	engine.insert_test_record(candidate);
	fs::create_dir_all(engine.sandbox("token-fence").dir()).expect("live candidate directory");
	engine
		.write_rollback_journal(&journal("token-fence", "target", "safety", 13))
		.expect("durable journal");

	engine
		.test_recover_rollback_journals_after_durable()
		.expect("exact safety recovery");
	let restored = engine
		.inner
		.registry
		.get("token-fence")
		.expect("safety replacement");
	assert_eq!(restored.detail["fixture_point"], "safety");
	assert_eq!(restored.lifecycle.generation, StateGeneration(13));
	assert!(restored.lifecycle.is_converged());
}
#[test]
fn post_default_generation_insert_is_repaired_before_convergence() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	seal_recovery(&engine, "generation", "target");
	seal_recovery(&engine, "generation", "safety");
	engine
		.write_rollback_journal(&journal("generation", "target", "safety", 23))
		.expect("journal");

	engine
		.test_recover_rollback_journals_after_durable()
		.expect("generation repair");
	let record = engine
		.inner
		.registry
		.get("generation")
		.expect("replacement record");
	assert_eq!(record.lifecycle.generation, StateGeneration(23));
	assert_eq!(record.detail["checkpoint_generation"], json!(41));
	assert_eq!(
		record.lifecycle.operation, None,
		"convergence clears only the acquired rollback operation"
	);
	assert!(record.lifecycle.is_converged());
}

#[test]
fn target_failure_restores_runnable_safety_checkpoint() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(1);
	let (engine, _home) = engine_for(&temp, runtime.clone());
	engine.set_restore_handoff(Arc::new(NoopOwnershipHandoff));
	let safety = "safety-rollback-safety-";
	let events = Arc::new(Mutex::new(Vec::new()));
	let capture_events = Arc::clone(&events);
	let resume_events = Arc::clone(&events);
	engine.test_set_rollback_resume_executor(Arc::new(move |_vm| {
		resume_events.lock().push("resume");
	}));
	seal_recovery(&engine, "fallback", "target");
	seal_recovery(&engine, "fallback", safety);
	let mut source = VmRecord::new("fallback", "fallback", "running");
	source.pid = Some(71);
	engine.insert_test_record(source);
	fs::create_dir_all(engine.sandbox("fallback").dir()).expect("source VM directory");
	engine
		.sandbox("fallback")
		.save_meta(Map::from_iter([("pid".to_owned(), json!(71))]))
		.expect("source pid metadata");
	engine.test_set_capture_executor(Arc::new(
		move |_engine, sid, kind, keep_running, publish_portable| {
			capture_events.lock().push("capture");
			assert_eq!(sid, "fallback");
			assert_eq!(kind, "rollback-safety");
			assert!(!keep_running, "safety capture must retain the source pause through cutover");
			assert!(!publish_portable, "local fixture has no production ownership bridge");
			Ok(RecoveryPoint {
				name:                   safety.to_owned(),
				kind:                   "checkpoint".to_owned(),
				created_at_unix_millis: 1,
				size_bytes:             0,
			})
		},
	));

	assert!(<Engine as EngineApi>::rollback(&engine, "fallback", "target").is_err());
	let record = engine
		.inner
		.registry
		.get("fallback")
		.expect("safety replacement");
	assert_eq!(record.status, "running");
	assert_eq!(record.detail["fixture_point"], safety);
	assert_eq!(
		runtime.launches.load(Ordering::SeqCst),
		2,
		"target failure must launch the safety point"
	);
	assert!(
		!engine
			.rollback_journal_path("fallback")
			.expect("journal path")
			.exists(),
		"journal clears only after the safety restore converges"
	);
	assert!(
		!engine
			.recovery_archive("fallback", safety)
			.expect("safety archive")
			.exists()
	);
	assert_eq!(
		*events.lock(),
		vec!["capture"],
		"a serving replacement never resumes the old source"
	);
}

#[test]
fn rollback_precommit_capture_failure_resumes_paused_source_exactly_once() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	engine.set_restore_handoff(Arc::new(NoopOwnershipHandoff));
	seal_recovery(&engine, "resume-on-error", "target");
	let mut source = VmRecord::new("resume-on-error", "resume-on-error", "running");
	source.pid = Some(72);
	engine.insert_test_record(source);
	fs::create_dir_all(engine.sandbox("resume-on-error").dir()).expect("source VM directory");
	engine
		.sandbox("resume-on-error")
		.save_meta(Map::from_iter([("pid".to_owned(), json!(72))]))
		.expect("source pid metadata");
	let events = Arc::new(Mutex::new(Vec::new()));
	let capture_events = Arc::clone(&events);
	let resume_events = Arc::clone(&events);
	engine.test_set_capture_executor(Arc::new(
		move |_engine, _sid, kind, keep_running, _publish_portable| {
			capture_events.lock().push("capture");
			assert_eq!(kind, "rollback-safety");
			assert!(!keep_running, "source is paused before safety capture");
			Err(EngineError::engine("injected precommit capture failure"))
		},
	));
	engine.test_set_rollback_resume_executor(Arc::new(move |_vm| {
		resume_events.lock().push("resume");
	}));

	assert!(<Engine as EngineApi>::rollback(&engine, "resume-on-error", "target").is_err());
	assert_eq!(*events.lock(), vec!["capture", "resume"]);
	assert!(
		!engine
			.rollback_journal_path("resume-on-error")
			.expect("journal path")
			.exists(),
		"precommit failure does not leave a rollback journal"
	);
}
#[test]
fn slow_readiness_restore_is_cancelled_before_lease_safety_deadline() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime.clone());
	seal_recovery(&engine, "cancelled", "target");
	seal_recovery(&engine, "cancelled", "safety");
	engine
		.write_rollback_journal(&journal("cancelled", "target", "safety", 3))
		.expect("journal");
	engine.test_set_restore_executor(Arc::new(|_, _, _, _, cancellation| {
		loop {
			if cancellation
				.as_ref()
				.is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
			{
				return Err(EngineError::engine("restore cancelled while awaiting readiness"));
			}
			thread::sleep(Duration::from_millis(1));
		}
	}));
	let readiness_token = engine.test_restore_cancellation_token("cancelled");
	let signal = thread::spawn(move || {
		thread::sleep(Duration::from_millis(10));
		readiness_token.store(true, Ordering::Release);
	});
	let started = Instant::now();
	assert!(
		engine
			.test_recover_rollback_journals_after_durable()
			.is_err(),
		"cancelled restore must stop readiness"
	);
	signal.join().expect("cancellation signal");
	assert!(
		started.elapsed() < Duration::from_secs(1),
		"cancellation precedes the lease safety deadline"
	);
	assert!(
		engine.inner.registry.get("cancelled").is_none(),
		"cancelled candidate is not published"
	);
	assert!(!engine.sandbox("cancelled").dir().exists(), "cancelled candidate is removed");
	engine.test_clear_restore_cancellation("cancelled");

	// The same cancellation must fence the narrow post-spawn/pre-registration
	// window: the fake runtime sets this token only after it created VM state.
	engine.test_clear_restore_executor();
	let spawn_token = engine.test_restore_cancellation_token("spawn-boundary");
	runtime.cancel_after_launch(spawn_token);
	seal_recovery(&engine, "spawn-boundary", "target");
	seal_recovery(&engine, "spawn-boundary", "safety");
	engine
		.write_rollback_journal(&journal("spawn-boundary", "target", "safety", 4))
		.expect("spawn boundary journal");
	assert!(
		engine
			.test_recover_rollback_journals_after_durable()
			.is_err(),
		"post-spawn cancellation aborts the candidate before registration"
	);
	assert!(engine.inner.registry.get("spawn-boundary").is_none());
	assert!(!engine.sandbox("spawn-boundary").dir().exists());
}

#[test]
fn repeated_capture_publication_failures_leave_no_local_archive_growth() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	let root = engine.recovery_root("publication").expect("recovery root");
	for attempt in 0..3 {
		let archive = root.join(format!("failed-{attempt}.venc"));
		fs::create_dir_all(&root).expect("recovery root");
		fs::write(&archive, b"publication staging").expect("staged archive");
		let cleanup = ArchiveCleanup { path: archive.clone(), preserve: false };
		drop(cleanup); // Models the error return after production publication rejects the staged archive.
		assert!(!archive.exists(), "failed publication {attempt} leaves no local .venc");
	}
	assert!(fs::read_dir(root).expect("recovery root").next().is_none());
}

#[test]
fn repeated_open_success_and_error_leave_no_downloaded_archive() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	seal_recovery(&engine, "opened", "valid");
	for _ in 0..3 {
		let (source, _) = engine
			.open_recovery("opened", "valid")
			.expect("open recovery");
		assert!(source.path.join("rootfs.img").is_file());
		drop(source);
		assert!(
			runtime_root_is_empty(engine.home()),
			"successful open cleans its downloaded archive"
		);
		assert!(engine.open_recovery("opened", "missing").is_err());
		assert!(runtime_root_is_empty(engine.home()), "failed open leaves no downloaded archive");
	}
}

#[test]
fn unpublished_suspend_reconciliation_resumes_before_exact_cancel_and_retries() {
	let temp = TempDir::new().expect("temp");
	let runtime = AcceptanceRuntime::new(0);
	let (engine, _home) = engine_for(&temp, runtime);
	let mut record = VmRecord::new("suspend", "suspend", "running");
	record.lifecycle = LifecycleState {
		desired:    LifecyclePhase::Suspended,
		observed:   LifecyclePhase::Running,
		generation: StateGeneration(17),
		failure:    None,
		operation:  None,
	};
	engine.insert_test_record(record);
	let resumes = Arc::new(AtomicUsize::new(0));

	let crash_resumes = Arc::clone(&resumes);
	assert!(
		engine
			.test_resume_unpublished_suspend(
				"suspend",
				move || {
					crash_resumes.fetch_add(1, Ordering::SeqCst);
					Ok(())
				},
				|| Err(EngineError::engine("injected crash after resume")),
			)
			.is_err()
	);
	let pending = engine
		.inner
		.registry
		.get("suspend")
		.expect("pending record");
	assert_eq!(pending.lifecycle.desired, LifecyclePhase::Suspended);
	assert_eq!(pending.lifecycle.generation, StateGeneration(17));
	assert!(!pending.lifecycle.is_converged());

	let retry_resumes = Arc::clone(&resumes);
	engine
		.test_resume_unpublished_suspend(
			"suspend",
			move || {
				retry_resumes.fetch_add(1, Ordering::SeqCst);
				Ok(())
			},
			|| Ok(()),
		)
		.expect("retry resumes then cancels the unpublished suspend");
	assert_eq!(resumes.load(Ordering::SeqCst), 2);
	let converged = engine
		.inner
		.registry
		.get("suspend")
		.expect("converged record");
	assert_eq!(converged.lifecycle.desired, LifecyclePhase::Running);
	assert!(converged.lifecycle.is_converged());

	let mut failing = VmRecord::new("resume-failure", "resume-failure", "running");
	failing.lifecycle = LifecycleState {
		desired:    LifecyclePhase::Suspended,
		observed:   LifecyclePhase::Running,
		generation: StateGeneration(19),
		failure:    None,
		operation:  None,
	};
	engine.insert_test_record(failing);
	assert!(
		engine
			.test_resume_unpublished_suspend(
				"resume-failure",
				|| Err(EngineError::engine("injected resume failure")),
				|| Ok(()),
			)
			.is_err()
	);
	let still_pending = engine
		.inner
		.registry
		.get("resume-failure")
		.expect("failed pending record");
	assert_eq!(still_pending.lifecycle.desired, LifecyclePhase::Suspended);
	assert_eq!(still_pending.lifecycle.generation, StateGeneration(19));
	assert!(!still_pending.lifecycle.is_converged());
}
