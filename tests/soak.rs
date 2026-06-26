mod common;

use std::time::{Duration, Instant};

#[test]
fn boot_quit_loop_has_no_panics() {
	if !common::require_soak() {
		return;
	}

	let loops = common::parse_env_usize("VMON_SOAK_LOOPS", 200);
	for i in 0..loops {
		let dir = common::test_dir(&format!("soak-loop-{i}"));
		let sock = dir.join("control.sock");
		let mut args = common::base_args("soak");
		args.push("--api-sock".into());
		args.push(sock.display().to_string());

		let refs = common::as_refs(&args);
		let mut vm = common::spawn_vmm(&refs);
		vm.wait_for("SOAK_READY", Duration::from_mins(1));
		let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
		assert_eq!(control.command("quit"), "OK");
		let (status, output) = vm.wait(Duration::from_secs(30));
		assert!(status.success(), "loop {i} exited with {status}; output:\n{output}");
		common::assert_no_panic(&output);
	}
}

#[test]
fn long_run_stays_within_fd_and_rss_ceilings() {
	if !common::require_soak() {
		return;
	}

	let run_secs = common::parse_env_u64("VMON_SOAK_LONG_SECS", 10 * 60);
	let fd_ceiling = common::parse_env_usize("VMON_SOAK_FD_CEILING", 256);
	let rss_kb_ceiling = common::parse_env_u64("VMON_SOAK_RSS_KB_CEILING", 1024 * 1024);

	let dir = common::test_dir("soak-long");
	let sock = dir.join("control.sock");
	let mut args = common::base_args("soak");
	args.push("--api-sock".into());
	args.push(sock.display().to_string());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SOAK_READY", Duration::from_mins(1));
	let pid = vm.pid();
	let deadline = Instant::now() + Duration::from_secs(run_secs);

	while Instant::now() < deadline {
		if let Some(status) = vm.has_exited() {
			let output = vm.output();
			panic!("vmon exited early with {status}; output:\n{output}");
		}
		if let Some(fds) = common::fd_count(pid) {
			assert!(fds <= fd_ceiling, "fd count {fds} exceeded ceiling {fd_ceiling}");
		}
		if let Some(rss_kb) = common::rss_kb(pid) {
			assert!(
				rss_kb <= rss_kb_ceiling,
				"VmRSS {rss_kb} KiB exceeded ceiling {rss_kb_ceiling} KiB"
			);
		}
		std::thread::park_timeout(
			Duration::from_secs(5).min(deadline.saturating_duration_since(Instant::now())),
		);
	}

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "long-run exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}
