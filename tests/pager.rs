#![cfg(unix)]

mod common;

use std::{
	thread,
	time::{Duration, Instant},
};

use serde_json::json;

#[test]
fn mem_target_pager_evicts_and_reports_metrics() {
	if !common::require_hv() || !common::supports_linux_isolation() {
		return;
	}

	let dir = common::test_dir("pager-mem-target");
	let sock = dir.join("control.sock");
	let mut args = common::base_args("soak");
	if let Some(mem) = args
		.iter_mut()
		.skip_while(|arg| arg.as_str() != "--mem")
		.nth(1)
	{
		*mem = "128".into();
	}
	args.push("--mem-target-mib".into());
	args.push("32".into());
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SOAK_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	let deadline = Instant::now() + Duration::from_secs(10);
	let metrics = loop {
		let metrics = control.request("metrics", json!({}));
		let evictions = metrics["pager"]["evictions"].as_u64().unwrap_or(0);
		if evictions > 0 || Instant::now() >= deadline {
			break metrics;
		}
		thread::sleep(Duration::from_millis(200));
	};

	assert!(
		metrics["pager"]["evictions"].as_u64().unwrap_or(0) > 0,
		"pager did not evict before deadline: {metrics}"
	);
	assert!(
		metrics["pager"]["resident_pages"]
			.as_u64()
			.unwrap_or(u64::MAX)
			<= (32 * 1024 * 1024 / 4096) + 8192,
		"pager resident_pages not near target: {metrics}"
	);
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}
