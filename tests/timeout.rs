mod common;

use std::{fs, time::Duration};

use serde_json::Value;

#[test]
fn timeout_secs_self_terminates_with_status_json() {
	if !common::require_hv() {
		return;
	}

	// Volume write round-trips require the agent/e2e path and are covered by the
	// Python e2e suite; this legacy initramfs harness only needs lifecycle smoke.
	let dir = common::test_dir("timeout");
	let sock = dir.join("api.sock");
	let mut args = common::base_args("soak");
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--timeout-secs".into());
	args.push("2".into());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);

	let Some((status, output)) = vm.wait_for_exit(Duration::from_secs(10)) else {
		let output = vm.output();
		vm.kill();
		panic!("vmon did not exit within 10s; output:\n{output}");
	};

	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);

	let status_path = dir.join("status.json");
	let status_json = fs::read_to_string(&status_path)
		.unwrap_or_else(|e| panic!("reading {}: {e}", status_path.display()));
	let status: Value = serde_json::from_str(&status_json)
		.unwrap_or_else(|e| panic!("parsing {}: {e}; body={status_json:?}", status_path.display()));

	assert_eq!(status.get("state").and_then(Value::as_str), Some("finished"));
	assert_eq!(status.get("reason").and_then(Value::as_str), Some("timeout"));
	assert_eq!(status.get("vmm_returncode").and_then(Value::as_i64), Some(124));
}
