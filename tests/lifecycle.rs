#![cfg(unix)]

mod common;

use std::time::Duration;

use serde_json::json;

#[test]
fn json_control_ping_info_errors_pause_snapshot_resume_quit() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("lifecycle");
	let sock = dir.join("control.sock");
	let snap_name = "snapshot";
	let snap = dir.join(snap_name);
	let mut args = common::base_args("soak");
	// Attach a virtio-blk device so device reporting is exercised on every
	// backend; soak mode never touches /dev/vda, so the disk stays untouched.
	let rootfs = common::copy_rootfs("lifecycle");
	args.push("--rootfs".into());
	args.push(rootfs.display().to_string());
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--snapshot-root".into());
	args.push(dir.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SOAK_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	control.ping();
	let info = control.request("info", json!({}));
	assert_eq!(info.get("cpus").and_then(|v| v.as_u64()), Some(1));
	assert_eq!(info.get("mem_mib").and_then(|v| v.as_u64()), Some(256));
	assert_eq!(info.get("state").and_then(|v| v.as_str()), Some("running"));
	assert!(
		info.get("devices").and_then(|v| v.as_array()).is_some(),
		"missing devices in info: {info}"
	);
	let metrics = control.request("metrics", json!({}));
	assert_eq!(metrics.get("cpus").and_then(|v| v.as_u64()), Some(1));
	assert_eq!(metrics.get("mem_mib").and_then(|v| v.as_u64()), Some(256));
	assert_eq!(metrics.get("state").and_then(|v| v.as_str()), Some("running"));
	assert!(
		metrics
			.get("device_count")
			.and_then(|v| v.as_u64())
			.is_some_and(|count| count > 0),
		"missing device_count in metrics: {metrics}"
	);
	let bad = control.raw_line("{not-json");
	assert_eq!(bad.get("ok").and_then(|v| v.as_bool()), Some(false));
	assert_eq!(bad.pointer("/error/code").and_then(|v| v.as_str()), Some("bad_request"));
	let denied = control.request_error("snapshot", json!({"name": ".."}));
	assert_eq!(denied.get("code").and_then(|v| v.as_str()), Some("path_denied"));
	let running = control.request_error("snapshot", json!({"name": "running"}));
	assert_eq!(running.get("code").and_then(|v| v.as_str()), Some("not_paused"));
	control.ping();
	assert_eq!(control.command("pause"), "OK");
	assert_eq!(control.command(&format!("snapshot {snap_name}")), "OK");
	assert_eq!(control.command("resume"), "OK");
	assert_eq!(control.command("quit"), "OK");

	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_snapshot_written(&snap);
	common::assert_no_panic(&output);
}

#[test]
fn two_vcpu_busy_guest_pause_resume_survives() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("lifecycle-busy-smp");
	let sock = dir.join("control.sock");
	let mut args = common::base_args("busy_smp");
	args.push("--cpus".into());
	args.push("2".into());
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	let boot = vm.wait_for("BUSY_READY", Duration::from_mins(1));
	assert!(boot.contains("CPUS=2"), "SMP guest did not expose 2 CPUs:\n{boot}");

	{
		let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
		assert_eq!(control.command("pause"), "OK");
		assert_eq!(control.command("resume"), "OK");
	}
	std::thread::sleep(Duration::from_secs(3));
	assert!(vm.has_exited().is_none(), "busy SMP guest exited after pause/resume:\n{}", vm.output());

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	control.ping();
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "busy SMP guest exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}
