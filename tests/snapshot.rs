#![cfg(unix)]

mod common;

use std::{path::PathBuf, time::Duration};

use serde_json::json;

#[test]
fn restore_resumes_past_snapshot_marker() {
	if !common::require_hv() {
		return;
	}

	let snap = create_snapshot("restore");
	let args = vec!["--restore".to_string(), snap.display().to_string()];
	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "SNAPSHOT_AFTER_RESTORE", Duration::from_secs(90));

	assert!(
		output.contains("SNAPSHOT_AFTER_RESTORE") || output.contains("SNAPSHOT_RESTORED"),
		"restore marker missing:\n{output}"
	);
	common::assert_no_panic(&output);
}

#[test]
fn fork_from_count_two_starts_two_clones() {
	if !common::require_hv() {
		return;
	}

	let snap = create_snapshot("fork");
	let args = vec![
		"--fork-from".to_string(),
		snap.display().to_string(),
		"--count".to_string(),
		"2".to_string(),
	];
	let refs = common::as_refs(&args);
	let mut parent = common::spawn_vmm(&refs);
	let (status, output) = parent.wait(Duration::from_mins(2));

	assert!(status.success(), "fork parent exited with {status}; output:\n{output}");
	// Each clone's guest serial output is byte-interleaved on the shared parent
	// stdout, so per-guest restore markers can be mangled past exact matching
	// when clones boot in lockstep (observed under nested KVM). Verify instead
	// that two clones were launched and the fork supervisor waited for all of
	// them to exit cleanly; single-clone resume-past-snapshot correctness is
	// covered by `restore_resumes_past_snapshot_marker`.
	let clones = output.matches("fork child").count();
	assert!(clones >= 2, "expected two forked clones, saw {clones}:\n{output}");
	common::assert_no_panic(&output);
}

fn create_snapshot(name: &str) -> PathBuf {
	let dir = common::test_dir(&format!("snapshot-{name}"));
	let sock = dir.join("control.sock");
	let snap_name = "snapshot";
	let snap = dir.join(snap_name);
	let mut args = common::base_args("snapshot");
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--snapshot-root".into());
	args.push(dir.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SNAPSHOT_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	assert_eq!(control.command("pause"), "OK");
	assert_eq!(control.command(&format!("snapshot {snap_name}")), "OK");
	assert_eq!(control.command("quit"), "OK");

	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "snapshot source exited with {status}; output:\n{output}");
	common::assert_snapshot_written(&snap);
	common::assert_no_panic(&output);
	snap
}

#[test]
fn delta_snapshot_restores_and_is_smaller() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("snapshot-delta");
	let sock = dir.join("control.sock");
	let base_name = "base";
	let delta_name = "delta";
	let base = dir.join(base_name);
	let delta = dir.join(delta_name);

	let cmdline = common::cmdline_with("snapshot", &[("vmon.snapshot_after_base_hold", "30")]);
	let mut args = common::base_args_with_cmdline(cmdline);
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--snapshot-root".into());
	args.push(dir.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SNAPSHOT_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	assert_eq!(control.command("pause"), "OK");
	control.request("snapshot", json!({"name": base_name}));
	assert_eq!(control.command("resume"), "OK");
	vm.wait_for("SNAPSHOT_AFTER_BASE", Duration::from_mins(1));

	assert_eq!(control.command("pause"), "OK");
	control.request("snapshot", json!({"name": delta_name, "base": base_name}));
	assert_eq!(control.command("quit"), "OK");

	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "snapshot source exited with {status}; output:\n{output}");
	common::assert_snapshot_written(&base);
	common::assert_snapshot_written(&delta);
	common::assert_no_panic(&output);

	let base_len = common::current_memory_file_len(&base);
	let delta_len = common::current_memory_file_len(&delta);
	assert!(delta_len < base_len, "delta memory file {delta_len} not smaller than base {base_len}");

	let restore = common::test_dir("snapshot-delta-restore");
	let mut restore_args = common::base_args("snapshot");
	restore_args.push("--restore".into());
	restore_args.push(delta.display().to_string());
	restore_args.push("--snapshot-root".into());
	restore_args.push(restore.display().to_string());
	let refs = common::as_refs(&restore_args);
	let output = common::boot_capture(&refs, "SNAPSHOT_AFTER_RESTORE", Duration::from_secs(90));
	assert!(
		output.contains("SNAPSHOT_AFTER_RESTORE") || output.contains("SNAPSHOT_RESTORED"),
		"restore marker missing:\n{output}"
	);
	common::assert_no_panic(&output);
}
