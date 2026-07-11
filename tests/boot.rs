mod common;

use std::{
	thread,
	time::{Duration, Instant},
};

use serde_json::{Value, json};

#[test]
fn boots_initramfs_and_prints_marker() {
	if !common::require_hv() {
		return;
	}

	let args = common::base_args("boot");
	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "VMON_OK", Duration::from_mins(1));

	assert!(output.contains("VMON_OK"), "boot marker missing:\n{output}");
	common::assert_no_panic(&output);
}

/// `--log-format json`: tracing output must be machine-parseable JSON lines.
#[test]
fn json_log_format_emits_json_lines() {
	if !common::require_hv() {
		return;
	}

	let mut args = common::base_args("boot");
	args.push("--log-format".into());
	args.push("json".into());
	args.push("--log-level".into());
	args.push("info".into());

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "VMON_OK", Duration::from_mins(1));

	assert!(output.contains("VMON_OK"), "boot marker missing:\n{output}");
	let has_json_log = output.lines().any(|line| {
		serde_json::from_str::<Value>(line).is_ok_and(|v| v.is_object() && v.get("level").is_some())
	});
	assert!(has_json_log, "no JSON log line with a \"level\" field:\n{output}");
	common::assert_no_panic(&output);
}

/// `--transport pci`: direct-kernel boot over the PCI virtio transport
/// (`x86_64` only; the UEFI test covers PCI only via firmware boot).
#[test]
fn boots_with_pci_transport() {
	if !common::require_hv() || !common::supports_pci() {
		return;
	}

	let mut args = common::base_args("boot");
	args.push("--transport".into());
	args.push("pci".into());

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "VMON_OK", Duration::from_mins(1));

	assert!(output.contains("VMON_OK"), "boot marker missing:\n{output}");
	common::assert_no_panic(&output);
}

/// `--ksm`: guest RAM is advised `MADV_MERGEABLE` and the metrics counter
/// reports it. Uses the long-lived soak mode so the control query cannot race
/// guest poweroff; semantic KSM merging is host-policy dependent and out of
/// scope (boot + counter only).
#[test]
fn boots_with_ksm_enabled() {
	if !common::require_hv() || !common::supports_linux_isolation() {
		return;
	}

	let dir = common::test_dir("boot-ksm");
	let sock = dir.join("control.sock");
	let mut args = common::base_args("soak");
	args.push("--ksm".into());
	args.push("--api-sock".into());
	args.push(sock.display().to_string());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SOAK_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	let deadline = Instant::now() + Duration::from_secs(10);
	let metrics = loop {
		let metrics = control.request("metrics", json!({}));
		let advised = metrics["ksm"]["regions_advised"].as_u64().unwrap_or(0);
		if advised >= 1 || Instant::now() >= deadline {
			break metrics;
		}
		thread::sleep(Duration::from_millis(200));
	};
	assert!(
		metrics["ksm"]["regions_advised"].as_u64().unwrap_or(0) >= 1,
		"ksm.regions_advised never reached 1: {metrics}"
	);

	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}
