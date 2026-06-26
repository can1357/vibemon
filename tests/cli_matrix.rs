//! Cross-environment CLI capability matrix.
//!
//! These assert the binary rejects unsupported flag combinations on the current
//! host with a clear error. Unlike the boot tests they need no hypervisor —
//! `Config` validation runs before any VM is created — so they execute on every
//! platform under a plain `cargo test`, locking in the per-environment support
//! matrix (KVM/HVF, `x86_64/aarch64`, TAP/user-net) the README documents.

mod common;

use std::process::Stdio;

/// Run the binary with `args`, expecting a non-zero exit whose stderr contains
/// `needle`. Config rejections never touch the hypervisor, so this is hermetic.
fn assert_cli_rejects(args: &[&str], needle: &str) {
	let output = common::vmm()
		.args(args)
		.stdin(Stdio::null())
		.output()
		.unwrap_or_else(|e| panic!("spawning vmon {args:?}: {e}"));
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "expected {args:?} to fail; stderr:\n{stderr}");
	assert!(stderr.contains(needle), "expected error {needle:?} for {args:?}; got:\n{stderr}");
}

/// `--net user` and `--tap` are mutually exclusive on every host.
#[test]
fn user_net_conflicts_with_tap() {
	assert_cli_rejects(
		&["--kernel", "k", "--net", "user", "--tap", "tap0"],
		"cannot be combined with --tap",
	);
}

/// UEFI boot needs operator-supplied firmware everywhere.
#[test]
fn uefi_requires_firmware() {
	assert_cli_rejects(&["--boot-mode", "uefi"], "requires --firmware");
}

/// User-mode NAT is the macOS/HVF networking path; KVM hosts must use TAP.
#[cfg(not(target_os = "macos"))]
#[test]
fn user_net_rejected_off_macos() {
	assert_cli_rejects(&["--kernel", "k", "--net", "user"], "only on macOS");
}

/// PCI virtio transport is x86_64-only; aarch64 (HVF and Lima/KVM) uses MMIO.
#[cfg(not(target_arch = "x86_64"))]
#[test]
fn pci_transport_rejected_off_x86_64() {
	assert_cli_rejects(&["--kernel", "k", "--transport", "pci"], "only supported on x86_64");
}
