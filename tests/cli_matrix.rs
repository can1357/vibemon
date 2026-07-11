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

/// Invalid tracing filters should be rejected instead of silently falling back.
#[test]
fn invalid_log_level_is_reported() {
	assert_cli_rejects(
		&["--kernel", "k", "--no-sandbox", "--log-level", "???"],
		"invalid --log-level",
	);
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

/// The volume fanout is hard-capped at 8 mounts.
#[test]
fn ninth_volume_rejected() {
	let volumes: Vec<String> = (1..=9).map(|i| format!("tag{i}:/srv/v{i}")).collect();
	let mut args = vec!["--kernel".to_string(), "k".to_string()];
	for volume in &volumes {
		args.push("--volume".to_string());
		args.push(volume.clone());
	}
	let refs: Vec<&str> = args.iter().map(String::as_str).collect();
	assert_cli_rejects(&refs, "at most 8 --volume mounts are supported");
}

/// Volume tags are guest mount tags and must be unique.
#[test]
fn duplicate_volume_tag_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--volume", "dup:/srv/a", "--volume", "dup:/srv/b"],
		"--volume tags must be unique",
	);
}

/// Remote virtio-fs tags share the guest namespace with host-backed volumes.
#[test]
fn remote_fs_duplicate_tag_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--volume", "data:/srv/data", "--remote-fs", "data:/tmp/vmon-s3.sock"],
		"--volume tags must be unique",
	);
}

/// Proxy-backed virtio-fs tags follow the same restricted tag syntax as
/// volumes.
#[test]
fn remote_fs_bad_tag_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--remote-fs", "Tag!:/tmp/vmon-s3.sock"],
		"--remote-fs tag Tag! must match",
	);
}

/// Remote proxy mounts require an explicit tag-to-socket separator.
#[test]
fn remote_fs_missing_separator_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--remote-fs", "data"],
		"expected <tag>:<absolute-socket>",
	);
}

/// The VMM sandbox and jail can only expose absolute proxy socket paths.
#[test]
fn remote_fs_relative_socket_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--remote-fs", "data:relative.sock"],
		"--remote-fs socket path must be absolute",
	);
}

/// A virtio-fs share needs both the guest tag and the host directory.
#[test]
fn fs_tag_without_dir_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--fs-tag", "host"],
		"--fs-tag and --fs-dir must be used together",
	);
}

/// cgroup limits only exist inside the jail.
#[test]
fn cgroup_flag_without_jail_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--cgroup-pids-max", "64"],
		"--cgroup-cpu-max/--cgroup-mem-max/--cgroup-pids-max require --jail",
	);
}

/// A cold-boot `--agent-exec` hand-off waits for agent readiness on the agent
/// channel, so it needs `--agent-sock`; warm boots (`--restore`/`--fork-from`)
/// deliver immediately and are exempt.
#[test]
fn cold_agent_exec_without_agent_sock_rejected() {
	assert_cli_rejects(
		&["--kernel", "k", "--agent-exec", "id"],
		"--agent-exec on a cold boot requires --agent-sock",
	);
}
