//! `--jail` isolation smoke: cgroup limits, `--cgroup-mode off`, and operator
//! netns joining. All require Linux root (`--jail` refuses otherwise); run via
//! `just smoke-jail`. Jail arguments are constructed explicitly on a raw
//! command so the `VMON_JAIL=1` auto-jail extras in `common::vmm()` never
//! duplicate them.

mod common;

use std::{
	fs,
	os::unix::fs::MetadataExt,
	path::{Path, PathBuf},
	process::Command,
	time::{Duration, Instant},
};

struct JailSpec {
	args: Vec<String>,
	id:   String,
}

fn jail_spec(test: &str, mode: &str) -> JailSpec {
	let dir = common::test_dir(&format!("jail-{test}"));
	let jail_root = dir.join("root");
	fs::create_dir_all(&jail_root)
		.unwrap_or_else(|e| panic!("creating jail root {}: {e}", jail_root.display()));
	let jail_root = fs::canonicalize(&jail_root)
		.unwrap_or_else(|e| panic!("canonicalizing {}: {e}", jail_root.display()));
	let id = format!("smoke-{test}-{}", std::process::id());
	let uid = std::env::var("SUDO_UID").unwrap_or_else(|_| "65534".to_string());
	let gid = std::env::var("SUDO_GID").unwrap_or_else(|_| "65534".to_string());
	let mut args = common::base_args(mode);
	args.push("--jail".into());
	args.push("--id".into());
	args.push(id.clone());
	args.push("--jail-root".into());
	args.push(jail_root.display().to_string());
	args.push("--sandbox-uid".into());
	args.push(uid);
	args.push("--sandbox-gid".into());
	args.push(gid);
	JailSpec { args, id }
}

fn cgroup_dir(id: &str) -> PathBuf {
	Path::new("/sys/fs/cgroup/vmon").join(id)
}

fn read_cgroup_value(dir: &Path, file: &str) -> String {
	let path = dir.join(file);
	fs::read_to_string(&path)
		.unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
		.trim()
		.to_string()
}

#[test]
fn jail_applies_cgroup_limits() {
	if !common::require_jail() {
		return;
	}

	let mut spec = jail_spec("cgroup", "soak");
	spec.args.push("--cgroup-pids-max".into());
	spec.args.push("64".into());
	spec.args.push("--cgroup-mem-max".into());
	spec.args.push("268435456".into());

	let refs = common::as_refs(&spec.args);
	let mut vm = common::spawn_command_with_input(common::vmm_raw(), &refs, b"");
	vm.wait_for("SOAK_READY", Duration::from_mins(1));

	let cgroup = cgroup_dir(&spec.id);
	assert!(cgroup.is_dir(), "expected jail cgroup at {}", cgroup.display());
	assert_eq!(read_cgroup_value(&cgroup, "pids.max"), "64");
	assert_eq!(read_cgroup_value(&cgroup, "memory.max"), "268435456");

	vm.kill();
	common::assert_no_panic(&vm.output());
}

#[test]
fn jail_cgroup_off_creates_no_cgroup() {
	if !common::require_jail() {
		return;
	}

	let mut spec = jail_spec("cgroup-off", "soak");
	spec.args.push("--cgroup-mode".into());
	spec.args.push("off".into());

	let refs = common::as_refs(&spec.args);
	let mut vm = common::spawn_command_with_input(common::vmm_raw(), &refs, b"");
	vm.wait_for("SOAK_READY", Duration::from_mins(1));

	let cgroup = cgroup_dir(&spec.id);
	assert!(!cgroup.exists(), "--cgroup-mode off still created a cgroup at {}", cgroup.display());

	vm.kill();
	common::assert_no_panic(&vm.output());
}

/// The jailed VM child joins an operator-created network namespace: its
/// `/proc/<child>/ns/net` inode must match the bind-mounted netns file.
#[test]
fn jail_joins_operator_netns() {
	if !common::require_jail() {
		return;
	}

	let netns_name = format!("vmon-smoke-{}", std::process::id());
	let netns_path = PathBuf::from("/run/netns").join(&netns_name);
	let added = Command::new("ip")
		.args(["netns", "add", &netns_name])
		.status();
	match added {
		Ok(status) if status.success() => {},
		Ok(status) => {
			eprintln!("SKIP jail_joins_operator_netns: `ip netns add` exited with {status}");
			return;
		},
		Err(e) => {
			eprintln!("SKIP jail_joins_operator_netns: `ip` unavailable: {e}");
			return;
		},
	}

	// Everything below must reach the `ip netns del` cleanup, so run it in a
	// closure and clean up before propagating a panic.
	let result = std::panic::catch_unwind(|| {
		let mut spec = jail_spec("netns", "soak");
		spec.args.push("--netns".into());
		spec.args.push(netns_path.display().to_string());

		let refs = common::as_refs(&spec.args);
		let mut vm = common::spawn_command_with_input(common::vmm_raw(), &refs, b"");
		vm.wait_for("SOAK_READY", Duration::from_mins(1));

		let child = jail_child_pid(vm.pid());
		let child_ns = fs::metadata(format!("/proc/{child}/ns/net"))
			.unwrap_or_else(|e| panic!("stat /proc/{child}/ns/net: {e}"));
		let netns =
			fs::metadata(&netns_path).unwrap_or_else(|e| panic!("stat {}: {e}", netns_path.display()));
		assert_eq!(
			child_ns.ino(),
			netns.ino(),
			"jail child pid {child} is not in netns {netns_name}"
		);

		vm.kill();
		common::assert_no_panic(&vm.output());
	});

	let _ = Command::new("ip")
		.args(["netns", "del", &netns_name])
		.status();
	if let Err(panic) = result {
		std::panic::resume_unwind(panic);
	}
}

/// The jailer parent forks the VM child into the jail; resolve it via the
/// parent's `children` proc file (retrying while the fork is in flight).
fn jail_child_pid(parent: u32) -> u32 {
	let path = format!("/proc/{parent}/task/{parent}/children");
	let deadline = Instant::now() + Duration::from_secs(10);
	loop {
		if let Ok(children) = fs::read_to_string(&path)
			&& let Some(child) = children.split_whitespace().next()
		{
			return child
				.parse()
				.unwrap_or_else(|e| panic!("parsing jail child pid {child:?}: {e}"));
		}
		assert!(Instant::now() < deadline, "jail child of pid {parent} never appeared");
		std::thread::sleep(Duration::from_millis(50));
	}
}
