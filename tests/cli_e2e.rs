//! CLI acceptance suite: drives the real `vmon` binary against `vmon serve`
//! through the Phase 3 command surface, porting every scenario from
//! `python/cli_e2e.py`.
//!
//! Gated by `VMON_E2E=1` + a usable hypervisor + OCI image tools. This is a
//! serial driver by design: the daemon lifecycle is part of the contract and
//! the scenarios intentionally share one short `$VMON_HOME` so the image
//! template is built once.

mod common;

use std::{
	collections::HashSet,
	ffi::{OsStr, OsString},
	fs,
	io::{Read, Write},
	net::{TcpListener, TcpStream},
	os::unix::{fs::DirBuilderExt, process::ExitStatusExt},
	path::{Path, PathBuf},
	process::{Command, Stdio},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

const BOOT_TIMEOUT: Duration = Duration::from_mins(5);
const FAST_TIMEOUT: Duration = Duration::from_mins(1);
const POLL_INTERVAL: Duration = Duration::from_millis(500);
const EXTRA_PATH: &str =
	"/opt/homebrew/bin:/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/bin:/usr/sbin:/sbin";

fn tool_path() -> String {
	let inherited = std::env::var("PATH").unwrap_or_default();
	format!("{EXTRA_PATH}:{inherited}")
}

fn have_tool(name: &str) -> bool {
	tool_path()
		.split(':')
		.any(|dir| !dir.is_empty() && Path::new(dir).join(name).is_file())
}

fn require_cli_e2e() -> bool {
	if !common::require_hv() {
		eprintln!("SKIP cli_e2e: set VMON_E2E=1 on a host with KVM/HVF to run CLI e2e tests");
		return false;
	}
	for tool in ["skopeo", "umoci", "mkfs.ext4"] {
		if !have_tool(tool) {
			eprintln!("SKIP cli_e2e: {tool} not found on PATH");
			return false;
		}
	}
	true
}

fn prepared_image_ref(home: &Path) -> String {
	if let Ok(image) = std::env::var("VMON_E2E_IMAGE") {
		return image;
	}
	let layout = home.join("oci");
	let target = format!("oci:{}:cli-e2e", layout.display());
	let mut last = String::new();
	let arch = if cfg!(target_arch = "aarch64") {
		"arm64"
	} else {
		"amd64"
	};
	for attempt in 1..=3 {
		let output = Command::new("skopeo")
			.args([
				"copy",
				"--override-os",
				"linux",
				"--override-arch",
				arch,
				"docker://alpine:latest",
				&target,
			])
			.env("PATH", tool_path())
			.output()
			.unwrap_or_else(|e| panic!("spawning skopeo copy for CLI e2e: {e}"));
		if output.status.success() {
			return target;
		}
		last = format!(
			"attempt {attempt} exit {:?}\nstdout:\n{}\nstderr:\n{}",
			output.status.code(),
			String::from_utf8_lossy(&output.stdout),
			String::from_utf8_lossy(&output.stderr),
		);
		thread::sleep(Duration::from_secs(1));
	}
	panic!("preparing local CLI e2e image failed:\n{last}");
}

#[test]
fn phase3_cli_contract() {
	if !require_cli_e2e() {
		return;
	}

	let mut h = CliHarness::new();
	t_daemon_lifecycle(&h);
	t_run_foreground(&h);
	t_run_exit_code(&h);
	t_detach_ps_exec_stop_rm(&mut h);
	t_run_networked(&mut h);
	t_shell_ephemeral(&h);
	t_shell_attach(&mut h);
	t_cp(&mut h);
	t_logs(&mut h);
	t_pause_resume(&mut h);
	t_durable_lifecycle(&mut h);
	t_snapshot_restore(&mut h);
	t_fork(&mut h);
	t_daemon_stop(&mut h);
}

struct CliHarness {
	home:    PathBuf,
	image:   String,
	run_id:  String,
	seq:     u64,
	created: HashSet<String>,
}

impl CliHarness {
	fn new() -> Self {
		let home = PathBuf::from(format!("/tmp/vc{}", std::process::id()));
		let _ = fs::remove_dir_all(&home);
		fs::DirBuilder::new()
			.recursive(true)
			.mode(0o700)
			.create(&home)
			.unwrap_or_else(|e| panic!("creating {}: {e}", home.display()));
		let image = prepared_image_ref(&home);
		Self {
			home,
			image,
			run_id: format!("clie2e{}", std::process::id()),
			seq: 0,
			created: HashSet::new(),
		}
	}

	fn uid(&mut self, prefix: &str) -> String {
		self.seq += 1;
		format!("{prefix}-{}-{}", self.run_id, self.seq)
	}

	fn cli<I, S>(&self, args: I, timeout: Duration) -> CliOutput
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>,
	{
		let argv: Vec<OsString> = args
			.into_iter()
			.map(|arg| arg.as_ref().to_os_string())
			.collect();
		let command = argv
			.iter()
			.map(|arg| arg.to_string_lossy().into_owned())
			.collect::<Vec<_>>()
			.join(" ");

		let mut command_proc = Command::new(env!("CARGO_BIN_EXE_vmon"));
		command_proc.args(&argv);
		for key in [
			"VMON_CONTEXT",
			"VMON_CONFIG",
			"VMON_API_TOKEN",
			"VMON_CLIENT_TOKEN",
			"VMON_SERVE_HOST",
			"VMON_SERVE_PORT",
			"VMON_TLS_CERT",
			"VMON_TLS_KEY",
			"VMON_IDLE_TIMEOUT",
			"VMON_REPLICATE_SEC",
			"VMON_REPLICAS",
			"VMON_REPLICATE_CONCURRENCY",
			"VMON_RESTORE_QUORUM",
			"VMON_WARM_POOL_SIZE",
			"VMON_WARM_IMAGES",
			"VMON_MESH_HEARTBEAT_SEC",
			"VMON_MESH_REAP_SEC",
			"VMON_MESH_IDEM_TTL_SEC",
			"VMON_MESH_CREATE_TIMEOUT_SEC",
			"VMON_MESH_W_WARM",
			"VMON_MESH_W_FREE",
			"VMON_MESH_W_LOCAL",
			"VMON_MESH_W_REGION",
			"VMON_MESH_W_INFLIGHT",
		] {
			command_proc.env_remove(key);
		}
		let mut child = command_proc
			.env("VMON_HOME", &self.home)
			.env("COLUMNS", "200")
			.env("PATH", tool_path())
			.stdin(Stdio::null())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.spawn()
			.unwrap_or_else(|e| panic!("spawning `vmon {command}`: {e}"));

		let deadline = Instant::now() + timeout;
		loop {
			if child
				.try_wait()
				.unwrap_or_else(|e| panic!("polling `vmon {command}`: {e}"))
				.is_some()
			{
				let output = child
					.wait_with_output()
					.unwrap_or_else(|e| panic!("collecting `vmon {command}` output: {e}"));
				return CliOutput::from_output(command, output);
			}
			if Instant::now() >= deadline {
				let _ = child.kill();
				let output = child
					.wait_with_output()
					.unwrap_or_else(|e| panic!("collecting timed-out `vmon {command}` output: {e}"));
				let timed_out = CliOutput::from_output(command, output);
				panic!(
					"`vmon {}` timed out after {:.0}s\n--stdout--\n{}\n--stderr--\n{}",
					timed_out.command,
					timeout.as_secs_f64(),
					timed_out.stdout,
					timed_out.stderr
				);
			}
			thread::sleep(Duration::from_millis(100));
		}
	}

	fn track(&mut self, name: &str) {
		self.created.insert(name.to_string());
	}

	fn untrack(&mut self, name: &str) {
		self.created.remove(name);
	}

	fn ps_status(&self, name: &str) -> Option<String> {
		let out = self.cli(["ps"], FAST_TIMEOUT);
		for line in out.stdout.lines() {
			let mut parts = line.split_whitespace();
			if parts.next() == Some(name) {
				return parts.next().map(ToString::to_string);
			}
		}
		None
	}

	fn wait_running(&self, name: &str, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		while Instant::now() < deadline {
			if self.ps_status(name).as_deref() == Some("running") {
				return;
			}
			thread::sleep(POLL_INTERVAL);
		}
		panic!("{name} never reached running in ps after detached run");
	}

	fn start_detached(&mut self, name: &str) {
		let entry = format!("echo ready-{}; sleep 300", self.run_id);
		let args = vec![
			"run".to_string(),
			"-d".to_string(),
			"--block-network".to_string(),
			"--name".to_string(),
			name.to_string(),
			self.image.clone(),
			"--".to_string(),
			"sh".to_string(),
			"-c".to_string(),
			entry,
		];
		self.cli(args, BOOT_TIMEOUT).assert_success();
		self.track(name);
		self.wait_running(name, Duration::from_secs(30));
	}

	fn exec_until(&self, name: &str, argv: &[&str], want: &str, timeout: Duration) -> String {
		let deadline = Instant::now() + timeout;
		let mut last = String::new();
		while Instant::now() < deadline {
			let mut args = vec!["exec".to_string(), name.to_string(), "--".to_string()];
			args.extend(argv.iter().map(|arg| (*arg).to_string()));
			let out = self.cli(args, FAST_TIMEOUT);
			if out.status == 0 && out.stdout.contains(want) {
				return out.stdout;
			}
			last = format!("rc={} out={:?} err={:?}", out.status, out.stdout, out.stderr);
			thread::sleep(Duration::from_secs(1));
		}
		panic!(
			"exec on {name} never produced {want:?} in {:.0}s; last: {last}",
			timeout.as_secs_f64()
		);
	}

	fn wait_daemon_stopped(&self, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		while Instant::now() < deadline {
			let out = self.cli(["daemon", "status"], FAST_TIMEOUT);
			let text = out.combined();
			if out.status != 0
				&& text.contains("not running")
				&& !self.home.join("vmond.sock").exists()
			{
				return;
			}
			thread::sleep(Duration::from_millis(200));
		}
		panic!("daemon still reachable after stop");
	}

	fn remove_if_tracked(&mut self, name: &str) {
		if self.created.contains(name) {
			let _ = self.cli(["rm", name], FAST_TIMEOUT);
			self.untrack(name);
		}
	}

	fn cleanup(&mut self) {
		let names = self.created.iter().cloned().collect::<Vec<_>>();
		for name in names {
			let _ = self.cli(["rm", name.as_str()], FAST_TIMEOUT);
			self.untrack(&name);
		}
		let _ = self.cli(["daemon", "stop"], FAST_TIMEOUT);
	}
}

impl Drop for CliHarness {
	fn drop(&mut self) {
		self.cleanup();
		if std::env::var("VMON_KEEP_CLI_E2E_HOME").as_deref() != Ok("1") {
			let _ = fs::remove_dir_all(&self.home);
		}
	}
}

struct CliOutput {
	command: String,
	status:  i32,
	stdout:  String,
	stderr:  String,
}

impl CliOutput {
	fn from_output(command: String, output: std::process::Output) -> Self {
		let status = output
			.status
			.code()
			.or_else(|| output.status.signal().map(|signal| -signal))
			.unwrap_or(-1);
		Self {
			command,
			status,
			stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
			stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
		}
	}

	fn assert_success(&self) {
		assert_eq!(
			self.status, 0,
			"`vmon {}` exited {}\n--stdout--\n{}\n--stderr--\n{}",
			self.command, self.status, self.stdout, self.stderr
		);
	}

	fn combined(&self) -> String {
		format!("{}{}", self.stdout, self.stderr)
	}
}

fn t_daemon_lifecycle(h: &CliHarness) {
	let _ = h.cli(["daemon", "stop"], FAST_TIMEOUT);
	h.wait_daemon_stopped(Duration::from_secs(10));
	let out = h.cli(["ps"], FAST_TIMEOUT);
	assert_eq!(out.status, 0, "ps auto-start failed: {:?}", out.combined());
	assert!(
		out.stdout.contains("NAME")
			|| out.stdout.contains("no microVMs")
			|| out.stdout.contains("no sandboxes"),
		"unexpected ps output: {:?}",
		out.stdout
	);
	let status = h.cli(["daemon", "status"], FAST_TIMEOUT);
	assert!(
		status.status == 0
			&& status.stdout.contains("running")
			&& status.stdout.contains("vmond.sock"),
		"daemon status after auto-start: rc={} out={:?} err={:?}",
		status.status,
		status.stdout,
		status.stderr
	);
	assert!(h.home.join("vmond.sock").exists(), "vmond.sock missing after auto-start");
}

fn t_run_foreground(h: &CliHarness) {
	let marker = format!("hello-{}", h.run_id);
	let entry = format!("echo {marker}; uname -s");
	let out = h.cli(
		["run", "--block-network", h.image.as_str(), "--", "sh", "-c", entry.as_str()],
		BOOT_TIMEOUT,
	);
	assert_eq!(out.status, 0, "foreground run failed: {:?}", out.stderr);
	assert!(out.stdout.contains(&marker), "streamed stdout missing marker: {:?}", out.stdout);
	assert!(out.stdout.contains("Linux"), "uname -s not Linux: {:?}", out.stdout);
}

fn t_run_exit_code(h: &CliHarness) {
	let out =
		h.cli(["run", "--block-network", h.image.as_str(), "--", "sh", "-c", "exit 7"], BOOT_TIMEOUT);
	assert_eq!(
		out.status, 7,
		"expected entry exit code 7 through the daemon, got {} (stderr={:?})",
		out.status, out.stderr
	);
}

fn t_detach_ps_exec_stop_rm(h: &mut CliHarness) {
	let name = h.uid("vm");
	h.start_detached(&name);
	assert_eq!(h.ps_status(&name).as_deref(), Some("running"), "ps did not show {name} running");
	let entry = format!("echo execd-{}; pwd", h.run_id);
	let out = h.cli(["exec", name.as_str(), "--", "sh", "-lc", entry.as_str()], FAST_TIMEOUT);
	assert!(
		out.status == 0 && out.stdout.contains(&format!("execd-{}", h.run_id)),
		"exec rc={} out={:?} err={:?}",
		out.status,
		out.stdout,
		out.stderr
	);
	h.cli(["stop", name.as_str()], FAST_TIMEOUT)
		.assert_success();
	h.cli(["rm", name.as_str()], FAST_TIMEOUT).assert_success();
	h.untrack(&name);
	assert!(h.ps_status(&name).is_none(), "{name} still listed after rm");
}

#[allow(
	clippy::missing_const_for_fn,
	reason = "the Linux implementation calls a non-const geteuid capability probe"
)]
fn networking_supported() -> bool {
	common::supports_user_net() || linux_net_admin()
}

#[cfg(target_os = "linux")]
fn linux_net_admin() -> bool {
	// SAFETY: `geteuid` reads the effective user ID without dereferencing pointers.
	if unsafe { libc::geteuid() } == 0 {
		return true;
	}
	let Ok(status) = fs::read_to_string("/proc/self/status") else {
		return false;
	};
	status.lines().any(|line| {
		let Some(hex) = line.strip_prefix("CapEff:\t") else {
			return false;
		};
		u64::from_str_radix(hex.trim(), 16).is_ok_and(|bits| bits & (1 << 12) != 0)
	})
}

#[cfg(not(target_os = "linux"))]
const fn linux_net_admin() -> bool {
	false
}

fn default_gateway(route: &str) -> Option<String> {
	for line in route.lines() {
		let parts = line.split_whitespace().collect::<Vec<_>>();
		if parts.first() != Some(&"default") {
			continue;
		}
		if let Some(idx) = parts.iter().position(|part| *part == "via") {
			return parts.get(idx + 1).map(|gateway| (*gateway).to_string());
		}
		return Some(String::new());
	}
	None
}

fn t_run_networked(h: &mut CliHarness) {
	if !networking_supported() {
		eprintln!("SKIP cli_e2e::run_networked: needs macOS user-net or Linux root/CAP_NET_ADMIN");
		return;
	}

	let name = h.uid("net");
	let marker = format!("NET-OK-{}", h.run_id);
	let server = HostHttpServer::start(marker.as_bytes());
	let args = vec![
		"run".to_string(),
		"-d".to_string(),
		"--name".to_string(),
		name.clone(),
		h.image.clone(),
		"--".to_string(),
		"sh".to_string(),
		"-c".to_string(),
		"sleep 300".to_string(),
	];
	h.cli(args, BOOT_TIMEOUT).assert_success();
	h.track(&name);
	h.wait_running(&name, Duration::from_secs(30));

	let addr =
		h.exec_until(&name, &["ip", "-4", "addr", "show", "eth0"], "inet ", Duration::from_secs(15));
	let route = h.exec_until(&name, &["ip", "route"], "default", Duration::from_secs(15));
	let gateway = default_gateway(&route)
		.unwrap_or_else(|| panic!("could not parse default gateway: {route:?}"));

	if gateway == "10.0.2.2" {
		assert!(addr.contains("10.0.2.15"), "user-net IP missing from eth0: {addr:?}");
		let resolv = h.cli(["exec", name.as_str(), "--", "cat", "/etc/resolv.conf"], FAST_TIMEOUT);
		assert!(
			resolv.status == 0 && resolv.stdout.contains("10.0.2.3"),
			"user-net DNS missing: rc={} out={:?} err={:?}",
			resolv.status,
			resolv.stdout,
			resolv.stderr
		);
		let url = format!("http://10.0.2.2:{}/", server.port());
		let wget = format!("wget -T 8 -q -O- {url}");
		let fetched = h.cli(["exec", name.as_str(), "--", "sh", "-lc", wget.as_str()], FAST_TIMEOUT);
		assert!(
			fetched.status == 0 && fetched.stdout.contains(&marker),
			"network exec rc={} out={:?} err={:?}",
			fetched.status,
			fetched.stdout,
			fetched.stderr
		);
	} else {
		assert!(!gateway.is_empty(), "default route has no gateway: {route:?}");
	}

	h.remove_if_tracked(&name);
}

fn t_shell_ephemeral(h: &CliHarness) {
	let marker = format!("shelled-{}", h.run_id);
	let entry = format!("echo {marker}; uname -s");
	let out = h.cli(["shell", "--image", h.image.as_str(), "-c", entry.as_str()], BOOT_TIMEOUT);
	assert_eq!(out.status, 0, "shell rc={} err={:?}", out.status, out.stderr);
	assert!(out.stdout.contains(&marker), "shell stdout missing marker: {:?}", out.stdout);
	assert!(out.stdout.contains("Linux"), "uname -s not Linux: {:?}", out.stdout);
	let ps = h.cli(["ps"], FAST_TIMEOUT);
	assert!(
		!ps.stdout.contains(&marker) && !ps.stdout.contains("shell"),
		"ephemeral shell VM leaked:\n{}",
		ps.stdout
	);
}

fn t_shell_attach(h: &mut CliHarness) {
	let name = h.uid("sh");
	let marker = format!("attach-{}", h.run_id);
	h.start_detached(&name);
	let entry = format!("echo {marker}");
	let out = h.cli(["shell", name.as_str(), "-c", entry.as_str()], FAST_TIMEOUT);
	assert!(
		out.status == 0 && out.stdout.contains(&marker),
		"attach shell rc={} out={:?} err={:?}",
		out.status,
		out.stdout,
		out.stderr
	);
	assert_eq!(
		h.ps_status(&name).as_deref(),
		Some("running"),
		"{name} not running after attach shell"
	);
	h.remove_if_tracked(&name);
}

fn t_cp(h: &mut CliHarness) {
	let name = h.uid("cp");
	h.start_detached(&name);
	let work = h.home.join("cp-work");
	fs::create_dir_all(&work).unwrap_or_else(|e| panic!("creating {}: {e}", work.display()));
	let payload = format!("cli-cp-{}", h.run_id);
	let src = work.join("up.txt");
	fs::write(&src, &payload).unwrap_or_else(|e| panic!("writing {}: {e}", src.display()));
	let guest = format!("{name}:/root/up.txt");
	h.cli(["cp", src.to_str().expect("utf-8 test path"), guest.as_str()], FAST_TIMEOUT)
		.assert_success();
	let cat = h.cli(["exec", name.as_str(), "--", "cat", "/root/up.txt"], FAST_TIMEOUT);
	assert!(
		cat.status == 0 && cat.stdout.contains(&payload),
		"guest content: rc={} out={:?} err={:?}",
		cat.status,
		cat.stdout,
		cat.stderr
	);
	let dst = work.join("down.txt");
	h.cli(["cp", guest.as_str(), dst.to_str().expect("utf-8 test path")], FAST_TIMEOUT)
		.assert_success();
	let downloaded =
		fs::read_to_string(&dst).unwrap_or_else(|e| panic!("reading {}: {e}", dst.display()));
	assert_eq!(downloaded, payload, "download mismatch");
	h.remove_if_tracked(&name);
}

fn t_logs(h: &mut CliHarness) {
	let name = h.uid("logs");
	h.start_detached(&name);
	let out = h.cli(["logs", name.as_str()], FAST_TIMEOUT);
	assert_eq!(out.status, 0, "logs failed: {:?}", out.stderr);
	assert_eq!(h.ps_status(&name).as_deref(), Some("running"), "logs should not stop a running VM");
	h.cli(["stop", name.as_str()], FAST_TIMEOUT)
		.assert_success();
	h.remove_if_tracked(&name);
}

fn t_pause_resume(h: &mut CliHarness) {
	let name = h.uid("pr");
	h.start_detached(&name);
	h.cli(["pause", name.as_str()], FAST_TIMEOUT)
		.assert_success();
	h.cli(["resume", name.as_str()], FAST_TIMEOUT)
		.assert_success();
	let entry = format!("echo alive-{}", h.run_id);
	let out = h.cli(["exec", name.as_str(), "--", "sh", "-lc", entry.as_str()], FAST_TIMEOUT);
	assert!(
		out.status == 0 && out.stdout.contains(&format!("alive-{}", h.run_id)),
		"exec after resume: out={:?} err={:?}",
		out.stdout,
		out.stderr
	);
	h.remove_if_tracked(&name);
}

fn t_durable_lifecycle(h: &mut CliHarness) {
	let name = h.uid("lifecycle");
	let initial = format!("before-{}", h.run_id);
	let changed = format!("after-{}", h.run_id);
	h.start_detached(&name);
	h.cli(
		["exec", name.as_str(), "--", "sh", "-lc", &format!("echo {initial} > /root/state")],
		FAST_TIMEOUT,
	)
	.assert_success();
	h.cli(["suspend", name.as_str()], BOOT_TIMEOUT)
		.assert_success();

	let history = h.cli(["history", name.as_str()], FAST_TIMEOUT);
	assert_eq!(history.status, 0, "history failed: {:?}", history.stderr);
	let point = history
		.stdout
		.lines()
		.nth(1)
		.and_then(|line| line.split_whitespace().next())
		.expect("suspend should publish one recovery point")
		.to_owned();
	assert!(history.stdout.contains("checkpoint"), "unexpected history: {}", history.stdout);

	h.cli(["resume", name.as_str()], BOOT_TIMEOUT)
		.assert_success();
	h.exec_until(&name, &["cat", "/root/state"], &initial, Duration::from_secs(40));
	h.cli(
		["exec", name.as_str(), "--", "sh", "-lc", &format!("echo {changed} > /root/state")],
		FAST_TIMEOUT,
	)
	.assert_success();
	h.cli(["rollback", name.as_str(), point.as_str()], BOOT_TIMEOUT)
		.assert_success();
	h.exec_until(&name, &["cat", "/root/state"], &initial, Duration::from_secs(40));
	h.remove_if_tracked(&name);
}

fn t_snapshot_restore(h: &mut CliHarness) {
	let name = h.uid("snap");
	let tpl = h.uid("tpl");
	let restored = h.uid("rs");
	let marker = format!("snap-{}", h.run_id);
	h.start_detached(&name);
	let write_marker = format!("echo {marker} > /root/m && sync");
	h.cli(["exec", name.as_str(), "--", "sh", "-lc", write_marker.as_str()], FAST_TIMEOUT)
		.assert_success();
	h.cli(["snapshot", name.as_str(), tpl.as_str(), "--stop"], BOOT_TIMEOUT)
		.assert_success();
	h.cli(["rm", name.as_str()], FAST_TIMEOUT).assert_success();
	h.untrack(&name);
	h.cli(["restore", tpl.as_str(), "--name", restored.as_str(), "--agent", "-d"], BOOT_TIMEOUT)
		.assert_success();
	h.track(&restored);
	h.exec_until(&restored, &["cat", "/root/m"], &marker, Duration::from_secs(40));
	h.remove_if_tracked(&restored);
}

fn t_fork(h: &mut CliHarness) {
	let name = h.uid("fk");
	let tpl = h.uid("fktpl");
	h.start_detached(&name);
	h.cli(["snapshot", name.as_str(), tpl.as_str()], BOOT_TIMEOUT)
		.assert_success();
	h.cli(["rm", name.as_str()], FAST_TIMEOUT).assert_success();
	h.untrack(&name);
	let fork = h.cli(["fork", tpl.as_str(), "--count", "2"], BOOT_TIMEOUT);
	assert_eq!(fork.status, 0, "fork failed: {:?}", fork.stderr);
	let clones = fork
		.stdout
		.lines()
		.filter(|line| line.contains("(pid"))
		.filter_map(|line| line.split_whitespace().next().map(ToString::to_string))
		.collect::<Vec<_>>();
	assert_eq!(clones.len(), 2, "expected 2 clones, parsed {clones:?} from:\n{}", fork.stdout);
	for clone in &clones {
		h.track(clone);
	}
	let ps = h.cli(["ps"], FAST_TIMEOUT);
	for clone in &clones {
		assert!(ps.stdout.contains(clone), "clone {clone} not listed in ps:\n{}", ps.stdout);
	}
	for clone in clones {
		h.remove_if_tracked(&clone);
	}
}

fn t_daemon_stop(h: &mut CliHarness) {
	let names = h.created.iter().cloned().collect::<Vec<_>>();
	for name in names {
		h.remove_if_tracked(&name);
	}
	let out = h.cli(["daemon", "stop"], FAST_TIMEOUT);
	assert_eq!(out.status, 0, "daemon stop: {:?}", out.combined());
	h.wait_daemon_stopped(Duration::from_secs(10));
}

struct HostHttpServer {
	addr:   std::net::SocketAddr,
	stop:   Arc<AtomicBool>,
	handle: Option<thread::JoinHandle<()>>,
}

impl HostHttpServer {
	fn start(body: &[u8]) -> Self {
		let listener = TcpListener::bind(("127.0.0.1", 0)).expect("binding host HTTP listener");
		listener
			.set_nonblocking(true)
			.expect("setting host HTTP listener nonblocking");
		let addr = listener.local_addr().expect("host HTTP listener address");
		let body = body.to_vec();
		let stop = Arc::new(AtomicBool::new(false));
		let thread_stop = Arc::clone(&stop);
		let handle = thread::spawn(move || {
			while !thread_stop.load(Ordering::Relaxed) {
				match listener.accept() {
					Ok((mut stream, _)) => serve_http_response(&mut stream, &body),
					Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
						thread::sleep(Duration::from_millis(20));
					},
					Err(_) => break,
				}
			}
		});
		Self { addr, stop, handle: Some(handle) }
	}

	const fn port(&self) -> u16 {
		self.addr.port()
	}
}

impl Drop for HostHttpServer {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::Relaxed);
		let _ = TcpStream::connect(self.addr);
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}
	}
}

fn serve_http_response(stream: &mut TcpStream, body: &[u8]) {
	let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
	let mut request = [0_u8; 1024];
	let _ = stream.read(&mut request);
	let header =
		format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
	let _ = stream.write_all(header.as_bytes());
	let _ = stream.write_all(body);
}
