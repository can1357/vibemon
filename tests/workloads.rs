mod common;

use std::{env, fs, path::PathBuf, time::Duration};

use common::agent::AgentClient;
use serde_json::Value;

struct GuestFile {
	path:    &'static str,
	content: &'static [u8],
	mode:    u32,
}

struct Workload {
	name:    &'static str,
	script:  &'static [u8],
	files:   &'static [GuestFile],
	marker:  &'static str,
	timeout: Duration,
}

const BROWSER_FILES: &[GuestFile] = &[GuestFile {
	path:    "/opt/vmon-workloads/browser-page.html",
	content: include_bytes!("../demo/guest-workloads/browser-page.html"),
	mode:    0o644,
}];

fn require_workload_e2e(name: &str) -> bool {
	if env::var("VMON_WORKLOAD_E2E").as_deref() != Ok("1") {
		eprintln!("SKIP {name}: VMON_WORKLOAD_E2E=1 is not set");
		return false;
	}
	if env::var("VMON_E2E").as_deref() != Ok("1") {
		eprintln!("SKIP {name}: VMON_E2E=1 is not set");
		return false;
	}
	if !common::require_hv() {
		eprintln!("SKIP {name}: usable KVM/HVF hypervisor unavailable");
		return false;
	}
	true
}

fn workload_initramfs() -> PathBuf {
	let path = env::var_os("VMON_WORKLOAD_INITRAMFS")
		.map(PathBuf::from)
		.expect(
			"VMON_WORKLOAD_INITRAMFS must name the image built by demo/build-workload-initramfs.sh",
		);
	assert!(path.is_file(), "VMON_WORKLOAD_INITRAMFS points at missing file {}", path.display());
	fs::canonicalize(&path)
		.unwrap_or_else(|error| panic!("canonicalizing {}: {error}", path.display()))
}

fn run_workload(workload: &Workload) {
	if !require_workload_e2e(workload.name) {
		return;
	}

	let dir = common::test_dir(workload.name);
	let agent_sock = dir.join("agent.sock");
	let api_sock = dir.join("control.sock");
	let args = vec![
		"--kernel".to_owned(),
		common::kernel().display().to_string(),
		"--initrd".to_owned(),
		workload_initramfs().display().to_string(),
		"--mem".to_owned(),
		"3072".to_owned(),
		"--cpus".to_owned(),
		"2".to_owned(),
		"--rng".to_owned(),
		"--cmdline".to_owned(),
		common::cmdline("agent"),
		"--console-agent".to_owned(),
		"--agent-sock".to_owned(),
		agent_sock.display().to_string(),
		"--api-sock".to_owned(),
		api_sock.display().to_string(),
	];

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	let _ = vm.wait_for("AGENT_INIT_OK", Duration::from_mins(2));

	let mut client = AgentClient::connect(&agent_sock, Duration::from_secs(10));
	let response = client.ping(Duration::from_secs(30));
	assert_eq!(response.get("ok"), Some(&Value::Bool(true)), "agent ping failed: {response}");

	let (code, output) =
		client.exec(&["/bin/mkdir", "-p", "/opt/vmon-workloads"], Duration::from_secs(10));
	assert_eq!(code, 0, "failed to create workload directory: {}", String::from_utf8_lossy(&output));

	let script_path = format!("/opt/vmon-workloads/{}.sh", workload.name);
	client.write_file(&script_path, workload.script, 0o755, Duration::from_secs(10));
	for file in workload.files {
		client.write_file(file.path, file.content, file.mode, Duration::from_secs(10));
	}

	let (exit_code, output) = client.exec(&[&script_path], workload.timeout);
	let output = String::from_utf8_lossy(&output);
	assert_ne!(exit_code, 77, "{} workload prerequisites are missing:\n{output}", workload.name);
	assert_eq!(
		exit_code, 0,
		"{} workload failed with exit code {exit_code}:\n{output}",
		workload.name
	);
	assert!(
		output.contains(workload.marker),
		"{} success marker is missing:\n{output}",
		workload.name
	);

	let mut control = common::ControlClient::connect(&api_sock, Duration::from_secs(10));
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}

#[test]
fn test_headless_chromium_workload() {
	run_workload(&Workload {
		name:    "browser",
		script:  include_bytes!("../demo/guest-workloads/browser.sh"),
		files:   BROWSER_FILES,
		marker:  "VMON_BROWSER_OK",
		timeout: Duration::from_secs(90),
	});
}

#[test]
fn test_docker_in_sandbox_workload() {
	run_workload(&Workload {
		name:    "docker",
		script:  include_bytes!("../demo/guest-workloads/docker.sh"),
		files:   &[],
		marker:  "VMON_DOCKER_OK",
		timeout: Duration::from_mins(2),
	});
}
