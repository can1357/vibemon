mod common;

use std::time::Duration;

use common::agent::AgentClient;
use serde_json::Value;

/// `--console-agent` + `--agent-sock`: host connects to the agent socket,
/// speaks the GC4 frame protocol, and runs a command in the guest.
#[test]
fn agent_handshake_and_exec_roundtrip() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("agent-roundtrip");
	let agent_sock = dir.join("agent.sock");
	let api_sock = dir.join("control.sock");
	let mut args = common::base_args("agent");
	args.push("--console-agent".into());
	args.push("--agent-sock".into());
	args.push(agent_sock.display().to_string());
	args.push("--api-sock".into());
	args.push(api_sock.display().to_string());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	let booted = vm.wait_for("AGENT_", Duration::from_mins(1));
	if booted.contains("AGENT_MISSING") {
		eprintln!(
			"SKIP agent_handshake_and_exec_roundtrip: guest agent not embedded in initramfs (build \
			 with `just agent-musl`, then re-run `just fetch-assets`)"
		);
		return;
	}

	let mut client = AgentClient::connect(&agent_sock, Duration::from_secs(10));
	let resp = client.ping(Duration::from_secs(30));
	assert_eq!(resp.get("ok"), Some(&Value::Bool(true)), "agent ping failed: {resp}");

	let (code, stdout) = client.exec(&["/bin/echo", "smoke"], Duration::from_secs(30));
	assert_eq!(code, 0, "guest echo exit code");
	assert_eq!(String::from_utf8_lossy(&stdout).trim(), "smoke");

	let mut control = common::ControlClient::connect(&api_sock, Duration::from_secs(10));
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}

/// `--agent-exec`: the boot-time entrypoint hand-off reaches the guest agent
/// once it announces readiness, and its output lands on the guest console.
#[test]
fn agent_exec_runs_command_at_boot() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("agent-boot-exec");
	let agent_sock = dir.join("agent.sock");
	let api_sock = dir.join("control.sock");
	let mut args = common::base_args("agent");
	args.push("--console-agent".into());
	args.push("--agent-sock".into());
	args.push(agent_sock.display().to_string());
	args.push("--api-sock".into());
	args.push(api_sock.display().to_string());
	args.push("--agent-exec".into());
	args.push("echo AGENT_EXEC_OK".into());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	let booted = vm.wait_for("AGENT_", Duration::from_mins(1));
	if booted.contains("AGENT_MISSING") {
		eprintln!(
			"SKIP agent_exec_runs_command_at_boot: guest agent not embedded in initramfs (build with \
			 `just agent-musl`, then re-run `just fetch-assets`)"
		);
		return;
	}

	vm.wait_for("AGENT_EXEC_OK", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&api_sock, Duration::from_secs(10));
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}
