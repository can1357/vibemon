use std::{
	fs,
	io::{Read, Write},
	os::unix::{
		fs::{DirBuilderExt, PermissionsExt},
		net::UnixStream,
	},
	path::{Path, PathBuf},
	process::{Child, Command, Stdio},
	thread,
	time::{Duration, Instant},
};

use serde_json::{Value, json};
use vmon_proto::v1 as pb;

use crate::common;

const EXTRA_PATH: &str =
	"/opt/homebrew/bin:/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/bin:/usr/sbin:/sbin";

pub fn require_lifecycle_e2e() -> bool {
	if std::env::var("VMON_LIFECYCLE_E2E").as_deref() != Ok("1") {
		eprintln!("SKIP lifecycle_regression: VMON_LIFECYCLE_E2E=1 is not set");
		return false;
	}
	if !common::require_hv() {
		eprintln!("SKIP lifecycle_regression: VMON_E2E=1 and a usable hypervisor are required");
		return false;
	}
	for tool in ["skopeo", "umoci", "mkfs.ext4"] {
		if !have_tool(tool) {
			eprintln!("SKIP lifecycle_regression: {tool} not found on PATH");
			return false;
		}
	}
	true
}

pub fn require_portable_e2e() -> bool {
	if !require_lifecycle_e2e() {
		return false;
	}
	if std::env::var("VMON_LIFECYCLE_PORTABLE_E2E").as_deref() != Ok("1") {
		eprintln!("SKIP lifecycle_regression: VMON_LIFECYCLE_PORTABLE_E2E=1 is not set");
		return false;
	}
	for name in [
		"VMON_POSTGRES_URL",
		"VMON_S3_ENDPOINT",
		"VMON_S3_BUCKET",
		"VMON_S3_REGION",
		"VMON_S3_ACCESS_KEY",
		"VMON_S3_SECRET_KEY",
		"VMON_PORTABLE_HISTORY_KEY_ID",
		"VMON_PORTABLE_HISTORY_KEY_MATERIAL",
	] {
		if std::env::var_os(name).is_none() {
			eprintln!("SKIP lifecycle_regression: {name} is required for a real PostgreSQL/S3 run");
			return false;
		}
	}
	true
}

pub fn short_home(label: &str) -> PathBuf {
	let home = PathBuf::from(format!("/tmp/vlr-{label}-{}", std::process::id()));
	fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(&home)
		.expect("create lifecycle test home");
	home
}

pub fn production_env() -> Vec<(&'static str, String)> {
	let mut env = vec![("VMON_CLUSTER_MODE", "production".to_owned())];
	for name in [
		"VMON_POSTGRES_URL",
		"VMON_S3_ENDPOINT",
		"VMON_S3_BUCKET",
		"VMON_S3_REGION",
		"VMON_S3_ACCESS_KEY",
		"VMON_S3_SECRET_KEY",
		"VMON_S3_PREFIX",
		"VMON_PORTABLE_HISTORY_KEY_ID",
	] {
		if let Ok(value) = std::env::var(name) {
			env.push((name, value));
		}
	}
	env
}

pub struct Server {
	child: Child,
	home:  PathBuf,
	sock:  PathBuf,
	log:   PathBuf,
	env:   Vec<(&'static str, String)>,
}

impl Server {
	pub fn start(home: PathBuf, env: Vec<(&'static str, String)>) -> Self {
		let log = home.join("lifecycle-regression.log");
		let sock = home.join("vmond.sock");
		provision_shared_portable_key(&home);
		let child = spawn_server(&home, &log, &env);
		let server = Self { child, home, sock, log, env };
		server.wait_healthy(Duration::from_mins(1));
		server
	}

	pub fn restart(&mut self) {
		self.kill_hard();
		self.child = spawn_server(&self.home, &self.log, &self.env);
		self.wait_healthy(Duration::from_mins(1));
	}

	pub fn kill_hard(&mut self) {
		// SAFETY: target is the process owned by this fixture.
		unsafe { libc::kill(self.child.id() as i32, libc::SIGKILL) };
		let _ = self.child.wait();
	}

	pub fn socket(&self) -> PathBuf {
		self.sock.clone()
	}

	pub fn grpc(&self) -> common::api::Grpc {
		common::api::Grpc::connect_uds(&self.sock)
			.unwrap_or_else(|error| panic!("{error}; server log tail:\n{}", self.log_tail()))
	}

	pub fn log_tail(&self) -> String {
		let text = fs::read_to_string(&self.log).unwrap_or_default();
		let lines: Vec<_> = text.lines().collect();
		lines[lines.len().saturating_sub(40)..].join("\n")
	}

	fn wait_healthy(&self, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		loop {
			if self.sock.exists()
				&& let Ok((200, body)) = self.try_http("GET", "/healthz")
				&& body.get("ok").and_then(Value::as_bool) == Some(true)
			{
				return;
			}
			assert!(
				Instant::now() < deadline,
				"server never became healthy; log tail:\n{}",
				self.log_tail()
			);
			thread::sleep(Duration::from_millis(200));
		}
	}

	fn try_http(&self, method: &str, path: &str) -> std::io::Result<(u16, Value)> {
		let mut stream = UnixStream::connect(&self.sock)?;
		stream.write_all(
			format!("{method} {path} HTTP/1.1\r\nHost: vmon\r\nConnection: close\r\n\r\n").as_bytes(),
		)?;
		let mut response = Vec::new();
		stream.read_to_end(&mut response)?;
		let split = response
			.windows(4)
			.position(|bytes| bytes == b"\r\n\r\n")
			.ok_or_else(|| std::io::Error::other("health response has no body separator"))?;
		let status = String::from_utf8_lossy(&response[..split])
			.split_whitespace()
			.nth(1)
			.and_then(|status| status.parse().ok())
			.ok_or_else(|| std::io::Error::other("invalid health response status"))?;
		let body = serde_json::from_slice(&response[split + 4..]).unwrap_or(Value::Null);
		Ok((status, body))
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		if self.child.try_wait().ok().flatten().is_none() {
			// SAFETY: target is the process owned by this fixture.
			unsafe { libc::kill(self.child.id() as i32, libc::SIGKILL) };
			let _ = self.child.wait();
		}
	}
}

pub fn create(server: &Server, name: &str) -> String {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(
			sandboxes.create(pb::CreateSandboxRequest {
				spec_json: json!({
					"name": name,
					"image": std::env::var("VMON_E2E_IMAGE").unwrap_or_else(|_| "alpine:latest".to_owned()),
					"block_network": true,
					"memory": 256,
				})
				.to_string(),
			}),
		)
		.unwrap_or_else(|status| {
			panic!(
				"create failed: {}; log tail:\n{}",
				common::api::status_detail(&status),
				server.log_tail()
			)
		})
		.into_inner();
	serde_json::from_str::<Value>(&view.json).expect("create view JSON")["id"]
		.as_str()
		.expect("created sandbox id")
		.to_owned()
}

pub fn view(server: &Server, id: &str) -> Value {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let result = grpc
		.block_on(sandboxes.get(pb::SandboxRef { id: id.to_owned() }))
		.unwrap_or_else(|status| panic!("get {id} failed: {}", common::api::status_detail(&status)))
		.into_inner();
	serde_json::from_str(&result.json).expect("sandbox view JSON")
}

pub fn exec(server: &Server, id: &str, command: &str) -> String {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let response = grpc
		.block_on(sandboxes.exec_capture(pb::ExecCaptureRequest {
			id:   id.to_owned(),
			exec: Some(pb::ExecStart {
				cmd: vec!["/bin/sh".into(), "-c".into(), command.into()],
				timeout: Some(30.0),
				..Default::default()
			}),
		}))
		.unwrap_or_else(|status| {
			panic!("exec {command:?} failed: {}", common::api::status_detail(&status))
		})
		.into_inner();
	assert_eq!(
		response.code,
		0,
		"guest command {command:?} failed: {}",
		String::from_utf8_lossy(&response.stderr)
	);
	String::from_utf8_lossy(&response.stdout).into_owned()
}

pub fn history(server: &Server, id: &str) -> Vec<pb::RecoveryPoint> {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	grpc
		.block_on(sandboxes.history(pb::SandboxRef { id: id.to_owned() }))
		.unwrap_or_else(|status| panic!("history failed: {}", common::api::status_detail(&status)))
		.into_inner()
		.points
}

fn provision_shared_portable_key(home: &Path) {
	let (Ok(key_id), Ok(material)) = (
		std::env::var("VMON_PORTABLE_HISTORY_KEY_ID"),
		std::env::var("VMON_PORTABLE_HISTORY_KEY_MATERIAL"),
	) else {
		return;
	};
	assert_eq!(material.len(), 64, "portable history key material must be 32 bytes of hexadecimal");
	assert!(
		material.bytes().all(|byte| byte.is_ascii_hexdigit()),
		"portable history key material must be hexadecimal"
	);
	let dir = home.join("security/keys");
	fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(&dir)
		.expect("create shared key directory");
	fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
		.expect("protect shared key directory");
	let key = dir.join(format!("{key_id}.key"));
	fs::write(&key, material).expect("provision shared portable key");
	fs::set_permissions(key, fs::Permissions::from_mode(0o600))
		.expect("protect shared portable key");
}
fn spawn_server(home: &Path, log: &Path, env: &[(&'static str, String)]) -> Child {
	let log_file = fs::OpenOptions::new()
		.create(true)
		.append(true)
		.open(log)
		.expect("open lifecycle server log");
	let mut command = Command::new(env!("CARGO_BIN_EXE_vmon"));
	command
		.arg("serve")
		.arg("--home")
		.arg(home)
		.env("PATH", tool_path())
		.stdin(Stdio::null())
		.stdout(log_file.try_clone().expect("clone log"))
		.stderr(log_file);
	for (name, value) in env {
		command.env(name, value);
	}
	if let Some(kernel) = cached_kernel() {
		command.env("VMON_KERNEL", kernel);
	}
	command.spawn().expect("spawn vmon serve")
}

fn have_tool(name: &str) -> bool {
	tool_path()
		.split(':')
		.any(|dir| !dir.is_empty() && Path::new(dir).join(name).is_file())
}
fn tool_path() -> String {
	format!("{EXTRA_PATH}:{}", std::env::var("PATH").unwrap_or_default())
}
fn cached_kernel() -> Option<PathBuf> {
	let name = if cfg!(target_arch = "aarch64") {
		"Image-aarch64"
	} else {
		"bzImage-x86_64"
	};
	Some(
		PathBuf::from(std::env::var("HOME").ok()?)
			.join(".vmon/assets")
			.join(name),
	)
	.filter(|path| path.is_file())
}
