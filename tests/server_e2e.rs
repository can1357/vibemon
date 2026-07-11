//! Engine acceptance suite: drives a REAL `vmon serve` over its UDS with
//! real microVMs, porting the server-facing scenarios of `python/e2e.py`
//! (`t_exec`, `t_filesystem`, `t_snapshot_restore`, `t_fork`, `t_secrets`,
//! `t_warm_pool`, `t_timeout`, `t_extend`, plus rehydrate-after-kill).
//!
//! Gated by `VMON_E2E=1` + a usable hypervisor + skopeo/umoci/mkfs.ext4 on
//! PATH. All tests share one `$VMON_HOME` under `target/test-runs` so the
//! OCI template is built once; each test runs its own short-lived server
//! (`--test-threads=1`) so the home's owner lock is never contended.

mod common;

use std::{
	fs,
	io::{Read, Write},
	os::unix::{fs::DirBuilderExt, net::UnixStream},
	path::{Path, PathBuf},
	process::{Child, Command, Stdio},
	sync::LazyLock,
	time::{Duration, Instant},
};

use serde_json::{Value, json};
use vmon_proto::v1 as pb;

/// Extra PATH entries the spawned server needs for skopeo/umoci/mkfs.ext4.
const EXTRA_PATH: &str =
	"/opt/homebrew/bin:/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/bin:/usr/sbin:/sbin";

static HOME: LazyLock<PathBuf> = LazyLock::new(|| {
	// Keep the home SHORT: VM control/agent sockets live under
	// `<home>/vms/<name>/`, and macOS caps `sockaddr_un` paths at ~104 bytes
	// (SUN_LEN). A `target/test-runs/...` home overflows it.
	// (macOS `temp_dir()` is `/var/folders/...`, itself too long.)
	let dir = PathBuf::from(format!("/tmp/ve{}", std::process::id()));
	fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(&dir)
		.expect("creating e2e home");
	dir
});

fn tool_path() -> String {
	let inherited = std::env::var("PATH").unwrap_or_default();
	format!("{EXTRA_PATH}:{inherited}")
}

fn have_tool(name: &str) -> bool {
	tool_path()
		.split(':')
		.any(|dir| !dir.is_empty() && Path::new(dir).join(name).is_file())
}

/// Gate: hypervisor + OCI tooling; prints a SKIP reason when unmet.
fn require_server_e2e() -> bool {
	if !common::require_hv() {
		return false;
	}
	for tool in ["skopeo", "umoci", "mkfs.ext4"] {
		if !have_tool(tool) {
			eprintln!("SKIP server_e2e: {tool} not found on PATH");
			return false;
		}
	}
	true
}

fn e2e_image() -> String {
	std::env::var("VMON_E2E_IMAGE").unwrap_or_else(|_| "alpine:latest".into())
}

fn cached_kernel() -> Option<PathBuf> {
	let name = if cfg!(target_arch = "aarch64") {
		"Image-aarch64"
	} else {
		"bzImage-x86_64"
	};
	let home = std::env::var("HOME").ok()?;
	let path = Path::new(&home).join(".vmon/assets").join(name);
	path.is_file().then_some(path)
}

struct Server {
	child: Child,
	sock:  PathBuf,
	log:   PathBuf,
}

impl Server {
	fn start(home: &Path) -> Self {
		let log = home.join("server-e2e.log");
		let log_file = fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&log)
			.expect("open server log");
		let mut cmd = Command::new(env!("CARGO_BIN_EXE_vmon"));
		cmd.arg("serve")
			.arg("--home")
			.arg(home)
			.env("PATH", tool_path())
			.stdin(Stdio::null())
			.stdout(log_file.try_clone().expect("clone log handle"))
			.stderr(log_file);
		if let Some(kernel) = cached_kernel() {
			cmd.env("VMON_KERNEL", kernel);
		}
		let child = cmd.spawn().expect("spawn vmon serve");
		let server = Self { child, sock: home.join("vmond.sock"), log };
		server.wait_healthy(Duration::from_secs(30));
		server
	}

	fn wait_healthy(&self, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		loop {
			if self.sock.exists()
				&& let Ok((status, body)) = self.try_http("GET", "/healthz", None)
				&& status == 200
				&& body.get("ok").and_then(Value::as_bool) == Some(true)
			{
				return;
			}
			assert!(
				Instant::now() < deadline,
				"vmon serve never became healthy; log tail:\n{}",
				self.log_tail()
			);
			std::thread::sleep(Duration::from_millis(200));
		}
	}

	fn log_tail(&self) -> String {
		let text = fs::read_to_string(&self.log).unwrap_or_default();
		let lines: Vec<&str> = text.lines().collect();
		let start = lines.len().saturating_sub(40);
		lines[start..].join("\n")
	}

	fn try_http(
		&self,
		method: &str,
		path: &str,
		body: Option<(&str, &[u8])>,
	) -> std::io::Result<(u16, Value)> {
		let mut stream = UnixStream::connect(&self.sock)?;
		stream.set_read_timeout(Some(Duration::from_mins(5)))?;
		stream.set_write_timeout(Some(Duration::from_mins(1)))?;
		let (content_type, payload): (&str, &[u8]) = body.unwrap_or(("", b""));
		let mut request = format!("{method} {path} HTTP/1.1\r\nHost: vmon\r\nConnection: close\r\n");
		if !payload.is_empty() || body.is_some() {
			use std::fmt::Write as _;
			let _ = write!(
				request,
				"Content-Type: {content_type}\r\nContent-Length: {}\r\n",
				payload.len()
			);
		}
		request.push_str("\r\n");
		stream.write_all(request.as_bytes())?;
		stream.write_all(payload)?;
		let mut response = Vec::new();
		stream.read_to_end(&mut response)?;
		let split = response
			.windows(4)
			.position(|w| w == b"\r\n\r\n")
			.ok_or_else(|| std::io::Error::other("no header terminator"))?;
		let head = String::from_utf8_lossy(&response[..split]).into_owned();
		let status: u16 = head
			.split_whitespace()
			.nth(1)
			.and_then(|s| s.parse().ok())
			.ok_or_else(|| std::io::Error::other(format!("bad status line: {head}")))?;
		let raw_body = &response[split + 4..];
		let parsed = if raw_body.is_empty() {
			Value::Null
		} else {
			serde_json::from_slice(raw_body)
				.unwrap_or_else(|_| Value::String(String::from_utf8_lossy(raw_body).into_owned()))
		};
		Ok((status, parsed))
	}

	/// Fresh gRPC channel over the server UDS; panics with the log tail when
	/// the connect fails.
	fn grpc(&self) -> common::api::Grpc {
		common::api::Grpc::connect_uds(&self.sock)
			.unwrap_or_else(|e| panic!("{e}; server log tail:\n{}", self.log_tail()))
	}

	fn pid(&self) -> u32 {
		self.child.id()
	}

	fn kill_hard(&mut self) {
		// SAFETY: kill(2) on this live child's pid.
		unsafe {
			libc::kill(self.child.id() as i32, libc::SIGKILL);
		}
		let _ = self.child.wait();
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		// Graceful first so the UDS/pid file are removed, then hard.
		// SAFETY: signals target this live child's pid.
		unsafe {
			libc::kill(self.child.id() as i32, libc::SIGTERM);
		}
		let deadline = Instant::now() + Duration::from_secs(5);
		while Instant::now() < deadline {
			if matches!(self.child.try_wait(), Ok(Some(_))) {
				return;
			}
			std::thread::sleep(Duration::from_millis(50));
		}
		self.kill_hard();
	}
}

/// Create a block-network sandbox from the e2e image; first call in the
/// process pays the template build (generous server-side wait).
fn create_sandbox(server: &Server, extra: Value) -> Value {
	let mut body = json!({
		"image": e2e_image(),
		"block_network": true,
		"memory": 256,
	});
	if let (Value::Object(base), Value::Object(more)) = (&mut body, extra) {
		for (key, value) in more {
			base.insert(key, value);
		}
	}
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(sandboxes.create(pb::CreateSandboxRequest { spec_json: body.to_string() }))
		.unwrap_or_else(|status| {
			panic!(
				"create failed: {}; log tail:\n{}",
				common::api::status_detail(&status),
				server.log_tail()
			)
		})
		.into_inner();
	let view: Value = serde_json::from_str(&view.json).expect("create view JSON");
	assert!(view.get("id").and_then(Value::as_str).is_some(), "view missing id: {view}");
	view
}

fn sandbox_id(view: &Value) -> String {
	view["id"].as_str().expect("sandbox id").to_string()
}

/// Fetch a sandbox view document via `SandboxService.Get`.
fn sandbox_view(server: &Server, id: &str) -> Value {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(sandboxes.get(pb::SandboxRef { id: id.to_owned() }))
		.unwrap_or_else(|status| panic!("view fetch failed: {}", common::api::status_detail(&status)))
		.into_inner();
	serde_json::from_str(&view.json).expect("sandbox view JSON")
}

/// Run a command via `SandboxService.ExecCapture`; returns (exit, stdout,
/// stderr).
fn exec(server: &Server, id: &str, cmd: &[&str]) -> (i64, String, String) {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExecCaptureRequest {
		id:   id.to_owned(),
		exec: Some(pb::ExecStart {
			cmd: cmd.iter().map(|&part| part.to_owned()).collect(),
			timeout: Some(30.0),
			..Default::default()
		}),
	};
	let out = grpc
		.block_on(sandboxes.exec_capture(request))
		.unwrap_or_else(|status| {
			panic!("exec {cmd:?} failed: {}", common::api::status_detail(&status))
		})
		.into_inner();
	(
		out.code,
		String::from_utf8_lossy(&out.stdout).into_owned(),
		String::from_utf8_lossy(&out.stderr).into_owned(),
	)
}

/// Remove is idempotent for the tests: `Ok` or `NotFound` both pass.
fn remove_sandbox(server: &Server, id: &str) {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	if let Err(status) = grpc.block_on(sandboxes.remove(pb::SandboxRef { id: id.to_owned() })) {
		assert_eq!(
			status.code(),
			tonic::Code::NotFound,
			"remove {id} -> {}",
			common::api::status_detail(&status)
		);
	}
}

#[test]
fn create_exec_roundtrip() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);
	let view = create_sandbox(&server, json!({}));
	let id = sandbox_id(&view);

	let (exit, stdout, _) = exec(&server, &id, &["/bin/sh", "-c", "echo e2e-ok"]);
	assert_eq!(exit, 0);
	assert_eq!(stdout.trim(), "e2e-ok");

	// Env + workdir pass through the exec body.
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExecCaptureRequest {
		id:   id.clone(),
		exec: Some(pb::ExecStart {
			cmd: vec!["/bin/sh".into(), "-c".into(), "printf %s-%s \"$PWD\" \"$MARK\"".into()],
			env: std::collections::HashMap::from([("MARK".to_owned(), "m1".to_owned())]),
			workdir: Some("/tmp".to_owned()),
			..Default::default()
		}),
	};
	let out = grpc
		.block_on(sandboxes.exec_capture(request))
		.unwrap_or_else(|status| {
			panic!("exec with env failed: {}", common::api::status_detail(&status))
		})
		.into_inner();
	assert_eq!(String::from_utf8_lossy(&out.stdout), "/tmp-m1");

	remove_sandbox(&server, &id);
}

#[test]
fn files_roundtrip_binary_clean() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);
	let view = create_sandbox(&server, json!({}));
	let id = sandbox_id(&view);

	let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let write =
		pb::FileWriteRequest { id: id.clone(), path: "/tmp/bin".to_owned(), data: payload };
	grpc
		.block_on(sandboxes.file_write(write))
		.unwrap_or_else(|status| {
			panic!("file write failed: {}", common::api::status_detail(&status))
		});

	// Verify the write from inside the guest.
	let (exit, stdout, _) = exec(&server, &id, &["/bin/sh", "-c", "wc -c < /tmp/bin"]);
	assert_eq!(exit, 0);
	assert_eq!(stdout.trim(), "4096", "guest file size mismatch");

	let list = grpc
		.block_on(
			sandboxes.file_list(pb::FilePathRequest { id: id.clone(), path: "/tmp".to_owned() }),
		)
		.unwrap_or_else(|status| panic!("files/list failed: {}", common::api::status_detail(&status)))
		.into_inner();
	assert!(list.json.contains("bin"), "listing missing file: {}", list.json);

	grpc
		.block_on(
			sandboxes.file_stat(pb::FilePathRequest { id: id.clone(), path: "/tmp/bin".to_owned() }),
		)
		.unwrap_or_else(|status| {
			panic!("files/stat failed: {}", common::api::status_detail(&status))
		});

	let delete = pb::FileDeleteRequest {
		id:        id.clone(),
		path:      "/tmp/bin".to_owned(),
		recursive: false,
	};
	grpc
		.block_on(sandboxes.file_delete(delete))
		.unwrap_or_else(|status| {
			panic!("file delete failed: {}", common::api::status_detail(&status))
		});
	let (exit, ..) = exec(&server, &id, &["/bin/sh", "-c", "test -e /tmp/bin"]);
	assert_ne!(exit, 0, "file still present after DELETE");

	remove_sandbox(&server, &id);
}

#[test]
fn snapshot_restore_preserves_disk_state() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);
	let view = create_sandbox(&server, json!({}));
	let id = sandbox_id(&view);
	let snap = format!("e2esnap-{}", std::process::id());

	let (exit, ..) =
		exec(&server, &id, &["/bin/sh", "-c", "echo snapshotted > /root/marker && sync"]);
	assert_eq!(exit, 0);

	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let request = pb::SnapshotRequest { id: id.clone(), name: Some(snap.clone()), stop: false };
	let out = grpc
		.block_on(sandboxes.snapshot(request))
		.unwrap_or_else(|status| panic!("snapshot failed: {}", common::api::status_detail(&status)))
		.into_inner();
	let out: Value = serde_json::from_str(&out.json).expect("snapshot view JSON");
	assert_eq!(out["snapshot"].as_str(), Some(snap.as_str()));

	remove_sandbox(&server, &id);

	let mut snapshots = grpc.snapshots();
	let snaps = grpc
		.block_on(snapshots.list(pb::ListSnapshotsRequest {}))
		.unwrap_or_else(|status| {
			panic!("snapshot list failed: {}", common::api::status_detail(&status))
		})
		.into_inner();
	assert!(
		snaps.snapshots.iter().any(|name| name.contains(&snap)),
		"snapshot missing from list: {:?}",
		snaps.snapshots
	);

	let restore = pb::RestoreSnapshotRequest { name: snap, body_json: "{}".to_owned() };
	let restored = grpc
		.block_on(snapshots.restore(restore))
		.unwrap_or_else(|status| {
			panic!(
				"restore failed: {}; log:\n{}",
				common::api::status_detail(&status),
				server.log_tail()
			)
		})
		.into_inner();
	let restored: Value = serde_json::from_str(&restored.json).expect("restore view JSON");
	let rid = sandbox_id(&restored);

	let (exit, stdout, _) = exec(&server, &rid, &["/bin/sh", "-c", "cat /root/marker"]);
	assert_eq!(exit, 0);
	assert_eq!(stdout.trim(), "snapshotted", "disk state lost across restore");

	remove_sandbox(&server, &rid);
}

#[test]
fn fork_clones_are_cow_isolated() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);
	let view = create_sandbox(&server, json!({}));
	let id = sandbox_id(&view);
	let snap = format!("e2efork-{}", std::process::id());

	let (exit, ..) = exec(&server, &id, &["/bin/sh", "-c", "echo base > /root/shared && sync"]);
	assert_eq!(exit, 0);
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let request = pb::SnapshotRequest { id: id.clone(), name: Some(snap.clone()), stop: false };
	grpc
		.block_on(sandboxes.snapshot(request))
		.unwrap_or_else(|status| panic!("snapshot failed: {}", common::api::status_detail(&status)));
	remove_sandbox(&server, &id);

	let mut snapshots = grpc.snapshots();
	let fork =
		pb::ForkSnapshotRequest { name: snap, body_json: json!({"count": 2}).to_string() };
	let forked = grpc
		.block_on(snapshots.fork(fork))
		.unwrap_or_else(|status| {
			panic!("fork failed: {}; log:\n{}", common::api::status_detail(&status), server.log_tail())
		})
		.into_inner();
	let forked: Value = serde_json::from_str(&forked.json).expect("fork view JSON");
	let clones = forked["clones"].as_array().expect("clones array");
	assert_eq!(clones.len(), 2, "expected two clones: {forked}");
	let names: Vec<String> = clones
		.iter()
		.map(|c| c["name"].as_str().expect("clone name").to_string())
		.collect();

	for name in &names {
		let (exit, stdout, _) = exec(&server, name, &["/bin/sh", "-c", "cat /root/shared"]);
		assert_eq!(exit, 0, "clone {name} exec failed");
		assert_eq!(stdout.trim(), "base");
	}

	let (exit, ..) = exec(&server, &names[0], &["/bin/sh", "-c", "echo c0 > /root/only0 && sync"]);
	assert_eq!(exit, 0);
	let (exit, ..) = exec(&server, &names[1], &["/bin/sh", "-c", "test -e /root/only0"]);
	assert_ne!(exit, 0, "fork clones are not CoW-isolated");

	for name in &names {
		remove_sandbox(&server, name);
	}
}

#[test]
fn volumes_rw_and_ro_roundtrip() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);
	let volume = format!("e2evol{}", std::process::id());
	let grpc = server.grpc();
	let mut volumes = grpc.volumes();
	grpc
		.block_on(volumes.create(pb::VolumeRef { name: volume.clone() }))
		.unwrap_or_else(|status| {
			panic!("volume create failed: {}", common::api::status_detail(&status))
		});

	let writer = create_sandbox(&server, json!({"volumes": {"/data": volume}}));
	let wid = sandbox_id(&writer);
	let (exit, ..) = exec(&server, &wid, &["/bin/sh", "-c", "echo vol-data > /data/f && sync"]);
	assert_eq!(exit, 0, "guest write to rw volume failed");
	remove_sandbox(&server, &wid);

	// Host sees the write.
	let host_file = HOME.join("volumes").join(&volume).join("f");
	let content = fs::read_to_string(&host_file)
		.unwrap_or_else(|e| panic!("host volume file {}: {e}", host_file.display()));
	assert_eq!(content.trim(), "vol-data");

	let reader =
		create_sandbox(&server, json!({"volumes": {"/ro": {"name": volume, "read_only": true}}}));
	let rid = sandbox_id(&reader);
	let (exit, stdout, _) = exec(&server, &rid, &["/bin/sh", "-c", "cat /ro/f"]);
	assert_eq!(exit, 0, "read from ro volume failed");
	assert_eq!(stdout.trim(), "vol-data");
	let (exit, ..) = exec(&server, &rid, &["/bin/sh", "-c", "echo deny > /ro/g"]);
	assert_ne!(exit, 0, "write to read-only volume unexpectedly succeeded");
	remove_sandbox(&server, &rid);

	grpc
		.block_on(volumes.delete(pb::VolumeRef { name: volume }))
		.unwrap_or_else(|status| {
			panic!(
				"volume delete after unmount should succeed: {}",
				common::api::status_detail(&status)
			)
		});
}

#[test]
fn secrets_reach_exec_env_but_never_disk() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);
	let secret_value = format!("topsecret-{}", std::process::id());
	let view = create_sandbox(
		&server,
		json!({"secrets": [{"name": "e2e", "values": {"E2E_SECRET": secret_value}}]}),
	);
	let id = sandbox_id(&view);

	let (exit, stdout, _) = exec(&server, &id, &["/bin/sh", "-c", "printf %s \"$E2E_SECRET\""]);
	assert_eq!(exit, 0);
	assert_eq!(stdout, secret_value, "secret not injected into exec env");

	// The value must appear nowhere in persisted state.
	let mut hits = Vec::new();
	scan_for(&HOME, &secret_value, &mut hits);
	assert!(hits.is_empty(), "secret value persisted to disk: {hits:?}");

	remove_sandbox(&server, &id);
}

/// Recursively scan text-ish files under `dir` for `needle` (bounded depth).
fn scan_for(dir: &Path, needle: &str, hits: &mut Vec<PathBuf>) {
	let Ok(entries) = fs::read_dir(dir) else {
		return;
	};
	for entry in entries.flatten() {
		let path = entry.path();
		let Ok(kind) = entry.file_type() else {
			continue;
		};
		if kind.is_dir() {
			// Skip bulky binary areas that cannot hold the secret as text.
			let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
			if name == "images" || name == "assets" {
				continue;
			}
			scan_for(&path, needle, hits);
		} else if kind.is_file()
			&& path.extension().and_then(|e| e.to_str()) == Some("json")
			&& fs::read_to_string(&path).is_ok_and(|text| text.contains(needle))
		{
			hits.push(path);
		}
	}
}

#[test]
fn warm_pool_prewarms_and_claims() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);
	let image = e2e_image();

	// The pool key includes template params (memory/cpus/disk); match the
	// create defaults used by `create_sandbox` so the claim hits this pool.
	let grpc = server.grpc();
	let mut pools = grpc.pools();
	let request = pb::PoolSetRequest {
		reference: image.clone(),
		body_json: json!({"size": 1, "memory": 256}).to_string(),
	};
	grpc
		.block_on(pools.set(request))
		.unwrap_or_else(|status| panic!("pool set failed: {}", common::api::status_detail(&status)));

	// Wait for the refiller to stock one clone.
	let deadline = Instant::now() + Duration::from_mins(3);
	let mut ready = 0;
	while Instant::now() < deadline {
		let pools_view = pool_view(&server);
		ready = pools_view
			.as_object()
			.map_or(0, |m| m.values().filter_map(|v| v["ready"].as_u64()).sum::<u64>())
			as usize;
		if ready >= 1 {
			break;
		}
		std::thread::sleep(Duration::from_millis(500));
	}
	assert!(ready >= 1, "pool never warmed; log:\n{}", server.log_tail());

	let before_hits = pool_hits(&server);
	let view = create_sandbox(&server, json!({}));
	let id = sandbox_id(&view);
	let (exit, stdout, _) = exec(&server, &id, &["/bin/sh", "-c", "echo pooled"]);
	assert_eq!(exit, 0);
	assert_eq!(stdout.trim(), "pooled");
	let after_hits = pool_hits(&server);
	assert!(
		after_hits > before_hits,
		"pool hit counter did not increase ({before_hits} -> {after_hits})"
	);

	remove_sandbox(&server, &id);
	grpc
		.block_on(pools.delete(pb::PoolRef { reference: image }))
		.unwrap_or_else(|status| {
			panic!("pool delete failed: {}", common::api::status_detail(&status))
		});
}

fn pool_view(server: &Server) -> Value {
	let grpc = server.grpc();
	let mut pools = grpc.pools();
	let view = grpc
		.block_on(pools.list(pb::ListPoolsRequest {}))
		.unwrap_or_else(|status| panic!("pool list failed: {}", common::api::status_detail(&status)))
		.into_inner();
	serde_json::from_str(&view.json).expect("pools view JSON")
}

fn pool_hits(server: &Server) -> u64 {
	pool_view(server)
		.as_object()
		.map_or(0, |m| m.values().filter_map(|v| v["hits"].as_u64()).sum())
}

#[test]
fn timeout_terminates_and_extend_defers() {
	if !require_server_e2e() {
		return;
	}
	let server = Server::start(&HOME);

	// Timeout: the VMM self-terminates with return code 124.
	let doomed = create_sandbox(&server, json!({"timeout_secs": 3}));
	let did = sandbox_id(&doomed);
	let deadline = Instant::now() + Duration::from_secs(30);
	let mut last;
	let timed_out = loop {
		let view = sandbox_view(&server, &did);
		let returncode = view.get("returncode").and_then(Value::as_i64);
		let running = view.get("status").and_then(Value::as_str) == Some("running");
		last = view;
		if !running && returncode == Some(124) {
			break true;
		}
		if Instant::now() >= deadline {
			break false;
		}
		std::thread::sleep(Duration::from_millis(500));
	};
	assert!(timed_out, "sandbox never hit the 124 timeout exit: {last}");
	remove_sandbox(&server, &did);

	// Extend: re-arming the deadline outlives the original timeout.
	let survivor = create_sandbox(&server, json!({"timeout_secs": 4}));
	let sid = sandbox_id(&survivor);
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let out = grpc
		.block_on(sandboxes.extend(pb::ExtendSandboxRequest { id: sid.clone(), secs: 60 }))
		.unwrap_or_else(|status| panic!("extend failed: {}", common::api::status_detail(&status)))
		.into_inner();
	let out: Value = serde_json::from_str(&out.json).expect("extend view JSON");
	assert!(out.get("deadline_unix").and_then(Value::as_u64).is_some(), "no deadline: {out}");
	std::thread::sleep(Duration::from_secs(7));
	let view = sandbox_view(&server, &sid);
	assert_eq!(
		view.get("status").and_then(Value::as_str),
		Some("running"),
		"sandbox died at the original deadline despite extend: {view}"
	);
	remove_sandbox(&server, &sid);
}

#[test]
fn rehydrate_after_server_kill() {
	if !require_server_e2e() {
		return;
	}
	let mut server = Server::start(&HOME);
	let view = create_sandbox(&server, json!({}));
	let id = sandbox_id(&view);
	let (exit, ..) = exec(&server, &id, &["/bin/sh", "-c", "true"]);
	assert_eq!(exit, 0);

	let old_pid = server.pid();
	server.kill_hard();
	drop(server);
	assert_ne!(old_pid, 0);

	let server = Server::start(&HOME);
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let listing = grpc
		.block_on(sandboxes.list(pb::ListSandboxesRequest { tags: Vec::new() }))
		.unwrap_or_else(|status| panic!("list failed: {}", common::api::status_detail(&status)))
		.into_inner();
	let rows: Vec<Value> = listing
		.sandboxes_json
		.iter()
		.map(|row| serde_json::from_str(row).expect("sandbox view JSON"))
		.collect();
	let listed = rows.iter().any(|v| v["id"].as_str() == Some(id.as_str()));
	assert!(listed, "sandbox {id} missing after rehydrate: {rows:?}");

	let view = sandbox_view(&server, &id);
	assert_eq!(
		view.get("status").and_then(Value::as_str),
		Some("running"),
		"rehydrated record not running: {view}"
	);

	let (exit, stdout, _) = exec(&server, &id, &["/bin/sh", "-c", "echo revived"]);
	assert_eq!(exit, 0, "exec after rehydrate failed");
	assert_eq!(stdout.trim(), "revived");

	remove_sandbox(&server, &id);
}
