// `proto` is consumed only by the Linux-gated `linux_agent` below; the guest
// agent only ever runs inside Linux guests, so on other hosts it is unused.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod proto;

#[cfg(target_os = "linux")]
fn main() {
	if let Err(err) = linux_agent::run() {
		eprintln!("vmon-agent: {err}");
		std::process::exit(1);
	}
}

#[cfg(not(target_os = "linux"))]
fn main() {
	eprintln!("vmon-agent is only supported on Linux guests");
	std::process::exit(1);
}

#[cfg(target_os = "linux")]
mod linux_agent {
	use std::{
		collections::{HashMap, HashSet},
		ffi::CString,
		fs::{self, File, OpenOptions},
		io::{self, Read, Write},
		net::{Ipv4Addr, TcpStream, ToSocketAddrs},
		os::unix::{
			fs::{MetadataExt, OpenOptionsExt},
			io::{AsRawFd, FromRawFd},
			process::{CommandExt, ExitStatusExt},
		},
		process::{Command, Stdio},
		str::FromStr,
		sync::{Arc, Mutex, MutexGuard},
		thread,
		time::Duration,
	};

	use serde::Deserialize;
	use serde_json::{Value, json};

	use crate::proto::{self, Frame};

	type SharedWriter = Arc<Mutex<File>>;
	type Sessions = Arc<Mutex<HashMap<u32, Session>>>;
	type PendingWrites = Arc<Mutex<HashMap<u32, PendingWrite>>>;

	const HVC0: &str = "/dev/hvc0";
	const CONSOLE: &str = "/dev/console";
	const DEFAULT_IFACE: &str = "eth0";
	const STREAM_CHUNK: usize = 64 * 1024;

	#[repr(C)]
	struct IfInfoMsg {
		ifi_family: u8,
		ifi_pad:    u8,
		ifi_type:   u16,
		ifi_index:  i32,
		ifi_flags:  u32,
		ifi_change: u32,
	}

	#[repr(C)]
	struct IfAddrMsg {
		ifa_family:    u8,
		ifa_prefixlen: u8,
		ifa_flags:     u8,
		ifa_scope:     u8,
		ifa_index:     u32,
	}

	#[repr(C)]
	struct RtMsg {
		rtm_family:   u8,
		rtm_dst_len:  u8,
		rtm_src_len:  u8,
		rtm_tos:      u8,
		rtm_table:    u8,
		rtm_protocol: u8,
		rtm_scope:    u8,
		rtm_type:     u8,
		rtm_flags:    u32,
	}

	#[repr(C)]
	struct RtAttr {
		rta_len:  u16,
		rta_type: u16,
	}

	struct Session {
		pid:        i32,
		stdin:      Option<std::process::ChildStdin>,
		tty_master: Option<File>,
	}

	struct PendingWrite {
		file:  File,
		bytes: u64,
	}

	pub fn run() -> Result<(), String> {
		setup_pid1()?;
		set_child_subreaper()?;

		let cmdline = read_cmdline();
		match cmdline.get("vmon.agent").map(String::as_str) {
			Some("run") => run_one_shot(&cmdline),
			_ => serve(),
		}
	}

	/// Put the virtio-console RPC device into raw mode.
	///
	/// `/dev/hvc0` is a tty, so it comes up in canonical line-discipline mode:
	/// reads block until a newline and the discipline rewrites CR/LF and
	/// intercepts control bytes. The agent protocol is length-prefixed binary,
	/// so without raw mode `read_frame` stalls and frames are corrupted. A
	/// non-tty backend (`ENOTTY`) needs no change.
	fn set_hvc_raw(device: &File) -> Result<(), String> {
		let fd = device.as_raw_fd();
		// SAFETY: `fd` is a valid open file descriptor owned by `device`, and
		// `termios` is fully initialized by `tcgetattr` before use.
		unsafe {
			let mut termios: libc::termios = std::mem::zeroed();
			if libc::tcgetattr(fd, &mut termios) != 0 {
				let err = io::Error::last_os_error();
				if err.raw_os_error() == Some(libc::ENOTTY) {
					return Ok(());
				}
				return Err(format!("tcgetattr {HVC0}: {err}"));
			}
			libc::cfmakeraw(&mut termios);
			if libc::tcsetattr(fd, libc::TCSANOW, &termios) != 0 {
				return Err(format!("tcsetattr {HVC0}: {}", io::Error::last_os_error()));
			}
		}
		Ok(())
	}

	fn serve() -> Result<(), String> {
		let device = OpenOptions::new()
			.read(true)
			.write(true)
			.open(HVC0)
			.map_err(|err| format!("open {HVC0}: {err}"))?;
		set_hvc_raw(&device)?;
		let mut reader = device
			.try_clone()
			.map_err(|err| format!("clone {HVC0}: {err}"))?;
		let writer = Arc::new(Mutex::new(device));
		let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
		let pending_writes: PendingWrites = Arc::new(Mutex::new(HashMap::new()));
		spawn_orphan_reaper(sessions.clone());

		let agent = Agent { writer, sessions, pending_writes };

		loop {
			match proto::read_frame(&mut reader) {
				Ok(Some(frame)) => agent.handle_frame(frame),
				Ok(None) => return Ok(()),
				Err(err) => return Err(format!("read frame: {err}")),
			}
		}
	}

	struct Agent {
		writer:         SharedWriter,
		sessions:       Sessions,
		pending_writes: PendingWrites,
	}

	impl Agent {
		fn handle_frame(&self, frame: Frame) {
			match frame.ty {
				proto::FRAME_REQ => self.handle_request(frame.id, &frame.payload),
				proto::FRAME_STDIN => self.handle_stdin(frame.id, &frame.payload),
				proto::FRAME_KILL => self.handle_kill(frame.id, &frame.payload),
				other => self.send_error(frame.id, format!("unknown frame type {other}")),
			}
		}

		fn handle_request(&self, id: u32, payload: &[u8]) {
			let request: Value = match serde_json::from_slice(payload) {
				Ok(request) => request,
				Err(err) => {
					self.send_error(id, format!("bad request json: {err}"));
					return;
				},
			};

			let Some(op) = request.get("op").and_then(Value::as_str) else {
				self.send_error(id, "missing op");
				return;
			};

			match op {
				"ping" => self.send_resp(
					id,
					json!({
						 "ok": true,
						 "version": env!("CARGO_PKG_VERSION"),
						 "arch": std::env::consts::ARCH,
					}),
				),
				"exec" => self.start_exec(id, &request),
				"fs_read" => self.fs_read(id, &request),
				"fs_write" => self.fs_write(id, &request),
				"fs_list" => self.fs_list(id, &request),
				"fs_stat" => self.fs_stat(id, &request),
				"fs_mkdir" => self.fs_mkdir(id, &request),
				"fs_remove" => self.fs_remove(id, &request),
				"net_config" => self.net_config(id, &request),
				"resize" => self.resize(id, &request),
				"tcp_probe" => self.tcp_probe(id, &request),
				"mount" => self.mount(id, &request),
				_ => self.send_error(id, "unknown op"),
			}
		}

		fn start_exec(&self, id: u32, request: &Value) {
			let tty = request.get("tty").and_then(Value::as_bool).unwrap_or(false);

			let cmd = match string_array(request, "cmd") {
				Ok(cmd) if !cmd.is_empty() => cmd,
				Ok(_) => {
					self.send_error(id, "cmd must not be empty");
					return;
				},
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			let mut command = Command::new(&cmd[0]);
			command.args(&cmd[1..]);
			if let Some(cwd) = request.get("cwd").and_then(Value::as_str) {
				command.current_dir(cwd);
			}
			command.env_clear();
			if let Some(env) = request.get("env") {
				match env_object(env) {
					Ok(env) => {
						for (key, value) in env {
							command.env(key, value);
						}
					},
					Err(err) => {
						self.send_error(id, err);
						return;
					},
				}
			}

			// A tty session runs the child as a session leader whose controlling
			// terminal is the pty slave bound to fd 0/1/2; the parent keeps the
			// master for stdin writes and resize. Non-tty sessions keep separate
			// piped stdin/stdout/stderr exactly as before.
			let mut pty_fds: Option<(libc::c_int, libc::c_int)> = None;
			if tty {
				let mut master_fd: libc::c_int = -1;
				let mut slave_fd: libc::c_int = -1;
				let rc = unsafe {
					libc::openpty(
						&mut master_fd,
						&mut slave_fd,
						std::ptr::null_mut(),
						std::ptr::null(),
						std::ptr::null(),
					)
				};
				if rc < 0 {
					self.send_error(id, format!("openpty: {}", io::Error::last_os_error()));
					return;
				}
				pty_fds = Some((master_fd, slave_fd));
				unsafe {
					command.pre_exec(move || {
						if libc::setsid() < 0 {
							return Err(io::Error::last_os_error());
						}
						if libc::ioctl(slave_fd, libc::TIOCSCTTY, 0 as libc::c_int) < 0 {
							return Err(io::Error::last_os_error());
						}
						if libc::dup2(slave_fd, 0) < 0
							|| libc::dup2(slave_fd, 1) < 0
							|| libc::dup2(slave_fd, 2) < 0
						{
							return Err(io::Error::last_os_error());
						}
						if slave_fd > 2 {
							libc::close(slave_fd);
						}
						libc::close(master_fd);
						Ok(())
					});
				}
			} else {
				command
					.stdin(Stdio::piped())
					.stdout(Stdio::piped())
					.stderr(Stdio::piped());
				unsafe {
					command.pre_exec(|| {
						if libc::setsid() < 0 {
							Err(io::Error::last_os_error())
						} else {
							Ok(())
						}
					});
				}
			}

			let mut child = match command.spawn() {
				Ok(child) => child,
				Err(err) => {
					if let Some((master_fd, slave_fd)) = pty_fds {
						unsafe {
							libc::close(master_fd);
							libc::close(slave_fd);
						}
					}
					self.send_error(id, format!("spawn: {err}"));
					return;
				},
			};

			let pid = child.id() as i32;

			let (stdin, tty_master, joins) = if let Some((master_fd, slave_fd)) = pty_fds {
				// The child holds its own slave reference now; drop the parent's.
				unsafe {
					libc::close(slave_fd);
				}
				let master = unsafe { File::from_raw_fd(master_fd) };
				let reader = match master.try_clone() {
					Ok(reader) => reader,
					Err(err) => {
						unsafe {
							libc::kill(-pid, libc::SIGKILL);
						}
						self.send_error(id, format!("pty clone: {err}"));
						return;
					},
				};
				// Under a tty, stdout and stderr are merged onto the master.
				let join = spawn_stream_thread(self.writer.clone(), id, proto::FRAME_STDOUT, reader);
				(None, Some(master), vec![join])
			} else {
				let stdin = child.stdin.take();
				let stdout = child.stdout.take();
				let stderr = child.stderr.take();
				let mut joins = Vec::new();
				if let Some(stream) = stdout {
					joins.push(spawn_stream_thread(
						self.writer.clone(),
						id,
						proto::FRAME_STDOUT,
						stream,
					));
				}
				if let Some(stream) = stderr {
					joins.push(spawn_stream_thread(
						self.writer.clone(),
						id,
						proto::FRAME_STDERR,
						stream,
					));
				}
				(stdin, None, joins)
			};

			lock(&self.sessions).insert(id, Session { pid, stdin, tty_master });

			let sessions = self.sessions.clone();
			let writer = self.writer.clone();
			thread::spawn(move || {
				let status = child.wait();
				let removed = lock(&sessions).remove(&id);
				drop(removed);

				for join in joins {
					let _ = join.join();
				}

				let payload = match status {
					Ok(status) => json!({
						 "code": status.code().unwrap_or(-1),
						 "signal": status.signal(),
					}),
					Err(err) => json!({
						 "code": -1,
						 "signal": Value::Null,
						 "error": err.to_string(),
					}),
				};
				send_json(&writer, proto::FRAME_EXIT, id, &payload);
			});
		}

		fn handle_stdin(&self, id: u32, payload: &[u8]) {
			if self.handle_pending_write_stdin(id, payload) {
				return;
			}

			let mut sessions = lock(&self.sessions);
			let Some(session) = sessions.get_mut(&id) else {
				drop(sessions);
				self.send_error(id, "unknown stdin session");
				return;
			};

			if session.tty_master.is_some() {
				if payload.is_empty() {
					session.tty_master = None;
					return;
				}
				if let Some(master) = session.tty_master.as_mut() {
					if let Err(err) = master.write_all(payload) {
						drop(sessions);
						self.send_error(id, format!("tty stdin write: {err}"));
					}
				}
				return;
			}

			if payload.is_empty() {
				session.stdin.take();
				return;
			}

			let Some(stdin) = session.stdin.as_mut() else {
				drop(sessions);
				self.send_error(id, "stdin closed");
				return;
			};

			if let Err(err) = stdin.write_all(payload) {
				drop(sessions);
				self.send_error(id, format!("stdin write: {err}"));
			}
		}

		fn handle_pending_write_stdin(&self, id: u32, payload: &[u8]) -> bool {
			let mut writes = lock(&self.pending_writes);
			let Some(write) = writes.get_mut(&id) else {
				return false;
			};

			if payload.is_empty() {
				let mut finished = match writes.remove(&id) {
					Some(write) => write,
					None => return true,
				};
				let result = finished.file.flush().map(|_| finished.bytes);
				drop(writes);
				match result {
					Ok(bytes) => self.send_resp(id, json!({"ok": true, "size": bytes})),
					Err(err) => self.send_error(id, format!("fs_write flush: {err}")),
				}
				return true;
			}

			match write.file.write_all(payload) {
				Ok(()) => write.bytes += payload.len() as u64,
				Err(err) => {
					writes.remove(&id);
					drop(writes);
					self.send_error(id, format!("fs_write: {err}"));
				},
			}
			true
		}

		fn handle_kill(&self, id: u32, payload: &[u8]) {
			let signal = if payload.is_empty() {
				libc::SIGTERM
			} else {
				match serde_json::from_slice::<Value>(payload)
					.ok()
					.and_then(|value| value.get("signal").and_then(Value::as_i64))
				{
					Some(signal) if signal > 0 && signal <= i32::MAX as i64 => signal as i32,
					_ => libc::SIGTERM,
				}
			};

			let pid = match lock(&self.sessions).get(&id).map(|session| session.pid) {
				Some(pid) => pid,
				None => {
					self.send_error(id, "unknown kill session");
					return;
				},
			};

			let rc = unsafe { libc::kill(-pid, signal) };
			if rc < 0 {
				self.send_error(id, format!("kill: {}", io::Error::last_os_error()));
			}
		}

		fn fs_read(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			let mut file = match File::open(&path) {
				Ok(file) => file,
				Err(err) => {
					self.send_error(id, format!("fs_read {path}: {err}"));
					return;
				},
			};

			let mut total = 0u64;
			let mut buf = vec![0u8; STREAM_CHUNK];
			loop {
				match file.read(&mut buf) {
					Ok(0) => break,
					Ok(n) => {
						total += n as u64;
						if send_frame(&self.writer, proto::FRAME_STDOUT, id, &buf[..n]).is_err() {
							return;
						}
					},
					Err(err) => {
						self.send_error(id, format!("fs_read {path}: {err}"));
						return;
					},
				}
			}
			let _ = send_frame(&self.writer, proto::FRAME_STDOUT, id, &[]);
			self.send_resp(id, json!({"ok": true, "size": total}));
		}

		fn fs_write(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let mode = request.get("mode").and_then(Value::as_u64).unwrap_or(0o644) as u32;

			let file = match OpenOptions::new()
				.create(true)
				.truncate(true)
				.write(true)
				.mode(mode)
				.open(&path)
			{
				Ok(file) => file,
				Err(err) => {
					self.send_error(id, format!("fs_write {path}: {err}"));
					return;
				},
			};

			lock(&self.pending_writes).insert(id, PendingWrite { file, bytes: 0 });
		}

		fn fs_list(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			let entries = match fs::read_dir(&path) {
				Ok(entries) => entries,
				Err(err) => {
					self.send_error(id, format!("fs_list {path}: {err}"));
					return;
				},
			};

			let mut result = Vec::new();
			for entry in entries {
				let entry = match entry {
					Ok(entry) => entry,
					Err(err) => {
						self.send_error(id, format!("fs_list {path}: {err}"));
						return;
					},
				};
				let metadata = match entry.metadata() {
					Ok(metadata) => metadata,
					Err(err) => {
						self.send_error(id, format!("fs_list metadata: {err}"));
						return;
					},
				};
				let name = entry.file_name().to_string_lossy().into_owned();
				result.push(json!({
					 "name": name,
					 "type": file_type(&metadata),
					 "size": metadata.len(),
					 "mode": metadata.mode(),
					 "mtime": metadata.mtime(),
				}));
			}

			self.send_resp(id, json!({"ok": true, "entries": result}));
		}

		fn fs_stat(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			match fs::symlink_metadata(&path) {
				Ok(metadata) => self.send_resp(
					id,
					json!({
						 "ok": true,
						 "type": file_type(&metadata),
						 "size": metadata.len(),
						 "mode": metadata.mode(),
						 "mtime": metadata.mtime(),
					}),
				),
				Err(err) => self.send_error(id, format!("fs_stat {path}: {err}")),
			}
		}

		fn fs_mkdir(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let parents = request
				.get("parents")
				.and_then(Value::as_bool)
				.unwrap_or(true);
			let result = if parents {
				fs::create_dir_all(&path)
			} else {
				fs::create_dir(&path)
			};
			match result {
				Ok(()) => self.send_resp(id, json!({"ok": true})),
				Err(err) => self.send_error(id, format!("fs_mkdir {path}: {err}")),
			}
		}

		fn fs_remove(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let recursive = request
				.get("recursive")
				.and_then(Value::as_bool)
				.unwrap_or(false);
			let metadata = match fs::symlink_metadata(&path) {
				Ok(metadata) => metadata,
				Err(err) => {
					self.send_error(id, format!("fs_remove {path}: {err}"));
					return;
				},
			};
			let result = if recursive && metadata.is_dir() {
				fs::remove_dir_all(&path)
			} else if metadata.is_dir() {
				fs::remove_dir(&path)
			} else {
				fs::remove_file(&path)
			};
			match result {
				Ok(()) => self.send_resp(id, json!({"ok": true})),
				Err(err) => self.send_error(id, format!("fs_remove {path}: {err}")),
			}
		}

		fn net_config(&self, id: u32, request: &Value) {
			let ip = match string_field(request, "ip") {
				Ok(ip) => ip,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let prefix = match request.get("prefix").and_then(Value::as_u64) {
				Some(prefix) if prefix <= 32 => prefix as u8,
				_ => {
					self.send_error(id, "prefix must be 0..=32");
					return;
				},
			};
			let gw = match string_field(request, "gw") {
				Ok(gw) => gw,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let dns = match request.get("dns") {
				Some(Value::Array(_)) => match string_array(request, "dns") {
					Ok(dns) => dns,
					Err(err) => {
						self.send_error(id, err);
						return;
					},
				},
				_ => Vec::new(),
			};

			match apply_net_config(DEFAULT_IFACE, &ip, prefix, &gw, &dns) {
				Ok(()) => self.send_resp(id, json!({"ok": true})),
				Err(err) => self.send_error(id, format!("net_config: {err}")),
			}
		}

		fn resize(&self, id: u32, request: &Value) {
			let rows = match request.get("rows").and_then(Value::as_u64) {
				Some(rows) if rows <= u16::MAX as u64 => rows as u16,
				_ => {
					self.send_error(id, "rows must be a u16");
					return;
				},
			};
			let cols = match request.get("cols").and_then(Value::as_u64) {
				Some(cols) if cols <= u16::MAX as u64 => cols as u16,
				_ => {
					self.send_error(id, "cols must be a u16");
					return;
				},
			};

			let fd = {
				let sessions = lock(&self.sessions);
				let Some(session) = sessions.get(&id) else {
					drop(sessions);
					self.send_error(id, "unknown resize session");
					return;
				};
				let Some(master) = session.tty_master.as_ref() else {
					drop(sessions);
					self.send_error(id, "session has no tty");
					return;
				};
				master.as_raw_fd()
			};

			let ws = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
			// SAFETY: fd refers to the pty master; TIOCSWINSZ reads a winsize.
			let rc = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &ws) };
			if rc < 0 {
				self.send_error(id, format!("resize: {}", io::Error::last_os_error()));
			} else {
				self.send_resp(id, json!({"ok": true}));
			}
		}

		fn tcp_probe(&self, id: u32, request: &Value) {
			let port = match request.get("port").and_then(Value::as_u64) {
				Some(port) if port >= 1 && port <= u16::MAX as u64 => port as u16,
				_ => {
					self.send_error(id, "port must be 1..=65535");
					return;
				},
			};
			let host = request
				.get("host")
				.and_then(Value::as_str)
				.unwrap_or("127.0.0.1");

			let addr = match format!("{host}:{port}").to_socket_addrs() {
				Ok(mut addrs) => match addrs.next() {
					Some(addr) => addr,
					None => {
						self.send_error(id, format!("tcp_probe: cannot resolve {host}:{port}"));
						return;
					},
				},
				Err(err) => {
					self.send_error(id, format!("tcp_probe resolve {host}:{port}: {err}"));
					return;
				},
			};

			// A refused or timed-out connection is a valid `connected: false`,
			// not an error: a closed port is exactly what a readiness probe asks.
			let connected = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok();
			self.send_resp(id, json!({"ok": true, "connected": connected}));
		}

		fn mount(&self, id: u32, request: &Value) {
			let tag = match string_field(request, "tag") {
				Ok(tag) => tag,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let fstype = request
				.get("fstype")
				.and_then(Value::as_str)
				.unwrap_or("virtiofs");
			let ro = request.get("ro").and_then(Value::as_bool).unwrap_or(false);

			if let Err(err) = fs::create_dir_all(&path) {
				self.send_error(id, format!("mount mkdir {path}: {err}"));
				return;
			}

			let source = match CString::new(tag.as_str()) {
				Ok(source) => source,
				Err(_) => {
					self.send_error(id, "tag contains an interior nul byte");
					return;
				},
			};
			let target = match CString::new(path.as_str()) {
				Ok(target) => target,
				Err(_) => {
					self.send_error(id, "path contains an interior nul byte");
					return;
				},
			};
			let fs_type = match CString::new(fstype) {
				Ok(fs_type) => fs_type,
				Err(_) => {
					self.send_error(id, "fstype contains an interior nul byte");
					return;
				},
			};
			let flags = if ro { libc::MS_RDONLY } else { 0 };

			// SAFETY: all three C strings are nul-terminated and live across the
			// call; data is null (virtiofs takes no mount options here).
			let rc = unsafe {
				libc::mount(source.as_ptr(), target.as_ptr(), fs_type.as_ptr(), flags, std::ptr::null())
			};
			if rc != 0 {
				self.send_error(id, format!("mount {tag} -> {path}: {}", io::Error::last_os_error()));
				return;
			}
			self.send_resp(id, json!({"ok": true}));
		}

		fn send_resp(&self, id: u32, value: Value) {
			send_json(&self.writer, proto::FRAME_RESP, id, &value);
		}

		fn send_error(&self, id: u32, error: impl Into<String>) {
			self.send_resp(id, json!({"ok": false, "error": error.into()}));
		}
	}

	fn spawn_stream_thread<R>(
		writer: SharedWriter,
		id: u32,
		ty: u8,
		mut stream: R,
	) -> thread::JoinHandle<()>
	where
		R: Read + Send + 'static,
	{
		thread::spawn(move || {
			let mut buf = vec![0u8; STREAM_CHUNK];
			loop {
				match stream.read(&mut buf) {
					Ok(0) => break,
					Ok(n) => {
						if send_frame(&writer, ty, id, &buf[..n]).is_err() {
							return;
						}
					},
					Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
					Err(_) => break,
				}
			}
			let _ = send_frame(&writer, ty, id, &[]);
		})
	}

	fn send_json(writer: &SharedWriter, ty: u8, id: u32, value: &Value) {
		if let Ok(payload) = serde_json::to_vec(value) {
			let _ = send_frame(writer, ty, id, &payload);
		}
	}

	fn send_frame(writer: &SharedWriter, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
		let mut guard = lock(writer);
		proto::write_frame(&mut *guard, ty, id, payload)?;
		guard.flush()
	}

	fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
		match mutex.lock() {
			Ok(guard) => guard,
			Err(poisoned) => poisoned.into_inner(),
		}
	}

	fn string_field(value: &Value, field: &str) -> Result<String, String> {
		value
			.get(field)
			.and_then(Value::as_str)
			.map(ToOwned::to_owned)
			.ok_or_else(|| format!("{field} must be a string"))
	}

	fn string_array(value: &Value, field: &str) -> Result<Vec<String>, String> {
		let Some(items) = value.get(field).and_then(Value::as_array) else {
			return Err(format!("{field} must be an array of strings"));
		};
		items
			.iter()
			.map(|item| {
				item
					.as_str()
					.map(ToOwned::to_owned)
					.ok_or_else(|| format!("{field} must be an array of strings"))
			})
			.collect()
	}

	fn env_object(value: &Value) -> Result<Vec<(String, String)>, String> {
		let Some(object) = value.as_object() else {
			return Err("env must be an object".to_string());
		};
		object
			.iter()
			.map(|(key, value)| {
				value
					.as_str()
					.map(|value| (key.clone(), value.to_string()))
					.ok_or_else(|| "env values must be strings".to_string())
			})
			.collect()
	}

	fn file_type(metadata: &fs::Metadata) -> &'static str {
		let ty = metadata.file_type();
		if ty.is_file() {
			"file"
		} else if ty.is_dir() {
			"dir"
		} else if ty.is_symlink() {
			"symlink"
		} else {
			"other"
		}
	}

	fn spawn_orphan_reaper(sessions: Sessions) {
		thread::spawn(move || {
			loop {
				reap_orphans(&sessions);
				thread::sleep(std::time::Duration::from_millis(100));
			}
		});
	}

	/// Reap reparented orphan/grandchild processes without stealing the exit
	/// status of any tracked exec-session leader.
	///
	/// As PID1 with `PR_SET_CHILD_SUBREAPER`, every descendant whose parent
	/// dies is reparented to us, so reaping must happen on every poll — not
	/// only when no sessions are active — or zombies accumulate while any exec
	/// is running.
	///
	/// Invariant: a session leader's zombie is owned by that session's waiter
	/// thread (`child.wait()` in `start_exec`), which is authoritative for
	/// emitting the EXIT frame with the real status. We must therefore never
	/// consume a leader's zombie here. To stay race-free we *peek* with
	/// `WNOWAIT` (which leaves the child reapable) instead of an unconditional
	/// `waitpid(-1)`: a tracked leader pid is left untouched for its own
	/// thread, while an untracked orphan is reaped (status discarded) with a
	/// targeted `waitpid(pid)`.
	///
	/// This is exempt from the snapshot/exit race because `start_exec` removes
	/// a session from the map only *after* `child.wait()` has reaped its
	/// leader: while a leader is a live zombie it is always present in the map
	/// (so we skip it), and once it leaves the map it has already been reaped
	/// (so `waitid` can no longer observe it). A leader's status is thus never
	/// double-reaped, regardless of how the 100ms poll interleaves with exits.
	fn reap_orphans(sessions: &Sessions) {
		// Snapshot tracked leader pids, then drop the lock *before* any wait
		// syscall so we never block the per-session waiter threads on it.
		let leaders: HashSet<i32> = lock(sessions).values().map(|s| s.pid).collect();

		loop {
			let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
			let rc = unsafe {
				libc::waitid(libc::P_ALL, 0, &mut info, libc::WEXITED | libc::WNOHANG | libc::WNOWAIT)
			};
			// rc == -1: no children left (ECHILD) or interrupted — retry next poll.
			if rc == -1 {
				break;
			}
			// WNOHANG with nothing waitable leaves si_pid == 0 (we pre-zeroed it).
			let pid = unsafe { info.si_pid() };
			if pid == 0 {
				break;
			}
			if leaders.contains(&pid) {
				// Tracked leader: leave the zombie for its owning session
				// thread. WNOWAIT did not consume it, so a re-peek would just
				// return this same front-of-list child again; stop scanning
				// this poll. The blocked `child.wait()` reaps it within
				// microseconds, and any orphan behind it is collected on the
				// next 100ms tick.
				break;
			}
			// Untracked orphan reparented to us: reap it and discard the status.
			let mut status = 0;
			unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
		}
	}

	fn setup_pid1() -> Result<(), String> {
		if unsafe { libc::getpid() } != 1 {
			return Ok(());
		}
		fs::create_dir_all("/proc").map_err(|err| format!("create /proc: {err}"))?;
		fs::create_dir_all("/sys").map_err(|err| format!("create /sys: {err}"))?;
		fs::create_dir_all("/dev").map_err(|err| format!("create /dev: {err}"))?;
		mount_fs("proc", "/proc", "proc")?;
		mount_fs("sysfs", "/sys", "sysfs")?;
		mount_fs("devtmpfs", "/dev", "devtmpfs")?;
		Ok(())
	}

	fn mount_fs(source: &str, target: &str, fstype: &str) -> Result<(), String> {
		let source = CString::new(source).map_err(|err| err.to_string())?;
		let target = CString::new(target).map_err(|err| err.to_string())?;
		let fstype = CString::new(fstype).map_err(|err| err.to_string())?;
		let rc = unsafe {
			libc::mount(source.as_ptr(), target.as_ptr(), fstype.as_ptr(), 0, std::ptr::null())
		};
		if rc == 0 {
			return Ok(());
		}
		let err = io::Error::last_os_error();
		if err.raw_os_error() == Some(libc::EBUSY) {
			Ok(())
		} else {
			Err(format!("mount {target:?}: {err}"))
		}
	}

	fn set_child_subreaper() -> Result<(), String> {
		let rc = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
		if rc == 0 {
			Ok(())
		} else {
			Err(format!("prctl PR_SET_CHILD_SUBREAPER: {}", io::Error::last_os_error()))
		}
	}

	fn read_cmdline() -> HashMap<String, String> {
		fs::read_to_string("/proc/cmdline")
			.unwrap_or_default()
			.split_whitespace()
			.map(|part| match part.split_once('=') {
				Some((key, value)) => (key.to_string(), value.to_string()),
				None => (part.to_string(), "1".to_string()),
			})
			.collect()
	}

	#[derive(Deserialize)]
	struct Entry {
		cmd: Vec<String>,
		cwd: Option<String>,
		env: Option<HashMap<String, String>>,
	}

	fn run_one_shot(cmdline: &HashMap<String, String>) -> Result<(), String> {
		let encoded = cmdline
			.get("vmon.entry")
			.ok_or_else(|| "vmon.entry missing".to_string())?;
		let bytes = decode_base64(encoded)?;
		let entry: Entry =
			serde_json::from_slice(&bytes).map_err(|err| format!("entry json: {err}"))?;
		if entry.cmd.is_empty() {
			return Err("entry cmd must not be empty".to_string());
		}

		let console = OpenOptions::new()
			.read(true)
			.write(true)
			.open(CONSOLE)
			.map_err(|err| format!("open {CONSOLE}: {err}"))?;

		let mut command = Command::new(&entry.cmd[0]);
		command.args(&entry.cmd[1..]);
		if let Some(cwd) = entry.cwd {
			command.current_dir(cwd);
		}
		command.env_clear();
		if let Some(env) = entry.env {
			command.envs(env);
		}
		command
			.stdin(Stdio::from(
				console
					.try_clone()
					.map_err(|err| format!("clone console stdin: {err}"))?,
			))
			.stdout(Stdio::from(
				console
					.try_clone()
					.map_err(|err| format!("clone console stdout: {err}"))?,
			))
			.stderr(Stdio::from(console));

		let mut child = command
			.spawn()
			.map_err(|err| format!("spawn entry: {err}"))?;
		let _ = child.wait().map_err(|err| format!("wait entry: {err}"))?;
		reboot_guest()
	}

	fn decode_base64(input: &str) -> Result<Vec<u8>, String> {
		let mut out = Vec::with_capacity(input.len() * 3 / 4);
		let mut buffer = 0u32;
		let mut bits = 0u8;

		for byte in input.bytes() {
			let value = match byte {
				b'A'..=b'Z' => byte - b'A',
				b'a'..=b'z' => byte - b'a' + 26,
				b'0'..=b'9' => byte - b'0' + 52,
				b'+' | b'-' => 62,
				b'/' | b'_' => 63,
				b'=' => break,
				b'\n' | b'\r' | b'\t' | b' ' => continue,
				_ => return Err(format!("invalid base64 byte 0x{byte:02x}")),
			} as u32;
			buffer = (buffer << 6) | value;
			bits += 6;
			if bits >= 8 {
				bits -= 8;
				out.push(((buffer >> bits) & 0xff) as u8);
			}
		}
		Ok(out)
	}

	fn reboot_guest() -> Result<(), String> {
		unsafe {
			libc::sync();
			if libc::reboot(libc::RB_AUTOBOOT) == 0 {
				Ok(())
			} else {
				Err(format!("reboot: {}", io::Error::last_os_error()))
			}
		}
	}

	fn apply_net_config(
		iface: &str,
		ip: &str,
		prefix: u8,
		gw: &str,
		dns: &[String],
	) -> Result<(), String> {
		let ip = Ipv4Addr::from_str(ip).map_err(|err| format!("ip: {err}"))?;
		let gw = Ipv4Addr::from_str(gw).map_err(|err| format!("gw: {err}"))?;
		let ifname = CString::new(iface).map_err(|err| err.to_string())?;
		let ifindex = unsafe { libc::if_nametoindex(ifname.as_ptr()) };
		if ifindex == 0 {
			return Err(format!("if_nametoindex {iface}: {}", io::Error::last_os_error()));
		}

		set_link_up(ifindex)?;
		add_ipv4_addr(ifindex, ip, prefix)?;
		add_default_route(ifindex, gw)?;
		write_resolv_conf(dns)?;
		Ok(())
	}

	fn write_resolv_conf(dns: &[String]) -> Result<(), String> {
		if dns.is_empty() {
			return Ok(());
		}
		let mut data = String::new();
		for server in dns {
			data.push_str("nameserver ");
			data.push_str(server);
			data.push('\n');
		}
		fs::write("/etc/resolv.conf", data).map_err(|err| format!("write /etc/resolv.conf: {err}"))
	}

	fn set_link_up(ifindex: u32) -> Result<(), String> {
		let mut payload = Vec::new();
		let msg = IfInfoMsg {
			ifi_family: libc::AF_UNSPEC as u8,
			ifi_pad:    0,
			ifi_type:   0,
			ifi_index:  ifindex as i32,
			ifi_flags:  libc::IFF_UP as u32,
			ifi_change: libc::IFF_UP as u32,
		};
		push_struct(&mut payload, &msg);
		netlink_request(
			libc::RTM_NEWLINK as u16,
			(libc::NLM_F_REQUEST | libc::NLM_F_ACK) as u16,
			&payload,
		)
		.map_err(|err| format!("link up: {err}"))
	}

	fn add_ipv4_addr(ifindex: u32, ip: Ipv4Addr, prefix: u8) -> Result<(), String> {
		let mut payload = Vec::new();
		let msg = IfAddrMsg {
			ifa_family:    libc::AF_INET as u8,
			ifa_prefixlen: prefix,
			ifa_flags:     0,
			ifa_scope:     libc::RT_SCOPE_UNIVERSE as u8,
			ifa_index:     ifindex,
		};
		push_struct(&mut payload, &msg);
		push_attr(&mut payload, libc::IFA_LOCAL as u16, &ip.octets());
		push_attr(&mut payload, libc::IFA_ADDRESS as u16, &ip.octets());
		netlink_request(
			libc::RTM_NEWADDR as u16,
			(libc::NLM_F_REQUEST | libc::NLM_F_ACK | libc::NLM_F_CREATE | libc::NLM_F_REPLACE) as u16,
			&payload,
		)
		.map_err(|err| format!("addr add: {err}"))
	}

	fn add_default_route(ifindex: u32, gw: Ipv4Addr) -> Result<(), String> {
		let mut payload = Vec::new();
		let msg = RtMsg {
			rtm_family:   libc::AF_INET as u8,
			rtm_dst_len:  0,
			rtm_src_len:  0,
			rtm_tos:      0,
			rtm_table:    libc::RT_TABLE_MAIN as u8,
			rtm_protocol: libc::RTPROT_BOOT as u8,
			rtm_scope:    libc::RT_SCOPE_UNIVERSE as u8,
			rtm_type:     libc::RTN_UNICAST as u8,
			rtm_flags:    0,
		};
		push_struct(&mut payload, &msg);
		push_attr(&mut payload, libc::RTA_GATEWAY as u16, &gw.octets());
		push_attr(&mut payload, libc::RTA_OIF as u16, &ifindex.to_ne_bytes());
		match netlink_request(
			libc::RTM_NEWROUTE as u16,
			(libc::NLM_F_REQUEST | libc::NLM_F_ACK | libc::NLM_F_CREATE | libc::NLM_F_REPLACE) as u16,
			&payload,
		) {
			Ok(()) => Ok(()),
			Err(err) if err.raw_os_error() == Some(libc::EEXIST) => Ok(()),
			Err(err) => Err(format!("route add: {err}")),
		}
	}

	fn netlink_request(message_type: u16, flags: u16, payload: &[u8]) -> io::Result<()> {
		let fd = unsafe {
			libc::socket(libc::AF_NETLINK, libc::SOCK_RAW | libc::SOCK_CLOEXEC, libc::NETLINK_ROUTE)
		};
		if fd < 0 {
			return Err(io::Error::last_os_error());
		}

		let result = (|| {
			let mut request =
				Vec::with_capacity(std::mem::size_of::<libc::nlmsghdr>() + payload.len());
			let header = libc::nlmsghdr {
				nlmsg_len:   (std::mem::size_of::<libc::nlmsghdr>() + payload.len()) as u32,
				nlmsg_type:  message_type,
				nlmsg_flags: flags,
				nlmsg_seq:   1,
				nlmsg_pid:   0,
			};
			push_struct(&mut request, &header);
			request.extend_from_slice(payload);

			let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
			addr.nl_family = libc::AF_NETLINK as libc::sa_family_t;
			addr.nl_pid = 0;
			addr.nl_groups = 0;
			let sent = unsafe {
				libc::sendto(
					fd,
					request.as_ptr().cast(),
					request.len(),
					0,
					(&addr as *const libc::sockaddr_nl).cast(),
					std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
				)
			};
			if sent < 0 {
				return Err(io::Error::last_os_error());
			}

			let mut ack = [0u8; 4096];
			let len = unsafe { libc::recv(fd, ack.as_mut_ptr().cast(), ack.len(), 0) };
			if len < 0 {
				return Err(io::Error::last_os_error());
			}
			parse_netlink_ack(&ack[..len as usize])
		})();

		let close_result = unsafe { libc::close(fd) };
		if result.is_ok() && close_result < 0 {
			Err(io::Error::last_os_error())
		} else {
			result
		}
	}

	fn parse_netlink_ack(buf: &[u8]) -> io::Result<()> {
		let header_len = std::mem::size_of::<libc::nlmsghdr>();
		if buf.len() < header_len {
			return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short netlink ack"));
		}
		let header = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast::<libc::nlmsghdr>()) };
		if header.nlmsg_type != libc::NLMSG_ERROR as u16 {
			return Ok(());
		}
		if buf.len() < header_len + std::mem::size_of::<libc::nlmsgerr>() {
			return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short netlink error"));
		}
		let err =
			unsafe { std::ptr::read_unaligned(buf[header_len..].as_ptr().cast::<libc::nlmsgerr>()) };
		if err.error == 0 {
			Ok(())
		} else {
			Err(io::Error::from_raw_os_error(-err.error))
		}
	}

	fn push_struct<T>(buf: &mut Vec<u8>, value: &T) {
		let bytes = unsafe {
			std::slice::from_raw_parts((value as *const T).cast::<u8>(), std::mem::size_of::<T>())
		};
		buf.extend_from_slice(bytes);
	}

	fn push_attr(buf: &mut Vec<u8>, attr_type: u16, data: &[u8]) {
		let len = std::mem::size_of::<RtAttr>() + data.len();
		let attr = RtAttr { rta_len: len as u16, rta_type: attr_type };
		push_struct(buf, &attr);
		buf.extend_from_slice(data);
		while buf.len() % 4 != 0 {
			buf.push(0);
		}
	}
}
