//! Control plane: vCPU pause/resume signalling primitives and a Unix-socket
//! command server.
//!
//! [`PauseGate`] coordinates parking every vCPU thread at a safe point so a
//! snapshot sees a frozen machine. It uses a real-time signal to kick threads
//! out of an in-flight `KVM_RUN`: the handler is a no-op installed *without*
//! `SA_RESTART`, so the interrupted ioctl returns `EINTR` and the vCPU loop can
//! re-check its run state.
//!
//! [`serve`] exposes a newline-delimited JSON lifecycle protocol over a
//! `UnixListener`. The socket thread never touches the `Vmm` directly; it
//! forwards parsed requests to the owning thread over a channel and
//! serializes that thread's JSON response back to the socket.

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::io::AsRawFd;
use std::{
	fs,
	io::{self, BufRead, BufReader, Write},
	os::unix::{
		ffi::OsStrExt,
		fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt},
		net::{UnixListener, UnixStream},
	},
	path::{Path, PathBuf},
	sync::Arc,
	time::Duration,
};

use flume::{RecvTimeoutError, Sender};
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};

use crate::result::{Result, err};

const CONTROL_MAX_REQUEST_LINE: usize = 65_536;
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_REPLY_TIMEOUT: Duration = Duration::from_mins(1);
/// Real-time signal used on Linux to kick a vCPU out of `KVM_RUN`.
///
/// HVF uses the backend kicker installed on [`PauseGate`] instead of POSIX
/// signals because Hypervisor.framework exposes an explicit vCPU-exit API.
#[cfg(target_os = "linux")]
pub mod pause_signal {
	use parking_lot::Mutex;

	use crate::result::Result;

	/// Real-time signal used to kick a vCPU out of `KVM_RUN`.
	pub fn signal() -> i32 {
		libc::SIGRTMIN()
	}

	const extern "C" fn handler(_: libc::c_int, _: *mut libc::siginfo_t, _: *mut libc::c_void) {}

	/// Install the no-op handler without `SA_RESTART` so a delivered signal
	/// makes an in-flight `KVM_RUN` ioctl return `EINTR`.
	pub fn install() -> Result<()> {
		vmm_sys_util::signal::register_signal_handler(signal(), handler)?;
		Ok(())
	}

	/// `pthread_kill` every registered vCPU thread to interrupt `KVM_RUN`.
	pub fn signal_threads(tids: &Mutex<Vec<Option<libc::pthread_t>>>) {
		for slot in tids.lock().iter() {
			if let Some(tid) = *slot {
				// SAFETY: `tid` came from `pthread_self` in the target thread;
				// the return value (e.g. ESRCH) is intentionally ignored.
				unsafe {
					libc::pthread_kill(tid, signal());
				}
			}
		}
	}
}

#[cfg(not(target_os = "linux"))]
pub mod pause_signal {
	use parking_lot::Mutex;

	use crate::result::Result;
	#[allow(clippy::unnecessary_wraps, reason = "uniform API across target operating systems")]
	pub fn install() -> Result<()> {
		Ok(())
	}
	/// No POSIX-signal kick on non-Linux backends.
	pub const fn signal_threads(_tids: &Mutex<Vec<Option<libc::pthread_t>>>) {}
}
/// Lifecycle state shared by all vCPU threads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RunState {
	Running,
	Paused,
	Stopping,
}

/// Per-vCPU command channel payload (the `Vmm` holds the `Sender`, the vCPU
/// thread the `Receiver`).
pub enum VcpuCmd {
	Resume,
	Stop,
	/// Snapshot this vCPU's backend state and send it back, then stay parked.
	/// Only the owning vCPU thread may touch backend-local vCPU handles.
	SaveState(Sender<Result<crate::arch::state::VcpuState>>),
}

/// Synchronisation gate used to park/unpark vCPU threads at a safe point.
pub struct PauseGate {
	state:  Mutex<RunState>,
	cv:     Condvar,
	parked: Mutex<Vec<bool>>,
	cpus:   u32,
	tids:   Mutex<Vec<Option<libc::pthread_t>>>,
	kicker: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl PauseGate {
	/// Create a gate for `cpus` vCPU threads, initially [`RunState::Running`].
	pub fn new(cpus: u32) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(RunState::Running),
			cv: Condvar::new(),
			parked: Mutex::new(vec![false; cpus as usize]),
			cpus,
			tids: Mutex::new(vec![None; cpus as usize]),
			kicker: Mutex::new(None),
		})
	}

	/// Current run state.
	pub fn state(&self) -> RunState {
		*self.state.lock()
	}

	/// Set the run state. (Callers drive the park/unpark handshake separately.)
	pub fn set_state(&self, s: RunState) {
		*self.state.lock() = s;
	}

	/// Transition the run state only if the current state is `expected`.
	pub fn set_state_if(&self, expected: RunState, next: RunState) -> bool {
		let mut state = self.state.lock();
		if *state == expected {
			*state = next;
			true
		} else {
			false
		}
	}

	/// Called by vCPU thread `vcpu_id` on entry: record its pthread id so the
	/// gate can later signal exactly the registered threads.
	pub fn register_tid(&self, vcpu_id: usize) {
		// SAFETY: `pthread_self` is always safe to call.
		self.tids.lock()[vcpu_id] = Some(unsafe { libc::pthread_self() });
	}

	/// Mark vCPU `vcpu_id` parked and wake any waiter (e.g. `pause()`).
	pub fn mark_parked(&self, vcpu_id: usize) {
		let mut g = self.parked.lock();
		if let Some(parked) = g.get_mut(vcpu_id)
			&& !*parked
		{
			*parked = true;
			self.cv.notify_all();
		}
	}

	/// Reset parked state (done once per `Running`->`Paused` transition).
	pub fn reset_parked(&self) {
		self.parked.lock().fill(false);
	}

	/// Number of vCPUs currently parked.
	pub fn parked_count(&self) -> u32 {
		self.parked.lock().iter().filter(|parked| **parked).count() as u32
	}

	/// Parked vCPU IDs for diagnostics.
	pub fn parked_vcpus(&self) -> Vec<usize> {
		self
			.parked
			.lock()
			.iter()
			.enumerate()
			.filter_map(|(id, parked)| (*parked).then_some(id))
			.collect()
	}

	/// Block until every vCPU is parked or `timeout` elapses; returns whether
	/// all vCPUs are parked.
	pub fn wait_for_all_parked(&self, timeout: Duration) -> bool {
		let mut g = self.parked.lock();
		self.cv.wait_while_for(
			&mut g,
			|parked| parked.iter().filter(|is_parked| **is_parked).count() < self.cpus as usize,
			timeout,
		);
		g.iter().filter(|parked| **parked).count() >= self.cpus as usize
	}

	/// Install a backend-specific wakeup for vCPU APIs that are not
	/// signal-driven.
	#[cfg(target_os = "macos")]
	pub fn set_kicker(&self, kicker: Arc<dyn Fn() + Send + Sync>) {
		*self.kicker.lock() = Some(kicker);
	}

	/// Kick every registered vCPU out of the hypervisor run call.
	pub fn signal_all_vcpus(&self) {
		pause_signal::signal_threads(&self.tids);
		let kicker = self.kicker.lock().clone();
		if let Some(kicker) = kicker {
			kicker();
		}
	}
}

/// Lifecycle control request parsed off the socket.
#[derive(Debug)]
pub enum ControlKind {
	Ping,
	Info,
	Pause,
	Resume,
	Snapshot { name: String, base: Option<String>, allow_user_net: bool },
	Quit,
	Metrics,
	Extend { secs: u64 },
}

/// JSON error object returned by the lifecycle API.
#[derive(Clone, Debug, Serialize)]
pub struct ApiError {
	pub code:    String,
	pub message: String,
}

impl ApiError {
	pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
		Self { code: code.into(), message: message.into() }
	}

	pub fn bad_request(message: impl Into<String>) -> Self {
		Self::new("bad_request", message)
	}

	pub fn unknown_method(message: impl Into<String>) -> Self {
		Self::new("unknown_method", message)
	}

	pub fn snapshots_disabled(message: impl Into<String>) -> Self {
		Self::new("snapshots_disabled", message)
	}

	pub fn path_denied(message: impl Into<String>) -> Self {
		Self::new("path_denied", message)
	}

	pub fn not_paused(message: impl Into<String>) -> Self {
		Self::new("not_paused", message)
	}

	pub fn internal(message: impl Into<String>) -> Self {
		Self::new("internal", message)
	}
}

pub type ControlReply = std::result::Result<serde_json::Value, ApiError>;

/// A parsed command plus the oneshot channel its JSON reply is sent back on.
pub struct ControlCmd {
	pub kind:  ControlKind,
	pub reply: Sender<ControlReply>,
}

/// Filesystem owner to prepare for a pre-bound control socket.
#[derive(Clone, Copy)]
pub struct SocketOwner {
	pub uid: u32,
	pub gid: u32,
}

#[derive(Clone, Copy)]
struct SocketIdentity {
	dev: u64,
	ino: u64,
}

impl SocketIdentity {
	fn from_metadata(meta: &fs::Metadata) -> Self {
		Self { dev: meta.dev(), ino: meta.ino() }
	}

	fn matches(self, meta: &fs::Metadata) -> bool {
		self.dev == meta.dev() && self.ino == meta.ino()
	}
}

/// Bound control socket listener. Binding can happen before sandbox uid/gid
/// drops; serving starts later, after the process sandbox is in place.
pub struct ControlServer {
	sock_path:  PathBuf,
	owner_uid:  u32,
	launch_uid: u32,
	listener:   UnixListener,
	identity:   SocketIdentity,
}

pub struct ControlCleanup {
	sock_path: PathBuf,
	owner_uid: u32,
	identity:  SocketIdentity,
}

impl ControlServer {
	pub fn cleanup(&self) {
		cleanup_socket_for_uid(&self.sock_path, self.owner_uid, Some(self.identity));
	}

	pub fn cleanup_token(&self) -> ControlCleanup {
		ControlCleanup {
			sock_path: self.sock_path.clone(),
			owner_uid: self.owner_uid,
			identity:  self.identity,
		}
	}

	pub fn start(self, tx: Sender<ControlCmd>) {
		let launch_uid = self.launch_uid;
		std::thread::spawn(move || {
			for stream in self.listener.incoming() {
				if let Ok(stream) = stream
					&& configure_stream(&stream)
						.and_then(|()| verify_peer_credentials(&stream, launch_uid))
						.is_ok()
				{
					let tx = tx.clone();
					std::thread::spawn(move || handle_connection(stream, tx));
				}
			}
		});
	}
}

impl ControlCleanup {
	pub fn cleanup(&self) {
		cleanup_socket_for_uid(&self.sock_path, self.owner_uid, Some(self.identity));
	}
}

/// Bind a `UnixListener` at `sock_path` without spawning the server thread.
///
/// The socket is only created inside a private (0700-or-stricter) directory
/// owned by the requested `owner` uid, by this process uid when `owner` is
/// `None`, or by the current root euid while pre-binding a sandbox-owned
/// socket before the uid/gid drop. If that immediate parent is missing, it is
/// created with 0700 permissions and chowned to `owner` when requested;
/// otherwise unsafe parents are rejected. Stale sockets are unlinked only after
/// verifying they are inactive sockets owned by the expected uid or allowed
/// pre-bind uid.
pub fn bind(
	sock_path: PathBuf,
	owner: Option<SocketOwner>,
	launch_uid: u32,
) -> Result<ControlServer> {
	prepare_socket_path(&sock_path, owner)?;
	let owner_uid = owner.map_or_else(current_euid, |owner| owner.uid);
	let listener = UnixListener::bind(&sock_path)?;
	if let Some(owner) = owner
		&& let Err(e) = chown_path(&sock_path, owner)
	{
		cleanup_socket_for_uid(&sock_path, current_euid(), None);
		return Err(e);
	}
	if let Err(e) = chmod_path(&sock_path, 0o600) {
		cleanup_socket_for_uid(&sock_path, owner_uid, None);
		return Err(e);
	}
	let identity = match bound_socket_identity(&sock_path, owner_uid) {
		Ok(identity) => identity,
		Err(e) => {
			cleanup_socket_for_uid(&sock_path, owner_uid, None);
			return Err(e);
		},
	};
	Ok(ControlServer { sock_path, owner_uid, launch_uid, listener, identity })
}

fn bound_socket_identity(sock_path: &Path, expected_uid: u32) -> Result<SocketIdentity> {
	let meta = fs::symlink_metadata(sock_path)?;
	if !meta.file_type().is_socket() {
		return Err(err(format!("bound control path {} is not a socket", sock_path.display())));
	}
	if meta.uid() != expected_uid {
		return Err(err(format!(
			"bound control socket {} is owned by uid {}, expected uid {expected_uid}",
			sock_path.display(),
			meta.uid()
		)));
	}
	Ok(SocketIdentity::from_metadata(&meta))
}

fn cleanup_socket_for_uid(sock_path: &Path, expected_uid: u32, identity: Option<SocketIdentity>) {
	let Ok(meta) = fs::symlink_metadata(sock_path) else {
		return;
	};
	if !meta.file_type().is_socket() || meta.uid() != expected_uid {
		return;
	}
	if let Some(identity) = identity
		&& !identity.matches(&meta)
	{
		return;
	}
	let _ = fs::remove_file(sock_path);
}

fn prepare_socket_path(sock_path: &Path, owner: Option<SocketOwner>) -> Result<()> {
	if sock_path.as_os_str().is_empty() {
		return Err(err("control socket path must not be empty"));
	}
	let bind_uid = current_euid();
	let expected_uid = owner.map_or(bind_uid, |owner| owner.uid);
	let prebind_uid = allowed_prebind_parent_uid(owner, expected_uid, bind_uid);
	ensure_private_parent(socket_parent(sock_path), sock_path, owner, expected_uid, prebind_uid)?;
	remove_stale_socket(sock_path, expected_uid, prebind_uid)?;
	Ok(())
}

fn socket_parent(sock_path: &Path) -> &Path {
	sock_path
		.parent()
		.filter(|parent| !parent.as_os_str().is_empty())
		.unwrap_or_else(|| Path::new("."))
}

fn ensure_private_parent(
	parent: &Path,
	sock_path: &Path,
	owner: Option<SocketOwner>,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	match fs::symlink_metadata(parent) {
		Ok(meta) => {
			verify_private_parent(parent, &meta, expected_uid, prebind_uid)?;
			reown_prebind_parent(parent, sock_path, owner, &meta, expected_uid, prebind_uid)
		},
		Err(e) if e.kind() == io::ErrorKind::NotFound => {
			fs::DirBuilder::new()
				.mode(0o700)
				.create(parent)
				.map_err(|e| {
					err(format!("creating private control socket directory {}: {e}", parent.display()))
				})?;
			if let Some(owner) = owner {
				chown_path(parent, owner)?;
			}
			chmod_path(parent, 0o700)?;
			let meta = fs::symlink_metadata(parent)?;
			verify_private_parent(parent, &meta, expected_uid, prebind_uid)
		},
		Err(e) => Err(e.into()),
	}
}

fn reown_prebind_parent(
	parent: &Path,
	sock_path: &Path,
	owner: Option<SocketOwner>,
	meta: &fs::Metadata,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	let Some(owner) = owner else {
		return Ok(());
	};
	if meta.uid() == expected_uid || prebind_uid != Some(meta.uid()) {
		return Ok(());
	}

	ensure_parent_can_be_reowned(parent, sock_path, expected_uid, prebind_uid)?;
	chown_path(parent, owner)?;
	chmod_path(parent, 0o700)?;
	let meta = fs::symlink_metadata(parent)?;
	verify_private_parent(parent, &meta, expected_uid, None)
}

fn ensure_parent_can_be_reowned(
	parent: &Path,
	sock_path: &Path,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	let sock_name = sock_path.file_name().ok_or_else(|| {
		err(format!("control socket path {} must include a file name", sock_path.display()))
	})?;
	for entry in fs::read_dir(parent)? {
		let entry = entry?;
		if entry.file_name().as_os_str() != sock_name {
			return Err(err(format!(
				"refusing to chown non-empty control socket parent {}",
				parent.display()
			)));
		}
		let meta = fs::symlink_metadata(entry.path())?;
		if !meta.file_type().is_socket() {
			return Err(err(format!(
				"refusing to chown control socket parent {} with non-socket entry {}",
				parent.display(),
				entry.path().display()
			)));
		}
		if !socket_owner_is_allowed(meta.uid(), expected_uid, prebind_uid) {
			let expected = prebind_uid.map_or_else(
				|| format!("uid {expected_uid}"),
				|uid| format!("uid {expected_uid} or pre-bind uid {uid}"),
			);
			return Err(err(format!(
				"refusing to chown control socket parent {} with socket entry owned by uid {}, \
				 expected {expected}",
				parent.display(),
				meta.uid()
			)));
		}
	}
	Ok(())
}

const fn allowed_prebind_parent_uid(
	owner: Option<SocketOwner>,
	expected_uid: u32,
	bind_uid: u32,
) -> Option<u32> {
	if owner.is_some() && bind_uid == 0 && bind_uid != expected_uid {
		Some(bind_uid)
	} else {
		None
	}
}

fn parent_owner_is_allowed(parent_uid: u32, expected_uid: u32, prebind_uid: Option<u32>) -> bool {
	parent_uid == expected_uid || prebind_uid == Some(parent_uid)
}

fn socket_owner_is_allowed(socket_uid: u32, expected_uid: u32, prebind_uid: Option<u32>) -> bool {
	socket_uid == expected_uid || prebind_uid == Some(socket_uid)
}

const fn private_parent_mode_is_safe(mode: u32) -> bool {
	mode & 0o077 == 0
}

fn verify_private_parent(
	parent: &Path,
	meta: &fs::Metadata,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	if meta.file_type().is_symlink() {
		return Err(err(format!("control socket parent {} must not be a symlink", parent.display())));
	}
	if !meta.is_dir() {
		return Err(err(format!("control socket parent {} is not a directory", parent.display())));
	}

	if !parent_owner_is_allowed(meta.uid(), expected_uid, prebind_uid) {
		let expected = prebind_uid.map_or_else(
			|| format!("uid {expected_uid}"),
			|uid| format!("uid {expected_uid} or pre-bind uid {uid}"),
		);
		return Err(err(format!(
			"control socket parent {} is owned by uid {}, expected {expected}",
			parent.display(),
			meta.uid()
		)));
	}

	let mode = meta.permissions().mode() & 0o777;
	if !private_parent_mode_is_safe(mode) {
		return Err(err(format!(
			"control socket parent {} must be private (mode 0700 or stricter), found {mode:03o}",
			parent.display()
		)));
	}
	Ok(())
}

fn remove_stale_socket(
	sock_path: &Path,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	let meta = match fs::symlink_metadata(sock_path) {
		Ok(meta) => meta,
		Err(e) => {
			return if e.kind() == io::ErrorKind::NotFound {
				Ok(())
			} else {
				Err(e.into())
			};
		},
	};

	if !meta.file_type().is_socket() {
		return Err(err(format!(
			"refusing to remove non-socket control path {}",
			sock_path.display()
		)));
	}
	if !socket_owner_is_allowed(meta.uid(), expected_uid, prebind_uid) {
		let expected = prebind_uid.map_or_else(
			|| format!("uid {expected_uid}"),
			|uid| format!("uid {expected_uid} or pre-bind uid {uid}"),
		);
		return Err(err(format!(
			"refusing to remove control socket {} owned by uid {}, expected {expected}",
			sock_path.display(),
			meta.uid()
		)));
	}
	match UnixStream::connect(sock_path) {
		Ok(_) => {
			Err(err(format!("refusing to remove active control socket {}", sock_path.display())))
		},
		Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => Ok(()),
		Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
		Err(e) => {
			Err(err(format!("refusing to probe stale control socket {}: {e}", sock_path.display())))
		},
	}?;
	fs::remove_file(sock_path)?;
	Ok(())
}

fn chown_path(path: &Path, owner: SocketOwner) -> Result<()> {
	let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
		err(format!("control socket path {} contains an interior NUL byte", path.display()))
	})?;
	// SAFETY: `c_path` is a NUL-terminated path derived from `path`; `lchown`
	// reads that pointer only for the duration of the call, and uid/gid are
	// plain value arguments from `SocketOwner`.
	let rc =
		unsafe { libc::lchown(c_path.as_ptr(), owner.uid as libc::uid_t, owner.gid as libc::gid_t) };
	if rc != 0 {
		return Err(err(format!(
			"chowning control socket path {} to {}:{}: {}",
			path.display(),
			owner.uid,
			owner.gid,
			io::Error::last_os_error()
		)));
	}
	Ok(())
}

#[allow(clippy::useless_conversion, reason = "mode_t is u16 on macOS but u32 on Linux")]
fn chmod_path(path: &Path, mode: libc::mode_t) -> Result<()> {
	let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
		err(format!("control socket path {} contains an interior NUL byte", path.display()))
	})?;
	// SAFETY: `c_path` is a NUL-terminated path derived from `path`; `fchmodat`
	// reads that pointer only for the duration of the call, with `AT_FDCWD`
	// and `AT_SYMLINK_NOFOLLOW` requiring no additional Rust-side invariants.
	let rc =
		unsafe { libc::fchmodat(libc::AT_FDCWD, c_path.as_ptr(), mode, libc::AT_SYMLINK_NOFOLLOW) };
	if rc == 0 {
		return Ok(());
	}

	let e = io::Error::last_os_error();
	if !chmod_fallback_allowed(&e) {
		return Err(err(format!("chmoding control socket path {} to {mode:o}: {e}", path.display())));
	}

	let meta = fs::symlink_metadata(path)?;
	if meta.file_type().is_symlink() {
		return Err(err(format!("refusing to chmod symlink control socket path {}", path.display())));
	}
	fs::set_permissions(path, fs::Permissions::from_mode(u32::from(mode)))?;
	Ok(())
}

fn chmod_fallback_allowed(e: &io::Error) -> bool {
	matches!(
		 e.raw_os_error(),
		 Some(code)
			  if code == libc::EINVAL || code == libc::ENOTSUP || code == libc::EOPNOTSUPP
	)
}

fn current_euid() -> u32 {
	// SAFETY: `geteuid` is thread-safe and has no preconditions.
	unsafe { libc::geteuid() as u32 }
}

fn configure_stream(stream: &UnixStream) -> io::Result<()> {
	stream.set_read_timeout(Some(CONTROL_READ_TIMEOUT))?;
	stream.set_write_timeout(Some(CONTROL_WRITE_TIMEOUT))?;
	Ok(())
}

#[cfg(target_os = "linux")]
fn verify_peer_credentials(stream: &UnixStream, launch_uid: u32) -> io::Result<()> {
	// SAFETY: `libc::ucred` is a C POD credential struct; an all-zero bit
	// pattern is valid storage before `getsockopt` fills it.
	let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
	let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
	// SAFETY: `cred` and `len` point to writable storage of the advertised
	// `libc::ucred` size, and the fd comes from a live UnixStream.
	let rc = unsafe {
		libc::getsockopt(
			stream.as_raw_fd(),
			libc::SOL_SOCKET,
			libc::SO_PEERCRED,
			&mut cred as *mut _ as *mut libc::c_void,
			&mut len,
		)
	};
	if rc != 0 {
		return Err(io::Error::last_os_error());
	}
	if cred.uid != 0 && cred.uid != launch_uid {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			format!("control socket peer uid {} is not root or launch uid {launch_uid}", cred.uid),
		));
	}
	Ok(())
}

#[cfg(target_os = "macos")]
fn verify_peer_credentials(stream: &UnixStream, launch_uid: u32) -> io::Result<()> {
	let mut euid: libc::uid_t = 0;
	let mut egid: libc::gid_t = 0;
	// SAFETY: pointers are valid for writes and fd comes from a live UnixStream.
	let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut euid, &mut egid) };
	if rc != 0 {
		return Err(io::Error::last_os_error());
	}
	if euid != 0 && euid != launch_uid {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			format!("control socket peer uid {euid} is not root or launch uid {launch_uid}"),
		));
	}
	Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn verify_peer_credentials(_: &UnixStream, _: u32) -> io::Result<()> {
	Ok(())
}

#[derive(Serialize)]
struct Banner<'a> {
	vmm: &'a str,
	api: u8,
}

#[derive(Deserialize)]
struct Request {
	id:     u64,
	method: String,
	#[serde(default)]
	params: serde_json::Value,
}

#[derive(Serialize)]
struct Response {
	#[serde(skip_serializing_if = "Option::is_none")]
	id:     Option<u64>,
	ok:     bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	result: Option<serde_json::Value>,
	#[serde(skip_serializing_if = "Option::is_none")]
	error:  Option<ApiError>,
}

enum RequestLine {
	Text(String),
	BadRequest { message: &'static str, close: bool },
}

/// Serve one client connection until EOF or an IO error.
fn handle_connection(stream: UnixStream, tx: Sender<ControlCmd>) {
	// One half for writing replies, the other (buffered) for reading commands.
	let Ok(mut writer) = stream.try_clone() else {
		return;
	};
	if write_json_line(&mut writer, &Banner { vmm: env!("CARGO_PKG_VERSION"), api: 1 }).is_err() {
		return;
	}

	let mut reader = BufReader::new(stream);
	let mut line = Vec::new();

	loop {
		let request = match read_request_line(&mut reader, &mut line) {
			Ok(Some(RequestLine::Text(request))) => request,
			Ok(Some(RequestLine::BadRequest { message, close })) => {
				let write_failed =
					write_error(&mut writer, None, ApiError::bad_request(message)).is_err();
				if write_failed || close {
					return;
				}
				continue;
			},
			Ok(None) | Err(_) => return,
		};

		let error_id = request_id_for_error(&request);
		let (id, kind) = match parse_request(&request) {
			Ok(parsed) => parsed,
			Err(error) => {
				if write_error(&mut writer, error_id, error).is_err() {
					return;
				}
				continue;
			},
		};

		crate::metrics::record_control_request();

		// Hand the command to the owning thread and wait for its JSON reply.
		let (rtx, rrx) = flume::bounded::<ControlReply>(1);
		if tx.send(ControlCmd { kind, reply: rtx }).is_err() {
			return; // VMM gone: nothing more to serve.
		}
		let reply = match rrx.recv_timeout(CONTROL_REPLY_TIMEOUT) {
			Ok(reply) => reply,
			Err(RecvTimeoutError::Timeout) => Err(ApiError::internal(format!(
				"control reply timed out after {CONTROL_REPLY_TIMEOUT:?}"
			))),
			Err(RecvTimeoutError::Disconnected) => return, // Reply dropped without a response.
		};
		let write_result = match reply {
			Ok(result) => write_success(&mut writer, id, result),
			Err(error) => write_error(&mut writer, Some(id), error),
		};
		if write_result.is_err() {
			return;
		}
	}
}

fn write_success(writer: &mut UnixStream, id: u64, result: serde_json::Value) -> io::Result<()> {
	write_json_line(writer, &Response {
		id:     Some(id),
		ok:     true,
		result: Some(result),
		error:  None,
	})
}

fn write_error(writer: &mut UnixStream, id: Option<u64>, error: ApiError) -> io::Result<()> {
	write_json_line(writer, &Response { id, ok: false, result: None, error: Some(error) })
}

fn write_json_line<T: Serialize>(writer: &mut UnixStream, value: &T) -> io::Result<()> {
	serde_json::to_writer(&mut *writer, value)
		.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
	writer.write_all(b"\n")
}

fn read_request_line(
	reader: &mut BufReader<UnixStream>,
	line: &mut Vec<u8>,
) -> io::Result<Option<RequestLine>> {
	line.clear();
	loop {
		let available = reader.fill_buf()?;
		if available.is_empty() {
			if line.is_empty() {
				return Ok(None);
			}
			break;
		}

		let take = available
			.iter()
			.position(|byte| *byte == b'\n')
			.map_or(available.len(), |pos| pos + 1);
		let newline = available[..take].last() == Some(&b'\n');
		let payload_len = take - usize::from(newline);
		if line.len() + payload_len > CONTROL_MAX_REQUEST_LINE {
			reader.consume(take);
			return Ok(Some(RequestLine::BadRequest {
				message: "control request line exceeds 65536 bytes",
				close:   true,
			}));
		}
		line.extend_from_slice(&available[..take]);
		reader.consume(take);
		if newline {
			break;
		}
	}

	let Ok(request) = std::str::from_utf8(line) else {
		return Ok(Some(RequestLine::BadRequest {
			message: "control request line is not valid UTF-8",
			close:   false,
		}));
	};
	Ok(Some(RequestLine::Text(request.trim().to_owned())))
}

fn request_id_for_error(request: &str) -> Option<u64> {
	serde_json::from_str::<serde_json::Value>(request)
		.ok()?
		.get("id")?
		.as_u64()
}

fn parse_request(request: &str) -> std::result::Result<(u64, ControlKind), ApiError> {
	let request: Request = serde_json::from_str(request)
		.map_err(|e| ApiError::bad_request(format!("malformed JSON request: {e}")))?;
	let kind = match request.method.as_str() {
		"ping" => ControlKind::Ping,
		"info" => ControlKind::Info,
		"pause" => ControlKind::Pause,
		"resume" => ControlKind::Resume,
		"snapshot" => {
			let name = request
				.params
				.get("name")
				.and_then(serde_json::Value::as_str)
				.ok_or_else(|| ApiError::bad_request("snapshot params.name must be a string"))?;
			let base = match request.params.get("base") {
				None | Some(serde_json::Value::Null) => None,
				Some(serde_json::Value::String(s)) => Some(s.clone()),
				Some(_) => {
					return Err(ApiError::bad_request("snapshot params.base must be a string"));
				},
			};
			let allow_user_net = match request.params.get("allow_user_net") {
				None | Some(serde_json::Value::Null) => false,
				Some(serde_json::Value::Bool(v)) => *v,
				Some(_) => {
					return Err(ApiError::bad_request("snapshot params.allow_user_net must be a bool"));
				},
			};
			ControlKind::Snapshot { name: name.to_owned(), base, allow_user_net }
		},
		"quit" => ControlKind::Quit,
		"metrics" => ControlKind::Metrics,
		"extend" => {
			let secs = request
				.params
				.get("secs")
				.and_then(serde_json::Value::as_u64)
				.ok_or_else(|| ApiError::bad_request("extend params.secs must be a number"))?;
			ControlKind::Extend { secs }
		},
		method => return Err(ApiError::unknown_method(format!("unknown lifecycle method {method}"))),
	};
	Ok((request.id, kind))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn json_parse_maps_methods_and_snapshot_name() {
		let (id, kind) =
			parse_request(r#"{"id":7,"method":"snapshot","params":{"name":"safe"}}"#).unwrap();
		assert_eq!(id, 7);
		assert!(matches!(
			kind,
			ControlKind::Snapshot {
				name,
				base: None,
				allow_user_net: false,
			} if name == "safe"
		));

		let (_, kind) =
			parse_request(r#"{"id":8,"method":"snapshot","params":{"name":"d","base":"base0"}}"#)
				.unwrap();
		assert!(matches!(
			kind,
			ControlKind::Snapshot {
				name,
				base,
				allow_user_net: false,
			} if name == "d" && base.as_deref() == Some("base0")
		));

		let (_, kind) = parse_request(
			r#"{"id":10,"method":"snapshot","params":{"name":"net","allow_user_net":true}}"#,
		)
		.unwrap();
		assert!(matches!(
			kind,
			ControlKind::Snapshot {
				name,
				base: None,
				allow_user_net: true,
			} if name == "net"
		));

		let (_, kind) = parse_request(r#"{"id":9,"method":"metrics","params":null}"#).unwrap();
		assert!(matches!(kind, ControlKind::Metrics));
	}

	#[test]
	fn json_parse_rejects_unknown_method() {
		let error = parse_request(r#"{"id":1,"method":"bogus","params":null}"#).unwrap_err();
		assert_eq!(error.code, "unknown_method");
	}

	#[test]
	fn root_prebind_allows_current_root_parent_for_sandbox_socket() {
		let owner = Some(SocketOwner { uid: 1000, gid: 1000 });

		let prebind_uid = allowed_prebind_parent_uid(owner, 1000, 0);

		assert_eq!(prebind_uid, Some(0));
		assert!(parent_owner_is_allowed(0, 1000, prebind_uid));
		assert!(parent_owner_is_allowed(1000, 1000, prebind_uid));
	}

	#[test]
	fn prebind_parent_allowance_is_limited_to_root_uid_drop() {
		let owner = Some(SocketOwner { uid: 1000, gid: 1000 });

		assert_eq!(allowed_prebind_parent_uid(owner, 1000, 1000), None);
		assert_eq!(allowed_prebind_parent_uid(owner, 1000, 501), None);
		assert_eq!(allowed_prebind_parent_uid(None, 0, 0), None);
		assert!(!parent_owner_is_allowed(0, 1000, None));
		assert!(!parent_owner_is_allowed(1001, 1000, Some(0)));
	}

	#[test]
	fn stale_socket_allowance_matches_expected_or_prebind_owner() {
		assert!(socket_owner_is_allowed(1000, 1000, None));
		assert!(socket_owner_is_allowed(0, 1000, Some(0)));
		assert!(!socket_owner_is_allowed(1001, 1000, Some(0)));
	}

	#[test]
	fn oversized_request_line_is_rejected_and_connection_is_closed() {
		use std::{io::Write, os::unix::net::UnixStream, thread};

		let (mut writer, reader) = UnixStream::pair().unwrap();
		let handle = thread::spawn(move || {
			let payload = vec![b'a'; CONTROL_MAX_REQUEST_LINE + 1];
			let _ = writer.write_all(&payload);
		});
		let mut reader = BufReader::new(reader);
		let mut line = Vec::new();

		let request = read_request_line(&mut reader, &mut line).unwrap();

		assert!(matches!(
			request,
			Some(RequestLine::BadRequest {
				message: "control request line exceeds 65536 bytes",
				close:   true,
			})
		));
		drop(reader);
		handle.join().unwrap();
	}

	#[test]
	fn cleanup_skips_reused_socket_path_with_different_inode() {
		use std::{fs, os::unix::net::UnixListener};

		let dir = std::env::temp_dir().join(format!(
			"vmon-control-cleanup-{}-{}",
			std::process::id(),
			"reuse"
		));
		let _ = fs::remove_dir_all(&dir);
		fs::create_dir(&dir).unwrap();
		let sock = dir.join("control.sock");
		let listener = UnixListener::bind(&sock).unwrap();
		let meta = fs::symlink_metadata(&sock).unwrap();
		let identity = SocketIdentity::from_metadata(&meta);
		drop(listener);
		fs::remove_file(&sock).unwrap();

		let replacement = UnixListener::bind(&sock).unwrap();
		cleanup_socket_for_uid(&sock, meta.uid(), Some(identity));

		assert!(sock.exists(), "cleanup removed a replacement socket");
		drop(replacement);
		fs::remove_file(&sock).unwrap();
		fs::remove_dir(&dir).unwrap();
	}

	#[test]
	fn private_parent_mode_rejects_group_or_world_access() {
		assert!(private_parent_mode_is_safe(0o700));
		assert!(private_parent_mode_is_safe(0o500));
		assert!(!private_parent_mode_is_safe(0o710));
		assert!(!private_parent_mode_is_safe(0o770));
		assert!(!private_parent_mode_is_safe(0o707));
	}
}
