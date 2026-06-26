//! Pipe-backed macOS implementation of the Linux `eventfd(2)` surface.
//!
//! The shim keeps the counter in shared userspace state and uses the pipe only
//! as a level-triggered readiness token.  A transition from zero to non-zero
//! writes one token; further writes coalesce into the counter until `read()`
//! drains it.  This preserves pollability without letting wakeup storms fill
//! the finite macOS pipe buffer.

use std::{
	io,
	os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
	sync::{Arc, Condvar, Mutex, MutexGuard},
};

const LINUX_EFD_NONBLOCK: i32 = 0x800;
const LINUX_EFD_CLOEXEC: i32 = 0x80000;
const SUPPORTED_FLAGS: i32 = LINUX_EFD_NONBLOCK | LINUX_EFD_CLOEXEC | libc::O_NONBLOCK;

#[derive(Debug, Default)]
struct CounterState {
	value:        u64,
	pending_wake: bool,
}

/// A pollable, counting wakeup object backed by a close-on-exec pipe.
#[derive(Debug)]
pub struct EventFd {
	read_fd:     OwnedFd,
	write_fd:    OwnedFd,
	nonblocking: bool,
	state:       Arc<(Mutex<CounterState>, Condvar)>,
}

impl EventFd {
	/// Creates a pipe-backed eventfd-compatible wakeup object.
	pub fn new(flags: i32) -> io::Result<Self> {
		if flags & !SUPPORTED_FLAGS != 0 {
			return Err(io::Error::from_raw_os_error(libc::EINVAL));
		}

		let mut fds = [-1; 2];
		// SAFETY: `fds` is a valid pointer to two `c_int` slots that `pipe`
		// initializes on success; the return value is checked before use.
		if unsafe { libc::pipe(fds.as_mut_ptr()) } == -1 {
			return Err(io::Error::last_os_error());
		}

		// SAFETY: `pipe` succeeded, so both returned file descriptors are
		// open and uniquely transferred into `OwnedFd` for RAII ownership.
		let (read_fd, write_fd) =
			unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };

		set_cloexec(read_fd.as_raw_fd())?;
		set_cloexec(write_fd.as_raw_fd())?;

		let nonblocking = flags & (LINUX_EFD_NONBLOCK | libc::O_NONBLOCK) != 0;
		set_nonblocking(read_fd.as_raw_fd())?;
		set_nonblocking(write_fd.as_raw_fd())?;

		Ok(Self {
			read_fd,
			write_fd,
			nonblocking,
			state: Arc::new((Mutex::new(CounterState::default()), Condvar::new())),
		})
	}

	/// Adds a value to the counter and arms the pollable wake token if needed.
	pub fn write(&self, v: u64) -> io::Result<()> {
		if v == u64::MAX {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"eventfd write value must be less than u64::MAX",
			));
		}
		if v == 0 {
			return Ok(());
		}

		let (lock, cv) = &*self.state;
		let mut state = lock_state(lock)?;
		while state.value > (u64::MAX - 1).saturating_sub(v) {
			if self.nonblocking {
				return Err(io::Error::new(io::ErrorKind::WouldBlock, "eventfd counter overflow"));
			}
			state = cv
				.wait(state)
				.map_err(|_| io::Error::other("eventfd counter state poisoned"))?;
		}
		let old_value = state.value;
		let old_pending_wake = state.pending_wake;
		state.value += v;

		if state.pending_wake {
			return Ok(());
		}

		state.pending_wake = true;
		match write_all_fd(self.write_fd.as_raw_fd(), &1u64.to_ne_bytes()) {
			Ok(()) => Ok(()),
			Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(()),
			Err(err) => {
				state.value = old_value;
				state.pending_wake = old_pending_wake;
				Err(err)
			},
		}
	}

	/// Drains the current counter value and clears the pollable wake token.
	pub fn read(&self) -> io::Result<u64> {
		loop {
			{
				let (lock, cv) = &*self.state;
				let mut state = lock_state(lock)?;
				if state.value != 0 {
					drain_pipe(self.read_fd.as_raw_fd())?;
					let total = state.value;
					state.value = 0;
					state.pending_wake = false;
					cv.notify_all();
					return Ok(total);
				}
				if state.pending_wake {
					drain_pipe(self.read_fd.as_raw_fd())?;
					state.pending_wake = false;
				}
			}

			if self.nonblocking {
				return Err(io::Error::from(io::ErrorKind::WouldBlock));
			}
			wait_readable(self.read_fd.as_raw_fd())?;
		}
	}

	/// Duplicates both pipe ends into a new handle for the same wakeup stream.
	pub fn try_clone(&self) -> io::Result<Self> {
		Ok(Self {
			read_fd:     dup_fd(self.read_fd.as_raw_fd())?,
			write_fd:    dup_fd(self.write_fd.as_raw_fd())?,
			nonblocking: self.nonblocking,
			state:       Arc::clone(&self.state),
		})
	}
}

impl AsRawFd for EventFd {
	fn as_raw_fd(&self) -> RawFd {
		self.read_fd.as_raw_fd()
	}
}

fn lock_state(state: &Mutex<CounterState>) -> io::Result<MutexGuard<'_, CounterState>> {
	state
		.lock()
		.map_err(|_| io::Error::other("eventfd counter state poisoned"))
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
	// SAFETY: `fd` is passed by value to `fcntl`; no pointers are involved and
	// the syscall result is checked for failure.
	let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
	if flags == -1 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: `fd` is a raw file descriptor and `flags` came from `F_GETFD`;
	// `fcntl` does not retain references and its return value is checked.
	if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
	// SAFETY: `fd` is passed by value to `fcntl`; no pointers are involved and
	// the syscall result is checked for failure.
	let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
	if flags == -1 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: `fd` is a raw file descriptor and `flags` came from `F_GETFL`;
	// `fcntl` does not retain references and its return value is checked.
	if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

fn dup_fd(fd: RawFd) -> io::Result<OwnedFd> {
	// SAFETY: `fd` is passed by value to `fcntl`; duplicating it does not
	// access Rust memory, and the returned descriptor is checked before use.
	let new_fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
	if new_fd == -1 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: `F_DUPFD_CLOEXEC` returned a fresh open descriptor that is not
	// owned elsewhere in Rust, so transferring it to `OwnedFd` is unique.
	let new_fd = unsafe { OwnedFd::from_raw_fd(new_fd) };
	Ok(new_fd)
}

fn write_all_fd(fd: RawFd, mut buf: &[u8]) -> io::Result<()> {
	while !buf.is_empty() {
		// SAFETY: `buf.as_ptr()` is valid for `buf.len()` bytes for the duration
		// of the call, and `write` does not retain the pointer.
		let ret = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
		if ret == -1 {
			let err = io::Error::last_os_error();
			if err.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			return Err(err);
		}
		if ret == 0 {
			return Err(io::Error::new(io::ErrorKind::WriteZero, "eventfd pipe write returned zero"));
		}
		buf = &buf[ret as usize..];
	}
	Ok(())
}

fn read_u64(fd: RawFd) -> io::Result<u64> {
	let mut buf = [0u8; 8];
	loop {
		// SAFETY: `buf.as_mut_ptr()` is valid for `buf.len()` writable bytes for
		// the duration of the call, and `read` does not retain the pointer.
		let ret = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
		if ret == -1 {
			let err = io::Error::last_os_error();
			if err.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			return Err(err);
		}
		if ret == 0 {
			return Err(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"eventfd pipe read returned EOF",
			));
		}
		if ret != buf.len() as isize {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"eventfd pipe read returned a partial counter record",
			));
		}
		return Ok(u64::from_ne_bytes(buf));
	}
}

fn drain_pipe(fd: RawFd) -> io::Result<()> {
	loop {
		match read_u64(fd) {
			Ok(_) => {},
			Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
			Err(err) => return Err(err),
		}
	}
}

fn wait_readable(fd: RawFd) -> io::Result<()> {
	let mut pollfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
	loop {
		// SAFETY: `pollfd` points to one initialized `pollfd` entry for this
		// blocking call; `poll` only mutates it before returning.
		let ret = unsafe { libc::poll(&mut pollfd, 1, -1) };
		if ret == -1 {
			let err = io::Error::last_os_error();
			if err.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			return Err(err);
		}
		return Ok(());
	}
}

#[cfg(test)]
mod tests {
	use super::{EventFd, LINUX_EFD_NONBLOCK};

	#[test]
	fn write_then_read_coalesces_values() {
		let evt = EventFd::new(LINUX_EFD_NONBLOCK).unwrap();
		evt.write(2).unwrap();
		evt.write(3).unwrap();

		assert_eq!(evt.read().unwrap(), 5);
		assert_eq!(evt.read().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
	}

	#[test]
	fn clone_shares_counter_stream() {
		let evt = EventFd::new(libc::O_NONBLOCK).unwrap();
		let clone = evt.try_clone().unwrap();

		evt.write(1).unwrap();
		clone.write(4).unwrap();

		assert_eq!(clone.read().unwrap(), 5);
		assert_eq!(evt.read().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
	}

	#[test]
	fn repeated_nonblocking_writes_coalesce_without_filling_pipe() {
		let evt = EventFd::new(LINUX_EFD_NONBLOCK).unwrap();

		for _ in 0..65_536 {
			evt.write(1).unwrap();
		}

		assert_eq!(evt.read().unwrap(), 65_536);
		assert_eq!(evt.read().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
	}

	#[test]
	fn coalesced_counter_stays_pollable_until_read() {
		use std::os::fd::AsRawFd;

		let evt = EventFd::new(LINUX_EFD_NONBLOCK).unwrap();
		evt.write(1).unwrap();
		evt.write(1).unwrap();

		let mut pollfd = libc::pollfd { fd: evt.as_raw_fd(), events: libc::POLLIN, revents: 0 };
		// SAFETY: `pollfd` points to one initialized entry and remains valid
		// for the nonblocking `poll` call.
		let ready = unsafe { libc::poll(&mut pollfd, 1, 0) };
		assert_eq!(ready, 1);
		assert_ne!(pollfd.revents & libc::POLLIN, 0);

		assert_eq!(evt.read().unwrap(), 2);
		pollfd.revents = 0;
		// SAFETY: `pollfd` points to one initialized entry and remains valid
		// for the nonblocking `poll` call.
		let ready = unsafe { libc::poll(&mut pollfd, 1, 0) };
		assert_eq!(ready, 0);
	}
}
