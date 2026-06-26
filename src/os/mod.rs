//! Host OS primitives that differ between Linux and macOS.
//!
//! [`EventFd`] is a counting, pollable wakeup object with the
//! `vmm_sys_util::eventfd::EventFd` API (`new(flags)`, `write(u64)`,
//! `read() -> io::Result<u64>`, `try_clone()`, `AsRawFd`). On Linux it is the
//! real `eventfd(2)` (required by KVM irqfd/ioeventfd). On macOS it is a
//! pipe-backed shim with the same surface.

/// `EFD_NONBLOCK` flag for [`EventFd::new`]. Same numeric value on both
/// backends (the macOS shim recognizes it); on Linux it equals
/// `libc::EFD_NONBLOCK`.
pub const EFD_NONBLOCK: i32 = 0x800;

#[cfg(target_os = "linux")]
pub use vmm_sys_util::eventfd::EventFd;

#[cfg(target_os = "macos")]
mod eventfd;
#[cfg(target_os = "macos")]
pub use eventfd::EventFd;
