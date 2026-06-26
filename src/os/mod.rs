//! Host OS primitives that differ across supported hosts.
//!
//! [`EventFd`] is a counting worker wakeup object with the `vmm_sys_util`
//! eventfd-shaped API (`new(flags)`, `write(u64)`, `read() -> io::Result<u64>`,
//! `try_clone()`). On Linux it is the real `eventfd(2)` (required by KVM
//! irqfd/ioeventfd). On macOS it is a pipe-backed shim. On Windows it is a
//! counter plus a waitable event handle.

/// `EFD_NONBLOCK` flag for [`EventFd::new`]. Same numeric value on all
/// backends; non-Linux shims recognize it. On Linux it equals
/// `libc::EFD_NONBLOCK`.
pub const EFD_NONBLOCK: i32 = 0x800;

#[cfg(target_os = "linux")]
pub use vmm_sys_util::eventfd::EventFd;

#[cfg(target_os = "macos")]
mod eventfd;
#[cfg(target_os = "macos")]
pub use eventfd::EventFd;

#[cfg(target_os = "windows")]
mod windows_eventfd;
#[cfg(target_os = "windows")]
pub use windows_eventfd::EventFd;
