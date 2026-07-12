//! Host OS primitives that differ across supported hosts.
//!
//! [`EventFd`] is a counting worker wakeup object with the `vmm_sys_util`
//! eventfd-shaped API (`new(flags)`, `write(u64)`, `read() -> io::Result<u64>`,
//! `try_clone()`). Linux uses `eventfd(2)`, macOS uses a pipe, and Windows uses
//! a counter paired with a waitable event handle.

/// `EFD_NONBLOCK` flag for [`EventFd::new`]. Non-Linux shims recognize the
/// Linux numeric value so callers can use one platform-neutral constant.
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
