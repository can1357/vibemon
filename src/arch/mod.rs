//! Architecture-specific support, selected at compile time.
//!
//! Both arches share the device model (`crate::virtio`, `crate::devices`); only
//! boot setup, vCPU register state, and the interrupt controller differ.

#[cfg(target_arch = "x86_64")]
mod x86_64;
#[cfg(target_arch = "x86_64")]
pub use x86_64::*;

#[cfg(target_arch = "aarch64")]
mod aarch64;
#[cfg(target_arch = "aarch64")]
pub use aarch64::*;
