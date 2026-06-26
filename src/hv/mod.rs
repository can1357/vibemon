//! Hypervisor backend seam.
//!
//! The VMM is written against one concrete `Vm` / `Vcpu` / `IrqLine` / `Gic`
//! type whose implementation is selected at compile time: KVM on Linux,
//! Apple Hypervisor.framework (HVF) on macOS. Both backends expose identical
//! inherent method surfaces (see `local://hvf-design.md`), so call sites stay
//! monomorphic — no trait objects in the vCPU run loop.

mod neutral;
pub use neutral::*;

#[cfg(target_os = "linux")]
mod kvm;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub use kvm::Gic;
#[cfg(target_os = "linux")]
pub use kvm::{IrqLine, Vcpu, Vm};

#[cfg(all(target_os = "macos", not(target_arch = "aarch64")))]
compile_error!("the macOS Hypervisor.framework backend supports only Apple Silicon (aarch64)");

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
mod hvf;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub use hvf::{Gic, IrqLine, Vcpu, Vm};
