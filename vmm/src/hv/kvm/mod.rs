//! KVM hypervisor backend.

#[cfg(target_arch = "aarch64")]
mod gic;
mod irq;
#[cfg(target_arch = "aarch64")]
mod state;
mod vcpu;
mod vm;

#[cfg(target_arch = "aarch64")]
pub use gic::Gic;
pub use irq::IrqLine;
pub use vcpu::Vcpu;
pub use vm::Vm;
