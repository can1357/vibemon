//! Windows Hypervisor Platform backend for `x86_64` Windows hosts.

mod cpuid;
mod ioapic;
mod vcpu;
mod vm;

pub use cpuid::{CpuId, CpuIdEntry};
pub use ioapic::{IoApic, IoApicState, IrqLine};
pub use vcpu::Vcpu;
pub use vm::Vm;
