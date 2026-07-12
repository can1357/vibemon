//! `x86_64` architecture support: boot setup, vCPU registers, MP tables, MSRs.

pub mod boot;
mod cpuid;
#[cfg(target_os = "linux")]
mod gdt;
mod interrupts;
pub mod mptable;
mod msr;
mod regs;
#[cfg(target_os = "linux")]
pub mod state;
#[cfg(target_os = "windows")]
#[path = "state_windows.rs"]
pub mod state;

#[cfg(target_os = "linux")]
use kvm_bindings::CpuId;
use vm_memory::{Address, GuestAddress};

#[cfg(target_os = "windows")]
use crate::hv::CpuId;
use crate::{hv::Vcpu, memory::GuestMemoryMmap, result::Result};

/// Configure one x86 vCPU for boot. Every vCPU gets CPUID, boot MSRs and LAPIC
/// LINT config; only the boot CPU (`cpu_id == 0`) is dropped into long mode.
pub fn configure_vcpu(
	vcpu: &Vcpu,
	mem: &GuestMemoryMmap,
	base_cpuid: &CpuId,
	cpu_id: u8,
	entry: GuestAddress,
) -> Result<()> {
	cpuid::setup_cpuid(vcpu, base_cpuid, cpu_id)?;
	msr::setup_msrs(vcpu)?;
	interrupts::set_lint(vcpu)?;

	if cpu_id == 0 {
		regs::setup_regs(vcpu, entry.raw_value())?;
		regs::setup_fpu(vcpu)?;
		regs::setup_sregs(mem, vcpu)?;
	}
	Ok(())
}
