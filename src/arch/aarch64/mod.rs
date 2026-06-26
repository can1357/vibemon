//! aarch64 architecture support: Image boot, FDT, and boot vCPU registers.

pub mod boot;
pub mod fdt;
pub mod gic;
pub mod regs;
pub mod state;

use vm_memory::{Address, GuestAddress};

use crate::{hv::Vcpu, result::Result};

/// Configure one aarch64 vCPU for boot.
///
/// Application processors stay powered off until the guest issues PSCI
/// `CPU_ON`.
pub fn configure_vcpu(vcpu: &Vcpu, cpu_id: u8, entry: GuestAddress, fdt_addr: u64) -> Result<()> {
	if cpu_id == 0 {
		regs::setup_boot_regs(vcpu, entry.raw_value(), fdt_addr)?;
	}
	Ok(())
}
