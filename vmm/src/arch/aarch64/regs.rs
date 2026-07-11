//! aarch64 vCPU register setup for the Linux boot protocol.
//!
//! The boot CPU enters at `EL1h` with the kernel entry in PC and the FDT
//! address in X0, per the arm64 Linux boot ABI.

use crate::{
	hv::{SysReg, Vcpu},
	result::Result,
};

// PSTATE bits (arch/arm64/include/uapi/asm/ptrace.h).
const PSR_MODE_EL1H: u64 = 0x0000_0005;
const PSR_F_BIT: u64 = 0x0000_0040;
const PSR_I_BIT: u64 = 0x0000_0080;
const PSR_A_BIT: u64 = 0x0000_0100;
const PSR_D_BIT: u64 = 0x0000_0200;
const PSTATE_FAULT_BITS_64: u64 = PSR_MODE_EL1H | PSR_A_BIT | PSR_F_BIT | PSR_I_BIT | PSR_D_BIT;

/// `MPIDR_EL1` — Multiprocessor Affinity Register.
pub const MPIDR_EL1: SysReg = SysReg::new(3, 0, 0, 0, 5);

/// Set PSTATE, PC, X0 (FDT), and the boot-ABI-reserved argument registers.
pub fn setup_boot_regs(vcpu: &Vcpu, entry: u64, fdt_addr: u64) -> Result<()> {
	vcpu.set_pstate(PSTATE_FAULT_BITS_64)?;
	vcpu.set_pc(entry)?;
	vcpu.set_gpr(0, fdt_addr)?;
	for reg in 1..=3 {
		vcpu.set_gpr(reg, 0)?;
	}
	Ok(())
}

/// Read the vCPU's MPIDR for the FDT `cpu@N` reg property.
pub fn read_mpidr(vcpu: &Vcpu) -> Result<u64> {
	vcpu.get_sys_reg(MPIDR_EL1)
}
