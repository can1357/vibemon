//! HVF aarch64 vCPU snapshot capture and replay.
//!
//! Captures a curated migratable register set (the GP registers, PC, CPSR,
//! FPCR/FPSR, Q0..Q31, EL1/timer system registers, GIC CPU-interface
//! registers, PSCI power state, and virtual-timer controls) into the
//! [`VcpuState::Hvf`] variant. The KVM backend uses a different variant, so a
//! cross-backend restore fails the `match` below.

use applevisor::prelude::{GicIccReg, Reg, SimdFpReg};

use crate::{
	arch::state::VcpuState,
	hv::{
		SysReg,
		hvf::vcpu::{Vcpu, sys_reg_to_hvf},
	},
	result::{Result, err},
};

/// System registers captured for migration, in a fixed order.
const MIGRATION_SYSREGS: &[SysReg] = &[
	SysReg::new(3, 0, 0, 0, 5),  // MPIDR_EL1
	SysReg::new(3, 0, 1, 0, 0),  // SCTLR_EL1
	SysReg::new(3, 0, 1, 0, 1),  // ACTLR_EL1
	SysReg::new(3, 0, 1, 0, 2),  // CPACR_EL1
	SysReg::new(3, 0, 2, 0, 0),  // TTBR0_EL1
	SysReg::new(3, 0, 2, 0, 1),  // TTBR1_EL1
	SysReg::new(3, 0, 2, 0, 2),  // TCR_EL1
	SysReg::new(3, 0, 2, 1, 0),  // APIAKEYLO_EL1
	SysReg::new(3, 0, 2, 1, 1),  // APIAKEYHI_EL1
	SysReg::new(3, 0, 2, 1, 2),  // APIBKEYLO_EL1
	SysReg::new(3, 0, 2, 1, 3),  // APIBKEYHI_EL1
	SysReg::new(3, 0, 2, 2, 0),  // APDAKEYLO_EL1
	SysReg::new(3, 0, 2, 2, 1),  // APDAKEYHI_EL1
	SysReg::new(3, 0, 2, 2, 2),  // APDBKEYLO_EL1
	SysReg::new(3, 0, 2, 2, 3),  // APDBKEYHI_EL1
	SysReg::new(3, 0, 2, 3, 0),  // APGAKEYLO_EL1
	SysReg::new(3, 0, 2, 3, 1),  // APGAKEYHI_EL1
	SysReg::new(3, 0, 4, 0, 0),  // SPSR_EL1
	SysReg::new(3, 0, 4, 0, 1),  // ELR_EL1
	SysReg::new(3, 0, 4, 1, 0),  // SP_EL0
	SysReg::new(3, 0, 5, 1, 0),  // AFSR0_EL1
	SysReg::new(3, 0, 5, 1, 1),  // AFSR1_EL1
	SysReg::new(3, 0, 5, 2, 0),  // ESR_EL1
	SysReg::new(3, 0, 6, 0, 0),  // FAR_EL1
	SysReg::new(3, 0, 7, 4, 0),  // PAR_EL1
	SysReg::new(3, 0, 10, 2, 0), // MAIR_EL1
	SysReg::new(3, 0, 10, 3, 0), // AMAIR_EL1
	SysReg::new(3, 0, 12, 0, 0), // VBAR_EL1
	SysReg::new(3, 0, 13, 0, 1), // CONTEXTIDR_EL1
	SysReg::new(3, 0, 13, 0, 4), // TPIDR_EL1
	SysReg::new(3, 0, 14, 1, 0), // CNTKCTL_EL1
	SysReg::new(3, 2, 0, 0, 0),  // CSSELR_EL1
	SysReg::new(3, 3, 13, 0, 2), // TPIDR_EL0
	SysReg::new(3, 3, 13, 0, 3), // TPIDRRO_EL0
	SysReg::new(3, 3, 14, 3, 1), // CNTV_CTL_EL0
	SysReg::new(3, 3, 14, 3, 2), // CNTV_CVAL_EL0
	SysReg::new(3, 4, 4, 1, 0),  // SP_EL1
];

/// GIC CPU-interface registers captured for migration, in restore-safe order.
const MIGRATION_ICC_REGS: &[(u64, GicIccReg)] = &[
	(0xc665, GicIccReg::SRE_EL1),
	(0xc664, GicIccReg::CTLR_EL1),
	(0xc666, GicIccReg::IGRPEN0_EL1),
	(0xc667, GicIccReg::IGRPEN1_EL1),
	(0xc230, GicIccReg::PMR_EL1),
	(0xc643, GicIccReg::BPR0_EL1),
	(0xc663, GicIccReg::BPR1_EL1),
	(0xc644, GicIccReg::AP0R0_EL1),
	(0xc648, GicIccReg::AP1R0_EL1),
];

/// Index used for PC in the `regs` vector (after X0..X30).
const REG_PC: u8 = 31;
/// Index used for CPSR/PSTATE in the `regs` vector.
const REG_CPSR: u8 = 32;
/// Index used for FPCR in the `regs` vector.
const REG_FPCR: u8 = 33;
/// Index used for FPSR in the `regs` vector.
const REG_FPSR: u8 = 34;

/// Capture a curated migratable HVF aarch64 vCPU state.
pub fn save_vcpu(vcpu: &Vcpu) -> Result<VcpuState> {
	let inner = vcpu.inner();
	let mut regs = Vec::with_capacity(35);
	for idx in 0..=30u8 {
		regs.push((idx, vcpu.get_gpr(idx)?));
	}
	regs.push((REG_PC, inner.get_reg(Reg::PC)?));
	regs.push((REG_CPSR, inner.get_reg(Reg::CPSR)?));
	regs.push((REG_FPCR, inner.get_reg(Reg::FPCR)?));
	regs.push((REG_FPSR, inner.get_reg(Reg::FPSR)?));

	let mut sys_regs = Vec::with_capacity(MIGRATION_SYSREGS.len());
	for &reg in MIGRATION_SYSREGS {
		sys_regs.push((reg, inner.get_sys_reg(sys_reg_to_hvf(reg)?)?));
	}

	let mut icc_regs = Vec::with_capacity(MIGRATION_ICC_REGS.len());
	for &(encoding, reg) in MIGRATION_ICC_REGS {
		icc_regs.push((encoding, inner.get_icc_reg(reg)?));
	}

	let mut simd_fp_regs = Vec::with_capacity(32);
	for idx in 0..32u8 {
		simd_fp_regs.push((idx, inner.get_simd_fp_reg(simd_fp_reg(idx)?)?));
	}

	Ok(VcpuState::Hvf {
		regs,
		sys_regs,
		icc_regs,
		simd_fp_regs,
		powered: vcpu.is_powered(),
		vtimer_offset: inner.get_vtimer_offset()?,
		vtimer_masked: inner.get_vtimer_mask()?,
	})
}

/// Restore a curated migratable HVF aarch64 vCPU state.
pub fn restore_vcpu(vcpu: &Vcpu, st: &VcpuState) -> Result<()> {
	let inner = vcpu.inner();
	let (regs, sys_regs, icc_regs, simd_fp_regs, powered, vtimer_offset, vtimer_masked) = match st {
		VcpuState::Hvf {
			regs,
			sys_regs,
			icc_regs,
			simd_fp_regs,
			powered,
			vtimer_offset,
			vtimer_masked,
		} => (regs, sys_regs, icc_regs, simd_fp_regs, *powered, *vtimer_offset, *vtimer_masked),
		_ => return Err(err("snapshot vCPU state was not captured on HVF")),
	};

	for &(idx, value) in regs {
		match idx {
			0..=30 => vcpu.set_gpr(idx, value)?,
			REG_PC => inner.set_reg(Reg::PC, value)?,
			REG_CPSR => inner.set_reg(Reg::CPSR, value)?,
			REG_FPCR => inner.set_reg(Reg::FPCR, value)?,
			REG_FPSR => inner.set_reg(Reg::FPSR, value)?,
			_ => return Err(err(format!("invalid HVF saved register index {idx}"))),
		}
	}
	for &(reg, value) in sys_regs {
		inner.set_sys_reg(sys_reg_to_hvf(reg)?, value)?;
	}
	for &(reg, value) in icc_regs {
		inner.set_icc_reg(icc_reg_to_hvf(reg)?, value)?;
	}
	for &(idx, value) in simd_fp_regs {
		inner.set_simd_fp_reg(simd_fp_reg(idx)?, value)?;
	}
	vcpu.set_powered(powered)?;
	inner.set_vtimer_offset(vtimer_offset)?;
	inner.set_vtimer_mask(vtimer_masked)?;
	Ok(())
}

fn icc_reg_to_hvf(reg: u64) -> Result<GicIccReg> {
	Ok(match reg {
		0xc230 => GicIccReg::PMR_EL1,
		0xc643 => GicIccReg::BPR0_EL1,
		0xc644 => GicIccReg::AP0R0_EL1,
		0xc648 => GicIccReg::AP1R0_EL1,
		0xc663 => GicIccReg::BPR1_EL1,
		0xc664 => GicIccReg::CTLR_EL1,
		0xc665 => GicIccReg::SRE_EL1,
		0xc666 => GicIccReg::IGRPEN0_EL1,
		0xc667 => GicIccReg::IGRPEN1_EL1,
		_ => return Err(err(format!("invalid HVF saved ICC register {reg:#x}"))),
	})
}

fn simd_fp_reg(idx: u8) -> Result<SimdFpReg> {
	Ok(match idx {
		0 => SimdFpReg::Q0,
		1 => SimdFpReg::Q1,
		2 => SimdFpReg::Q2,
		3 => SimdFpReg::Q3,
		4 => SimdFpReg::Q4,
		5 => SimdFpReg::Q5,
		6 => SimdFpReg::Q6,
		7 => SimdFpReg::Q7,
		8 => SimdFpReg::Q8,
		9 => SimdFpReg::Q9,
		10 => SimdFpReg::Q10,
		11 => SimdFpReg::Q11,
		12 => SimdFpReg::Q12,
		13 => SimdFpReg::Q13,
		14 => SimdFpReg::Q14,
		15 => SimdFpReg::Q15,
		16 => SimdFpReg::Q16,
		17 => SimdFpReg::Q17,
		18 => SimdFpReg::Q18,
		19 => SimdFpReg::Q19,
		20 => SimdFpReg::Q20,
		21 => SimdFpReg::Q21,
		22 => SimdFpReg::Q22,
		23 => SimdFpReg::Q23,
		24 => SimdFpReg::Q24,
		25 => SimdFpReg::Q25,
		26 => SimdFpReg::Q26,
		27 => SimdFpReg::Q27,
		28 => SimdFpReg::Q28,
		29 => SimdFpReg::Q29,
		30 => SimdFpReg::Q30,
		31 => SimdFpReg::Q31,
		_ => return Err(err(format!("invalid SIMD/FP register index {idx}"))),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn icc_mapping_accepts_migration_registers() {
		for &(encoding, reg) in MIGRATION_ICC_REGS {
			assert_eq!(icc_reg_to_hvf(encoding).unwrap(), reg);
		}
	}

	#[test]
	fn icc_mapping_rejects_unknown_register() {
		assert!(icc_reg_to_hvf(0).is_err());
	}
}
