//! KVM aarch64 vCPU and GICv3 snapshot / restore.
//!
//! The GICv3 register groups, MMIO offsets, conditional active-priority
//! register handling, and the MPIDR -> device-attr packing below are
//! transcribed from Firecracker v1.10.0 (Apache-2.0). The serialized structs
//! live in `arch::state`; this module only captures and replays KVM-specific
//! state.

use kvm_bindings::{
	KVM_DEV_ARM_VGIC_CTRL_INIT, KVM_DEV_ARM_VGIC_GRP_CPU_SYSREGS, KVM_DEV_ARM_VGIC_GRP_CTRL,
	KVM_DEV_ARM_VGIC_GRP_DIST_REGS, KVM_DEV_ARM_VGIC_GRP_LEVEL_INFO,
	KVM_DEV_ARM_VGIC_GRP_REDIST_REGS, KVM_DEV_ARM_VGIC_LINE_LEVEL_INFO_SHIFT,
	KVM_DEV_ARM_VGIC_SAVE_PENDING_TABLES, KVM_REG_ARM64_SYSREG_CRM_MASK,
	KVM_REG_ARM64_SYSREG_CRM_SHIFT, KVM_REG_ARM64_SYSREG_CRN_MASK, KVM_REG_ARM64_SYSREG_CRN_SHIFT,
	KVM_REG_ARM64_SYSREG_OP0_MASK, KVM_REG_ARM64_SYSREG_OP0_SHIFT, KVM_REG_ARM64_SYSREG_OP1_MASK,
	KVM_REG_ARM64_SYSREG_OP1_SHIFT, KVM_REG_ARM64_SYSREG_OP2_MASK, KVM_REG_ARM64_SYSREG_OP2_SHIFT,
	KVM_REG_SIZE_MASK, KVM_REG_SIZE_SHIFT, RegList, VGIC_LEVEL_INFO_LINE_LEVEL, kvm_device_attr,
	kvm_mp_state, kvm_vcpu_events,
};
use kvm_ioctls::{DeviceFd, VcpuFd};

use crate::{
	arch::state::{GicState, MachineState, VcpuState},
	bail,
	result::{Result, err},
};

/// First SPI INTID (SGIs 0-15 and PPIs 16-31 are private / per-redistributor).
const IRQ_BASE: u32 = 32;

/// Capture all migratable KVM arm64 vCPU state.
pub fn save_vcpu(vcpu: &VcpuFd, _xsave_size: usize) -> Result<VcpuState> {
	let mut reg_list = RegList::new(500).map_err(|e| err(format!("RegList alloc: {e:?}")))?;
	match vcpu.get_reg_list(&mut reg_list) {
		Ok(()) => {},
		Err(e) if e.errno() == libc::E2BIG => {
			let needed = reg_list.as_fam_struct_ref().n as usize;
			reg_list = RegList::new(needed).map_err(|e| err(format!("RegList alloc: {e:?}")))?;
			vcpu.get_reg_list(&mut reg_list)?;
		},
		Err(e) => return Err(e.into()),
	}

	let ids = reg_list.as_slice();
	let mut regs = Vec::with_capacity(ids.len());
	for &id in ids {
		let size = 1usize << (((id & KVM_REG_SIZE_MASK) >> KVM_REG_SIZE_SHIFT) as u32);
		let mut buf = vec![0u8; size];
		vcpu.get_one_reg(id, &mut buf)?;
		regs.push((id, buf));
	}

	let mp = vcpu.get_mp_state()?;
	// SAFETY: `mp` is a fully initialized plain KVM UAPI struct returned by the
	// kernel. Reading exactly its byte representation into an owned Vec does not
	// outlive or mutate the source object.
	let mp_state = unsafe {
		std::slice::from_raw_parts(
			(&mp as *const kvm_mp_state) as *const u8,
			std::mem::size_of::<kvm_mp_state>(),
		)
	}
	.to_vec();

	let ev = vcpu.get_vcpu_events()?;
	// SAFETY: `ev` is a fully initialized plain KVM UAPI struct returned by the
	// kernel. Reading exactly its byte representation into an owned Vec does not
	// outlive or mutate the source object.
	let vcpu_events = unsafe {
		std::slice::from_raw_parts(
			(&ev as *const kvm_vcpu_events) as *const u8,
			std::mem::size_of::<kvm_vcpu_events>(),
		)
	}
	.to_vec();

	Ok(VcpuState::Kvm { regs, mp_state, vcpu_events })
}

/// Apply saved KVM arm64 vCPU state after KVM_ARM_VCPU_INIT.
pub fn restore_vcpu(vcpu: &VcpuFd, st: &VcpuState) -> Result<()> {
	let (regs, mp_state, vcpu_events) = match st {
		VcpuState::Kvm { regs, mp_state, vcpu_events } => (regs, mp_state, vcpu_events),
		_ => bail!("cannot restore non-KVM vCPU state with the KVM backend"),
	};

	for (id, val) in regs {
		if let Err(e) = vcpu.set_one_reg(*id, val) {
			bail!("set_one_reg failed for reg {id:#x}: {e:?}");
		}
	}

	if mp_state.len() == std::mem::size_of::<kvm_mp_state>() {
		let mut mp = kvm_mp_state::default();
		// SAFETY: the length check above guarantees the source slice contains
		// exactly one `kvm_mp_state` byte representation, and `mp` is a valid,
		// writable destination of that size.
		unsafe {
			std::ptr::copy_nonoverlapping(
				mp_state.as_ptr(),
				(&mut mp as *mut kvm_mp_state) as *mut u8,
				std::mem::size_of::<kvm_mp_state>(),
			);
		}
		vcpu.set_mp_state(mp)?;
	} else {
		bail!("invalid KVM MP state length {}", mp_state.len());
	}

	if vcpu_events.len() == std::mem::size_of::<kvm_vcpu_events>() {
		let mut ev = kvm_vcpu_events::default();
		// SAFETY: the length check above guarantees the source slice contains
		// exactly one `kvm_vcpu_events` byte representation, and `ev` is a valid,
		// writable destination of that size.
		unsafe {
			std::ptr::copy_nonoverlapping(
				vcpu_events.as_ptr(),
				(&mut ev as *mut kvm_vcpu_events) as *mut u8,
				std::mem::size_of::<kvm_vcpu_events>(),
			);
		}
		vcpu.set_vcpu_events(&ev)?;
	} else {
		bail!("invalid KVM vCPU events length {}", vcpu_events.len());
	}
	Ok(())
}

// ===========================================================================
// GICv3 register tables (transcribed from Firecracker v1.10.0)
// ===========================================================================

/// Distributor registers in Firecracker's `VGIC_DIST_REGS` order.
/// `(mmio_offset, bits_per_irq)`; `bits_per_irq == 0` marks a simple 32-bit
/// register (one u32 access at `mmio_offset`).
const DIST_REGS: &[(u64, u32)] = &[
	(0x0000, 0),  // GICD_CTLR
	(0x0010, 0),  // GICD_STATUSR
	(0x0180, 1),  // GICD_ICENABLER
	(0x0100, 1),  // GICD_ISENABLER
	(0x0080, 1),  // GICD_IGROUPR
	(0x6000, 64), // GICD_IROUTER
	(0x0c00, 2),  // GICD_ICFGR
	(0x0280, 1),  // GICD_ICPENDR
	(0x0200, 1),  // GICD_ISPENDR
	(0x0380, 1),  // GICD_ICACTIVER
	(0x0300, 1),  // GICD_ISACTIVER
	(0x0400, 8),  // GICD_IPRIORITYR
];

/// SGI frame offset within the redistributor.
const GICR_SGI_BASE: u64 = 0x0001_0000;

/// Redistributor registers: PPI frame (`VGIC_RDIST_REGS`) chained with the SGI
/// frame (`VGIC_SGI_REGS`), in Firecracker's order. `(mmio_offset,
/// size_bytes)`, each accessed in 32-bit chunks.
const REDIST_REGS: &[(u64, u64)] = &[
	// PPI frame.
	(0x0000, 4), // GICR_CTLR
	(0x0010, 4), // GICR_STATUSR
	(0x0014, 4), // GICR_WAKER
	(0x0070, 8), // GICR_PROPBASER
	(0x0078, 8), // GICR_PENDBASER
	// SGI frame.
	(GICR_SGI_BASE + 0x0080, 4),  // GICR_IGROUPR0
	(GICR_SGI_BASE + 0x0180, 4),  // GICR_ICENABLER0
	(GICR_SGI_BASE + 0x0100, 4),  // GICR_ISENABLER0
	(GICR_SGI_BASE + 0x0c00, 8),  // GICR_ICFGR0
	(GICR_SGI_BASE + 0x0280, 4),  // GICR_ICPENDR0
	(GICR_SGI_BASE + 0x0200, 4),  // GICR_ISPENDR0
	(GICR_SGI_BASE + 0x0380, 4),  // GICR_ICACTIVER0
	(GICR_SGI_BASE + 0x0300, 4),  // GICR_ISACTIVER0
	(GICR_SGI_BASE + 0x0400, 32), // GICR_IPRIORITYR0
];

/// Build the CPU-interface sysreg "offset" used as the low bits of a
/// `KVM_DEV_ARM_VGIC_GRP_CPU_SYSREGS` attr (the (op0,op1,crn,crm,op2) encoding
/// without the KVM ONE_REG prefix). Mirrors Firecracker
/// `SimpleReg::vgic_sys_reg`.
const fn icc_off(op0: u64, op1: u64, crn: u64, crm: u64, op2: u64) -> u64 {
	((op0 << KVM_REG_ARM64_SYSREG_OP0_SHIFT) & KVM_REG_ARM64_SYSREG_OP0_MASK as u64)
		| ((op1 << KVM_REG_ARM64_SYSREG_OP1_SHIFT) & KVM_REG_ARM64_SYSREG_OP1_MASK as u64)
		| ((crn << KVM_REG_ARM64_SYSREG_CRN_SHIFT) & KVM_REG_ARM64_SYSREG_CRN_MASK as u64)
		| ((crm << KVM_REG_ARM64_SYSREG_CRM_SHIFT) & KVM_REG_ARM64_SYSREG_CRM_MASK as u64)
		| ((op2 << KVM_REG_ARM64_SYSREG_OP2_SHIFT) & KVM_REG_ARM64_SYSREG_OP2_MASK as u64)
}

/// ICC_CTLR_EL1 — read to discover the implemented number of priority bits.
const ICC_CTLR_EL1: u64 = icc_off(3, 0, 12, 12, 4);
const ICC_CTLR_EL1_PRIBITS_SHIFT: u64 = 8;
const ICC_CTLR_EL1_PRIBITS_MASK: u64 = 7 << ICC_CTLR_EL1_PRIBITS_SHIFT;

/// Always-present CPU-interface registers (`MAIN_VGIC_ICC_REGS` order).
const ICC_MAIN_REGS: &[u64] = &[
	icc_off(3, 0, 12, 12, 5), // ICC_SRE_EL1
	icc_off(3, 0, 12, 12, 4), // ICC_CTLR_EL1
	icc_off(3, 0, 12, 12, 6), // ICC_IGRPEN0_EL1
	icc_off(3, 0, 12, 12, 7), // ICC_IGRPEN1_EL1
	icc_off(3, 0, 4, 6, 0),   // ICC_PMR_EL1
	icc_off(3, 0, 12, 8, 3),  // ICC_BPR0_EL1
	icc_off(3, 0, 12, 12, 3), // ICC_BPR1_EL1
];

/// Active-priority registers (`AP_VGIC_ICC_REGS` order) with their AnRn index
/// `n`, which gates availability (Firecracker `is_ap_reg_available`).
const ICC_AP_REGS: &[(u64, u32)] = &[
	(icc_off(3, 0, 12, 8, 4), 0), // ICC_AP0R0_EL1
	(icc_off(3, 0, 12, 8, 5), 1), // ICC_AP0R1_EL1
	(icc_off(3, 0, 12, 8, 6), 2), // ICC_AP0R2_EL1
	(icc_off(3, 0, 12, 8, 7), 3), // ICC_AP0R3_EL1
	(icc_off(3, 0, 12, 9, 0), 0), // ICC_AP1R0_EL1
	(icc_off(3, 0, 12, 9, 1), 1), // ICC_AP1R1_EL1
	(icc_off(3, 0, 12, 9, 2), 2), // ICC_AP1R2_EL1
	(icc_off(3, 0, 12, 9, 3), 3), // ICC_AP1R3_EL1
];

/// Whether AnRn (index `n`) is implemented for `priority_bits` priority bits.
/// n==0 always; n==1 needs >=6; n>=2 needs exactly 7.
fn ap_reg_available(n: u32, priority_bits: u64) -> bool {
	match n {
		0 => true,
		1 => priority_bits >= 6,
		_ => priority_bits == 7,
	}
}

/// Pack a vCPU index into the GIC device-attr `mpidr` field (bits [63:32]).
///
/// KVM `reset_mpidr` assigns vCPU `i` the affinity Aff0=i[3:0], Aff1=i[11:4],
/// Aff2=i[19:12] (Aff3 unused). Firecracker's `construct_kvm_mpidrs` then packs
/// that affinity (`cpu_affid`) and shifts it into the attribute's high half.
/// Because Aff3 is always 0 here, `cpu_affid` equals MPIDR_EL1[23:0].
fn vcpu_mpidr_attr(index: u64) -> u64 {
	let aff0 = index & 0xf;
	let aff1 = (index >> 4) & 0xff;
	let aff2 = (index >> 12) & 0xff;
	let cpu_affid = aff0 | (aff1 << 8) | (aff2 << 16);
	cpu_affid << 32
}

/// `(start_offset, byte_len)` of a distributor register for `num_irqs` IRQs.
/// Mirrors Firecracker `DistReg::range()`: the SGI/PPI portion (0..32) is
/// RAZ/WI and skipped; only SPI bits are saved.
fn dist_reg_span(offset: u64, bits_per_irq: u32, num_irqs: u32) -> (u64, u64) {
	if bits_per_irq == 0 {
		return (offset, 4);
	}
	let bpi = u64::from(bits_per_irq);
	let start = offset + u64::from(IRQ_BASE) * bpi / 8;
	let size_in_bits = bpi * u64::from(num_irqs.saturating_sub(IRQ_BASE));
	(start, size_in_bits.div_ceil(8))
}

// ===========================================================================
// GIC device-attr access helpers
// ===========================================================================

/// Read one register chunk via `KVM_GET_DEVICE_ATTR` into a `u64`.
fn read_attr_val(gic: &DeviceFd, group: u32, attr_id: u64) -> Result<u64> {
	let mut val: u64 = 0;
	let mut dev_attr =
		kvm_device_attr { flags: 0, group, attr: attr_id, addr: std::ptr::addr_of_mut!(val) as u64 };
	// SAFETY: `val` is a valid, writable u64; the access size (<= 8 bytes) is
	// fixed by `group`/`attr_id`, so KVM never writes past it.
	unsafe { gic.get_device_attr(&mut dev_attr)? };
	Ok(val)
}

/// Read one chunk and append it to the flattened state.
fn read_attr(
	gic: &DeviceFd,
	group: u32,
	attr_id: u64,
	out: &mut Vec<(u32, u64, u64)>,
) -> Result<()> {
	let val = read_attr_val(gic, group, attr_id)?;
	out.push((group, attr_id, val));
	Ok(())
}

/// Write one register chunk back via `KVM_SET_DEVICE_ATTR`.
fn write_attr(gic: &DeviceFd, group: u32, attr_id: u64, value: u64) -> Result<()> {
	let dev_attr =
		kvm_device_attr { flags: 0, group, attr: attr_id, addr: std::ptr::addr_of!(value) as u64 };
	gic.set_device_attr(&dev_attr)?;
	Ok(())
}

/// Dump a register spanning `len` bytes at `base` in 32-bit accesses, each
/// OR-ed with the per-target `mpidr` selector (DIST and REDIST are both
/// accessed 4 bytes at a time).
fn dump_reg(
	gic: &DeviceFd,
	group: u32,
	mpidr: u64,
	base: u64,
	len: u64,
	out: &mut Vec<(u32, u64, u64)>,
) -> Result<()> {
	for off in (base..base + len).step_by(4) {
		read_attr(gic, group, mpidr | off, out)?;
	}
	Ok(())
}

/// Save the CPU-interface (ICC) registers for one vCPU, honoring the
/// implemented active-priority register count (Firecracker `get_icc_regs`).
fn save_icc_regs(gic: &DeviceFd, mpidr: u64, out: &mut Vec<(u32, u64, u64)>) -> Result<()> {
	let group = KVM_DEV_ARM_VGIC_GRP_CPU_SYSREGS;
	for &off in ICC_MAIN_REGS {
		read_attr(gic, group, mpidr | off, out)?;
	}

	let ctlr = read_attr_val(gic, group, mpidr | ICC_CTLR_EL1)?;
	let priority_bits = ((ctlr & ICC_CTLR_EL1_PRIBITS_MASK) >> ICC_CTLR_EL1_PRIBITS_SHIFT) + 1;
	for &(off, n) in ICC_AP_REGS {
		if ap_reg_available(n, priority_bits) {
			read_attr(gic, group, mpidr | off, out)?;
		}
	}
	Ok(())
}

// ===========================================================================
// GIC machine state
// ===========================================================================

/// Snapshot the whole KVM GICv3 from its device fd.
pub fn save_gic(gic: &DeviceFd, num_cpus: u64, num_irqs: u32) -> Result<GicState> {
	let pending = kvm_device_attr {
		flags: 0,
		group: KVM_DEV_ARM_VGIC_GRP_CTRL,
		attr:  u64::from(KVM_DEV_ARM_VGIC_SAVE_PENDING_TABLES),
		addr:  0,
	};
	gic.set_device_attr(&pending)?;

	let mut entries: Vec<(u32, u64, u64)> = Vec::new();

	for &(off, bits_per_irq) in DIST_REGS {
		let (base, len) = dist_reg_span(off, bits_per_irq, num_irqs);
		dump_reg(gic, KVM_DEV_ARM_VGIC_GRP_DIST_REGS, 0, base, len, &mut entries)?;
	}

	let info = u64::from(VGIC_LEVEL_INFO_LINE_LEVEL) << KVM_DEV_ARM_VGIC_LINE_LEVEL_INFO_SHIFT;
	let mut intid = u64::from(IRQ_BASE);
	while intid < u64::from(num_irqs) {
		read_attr(gic, KVM_DEV_ARM_VGIC_GRP_LEVEL_INFO, info | intid, &mut entries)?;
		intid += 32;
	}

	for cpu in 0..num_cpus {
		let mpidr = vcpu_mpidr_attr(cpu);
		for &(off, size) in REDIST_REGS {
			dump_reg(gic, KVM_DEV_ARM_VGIC_GRP_REDIST_REGS, mpidr, off, size, &mut entries)?;
		}
		save_icc_regs(gic, mpidr, &mut entries)?;
	}

	Ok(GicState::Kvm { entries })
}

/// Restore KVM GICv3 state by replaying every saved register.
pub fn restore_gic(gic: &DeviceFd, st: &GicState) -> Result<()> {
	let GicState::Kvm { entries } = st else {
		bail!("cannot restore HVF GIC state with KVM backend");
	};

	for &(group, attr_id, value) in entries {
		write_attr(gic, group, attr_id, value)?;
	}

	let init = kvm_device_attr {
		flags: 0,
		group: KVM_DEV_ARM_VGIC_GRP_CTRL,
		attr:  u64::from(KVM_DEV_ARM_VGIC_CTRL_INIT),
		addr:  0,
	};
	gic.set_device_attr(&init)?;
	Ok(())
}
