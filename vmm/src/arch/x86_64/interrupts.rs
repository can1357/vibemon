//! Local APIC LVT configuration.
//!
//! Ported from Firecracker (Apache-2.0). For the legacy PIC path to deliver
//! interrupts in the guest, LINT0 must be `ExtINT` and LINT1 must be NMI.

use kvm_bindings::kvm_lapic_state;

use crate::{hv::Vcpu, result::Result};

// Offsets from apicdef.h.
const APIC_LVT0: usize = 0x350;
const APIC_LVT1: usize = 0x360;
const APIC_MODE_NMI: u32 = 0x4;
const APIC_MODE_EXTINT: u32 = 0x7;

fn get_klapic_reg(klapic: &kvm_lapic_state, reg_offset: usize) -> u32 {
	let r = &klapic.regs[reg_offset..reg_offset + 4];
	u32::from_le_bytes([r[0] as u8, r[1] as u8, r[2] as u8, r[3] as u8])
}

fn set_klapic_reg(klapic: &mut kvm_lapic_state, reg_offset: usize, value: u32) {
	let bytes = value.to_le_bytes();
	let r = &mut klapic.regs[reg_offset..reg_offset + 4];
	for (i, b) in bytes.iter().enumerate() {
		r[i] = *b as i8 as std::os::raw::c_char;
	}
}

const fn set_apic_delivery_mode(reg: u32, mode: u32) -> u32 {
	(reg & !0x700) | (mode << 8)
}

/// Configure LINT0 = `ExtINT` and LINT1 = NMI for the given vCPU.
pub fn set_lint(vcpu: &Vcpu) -> Result<()> {
	let mut klapic = vcpu.fd().get_lapic()?;

	let lvt0 = get_klapic_reg(&klapic, APIC_LVT0);
	set_klapic_reg(&mut klapic, APIC_LVT0, set_apic_delivery_mode(lvt0, APIC_MODE_EXTINT));

	let lvt1 = get_klapic_reg(&klapic, APIC_LVT1);
	set_klapic_reg(&mut klapic, APIC_LVT1, set_apic_delivery_mode(lvt1, APIC_MODE_NMI));

	vcpu.fd().set_lapic(&klapic)?;
	Ok(())
}
