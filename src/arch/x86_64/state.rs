#[cfg(not(target_os = "linux"))]
use serde::{Deserialize, Serialize};

#[cfg(not(target_os = "linux"))]
use crate::{bail, result::Result};

#[cfg(not(target_os = "linux"))]
#[derive(Serialize, Deserialize, Clone)]
pub struct VcpuState {}

#[cfg(not(target_os = "linux"))]
#[derive(Serialize, Deserialize, Clone)]
pub struct MachineState {}

#[cfg(not(target_os = "linux"))]
pub fn save_vcpu(_: &crate::hv::Vcpu, _: usize) -> Result<VcpuState> {
	bail!("x86_64 snapshots are not supported on this host");
}

#[cfg(not(target_os = "linux"))]
pub fn restore_vcpu(_: &crate::hv::Vcpu, _: &VcpuState) -> Result<()> {
	bail!("x86_64 snapshot restore is not supported on this host");
}

#[cfg(not(target_os = "linux"))]
pub fn save_machine() -> Result<MachineState> {
	bail!("x86_64 snapshots are not supported on this host");
}

// `x86_64` vCPU and machine snapshot state.
//
// Captures the full migratable KVM state of a paused vCPU and of the
// VM-global interrupt/timer devices, plus the inverse restore path. We
// serialize the dynamically sized `Xsave` wrapper so `KVM_XSAVE2` hosts keep
// the complete xstate image (legacy FPU/SSE, AVX, PKRU, and larger dynamic
// components such as AMX when present), paired with `kvm_xcrs`.

#[cfg(target_os = "linux")]
use std::{collections::BTreeSet, mem::size_of};

#[cfg(target_os = "linux")]
use kvm_bindings::{
	CpuId, KVM_MAX_CPUID_ENTRIES, Msrs, Xsave, kvm_clock_data, kvm_debugregs, kvm_irqchip,
	kvm_lapic_state, kvm_mp_state, kvm_msr_entry, kvm_pit_state2, kvm_regs, kvm_sregs,
	kvm_vcpu_events, kvm_xcrs, kvm_xsave, kvm_xsave2,
};
#[cfg(target_os = "linux")]
use kvm_ioctls::{Kvm, VmFd};
#[cfg(target_os = "linux")]
use vmm_sys_util::fam::FamStruct;

#[cfg(target_os = "linux")]
use crate::{
	bail,
	hv::Vcpu,
	result::{Result, err},
};

#[cfg(target_os = "linux")]
/// Complete migratable state of one x86 vCPU.
#[derive(Serialize, Deserialize, Clone)]
pub struct VcpuState {
	pub cpuid:       CpuId,
	/// Curated migration MSRs as `(index, data)` pairs. Only the MSRs KVM
	/// actually reported are stored (see [`save_vcpu`]); an MSR the running
	/// guest never enabled must not be blindly re-asserted on restore.
	pub msrs:        Vec<(u32, u64)>,
	pub regs:        kvm_regs,
	pub sregs:       kvm_sregs,
	/// Full extended state via XSAVE2 — covers AVX-512/AMX/PKRU on CPUs whose
	/// xstate exceeds the legacy 4096-byte `kvm_xsave` (e.g. Sapphire Rapids).
	/// `Xsave` is a dynamically-sized FAM wrapper around `kvm_xsave2`; the
	/// `with-serde` feature serializes it.
	pub xsave:       Xsave,
	pub xcrs:        kvm_xcrs,
	pub debug_regs:  kvm_debugregs,
	pub lapic:       kvm_lapic_state,
	pub mp_state:    kvm_mp_state,
	pub vcpu_events: kvm_vcpu_events,
}

/// VM-global x86 device state: the master/slave PICs, the IOAPIC, the PIT and
/// the KVM paravirtual clock.
#[cfg(target_os = "linux")]
#[derive(Serialize, Deserialize, Clone)]
pub struct MachineState {
	pub pic_master: kvm_irqchip,
	pub pic_slave:  kvm_irqchip,
	pub ioapic:     kvm_irqchip,
	pub pit:        kvm_pit_state2,
	pub clock:      kvm_clock_data,
}

/// Read the complete migratable state of a (paused) vCPU.
#[cfg(target_os = "linux")]
pub fn save_vcpu(vcpu: &Vcpu, xsave_size: usize) -> Result<VcpuState> {
	let vcpu = vcpu.fd();
	// KVM_GET_MP_STATE calls into the in-kernel APIC event acceptance path and
	// can modify vCPU/LAPIC state. Snapshot it before the register files so the
	// saved regs/sregs/LAPIC/events all describe the same architectural point.
	let mp_state = vcpu.get_mp_state()?;
	let regs = vcpu.get_regs()?;
	let sregs = vcpu.get_sregs()?;

	// Capture the full extended state. Hosts advertising KVM_CAP_XSAVE2 can have
	// an xstate larger than the legacy 4096-byte area (AVX-512/AMX/PKRU) — use
	// KVM_GET_XSAVE2 there; otherwise fall back to the legacy KVM_GET_XSAVE.
	let xsave = if xsave_size > size_of::<kvm_xsave>() {
		let extra = (xsave_size - size_of::<kvm_xsave>())
			.div_ceil(size_of::<<kvm_xsave2 as FamStruct>::Entry>());
		let mut x = Xsave::new(extra).map_err(|e| err(format!("alloc xsave2: {e:?}")))?;
		// SAFETY: `x` is sized to the host's reported XSAVE2 length.
		unsafe { vcpu.get_xsave2(&mut x)? };
		x
	} else {
		let legacy = vcpu.get_xsave()?;
		let mut x = Xsave::new(0).map_err(|e| err(format!("alloc xsave: {e:?}")))?;
		// SAFETY: the pointer is valid; we only overwrite the embedded fixed kvm_xsave.
		unsafe { (*x.as_mut_fam_struct_ptr()).xsave = legacy };
		x
	};

	let xcrs = vcpu.get_xcrs()?;
	let debug_regs = vcpu.get_debug_regs()?;
	let lapic = vcpu.get_lapic()?;
	let cpuid = vcpu.get_cpuid2(KVM_MAX_CPUID_ENTRIES)?;

	// Read only MSRs the host KVM advertises. A blind KVM_GET_MSRS over an
	// optional curated list stops at the first unsupported index, which can
	// silently drop later critical state (e.g. CET/XSS on newer Intel hosts).
	let supported: BTreeSet<u32> = Kvm::new()?
		.get_msr_index_list()?
		.as_slice()
		.iter()
		.copied()
		.collect();
	let probe: Vec<kvm_msr_entry> = super::msr::MIGRATION_MSRS
		.iter()
		.copied()
		.filter(|index| supported.contains(index))
		.map(|index| kvm_msr_entry { index, data: 0, ..Default::default() })
		.collect();
	let mut msrs = Msrs::from_entries(&probe)?;
	let n = vcpu.get_msrs(&mut msrs)?;
	let msrs: Vec<(u32, u64)> = msrs.as_slice()[..n]
		.iter()
		.map(|e| (e.index, e.data))
		.collect();

	// KVM_GET_VCPU_EVENTS is order-sensitive: keep it last because earlier KVM
	// GET ioctls may accept pending APIC events or otherwise adjust event state.
	let vcpu_events = vcpu.get_vcpu_events()?;

	Ok(VcpuState { cpuid, msrs, regs, sregs, xsave, xcrs, debug_regs, lapic, mp_state, vcpu_events })
}

/// Restore a vCPU from saved state. Order follows the x86 KVM migration
/// dependencies used by QEMU/Firecracker: restore register files before
/// events, LAPIC before TSC-deadline MSRs, and events last because `SET_REGS`
/// clears pending exceptions.
#[cfg(target_os = "linux")]
pub fn restore_vcpu(vcpu: &Vcpu, st: &VcpuState) -> Result<()> {
	let vcpu = vcpu.fd();
	vcpu.set_cpuid2(&st.cpuid)?;
	vcpu.set_mp_state(st.mp_state)?;
	vcpu.set_regs(&st.regs)?;
	vcpu.set_sregs(&st.sregs)?;

	// Restore the extended state before XCR0. This is the ordering used by KVM
	// migration users: KVM_SET_XSAVE copies the raw xstate image, then
	// KVM_SET_XCRS restores which components the guest has enabled.
	if st.xsave.as_slice().is_empty() {
		// SAFETY: `st.xsave` was produced by get_xsave2/get_xsave on a vCPU of this
		// same host, so its buffer matches this host's legacy XSAVE layout.
		unsafe { vcpu.set_xsave(&st.xsave.as_fam_struct_ref().xsave)? };
	} else {
		// SAFETY: `st.xsave` was produced by get_xsave2/get_xsave on a vCPU of this
		// same host, so its buffer matches this host's XSAVE2 layout.
		unsafe { vcpu.set_xsave2(&st.xsave)? };
	}
	vcpu.set_xcrs(&st.xcrs)?;
	vcpu.set_debug_regs(&st.debug_regs)?;
	vcpu.set_lapic(&st.lapic)?;

	let entries: Vec<kvm_msr_entry> = st
		.msrs
		.iter()
		.map(|&(index, data)| kvm_msr_entry { index, data, ..Default::default() })
		.collect();
	let msrs = Msrs::from_entries(&entries)?;
	let n = vcpu.set_msrs(&msrs)?;
	if n < st.msrs.len() {
		bail!("set_msrs wrote {n} of {} entries", st.msrs.len());
	}

	vcpu.set_vcpu_events(&st.vcpu_events)?;
	Ok(())
}

/// Read the VM-global interrupt-controller, PIT and clock state.
#[cfg(target_os = "linux")]
pub fn save_machine(vm: &VmFd) -> Result<MachineState> {
	let mut pic_master =
		kvm_irqchip { chip_id: kvm_bindings::KVM_IRQCHIP_PIC_MASTER, ..Default::default() };
	vm.get_irqchip(&mut pic_master)?;
	let mut pic_slave =
		kvm_irqchip { chip_id: kvm_bindings::KVM_IRQCHIP_PIC_SLAVE, ..Default::default() };
	vm.get_irqchip(&mut pic_slave)?;
	let mut ioapic = kvm_irqchip { chip_id: kvm_bindings::KVM_IRQCHIP_IOAPIC, ..Default::default() };
	vm.get_irqchip(&mut ioapic)?;
	let pit = vm.get_pit2()?;
	let clock = vm.get_clock()?;
	Ok(MachineState { pic_master, pic_slave, ioapic, pit, clock })
}

/// Restore the VM-global interrupt-controller, PIT and clock state.
#[cfg(target_os = "linux")]
pub fn restore_machine(vm: &VmFd, st: &MachineState) -> Result<()> {
	vm.set_irqchip(&st.pic_master)?;
	vm.set_irqchip(&st.pic_slave)?;
	vm.set_irqchip(&st.ioapic)?;
	vm.set_pit2(&st.pit)?;
	vm.set_clock(&st.clock)?;
	Ok(())
}
