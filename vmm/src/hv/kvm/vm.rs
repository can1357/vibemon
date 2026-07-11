//! KVM VM creation and guest memory registration.
//!
//! The x86 path also installs the in-kernel irqchip/PIT here; on aarch64 the
//! GIC is created later (after vCPUs exist), so `new` just sets up memory.

use std::sync::Arc;

use kvm_bindings::kvm_userspace_memory_region;
#[cfg(target_arch = "x86_64")]
use kvm_bindings::{CpuId, KVM_MAX_CPUID_ENTRIES, KVM_PIT_SPEAKER_DUMMY, kvm_pit_config};
use kvm_ioctls::{IoEventAddress, Kvm, NoDatamatch, VmFd};
use vm_memory::{Address, GuestMemory, GuestMemoryRegion};

#[cfg(target_arch = "aarch64")]
use super::gic::Gic;
use super::{irq::IrqLine, vcpu::Vcpu};
use crate::{
	memory::{GuestMemoryMmap, create_guest_memory},
	os::EventFd,
	result::Result,
};

#[cfg(target_arch = "x86_64")]
const KVM_TSS_ADDRESS: usize = 0xfffb_d000;
#[cfg(target_arch = "x86_64")]
const KVM_IDENTITY_MAP_ADDRESS: u64 = 0xfffb_c000;

/// Owns the KVM handles and guest RAM for a single virtual machine.
pub struct Vm {
	/// KVM system fd kept alive for the VM lifetime.
	#[cfg_attr(
		target_arch = "aarch64",
		allow(dead_code, reason = "keeps the KVM fd alive on every backend")
	)]
	pub kvm:         Kvm,
	/// KVM VM fd shared by backend-owned vCPUs and arch setup code.
	pub fd:          Arc<VmFd>,
	memory:          GuestMemoryMmap,
	/// Host-supported CPUID, queried once and handed to every vCPU.
	#[cfg(target_arch = "x86_64")]
	supported_cpuid: CpuId,
}

impl Vm {
	/// Create a VM with freshly-allocated guest RAM of `mem_size` bytes.
	pub fn new(mem_size: usize) -> Result<Self> {
		Self::with_memory(create_guest_memory(mem_size)?)
	}

	/// Create a VM around already-built guest memory.
	pub fn with_memory(memory: GuestMemoryMmap) -> Result<Self> {
		let kvm = Kvm::new()?;
		let fd = kvm.create_vm()?;
		Self::register_memory(&fd, &memory)?;
		let fd = Arc::new(fd);

		#[cfg(target_arch = "x86_64")]
		{
			fd.set_identity_map_address(KVM_IDENTITY_MAP_ADDRESS)?;
			fd.set_tss_address(KVM_TSS_ADDRESS)?;
			fd.create_irq_chip()?;
			fd.create_pit2(kvm_pit_config { flags: KVM_PIT_SPEAKER_DUMMY, ..Default::default() })?;
			let supported_cpuid = kvm.get_supported_cpuid(KVM_MAX_CPUID_ENTRIES)?;
			// Capture the supported-MSR list now, while /dev/kvm is open and the
			// sandbox filters have not engaged; the snapshot path reuses it.
			crate::arch::state::cache_supported_msrs(&kvm)?;
			Ok(Self { kvm, fd, memory, supported_cpuid })
		}

		#[cfg(target_arch = "aarch64")]
		{
			Ok(Self { kvm, fd, memory })
		}
	}

	/// Return the guest memory owned by this VM.
	pub const fn memory(&self) -> &GuestMemoryMmap {
		&self.memory
	}

	/// Hand every guest-RAM region to KVM as a userspace memory slot.
	fn register_memory(fd: &VmFd, memory: &GuestMemoryMmap) -> Result<()> {
		for (slot, region) in memory.iter().enumerate() {
			let slot = u32::try_from(slot)?;
			let region = kvm_userspace_memory_region {
				slot,
				guest_phys_addr: region.start_addr().raw_value(),
				memory_size: region.len(),
				userspace_addr: region.as_ptr() as u64,
				flags: 0,
			};
			// SAFETY: the userspace_addr points at an mmap owned by `memory`
			// that outlives the VM, and the regions are non-overlapping.
			unsafe { fd.set_user_memory_region(region)? };
		}
		Ok(())
	}

	/// Create a backend-owned vCPU with the given index.
	pub fn create_vcpu(&self, id: u8) -> Result<Vcpu> {
		Ok(Vcpu::new(self.fd.clone(), self.fd.create_vcpu(u64::from(id))?))
	}

	/// Create an irqfd-backed trigger for guest interrupt line `gsi`.
	pub fn irq_line(&self, gsi: u32) -> Result<IrqLine> {
		let evt = EventFd::new(libc::EFD_NONBLOCK)?;
		self.fd.register_irqfd(&evt, gsi)?;
		Ok(IrqLine::new(evt))
	}

	/// Wake `evt` when the guest writes an optional `datamatch` at MMIO `addr`.
	pub fn register_ioevent(&self, evt: &EventFd, addr: u64, datamatch: Option<u64>) -> Result<()> {
		let ioevent_addr = IoEventAddress::Mmio(addr);
		match datamatch {
			Some(value) => self.fd.register_ioevent(evt, &ioevent_addr, value)?,
			None => self.fd.register_ioevent(evt, &ioevent_addr, NoDatamatch)?,
		}
		Ok(())
	}

	/// Create the in-kernel `GICv3` after all `vCPUs` exist.
	#[cfg(target_arch = "aarch64")]
	pub fn create_gic(&self, num_cpus: u64) -> Result<Gic> {
		Gic::new(&self.fd, num_cpus)
	}

	/// Return the host-supported CPUID template for x86 vCPU setup.
	#[cfg(target_arch = "x86_64")]
	pub const fn supported_cpuid(&self) -> &CpuId {
		&self.supported_cpuid
	}
}
