//! KVM in-kernel GICv3 interrupt controller setup.

use kvm_bindings::{
	KVM_DEV_ARM_VGIC_CTRL_INIT, KVM_DEV_ARM_VGIC_GRP_ADDR, KVM_DEV_ARM_VGIC_GRP_CTRL,
	KVM_DEV_ARM_VGIC_GRP_NR_IRQS, KVM_VGIC_V3_ADDR_TYPE_DIST, KVM_VGIC_V3_ADDR_TYPE_REDIST,
	kvm_create_device, kvm_device_attr, kvm_device_type_KVM_DEV_TYPE_ARM_VGIC_V3,
};
use kvm_ioctls::{DeviceFd, VmFd};

use crate::{
	layout::{
		GIC_DIST_BASE, GIC_DIST_SIZE, GIC_REDIST_BASE, GIC_REDIST_SIZE_PER_CPU, GIC_SPI_COUNT,
	},
	result::{Result, err},
};

/// Number of interrupt IDs the GIC supports (SGIs + PPIs + SPIs, multiple of
/// 32).
const GIC_NR_IRQS: u32 = 32 + GIC_SPI_COUNT;
/// GICv3 maintenance interrupt (PPI 9).
const GIC_MAINT_IRQ: u32 = 9;

/// A configured in-kernel GICv3 kept alive by its device fd.
pub struct Gic {
	device:      DeviceFd,
	redist_size: u64,
}

impl Gic {
	/// Create and initialize the GICv3 after all vCPUs exist.
	pub fn new(vm: &VmFd, num_cpus: u64) -> Result<Gic> {
		let mut create =
			kvm_create_device { type_: kvm_device_type_KVM_DEV_TYPE_ARM_VGIC_V3, fd: 0, flags: 0 };
		let device = vm.create_device(&mut create)?;

		set_u64_attr(
			&device,
			KVM_DEV_ARM_VGIC_GRP_ADDR,
			u64::from(KVM_VGIC_V3_ADDR_TYPE_DIST),
			GIC_DIST_BASE,
		)?;
		set_u64_attr(
			&device,
			KVM_DEV_ARM_VGIC_GRP_ADDR,
			u64::from(KVM_VGIC_V3_ADDR_TYPE_REDIST),
			GIC_REDIST_BASE,
		)?;

		let nr_irqs = GIC_NR_IRQS;
		let attr = kvm_device_attr {
			group: KVM_DEV_ARM_VGIC_GRP_NR_IRQS,
			attr:  0,
			addr:  std::ptr::addr_of!(nr_irqs) as u64,
			flags: 0,
		};
		device.set_device_attr(&attr)?;

		let init = kvm_device_attr {
			group: KVM_DEV_ARM_VGIC_GRP_CTRL,
			attr:  u64::from(KVM_DEV_ARM_VGIC_CTRL_INIT),
			addr:  0,
			flags: 0,
		};
		device.set_device_attr(&init)?;

		let redist_size = GIC_REDIST_SIZE_PER_CPU
			.checked_mul(num_cpus)
			.ok_or_else(|| err("GIC redistributor size overflow"))?;
		Ok(Gic { device, redist_size })
	}

	/// Return FDT `reg` pairs: distributor then redistributor.
	pub fn fdt_reg(&self) -> [u64; 4] {
		[GIC_DIST_BASE, GIC_DIST_SIZE, GIC_REDIST_BASE, self.redist_size]
	}

	/// Return the GIC FDT compatible string.
	pub fn fdt_compatible(&self) -> &'static str {
		"arm,gic-v3"
	}

	/// Return the GIC maintenance interrupt PPI number.
	pub fn fdt_maint_irq(&self) -> u32 {
		GIC_MAINT_IRQ
	}

	/// Return the GIC device fd for KVM-specific callers.
	pub fn device(&self) -> &DeviceFd {
		&self.device
	}

	/// Save KVM GICv3 register state into the arch serialized form.
	pub fn save_state(&self, num_cpus: u64) -> Result<crate::arch::state::GicState> {
		super::state::save_gic(&self.device, num_cpus, GIC_NR_IRQS)
	}

	/// Restore KVM GICv3 register state from the arch serialized form.
	pub fn restore_state(&self, st: &crate::arch::state::GicState) -> Result<()> {
		super::state::restore_gic(&self.device, st)
	}
}

fn set_u64_attr(device: &DeviceFd, group: u32, attr: u64, value: u64) -> Result<()> {
	let dev_attr = kvm_device_attr { group, attr, addr: std::ptr::addr_of!(value) as u64, flags: 0 };
	device.set_device_attr(&dev_attr)?;
	Ok(())
}
