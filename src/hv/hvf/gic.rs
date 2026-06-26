//! HVF in-kernel `GICv3` wrapper and interrupt lines.

use std::{io, sync::Arc};

use applevisor::prelude::{GicEnabled, VirtualMachineInstance};

use crate::{
	layout::{GIC_DIST_BASE, GIC_DIST_SIZE, GIC_REDIST_BASE, GIC_REDIST_SIZE_PER_CPU},
	result::{Result, err},
};

const GIC_MAINT_IRQ: u32 = 9;
const FIRST_SPI_INTID: u32 = 32;

/// Shared HVF GIC state kept alive by the VM and IRQ lines.
pub struct GicInner {
	pub(crate) vm: VirtualMachineInstance<GicEnabled>,
	redist_size:   u64,
}

/// A configured in-kernel HVF `GICv3` handle.
#[derive(Clone)]
pub struct Gic {
	pub(crate) inner: Arc<GicInner>,
}

/// A triggerable HVF GIC SPI line.
#[derive(Clone)]
pub struct IrqLine {
	gic: Arc<GicInner>,
	spi: u32,
}

impl Gic {
	/// Wrap the GIC already created with the HVF VM.
	pub(crate) fn new(vm: VirtualMachineInstance<GicEnabled>, num_cpus: u64) -> Self {
		Self { inner: Arc::new(GicInner { vm, redist_size: GIC_REDIST_SIZE_PER_CPU * num_cpus }) }
	}

	/// Create an interrupt line bound to a guest SPI number.
	pub(crate) fn irq_line(&self, spi: u32) -> Result<IrqLine> {
		if spi >= crate::layout::GIC_SPI_COUNT {
			return Err(err(format!("SPI {spi} exceeds GIC SPI count")));
		}
		Ok(IrqLine { gic: self.inner.clone(), spi })
	}

	/// FDT `reg` property: distributor then redistributor (addr/size pairs).
	pub fn fdt_reg(&self) -> [u64; 4] {
		[GIC_DIST_BASE, GIC_DIST_SIZE, GIC_REDIST_BASE, self.inner.redist_size]
	}

	/// FDT compatible string for the in-kernel interrupt controller.
	#[expect(
		clippy::unused_self,
		reason = "HVF and KVM GIC backends intentionally expose the same instance-shaped FDT API"
	)]
	pub const fn fdt_compatible(&self) -> &'static str {
		"arm,gic-v3"
	}

	/// `GICv3` maintenance interrupt PPI number.
	#[expect(
		clippy::unused_self,
		reason = "HVF and KVM GIC backends intentionally expose the same instance-shaped FDT API"
	)]
	pub const fn fdt_maint_irq(&self) -> u32 {
		GIC_MAINT_IRQ
	}

	/// Save the opaque HVF GIC device blob.
	pub fn save_state(&self, _num_cpus: u64) -> Result<crate::arch::state::GicState> {
		let mut state = self.inner.vm.gic_state_create()?;
		let mut blob = vec![0u8; state.size()?];
		state.get(&mut blob)?;
		Ok(crate::arch::state::GicState::Hvf { blob })
	}

	/// Restore the opaque HVF GIC device blob before any vCPU is run.
	pub fn restore_state(&self, st: &crate::arch::state::GicState) -> Result<()> {
		match st {
			crate::arch::state::GicState::Hvf { blob } => {
				let state = self.inner.vm.gic_state_create()?;
				state.set(blob)?;
				Ok(())
			},
			_ => Err(err("snapshot GIC state was not captured on HVF")),
		}
	}
}

impl IrqLine {
	/// Pulse the SPI through the HVF in-kernel GIC.
	pub fn trigger(&self) -> Result<()> {
		let intid = FIRST_SPI_INTID + self.spi;
		self.gic.vm.gic_set_spi(intid, true)?;
		self.gic.vm.gic_set_spi(intid, false)?;
		Ok(())
	}
}

impl vm_superio::Trigger for IrqLine {
	type E = io::Error;

	fn trigger(&self) -> io::Result<()> {
		Self::trigger(self).map_err(io::Error::other)
	}
}
