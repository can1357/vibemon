//! Flattened Device Tree generation for the aarch64 guest.
//!
//! Without firmware, the FDT is how the guest kernel discovers RAM, CPUs, the
//! GIC, the architected timer, PSCI, and our MMIO devices (serial + virtio).
//! Structure follows Firecracker's aarch64 FDT (Apache-2.0), trimmed to the
//! nodes a barebones VMM needs.

use vm_fdt::{FdtReserveEntry, FdtWriter};

use crate::{bail, result::Result};

const GIC_PHANDLE: u32 = 1;
const CLOCK_PHANDLE: u32 = 2;

// Interrupt specifier encodings (dt-bindings/interrupt-controller).
const IRQ_TYPE_SPI: u32 = 0;
const IRQ_TYPE_PPI: u32 = 1;
const IRQ_TYPE_EDGE_RISING: u32 = 1;
const IRQ_TYPE_LEVEL_HI: u32 = 4;

/// One MMIO device to describe in the FDT.
pub struct FdtDevice {
	pub addr: u64,
	pub len:  u64,
	pub gsi:  u32,
}

/// Inputs for building the device tree.
pub struct FdtParams<'a> {
	pub fdt_addr:       u64,
	pub fdt_size:       u64,
	pub mem_start:      u64,
	pub mem_size:       u64,
	pub mpidrs:         &'a [u64],
	pub cmdline:        &'a str,
	pub initrd:         Option<(u64, usize)>,
	pub gic_reg:        [u64; 4],
	pub gic_compatible: &'a str,
	pub gic_maint_irq:  u32,
	pub serial:         &'a FdtDevice,
	pub virtio:         &'a [FdtDevice],
}

/// Build the flattened device tree blob.
pub fn build_fdt(p: &FdtParams) -> Result<Vec<u8>> {
	if p.mpidrs.is_empty() {
		bail!("FDT requires at least one CPU");
	}
	let mut reservations = Vec::new();
	if p.fdt_size != 0 {
		reservations.push(FdtReserveEntry::new(p.fdt_addr, p.fdt_size)?);
	}
	if let Some((addr, size)) = p.initrd
		&& size != 0
	{
		reservations.push(FdtReserveEntry::new(addr, size as u64)?);
	}
	let mut fdt = FdtWriter::new_with_mem_reserv(&reservations)?;

	let root = fdt.begin_node("")?;
	fdt.property_string("compatible", "linux,dummy-virt")?;
	fdt.property_u32("#address-cells", 2)?;
	fdt.property_u32("#size-cells", 2)?;
	fdt.property_u32("interrupt-parent", GIC_PHANDLE)?;

	// CPUs.
	let cpus = fdt.begin_node("cpus")?;
	fdt.property_u32("#address-cells", 2)?;
	fdt.property_u32("#size-cells", 0)?;
	for &mpidr in p.mpidrs {
		let reg = mpidr & 0x7f_ffff;
		let cpu = fdt.begin_node(&format!("cpu@{reg:x}"))?;
		fdt.property_string("device_type", "cpu")?;
		fdt.property_string("compatible", "arm,arm-v8")?;
		fdt.property_string("enable-method", "psci")?;
		fdt.property_u64("reg", reg)?;
		fdt.end_node(cpu)?;
	}
	fdt.end_node(cpus)?;
	let mem = fdt.begin_node("memory@ram")?;
	fdt.property_string("device_type", "memory")?;
	fdt.property_array_u64("reg", &[p.mem_start, p.mem_size])?;
	fdt.end_node(mem)?;

	// Chosen (command line + initrd).
	let chosen = fdt.begin_node("chosen")?;
	fdt.property_string("bootargs", p.cmdline)?;
	if let Some((addr, size)) = p.initrd {
		let Some(end) = addr.checked_add(size as u64) else {
			bail!("initrd end address overflows in FDT");
		};
		fdt.property_u64("linux,initrd-start", addr)?;
		fdt.property_u64("linux,initrd-end", end)?;
	}
	fdt.end_node(chosen)?;

	// GIC.
	let intc = fdt.begin_node("intc")?;
	fdt.property_string("compatible", p.gic_compatible)?;
	fdt.property_null("interrupt-controller")?;
	fdt.property_u32("#interrupt-cells", 3)?;
	fdt.property_array_u64("reg", &p.gic_reg)?;
	fdt.property_u32("phandle", GIC_PHANDLE)?;
	fdt.property_u32("#address-cells", 2)?;
	fdt.property_u32("#size-cells", 2)?;
	fdt.property_null("ranges")?;
	fdt.property_array_u32("interrupts", &[IRQ_TYPE_PPI, p.gic_maint_irq, IRQ_TYPE_LEVEL_HI])?;
	fdt.end_node(intc)?;

	// Architected timer (fixed PPIs 13,14,11,10).
	let timer = fdt.begin_node("timer")?;
	fdt.property_string("compatible", "arm,armv8-timer")?;
	fdt.property_null("always-on")?;
	let mut timer_irqs = Vec::new();
	for irq in [13u32, 14, 11, 10] {
		timer_irqs.extend_from_slice(&[IRQ_TYPE_PPI, irq, IRQ_TYPE_LEVEL_HI]);
	}
	fdt.property_array_u32("interrupts", &timer_irqs)?;
	fdt.end_node(timer)?;

	// Reference clock for the UART.
	let clock = fdt.begin_node("apb-pclk")?;
	fdt.property_string("compatible", "fixed-clock")?;
	fdt.property_u32("#clock-cells", 0)?;
	fdt.property_u32("clock-frequency", 24_000_000)?;
	fdt.property_string("clock-output-names", "clk24mhz")?;
	fdt.property_u32("phandle", CLOCK_PHANDLE)?;
	fdt.end_node(clock)?;

	// PSCI (for SMP bring-up + power-off).
	let psci = fdt.begin_node("psci")?;
	fdt.property_string_list("compatible", vec!["arm,psci-1.0".into(), "arm,psci-0.2".into()])?;
	fdt.property_string("method", "hvc")?;
	fdt.end_node(psci)?;

	// 16550 serial.
	let uart = fdt.begin_node(&format!("uart@{:x}", p.serial.addr))?;
	fdt.property_string("compatible", "ns16550a")?;
	fdt.property_array_u64("reg", &[p.serial.addr, p.serial.len])?;
	fdt.property_u32("clocks", CLOCK_PHANDLE)?;
	fdt.property_string("clock-names", "apb_pclk")?;
	fdt.property_array_u32("interrupts", &[IRQ_TYPE_SPI, p.serial.gsi, IRQ_TYPE_EDGE_RISING])?;
	fdt.end_node(uart)?;

	// virtio-mmio devices.
	for dev in p.virtio {
		let node = fdt.begin_node(&format!("virtio_mmio@{:x}", dev.addr))?;
		fdt.property_null("dma-coherent")?;
		fdt.property_string("compatible", "virtio,mmio")?;
		fdt.property_array_u64("reg", &[dev.addr, dev.len])?;
		fdt.property_array_u32("interrupts", &[IRQ_TYPE_SPI, dev.gsi, IRQ_TYPE_EDGE_RISING])?;
		fdt.property_u32("interrupt-parent", GIC_PHANDLE)?;
		fdt.end_node(node)?;
	}

	fdt.end_node(root)?;
	Ok(fdt.finish()?)
}
