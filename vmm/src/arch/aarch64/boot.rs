//! aarch64 kernel/initrd loading and FDT placement.

use std::{fs::File, io::Read, path::Path};

use vm_memory::{Address, Bytes, GuestAddress, GuestMemory, GuestMemoryRegion};

use crate::{
	arch::aarch64::fdt::{self, FdtDevice, FdtParams},
	bail,
	hv::Gic,
	layout::{DRAM_BASE, FDT_MAX_SIZE, KERNEL_OFFSET},
	memory::GuestMemoryMmap,
	result::Result,
};

const ARM64_IMAGE_MAGIC: u32 = 0x644d_5241; // "ARM\x64"
const ARM64_IMAGE_SIZE_OFFSET: usize = 16;
const ARM64_IMAGE_MAGIC_OFFSET: usize = 56;
const LEGACY_ARM64_IMAGE_SIZE: u64 = 16 << 20;
const GUEST_PAGE_SIZE: u64 = 4096;
const FDT_ALIGN: u64 = 0x20_0000;

/// A loaded guest kernel.
pub struct LoadedKernel {
	pub entry: GuestAddress,
	/// Exclusive end of the arm64 Image's reserved in-memory footprint.
	pub end:   GuestAddress,
}

/// Load an uncompressed arm64 `Image` into guest memory.
pub fn load_kernel(path: &Path, mem: &GuestMemoryMmap) -> Result<LoadedKernel> {
	let path_display = path.display();
	let mut file = File::open(path).map_err(|e| format!("opening kernel {path_display}: {e}"))?;
	let mut buf = Vec::new();
	file.read_to_end(&mut buf)?;

	if buf.len() >= 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		bail!(
			"kernel {path_display} is gzip-compressed; pass an uncompressed arm64 Image (gunzip it)"
		);
	}
	if buf.len() < ARM64_IMAGE_MAGIC_OFFSET + 4 {
		bail!("kernel {path_display} is too small to be an arm64 Image");
	}
	let magic = u32::from_le_bytes(
		buf[ARM64_IMAGE_MAGIC_OFFSET..ARM64_IMAGE_MAGIC_OFFSET + 4]
			.try_into()
			.unwrap(),
	);
	if magic != ARM64_IMAGE_MAGIC {
		bail!("kernel {path_display} is not an arm64 Image (bad magic {magic:#x})");
	}

	let header_image_size = u64::from_le_bytes(
		buf[ARM64_IMAGE_SIZE_OFFSET..ARM64_IMAGE_SIZE_OFFSET + 8]
			.try_into()
			.unwrap(),
	);
	let image_size = if header_image_size == 0 {
		LEGACY_ARM64_IMAGE_SIZE
	} else {
		header_image_size
	}
	.max(buf.len() as u64);
	let entry = GuestAddress(DRAM_BASE + KERNEL_OFFSET);
	let Some(end) = entry.checked_add(image_size) else {
		bail!("kernel {path_display} reserved end address overflows");
	};
	let fdt_addr = checked_fdt_addr(mem)?;
	if end.raw_value() > fdt_addr {
		bail!(
			"kernel {path_display} ({} bytes, reserves {image_size} bytes) overlaps FDT area at \
			 {fdt_addr:#x}",
			buf.len()
		);
	}
	mem.write_slice(&buf, entry)?;
	Ok(LoadedKernel { entry, end })
}

/// Load an aarch64 UEFI firmware image (for example `QEMU_EFI.fd`) at the start
/// of guest DRAM. The existing FDT remains the hardware description and is
/// passed to firmware in X0 by the normal aarch64 vCPU setup.
pub fn load_uefi_firmware(path: &Path, mem: &GuestMemoryMmap) -> Result<LoadedKernel> {
	let path_display = path.display();
	let mut file = File::open(path).map_err(|e| format!("opening firmware {path_display}: {e}"))?;
	let mut buf = Vec::new();
	file.read_to_end(&mut buf)?;
	if buf.is_empty() {
		bail!("firmware {path_display} is empty");
	}

	let entry = GuestAddress(DRAM_BASE);
	let Some(end) = entry.checked_add(buf.len() as u64) else {
		bail!("firmware {path_display} end address overflows");
	};
	let fdt_addr = checked_fdt_addr(mem)?;
	if end.raw_value() > fdt_addr {
		bail!("firmware {path_display} ({} bytes) overlaps FDT area at {fdt_addr:#x}", buf.len());
	}
	mem.write_slice(&buf, entry)?;
	Ok(LoadedKernel { entry, end })
}

fn checked_fdt_addr(mem: &GuestMemoryMmap) -> Result<u64> {
	let Some(region) = mem.find_region(GuestAddress(DRAM_BASE)) else {
		bail!("guest RAM does not contain DRAM base {DRAM_BASE:#x}");
	};
	let region_start = region.start_addr().raw_value();
	let Some(region_end) = region_start.checked_add(region.len()) else {
		bail!("guest RAM region {region_start:#x}+{:#x} overflows", region.len());
	};
	let Some(fdt_floor) = region_end.checked_sub(FDT_MAX_SIZE) else {
		bail!("guest RAM region is smaller than the reserved FDT area ({FDT_MAX_SIZE:#x} bytes)");
	};
	let fdt_addr = fdt_floor & !(FDT_ALIGN - 1);
	if fdt_addr < region_start {
		bail!(
			"guest RAM region {region_start:#x}-{region_end:#x} is too small for an aligned FDT \
			 reservation"
		);
	}
	Ok(fdt_addr)
}

/// Load an initrd into RAM just below the FDT.
pub fn load_initrd(
	path: &Path,
	mem: &GuestMemoryMmap,
	kernel: &LoadedKernel,
) -> Result<(GuestAddress, usize)> {
	let path_display = path.display();
	let mut file = File::open(path).map_err(|e| format!("opening initrd {path_display}: {e}"))?;
	let mut buf = Vec::new();
	file.read_to_end(&mut buf)?;
	let size = buf.len();
	let size_u64 = size as u64;

	let fdt_addr = checked_fdt_addr(mem)?;
	let Some(unrounded_addr) = fdt_addr.checked_sub(size_u64) else {
		bail!("initrd ({size} bytes) does not fit below FDT");
	};
	let addr = unrounded_addr & !(GUEST_PAGE_SIZE - 1);
	let Some(initrd_end) = addr.checked_add(size_u64) else {
		bail!("initrd ({size} bytes at {addr:#x}) end address overflows");
	};
	let kernel_entry = kernel.entry.raw_value();
	let kernel_end = kernel.end.raw_value();
	if addr < kernel_end {
		bail!(
			"initrd ({size} bytes at {addr:#x}-{initrd_end:#x}) does not fit between kernel \
			 ({kernel_entry:#x}-{kernel_end:#x}) and FDT"
		);
	}
	let addr = GuestAddress(addr);
	mem.write_slice(&buf, addr)?;
	Ok((addr, size))
}

/// Build and write the FDT; returns its guest address (passed to the vCPU in
/// X0).
#[allow(clippy::too_many_arguments, reason = "necessary system parameters for FDT config")]
pub fn configure_system(
	mem: &GuestMemoryMmap,
	cmdline: &str,
	initrd: Option<(GuestAddress, usize)>,
	gic: &Gic,
	mpidrs: &[u64],
	serial: &FdtDevice,
	virtio: &[FdtDevice],
) -> Result<u64> {
	let Some(region) = mem.find_region(GuestAddress(DRAM_BASE)) else {
		bail!("guest RAM does not contain DRAM base {DRAM_BASE:#x}");
	};
	let fdt_addr = checked_fdt_addr(mem)?;
	let params = FdtParams {
		fdt_addr,
		fdt_size: FDT_MAX_SIZE,
		mem_start: region.start_addr().raw_value(),
		mem_size: region.len(),
		mpidrs,
		cmdline,
		initrd: initrd.map(|(a, s)| (a.raw_value(), s)),
		gic_reg: gic.fdt_reg(),
		gic_compatible: gic.fdt_compatible(),
		gic_maint_irq: gic.fdt_maint_irq(),
		serial,
		virtio,
	};
	let blob = fdt::build_fdt(&params)?;

	if blob.len() as u64 > FDT_MAX_SIZE {
		bail!("device tree ({} bytes) exceeds FDT_MAX_SIZE", blob.len());
	}
	mem.write_slice(&blob, GuestAddress(fdt_addr))?;
	Ok(fdt_addr)
}
