//! aarch64 kernel/initrd loading and FDT placement.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use vm_memory::{Address, Bytes, GuestAddress, GuestMemory, GuestMemoryRegion};

use crate::arch::aarch64::fdt::{self, FdtDevice, FdtParams};
use crate::bail;
use crate::hv::Gic;
use crate::layout::{DRAM_BASE, FDT_MAX_SIZE, KERNEL_OFFSET};
use crate::memory::GuestMemoryMmap;
use crate::result::Result;

const ARM64_IMAGE_MAGIC: u32 = 0x644d_5241; // "ARM\x64"
const ARM64_IMAGE_MAGIC_OFFSET: usize = 56;
const GUEST_PAGE_SIZE: u64 = 4096;

/// A loaded guest kernel.
pub struct LoadedKernel {
    pub entry: GuestAddress,
    /// Exclusive end of the bytes loaded for the arm64 Image.
    pub end: GuestAddress,
}

/// Load an uncompressed arm64 `Image` into guest memory.
pub fn load_kernel(path: &Path, mem: &GuestMemoryMmap) -> Result<LoadedKernel> {
    let path_display = path.display();
    let mut file = File::open(path).map_err(|e| format!("opening kernel {path_display}: {e}"))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    if buf.len() >= 2 && buf[0] == 0x1f && buf[1] == 0x8b {
        bail!("kernel {path_display} is gzip-compressed; pass an uncompressed arm64 Image (gunzip it)");
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

    let entry = GuestAddress(DRAM_BASE + KERNEL_OFFSET);
    let Some(end) = entry.checked_add(buf.len() as u64) else {
        bail!("kernel {path_display} end address overflows");
    };
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
    let fdt_addr = get_fdt_addr(mem);
    if end.raw_value() > fdt_addr {
        bail!(
            "firmware {path_display} ({} bytes) overlaps FDT area at {fdt_addr:#x}",
            buf.len()
        );
    }
    mem.write_slice(&buf, entry)?;
    Ok(LoadedKernel { entry, end })
}

/// Guest-physical address where the FDT is placed (top of the first RAM region).
pub fn get_fdt_addr(mem: &GuestMemoryMmap) -> u64 {
    let region = mem.find_region(GuestAddress(DRAM_BASE)).unwrap();
    let end = region.start_addr().raw_value() + region.len();
    (end - FDT_MAX_SIZE) & !(0x20_0000 - 1)
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

    let fdt_addr = get_fdt_addr(mem);
    let Some(unrounded_addr) = fdt_addr.checked_sub(size_u64) else {
        bail!("initrd ({size} bytes) does not fit below FDT");
    };
    let addr = unrounded_addr & !(GUEST_PAGE_SIZE - 1);
    let initrd_end = addr + size_u64;
    let kernel_entry = kernel.entry.raw_value();
    let kernel_end = kernel.end.raw_value();
    if addr < kernel_end {
        bail!(
            "initrd ({size} bytes at {addr:#x}-{initrd_end:#x}) does not fit between kernel ({kernel_entry:#x}-{kernel_end:#x}) and FDT"
        );
    }
    let addr = GuestAddress(addr);
    mem.write_slice(&buf, addr)?;
    Ok((addr, size))
}

/// Build and write the FDT; returns its guest address (passed to the vCPU in X0).
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
    let region = mem.find_region(GuestAddress(DRAM_BASE)).unwrap();
    let params = FdtParams {
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

    let fdt_addr = get_fdt_addr(mem);
    if blob.len() as u64 > FDT_MAX_SIZE {
        bail!("device tree ({} bytes) exceeds FDT_MAX_SIZE", blob.len());
    }
    mem.write_slice(&blob, GuestAddress(fdt_addr))?;
    Ok(fdt_addr)
}
