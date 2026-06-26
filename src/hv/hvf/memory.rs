//! Guest-memory registration helpers for HVF.

use core::ffi::c_void;

use applevisor_sys::{HV_MEMORY_EXEC, HV_MEMORY_READ, HV_MEMORY_WRITE, hv_error_t, hv_vm_map};
use vm_memory::{Address, GuestMemory, GuestMemoryRegion};

use crate::memory::GuestMemoryMmap;
use crate::result::{Result, err};

const HVF_PAGE_SIZE: u64 = applevisor_sys::PAGE_SIZE as u64;

/// Map every vm-memory region into the current HVF VM with RWX permissions.
pub fn map_guest_memory(memory: &GuestMemoryMmap) -> Result<()> {
    for region in memory.iter() {
        let gpa = region.start_addr().raw_value();
        let len = region.len();
        if gpa % HVF_PAGE_SIZE != 0 || len % HVF_PAGE_SIZE != 0 {
            return Err(err(format!(
                "HVF memory region is not 16KiB-aligned: gpa={gpa:#x} len={len:#x}"
            )));
        }
        let host = region.as_ptr() as *const c_void;
        let flags = HV_MEMORY_READ | HV_MEMORY_WRITE | HV_MEMORY_EXEC;
        // SAFETY: `host` points to the mmap owned by `memory`, the guest IPA and
        // length are page-aligned, and `memory` outlives the HVF VM wrapper.
        let ret = unsafe { hv_vm_map(host, gpa, len as usize, flags) };
        if ret != hv_error_t::HV_SUCCESS as i32 {
            return Err(err(format!(
                "hv_vm_map({host:p}, {gpa:#x}, {len:#x}) failed: {ret:#x}"
            )));
        }
    }
    Ok(())
}
