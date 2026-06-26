//! Guest-memory registration helpers for HVF.

use core::ffi::c_void;

use applevisor_sys::{HV_MEMORY_EXEC, HV_MEMORY_READ, HV_MEMORY_WRITE, hv_error_t, hv_vm_map};
use vm_memory::{Address, GuestMemory, GuestMemoryRegion};

use crate::{
	memory::GuestMemoryMmap,
	result::{Result, err},
};

const HVF_PAGE_SIZE: u64 = applevisor_sys::PAGE_SIZE as u64;

/// Map every vm-memory region into the current HVF VM with RWX permissions.
pub fn map_guest_memory(memory: &GuestMemoryMmap) -> Result<()> {
	for region in memory.iter() {
		let gpa = region.start_addr().raw_value();
		let len = region.len();
		let host = region.as_ptr() as *const c_void;
		let host_addr = host as usize as u64;
		if !gpa.is_multiple_of(HVF_PAGE_SIZE)
			|| !len.is_multiple_of(HVF_PAGE_SIZE)
			|| !host_addr.is_multiple_of(HVF_PAGE_SIZE)
		{
			return Err(err(format!(
				"HVF memory region is not 16KiB-aligned: host={host:p} gpa={gpa:#x} len={len:#x}"
			)));
		}
		let flags = HV_MEMORY_READ | HV_MEMORY_WRITE | HV_MEMORY_EXEC;
		// SAFETY: `host` points to the mmap owned by `memory`; the host
		// address, guest IPA and length are page-aligned; and `memory` outlives
		// the HVF VM wrapper.
		let ret = unsafe { hv_vm_map(host, gpa, len as usize, flags) };
		if ret != hv_error_t::HV_SUCCESS as i32 {
			return Err(err(format!("hv_vm_map({host:p}, {gpa:#x}, {len:#x}) failed: {ret:#x}")));
		}
	}
	Ok(())
}
