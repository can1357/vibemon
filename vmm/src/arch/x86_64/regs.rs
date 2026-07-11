//! vCPU register setup for the Linux 64-bit boot protocol.
//!
//! Ported from Firecracker (Apache-2.0). We drop the guest directly into
//! 64-bit long mode: flat segments, identity-mapped 2 MiB pages covering the
//! first 1 GiB, and `%rip`/`%rsi` set per the Linux boot ABI.

use std::mem::size_of;

use kvm_bindings::{kvm_fpu, kvm_regs, kvm_sregs};
use vm_memory::{Address, Bytes, GuestAddress, GuestMemory};

use super::gdt::{gdt_entry, kvm_segment_from_gdt};
use crate::{
	hv::Vcpu,
	layout::{
		BOOT_GDT_OFFSET, BOOT_IDT_OFFSET, BOOT_STACK_POINTER, PDE_START, PDPTE_START, PML4_START,
		ZERO_PAGE_START,
	},
	memory::GuestMemoryMmap,
	result::Result,
};

const BOOT_GDT_MAX: usize = 4;

const EFER_LMA: u64 = 0x400;
const EFER_LME: u64 = 0x100;
const X86_CR0_PE: u64 = 0x1;
const X86_CR0_PG: u64 = 0x8000_0000;
const X86_CR4_PAE: u64 = 0x20;

/// Configure the x87/SSE control words to sane reset defaults.
pub fn setup_fpu(vcpu: &Vcpu) -> Result<()> {
	let fpu = kvm_fpu { fcw: 0x37f, mxcsr: 0x1f80, ..Default::default() };
	vcpu.fd().set_fpu(&fpu)?;
	Ok(())
}

/// Set the general-purpose registers for the boot entry point.
pub fn setup_regs(vcpu: &Vcpu, boot_ip: u64) -> Result<()> {
	let regs = kvm_regs {
		rflags: 0x0000_0000_0000_0002,
		rip: boot_ip,
		rsp: BOOT_STACK_POINTER,
		rbp: BOOT_STACK_POINTER,
		// The Linux 64-bit boot ABI requires %rsi to point at the zero page.
		rsi: ZERO_PAGE_START,
		..Default::default()
	};
	vcpu.fd().set_regs(&regs)?;
	Ok(())
}

/// Configure segment/control registers and install boot page tables.
pub fn setup_sregs(mem: &GuestMemoryMmap, vcpu: &Vcpu) -> Result<()> {
	let mut sregs = vcpu.fd().get_sregs()?;
	configure_segments_and_sregs(mem, &mut sregs)?;
	setup_page_tables(mem, &mut sregs)?;
	vcpu.fd().set_sregs(&sregs)?;
	Ok(())
}

fn write_gdt_table(table: &[u64], mem: &GuestMemoryMmap) -> Result<()> {
	let boot_gdt_addr = GuestAddress(BOOT_GDT_OFFSET);
	for (index, entry) in table.iter().enumerate() {
		let addr = mem
			.checked_offset(boot_gdt_addr, index * size_of::<u64>())
			.ok_or("failed to compute GDT entry address")?;
		mem.write_obj(*entry, addr)?;
	}
	Ok(())
}

fn configure_segments_and_sregs(mem: &GuestMemoryMmap, sregs: &mut kvm_sregs) -> Result<()> {
	// NULL, CODE, DATA, TSS — flat 4 GiB segments for 64-bit boot.
	let gdt_table: [u64; BOOT_GDT_MAX] = [
		gdt_entry(0, 0, 0),
		gdt_entry(0xa09b, 0, 0xfffff), // CODE
		gdt_entry(0xc093, 0, 0xfffff), // DATA
		gdt_entry(0x808b, 0, 0xfffff), // TSS
	];

	let code_seg = kvm_segment_from_gdt(gdt_table[1], 1);
	let data_seg = kvm_segment_from_gdt(gdt_table[2], 2);
	let tss_seg = kvm_segment_from_gdt(gdt_table[3], 3);

	write_gdt_table(&gdt_table[..], mem)?;
	sregs.gdt.base = BOOT_GDT_OFFSET;
	sregs.gdt.limit = u16::try_from(size_of::<u64>() * BOOT_GDT_MAX).unwrap() - 1;

	mem.write_obj(0u64, GuestAddress(BOOT_IDT_OFFSET))?;
	sregs.idt.base = BOOT_IDT_OFFSET;
	sregs.idt.limit = u16::try_from(size_of::<u64>()).unwrap() - 1;

	sregs.cs = code_seg;
	sregs.ds = data_seg;
	sregs.es = data_seg;
	sregs.fs = data_seg;
	sregs.gs = data_seg;
	sregs.ss = data_seg;
	sregs.tr = tss_seg;

	sregs.cr0 |= X86_CR0_PE;
	sregs.efer |= EFER_LME | EFER_LMA;
	Ok(())
}

fn setup_page_tables(mem: &GuestMemoryMmap, sregs: &mut kvm_sregs) -> Result<()> {
	let boot_pml4_addr = GuestAddress(PML4_START);
	let boot_pdpte_addr = GuestAddress(PDPTE_START);
	let boot_pde_addr = GuestAddress(PDE_START);

	// PML4[0] -> PDPTE, covering VA [0, 512 GiB).
	mem.write_obj(boot_pdpte_addr.raw_value() | 0x03, boot_pml4_addr)?;
	// PDPTE[0] -> PDE, covering VA [0, 1 GiB).
	mem.write_obj(boot_pde_addr.raw_value() | 0x03, boot_pdpte_addr)?;
	// 512 * 2 MiB pages covering VA [0, 1 GiB), present+writable+huge (0x83).
	for i in 0..512u64 {
		mem.write_obj((i << 21) + 0x83u64, boot_pde_addr.unchecked_add(i * 8))?;
	}

	sregs.cr3 = boot_pml4_addr.raw_value();
	sregs.cr4 |= X86_CR4_PAE;
	sregs.cr0 |= X86_CR0_PG;
	Ok(())
}
