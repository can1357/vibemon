//! Minimal GDT helpers for 64-bit long-mode boot.
//!
//! Ported from Firecracker (Apache-2.0), itself derived from the Linux
//! kernel's `segment.h`. We build flat code/data/TSS descriptors and translate
//! them into the `kvm_segment` form KVM expects for `KVM_SET_SREGS`.

use kvm_bindings::kvm_segment;

/// Build a conventional segment descriptor from flags/base/limit.
pub fn gdt_entry(flags: u16, base: u32, limit: u32) -> u64 {
	((u64::from(base) & 0xff00_0000u64) << (56 - 24))
		| ((u64::from(flags) & 0x0000_f0ffu64) << 40)
		| ((u64::from(limit) & 0x000f_0000u64) << (48 - 16))
		| ((u64::from(base) & 0x00ff_ffffu64) << 16)
		| (u64::from(limit) & 0x0000_ffffu64)
}

fn get_base(entry: u64) -> u64 {
	((entry & 0xff00_0000_0000_0000) >> 32)
		| ((entry & 0x0000_00ff_0000_0000) >> 16)
		| ((entry & 0x0000_0000_ffff_0000) >> 16)
}

fn get_limit(entry: u64) -> u32 {
	let limit: u32 =
		(((entry & 0x000f_0000_0000_0000) >> 32) | (entry & 0x0000_0000_0000_ffff)) as u32;
	match get_g(entry) {
		0 => limit,
		_ => (limit << 12) | 0xfff,
	}
}

fn get_g(entry: u64) -> u8 {
	((entry & 0x0080_0000_0000_0000) >> 55) as u8
}
fn get_db(entry: u64) -> u8 {
	((entry & 0x0040_0000_0000_0000) >> 54) as u8
}
fn get_l(entry: u64) -> u8 {
	((entry & 0x0020_0000_0000_0000) >> 53) as u8
}
fn get_avl(entry: u64) -> u8 {
	((entry & 0x0010_0000_0000_0000) >> 52) as u8
}
fn get_p(entry: u64) -> u8 {
	((entry & 0x0000_8000_0000_0000) >> 47) as u8
}
fn get_dpl(entry: u64) -> u8 {
	((entry & 0x0000_6000_0000_0000) >> 45) as u8
}
fn get_s(entry: u64) -> u8 {
	((entry & 0x0000_1000_0000_0000) >> 44) as u8
}
fn get_type(entry: u64) -> u8 {
	((entry & 0x0000_0f00_0000_0000) >> 40) as u8
}

/// Translate a GDT entry at `table_index` into a `kvm_segment`.
pub fn kvm_segment_from_gdt(entry: u64, table_index: u8) -> kvm_segment {
	kvm_segment {
		base:     get_base(entry),
		limit:    get_limit(entry),
		selector: u16::from(table_index * 8),
		type_:    get_type(entry),
		present:  get_p(entry),
		dpl:      get_dpl(entry),
		db:       get_db(entry),
		s:        get_s(entry),
		l:        get_l(entry),
		g:        get_g(entry),
		avl:      get_avl(entry),
		padding:  0,
		unusable: if get_p(entry) == 0 { 1 } else { 0 },
	}
}
