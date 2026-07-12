//! Host CPUID template used by the WHP backend.

use std::arch::x86_64::__cpuid_count;

/// One CPUID leaf/subleaf result exposed to a WHP vCPU.
#[derive(Clone, Copy, Debug, Default)]
pub struct CpuIdEntry {
	/// CPUID leaf in EAX.
	pub function: u32,
	/// CPUID subleaf in ECX.
	pub index:    u32,
	/// Result EAX.
	pub eax:      u32,
	/// Result EBX.
	pub ebx:      u32,
	/// Result ECX.
	pub ecx:      u32,
	/// Result EDX.
	pub edx:      u32,
}

/// Host CPUID leaves patched per vCPU before guest entry.
#[derive(Clone, Debug, Default)]
pub struct CpuId {
	entries: Vec<CpuIdEntry>,
}

impl CpuId {
	/// Build a conservative host CPUID template from the current processor.
	pub fn host_supported() -> Self {
		let mut entries = Vec::new();
		let max_basic = cpuid(0, 0).eax;
		for leaf in 0..=max_basic.min(0x16) {
			push_leaf(&mut entries, leaf);
		}
		push_leaf(&mut entries, 0x4000_0000);

		let max_extended = cpuid(0x8000_0000, 0).eax;
		for leaf in 0x8000_0000..=max_extended.min(0x8000_001f) {
			push_leaf(&mut entries, leaf);
		}

		Self { entries }
	}

	/// Return the CPUID entries as an immutable slice.
	pub fn as_slice(&self) -> &[CpuIdEntry] {
		&self.entries
	}

	/// Return the CPUID entries as a mutable slice.
	pub fn as_mut_slice(&mut self) -> &mut [CpuIdEntry] {
		&mut self.entries
	}

	pub(crate) fn result(&self, function: u32, index: u32) -> Option<CpuIdEntry> {
		self
			.entries
			.iter()
			.copied()
			.find(|entry| entry.function == function && entry.index == index)
	}
}

fn push_leaf(entries: &mut Vec<CpuIdEntry>, function: u32) {
	match function {
		0x4 | 0xb | 0xd | 0xf | 0x10 | 0x14 | 0x1f => {
			for index in 0..32 {
				let entry = cpuid_entry(function, index);
				let stop = match function {
					0x4 => entry.eax & 0x1f == 0,
					0xb | 0x1f => entry.ebx == 0,
					0xd => {
						index > 1 && entry.eax == 0 && entry.ebx == 0 && entry.ecx == 0 && entry.edx == 0
					},
					_ => {
						index > 0 && entry.eax == 0 && entry.ebx == 0 && entry.ecx == 0 && entry.edx == 0
					},
				};
				entries.push(entry);
				if stop {
					break;
				}
			}
		},
		_ => entries.push(cpuid_entry(function, 0)),
	}
}

fn cpuid_entry(function: u32, index: u32) -> CpuIdEntry {
	let r = cpuid(function, index);
	CpuIdEntry { function, index, eax: r.eax, ebx: r.ebx, ecx: r.ecx, edx: r.edx }
}

fn cpuid(function: u32, index: u32) -> std::arch::x86_64::CpuidResult {
	__cpuid_count(function, index)
}
