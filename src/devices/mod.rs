//! Device-model plumbing: a portable port/MMIO bus.

pub mod serial;

use std::sync::Arc;

use parking_lot::Mutex;

/// A device addressable on a port-IO or MMIO bus.
///
/// `offset` is relative to the device's base address. Accesses are at most 8
/// bytes; devices read/write only the widths they care about.
pub trait BusDevice: Send {
	fn read(&mut self, offset: u64, data: &mut [u8]);
	fn write(&mut self, offset: u64, data: &[u8]);
}

struct Entry {
	base:   u64,
	len:    u64,
	device: Arc<Mutex<dyn BusDevice>>,
}

/// A flat address bus mapping ranges to devices. Immutable after setup, so it
/// is shared (`Arc`) across vCPU threads; each device is individually locked.
#[derive(Default)]
pub struct Bus {
	entries: Vec<Entry>,
}

impl Bus {
	pub fn new() -> Self {
		Self::default()
	}

	/// Map `[base, base+len)` to `device`.
	pub fn register(&mut self, base: u64, len: u64, device: Arc<Mutex<dyn BusDevice>>) {
		self.entries.push(Entry { base, len, device });
	}

	fn find(&self, addr: u64) -> Option<(u64, &Arc<Mutex<dyn BusDevice>>)> {
		self
			.entries
			.iter()
			.find(|e| addr >= e.base && addr < e.base + e.len)
			.map(|e| (e.base, &e.device))
	}

	/// Dispatch a read; returns false if no device owns `addr`.
	pub fn read(&self, addr: u64, data: &mut [u8]) -> bool {
		match self.find(addr) {
			Some((base, dev)) => {
				dev.lock().read(addr - base, data);
				true
			},
			None => false,
		}
	}

	/// Dispatch a write; returns false if no device owns `addr`.
	pub fn write(&self, addr: u64, data: &[u8]) -> bool {
		match self.find(addr) {
			Some((base, dev)) => {
				dev.lock().write(addr - base, data);
				true
			},
			None => false,
		}
	}
}
