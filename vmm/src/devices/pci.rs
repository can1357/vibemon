//! PCI host bridge and MMIO dispatch shared by emulated and assigned devices.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
	devices::BusDevice,
	result::{Result, err},
};

/// A PCI function reachable through config space and memory BARs.
pub trait PciFunction: Send + Sync {
	/// Read PCI configuration bytes at `offset`.
	fn read_config(&self, offset: usize, data: &mut [u8]);
	/// Write PCI configuration bytes at `offset`.
	fn write_config(&self, offset: usize, data: &[u8]);
	/// Read a guest physical address owned by one of this function's BARs.
	fn read_bar(&self, addr: u64, data: &mut [u8]) -> bool;
	/// Write a guest physical address owned by one of this function's BARs.
	fn write_bar(&self, addr: u64, data: &[u8]) -> bool;
}

/// Bus-zero PCI host bridge exposed through config mechanism #1 and ECAM.
pub struct PciRoot {
	config_address: u32,
	functions:      Vec<RegisteredFunction>,
}

struct RegisteredFunction {
	device:   u8,
	function: u8,
	pci:      Arc<dyn PciFunction>,
}

impl PciRoot {
	/// Create an empty bus-zero PCI host bridge.
	pub const fn new() -> Self {
		Self { config_address: 0, functions: Vec::new() }
	}

	/// Register one PCI function at `00:device.function`.
	pub fn register(&mut self, device: u8, function: u8, pci: Arc<dyn PciFunction>) -> Result<()> {
		if device >= 32 || function >= 8 {
			return Err(err(format!("invalid PCI address 00:{device:02x}.{function}")));
		}
		if self
			.functions
			.iter()
			.any(|registered| registered.device == device && registered.function == function)
		{
			return Err(err(format!("duplicate PCI address 00:{device:02x}.{function}")));
		}
		self
			.functions
			.push(RegisteredFunction { device, function, pci });
		Ok(())
	}

	fn selected(&self) -> Option<(usize, &Arc<dyn PciFunction>)> {
		if self.config_address & 0x8000_0000 == 0 {
			return None;
		}
		let bus = ((self.config_address >> 16) & 0xff) as u8;
		if bus != 0 {
			return None;
		}
		let device = ((self.config_address >> 11) & 0x1f) as u8;
		let function = ((self.config_address >> 8) & 0x07) as u8;
		let register = (self.config_address & 0xfc) as usize;
		self
			.functions
			.iter()
			.find(|registered| registered.device == device && registered.function == function)
			.map(|registered| (register, &registered.pci))
	}

	fn read_config_data(&self, byte_offset: usize, data: &mut [u8]) {
		data.fill(0xff);
		let Some((register, function)) = self.selected() else {
			return;
		};
		function.read_config(register + byte_offset, data);
	}

	fn write_config_data(&self, byte_offset: usize, data: &[u8]) {
		let Some((register, function)) = self.selected() else {
			return;
		};
		function.write_config(register + byte_offset, data);
	}

	fn ecam_selected(&self, offset: u64) -> Option<(usize, &Arc<dyn PciFunction>)> {
		let bus = ((offset >> 20) & 0xff) as u8;
		if bus != 0 {
			return None;
		}
		let device = ((offset >> 15) & 0x1f) as u8;
		let function = ((offset >> 12) & 0x07) as u8;
		let register = (offset & 0xfff) as usize;
		self
			.functions
			.iter()
			.find(|registered| registered.device == device && registered.function == function)
			.map(|registered| (register, &registered.pci))
	}

	/// Read one function's `PCIe` extended configuration space.
	pub fn read_ecam(&self, offset: u64, data: &mut [u8]) {
		data.fill(0xff);
		let Some((register, function)) = self.ecam_selected(offset) else {
			return;
		};
		function.read_config(register, data);
	}

	/// Write one function's `PCIe` extended configuration space.
	pub fn write_ecam(&self, offset: u64, data: &[u8]) {
		let Some((register, function)) = self.ecam_selected(offset) else {
			return;
		};
		function.write_config(register, data);
	}
}

/// `PCIe` ECAM/MMCONFIG view of [`PciRoot`].
pub struct PciEcam {
	root: Arc<Mutex<PciRoot>>,
}

impl PciEcam {
	/// Create an ECAM view backed by `root`.
	pub const fn new(root: Arc<Mutex<PciRoot>>) -> Self {
		Self { root }
	}
}

impl BusDevice for PciEcam {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		self.root.lock().read_ecam(offset, data);
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		self.root.lock().write_ecam(offset, data);
	}
}

impl BusDevice for PciRoot {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		if offset < 4 {
			let address = self.config_address.to_le_bytes();
			copy_from(&address, offset as usize, data, 0);
		} else if offset < 8 {
			self.read_config_data((offset - 4) as usize, data);
		} else {
			data.fill(0xff);
		}
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		if offset < 4 {
			let mut address = self.config_address.to_le_bytes();
			overlay(&mut address, offset as usize, data);
			self.config_address = u32::from_le_bytes(address);
		} else if offset < 8 {
			self.write_config_data((offset - 4) as usize, data);
		}
	}
}

/// Dispatches MMIO exits across the BARs registered on a PCI bus.
pub struct PciMmioDispatcher {
	aperture_base: u64,
	functions:     Vec<Arc<dyn PciFunction>>,
}

impl PciMmioDispatcher {
	/// Create a dispatcher for a bus mapping beginning at `aperture_base`.
	pub const fn new(aperture_base: u64) -> Self {
		Self { aperture_base, functions: Vec::new() }
	}

	/// Add a PCI function whose BARs may overlap this aperture.
	pub fn register(&mut self, function: Arc<dyn PciFunction>) {
		self.functions.push(function);
	}
}

impl BusDevice for PciMmioDispatcher {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		let Some(addr) = self.aperture_base.checked_add(offset) else {
			data.fill(0xff);
			return;
		};
		for function in &self.functions {
			if function.read_bar(addr, data) {
				return;
			}
		}
		data.fill(0xff);
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		let Some(addr) = self.aperture_base.checked_add(offset) else {
			return;
		};
		for function in &self.functions {
			if function.write_bar(addr, data) {
				return;
			}
		}
	}
}

fn copy_from(source: &[u8], offset: usize, data: &mut [u8], fill: u8) {
	data.fill(fill);
	let Some(source) = source.get(offset..) else {
		return;
	};
	let len = source.len().min(data.len());
	data[..len].copy_from_slice(&source[..len]);
}

fn overlay(destination: &mut [u8], offset: usize, data: &[u8]) {
	let Some(destination) = destination.get_mut(offset..) else {
		return;
	};
	let len = destination.len().min(data.len());
	destination[..len].copy_from_slice(&data[..len]);
}

#[cfg(test)]
mod tests {
	use super::*;

	struct TestFunction {
		config:   Mutex<Vec<u8>>,
		bar_base: u64,
		bar:      Mutex<[u8; 8]>,
	}

	impl TestFunction {
		fn new(bar_base: u64) -> Self {
			let mut config = vec![0; 4096];
			config[..4].copy_from_slice(&[0xde, 0x10, 0xef, 0xbe]);
			Self { config: Mutex::new(config), bar_base, bar: Mutex::new([0; 8]) }
		}
	}

	impl PciFunction for TestFunction {
		fn read_config(&self, offset: usize, data: &mut [u8]) {
			copy_from(&self.config.lock(), offset, data, 0xff);
		}

		fn write_config(&self, offset: usize, data: &[u8]) {
			overlay(&mut self.config.lock(), offset, data);
		}

		fn read_bar(&self, addr: u64, data: &mut [u8]) -> bool {
			let Some(offset) = addr
				.checked_sub(self.bar_base)
				.and_then(|value| usize::try_from(value).ok())
			else {
				return false;
			};
			let Some(end) = offset.checked_add(data.len()) else {
				return false;
			};
			if end > 8 {
				return false;
			}
			data.copy_from_slice(&self.bar.lock()[offset..end]);
			true
		}

		fn write_bar(&self, addr: u64, data: &[u8]) -> bool {
			let Some(offset) = addr
				.checked_sub(self.bar_base)
				.and_then(|value| usize::try_from(value).ok())
			else {
				return false;
			};
			let Some(end) = offset.checked_add(data.len()) else {
				return false;
			};
			if end > 8 {
				return false;
			}
			self.bar.lock()[offset..end].copy_from_slice(data);
			true
		}
	}

	#[test]
	fn routes_legacy_ecam_and_bar_accesses_to_the_selected_function() {
		let function = Arc::new(TestFunction::new(0xc100_0000));
		let mut root = PciRoot::new();
		root
			.register(3, 2, function.clone())
			.expect("register PCI function");
		assert!(root.register(3, 2, function.clone()).is_err());

		let address = 0x8000_0000u32 | (3 << 11) | (2 << 8);
		root.write(0, &address.to_le_bytes());
		let mut legacy = [0; 4];
		root.read(4, &mut legacy);
		assert_eq!(legacy, [0xde, 0x10, 0xef, 0xbe]);

		root.write_ecam((3 << 15) | (2 << 12) | 2, &[0xaa, 0xbb]);
		let mut ecam = [0; 4];
		root.read_ecam((3 << 15) | (2 << 12), &mut ecam);
		assert_eq!(ecam, [0xde, 0x10, 0xaa, 0xbb]);

		let mut mmio = PciMmioDispatcher::new(0xc000_0000);
		mmio.register(function);
		mmio.write(0x0100_0003, &[7, 8]);
		let mut bar = [0; 2];
		mmio.read(0x0100_0003, &mut bar);
		assert_eq!(bar, [7, 8]);

		let mut absent = [0; 2];
		mmio.read(0, &mut absent);
		assert_eq!(absent, [0xff; 2]);
	}
}
