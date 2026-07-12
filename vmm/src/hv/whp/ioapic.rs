//! Userspace IOAPIC model for WHP interrupt injection.

use std::{io, sync::Arc};

use libwhp::{
	Partition, WHV_INTERRUPT_CONTROL, WHV_INTERRUPT_DESTINATION_MODE, WHV_INTERRUPT_TRIGGER_MODE,
	WHV_INTERRUPT_TYPE,
};
use parking_lot::Mutex;

use crate::{
	devices::BusDevice,
	result::{Result, err},
};

const IOAPIC_REG_ID: u32 = 0x00;
const IOAPIC_REG_VER: u32 = 0x01;
const IOAPIC_REG_REDIR_BASE: u32 = 0x10;
const IOAPIC_REDIRECTION_ENTRIES: usize = 24;
const IOAPIC_REGSEL: u64 = 0x00;
const IOAPIC_WINDOW: u64 = 0x10;
const REDIR_MASKED: u64 = 1 << 16;

/// Serializable userspace IOAPIC register state.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct IoApicState {
	pub selector:    u32,
	pub id:          u8,
	pub redirection: Vec<u64>,
}

/// Userspace IOAPIC whose redirection entries inject through
/// `WHvRequestInterrupt`.
pub struct IoApic {
	partition:   Partition,
	selector:    u32,
	id:          u8,
	redirection: [u64; IOAPIC_REDIRECTION_ENTRIES],
}

impl IoApic {
	/// Create an IOAPIC attached to a WHP partition.
	pub const fn new(partition: Partition) -> Self {
		Self {
			partition,
			selector: 0,
			id: 0,
			redirection: [REDIR_MASKED; IOAPIC_REDIRECTION_ENTRIES],
		}
	}

	/// Return the number of GSI pins implemented by this IOAPIC.
	pub const fn pin_count() -> u32 {
		IOAPIC_REDIRECTION_ENTRIES as u32
	}

	/// Return whether `gsi` is backed by an IOAPIC redirection entry.
	pub const fn supports_gsi(gsi: u32) -> bool {
		gsi < Self::pin_count()
	}

	/// Inject the interrupt currently routed for global system interrupt `gsi`.
	pub fn trigger(&self, gsi: u32) -> Result<()> {
		let entry = *self
			.redirection
			.get(gsi as usize)
			.ok_or_else(|| err(format!("WHP IOAPIC GSI {gsi} is out of range")))?;
		if entry & REDIR_MASKED != 0 {
			return Ok(());
		}

		let vector = (entry & 0xff) as u32;
		if vector == 0 {
			return Err(err(format!("WHP IOAPIC GSI {gsi} has no vector")));
		}

		let delivery_mode = ((entry >> 8) & 0x7) as u8;
		let destination_mode = ((entry >> 11) & 0x1) as u8;
		let trigger_mode = ((entry >> 15) & 0x1) as u8;
		let destination = ((entry >> 56) & 0xff) as u32;

		let mut interrupt = WHV_INTERRUPT_CONTROL::default();
		interrupt.set_InterruptType(interrupt_type(delivery_mode)? as u64);
		interrupt.set_DestinationMode(match destination_mode {
			0 => WHV_INTERRUPT_DESTINATION_MODE::WHvX64InterruptDestinationModePhysical as u64,
			1 => WHV_INTERRUPT_DESTINATION_MODE::WHvX64InterruptDestinationModeLogical as u64,
			_ => unreachable!(),
		});
		interrupt.set_TriggerMode(match trigger_mode {
			0 => WHV_INTERRUPT_TRIGGER_MODE::WHvX64InterruptTriggerModeEdge as u64,
			1 => WHV_INTERRUPT_TRIGGER_MODE::WHvX64InterruptTriggerModeLevel as u64,
			_ => unreachable!(),
		});
		interrupt.Destination = destination;
		interrupt.Vector = vector;
		self.partition.request_interrupt(&interrupt)?;
		Ok(())
	}

	/// Capture the userspace IOAPIC register file.
	pub fn save_state(&self) -> IoApicState {
		IoApicState {
			selector:    self.selector,
			id:          self.id,
			redirection: self.redirection.to_vec(),
		}
	}

	/// Restore the userspace IOAPIC register file.
	pub fn restore_state(&mut self, state: &IoApicState) -> Result<()> {
		if state.redirection.len() != IOAPIC_REDIRECTION_ENTRIES {
			return Err(err(format!(
				"WHP IOAPIC snapshot has {} entries, expected {IOAPIC_REDIRECTION_ENTRIES}",
				state.redirection.len()
			)));
		}
		self.selector = state.selector;
		self.id = state.id;
		self.redirection.copy_from_slice(&state.redirection);
		Ok(())
	}

	fn read_reg(&self, reg: u32) -> u32 {
		match reg {
			IOAPIC_REG_ID => u32::from(self.id) << 24,
			IOAPIC_REG_VER => 0x11 | (((IOAPIC_REDIRECTION_ENTRIES - 1) as u32) << 16),
			r if r >= IOAPIC_REG_REDIR_BASE => {
				let offset = r - IOAPIC_REG_REDIR_BASE;
				let index = (offset / 2) as usize;
				if index >= IOAPIC_REDIRECTION_ENTRIES {
					return 0;
				}
				let entry = self.redirection[index];
				if offset & 1 == 0 {
					entry as u32
				} else {
					(entry >> 32) as u32
				}
			},
			_ => 0,
		}
	}

	fn write_reg(&mut self, reg: u32, value: u32) {
		match reg {
			IOAPIC_REG_ID => self.id = ((value >> 24) & 0x0f) as u8,
			r if r >= IOAPIC_REG_REDIR_BASE => {
				let offset = r - IOAPIC_REG_REDIR_BASE;
				let index = (offset / 2) as usize;
				if index >= IOAPIC_REDIRECTION_ENTRIES {
					return;
				}
				let current = self.redirection[index];
				self.redirection[index] = if offset & 1 == 0 {
					(current & !0xffff_ffff) | u64::from(value)
				} else {
					(current & 0xffff_ffff) | (u64::from(value) << 32)
				};
			},
			_ => {},
		}
	}
}

impl BusDevice for IoApic {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		data.fill(0);
		let value = match offset {
			IOAPIC_REGSEL => self.selector,
			IOAPIC_WINDOW => self.read_reg(self.selector),
			_ => 0,
		};
		let bytes = value.to_le_bytes();
		let len = data.len().min(bytes.len());
		data[..len].copy_from_slice(&bytes[..len]);
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		let mut bytes = [0u8; 4];
		let len = data.len().min(bytes.len());
		bytes[..len].copy_from_slice(&data[..len]);
		let value = u32::from_le_bytes(bytes);
		match offset {
			IOAPIC_REGSEL => self.selector = value & 0xff,
			IOAPIC_WINDOW => self.write_reg(self.selector, value),
			_ => {},
		}
	}
}

/// Triggerable WHP interrupt line backed by the userspace IOAPIC redirection
/// table.
pub struct IrqLine {
	ioapic: Arc<Mutex<IoApic>>,
	gsi:    u32,
}

impl IrqLine {
	/// Bind an interrupt line to a global system interrupt.
	pub(crate) const fn new(ioapic: Arc<Mutex<IoApic>>, gsi: u32) -> Self {
		Self { ioapic, gsi }
	}

	/// Pulse the guest interrupt line.
	pub fn trigger(&self) -> Result<()> {
		self.ioapic.lock().trigger(self.gsi)
	}
}

impl vm_superio::Trigger for IrqLine {
	type E = io::Error;

	fn trigger(&self) -> io::Result<()> {
		Self::trigger(self).map_err(|e| io::Error::other(e.to_string()))
	}
}

fn interrupt_type(delivery_mode: u8) -> Result<WHV_INTERRUPT_TYPE> {
	match delivery_mode {
		0 => Ok(WHV_INTERRUPT_TYPE::WHvX64InterruptTypeFixed),
		1 => Ok(WHV_INTERRUPT_TYPE::WHvX64InterruptTypeLowestPriority),
		4 => Ok(WHV_INTERRUPT_TYPE::WHvX64InterruptTypeNmi),
		5 => Ok(WHV_INTERRUPT_TYPE::WHvX64InterruptTypeInit),
		6 => Ok(WHV_INTERRUPT_TYPE::WHvX64InterruptTypeSipi),
		_ => Err(err(format!("WHP IOAPIC delivery mode {delivery_mode} is unsupported"))),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn delivery_modes_map_to_whp_interrupt_types() {
		assert_eq!(interrupt_type(0).unwrap(), WHV_INTERRUPT_TYPE::WHvX64InterruptTypeFixed);
		assert_eq!(interrupt_type(1).unwrap(), WHV_INTERRUPT_TYPE::WHvX64InterruptTypeLowestPriority);
		assert_eq!(interrupt_type(4).unwrap(), WHV_INTERRUPT_TYPE::WHvX64InterruptTypeNmi);
		assert_eq!(interrupt_type(5).unwrap(), WHV_INTERRUPT_TYPE::WHvX64InterruptTypeInit);
		assert_eq!(interrupt_type(6).unwrap(), WHV_INTERRUPT_TYPE::WHvX64InterruptTypeSipi);
		assert!(interrupt_type(3).is_err());
	}
}
