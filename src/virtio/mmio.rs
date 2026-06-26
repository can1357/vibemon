//! Modern (v2) virtio-mmio transport.
//!
//! Maps the virtio-mmio register window onto a [`VirtioDevice`]. Control-plane
//! registers (features, queue configuration, status, config space) are handled
//! here in the vCPU thread; the `QueueNotify` register is serviced out-of-band
//! via an ioeventfd, so writes to it never reach this code.

use std::sync::Arc;

use parking_lot::Mutex;
use tracing::error;
use virtio_bindings::bindings::virtio_config::{VIRTIO_CONFIG_S_DRIVER_OK, VIRTIO_CONFIG_S_FAILED};
use virtio_queue::{Queue, QueueState, QueueT};

use crate::{
	devices::BusDevice,
	memory::GuestMemoryMmap,
	result::Result,
	virtio::{Interrupt, VirtioDevice},
};

// virtio-mmio register offsets.
const MAGIC_VALUE: u64 = 0x000;
const VERSION: u64 = 0x004;
const DEVICE_ID: u64 = 0x008;
const VENDOR_ID: u64 = 0x00c;
const DEVICE_FEATURES: u64 = 0x010;
const DEVICE_FEATURES_SEL: u64 = 0x014;
const DRIVER_FEATURES: u64 = 0x020;
const DRIVER_FEATURES_SEL: u64 = 0x024;
const QUEUE_SEL: u64 = 0x030;
const QUEUE_NUM_MAX: u64 = 0x034;
const QUEUE_NUM: u64 = 0x038;
const QUEUE_READY: u64 = 0x044;
const QUEUE_NOTIFY: u64 = 0x050;
const INTERRUPT_STATUS: u64 = 0x060;
const INTERRUPT_ACK: u64 = 0x064;
const STATUS: u64 = 0x070;
const QUEUE_DESC_LOW: u64 = 0x080;
const QUEUE_DESC_HIGH: u64 = 0x084;
const QUEUE_AVAIL_LOW: u64 = 0x090;
const QUEUE_AVAIL_HIGH: u64 = 0x094;
const QUEUE_USED_LOW: u64 = 0x0a0;
const QUEUE_USED_HIGH: u64 = 0x0a4;
const CONFIG_GENERATION: u64 = 0x0fc;
// Shared-memory (DAX) region length registers. We expose no DAX window, so a
// read returns all-ones ("region absent"); otherwise the virtio-fs driver tries
// to reserve a zero-length region and aborts probing.
const SHM_LEN_LOW: u64 = 0x0b0;
const SHM_LEN_HIGH: u64 = 0x0b4;
const CONFIG_SPACE: u64 = 0x100;

const MMIO_MAGIC: u32 = 0x7472_6976; // "virt"
const MMIO_VERSION: u32 = 2;
const MMIO_VENDOR: u32 = 0x4d56_4b56; // "VKVM"

pub struct MmioTransport {
	device:    Arc<Mutex<dyn VirtioDevice>>,
	mem:       GuestMemoryMmap,
	interrupt: Arc<Interrupt>,

	device_type:     u32,
	device_features: u64,
	queues:          Vec<Queue>,

	queue_select:           usize,
	device_features_select: u32,
	driver_features_select: u32,
	acked_features:         u64,
	status:                 u32,
	activated:              bool,
}

/// Serializable snapshot of a [`MmioTransport`]'s control-plane state.
///
/// Holds the transport registers plus a plain-data [`QueueState`] mirror of
/// each virtqueue, so a paused transport can be rebuilt with
/// [`MmioTransport::from_state`] and re-armed with
/// [`MmioTransport::reactivate`].
pub struct MmioState {
	pub device_features_select: u32,
	pub driver_features_select: u32,
	pub acked_features:         u64,
	pub status:                 u32,
	pub activated:              bool,
	pub queues:                 Vec<QueueState>,
}

impl MmioTransport {
	pub fn new(
		device: Arc<Mutex<dyn VirtioDevice>>,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
	) -> crate::result::Result<Self> {
		let (device_type, device_features, queue_sizes) = {
			let d = device.lock();
			(d.device_type(), d.features(), d.queue_max_sizes().to_vec())
		};
		let queues = new_queues(&queue_sizes)?;
		Ok(Self {
			device,
			mem,
			interrupt,
			device_type,
			device_features,
			queues,
			queue_select: 0,
			device_features_select: 0,
			driver_features_select: 0,
			acked_features: 0,
			status: 0,
			activated: false,
		})
	}

	/// Snapshot the transport's control-plane state for serialization.
	pub fn save(&self) -> MmioState {
		let live_queues = self.device.lock().queue_states();
		let queues = if live_queues.is_empty() {
			self.queues.iter().map(|q| q.state()).collect()
		} else {
			live_queues
		};
		MmioState {
			device_features_select: self.device_features_select,
			driver_features_select: self.driver_features_select,
			acked_features: self.acked_features,
			status: self.status,
			activated: self.activated,
			queues,
		}
	}

	/// Rebuild a transport from a [`MmioState`] snapshot.
	///
	/// Seeds the control-plane registers and rebuilds each virtqueue from its
	/// saved [`QueueState`]. The transport is left de-activated; the restore
	/// path must call [`reactivate`](Self::reactivate) to hand the queues to the
	/// device worker.
	pub fn from_state(
		device: Arc<Mutex<dyn VirtioDevice>>,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		state: &MmioState,
	) -> crate::result::Result<Self> {
		let (device_type, device_features) = {
			let d = device.lock();
			(d.device_type(), d.features())
		};
		let queues = state
			.queues
			.iter()
			.copied()
			.map(Queue::try_from)
			.collect::<Result<Vec<_>, _>>()?;
		Ok(Self {
			device,
			mem,
			interrupt,
			device_type,
			device_features,
			queues,
			queue_select: 0,
			device_features_select: state.device_features_select,
			driver_features_select: state.driver_features_select,
			acked_features: state.acked_features,
			status: state.status,
			activated: false,
		})
	}

	fn with_queue<R>(&self, f: impl FnOnce(&Queue) -> R, default: R) -> R {
		self.queues.get(self.queue_select).map_or(default, f)
	}

	fn with_queue_mut(&mut self, f: impl FnOnce(&mut Queue)) {
		if let Some(q) = self.queues.get_mut(self.queue_select) {
			f(q);
		}
	}

	fn reset(&mut self) -> Result<()> {
		let (device_features, queue_sizes) = {
			let mut device = self.device.lock();
			device.reset()?;
			(device.features(), device.queue_max_sizes().to_vec())
		};
		self.device_features = device_features;
		self.queues = new_queues(&queue_sizes)?;
		self.queue_select = 0;
		self.device_features_select = 0;
		self.driver_features_select = 0;
		self.acked_features = 0;
		self.status = 0;
		self.activated = false;
		self.interrupt.clear();
		Ok(())
	}

	fn set_status(&mut self, value: u32) {
		if value == 0 {
			if let Err(e) = self.reset() {
				error!("virtio-mmio reset failed: {e}");
				crate::metrics::record_device_worker_error();
				self.status = VIRTIO_CONFIG_S_FAILED;
				self.activated = false;
			}
			return;
		}
		self.status = value;
		if value & VIRTIO_CONFIG_S_DRIVER_OK != 0 && !self.activated {
			self.activate();
		}
	}

	fn activate(&mut self) {
		if !self.queues.iter().all(|q| q.is_valid(&self.mem)) {
			error!("virtio device activation refused invalid queue configuration");
			crate::metrics::record_device_worker_error();
			self.status |= VIRTIO_CONFIG_S_FAILED;
			return;
		}

		let queues = std::mem::take(&mut self.queues);
		let mut device = self.device.lock();
		device.ack_features(self.acked_features);
		match device.activate(self.mem.clone(), self.interrupt.clone(), queues) {
			Ok(()) => self.activated = true,
			Err(e) => {
				error!("virtio device activation failed: {e}");
				crate::metrics::record_device_worker_error();
				self.status |= VIRTIO_CONFIG_S_FAILED;
			},
		}
	}

	/// Restore-path counterpart to [`activate`](Self::activate): hand the
	/// restored queues to the device and mark the transport active.
	///
	/// Unlike `activate`, which records failure in the status register, this
	/// propagates activation errors to the caller.
	pub fn reactivate(&mut self) -> crate::result::Result<()> {
		if !self.queues.iter().all(|q| q.is_valid(&self.mem)) {
			crate::bail!("virtio device restore refused invalid queue configuration");
		}
		let queues = std::mem::take(&mut self.queues);
		let mut device = self.device.lock();
		device.ack_features(self.acked_features);
		device.activate(self.mem.clone(), self.interrupt.clone(), queues)?;
		self.activated = true;
		Ok(())
	}
}

impl BusDevice for MmioTransport {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		if offset >= CONFIG_SPACE {
			self.device.lock().read_config(offset - CONFIG_SPACE, data);
			return;
		}
		let value: u32 = match offset {
			MAGIC_VALUE => MMIO_MAGIC,
			VERSION => MMIO_VERSION,
			DEVICE_ID => self.device_type,
			VENDOR_ID => MMIO_VENDOR,
			DEVICE_FEATURES => match self.device_features_select {
				0 => self.device_features as u32,
				1 => (self.device_features >> 32) as u32,
				_ => 0,
			},
			QUEUE_NUM_MAX => self.with_queue(|q| u32::from(q.max_size()), 0),
			QUEUE_READY => self.with_queue(|q| u32::from(q.ready()), 0),
			INTERRUPT_STATUS => self.interrupt.status(),
			STATUS => self.status,
			CONFIG_GENERATION => 0,
			SHM_LEN_LOW | SHM_LEN_HIGH => 0xffff_ffff,
			_ => 0,
		};
		write_le(data, value);
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		if offset >= CONFIG_SPACE {
			self.device.lock().write_config(offset - CONFIG_SPACE, data);
			return;
		}
		let value = read_le(data);
		match offset {
			DEVICE_FEATURES_SEL => self.device_features_select = value,
			DRIVER_FEATURES if self.driver_features_select < 2 => {
				let shift = 32 * self.driver_features_select;
				let mask = 0xffff_ffffu64 << shift;
				let selected = (u64::from(value) << shift) & self.device_features & mask;
				self.acked_features = (self.acked_features & !mask) | selected;
			},
			DRIVER_FEATURES_SEL => self.driver_features_select = value,
			QUEUE_SEL => self.queue_select = value as usize,
			QUEUE_NUM => self.with_queue_mut(|q| {
				if u16::try_from(value).is_ok() {
					let _ = q.try_set_size(value as u16);
				}
			}),
			QUEUE_READY => self.with_queue_mut(|q| q.set_ready(value == 1)),
			QUEUE_NOTIFY => {}, // serviced via ioeventfd
			INTERRUPT_ACK => self.interrupt.ack(value),
			STATUS => self.set_status(value),
			QUEUE_DESC_LOW => self.with_queue_mut(|q| q.set_desc_table_address(Some(value), None)),
			QUEUE_DESC_HIGH => self.with_queue_mut(|q| q.set_desc_table_address(None, Some(value))),
			QUEUE_AVAIL_LOW => self.with_queue_mut(|q| q.set_avail_ring_address(Some(value), None)),
			QUEUE_AVAIL_HIGH => {
				self.with_queue_mut(|q| q.set_avail_ring_address(None, Some(value)));
			},
			QUEUE_USED_LOW => self.with_queue_mut(|q| q.set_used_ring_address(Some(value), None)),
			QUEUE_USED_HIGH => self.with_queue_mut(|q| q.set_used_ring_address(None, Some(value))),
			_ => {},
		}
	}
}

fn new_queues(queue_sizes: &[u16]) -> crate::result::Result<Vec<Queue>> {
	let mut queues = Vec::with_capacity(queue_sizes.len());
	for &size in queue_sizes {
		queues.push(Queue::new(size)?);
	}
	Ok(queues)
}

fn read_le(data: &[u8]) -> u32 {
	let mut buf = [0u8; 4];
	let n = data.len().min(4);
	buf[..n].copy_from_slice(&data[..n]);
	u32::from_le_bytes(buf)
}

fn write_le(data: &mut [u8], value: u32) {
	let bytes = value.to_le_bytes();
	let n = data.len().min(4);
	data[..n].copy_from_slice(&bytes[..n]);
}
