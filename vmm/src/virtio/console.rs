//! virtio-console single-port device used as the warm-boot guest-agent channel.
//!
//! Only two queues are wired (no control queue, no multiport): index 0 is the
//! receiveq (host -> guest) and index 1 is the transmitq (guest -> host). The
//! host injects bytes by pushing them into the shared `input` deque and kicking
//! `input_evt`; the worker copies them into the guest's RX descriptors. Guest
//! console output is drained off the TX queue into a capped `output` buffer and
//! signalled through `output_evt` for the host bridge to forward or discard. No
//! features beyond `VIRTIO_F_VERSION_1` are offered, so the config space stays
//! zero (no `F_SIZE` / `F_MULTIPORT`).

use std::{
	collections::VecDeque,
	os::unix::io::{AsRawFd, RawFd},
	sync::Arc,
};

use parking_lot::Mutex;
use virtio_bindings::{bindings::virtio_config::VIRTIO_F_VERSION_1, virtio_ids::VIRTIO_ID_CONSOLE};
use virtio_queue::{Queue, QueueT};
use vm_memory::{Address, Bytes, GuestAddress};

use crate::{
	memory::GuestMemoryMmap,
	os::EventFd,
	result::{Result, err},
	virtio::{Interrupt, VirtioDevice, descriptor_range_valid},
};

const QUEUE_SIZE: u16 = 64;
const NUM_QUEUES: usize = 2;
const RX_QUEUE: usize = 0;
const TX_QUEUE: usize = 1;
/// `virtio_console_config` is `cols/rows/max_nr_ports/emerg_wr`; all left zero
/// since no config-bearing feature is negotiated. 16 bytes comfortably covers
/// it.
const CONFIG_SPACE_SIZE: usize = 16;
pub const OUTPUT_CAP: usize = 8 << 20;

struct TxDesc {
	addr: GuestAddress,
	len:  usize,
}

struct PendingTxChain {
	head:        u16,
	descs:       Vec<TxDesc>,
	desc_index:  usize,
	desc_offset: usize,
}

pub struct Console {
	features:       u64,
	acked_features: u64,
	queue_sizes:    Vec<u16>,
	config:         Vec<u8>,

	/// Host -> guest bytes awaiting injection into the RX queue.
	input:            Arc<Mutex<VecDeque<u8>>>,
	/// Kicked by the host through `input_handle` to wake the worker for RX.
	input_evt:        EventFd,
	/// Guest -> host bytes drained from the TX queue.
	output:           Arc<Mutex<Vec<u8>>>,
	/// Kicked by the worker when fresh guest output is appended.
	output_evt:       EventFd,
	/// Kicked by the host bridge after it frees output-buffer capacity.
	drain_resume_evt: EventFd,
	/// TX chain popped while the host output buffer was full; completed later.
	pending_tx:       Option<PendingTxChain>,
	mem:              Option<GuestMemoryMmap>,
	interrupt:        Option<Arc<Interrupt>>,
	rx_queue:         Option<Queue>,
	tx_queue:         Option<Queue>,
}

impl Console {
	pub fn new() -> Result<Self> {
		Ok(Self {
			features:         1u64 << VIRTIO_F_VERSION_1,
			acked_features:   0,
			queue_sizes:      vec![QUEUE_SIZE; NUM_QUEUES],
			config:           vec![0u8; CONFIG_SPACE_SIZE],
			input:            Arc::new(Mutex::new(VecDeque::new())),
			input_evt:        EventFd::new(crate::os::EFD_NONBLOCK)?,
			output:           Arc::new(Mutex::new(Vec::new())),
			output_evt:       EventFd::new(crate::os::EFD_NONBLOCK)?,
			drain_resume_evt: EventFd::new(crate::os::EFD_NONBLOCK)?,
			pending_tx:       None,
			mem:              None,
			interrupt:        None,
			rx_queue:         None,
			tx_queue:         None,
		})
	}

	/// Shared host -> guest buffer plus a wake handle, for a VMM that drives the
	/// channel after the device has been moved into its worker: push bytes into
	/// the deque, then write `1` to the returned eventfd to kick RX servicing.
	pub fn input_handle(&self) -> (Arc<Mutex<VecDeque<u8>>>, EventFd) {
		(
			Arc::clone(&self.input),
			self
				.input_evt
				.try_clone()
				.expect("dup virtio-console input eventfd"),
		)
	}

	/// Shared guest -> host output buffer plus wake/resume eventfds for the
	/// host bridge. The bridge drains `output`, then kicks `drain_resume_evt`
	/// so the worker can resume TX once capacity is available.
	pub fn output_handle(&self) -> (Arc<Mutex<Vec<u8>>>, EventFd, EventFd) {
		(
			Arc::clone(&self.output),
			self
				.output_evt
				.try_clone()
				.expect("dup virtio-console output eventfd"),
			self
				.drain_resume_evt
				.try_clone()
				.expect("dup virtio-console drain-resume eventfd"),
		)
	}

	/// Raise the used-buffer interrupt if the device is activated.
	fn signal_used(&self) -> Result<()> {
		if let Some(interrupt) = &self.interrupt {
			interrupt.signal_used_queue()?;
		}
		Ok(())
	}

	/// Drain the transmit queue (guest -> host): append each chain's readable
	/// descriptor bytes to `output`. Returns whether any buffer was used.
	fn drain_pending_tx(
		pending_tx: &mut Option<PendingTxChain>,
		mem: &GuestMemoryMmap,
		tx_queue: &mut Queue,
		out: &mut Vec<u8>,
	) -> Result<(bool, bool)> {
		let mut produced = false;
		let (complete, head) = {
			let Some(pending) = pending_tx.as_mut() else {
				return Ok((false, false));
			};

			while pending.desc_index < pending.descs.len() {
				if out.len() >= OUTPUT_CAP {
					break;
				}
				let desc = &pending.descs[pending.desc_index];
				if pending.desc_offset >= desc.len {
					pending.desc_index += 1;
					pending.desc_offset = 0;
					continue;
				}

				let take = (desc.len - pending.desc_offset).min(OUTPUT_CAP - out.len());
				if take == 0 || out.try_reserve(take).is_err() {
					break;
				}

				let start = out.len();
				let Some(addr) = desc.addr.checked_add(pending.desc_offset as u64) else {
					pending.desc_index = pending.descs.len();
					break;
				};
				out.resize(start + take, 0);
				if mem.read_slice(&mut out[start..start + take], addr).is_err() {
					out.truncate(start);
					pending.desc_index = pending.descs.len();
					break;
				}

				pending.desc_offset += take;
				produced = true;
				if pending.desc_offset == desc.len {
					pending.desc_index += 1;
					pending.desc_offset = 0;
				}
			}

			(pending.desc_index >= pending.descs.len(), pending.head)
		};

		if !complete {
			return Ok((false, produced));
		}

		*pending_tx = None;
		tx_queue
			.add_used(mem, head, 0)
			.map_err(|e| err(format!("virtio-console TX used-ring update failed: {e}")))?;
		Ok((true, produced))
	}

	/// Drain the transmit queue (guest -> host): append each chain's readable
	/// descriptor bytes to `output`. Returns whether any buffer was used.
	fn drain_tx(&mut self) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(tx_queue) = self.tx_queue.as_mut() else {
			return Ok(false);
		};

		let mut used = false;
		let mut produced = false;
		{
			let mut out = self.output.lock();
			let (pending_used, pending_produced) =
				Self::drain_pending_tx(&mut self.pending_tx, &mem, tx_queue, &mut out)?;
			used |= pending_used;
			produced |= pending_produced;

			while self.pending_tx.is_none() && out.len() < OUTPUT_CAP {
				let Some(chain) = tx_queue.pop_descriptor_chain(&mem) else {
					break;
				};
				let head = chain.head_index();
				let mut descs = Vec::new();
				let mut valid = true;
				for d in chain {
					if d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
						valid = false;
						break;
					}
					descs.push(TxDesc { addr: d.addr(), len: d.len() as usize });
				}

				if valid {
					self.pending_tx =
						Some(PendingTxChain { head, descs, desc_index: 0, desc_offset: 0 });
					let (chain_used, chain_produced) =
						Self::drain_pending_tx(&mut self.pending_tx, &mem, tx_queue, &mut out)?;
					used |= chain_used;
					produced |= chain_produced;
				} else {
					// TX buffers are guest-readable only, so nothing is written back.
					tx_queue
						.add_used(&mem, head, 0)
						.map_err(|e| err(format!("virtio-console TX used-ring update failed: {e}")))?;
					used = true;
				}
			}
		}

		if produced {
			self.output_evt.write(1)?;
		}
		Ok(used)
	}

	/// Fill the receive queue (host -> guest) from `input`: while bytes remain
	/// and an RX chain is available, copy as many as fit into the chain's
	/// writable descriptors. Returns whether any buffer was used.
	fn fill_rx(&mut self) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(rx_queue) = self.rx_queue.as_mut() else {
			return Ok(false);
		};
		let mut inp = self.input.lock();

		let mut used = false;
		while !inp.is_empty() {
			let Some(chain) = rx_queue.pop_descriptor_chain(&mem) else {
				break;
			};
			let head = chain.head_index();
			let mut descs = Vec::new();
			let mut valid = true;
			let mut capacity = 0u32;
			for d in chain {
				let Some(next_capacity) = capacity.checked_add(d.len()) else {
					valid = false;
					break;
				};
				if !d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
					valid = false;
					break;
				}
				capacity = next_capacity;
				descs.push((d.addr(), d.len() as usize));
			}

			let mut written = 0u32;
			if valid {
				for (addr, len) in descs {
					if inp.is_empty() {
						break;
					}
					let take = len.min(inp.len());
					let Some(next_written) = written.checked_add(take as u32) else {
						break;
					};
					if take == 0 {
						continue;
					}
					// Peek the front bytes, write them, and only drain on success so
					// a transient guest-memory fault never silently eats input.
					let ok = {
						let front = inp.make_contiguous();
						mem.write_slice(&front[..take], addr).is_ok()
					};
					if !ok {
						break;
					}
					inp.drain(..take);
					written = next_written;
				}
			}
			rx_queue
				.add_used(&mem, head, written)
				.map_err(|e| err(format!("virtio-console RX used-ring update failed: {e}")))?;
			used = true;
		}
		Ok(used)
	}
}

impl VirtioDevice for Console {
	fn device_type(&self) -> u32 {
		VIRTIO_ID_CONSOLE
	}

	fn queue_max_sizes(&self) -> &[u16] {
		&self.queue_sizes
	}

	fn features(&self) -> u64 {
		self.features
	}

	fn ack_features(&mut self, value: u64) {
		self.acked_features = value & self.features;
	}

	fn read_config(&self, offset: u64, data: &mut [u8]) {
		let offset = offset as usize;
		for (i, b) in data.iter_mut().enumerate() {
			*b = self.config.get(offset + i).copied().unwrap_or(0);
		}
	}

	fn write_config(&mut self, _offset: u64, _data: &[u8]) {
		// virtio-console config space is read-only for the driver.
	}

	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()> {
		let mut queues = queues.into_iter();
		self.rx_queue = queues.next();
		self.tx_queue = queues.next();
		self.mem = Some(mem);
		self.interrupt = Some(interrupt);
		Ok(())
	}

	fn reset(&mut self) -> Result<()> {
		self.acked_features = 0;
		self.pending_tx = None;
		self.mem = None;
		self.interrupt = None;
		self.rx_queue = None;
		self.tx_queue = None;
		Ok(())
	}

	fn worker_fds(&self) -> Vec<RawFd> {
		vec![self.input_evt.as_raw_fd(), self.drain_resume_evt.as_raw_fd()]
	}

	fn process_queue_notify(&mut self) -> Result<()> {
		// The guest rang the doorbell after queueing TX data (and possibly fresh
		// RX buffers). Drain TX, then top up RX from any pending host input.
		let _ = (RX_QUEUE, TX_QUEUE);
		let tx_used = self.drain_tx()?;
		let rx_used = self.fill_rx()?;
		if tx_used || rx_used {
			self.signal_used()?;
		}
		Ok(())
	}

	fn process_worker_fd(&mut self, fd: RawFd) -> Result<()> {
		if fd == self.input_evt.as_raw_fd() {
			// The host injected input; clear the level-triggered eventfd, then
			// push the bytes into whatever RX buffers the guest has posted.
			let _ = self.input_evt.read();
			if self.fill_rx()? {
				self.signal_used()?;
			}
		} else if fd == self.drain_resume_evt.as_raw_fd() {
			// The host drained guest output below the cap; resume TX draining so
			// posted guest buffers can be completed and backpressure can clear.
			let _ = self.drain_resume_evt.read();
			if self.drain_tx()? {
				self.signal_used()?;
			}
		}
		Ok(())
	}

	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		// Order must mirror the transport's queue vec: rx (0) then tx (1).
		let mut v = Vec::new();
		if let Some(q) = &self.rx_queue {
			v.push(q.state());
		}
		if let Some(q) = &self.tx_queue {
			v.push(q.state());
		}
		v
	}

	fn drain(&mut self) -> Result<()> {
		let used = self.drain_tx()?;
		if used {
			self.signal_used()?;
		}
		if self.pending_tx.is_some() {
			return Err(err("virtio-console snapshot blocked by backpressured TX output"));
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use virtio_bindings::bindings::virtio_ring::VRING_DESC_F_WRITE;
	use virtio_queue::{
		QueueT,
		desc::{RawDescriptor, split::Descriptor as SplitDescriptor},
	};
	use vm_memory::{Bytes, GuestAddress};

	use super::*;

	const MEM_SIZE: usize = 0x1_0000;
	const DESC_TABLE: u64 = 0x1000;
	const AVAIL_RING: u64 = 0x2000;
	const USED_RING: u64 = 0x3000;

	fn guest_mem() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(0), MEM_SIZE)]).expect("guest memory")
	}

	fn queue_with_descs(mem: &GuestMemoryMmap, descs: &[SplitDescriptor]) -> Queue {
		for (idx, desc) in descs.iter().enumerate() {
			mem.write_obj(RawDescriptor::from(*desc), GuestAddress(DESC_TABLE + (idx as u64 * 16)))
				.expect("write descriptor");
		}
		mem.write_obj(u16::to_le(0), GuestAddress(AVAIL_RING))
			.expect("write avail flags");
		mem.write_obj(u16::to_le(1), GuestAddress(AVAIL_RING + 2))
			.expect("write avail idx");
		mem.write_obj(u16::to_le(0), GuestAddress(AVAIL_RING + 4))
			.expect("write avail head");

		let mut queue = Queue::new(8).expect("queue");
		queue.set_size(8);
		queue.set_desc_table_address(Some(DESC_TABLE as u32), Some((DESC_TABLE >> 32) as u32));
		queue.set_avail_ring_address(Some(AVAIL_RING as u32), Some((AVAIL_RING >> 32) as u32));
		queue.set_used_ring_address(Some(USED_RING as u32), Some((USED_RING >> 32) as u32));
		queue.set_ready(true);
		queue
	}

	fn used_len(mem: &GuestMemoryMmap) -> u32 {
		u32::from_le(
			mem.read_obj::<u32>(GuestAddress(USED_RING + 8))
				.expect("read used len"),
		)
	}

	fn used_idx(mem: &GuestMemoryMmap) -> u16 {
		u16::from_le(
			mem.read_obj::<u16>(GuestAddress(USED_RING + 2))
				.expect("read used idx"),
		)
	}

	fn console_with_rx(mem: GuestMemoryMmap, queue: Queue, input: &[u8]) -> Console {
		let mut console = Console::new().expect("console");
		console.mem = Some(mem);
		console.rx_queue = Some(queue);
		console.input.lock().extend(input.iter().copied());
		console
	}

	#[test]
	fn fill_rx_rejects_readable_descriptor_without_consuming_input() {
		let mem = guest_mem();
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(0x4000, 64, 0, 0)]);
		let mut console = console_with_rx(mem, queue, b"ping");

		assert!(console.fill_rx().expect("fill_rx"));

		let remaining: Vec<u8> = console.input.lock().iter().copied().collect();
		assert_eq!(remaining, b"ping");
		assert_eq!(used_len(console.mem.as_ref().expect("mem")), 0);
	}

	#[test]
	fn fill_rx_rejects_out_of_range_descriptor_without_consuming_input() {
		let mem = guest_mem();
		let bad_addr = MEM_SIZE as u64 - 8;
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			bad_addr,
			64,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);
		let mut console = console_with_rx(mem, queue, b"pong");

		assert!(console.fill_rx().expect("fill_rx"));

		let remaining: Vec<u8> = console.input.lock().iter().copied().collect();
		assert_eq!(remaining, b"pong");
		assert_eq!(used_len(console.mem.as_ref().expect("mem")), 0);
	}

	#[test]
	fn drain_tx_discards_write_only_descriptor_output() {
		let mem = guest_mem();
		mem.write_slice(b"guest", GuestAddress(0x4000))
			.expect("write guest output");
		let queue =
			queue_with_descs(&mem, &[SplitDescriptor::new(0x4000, 5, VRING_DESC_F_WRITE as u16, 0)]);
		let mut console = Console::new().expect("console");
		console.mem = Some(mem);
		console.tx_queue = Some(queue);

		assert!(console.drain_tx().expect("drain_tx"));

		assert!(console.output.lock().is_empty());
		assert_eq!(used_len(console.mem.as_ref().expect("mem")), 0);
	}

	#[test]
	fn drain_tx_stops_at_cap_and_resumes_on_drain_event() {
		let mem = guest_mem();
		mem.write_slice(b"guest", GuestAddress(0x4000))
			.expect("write guest output");
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(0x4000, 5, 0, 0)]);
		let mut console = Console::new().expect("console");
		console.mem = Some(mem);
		console.tx_queue = Some(queue);
		console.output.lock().resize(OUTPUT_CAP, b'x');

		assert!(!console.drain_tx().expect("drain_tx"));
		assert_eq!(console.output.lock().len(), OUTPUT_CAP);
		assert_eq!(used_idx(console.mem.as_ref().expect("mem")), 0);

		console.output.lock().clear();
		console.drain_resume_evt.write(1).expect("signal resume");
		console
			.process_worker_fd(console.drain_resume_evt.as_raw_fd())
			.expect("process worker fd");

		assert_eq!(&console.output.lock()[..], b"guest");
		assert_eq!(used_idx(console.mem.as_ref().expect("mem")), 1);
		assert_eq!(console.output_evt.read().expect("read output evt"), 1);
	}
}
