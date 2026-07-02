//! virtio core: device trait, shared interrupt, and the per-device worker.

pub mod block;
pub mod console;
pub mod fs;
pub mod mmio;
pub mod net;
#[cfg(target_arch = "x86_64")]
pub mod pci;
pub mod rng;

use std::{
	io,
	os::unix::io::{AsRawFd, RawFd},
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
	},
};

use parking_lot::Mutex;
use virtio_queue::Queue;
use vm_memory::{GuestAddress, GuestMemory};

use crate::{
	hv::IrqLine,
	memory::GuestMemoryMmap,
	os::EventFd,
	result::{Result, err},
};

/// virtio-mmio `InterruptStatus` bits.
pub const VIRTIO_MMIO_INT_VRING: u32 = 0x1;

pub fn descriptor_range_valid(mem: &GuestMemoryMmap, addr: GuestAddress, len: u32) -> bool {
	len == 0 || mem.check_range(addr, len as usize)
}

#[cfg(target_arch = "x86_64")]
type InterruptNotifier = Arc<dyn Fn() -> Result<bool> + Send + Sync>;

/// Shared interrupt state for one virtio device.
///
/// `status` backs the transport ISR/InterruptStatus register. `irq_line` is the
/// hypervisor-coupled guest interrupt line; PCI transports may install a
/// notifier that attempts MSI-X injection first and falls back to this line.
pub struct Interrupt {
	status:   AtomicU32,
	irq_line: IrqLine,
	#[cfg(target_arch = "x86_64")]
	notifier: Mutex<Option<InterruptNotifier>>,
}

impl Interrupt {
	/// Create interrupt state backed by the hypervisor-coupled guest IRQ line.
	pub const fn new(irq_line: IrqLine) -> Self {
		Self {
			status: AtomicU32::new(0),
			irq_line,
			#[cfg(target_arch = "x86_64")]
			notifier: Mutex::new(None),
		}
	}

	/// Read the current virtio transport ISR/InterruptStatus bits.
	pub fn status(&self) -> u32 {
		self.status.load(Ordering::SeqCst)
	}

	/// Clear the `InterruptStatus` bits the driver acked.
	pub fn ack(&self, value: u32) {
		self.status.fetch_and(!value, Ordering::SeqCst);
	}

	/// Restore the `InterruptStatus` register from a snapshot.
	pub fn restore_status(&self, value: u32) {
		self.status.store(value, Ordering::SeqCst);
	}

	/// Clear pending transport interrupt state during a device reset.
	pub fn clear(&self) {
		self.status.store(0, Ordering::SeqCst);
	}

	/// Signal "used buffer notification" and raise the transport interrupt.
	pub fn signal_used_queue(&self) -> Result<()> {
		self
			.status
			.fetch_or(VIRTIO_MMIO_INT_VRING, Ordering::SeqCst);
		#[cfg(target_arch = "x86_64")]
		if self.notify_msi() {
			return Ok(());
		}
		self.irq_line.trigger()?;
		Ok(())
	}
}

#[cfg(target_arch = "x86_64")]
impl Interrupt {
	/// Install the virtio-pci MSI-X notifier, returning true when it delivers.
	pub fn set_notifier(&self, notifier: impl Fn() -> Result<bool> + Send + Sync + 'static) {
		*self.notifier.lock() = Some(Arc::new(notifier));
	}

	/// Read and clear all pending interrupt status bits (virtio-pci ISR read).
	pub fn take_status(&self) -> u32 {
		self.status.swap(0, Ordering::SeqCst)
	}

	fn notify_msi(&self) -> bool {
		let notifier = self.notifier.lock().clone();
		notifier
			.as_ref()
			.is_some_and(|notify| notify().unwrap_or(false))
	}
}

/// A virtio device backend (block, net, ...).
///
/// The transport handles the control-plane registers and, on `DRIVER_OK`, calls
/// [`activate`](VirtioDevice::activate), handing over guest memory, the shared
/// interrupt, and the configured queues. Data-plane work then runs on the
/// device worker thread driven by [`run_worker`].
pub trait VirtioDevice: Send {
	/// virtio device type id (e.g. NET = 1, BLOCK = 2).
	fn device_type(&self) -> u32;
	/// Maximum size of each virtqueue, one entry per queue.
	fn queue_max_sizes(&self) -> &[u16];
	/// Feature bits offered to the driver.
	fn features(&self) -> u64;
	/// Record the subset of features the driver accepted.
	fn ack_features(&mut self, value: u64);
	/// Read device-specific config space.
	fn read_config(&self, offset: u64, data: &mut [u8]);
	/// Write device-specific config space.
	fn write_config(&mut self, offset: u64, data: &[u8]);
	/// Take ownership of the data-plane resources once the driver is ready.
	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()>;
	/// Drop active data-plane state after the driver resets the device.
	///
	/// Backends must stop using queues, guest memory, interrupt handles, and any
	/// in-flight state from the previous activation. Configuration that belongs
	/// to the device itself may be retained.
	fn reset(&mut self) -> Result<()> {
		Ok(())
	}
	/// Extra host fds (besides the queue-notify eventfd) to poll for readiness.
	fn worker_fds(&self) -> Vec<RawFd> {
		Vec::new()
	}
	fn process_queue_notify(&mut self) -> Result<()>;
	/// One of [`worker_fds`](VirtioDevice::worker_fds) became readable.
	fn process_worker_fd(&mut self, _fd: RawFd) -> Result<()> {
		Ok(())
	}
	/// Live virtqueue state for snapshotting (read AFTER activation, when the
	/// device owns the queues). Default: no queues. Block/Net override this.
	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		Vec::new()
	}
	/// Optional serialized backend state for user-mode networking. Non-net
	/// devices and TAP/vmnet net backends return `None`.
	fn user_net_state(&self) -> Result<Option<Vec<u8>>> {
		Ok(None)
	}
	/// Quiesce in-flight backend IO before a snapshot (e.g. drain async
	/// completions). Default: nothing to do. Block (`io_uring`) overrides this.
	fn drain(&mut self) -> Result<()> {
		Ok(())
	}
}

const QUEUE_TOKEN: u64 = 0;
const KILL_TOKEN: u64 = 1;
const PAUSE_TOKEN: u64 = 2;
const RESUME_TOKEN: u64 = 3;
const FD_TOKEN_BASE: u64 = 4;

/// Pause-coordination eventfds shared between the VMM and a device worker.
///
/// During VM pause/snapshot the VMM writes `pause_evt`; the worker quiesces and
/// writes `ack_evt` to confirm it has stopped servicing queues, or
/// `failure_evt` if quiescing fails. The VMM later writes `resume_evt` to
/// release the worker back into its normal loop.
pub struct WorkerControl {
	/// VMM writes 1 -> worker should quiesce.
	pub pause_evt:   EventFd,
	/// VMM writes 1 -> worker resumes.
	pub resume_evt:  EventFd,
	/// Worker writes 1 once quiesced (VMM waits on this).
	pub ack_evt:     EventFd,
	/// Worker writes 1 if quiescing fails; the VMM rejects the pause/snapshot.
	pub failure_evt: EventFd,
	/// Human-readable failure recorded before `failure_evt` is signalled.
	pub failure_msg: Arc<Mutex<Option<String>>>,
}

/// Event loop for a single virtio device: waits for queue notifications,
/// backend IO readiness, pause/resume control, or the kill signal, dispatching
/// into the device under lock.
pub fn run_worker(
	device: Arc<Mutex<dyn VirtioDevice>>,
	queue_evt: EventFd,
	kill_evt: EventFd,
	control: WorkerControl,
) -> Result<()> {
	let extra_fds = device.lock().worker_fds();

	let mut poll_fds = Vec::with_capacity(4 + extra_fds.len());
	let mut tokens = Vec::with_capacity(4 + extra_fds.len());
	push_poll_fd(&mut poll_fds, &mut tokens, queue_evt.as_raw_fd(), QUEUE_TOKEN);
	push_poll_fd(&mut poll_fds, &mut tokens, kill_evt.as_raw_fd(), KILL_TOKEN);
	push_poll_fd(&mut poll_fds, &mut tokens, control.pause_evt.as_raw_fd(), PAUSE_TOKEN);
	push_poll_fd(&mut poll_fds, &mut tokens, control.resume_evt.as_raw_fd(), RESUME_TOKEN);
	for (i, fd) in extra_fds.iter().enumerate() {
		push_poll_fd(&mut poll_fds, &mut tokens, *fd, FD_TOKEN_BASE + i as u64);
	}

	// A second poll set holds only resume/kill while paused so queue_evt and
	// worker fds are NOT consumed during pause: their pending notifications must
	// survive to be serviced after resume (the guest is frozen and cannot
	// re-ring the doorbell).
	let mut pause_poll_fds = Vec::with_capacity(2);
	let mut pause_tokens = Vec::with_capacity(2);
	push_poll_fd(
		&mut pause_poll_fds,
		&mut pause_tokens,
		control.resume_evt.as_raw_fd(),
		RESUME_TOKEN,
	);
	push_poll_fd(&mut pause_poll_fds, &mut pause_tokens, kill_evt.as_raw_fd(), KILL_TOKEN);

	loop {
		wait_poll(&mut poll_fds)?;
		for idx in 0..poll_fds.len() {
			if !take_revents(&mut poll_fds[idx])? {
				continue;
			}
			match tokens[idx] {
				QUEUE_TOKEN => {
					let _ = queue_evt.read();
					device.lock().process_queue_notify()?;
				},
				KILL_TOKEN => {
					let _ = kill_evt.read();
					return Ok(());
				},
				PAUSE_TOKEN => {
					let _ = control.pause_evt.read();
					{
						let mut dev = device.lock();
						// Service any descriptors the guest made available just
						// before we paused: the queue-notify eventfd is NOT part
						// of the snapshot, so an unprocessed kick would be lost
						// and the guest would hang on restore. Submit them...
						let _ = queue_evt.read();
						if let Err(e) = dev.process_queue_notify() {
							let message = format!("pause queue drain failed: {e}");
							*control.failure_msg.lock() = Some(message.clone());
							let _ = control.failure_evt.write(1);
							return Err(err(message));
						}
						// ...then flush all in-flight backend IO so the snapshot
						// captures a quiescent device (no half-completed requests).
						if let Err(e) = dev.drain() {
							let message = format!("pause drain failed: {e}");
							*control.failure_msg.lock() = Some(message.clone());
							let _ = control.failure_evt.write(1);
							return Err(err(message));
						}
					}
					// Confirm to the VMM that this worker is now quiesced.
					let _ = control.ack_evt.write(1);
					// Stay quiesced, servicing no queues, until resume (or kill).
					'paused: loop {
						wait_poll(&mut pause_poll_fds)?;
						for pause_idx in 0..pause_poll_fds.len() {
							if !take_revents(&mut pause_poll_fds[pause_idx])? {
								continue;
							}
							match pause_tokens[pause_idx] {
								RESUME_TOKEN => {
									let _ = control.resume_evt.read();
									break 'paused;
								},
								KILL_TOKEN => {
									let _ = kill_evt.read();
									return Ok(());
								},
								_ => {},
							}
						}
					}
				},
				RESUME_TOKEN => {
					// Resume while not paused: harmless, just drain it.
					let _ = control.resume_evt.read();
				},
				token => {
					let idx = (token - FD_TOKEN_BASE) as usize;
					if let Some(fd) = extra_fds.get(idx) {
						device.lock().process_worker_fd(*fd)?;
					}
				},
			}
		}
	}
}

fn push_poll_fd(poll_fds: &mut Vec<libc::pollfd>, tokens: &mut Vec<u64>, fd: RawFd, token: u64) {
	poll_fds.push(libc::pollfd { fd, events: libc::POLLIN, revents: 0 });
	tokens.push(token);
}

fn wait_poll(poll_fds: &mut [libc::pollfd]) -> io::Result<()> {
	loop {
		// SAFETY: `poll_fds` is a live mutable slice of `libc::pollfd`; its pointer is
		// non-null for the duration of the call, and the element count is passed
		// unchanged as `nfds_t`.
		let n = unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as libc::nfds_t, -1) };
		if n >= 0 {
			return Ok(());
		}
		let e = io::Error::last_os_error();
		if e.kind() != io::ErrorKind::Interrupted {
			return Err(e);
		}
	}
}

fn take_revents(poll_fd: &mut libc::pollfd) -> io::Result<bool> {
	let revents = poll_fd.revents;
	poll_fd.revents = 0;
	if revents & libc::POLLNVAL != 0 {
		return Err(io::Error::from_raw_os_error(libc::EBADF));
	}
	Ok(revents != 0)
}
