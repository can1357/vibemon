//! virtio core: device trait, shared interrupt, and the per-device worker.

pub mod block;
pub mod console;
#[cfg(not(target_os = "windows"))]
pub mod fs;
pub mod mmio;
pub mod net;
#[cfg(all(target_arch = "x86_64", not(target_os = "windows")))]
pub mod pci;

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(target_os = "windows")]
use std::os::windows::io::{AsRawHandle, RawHandle};
use std::{
	io,
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
	},
};

use parking_lot::Mutex;
use virtio_queue::Queue;
use vm_memory::{GuestAddress, GuestMemory};
#[cfg(target_os = "windows")]
use windows_sys::Win32::Foundation::{HANDLE, WAIT_FAILED, WAIT_OBJECT_0};
#[cfg(target_os = "windows")]
use windows_sys::Win32::System::Threading::{INFINITE, WaitForMultipleObjects};

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

#[cfg(unix)]
pub type WorkerWaitSource = RawFd;
#[cfg(target_os = "windows")]
pub type WorkerWaitSource = RawHandle;

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
	/// Extra host wait sources (besides the queue-notify event) polled by the
	/// worker.
	fn worker_wait_sources(&self) -> Vec<WorkerWaitSource> {
		Vec::new()
	}
	fn process_queue_notify(&mut self) -> Result<()>;
	/// One of [`worker_wait_sources`](VirtioDevice::worker_wait_sources) became
	/// readable.
	fn process_worker_source(&mut self, _index: usize) -> Result<()> {
		Ok(())
	}
	/// Live virtqueue state for snapshotting (read AFTER activation, when the
	/// device owns the queues). Default: no queues. Block/Net override this.
	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		Vec::new()
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
	let extra_sources = device.lock().worker_wait_sources();

	let mut wait_set = WorkerWaitSet::with_capacity(4 + extra_sources.len());
	wait_set.push(event_wait_source(&queue_evt), QUEUE_TOKEN);
	wait_set.push(event_wait_source(&kill_evt), KILL_TOKEN);
	wait_set.push(event_wait_source(&control.pause_evt), PAUSE_TOKEN);
	wait_set.push(event_wait_source(&control.resume_evt), RESUME_TOKEN);
	for (i, source) in extra_sources.iter().enumerate() {
		wait_set.push(*source, FD_TOKEN_BASE + i as u64);
	}

	// A second wait set holds only resume/kill while paused so queue_evt and
	// worker sources are NOT consumed during pause: their pending notifications
	// must survive to be serviced after resume (the guest is frozen and cannot
	// re-ring the doorbell).
	let mut pause_wait_set = WorkerWaitSet::with_capacity(2);
	pause_wait_set.push(event_wait_source(&control.resume_evt), RESUME_TOKEN);
	pause_wait_set.push(event_wait_source(&kill_evt), KILL_TOKEN);

	loop {
		let (token, _) = wait_set.wait()?;
		match token {
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
					// Service any descriptors the guest made available just before
					// we paused: the queue-notify eventfd is NOT part of the
					// snapshot, so an unprocessed kick would be lost and the guest
					// would hang on restore. Submit them...
					let _ = queue_evt.read();
					if let Err(e) = dev.process_queue_notify() {
						let message = format!("pause queue drain failed: {e}");
						*control.failure_msg.lock() = Some(message.clone());
						let _ = control.failure_evt.write(1);
						return Err(err(message));
					}
					// ...then flush all in-flight backend IO so the snapshot captures
					// a quiescent device (no half-completed requests).
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
					let (pause_token, _) = pause_wait_set.wait()?;
					match pause_token {
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
			},
			RESUME_TOKEN => {
				// Resume while not paused: harmless, just drain it.
				let _ = control.resume_evt.read();
			},
			token => {
				let idx = (token - FD_TOKEN_BASE) as usize;
				device.lock().process_worker_source(idx)?;
			},
		}
	}
}

struct WorkerWaitSet {
	#[cfg(unix)]
	poll_fds: Vec<libc::pollfd>,
	#[cfg(target_os = "windows")]
	handles:  Vec<RawHandle>,
	tokens:   Vec<u64>,
}

impl WorkerWaitSet {
	fn with_capacity(capacity: usize) -> Self {
		Self {
			#[cfg(unix)]
			poll_fds: Vec::with_capacity(capacity),
			#[cfg(target_os = "windows")]
			handles: Vec::with_capacity(capacity),
			tokens: Vec::with_capacity(capacity),
		}
	}

	fn push(&mut self, source: WorkerWaitSource, token: u64) {
		#[cfg(unix)]
		self
			.poll_fds
			.push(libc::pollfd { fd: source, events: libc::POLLIN, revents: 0 });
		#[cfg(target_os = "windows")]
		self.handles.push(source);
		self.tokens.push(token);
	}

	fn wait(&mut self) -> io::Result<(u64, usize)> {
		#[cfg(unix)]
		{
			wait_poll(&mut self.poll_fds)?;
			for idx in 0..self.poll_fds.len() {
				if take_revents(&mut self.poll_fds[idx])? {
					return Ok((self.tokens[idx], idx));
				}
			}
			Err(io::Error::other("poll returned without readable worker source"))
		}
		#[cfg(target_os = "windows")]
		{
			wait_handles(&self.handles).map(|idx| (self.tokens[idx], idx))
		}
	}
}

#[cfg(unix)]
fn event_wait_source(evt: &EventFd) -> WorkerWaitSource {
	evt.as_raw_fd()
}

#[cfg(target_os = "windows")]
fn event_wait_source(evt: &EventFd) -> WorkerWaitSource {
	evt.as_raw_handle()
}

#[cfg(unix)]
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

#[cfg(unix)]
fn take_revents(poll_fd: &mut libc::pollfd) -> io::Result<bool> {
	let revents = poll_fd.revents;
	poll_fd.revents = 0;
	if revents & libc::POLLNVAL != 0 {
		return Err(io::Error::from_raw_os_error(libc::EBADF));
	}
	Ok(revents != 0)
}

#[cfg(target_os = "windows")]
const WINDOWS_MAXIMUM_WAIT_OBJECTS: usize = 64;

#[cfg(target_os = "windows")]
fn wait_handles(handles: &[RawHandle]) -> io::Result<usize> {
	if handles.is_empty() || handles.len() > WINDOWS_MAXIMUM_WAIT_OBJECTS {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("invalid Windows worker wait set size {}", handles.len()),
		));
	}
	loop {
		// SAFETY: `handles` points to live waitable HANDLE values for the duration
		// of the call. The worker waits for any one source and never mutates this
		// slice through the OS API.
		let ret = unsafe {
			WaitForMultipleObjects(
				handles.len() as u32,
				handles.as_ptr().cast::<HANDLE>(),
				0,
				INFINITE,
			)
		};
		if ret >= WAIT_OBJECT_0 && ret < WAIT_OBJECT_0 + handles.len() as u32 {
			return Ok((ret - WAIT_OBJECT_0) as usize);
		}
		if ret == WAIT_FAILED {
			return Err(io::Error::last_os_error());
		}
		return Err(io::Error::other(format!("unexpected WaitForMultipleObjects result {ret:#x}")));
	}
}
