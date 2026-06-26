//! Entitlement-free user-mode networking backend for virtio-net on macOS.
//!
//! The guest still sees a virtio-net NIC, but packets are handled by libslirp
//! in userspace instead of vmnet.framework. This provides outbound NAT, DHCP,
//! and DNS without `com.apple.vm.networking`; inbound connectivity requires
//! explicit forwarding support.

use std::{
	cell::RefCell,
	collections::VecDeque,
	io,
	net::{Ipv4Addr, Ipv6Addr},
	os::fd::{AsRawFd, RawFd},
	rc::Rc,
	sync::Arc,
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use flume::{Receiver, Sender, TryRecvError, TrySendError};
use libslirp::{Context, Handler, PollEvents};
use parking_lot::Mutex;

use crate::{
	os::{EFD_NONBLOCK, EventFd},
	result::{Result, err},
	tap::VNET_HDR_SIZE,
};

const USER_NET: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 0);
const USER_MASK: Ipv4Addr = Ipv4Addr::new(255, 255, 255, 0);
const USER_HOST: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 2);
const USER_DHCP_START: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 15);
const USER_DNS: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 3);
const MAX_PENDING_RX_FRAMES: usize = 1024;
const MAX_PENDING_TX_FRAMES: usize = 1024;

/// User-mode NAT backend driven by a private libslirp thread.
pub struct UserNet {
	tx:          Sender<Command>,
	command_evt: EventFd,
	packet_evt:  EventFd,
	packets:     Arc<Mutex<VecDeque<Vec<u8>>>>,
	thread:      Option<JoinHandle<()>>,
}

enum Command {
	Packet(Vec<u8>),
	Stop,
}

type TimerFn = Rc<RefCell<Box<dyn FnMut()>>>;

struct SlirpTimer {
	func:       TimerFn,
	expires_at: Option<Instant>,
}

struct SlirpHandler {
	start:      Instant,
	packets:    Arc<Mutex<VecDeque<Vec<u8>>>>,
	packet_evt: EventFd,
	loop_evt:   EventFd,
	timers:     Vec<Option<SlirpTimer>>,
}

impl UserNet {
	/// Start an entitlement-free user-mode NAT backend.
	pub fn new(_mac: [u8; 6]) -> Result<Self> {
		let (tx, rx) = flume::bounded(MAX_PENDING_TX_FRAMES);
		let command_evt = EventFd::new(EFD_NONBLOCK)
			.map_err(|e| err(format!("creating user-net command eventfd: {e}")))?;
		let packet_evt = EventFd::new(EFD_NONBLOCK)
			.map_err(|e| err(format!("creating user-net packet eventfd: {e}")))?;
		let packets = Arc::new(Mutex::new(VecDeque::new()));

		let thread_command_evt = command_evt
			.try_clone()
			.map_err(|e| err(format!("cloning user-net command eventfd: {e}")))?;
		let handler_command_evt = command_evt
			.try_clone()
			.map_err(|e| err(format!("cloning user-net loop eventfd: {e}")))?;
		let handler_packet_evt = packet_evt
			.try_clone()
			.map_err(|e| err(format!("cloning user-net packet eventfd: {e}")))?;
		let thread_packets = packets.clone();
		let thread = thread::Builder::new()
			.name("vmon-slirp".to_string())
			.spawn(move || {
				run_slirp(
					rx,
					thread_command_evt,
					handler_command_evt,
					handler_packet_evt,
					thread_packets,
				);
			})
			.map_err(|e| err(format!("spawning user-net slirp thread: {e}")))?;

		Ok(Self { tx, command_evt, packet_evt, packets, thread: Some(thread) })
	}

	/// Read one vnet-header-prefixed Ethernet frame produced by libslirp.
	pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let _ = self.packet_evt.read();
		let Some(packet) = self.packets.lock().pop_front() else {
			return Err(io::Error::from(io::ErrorKind::WouldBlock));
		};
		let len = VNET_HDR_SIZE + packet.len();
		if buf.len() < len {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"virtio-net RX buffer is smaller than the user-net packet",
			));
		}
		buf[..VNET_HDR_SIZE].fill(0);
		buf[VNET_HDR_SIZE..len].copy_from_slice(&packet);
		Ok(len)
	}

	/// Send one vnet-header-prefixed Ethernet frame from the guest into
	/// libslirp.
	pub fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		if buf.len() < VNET_HDR_SIZE {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"virtio-net TX buffer is smaller than the vnet header",
			));
		}
		let frame = buf[VNET_HDR_SIZE..].to_vec();
		match self.tx.try_send(Command::Packet(frame)) {
			Ok(()) => {
				self.command_evt.write(1)?;
				Ok(buf.len())
			},
			Err(TrySendError::Full(_)) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
			Err(TrySendError::Disconnected(_)) => Err(io::Error::from(io::ErrorKind::BrokenPipe)),
		}
	}

	/// Reject TAP offloads; libslirp consumes plain Ethernet frames.
	pub fn set_offloads(offloads: u32) -> Result<()> {
		if offloads == 0 {
			Ok(())
		} else {
			Err(err(format!(
				"user-mode networking does not support negotiated TAP offloads {offloads:#x}"
			)))
		}
	}

	/// Return offload flags this backend can safely advertise.
	pub const fn supported_offloads() -> u32 {
		0
	}

	/// Return the readiness fd polled by the virtio worker for RX packets.
	pub fn as_raw_fd(&self) -> RawFd {
		self.packet_evt.as_raw_fd()
	}
}

impl Drop for UserNet {
	fn drop(&mut self) {
		let mut stop = Command::Stop;
		loop {
			match self.tx.try_send(stop) {
				Ok(()) => {
					let _ = self.command_evt.write(1);
					break;
				},
				Err(TrySendError::Full(cmd)) => {
					stop = cmd;
					let _ = self.command_evt.write(1);
					thread::yield_now();
				},
				Err(TrySendError::Disconnected(_)) => break,
			}
		}
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
	}
}

impl Handler for SlirpHandler {
	type Timer = usize;

	fn clock_get_ns(&mut self) -> i64 {
		let elapsed = self.start.elapsed();
		elapsed
			.as_secs()
			.saturating_mul(1_000_000_000)
			.saturating_add(u64::from(elapsed.subsec_nanos())) as i64
	}

	fn send_packet(&mut self, buf: &[u8]) -> io::Result<usize> {
		let mut packets = self.packets.lock();
		if packets.len() >= MAX_PENDING_RX_FRAMES {
			packets.pop_front();
		}
		packets.push_back(buf.to_vec());
		drop(packets);
		self.packet_evt.write(1)?;
		Ok(buf.len())
	}

	fn register_poll_fd(&mut self, _fd: RawFd) {}

	fn unregister_poll_fd(&mut self, _fd: RawFd) {}

	fn guest_error(&mut self, msg: &str) {
		tracing::warn!("libslirp guest error: {msg}");
	}

	fn notify(&mut self) {
		let _ = self.loop_evt.write(1);
	}

	fn timer_new(&mut self, func: Box<dyn FnMut()>) -> Box<Self::Timer> {
		let id = self
			.timers
			.iter()
			.position(Option::is_none)
			.unwrap_or_else(|| {
				self.timers.push(None);
				self.timers.len() - 1
			});
		self.timers[id] =
			Some(SlirpTimer { func: Rc::new(RefCell::new(func)), expires_at: None });
		Box::new(id)
	}

	fn timer_mod(&mut self, timer: &mut Box<Self::Timer>, expire_time: i64) {
		let Some(Some(timer)) = self.timers.get_mut(**timer) else {
			return;
		};
		timer.expires_at = if expire_time < 0 {
			None
		} else {
			Some(Instant::now() + Duration::from_millis(expire_time as u64))
		};
		let _ = self.loop_evt.write(1);
	}

	fn timer_free(&mut self, timer: Box<Self::Timer>) {
		if let Some(slot) = self.timers.get_mut(*timer) {
			*slot = None;
		}
	}
}

impl SlirpHandler {
	fn next_timer_timeout(&self) -> Option<Duration> {
		let now = Instant::now();
		self
			.timers
			.iter()
			.filter_map(|timer| timer.as_ref().and_then(|timer| timer.expires_at))
			.map(|expires| expires.saturating_duration_since(now))
			.min()
	}

	fn due_timers(&mut self) -> Vec<TimerFn> {
		let now = Instant::now();
		let mut callbacks = Vec::new();
		for timer in self.timers.iter_mut().flatten() {
			if timer.expires_at.is_some_and(|expires| expires <= now) {
				timer.expires_at = None;
				callbacks.push(timer.func.clone());
			}
		}
		callbacks
	}
}

fn run_slirp(
	rx: Receiver<Command>,
	command_evt: EventFd,
	handler_loop_evt: EventFd,
	packet_evt: EventFd,
	packets: Arc<Mutex<VecDeque<Vec<u8>>>>,
) {
	let handler = Rc::new(RefCell::new(SlirpHandler {
		start: Instant::now(),
		packets,
		packet_evt,
		loop_evt: handler_loop_evt,
		timers: Vec::new(),
	}));
	let context = Context::new(
		false,
		true,
		USER_NET,
		USER_MASK,
		USER_HOST,
		false,
		Ipv6Addr::UNSPECIFIED,
		0,
		Ipv6Addr::UNSPECIFIED,
		Some("vmon".to_string()),
		None,
		None,
		None,
		USER_DHCP_START,
		USER_DNS,
		Ipv6Addr::UNSPECIFIED,
		Vec::new(),
		None,
		handler.clone(),
	);

	loop {
		if !drain_commands(&context, &rx) {
			return;
		}
		fire_due_timers(&handler);

		let mut timeout = u32::MAX;
		let mut slirp_fds = Vec::new();
		context.pollfds_fill(&mut timeout, |fd, events| {
			slirp_fds.push((fd, events));
			(slirp_fds.len() - 1) as i32
		});

		let timeout = merge_timeout(timeout, handler.borrow().next_timer_timeout());
		let mut pollfds = Vec::with_capacity(1 + slirp_fds.len());
		pollfds.push(libc::pollfd {
			fd:      command_evt.as_raw_fd(),
			events:  libc::POLLIN,
			revents: 0,
		});
		for &(fd, events) in &slirp_fds {
			pollfds.push(libc::pollfd { fd, events: to_poll_events(events), revents: 0 });
		}

		// SAFETY: pollfds points to a mutable contiguous array of pollfd entries,
		// len matches that array, and timeout is the clamped poll timeout.
		let ret = unsafe { libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, timeout) };
		if ret < 0 {
			let error = io::Error::last_os_error();
			if error.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			tracing::warn!("libslirp poll failed: {error}");
			return;
		}

		if pollfds[0].revents != 0 {
			let _ = command_evt.read();
			if !drain_commands(&context, &rx) {
				return;
			}
		}

		let revents = pollfds
			.iter()
			.skip(1)
			.map(|pollfd| from_poll_events(pollfd.revents))
			.collect::<Vec<_>>();
		if !revents.is_empty() {
			context.pollfds_poll(false, |idx| {
				revents
					.get(idx as usize)
					.copied()
					.unwrap_or_else(PollEvents::empty)
			});
		}
		fire_due_timers(&handler);
	}
}

fn drain_commands(context: &Context<Rc<RefCell<SlirpHandler>>>, rx: &Receiver<Command>) -> bool {
	loop {
		match rx.try_recv() {
			Ok(Command::Packet(frame)) => context.input(&frame),
			Ok(Command::Stop) => return false,
			Err(TryRecvError::Empty) => return true,
			Err(TryRecvError::Disconnected) => return false,
		}
	}
}

fn fire_due_timers(handler: &Rc<RefCell<SlirpHandler>>) {
	let callbacks = handler.borrow_mut().due_timers();
	for callback in callbacks {
		(callback.borrow_mut())();
	}
}

fn merge_timeout(slirp_timeout: u32, timer_timeout: Option<Duration>) -> libc::c_int {
	let timer_ms = timer_timeout.map(|timeout| timeout.as_millis().min(i32::MAX as u128) as i32);
	match (slirp_timeout, timer_ms) {
		(u32::MAX, None) => -1,
		(u32::MAX, Some(ms)) => ms,
		(ms, None) => ms.min(i32::MAX as u32) as i32,
		(ms, Some(timer_ms)) => (ms.min(i32::MAX as u32) as i32).min(timer_ms),
	}
}

fn to_poll_events(events: PollEvents) -> libc::c_short {
	let mut out = 0;
	if events.has_in() {
		out |= libc::POLLIN;
	}
	if events.has_out() {
		out |= libc::POLLOUT;
	}
	if events.has_err() {
		out |= libc::POLLERR;
	}
	if events.has_hup() {
		out |= libc::POLLHUP;
	}
	out
}

fn from_poll_events(revents: libc::c_short) -> PollEvents {
	let mut out = PollEvents::empty();
	if revents & libc::POLLIN != 0 {
		out |= PollEvents::poll_in();
	}
	if revents & libc::POLLOUT != 0 {
		out |= PollEvents::poll_out();
	}
	if revents & libc::POLLERR != 0 {
		out |= PollEvents::poll_err();
	}
	if revents & libc::POLLHUP != 0 {
		out |= PollEvents::poll_hup();
	}
	out
}
