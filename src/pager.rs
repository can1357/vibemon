//! Transparent guest-RAM zram/paging support.

#[cfg(target_os = "linux")]
mod linux {
	use std::{
		collections::HashMap,
		fs::{File, OpenOptions},
		io, mem,
		os::{
			fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
			unix::fs::OpenOptionsExt,
		},
		path::{Path, PathBuf},
		ptr,
		sync::{
			Arc,
			atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
		},
	};

	use parking_lot::Mutex;
	use tracing::warn;
	use vm_memory::{Address, GuestMemory, GuestMemoryRegion};

	use crate::{
		memory::GuestMemoryMmap,
		result::{Result, err},
	};

	const UFFD_API: u64 = 0xaa;
	const UFFDIO_REGISTER_MODE_MISSING: u64 = 1;
	const UFFD_FEATURE_MISSING_SHMEM: u64 = 1 << 5;
	const UFFD_EVENT_PAGEFAULT: u8 = 0x12;
	#[allow(dead_code)]
	const UFFD_USER_MODE_ONLY: i32 = 1;
	const UFFDIO_API_IOCTL: u64 = 0xc018_aa3f;
	const UFFDIO_REGISTER_IOCTL: u64 = 0xc020_aa00;
	const UFFDIO_COPY_IOCTL: u64 = 0xc028_aa03;
	const UFFD_REGISTER_COPY_BIT: u64 = 1 << 3;

	pub(crate) const PAGE_SIZE: usize = 4096;
	const SWAP_SLOT_SIZE: usize = PAGE_SIZE + 1;
	const MAX_EVICT_PER_SWEEP: usize = 8192;
	const SHARDS: usize = 256;

	#[repr(C)]
	struct UffdioApi {
		api:      u64,
		features: u64,
		ioctls:   u64,
	}

	#[repr(C)]
	struct UffdioRange {
		start: u64,
		len:   u64,
	}

	#[repr(C)]
	struct UffdioRegister {
		range:  UffdioRange,
		mode:   u64,
		ioctls: u64,
	}

	#[repr(C)]
	struct UffdioCopy {
		dst:  u64,
		src:  u64,
		len:  u64,
		mode: u64,
		copy: i64,
	}

	#[repr(C)]
	struct UffdMsg {
		event:      u8,
		_pad:       [u8; 7],
		pf_flags:   u64,
		pf_address: u64,
		pf_feat:    u64,
	}

	struct PagerRegion {
		base:       *mut u8,
		gpa:        u64,
		len:        usize,
		memfd:      File,
		foff:       u64,
		page_start: usize,
	}

	enum Loc {
		Zero,
		Ram(Box<[u8]>),
		Swap { slot: u32, len: u32 },
	}

	struct SwapAlloc {
		fd:   OwnedFd,
		next: u32,
		free: Vec<u32>,
		cap:  u32,
	}

	impl SwapAlloc {
		fn alloc(&mut self) -> Option<u32> {
			if let Some(slot) = self.free.pop() {
				return Some(slot);
			}
			if self.next < self.cap {
				let slot = self.next;
				self.next += 1;
				Some(slot)
			} else {
				None
			}
		}

		fn free(&mut self, slot: u32) {
			self.free.push(slot);
		}

		fn used_pages(&self) -> usize {
			self.next as usize - self.free.len()
		}
	}

	pub struct Pager {
		uffd:            OwnedFd,
		stop_evt:        OwnedFd,
		regions:         Vec<PagerRegion>,
		total_pages:     usize,
		target_pages:    usize,
		store_max_bytes: usize,
		swap:            Mutex<SwapAlloc>,
		shards:          Vec<Mutex<HashMap<u32, Loc>>>,
		evicted:         Vec<AtomicU64>,
		referenced:      Vec<AtomicU64>,
		resident_pages:  AtomicUsize,
		store_bytes:     AtomicUsize,
		clock_hand:      AtomicUsize,
		registered:      AtomicBool,
		disabled:        AtomicBool,
	}

	// SAFETY: PagerRegion raw pointers point into GuestMemoryMmap mappings owned by
	// Vmm. Vmm holds that memory for at least as long as the Pager and stops the
	// handler before drop.
	unsafe impl Send for Pager {}
	unsafe impl Sync for Pager {}

	static ZERO_PAGE: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

	pub fn create_uffd() -> Result<OwnedFd> {
		let mut last_error: Option<io::Error> = None;
		for features in [UFFD_FEATURE_MISSING_SHMEM, 0] {
			let fd = match raw_uffd() {
				Ok(fd) => fd,
				Err(e) if e.raw_os_error() == Some(libc::EPERM) => {
					return Err(err(
						"userfaultfd denied: run vmon as root or set sysctl \
						 vm.unprivileged_userfaultfd=1",
					));
				},
				Err(e) => return Err(err(format!("userfaultfd: {e}"))),
			};
			let mut api = UffdioApi { api: UFFD_API, features, ioctls: 0 };
			let rc = unsafe {
				libc::ioctl(
					fd.as_raw_fd(),
					UFFDIO_API_IOCTL as libc::c_ulong,
					&mut api as *mut UffdioApi,
				)
			};
			if rc == 0 {
				return Ok(fd);
			}
			let e = io::Error::last_os_error();
			if e.raw_os_error() == Some(libc::EINVAL) {
				last_error = Some(e);
				continue;
			}
			return Err(err(format!("UFFDIO_API: {e}")));
		}
		Err(err(format!("UFFDIO_API: {}", last_error.unwrap_or_else(io::Error::last_os_error))))
	}

	fn raw_uffd() -> io::Result<OwnedFd> {
		let flags = libc::O_CLOEXEC | libc::O_NONBLOCK;
		let fd = unsafe { libc::syscall(libc::SYS_userfaultfd, flags as libc::c_int) };
		if fd < 0 {
			Err(io::Error::last_os_error())
		} else {
			Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
		}
	}

	pub fn open_swap_file(path: Option<&Path>) -> Result<OwnedFd> {
		if let Some(path) = path {
			let file = OpenOptions::new()
				.read(true)
				.write(true)
				.create(true)
				.truncate(true)
				.mode(0o600)
				.open(path)
				.map_err(|e| err(format!("opening zram swap file {path:?}: {e}")))?;
			return Ok(unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) });
		}

		let dir = std::env::var_os("TMPDIR")
			.map(PathBuf::from)
			.unwrap_or_else(|| PathBuf::from("/tmp"));
		match OpenOptions::new()
			.read(true)
			.write(true)
			.custom_flags(libc::O_TMPFILE | libc::O_CLOEXEC)
			.mode(0o600)
			.open(&dir)
		{
			Ok(file) => Ok(unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) }),
			Err(e)
				if matches!(
					e.raw_os_error(),
					Some(libc::EOPNOTSUPP) | Some(libc::EISDIR) | Some(libc::EINVAL)
				) =>
			{
				open_unlinked_swap_file(&dir)
			},
			Err(e) => Err(err(format!("creating anonymous zram swap file in {dir:?}: {e}"))),
		}
	}

	fn open_unlinked_swap_file(dir: &Path) -> Result<OwnedFd> {
		let path = dir.join(format!("vmon-swap.{}", std::process::id()));
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(true)
			.mode(0o600)
			.open(&path)
			.map_err(|e| err(format!("creating zram swap file {path:?}: {e}")))?;
		if let Err(e) = std::fs::remove_file(&path) {
			return Err(err(format!("unlinking zram swap file {path:?}: {e}")));
		}
		Ok(unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) })
	}

	pub(crate) fn register_missing(uffd: RawFd, start: u64, len: u64) -> Result<()> {
		let mut reg = UffdioRegister {
			range:  UffdioRange { start, len },
			mode:   UFFDIO_REGISTER_MODE_MISSING,
			ioctls: 0,
		};
		let rc = unsafe {
			libc::ioctl(uffd, UFFDIO_REGISTER_IOCTL as libc::c_ulong, &mut reg as *mut UffdioRegister)
		};
		if rc < 0 {
			return Err(err(format!("UFFDIO_REGISTER: {}", io::Error::last_os_error())));
		}
		if reg.ioctls & UFFD_REGISTER_COPY_BIT == 0 {
			return Err(err("kernel uffd lacks copy support for registered range"));
		}
		Ok(())
	}

	fn uffd_copy(uffd: RawFd, dst_page_va: u64, src: *const u8, len: usize) -> io::Result<()> {
		let mut copy =
			UffdioCopy { dst: dst_page_va, src: src as u64, len: len as u64, mode: 0, copy: 0 };
		let rc = unsafe {
			libc::ioctl(uffd, UFFDIO_COPY_IOCTL as libc::c_ulong, &mut copy as *mut UffdioCopy)
		};
		if rc < 0 {
			return Err(io::Error::last_os_error());
		}
		if copy.copy < 0 {
			return Err(io::Error::from_raw_os_error((-copy.copy) as i32));
		}
		Ok(())
	}

	fn encode(page: &[u8; PAGE_SIZE]) -> Option<Vec<u8>> {
		if page.iter().all(|b| *b == 0) {
			return None;
		}
		let compressed = miniz_oxide::deflate::compress_to_vec(page, 6);
		if compressed.len() < PAGE_SIZE {
			let mut out = Vec::with_capacity(compressed.len() + 1);
			out.push(2);
			out.extend_from_slice(&compressed);
			Some(out)
		} else {
			let mut out = Vec::with_capacity(PAGE_SIZE + 1);
			out.push(1);
			out.extend_from_slice(page);
			Some(out)
		}
	}

	fn decode(buf: &[u8], out: &mut [u8; PAGE_SIZE]) -> Result<()> {
		let Some((&tag, payload)) = buf.split_first() else {
			return Err(err("empty pager page blob"));
		};
		match tag {
			1 => {
				if payload.len() != PAGE_SIZE {
					return Err(err(format!(
						"raw pager page has {} bytes, expected {PAGE_SIZE}",
						payload.len()
					)));
				}
				out.copy_from_slice(payload);
			},
			2 => {
				let decoded = miniz_oxide::inflate::decompress_to_vec_with_limit(payload, PAGE_SIZE)
					.map_err(|e| err(format!("inflating pager page: {e:?}")))?;
				if decoded.len() != PAGE_SIZE {
					return Err(err(format!(
						"inflated pager page has {} bytes, expected {PAGE_SIZE}",
						decoded.len()
					)));
				}
				out.copy_from_slice(&decoded);
			},
			other => return Err(err(format!("invalid pager page tag {other}"))),
		}
		Ok(())
	}

	impl Pager {
		pub fn new(
			uffd: OwnedFd,
			swap_fd: OwnedFd,
			mem: &GuestMemoryMmap,
			target_pages: usize,
			store_max_bytes: usize,
		) -> Result<Arc<Pager>> {
			let mut regions = Vec::new();
			let mut total_pages = 0usize;
			for region in mem.iter() {
				let len = usize::try_from(region.len())
					.map_err(|_| err("pager region length exceeds usize"))?;
				if len == 0 {
					return Err(err("pager memory region is empty"));
				}
				if !len.is_multiple_of(PAGE_SIZE) {
					return Err(err(format!("pager region length {len} is not page-aligned")));
				}
				let fo = region
					.file_offset()
					.ok_or_else(|| err("pager requires file-backed guest memory"))?;
				let memfd = fo
					.file()
					.try_clone()
					.map_err(|e| err(format!("cloning guest memory fd for pager: {e}")))?;
				regions.push(PagerRegion {
					base: region.as_ptr(),
					gpa: region.start_addr().raw_value(),
					len,
					memfd,
					foff: fo.start(),
					page_start: total_pages,
				});
				total_pages = total_pages
					.checked_add(len / PAGE_SIZE)
					.ok_or_else(|| err("pager page count overflow"))?;
			}
			if total_pages == 0 {
				return Err(err("pager guest memory has no pages"));
			}
			let cap = u32::try_from(total_pages)
				.map_err(|_| err("pager supports at most u32::MAX guest pages"))?;
			let stop_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
			if stop_fd < 0 {
				return Err(io::Error::last_os_error().into());
			}
			let words = total_pages.div_ceil(64);
			let mut shards = Vec::with_capacity(SHARDS);
			for _ in 0..SHARDS {
				shards.push(Mutex::new(HashMap::new()));
			}
			Ok(Arc::new(Pager {
				uffd,
				stop_evt: unsafe { OwnedFd::from_raw_fd(stop_fd) },
				regions,
				total_pages,
				target_pages,
				store_max_bytes,
				swap: Mutex::new(SwapAlloc { fd: swap_fd, next: 0, free: Vec::new(), cap }),
				shards,
				evicted: (0..words).map(|_| AtomicU64::new(0)).collect(),
				referenced: (0..words).map(|_| AtomicU64::new(0)).collect(),
				resident_pages: AtomicUsize::new(total_pages),
				store_bytes: AtomicUsize::new(0),
				clock_hand: AtomicUsize::new(0),
				registered: AtomicBool::new(false),
				disabled: AtomicBool::new(false),
			}))
		}

		pub fn handler_loop(self: Arc<Pager>) {
			let uffd = self.uffd.as_raw_fd();
			let stop = self.stop_evt.as_raw_fd();
			loop {
				let mut fds =
					[libc::pollfd { fd: uffd, events: libc::POLLIN, revents: 0 }, libc::pollfd {
						fd:      stop,
						events:  libc::POLLIN,
						revents: 0,
					}];
				let rc = unsafe {
					libc::ppoll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, ptr::null(), ptr::null())
				};
				if rc < 0 {
					let e = io::Error::last_os_error();
					if e.raw_os_error() == Some(libc::EINTR) {
						continue;
					}
					warn!("pager ppoll failed: {e}");
					return;
				}
				if fds[1].revents & libc::POLLIN != 0 {
					drain_eventfd(stop);
					return;
				}
				if fds[0].revents & libc::POLLIN == 0 {
					continue;
				}
				loop {
					let mut msg = mem::MaybeUninit::<UffdMsg>::zeroed();
					let n = unsafe {
						libc::read(uffd, msg.as_mut_ptr() as *mut libc::c_void, mem::size_of::<UffdMsg>())
					};
					if n < 0 {
						let e = io::Error::last_os_error();
						if e.raw_os_error() == Some(libc::EAGAIN) {
							break;
						}
						warn!("pager uffd read failed: {e}");
						break;
					}
					if n != mem::size_of::<UffdMsg>() as isize {
						warn!("pager uffd short read: {n}");
						break;
					}
					let msg = unsafe { msg.assume_init() };
					if msg.event == UFFD_EVENT_PAGEFAULT {
						self.serve(msg.pf_address);
					}
				}
			}
		}

		fn serve(&self, addr: u64) {
			let page_va = addr & !((PAGE_SIZE as u64) - 1);
			let Some(idx) = self.page_index_for_va(page_va as usize) else {
				self.disable_once(format!("pager fault outside guest RAM at {page_va:#x}"));
				return;
			};
			let shard_idx = idx % SHARDS;
			let mut guard = self.shards[shard_idx].lock();
			if !self.bit_is_set(&self.evicted, idx) {
				drop(guard);
				if let Err(e) = uffd_copy(self.uffd.as_raw_fd(), page_va, ZERO_PAGE.as_ptr(), PAGE_SIZE)
				{
					self.disable_once(format!("zero-fill UFFDIO_COPY failed: {e}"));
					return;
				}
				self.set_bit(&self.referenced, idx);
				crate::metrics::record_pager_fault_in();
				return;
			}

			let Some(loc) = guard.remove(&(idx as u32)) else {
				drop(guard);
				self.disable_once(format!("pager missing backing blob for page {idx}"));
				let _ = uffd_copy(self.uffd.as_raw_fd(), page_va, ZERO_PAGE.as_ptr(), PAGE_SIZE);
				return;
			};
			drop(guard);

			let mut tmp = [0u8; PAGE_SIZE];
			let src: *const u8 = match loc {
				Loc::Zero => ZERO_PAGE.as_ptr(),
				Loc::Ram(buf) => {
					if let Err(e) = decode(&buf, &mut tmp) {
						self.disable_once(format!("decoding pager RAM page {idx}: {e}"));
						ZERO_PAGE.as_ptr()
					} else {
						self.store_bytes.fetch_sub(buf.len(), Ordering::SeqCst);
						tmp.as_ptr()
					}
				},
				Loc::Swap { slot, len } => {
					let mut sbuf = vec![0u8; len as usize];
					let fd = self.swap.lock().fd.as_raw_fd();
					if let Err(e) = pread_exact(fd, &mut sbuf, swap_offset(slot)) {
						self.disable_once(format!("reading pager swap slot {slot}: {e}"));
						ZERO_PAGE.as_ptr()
					} else if let Err(e) = decode(&sbuf, &mut tmp) {
						self.disable_once(format!("decoding pager swap slot {slot}: {e}"));
						ZERO_PAGE.as_ptr()
					} else {
						self.swap.lock().free(slot);
						tmp.as_ptr()
					}
				},
			};

			if let Err(e) = uffd_copy(self.uffd.as_raw_fd(), page_va, src, PAGE_SIZE) {
				self.disable_once(format!("fault-in UFFDIO_COPY failed: {e}"));
				return;
			}
			self.clear_bit(&self.evicted, idx);
			self.set_bit(&self.referenced, idx);
			self.resident_pages.fetch_add(1, Ordering::SeqCst);
			crate::metrics::record_pager_fault_in();
			self.publish_gauges();
		}

		pub fn evict_to_target(&self) {
			if self.disabled.load(Ordering::SeqCst) {
				return;
			}
			if !self.registered.load(Ordering::SeqCst) {
				for region in &self.regions {
					if let Err(e) =
						register_missing(self.uffd.as_raw_fd(), region.base as u64, region.len as u64)
					{
						self.disable_once(e.to_string());
						return;
					}
				}
				self.registered.store(true, Ordering::SeqCst);
			}

			let mut budget = MAX_EVICT_PER_SWEEP;
			while self.resident_pages.load(Ordering::SeqCst) > self.target_pages && budget > 0 {
				let Some(idx) = self.next_victim() else {
					break;
				};
				self.evict_page(idx);
				budget -= 1;
				if self.disabled.load(Ordering::SeqCst) {
					break;
				}
			}
			self.publish_gauges();
		}

		pub fn over_target(&self) -> bool {
			!self.disabled.load(Ordering::SeqCst)
				&& self.resident_pages.load(Ordering::SeqCst) > self.target_pages
		}

		pub fn request_stop(&self) {
			let one = 1u64.to_ne_bytes();
			let _ = unsafe {
				libc::write(self.stop_evt.as_raw_fd(), one.as_ptr() as *const libc::c_void, one.len())
			};
		}

		fn next_victim(&self) -> Option<usize> {
			if self.total_pages == 0 {
				return None;
			}
			let start = self.clock_hand.load(Ordering::SeqCst) % self.total_pages;
			let mut first_resident = None;
			for step in 0..self.total_pages {
				let idx = (start + step) % self.total_pages;
				if self.bit_is_set(&self.evicted, idx) {
					continue;
				}
				if first_resident.is_none() {
					first_resident = Some(idx);
				}
				if self.bit_is_set(&self.referenced, idx) {
					self.clear_bit(&self.referenced, idx);
					continue;
				}
				self
					.clock_hand
					.store((idx + 1) % self.total_pages, Ordering::SeqCst);
				return Some(idx);
			}
			if let Some(idx) = first_resident {
				self
					.clock_hand
					.store((idx + 1) % self.total_pages, Ordering::SeqCst);
			}
			first_resident
		}

		fn evict_page(&self, idx: usize) {
			if self.bit_is_set(&self.evicted, idx) {
				return;
			}
			let Some((region_i, page_in_region)) = self.region_for_page(idx) else {
				self.disable_once(format!("pager page index {idx} outside regions"));
				return;
			};
			let region = &self.regions[region_i];
			let off = page_in_region * PAGE_SIZE;
			let mut page = [0u8; PAGE_SIZE];
			unsafe {
				ptr::copy_nonoverlapping(region.base.add(off), page.as_mut_ptr(), PAGE_SIZE);
			}
			let encoded = encode(&page);

			let shard_idx = idx % SHARDS;
			let mut guard = self.shards[shard_idx].lock();
			if self.bit_is_set(&self.evicted, idx) {
				return;
			}

			let loc = match encoded {
				None => Loc::Zero,
				Some(buf) => self.place_encoded(buf),
			};

			let rc = unsafe {
				libc::fallocate(
					region.memfd.as_raw_fd(),
					libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
					(region.foff + off as u64) as libc::off_t,
					PAGE_SIZE as libc::off_t,
				)
			};
			if rc < 0 {
				self.release_loc(loc);
				self.disable_once(format!(
					"punching guest RAM page {idx} (gpa {:#x}): {}",
					region.gpa + off as u64,
					io::Error::last_os_error()
				));
				return;
			}
			let rc = unsafe {
				libc::madvise(region.base.add(off) as *mut libc::c_void, PAGE_SIZE, libc::MADV_DONTNEED)
			};
			if rc < 0 {
				self.disable_once(format!(
					"dropping guest RAM PTE for page {idx} (gpa {:#x}): {}",
					region.gpa + off as u64,
					io::Error::last_os_error()
				));
			}

			guard.insert(idx as u32, loc);
			self.set_bit(&self.evicted, idx);
			self.clear_bit(&self.referenced, idx);
			self.resident_pages.fetch_sub(1, Ordering::SeqCst);
			crate::metrics::record_pager_eviction();
		}

		fn place_encoded(&self, buf: Vec<u8>) -> Loc {
			if self.store_bytes.load(Ordering::SeqCst) + buf.len() <= self.store_max_bytes {
				self.store_bytes.fetch_add(buf.len(), Ordering::SeqCst);
				return Loc::Ram(buf.into_boxed_slice());
			}

			let mut swap = self.swap.lock();
			if let Some(slot) = swap.alloc() {
				match pwrite_all(swap.fd.as_raw_fd(), &buf, swap_offset(slot)) {
					Ok(()) => Loc::Swap { slot, len: buf.len() as u32 },
					Err(e) => {
						warn!("writing pager swap slot {slot} failed; keeping page in RAM: {e}");
						swap.free(slot);
						drop(swap);
						self.store_bytes.fetch_add(buf.len(), Ordering::SeqCst);
						Loc::Ram(buf.into_boxed_slice())
					},
				}
			} else {
				drop(swap);
				self.store_bytes.fetch_add(buf.len(), Ordering::SeqCst);
				Loc::Ram(buf.into_boxed_slice())
			}
		}

		fn release_loc(&self, loc: Loc) {
			match loc {
				Loc::Zero => {},
				Loc::Ram(buf) => {
					self.store_bytes.fetch_sub(buf.len(), Ordering::SeqCst);
				},
				Loc::Swap { slot, .. } => self.swap.lock().free(slot),
			}
		}

		fn page_index_for_va(&self, page_va: usize) -> Option<usize> {
			for region in &self.regions {
				let start = region.base as usize;
				let end = start.checked_add(region.len)?;
				if (start..end).contains(&page_va) {
					return Some(region.page_start + (page_va - start) / PAGE_SIZE);
				}
			}
			None
		}

		fn region_for_page(&self, idx: usize) -> Option<(usize, usize)> {
			for (i, region) in self.regions.iter().enumerate() {
				let pages = region.len / PAGE_SIZE;
				if idx >= region.page_start && idx < region.page_start + pages {
					return Some((i, idx - region.page_start));
				}
			}
			None
		}

		fn bit_is_set(&self, bits: &[AtomicU64], idx: usize) -> bool {
			let word = idx / 64;
			let bit = 1u64 << (idx % 64);
			bits[word].load(Ordering::SeqCst) & bit != 0
		}

		fn set_bit(&self, bits: &[AtomicU64], idx: usize) {
			let word = idx / 64;
			let bit = 1u64 << (idx % 64);
			bits[word].fetch_or(bit, Ordering::SeqCst);
		}

		fn clear_bit(&self, bits: &[AtomicU64], idx: usize) {
			let word = idx / 64;
			let bit = !(1u64 << (idx % 64));
			bits[word].fetch_and(bit, Ordering::SeqCst);
		}

		fn disable_once(&self, message: String) {
			if !self.disabled.swap(true, Ordering::SeqCst) {
				warn!("disabling pager: {message}");
			}
		}

		fn publish_gauges(&self) {
			crate::metrics::set_pager_gauges(
				self.resident_pages.load(Ordering::SeqCst),
				self.store_bytes.load(Ordering::SeqCst),
				self.swap.lock().used_pages(),
			);
		}

		#[cfg(test)]
		fn register_for_test(&self) -> Result<()> {
			if !self.registered.load(Ordering::SeqCst) {
				for region in &self.regions {
					register_missing(self.uffd.as_raw_fd(), region.base as u64, region.len as u64)?;
				}
				self.registered.store(true, Ordering::SeqCst);
			}
			Ok(())
		}
	}

	fn swap_offset(slot: u32) -> i64 {
		i64::from(slot) * SWAP_SLOT_SIZE as i64
	}

	fn drain_eventfd(fd: RawFd) {
		let mut buf = [0u8; 8];
		let _ = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
	}

	fn pread_exact(fd: RawFd, mut buf: &mut [u8], mut offset: i64) -> io::Result<()> {
		while !buf.is_empty() {
			let n = unsafe {
				libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), offset as libc::off_t)
			};
			if n < 0 {
				let e = io::Error::last_os_error();
				if e.raw_os_error() == Some(libc::EINTR) {
					continue;
				}
				return Err(e);
			}
			if n == 0 {
				return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read from pager swap"));
			}
			let n = n as usize;
			let tmp = buf;
			buf = &mut tmp[n..];
			offset += n as i64;
		}
		Ok(())
	}

	fn pwrite_all(fd: RawFd, mut buf: &[u8], mut offset: i64) -> io::Result<()> {
		while !buf.is_empty() {
			let n = unsafe {
				libc::pwrite(fd, buf.as_ptr() as *const libc::c_void, buf.len(), offset as libc::off_t)
			};
			if n < 0 {
				let e = io::Error::last_os_error();
				if e.raw_os_error() == Some(libc::EINTR) {
					continue;
				}
				return Err(e);
			}
			if n == 0 {
				return Err(io::Error::new(io::ErrorKind::WriteZero, "short write to pager swap"));
			}
			let n = n as usize;
			buf = &buf[n..];
			offset += n as i64;
		}
		Ok(())
	}

	#[cfg(test)]
	mod tests {
		use std::{
			thread::{self, JoinHandle},
			time::{Duration, Instant},
		};

		use super::*;

		struct Fixture {
			mem:     GuestMemoryMmap,
			pager:   Arc<Pager>,
			handler: Option<JoinHandle<()>>,
		}

		impl Fixture {
			fn new(target_pages: usize, store_max_bytes: usize) -> Option<Self> {
				let uffd = match create_uffd() {
					Ok(uffd) => uffd,
					Err(e) if e.to_string().contains("userfaultfd denied") => {
						eprintln!("skipping pager userfaultfd test: {e}");
						return None;
					},
					Err(e) => panic!("create userfaultfd for pager test: {e}"),
				};
				let swap = open_swap_file(None).expect("open swap file");
				let mem = crate::memory::create_guest_memory(2 << 20).expect("guest memory");
				let pager =
					Pager::new(uffd, swap, &mem, target_pages, store_max_bytes).expect("create pager");
				pager.register_for_test().expect("register missing faults");
				let p = pager.clone();
				let handler = thread::Builder::new()
					.name("pager-test".into())
					.spawn(move || p.handler_loop())
					.expect("spawn pager handler");
				Some(Self { mem, pager, handler: Some(handler) })
			}

			fn page_ptr(&self, page: usize) -> *mut u8 {
				self
					.mem
					.iter()
					.next()
					.expect("memory region")
					.as_ptr()
					.wrapping_add(page * PAGE_SIZE)
			}

			fn write_page(&self, page: usize, bytes: &[u8; PAGE_SIZE]) {
				unsafe {
					ptr::copy_nonoverlapping(bytes.as_ptr(), self.page_ptr(page), PAGE_SIZE);
				}
			}

			fn read_page(&self, page: usize) -> [u8; PAGE_SIZE] {
				let mut out = [0u8; PAGE_SIZE];
				unsafe {
					ptr::copy_nonoverlapping(self.page_ptr(page), out.as_mut_ptr(), PAGE_SIZE);
				}
				out
			}

			fn wait_resident(&self, page: usize) {
				let deadline = Instant::now() + Duration::from_secs(1);
				while self.pager.bit_is_set(&self.pager.evicted, page) && Instant::now() < deadline {
					thread::sleep(Duration::from_millis(1));
				}
				assert!(!self.pager.bit_is_set(&self.pager.evicted, page));
			}
		}

		impl Drop for Fixture {
			fn drop(&mut self) {
				self.pager.request_stop();
				if let Some(handler) = self.handler.take() {
					let _ = handler.join();
				}
			}
		}

		fn noisy_page() -> [u8; PAGE_SIZE] {
			let mut noisy = [0u8; PAGE_SIZE];
			let mut x = 0x1234_5678u64;
			for chunk in noisy.chunks_mut(8) {
				x ^= x << 13;
				x ^= x >> 7;
				x ^= x << 17;
				chunk.copy_from_slice(&x.to_ne_bytes());
			}
			noisy
		}

		#[test]
		fn encode_decode_roundtrip_variants() {
			let zero = [0u8; PAGE_SIZE];
			assert!(encode(&zero).is_none());

			let repeated = [0xabu8; PAGE_SIZE];
			let compressed = encode(&repeated).expect("compressed blob");
			assert_eq!(compressed[0], 2);
			let mut out = [0u8; PAGE_SIZE];
			decode(&compressed, &mut out).expect("decode compressed");
			assert_eq!(out, repeated);

			let noisy = noisy_page();
			let raw = encode(&noisy).expect("raw blob");
			assert_eq!(raw[0], 1);
			decode(&raw, &mut out).expect("decode raw");
			assert_eq!(out, noisy);
		}

		#[test]
		fn evict_faults_in_compressible_page() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			let page = [0xabu8; PAGE_SIZE];
			f.write_page(0, &page);
			f.pager.evict_page(0);
			assert!(f.pager.bit_is_set(&f.pager.evicted, 0));
			assert_eq!(f.read_page(0), page);
			f.wait_resident(0);
		}

		#[test]
		fn evict_faults_in_zero_page() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			f.pager.evict_page(0);
			assert!(f.pager.bit_is_set(&f.pager.evicted, 0));
			assert_eq!(f.read_page(0), [0u8; PAGE_SIZE]);
			f.wait_resident(0);
		}

		#[test]
		fn evict_faults_in_incompressible_page() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			let page = noisy_page();
			f.write_page(0, &page);
			f.pager.evict_page(0);
			assert!(f.pager.bit_is_set(&f.pager.evicted, 0));
			assert_eq!(f.read_page(0), page);
			f.wait_resident(0);
		}

		#[test]
		fn evict_faults_in_swap_spill_page() {
			let Some(f) = Fixture::new(511, 0) else {
				return;
			};
			let page = noisy_page();
			f.write_page(0, &page);
			f.pager.evict_page(0);
			assert!(f.pager.swap.lock().used_pages() > 0);
			assert_eq!(f.read_page(0), page);
			f.wait_resident(0);
		}

		#[test]
		fn evict_to_target_prefers_unreferenced_pages() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			let page0 = [0x11u8; PAGE_SIZE];
			let page1 = [0x22u8; PAGE_SIZE];
			f.write_page(0, &page0);
			f.write_page(1, &page1);
			f.pager.set_bit(&f.pager.referenced, 0);
			f.pager.clear_bit(&f.pager.referenced, 1);
			f.pager.evict_to_target();
			assert!(!f.pager.bit_is_set(&f.pager.evicted, 0));
			assert!(f.pager.bit_is_set(&f.pager.evicted, 1));
			assert_eq!(f.pager.resident_pages.load(Ordering::SeqCst), 511);
		}
	}
}

#[cfg(target_os = "linux")]
pub use linux::{Pager, create_uffd, open_swap_file};

#[cfg(not(target_os = "linux"))]
mod non_linux {
	use std::{path::Path, sync::Arc};

	use crate::{
		memory::GuestMemoryMmap,
		result::{Result, err},
	};

	pub struct Pager;

	pub fn create_uffd() -> Result<std::os::fd::OwnedFd> {
		Err(err("pager requires Linux"))
	}

	pub fn open_swap_file(_path: Option<&Path>) -> Result<std::os::fd::OwnedFd> {
		Err(err("pager requires Linux"))
	}

	impl Pager {
		pub fn new(
			_uffd: std::os::fd::OwnedFd,
			_swap_fd: std::os::fd::OwnedFd,
			_mem: &GuestMemoryMmap,
			_target_pages: usize,
			_store_max_bytes: usize,
		) -> Result<Arc<Self>> {
			Err(err("pager requires Linux"))
		}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub fn handler_loop(self: Arc<Self>) {}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub const fn evict_to_target(&self) {}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub const fn over_target(&self) -> bool {
			false
		}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub const fn request_stop(&self) {}
	}
}

#[cfg(not(target_os = "linux"))]
pub use non_linux::{Pager, create_uffd, open_swap_file};
