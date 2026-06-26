//! PSCI emulation shared by all HVF vCPUs.

use std::{
	collections::HashMap,
	sync::{
		Condvar, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use crate::result::{Result, err};

/// PSCI 1.0 version returned to the guest.
pub const PSCI_VERSION_1_0: u64 = 0x0001_0000;
/// `PSCI_VERSION` function ID.
pub const PSCI_VERSION: u64 = 0x8400_0000;
/// `PSCI_CPU_OFF` function ID.
pub const PSCI_CPU_OFF: u64 = 0x8400_0002;
/// `PSCI_CPU_ON` 32-bit function ID.
pub const PSCI_CPU_ON_32: u64 = 0x8400_0003;
/// `PSCI_AFFINITY_INFO` 32-bit function ID.
pub const PSCI_AFFINITY_INFO_32: u64 = 0x8400_0004;
/// `PSCI_MIGRATE_INFO_TYPE` function ID.
pub const PSCI_MIGRATE_INFO_TYPE: u64 = 0x8400_0006;
/// `PSCI_SYSTEM_OFF` function ID.
pub const PSCI_SYSTEM_OFF: u64 = 0x8400_0008;
/// `PSCI_SYSTEM_RESET` function ID.
pub const PSCI_SYSTEM_RESET: u64 = 0x8400_0009;
/// `PSCI_FEATURES` function ID.
pub const PSCI_FEATURES: u64 = 0x8400_000a;
/// `PSCI_CPU_ON` 64-bit function ID.
pub const PSCI_CPU_ON_64: u64 = 0xc400_0003;
/// `PSCI_AFFINITY_INFO` 64-bit function ID.
pub const PSCI_AFFINITY_INFO_64: u64 = 0xc400_0004;

/// PSCI SUCCESS return code.
pub const PSCI_SUCCESS: i64 = 0;
/// PSCI `NOT_SUPPORTED` return code.
pub const PSCI_NOT_SUPPORTED: i64 = -1;
/// PSCI `INVALID_PARAMETERS` return code.
pub const PSCI_INVALID_PARAMETERS: i64 = -2;
/// PSCI `ALREADY_ON` return code.
pub const PSCI_ALREADY_ON: i64 = -4;
/// PSCI `ON_PENDING` return code.
pub const PSCI_ON_PENDING: i64 = -5;
/// PSCI `INTERNAL_FAILURE` return code.
pub const PSCI_INTERNAL_FAILURE: i64 = -6;

/// PSCI `AFFINITY_INFO` reports a powered-on target.
pub const PSCI_AFFINITY_ON: u64 = 0;
/// PSCI `AFFINITY_INFO` reports a powered-off target.
pub const PSCI_AFFINITY_OFF: u64 = 1;
/// PSCI `AFFINITY_INFO` reports a target currently transitioning to on.
pub const PSCI_AFFINITY_ON_PENDING: u64 = 2;

const CPU_ON_ENTRY_ALIGN: u64 = 4;

/// Boot parameters delivered to a secondary vCPU after PSCI `CPU_ON`.
#[derive(Clone, Copy, Debug)]
pub struct StartInfo {
	/// Guest physical entry point for the target vCPU.
	pub entry:      u64,
	/// Context ID to place in X0 when the target starts.
	pub context_id: u64,
}

#[derive(Default)]
struct StartSlot {
	pending: Option<StartInfo>,
}

/// Coordinates PSCI power state and `CPU_ON` delivery for HVF vCPUs.
pub struct PsciCoordinator {
	mpidr_to_id: Mutex<HashMap<u64, usize>>,
	powered:     Vec<AtomicBool>,
	starts:      Vec<(Mutex<StartSlot>, Condvar)>,
}

impl PsciCoordinator {
	/// Create a coordinator sized for the host's maximum vCPU count.
	pub fn new(max_vcpus: usize) -> Self {
		let powered = (0..max_vcpus).map(|_| AtomicBool::new(false)).collect();
		let starts = (0..max_vcpus)
			.map(|_| (Mutex::new(StartSlot::default()), Condvar::new()))
			.collect();
		Self { mpidr_to_id: Mutex::new(HashMap::new()), powered, starts }
	}

	/// Register the MPIDR affinity of a vCPU index.
	pub fn register_mpidr(&self, vcpu_id: usize, mpidr: u64) -> Result<()> {
		self.check_id(vcpu_id)?;
		let affinity = mpidr_affinity(mpidr);
		self
			.mpidr_to_id
			.lock()
			.map_err(|_| err("PSCI MPIDR map poisoned"))?
			.insert(affinity, vcpu_id);
		Ok(())
	}

	/// Mark a vCPU as initially running.
	pub fn mark_booted(&self, vcpu_id: usize) -> Result<()> {
		self.check_id(vcpu_id)?;
		self.clear_start(vcpu_id)?;
		self.powered[vcpu_id].store(true, Ordering::Release);
		Ok(())
	}

	/// Mark a vCPU as initially powered off.
	pub fn mark_off(&self, vcpu_id: usize) -> Result<()> {
		self.check_id(vcpu_id)?;
		self.clear_start(vcpu_id)?;
		self.powered[vcpu_id].store(false, Ordering::Release);
		Ok(())
	}

	/// Power on the target MPIDR and wake its vCPU thread with entry/context.
	pub fn power_on(&self, target_affinity: u64, entry: u64, context_id: u64) -> i64 {
		if !entry.is_multiple_of(CPU_ON_ENTRY_ALIGN) {
			return PSCI_INVALID_PARAMETERS;
		}
		let target = mpidr_affinity(target_affinity);
		let Ok(vcpu_id) = self
			.mpidr_to_id
			.lock()
			.map_err(|_| ())
			.and_then(|m| m.get(&target).copied().ok_or(()))
		else {
			return PSCI_INVALID_PARAMETERS;
		};
		if self.powered[vcpu_id].load(Ordering::Acquire) {
			return PSCI_ALREADY_ON;
		}
		let (lock, cv) = &self.starts[vcpu_id];
		let Ok(mut slot) = lock.lock() else {
			return PSCI_INTERNAL_FAILURE;
		};
		if slot.pending.is_some() {
			return PSCI_ON_PENDING;
		}
		slot.pending = Some(StartInfo { entry, context_id });
		cv.notify_one();
		PSCI_SUCCESS
	}

	/// Return the PSCI affinity state for a target MPIDR.
	pub fn affinity_info(&self, target_affinity: u64) -> u64 {
		let target = mpidr_affinity(target_affinity);
		let Some(vcpu_id) = self
			.mpidr_to_id
			.lock()
			.ok()
			.and_then(|m| m.get(&target).copied())
		else {
			return PSCI_AFFINITY_OFF;
		};

		if self.is_powered(vcpu_id) {
			return PSCI_AFFINITY_ON;
		}

		let (lock, _) = &self.starts[vcpu_id];
		match lock.lock() {
			Ok(slot) if slot.pending.is_some() => PSCI_AFFINITY_ON_PENDING,
			Ok(_) if self.is_powered(vcpu_id) => PSCI_AFFINITY_ON,
			_ => PSCI_AFFINITY_OFF,
		}
	}

	/// Return whether a vCPU index is currently powered on.
	pub fn is_powered(&self, vcpu_id: usize) -> bool {
		self
			.powered
			.get(vcpu_id)
			.is_some_and(|p| p.load(Ordering::Acquire))
	}

	/// Wait up to `timeout` for PSCI `CPU_ON` start parameters for this vCPU.
	pub fn wait_for_power_on_timeout(
		&self,
		vcpu_id: usize,
		timeout: Duration,
	) -> Result<Option<StartInfo>> {
		self.check_id(vcpu_id)?;
		let (lock, cv) = &self.starts[vcpu_id];
		let mut slot = lock.lock().map_err(|_| err("PSCI start slot poisoned"))?;
		if let Some(info) = slot.pending.take() {
			self.powered[vcpu_id].store(true, Ordering::Release);
			return Ok(Some(info));
		}
		let (mut slot, _) = cv
			.wait_timeout(slot, timeout)
			.map_err(|_| err("PSCI start condvar poisoned"))?;
		if let Some(info) = slot.pending.take() {
			self.powered[vcpu_id].store(true, Ordering::Release);
			Ok(Some(info))
		} else {
			Ok(None)
		}
	}

	/// Mark a vCPU powered off after PSCI `CPU_OFF`.
	pub fn power_off(&self, vcpu_id: usize) -> Result<()> {
		self.mark_off(vcpu_id)
	}

	fn check_id(&self, vcpu_id: usize) -> Result<()> {
		if vcpu_id >= self.powered.len() {
			return Err(err(format!("vCPU id {vcpu_id} exceeds PSCI coordinator size")));
		}
		Ok(())
	}

	fn clear_start(&self, vcpu_id: usize) -> Result<()> {
		let (lock, _) = &self.starts[vcpu_id];
		lock
			.lock()
			.map_err(|_| err("PSCI start slot poisoned"))?
			.pending = None;
		Ok(())
	}
}

const fn mpidr_affinity(mpidr: u64) -> u64 {
	(mpidr & 0x00ff_ffff) | ((mpidr >> 8) & 0xff00_0000)
}
