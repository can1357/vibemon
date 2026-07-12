//! Backend-neutral types shared by the KVM, HVF and WHP hypervisor backends.
//!
//! [`Exit`] is the unified vCPU run result: both backends self-complete
//! MMIO/PIO accesses, so after the run loop fills (read) or consumes (write)
//! `data`, the next [`crate::hv::Vcpu::run`] applies completion (KVM lets the
//! kernel finish the access; HVF writes the destination GPR and advances PC).

/// Which hypervisor backend produced a piece of serialized state.
///
/// Stored in the snapshot header (arch-neutral) so a restore on the wrong
/// backend is rejected up front rather than mis-decoding incompatible bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Backend {
	/// Linux KVM.
	Kvm,
	/// Apple Hypervisor.framework.
	Hvf,
	/// Windows Hypervisor Platform.
	Whp,
}

/// The hypervisor backend this binary was compiled against.
pub const fn current_backend() -> Backend {
	#[cfg(target_os = "linux")]
	{
		Backend::Kvm
	}
	#[cfg(target_os = "macos")]
	{
		Backend::Hvf
	}
	#[cfg(all(target_os = "windows", target_arch = "x86_64"))]
	{
		Backend::Whp
	}
}

/// Why a vCPU stopped executing.
///
/// Read variants (`MmioRead`/`PioIn`) borrow a scratch buffer the handler must
/// fill; write variants borrow the bytes the guest is storing. `Continue` is a
/// benign wake (EINTR / handled-in-place) that should re-run without recording
/// a VM-exit metric; `Other` is any modeled-but-uninteresting exit.
///
/// Not every variant is produced by every backend (e.g. PIO is x86/KVM only and
/// aarch64/HVF never yields `PioIn`/`PioOut`), so the unused-variant lint is
/// suppressed for this cross-backend type.
#[allow(dead_code, reason = "part of a multi-backend abstract interface")]
pub enum Exit<'a> {
	/// Guest read from an MMIO address; fill `data` with the device value.
	MmioRead { addr: u64, data: &'a mut [u8] },
	/// Guest wrote `data` to an MMIO address.
	MmioWrite { addr: u64, data: &'a [u8] },
	/// Guest `in` from a port (x86/KVM only); fill `data`.
	PioIn { port: u16, data: &'a mut [u8] },
	/// Guest `out` of `data` to a port (x86/KVM only).
	PioOut { port: u16, data: &'a [u8] },
	/// Guest executed HLT/WFI and is idle.
	Hlt,
	/// Guest requested power-off / triple fault: terminate the VM.
	Shutdown,
	/// PSCI/system event (e.g. `SYSTEM_RESET`); carries the event kind.
	SystemEvent(u32),
	/// The hypervisor failed to enter the guest.
	FailEntry { reason: u64, cpu: u32 },
	/// The hypervisor reported an internal error.
	InternalError,
	/// Benign wake (interrupted run / handled in place): re-run, do not record.
	Continue,
	/// Any other modeled exit: record a generic metric and re-run.
	Other,
}

/// An aarch64 system-register `(op0, op1, crn, crm, op2)` encoding.
///
/// Backends translate this to their native form: KVM to a
/// `KVM_REG_ARM64_SYSREG` id, HVF to its `hv_sys_reg_t` enum value.
#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct SysReg {
	pub op0: u8,
	pub op1: u8,
	pub crn: u8,
	pub crm: u8,
	pub op2: u8,
}

#[cfg(target_arch = "aarch64")]
impl SysReg {
	pub const fn new(op0: u8, op1: u8, crn: u8, crm: u8, op2: u8) -> Self {
		Self { op0, op1, crn, crm, op2 }
	}
}
