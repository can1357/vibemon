//! KVM vCPU wrapper and backend-neutral exit conversion.

use std::sync::Arc;

use kvm_ioctls::{VcpuExit, VcpuFd, VmFd};

use crate::{hv::Exit, result::Result};

/// A KVM vCPU plus the VM fd needed for arm64 initialization.
pub struct Vcpu {
	#[cfg_attr(target_arch = "x86_64", allow(dead_code, reason = "arm64 init needs the VM fd"))]
	vm_fd: Arc<VmFd>,
	fd:    VcpuFd,
}

impl Vcpu {
	/// Wrap a newly-created KVM vCPU fd.
	pub(super) const fn new(vm_fd: Arc<VmFd>, fd: VcpuFd) -> Self {
		Self { vm_fd, fd }
	}

	/// Return the underlying KVM vCPU fd for KVM-specific arch helpers.
	#[cfg_attr(target_arch = "aarch64", allow(dead_code, reason = "x86_64-only accessor"))]
	pub const fn fd(&self) -> &VcpuFd {
		&self.fd
	}

	/// Run the vCPU until KVM exits and translate the reason to the neutral
	/// seam.
	pub fn run(&mut self) -> Result<Exit<'_>> {
		match self.fd.run() {
			Ok(VcpuExit::IoIn(port, data)) => Ok(Exit::PioIn { port, data }),
			Ok(VcpuExit::IoOut(port, data)) => Ok(Exit::PioOut { port, data }),
			Ok(VcpuExit::MmioRead(addr, data)) => Ok(Exit::MmioRead { addr, data }),
			Ok(VcpuExit::MmioWrite(addr, data)) => Ok(Exit::MmioWrite { addr, data }),
			Ok(VcpuExit::Hlt) => Ok(Exit::Hlt),
			Ok(VcpuExit::Shutdown) => Ok(Exit::Shutdown),
			Ok(VcpuExit::SystemEvent(kind, _)) => Ok(Exit::SystemEvent(kind)),
			Ok(VcpuExit::FailEntry(reason, cpu)) => Ok(Exit::FailEntry { reason, cpu }),
			Ok(VcpuExit::InternalError) => Ok(Exit::InternalError),
			Ok(_) => Ok(Exit::Other),
			// EINTR (signal, e.g. the pause kick) and EAGAIN (KVM could not enter
			// the guest yet — common under nested virtualization) are transient:
			// re-enter the run loop, matching Cloud Hypervisor / Firecracker.
			Err(e) if matches!(e.errno(), libc::EINTR | libc::EAGAIN) => Ok(Exit::Continue),
			Err(e) => Err(e.into()),
		}
	}
}

#[cfg(target_arch = "x86_64")]
impl Vcpu {
	/// Capture backend-specific vCPU state into the arch serialized form.
	pub fn save_state(&self, xsave_size: usize) -> Result<crate::arch::state::VcpuState> {
		crate::arch::state::save_vcpu(self, xsave_size)
	}

	/// Restore backend-specific vCPU state from the arch serialized form.
	pub fn restore_state(&self, st: &crate::arch::state::VcpuState) -> Result<()> {
		crate::arch::state::restore_vcpu(self, st)
	}
}

#[cfg(target_arch = "aarch64")]
mod aarch64 {
	use std::mem::offset_of;

	use kvm_bindings::{
		KVM_ARM_VCPU_POWER_OFF, KVM_ARM_VCPU_PSCI_0_2, KVM_REG_ARM_CORE, KVM_REG_ARM64,
		KVM_REG_ARM64_SYSREG, KVM_REG_ARM64_SYSREG_CRM_MASK, KVM_REG_ARM64_SYSREG_CRM_SHIFT,
		KVM_REG_ARM64_SYSREG_CRN_MASK, KVM_REG_ARM64_SYSREG_CRN_SHIFT, KVM_REG_ARM64_SYSREG_OP0_MASK,
		KVM_REG_ARM64_SYSREG_OP0_SHIFT, KVM_REG_ARM64_SYSREG_OP1_MASK,
		KVM_REG_ARM64_SYSREG_OP1_SHIFT, KVM_REG_ARM64_SYSREG_OP2_MASK,
		KVM_REG_ARM64_SYSREG_OP2_SHIFT, KVM_REG_SIZE_U64, kvm_regs, kvm_vcpu_init, user_pt_regs,
	};

	use super::Vcpu;
	use crate::{bail, hv::SysReg, result::Result};

	impl Vcpu {
		/// Capture backend-specific vCPU state into the arch serialized form.
		pub fn save_state(&self, xsave_size: usize) -> Result<crate::arch::state::VcpuState> {
			super::super::state::save_vcpu(&self.fd, xsave_size)
		}

		/// Restore backend-specific vCPU state from the arch serialized form.
		pub fn restore_state(&self, st: &crate::arch::state::VcpuState) -> Result<()> {
			super::super::state::restore_vcpu(&self.fd, st)
		}

		/// Initialize the boot vCPU with PSCI enabled.
		pub fn init_boot(&self) -> Result<()> {
			self.init(false)
		}

		/// Initialize a secondary `vCPU` as powered off until PSCI `CPU_ON`.
		pub fn init_secondary(&self) -> Result<()> {
			self.init(true)
		}

		/// Set the aarch64 program counter.
		pub fn set_pc(&self, v: u64) -> Result<()> {
			let base = offset_of!(kvm_regs, regs);
			let pc = core_reg_id(base + offset_of!(user_pt_regs, pc));
			self.set_u64_reg(pc, v)
		}

		/// Set one aarch64 general-purpose register X0..X30.
		pub fn set_gpr(&self, idx: u8, v: u64) -> Result<()> {
			if idx > 30 {
				bail!("aarch64 GPR index {idx} is outside X0..X30");
			}
			let base = offset_of!(kvm_regs, regs) + offset_of!(user_pt_regs, regs);
			let off = base + usize::from(idx) * std::mem::size_of::<u64>();
			self.set_u64_reg(core_reg_id(off), v)
		}

		/// Read one aarch64 general-purpose register X0..X30.
		#[allow(dead_code, reason = "part of public API platform abstraction")]
		pub fn get_gpr(&self, idx: u8) -> Result<u64> {
			if idx > 30 {
				bail!("aarch64 GPR index {idx} is outside X0..X30");
			}
			let base = offset_of!(kvm_regs, regs) + offset_of!(user_pt_regs, regs);
			let off = base + usize::from(idx) * std::mem::size_of::<u64>();
			self.get_u64_reg(core_reg_id(off))
		}

		/// Set the aarch64 PSTATE register.
		pub fn set_pstate(&self, v: u64) -> Result<()> {
			let base = offset_of!(kvm_regs, regs);
			let pstate = core_reg_id(base + offset_of!(user_pt_regs, pstate));
			self.set_u64_reg(pstate, v)
		}

		/// Read an aarch64 system register.
		pub fn get_sys_reg(&self, r: SysReg) -> Result<u64> {
			self.get_u64_reg(sys_reg_id(r))
		}

		/// Set an aarch64 system register.
		#[allow(dead_code, reason = "part of public API platform abstraction")]
		pub fn set_sys_reg(&self, r: SysReg, v: u64) -> Result<()> {
			self.set_u64_reg(sys_reg_id(r), v)
		}

		fn init(&self, power_off: bool) -> Result<()> {
			let mut kvi = kvm_vcpu_init::default();
			self.vm_fd.get_preferred_target(&mut kvi)?;
			kvi.features[0] |= 1 << KVM_ARM_VCPU_PSCI_0_2;
			if power_off {
				kvi.features[0] |= 1 << KVM_ARM_VCPU_POWER_OFF;
			}
			self.fd.vcpu_init(&kvi)?;
			Ok(())
		}

		fn set_u64_reg(&self, id: u64, v: u64) -> Result<()> {
			self.fd.set_one_reg(id, &v.to_ne_bytes())?;
			Ok(())
		}

		fn get_u64_reg(&self, id: u64) -> Result<u64> {
			let mut buf = [0u8; 8];
			self.fd.get_one_reg(id, &mut buf)?;
			Ok(u64::from_ne_bytes(buf))
		}
	}

	/// Build a KVM core-register id from a byte offset into `kvm_regs`.
	fn core_reg_id(offset: usize) -> u64 {
		KVM_REG_ARM64
			| u64::from(KVM_REG_ARM_CORE)
			| KVM_REG_SIZE_U64
			| (offset as u64 / std::mem::size_of::<u32>() as u64)
	}

	/// Build a KVM system-register id from the neutral aarch64 encoding.
	const fn sys_reg_id(r: SysReg) -> u64 {
		KVM_REG_ARM64
			| KVM_REG_SIZE_U64
			| KVM_REG_ARM64_SYSREG as u64
			| (((r.op0 as u64) << KVM_REG_ARM64_SYSREG_OP0_SHIFT)
				& KVM_REG_ARM64_SYSREG_OP0_MASK as u64)
			| (((r.op1 as u64) << KVM_REG_ARM64_SYSREG_OP1_SHIFT)
				& KVM_REG_ARM64_SYSREG_OP1_MASK as u64)
			| (((r.crn as u64) << KVM_REG_ARM64_SYSREG_CRN_SHIFT)
				& KVM_REG_ARM64_SYSREG_CRN_MASK as u64)
			| (((r.crm as u64) << KVM_REG_ARM64_SYSREG_CRM_SHIFT)
				& KVM_REG_ARM64_SYSREG_CRM_MASK as u64)
			| (((r.op2 as u64) << KVM_REG_ARM64_SYSREG_OP2_SHIFT)
				& KVM_REG_ARM64_SYSREG_OP2_MASK as u64)
	}
}
