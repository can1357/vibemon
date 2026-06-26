//! Per-vCPU CPUID setup.
//!
//! We pass through the host-supported CPUID returned by KVM and patch the few
//! per-vCPU fields KVM does not fill in itself: the local APIC ID (leaf 1
//! `EBX[31:24]` and leaf 0xB `EDX`) and the "hypervisor present" hint (leaf 1
//! `ECX[31]`). Correct per-vCPU APIC IDs are what let SMP guests boot.

use kvm_bindings::CpuId;

use crate::hv::Vcpu;
use crate::result::Result;

const LEAF_FEATURE_INFO: u32 = 0x1;
const LEAF_EXTENDED_TOPOLOGY: u32 = 0xB;
const LEAF_V2_EXTENDED_TOPOLOGY: u32 = 0x1F;
const ECX_HYPERVISOR_BIT: u32 = 1 << 31;

/// Clone the base CPUID, patch it for `cpu_id`, and apply it to the vCPU.
pub fn setup_cpuid(vcpu: &Vcpu, base: &CpuId, cpu_id: u8) -> Result<()> {
    let mut cpuid = base.clone();

    for entry in cpuid.as_mut_slice() {
        match entry.function {
            LEAF_FEATURE_INFO => {
                entry.ebx = (entry.ebx & 0x00ff_ffff) | (u32::from(cpu_id) << 24);
                entry.ecx |= ECX_HYPERVISOR_BIT;
            }
            LEAF_EXTENDED_TOPOLOGY | LEAF_V2_EXTENDED_TOPOLOGY => {
                entry.edx = u32::from(cpu_id);
            }
            _ => {}
        }
    }

    vcpu.fd().set_cpuid2(&cpuid)?;
    Ok(())
}
