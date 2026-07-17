//! Per-vCPU CPUID setup.
//!
//! We pass through the host-supported CPUID returned by KVM and patch the few
//! per-vCPU fields KVM does not fill in itself: the local APIC ID (leaf 1
//! `EBX[31:24]` and leaf 0xB `EDX`) and the "hypervisor present" hint (leaf 1
//! `ECX[31]`). Correct per-vCPU APIC IDs are what let SMP guests boot.
//!
//! When vmon itself runs inside a hypervisor (EC2 nested virtualization,
//! any cloud VM), the L1 KVM may advertise xstate components the L0 refuses
//! to enable for an L2: the guest kernel then takes a `#GP` on `XSETBV` in
//! `fpu__init_cpu_xstate` and panics before printing anything useful. We
//! defend by clamping guest XCR0 to the always-enableable x87|SSE|AVX
//! baseline and scrubbing the matching AVX-512/AMX feature bits — the same
//! normalization Firecracker applies. Bare-metal hosts are untouched.

#[cfg(target_os = "linux")]
use kvm_bindings::CpuId;

#[cfg(target_os = "windows")]
use crate::hv::CpuId;
use crate::{hv::Vcpu, result::Result};

const LEAF_FEATURE_INFO: u32 = 0x1;
const LEAF_STRUCTURED_FEATURES: u32 = 0x7;
const LEAF_EXTENDED_TOPOLOGY: u32 = 0xb;
const LEAF_XSTATE: u32 = 0xd;
const LEAF_V2_EXTENDED_TOPOLOGY: u32 = 0x1f;
const ECX_HYPERVISOR_BIT: u32 = 1 << 31;

/// XCR0 bits every `x86_64` guest kernel can enable anywhere: x87, SSE, AVX.
const XSTATE_BASELINE: u32 = 0b111;
/// Leaf 7 subleaf 0 `EBX`: AVX512{F,DQ,IFMA,PF,ER,CD,BW,VL}.
const LEAF7_EBX_AVX512: u32 =
	1 << 16 | 1 << 17 | 1 << 21 | 1 << 26 | 1 << 27 | 1 << 28 | 1 << 30 | 1 << 31;
/// Leaf 7 subleaf 0 `ECX`: AVX512_{VBMI,VBMI2,VNNI,BITALG,VPOPCNTDQ}.
const LEAF7_ECX_AVX512: u32 = 1 << 1 | 1 << 6 | 1 << 11 | 1 << 12 | 1 << 14;
/// Leaf 7 subleaf 0 `EDX`: AVX512_{4VNNIW,4FMAPS,VP2INTERSECT,FP16} +
/// AMX_{BF16,TILE,INT8}.
const LEAF7_EDX_AVX512_AMX: u32 = 1 << 2 | 1 << 3 | 1 << 8 | 1 << 22 | 1 << 23 | 1 << 24 | 1 << 25;

/// Whether this process itself runs under a hypervisor (leaf 1 `ECX[31]`).
fn running_under_hypervisor() -> bool {
	std::arch::x86_64::__cpuid(LEAF_FEATURE_INFO).ecx & ECX_HYPERVISOR_BIT != 0
}

/// Clone the base CPUID, patch it for `cpu_id`, and apply it to the vCPU.
pub fn setup_cpuid(vcpu: &Vcpu, base: &CpuId, cpu_id: u8) -> Result<()> {
	let mut cpuid = base.clone();
	let nested = running_under_hypervisor();

	for entry in cpuid.as_mut_slice() {
		match entry.function {
			LEAF_FEATURE_INFO => {
				entry.ebx = (entry.ebx & 0x00ff_ffff) | (u32::from(cpu_id) << 24);
				entry.ecx |= ECX_HYPERVISOR_BIT;
			},
			LEAF_EXTENDED_TOPOLOGY | LEAF_V2_EXTENDED_TOPOLOGY => {
				entry.edx = u32::from(cpu_id);
			},
			LEAF_STRUCTURED_FEATURES if nested && entry.index == 0 => {
				entry.ebx &= !LEAF7_EBX_AVX512;
				entry.ecx &= !LEAF7_ECX_AVX512;
				entry.edx &= !LEAF7_EDX_AVX512_AMX;
			},
			LEAF_XSTATE if nested => match entry.index {
				// Subleaf 0: supported XCR0 mask (EAX low, EDX high).
				0 => {
					entry.eax &= XSTATE_BASELINE;
					entry.edx = 0;
				},
				// Subleaf 1: XSS supervisor-state mask (ECX low, EDX high).
				1 => {
					entry.ecx = 0;
					entry.edx = 0;
				},
				_ => {},
			},
			_ => {},
		}
	}

	#[cfg(target_os = "linux")]
	vcpu.fd().set_cpuid2(&cpuid)?;
	#[cfg(target_os = "windows")]
	vcpu.set_cpuid_template(&cpuid, cpu_id)?;
	Ok(())
}
