//! Windows x86 state surface for the WHP backend.
//!
//! WHP boot is supported, but snapshot and restore are rejected until the
//! complete WHP register and userspace IOAPIC state can be serialized.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
	bail,
	hv::{CpuId, Vcpu},
	result::Result,
};

/// Return the host x86 feature surface in the canonical placement format.
pub fn cpu_baseline() -> Result<String> {
	let cpuid = CpuId::host_supported();
	let mut baseline = BTreeMap::new();
	baseline.insert("v".to_string(), serde_json::json!(1_u32));
	for entry in cpuid.as_slice() {
		match (entry.function, entry.index) {
			(0x1, 0) => {
				baseline.insert("1.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert("1.0.edx".to_string(), serde_json::json!(entry.edx));
			},
			(0x7, 0) => {
				baseline.insert("7.0.ebx".to_string(), serde_json::json!(entry.ebx));
				baseline.insert("7.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert("7.0.edx".to_string(), serde_json::json!(entry.edx));
			},
			(0xd, 0) => {
				baseline.insert("D.0.eax".to_string(), serde_json::json!(entry.eax));
				baseline.insert("D.0.ecx".to_string(), serde_json::json!(entry.ecx));
				let xcr0 = u64::from(entry.eax) | (u64::from(entry.edx) << 32);
				baseline.insert("xcr0".to_string(), serde_json::json!(xcr0));
			},
			(0xd, 1) => {
				baseline.insert("D.1.eax".to_string(), serde_json::json!(entry.eax));
				baseline.insert("D.1.ecx".to_string(), serde_json::json!(entry.ecx));
			},
			(0x8000_0001, 0) => {
				baseline.insert("80000001.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert("80000001.0.edx".to_string(), serde_json::json!(entry.edx));
			},
			_ => {},
		}
	}
	Ok(serde_json::to_string(&baseline)?)
}

/// Placeholder serialized vCPU payload; WHP snapshots are rejected.
#[derive(Serialize, Deserialize, Clone)]
pub struct VcpuState {}

/// Placeholder serialized machine payload; WHP snapshots are rejected.
#[derive(Serialize, Deserialize, Clone)]
pub struct MachineState {}

/// Reject WHP vCPU snapshots until complete register state is modeled.
pub fn save_vcpu(_: &Vcpu, _: usize) -> Result<VcpuState> {
	bail!("WHP snapshots are not supported")
}

/// Reject WHP vCPU restore until complete register state is modeled.
pub fn restore_vcpu(_: &Vcpu, _: &VcpuState) -> Result<()> {
	bail!("WHP restore is not supported")
}

/// Reject WHP machine snapshots until IOAPIC state is modeled.
pub fn save_machine() -> Result<MachineState> {
	bail!("WHP snapshots are not supported")
}
