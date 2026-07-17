//! Preallocated network slot pool.
//!
//! A slot is a TAP device that was created, addressed, and wired with the
//! default (unrestricted) forwarding policy ahead of time, plus a dedicated
//! nftables set for custom egress policy. Slots are preallocated in one broker
//! batch at serve start (`--net-slots N` / `VMON_NET_SLOTS`); claiming a slot
//! for a sandbox with the default policy is a pure in-memory operation — zero
//! external commands and zero broker round trips. Only custom egress policies
//! (CIDR or domain allowlists, including the deny-all policy used by
//! credential-gateway sandboxes) touch the broker, and then as a single
//! batched `nft -f -` invocation per claim.
//!
//! nftables layout (installed once at pool fill):
//!
//! ```text
//! table ip vmon_slots {
//!     set restricted { type ipv4_addr; }            # guests in custom-policy mode
//!     set egress_s<i> { type ipv4_addr; flags interval; }  # per-slot allowlist
//!     chain forward {                                # hook forward, policy accept
//!         iifname "tv<i>" ip saddr @restricted ip daddr @egress_s<i> accept
//!         iifname "tv<i>" ip saddr @restricted drop
//!     }
//! }
//! ```
//!
//! A default-policy slot is not in `restricted`, so its packets fall through
//! to the preinstalled iptables base rules (masquerade, host deny,
//! protected-range denies, unrestricted accept) exactly like a per-create TAP.
//! A custom claim adds the guest to `restricted` and fills the slot's egress
//! set; recycling flushes the set and removes the guest again. Because the
//! iptables protected-range denies stay installed on pooled slots, a custom
//! egress allowlist cannot re-open protected ranges (strictly tighter than
//! the non-pooled iptables path).

#[cfg(any(test, target_os = "linux"))]
use std::fmt::Write as _;
use std::{collections::HashMap, net::Ipv4Addr, sync::LazyLock};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::{DEFAULT_DNS, GuestConfig};

/// The nftables table owning every pooled-slot set and rule.
pub const SLOT_TABLE: &str = "vmon_slots";
/// The set of guest addresses currently under a custom egress policy.
pub const RESTRICTED_SET: &str = "restricted";

/// Identity of one preallocated slot, shared with the privileged broker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotSpec {
	pub index:    u32,
	pub name:     String,
	pub guest_ip: String,
	pub host_ip:  String,
	pub prefix:   u8,
}

impl SlotSpec {
	/// The per-slot nftables egress allowlist set name.
	pub fn egress_set(&self) -> String {
		format!("egress_s{}", self.index)
	}
}

/// Deterministic TAP device name for a slot (`tv` + 10 hex digits, matching
/// the broker's TAP-name validation).
pub(crate) fn slot_tap_name(index: u32) -> String {
	format!("tv{index:010x}")
}

/// Reserved lease-journal key holding a slot's /30 block.
pub(crate) fn slot_lease_name(index: u32) -> String {
	format!("~slot-{index}")
}

/// One `nft -f -` payload installing the pooled-slot skeleton.
///
/// `reset` atomically wipes and recreates the whole table (first batch of a
/// pool fill); follow-up batches only append their slots.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn pool_init_payload(slots: &[SlotSpec], reset: bool) -> String {
	let mut out = String::new();
	out.push_str("add table ip vmon_slots\n");
	if reset {
		out.push_str("delete table ip vmon_slots\n");
		out.push_str("add table ip vmon_slots\n");
		out.push_str("add set ip vmon_slots restricted { type ipv4_addr; }\n");
		out.push_str(
			"add chain ip vmon_slots forward { type filter hook forward priority -5; policy accept; \
			 }\n",
		);
	}
	for slot in slots {
		let set = slot.egress_set();
		let _ = writeln!(out, "add set ip vmon_slots {set} {{ type ipv4_addr; flags interval; }}");
		let _ = writeln!(
			out,
			"add rule ip vmon_slots forward iifname \"{}\" ip saddr @restricted ip daddr @{set} \
			 accept",
			slot.name
		);
		let _ = writeln!(
			out,
			"add rule ip vmon_slots forward iifname \"{}\" ip saddr @restricted drop",
			slot.name
		);
	}
	out
}

/// One `nft -f -` payload applying a custom egress policy to a slot.
///
/// `elements` are IPv4 addresses/CIDRs (explicit allowlist entries plus
/// resolved domain addresses). When the slot is already under a custom policy
/// the set is flushed and refilled; otherwise the guest also joins the
/// `restricted` set.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn claim_payload(slot: &SlotSpec, elements: &[String], already_custom: bool) -> String {
	let mut out = String::new();
	let set = slot.egress_set();
	if already_custom {
		let _ = writeln!(out, "flush set ip vmon_slots {set}");
	}
	if !elements.is_empty() {
		let _ = writeln!(out, "add element ip vmon_slots {set} {{ {} }}", elements.join(", "));
	}
	if !already_custom {
		let _ = writeln!(out, "add element ip vmon_slots restricted {{ {} }}", slot.guest_ip);
	}
	out
}

/// One `nft -f -` payload returning a slot to the default policy.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn recycle_payload(slot: &SlotSpec) -> String {
	format!(
		"flush set ip vmon_slots {}\ndelete element ip vmon_slots restricted {{ {} }}\n",
		slot.egress_set(),
		slot.guest_ip
	)
}

/// One `nft -f -` payload adding/removing egress-set elements (DNS refresh).
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn update_set_payload(set: &str, add: &[String], remove: &[String]) -> String {
	let mut out = String::new();
	if !add.is_empty() {
		let _ = writeln!(out, "add element ip vmon_slots {set} {{ {} }}", add.join(", "));
	}
	if !remove.is_empty() {
		let _ = writeln!(out, "delete element ip vmon_slots {set} {{ {} }}", remove.join(", "));
	}
	out
}

struct SlotEntry {
	spec:    SlotSpec,
	sandbox: Option<String>,
	custom:  bool,
	dns:     Vec<String>,
}

/// In-memory pool state: free list plus sandbox and TAP bindings.
struct SlotPool {
	slots:  Vec<SlotEntry>,
	free:   Vec<u32>,
	bound:  HashMap<String, u32>,
	by_tap: HashMap<String, u32>,
}

impl SlotPool {
	fn empty() -> Self {
		Self {
			slots:  Vec::new(),
			free:   Vec::new(),
			bound:  HashMap::new(),
			by_tap: HashMap::new(),
		}
	}

	fn config_for(&self, index: u32) -> Option<GuestConfig> {
		let entry = self.slots.get(index as usize)?;
		let host = entry.spec.host_ip.parse::<Ipv4Addr>().ok()?;
		let network = Ipv4Addr::from(u32::from(host) - 1);
		Some(GuestConfig {
			tap:      entry.spec.name.clone(),
			host_ip:  entry.spec.host_ip.clone(),
			guest_ip: entry.spec.guest_ip.clone(),
			prefix:   entry.spec.prefix,
			network:  format!("{network}/{}", entry.spec.prefix),
			dns:      if entry.dns.is_empty() {
				DEFAULT_DNS.iter().map(|dns| (*dns).to_owned()).collect()
			} else {
				entry.dns.clone()
			},
		})
	}

	fn unbind(&mut self, index: u32) -> Option<(SlotSpec, bool)> {
		let entry = self.slots.get_mut(index as usize)?;
		let sandbox = entry.sandbox.take()?;
		let custom = entry.custom;
		entry.custom = false;
		entry.dns.clear();
		let spec = entry.spec.clone();
		self.bound.remove(&sandbox);
		self.free.push(index);
		Some((spec, custom))
	}
}

static POOL: LazyLock<Mutex<SlotPool>> = LazyLock::new(|| Mutex::new(SlotPool::empty()));

fn pool() -> parking_lot::MutexGuard<'static, SlotPool> {
	POOL.lock()
}

/// Install a freshly preallocated pool, replacing any previous one.
pub(crate) fn pool_install(specs: Vec<SlotSpec>) {
	let mut pool = pool();
	pool.by_tap = specs
		.iter()
		.map(|spec| (spec.name.clone(), spec.index))
		.collect();
	pool.free = specs.iter().rev().map(|spec| spec.index).collect();
	pool.slots = specs
		.into_iter()
		.map(|spec| SlotEntry { spec, sandbox: None, custom: false, dns: Vec::new() })
		.collect();
	pool.bound.clear();
}

/// Claim a free slot for `name`, or return its existing binding.
pub(crate) fn pool_claim(name: &str, dns: &[&str]) -> Option<GuestConfig> {
	let mut pool = pool();
	if let Some(&index) = pool.bound.get(name) {
		return pool.config_for(index);
	}
	let index = pool.free.pop()?;
	{
		let entry = &mut pool.slots[index as usize];
		entry.sandbox = Some(name.to_owned());
		entry.custom = false;
		entry.dns = dns.iter().map(|dns| (*dns).to_owned()).collect();
	}
	pool.bound.insert(name.to_owned(), index);
	pool.config_for(index)
}

/// Return the bound slot config for a sandbox, if it is slot-backed.
pub(crate) fn pool_lookup(name: &str) -> Option<GuestConfig> {
	let pool = pool();
	let &index = pool.bound.get(name)?;
	pool.config_for(index)
}

/// Return the slot behind a TAP name plus whether it runs a custom policy.
pub(crate) fn pool_slot_for_tap(tap: &str) -> Option<(SlotSpec, bool)> {
	let pool = pool();
	let &index = pool.by_tap.get(tap)?;
	let entry = pool.slots.get(index as usize)?;
	entry.sandbox.as_ref()?;
	Some((entry.spec.clone(), entry.custom))
}

/// Record whether a claimed slot currently carries a custom policy.
pub(crate) fn pool_set_custom(tap: &str, custom: bool) {
	let mut pool = pool();
	if let Some(&index) = pool.by_tap.get(tap)
		&& let Some(entry) = pool.slots.get_mut(index as usize)
	{
		entry.custom = custom;
	}
}

/// Recycle the slot behind a TAP name back into the free list.
///
/// Returns the slot and whether it carried a custom policy (which the caller
/// must reset through the broker). `None` when the TAP is not a claimed slot.
pub(crate) fn pool_recycle_tap(tap: &str) -> Option<(SlotSpec, bool)> {
	let mut pool = pool();
	let &index = pool.by_tap.get(tap)?;
	pool.slots.get(index as usize)?.sandbox.as_ref()?;
	pool.unbind(index)
}

/// Release a sandbox's slot binding without a prior TAP teardown.
pub(crate) fn pool_release_name(name: &str) -> Option<(SlotSpec, bool)> {
	let mut pool = pool();
	let &index = pool.bound.get(name)?;
	pool.unbind(index)
}

/// Number of currently free slots (0 when the pool is cold).
#[cfg(test)]
pub(crate) fn pool_free_len() -> usize {
	pool().free.len()
}

#[cfg(test)]
pub(crate) fn pool_reset() {
	let mut pool = pool();
	*pool = SlotPool::empty();
}

#[cfg(test)]
mod tests {
	use super::*;

	fn slot(index: u32) -> SlotSpec {
		let base = u32::from(Ipv4Addr::new(172, 20, 0, 0)) + index * 4;
		SlotSpec {
			index,
			name: slot_tap_name(index),
			guest_ip: Ipv4Addr::from(base + 2).to_string(),
			host_ip: Ipv4Addr::from(base + 1).to_string(),
			prefix: 30,
		}
	}

	#[test]
	fn pool_init_payload_installs_skeleton_and_per_slot_rules() {
		let payload = pool_init_payload(&[slot(0), slot(1)], true);
		assert_eq!(
			payload,
			"add table ip vmon_slots\ndelete table ip vmon_slots\nadd table ip vmon_slots\nadd set \
			 ip vmon_slots restricted { type ipv4_addr; }\nadd chain ip vmon_slots forward { type \
			 filter hook forward priority -5; policy accept; }\nadd set ip vmon_slots egress_s0 { \
			 type ipv4_addr; flags interval; }\nadd rule ip vmon_slots forward iifname \
			 \"tv0000000000\" ip saddr @restricted ip daddr @egress_s0 accept\nadd rule ip \
			 vmon_slots forward iifname \"tv0000000000\" ip saddr @restricted drop\nadd set ip \
			 vmon_slots egress_s1 { type ipv4_addr; flags interval; }\nadd rule ip vmon_slots \
			 forward iifname \"tv0000000001\" ip saddr @restricted ip daddr @egress_s1 accept\nadd \
			 rule ip vmon_slots forward iifname \"tv0000000001\" ip saddr @restricted drop\n"
		);
		let appended = pool_init_payload(&[slot(2)], false);
		assert!(appended.starts_with("add table ip vmon_slots\nadd set ip vmon_slots egress_s2"));
		assert!(!appended.contains("delete table"));
	}

	#[test]
	fn claim_payload_matches_expected_nft_batch() {
		let elements = vec!["93.184.216.34".to_owned(), "1.1.1.0/24".to_owned()];
		assert_eq!(
			claim_payload(&slot(3), &elements, false),
			"add element ip vmon_slots egress_s3 { 93.184.216.34, 1.1.1.0/24 }\nadd element ip \
			 vmon_slots restricted { 172.20.0.14 }\n"
		);
		// Policy replacement flushes and refills without re-adding the guest.
		assert_eq!(
			claim_payload(&slot(3), &elements, true),
			"flush set ip vmon_slots egress_s3\nadd element ip vmon_slots egress_s3 { 93.184.216.34, \
			 1.1.1.0/24 }\n"
		);
		// Deny-all custom policy (block_network + credentials) only marks the
		// guest restricted.
		assert_eq!(
			claim_payload(&slot(3), &[], false),
			"add element ip vmon_slots restricted { 172.20.0.14 }\n"
		);
	}

	#[test]
	fn recycle_and_update_payloads() {
		assert_eq!(
			recycle_payload(&slot(0)),
			"flush set ip vmon_slots egress_s0\ndelete element ip vmon_slots restricted { 172.20.0.2 \
			 }\n"
		);
		assert_eq!(
			update_set_payload("egress_s0", &["9.9.9.9".to_owned()], &["8.8.8.8".to_owned()]),
			"add element ip vmon_slots egress_s0 { 9.9.9.9 }\ndelete element ip vmon_slots egress_s0 \
			 { 8.8.8.8 }\n"
		);
		assert_eq!(update_set_payload("egress_s0", &[], &[]), "");
	}
}
