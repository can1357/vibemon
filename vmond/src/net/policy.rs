//! Resolver-pinned egress policy.
//!
//! The daemon resolves approved names itself, validates every returned address,
//! and retains the resulting address set only until its DNS TTL expires. Guests
//! never choose a resolver or turn a name into an arbitrary destination.

use std::{
	collections::{BTreeMap, BTreeSet},
	net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs},
	time::Duration,
};

use crate::error::{EngineError, Result};

/// A DNS result returned by the daemon resolver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedAddress {
	pub address: IpAddr,
	pub ttl:     Duration,
}

/// DNS lookup capability owned by the daemon, never by the guest.
pub trait DaemonResolver {
	fn resolve(&self, domain: &str) -> Result<Vec<ResolvedAddress>>;
}

/// The host resolver used by the daemon in production.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemResolver;

impl DaemonResolver for SystemResolver {
	fn resolve(&self, domain: &str) -> Result<Vec<ResolvedAddress>> {
		let addresses = (domain, 0_u16)
			.to_socket_addrs()
			.map_err(|err| EngineError::engine(format!("resolving egress domain {domain}: {err}")))?;
		let mut unique = BTreeSet::new();
		for address in addresses {
			unique.insert(address.ip());
		}
		if unique.is_empty() {
			return Err(EngineError::engine(format!(
				"resolving egress domain {domain}: no addresses"
			)));
		}
		// getaddrinfo does not expose DNS TTL. Bound pins conservatively until a
		// resolver with native TTL support is configured.
		Ok(unique
			.into_iter()
			.map(|address| ResolvedAddress { address, ttl: Duration::from_mins(1) })
			.collect())
	}
}

const PROTECTED_V4_BLOCKS: &[(Ipv4Addr, u8)] = &[
	(Ipv4Addr::UNSPECIFIED, 8),
	(Ipv4Addr::new(127, 0, 0, 0), 8),
	(Ipv4Addr::new(10, 0, 0, 0), 8),
	(Ipv4Addr::new(172, 16, 0, 0), 12),
	(Ipv4Addr::new(192, 168, 0, 0), 16),
	(Ipv4Addr::new(169, 254, 0, 0), 16),
	(Ipv4Addr::new(224, 0, 0, 0), 4),
	(Ipv4Addr::BROADCAST, 32),
	(Ipv4Addr::new(100, 64, 0, 0), 10),
	(Ipv4Addr::new(192, 0, 0, 0), 24),
	(Ipv4Addr::new(192, 0, 2, 0), 24),
	(Ipv4Addr::new(192, 88, 99, 0), 24),
	(Ipv4Addr::new(198, 18, 0, 0), 15),
	(Ipv4Addr::new(198, 51, 100, 0), 24),
	(Ipv4Addr::new(203, 0, 113, 0), 24),
	(Ipv4Addr::new(240, 0, 0, 0), 4),
];

const PROTECTED_V6_BLOCKS: &[(Ipv6Addr, u8)] = &[
	(Ipv6Addr::UNSPECIFIED, 128),
	(Ipv6Addr::LOCALHOST, 128),
	(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7),
	(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10),
	(Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0), 8),
	(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32),
	(Ipv6Addr::new(0x2001, 0x10, 0, 0, 0, 0, 0, 0), 28),
];

/// A normalized, explicit CIDR exception for a trusted destination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedCidr {
	network: IpAddr,
	prefix:  u8,
}

impl TrustedCidr {
	pub fn contains(&self, address: IpAddr) -> bool {
		match (self.network, address) {
			(IpAddr::V4(network), IpAddr::V4(address)) => {
				let mask = ipv4_mask(self.prefix);
				(u32::from(network) & mask) == (u32::from(address) & mask)
			},
			(IpAddr::V6(network), IpAddr::V6(address)) => {
				let mask = ipv6_mask(self.prefix);
				(u128::from(network) & mask) == (u128::from(address) & mask)
			},
			_ => false,
		}
	}
}

impl std::str::FromStr for TrustedCidr {
	type Err = EngineError;

	fn from_str(value: &str) -> Result<Self> {
		let (address, prefix) = value
			.trim()
			.split_once('/')
			.map_or_else(|| (value.trim(), None), |(addr, pref)| (addr, Some(pref)));
		let address = address
			.parse::<IpAddr>()
			.map_err(|_| EngineError::invalid(format!("invalid trusted CIDR {value}")))?;
		let max = if address.is_ipv4() { 32 } else { 128 };
		let prefix = prefix.map_or(Ok(max), |prefix| {
			prefix
				.parse::<u8>()
				.map_err(|_| EngineError::invalid(format!("invalid trusted CIDR {value}")))
		})?;
		if prefix > max {
			return Err(EngineError::invalid(format!("invalid trusted CIDR {value}")));
		}
		let network = match address {
			IpAddr::V4(address) => IpAddr::V4(Ipv4Addr::from(u32::from(address) & ipv4_mask(prefix))),
			IpAddr::V6(address) => IpAddr::V6(Ipv6Addr::from(u128::from(address) & ipv6_mask(prefix))),
		};
		let is_valid = match network {
			IpAddr::V4(net) => PROTECTED_V4_BLOCKS.iter().any(|&(block, p_pref)| {
				prefix >= p_pref && (u32::from(net) & ipv4_mask(p_pref)) == u32::from(block)
			}),
			IpAddr::V6(net) => PROTECTED_V6_BLOCKS.iter().any(|&(block, p_pref)| {
				prefix >= p_pref && (u128::from(net) & ipv6_mask(p_pref)) == u128::from(block)
			}),
		};
		if !is_valid {
			return Err(EngineError::invalid(format!(
				"trusted CIDR exception {value} must be contained within a recognized protected CIDR"
			)));
		}
		Ok(Self { network, prefix })
	}
}

/// A daemon-owned domain allowlist and TTL-bounded DNS pin cache.
pub struct EgressPolicy<R> {
	resolver:        R,
	allowed_domains: BTreeSet<String>,
	trusted:         Vec<TrustedCidr>,
	pins:            BTreeMap<String, PinnedAddresses>,
}

#[derive(Clone, Debug)]
struct PinnedAddresses {
	addresses:  BTreeSet<IpAddr>,
	expires_at: Duration,
}

impl<R: DaemonResolver> EgressPolicy<R> {
	pub fn new(resolver: R, allowed_domains: &[String], trusted: &[String]) -> Result<Self> {
		let allowed_domains = allowed_domains
			.iter()
			.map(|domain| normalize_domain(domain))
			.collect::<Result<BTreeSet<_>>>()?;
		let trusted = trusted
			.iter()
			.map(|cidr| cidr.parse())
			.collect::<Result<Vec<_>>>()?;
		Ok(Self { resolver, allowed_domains, trusted, pins: BTreeMap::new() })
	}

	/// Resolve an approved domain through the daemon and return its pinned IP
	/// set. A cache refresh replaces the entire set only after every answer
	/// validates.
	pub fn resolve_allowed(&mut self, domain: &str, now: Duration) -> Result<BTreeSet<IpAddr>> {
		let domain = normalize_domain(domain)?;
		if !self.domain_allowed(&domain) {
			return Err(EngineError::invalid(format!("egress domain is not allowed: {domain}")));
		}
		let host = domain.strip_prefix("*.").unwrap_or(&domain);
		if let Some(pin) = self.pins.get(host)
			&& now < pin.expires_at
		{
			return Ok(pin.addresses.clone());
		}

		let answers = self.resolver.resolve(host)?;
		if answers.is_empty() {
			return Err(EngineError::engine(format!(
				"resolving egress domain {domain}: no addresses"
			)));
		}
		let mut addresses = BTreeSet::new();
		let mut ttl = None;
		for answer in answers {
			if !self.destination_allowed(answer.address) {
				return Err(EngineError::invalid(format!(
					"egress domain {domain} resolved to protected address {}",
					answer.address
				)));
			}
			addresses.insert(answer.address);
			ttl = Some(ttl.map_or(answer.ttl, |current: Duration| current.min(answer.ttl)));
		}
		let ttl = ttl.expect("non-empty DNS answers have a TTL");
		let expires_at = now.checked_add(ttl).unwrap_or(Duration::MAX);
		self
			.pins
			.insert(host.to_owned(), PinnedAddresses { addresses: addresses.clone(), expires_at });
		Ok(addresses)
	}

	/// Validate daemon-selected DNS endpoints before injecting them into a
	/// guest.
	pub fn approved_dns(&self, servers: &[IpAddr]) -> Result<Vec<IpAddr>> {
		if servers.is_empty() {
			return Err(EngineError::invalid("at least one daemon DNS server is required"));
		}
		let mut approved = Vec::with_capacity(servers.len());
		for &server in servers {
			if !self.destination_allowed(server) {
				return Err(EngineError::invalid(format!("DNS server is protected: {server}")));
			}
			if !approved.contains(&server) {
				approved.push(server);
			}
		}
		Ok(approved)
	}

	/// Permit globally routable destinations, or a deliberate trusted CIDR.
	pub fn destination_allowed(&self, address: IpAddr) -> bool {
		self.trusted.iter().any(|cidr| cidr.contains(address)) || !is_protected(address)
	}

	fn domain_allowed(&self, domain: &str) -> bool {
		self.allowed_domains.iter().any(|allowed| {
			allowed == domain
				|| allowed.strip_prefix("*.").is_some_and(|suffix| {
					domain.len() > suffix.len()
						&& domain.ends_with(suffix)
						&& domain.as_bytes()[domain.len() - suffix.len() - 1] == b'.'
				})
		})
	}
}

/// Whether an address is protected from guest egress unless explicitly trusted.
pub fn is_protected(address: IpAddr) -> bool {
	match address {
		IpAddr::V4(address) => {
			address.is_unspecified()
				|| address.is_loopback()
				|| address.is_private()
				|| address.is_link_local()
				|| address.is_multicast()
				|| address.is_broadcast()
				|| in_v4(address, Ipv4Addr::new(100, 64, 0, 0), 10)
				|| in_v4(address, Ipv4Addr::new(192, 0, 0, 0), 24)
				|| in_v4(address, Ipv4Addr::new(192, 0, 2, 0), 24)
				|| in_v4(address, Ipv4Addr::new(192, 88, 99, 0), 24)
				|| in_v4(address, Ipv4Addr::new(198, 18, 0, 0), 15)
				|| in_v4(address, Ipv4Addr::new(198, 51, 100, 0), 24)
				|| in_v4(address, Ipv4Addr::new(203, 0, 113, 0), 24)
				|| in_v4(address, Ipv4Addr::new(240, 0, 0, 0), 4)
		},
		IpAddr::V6(address) => {
			address.is_unspecified()
				|| address.is_loopback()
				|| address.is_multicast()
				|| in_v6(address, "fc00::", 7)
				|| in_v6(address, "fe80::", 10)
				|| in_v6(address, "2001:db8::", 32)
				|| in_v6(address, "2001:10::", 28)
		},
	}
}

fn normalize_domain(value: &str) -> Result<String> {
	let domain = value.trim().trim_end_matches('.').to_ascii_lowercase();
	let host = domain.strip_prefix("*.").unwrap_or(&domain);
	if host.is_empty()
		|| host.len() > 253
		|| host.split('.').any(|label| {
			label.is_empty()
				|| label.len() > 63
				|| label.starts_with('-')
				|| label.ends_with('-')
				|| !label
					.bytes()
					.all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
		}) {
		return Err(EngineError::invalid(format!("invalid egress domain {value}")));
	}
	Ok(domain)
}

const fn ipv4_mask(prefix: u8) -> u32 {
	if prefix == 0 {
		0
	} else {
		u32::MAX << (32 - prefix)
	}
}

const fn ipv6_mask(prefix: u8) -> u128 {
	if prefix == 0 {
		0
	} else {
		u128::MAX << (128 - prefix)
	}
}

fn in_v4(address: Ipv4Addr, network: Ipv4Addr, prefix: u8) -> bool {
	(u32::from(address) & ipv4_mask(prefix)) == u32::from(network)
}

fn in_v6(address: Ipv6Addr, network: &str, prefix: u8) -> bool {
	let network = network
		.parse::<Ipv6Addr>()
		.expect("static IPv6 network is valid");
	(u128::from(address) & ipv6_mask(prefix)) == u128::from(network)
}

#[cfg(test)]
mod tests {
	use std::{cell::RefCell, collections::VecDeque};

	use super::*;

	#[derive(Default)]
	struct FakeResolver {
		answers: RefCell<VecDeque<Result<Vec<ResolvedAddress>>>>,
		calls:   RefCell<Vec<String>>,
	}

	impl FakeResolver {
		fn with(answers: Vec<Result<Vec<ResolvedAddress>>>) -> Self {
			Self { answers: RefCell::new(answers.into()), calls: RefCell::new(Vec::new()) }
		}
	}

	impl DaemonResolver for FakeResolver {
		fn resolve(&self, domain: &str) -> Result<Vec<ResolvedAddress>> {
			self.calls.borrow_mut().push(domain.to_owned());
			self
				.answers
				.borrow_mut()
				.pop_front()
				.unwrap_or_else(|| Err(EngineError::engine("unexpected resolver call")))
		}
	}

	fn answer(address: &str, ttl: u64) -> ResolvedAddress {
		ResolvedAddress { address: address.parse().unwrap(), ttl: Duration::from_secs(ttl) }
	}

	#[test]
	fn special_ranges_are_denied_for_both_families() {
		for address in [
			"127.0.0.1",
			"169.254.169.254",
			"10.0.0.1",
			"172.20.0.1",
			"192.168.1.1",
			"224.0.0.1",
			"198.18.0.1",
			"203.0.113.1",
			"::1",
			"fe80::1",
			"fd00::1",
			"ff02::1",
			"2001:db8::1",
		] {
			assert!(is_protected(address.parse().unwrap()), "{address} should be protected");
		}
		for address in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
			assert!(!is_protected(address.parse().unwrap()), "{address} should be public");
		}
	}

	#[test]
	fn trusted_exceptions_are_narrow_and_cover_ipv4_and_ipv6() {
		let policy = EgressPolicy::new(FakeResolver::default(), &[], &[
			"169.254.169.254/32".to_owned(),
			"fd00:1234::/48".to_owned(),
		])
		.unwrap();
		assert!(policy.destination_allowed("169.254.169.254".parse().unwrap()));
		assert!(!policy.destination_allowed("169.254.169.253".parse().unwrap()));
		assert!(policy.destination_allowed("fd00:1234::5".parse().unwrap()));
		assert!(!policy.destination_allowed("fd00:1235::5".parse().unwrap()));
	}

	#[test]
	fn trusted_exceptions_supernets_are_rejected() {
		// Broad supernets like 0.0.0.0/0 must be rejected
		assert!(EgressPolicy::new(FakeResolver::default(), &[], &["0.0.0.0/0".to_owned()]).is_err());
		assert!(EgressPolicy::new(FakeResolver::default(), &[], &["10.0.0.0/7".to_owned()]).is_err());
		assert!(EgressPolicy::new(FakeResolver::default(), &[], &["::/0".to_owned()]).is_err());

		// Exact and subsets of protected blocks are accepted
		assert!(EgressPolicy::new(FakeResolver::default(), &[], &["10.0.0.0/8".to_owned()]).is_ok());
		assert!(
			EgressPolicy::new(FakeResolver::default(), &[], &["169.254.169.254/32".to_owned()])
				.is_ok()
		);
	}

	#[test]
	fn mixed_public_and_private_answers_fail_closed() {
		let resolver =
			FakeResolver::with(vec![Ok(vec![answer("1.1.1.1", 30), answer("10.0.0.1", 30)])]);
		let mut policy = EgressPolicy::new(resolver, &["api.example.com".to_owned()], &[]).unwrap();
		assert!(
			policy
				.resolve_allowed("api.example.com", Duration::ZERO)
				.is_err()
		);
	}

	#[test]
	fn ttl_expiry_refreshes_and_rebinding_to_private_is_rejected() {
		let resolver = FakeResolver::with(vec![
			Ok(vec![answer("1.1.1.1", 10)]),
			Ok(vec![answer("10.0.0.7", 10)]),
		]);
		let mut policy = EgressPolicy::new(resolver, &["api.example.com".to_owned()], &[]).unwrap();
		let first = policy
			.resolve_allowed("api.example.com", Duration::ZERO)
			.unwrap();
		assert_eq!(first, BTreeSet::from(["1.1.1.1".parse().unwrap()]));
		assert_eq!(
			policy
				.resolve_allowed("api.example.com", Duration::from_secs(9))
				.unwrap(),
			first
		);
		assert!(
			policy
				.resolve_allowed("api.example.com", Duration::from_secs(10))
				.is_err()
		);
		assert_eq!(policy.resolver.calls.borrow().len(), 2);
	}

	#[test]
	fn resolver_failure_fails_closed_and_dns_endpoints_are_policy_checked() {
		let resolver = FakeResolver::with(vec![Err(EngineError::engine("resolver unavailable"))]);
		let mut policy = EgressPolicy::new(resolver, &["api.example.com".to_owned()], &[]).unwrap();
		assert!(
			policy
				.resolve_allowed("api.example.com", Duration::ZERO)
				.is_err()
		);
		assert!(
			policy
				.approved_dns(&["127.0.0.1".parse::<IpAddr>().unwrap()])
				.is_err()
		);
		assert_eq!(
			policy
				.approved_dns(&["1.1.1.1".parse::<IpAddr>().unwrap()])
				.unwrap(),
			vec!["1.1.1.1".parse::<IpAddr>().unwrap()]
		);
	}

	#[test]
	fn wildcard_domain_authorization_resolves_and_caches() {
		let resolver = FakeResolver::with(vec![Ok(vec![answer("1.1.1.1", 30)])]);
		let mut policy = EgressPolicy::new(resolver, &["*.example.com".to_owned()], &[]).unwrap();
		let first = policy
			.resolve_allowed("sub.example.com", Duration::ZERO)
			.unwrap();
		assert_eq!(first, BTreeSet::from(["1.1.1.1".parse().unwrap()]));
		assert_eq!(policy.resolver.calls.borrow().clone(), vec!["sub.example.com".to_owned()]);
		assert!(policy.resolve_allowed("other.com", Duration::ZERO).is_err());
	}
}
