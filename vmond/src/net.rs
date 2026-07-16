//! Host networking: TAP leases, iptables, egress, tunnels. Port of
//! python/vmon/net.py.

pub mod broker;
pub mod policy;

#[cfg(target_os = "linux")]
use std::collections::{HashMap, btree_map::Entry};
#[cfg(target_os = "linux")]
use std::process::Command;
#[cfg(target_os = "linux")]
use std::sync::LazyLock;
#[cfg(any(test, target_os = "linux"))]
use std::sync::mpsc;
#[cfg(any(test, target_os = "linux"))]
use std::thread::{self, JoinHandle};
#[cfg(any(test, target_os = "linux"))]
use std::time::Duration;
use std::{
	collections::{BTreeMap, BTreeSet},
	fmt,
	fs::{self, File, OpenOptions},
	io::{Read, Seek, SeekFrom, Write},
	net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
	path::PathBuf,
	sync::Arc,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tokio::{
	io,
	net::{TcpListener, TcpStream},
	task::JoinHandle as TokioJoinHandle,
};

use crate::error::{EngineError, Result};
#[cfg(target_os = "linux")]
use crate::security::CREDENTIAL_GATEWAY_PORT;

#[cfg(target_os = "linux")]
const CAP_NET_ADMIN: u32 = 12;
#[cfg(target_os = "linux")]
const IP_FORWARD: &str = "/proc/sys/net/ipv4/ip_forward";
#[cfg(any(test, target_os = "linux"))]
const RULE_PREFIX: &str = "vmon";
const POOL_BASE: Ipv4Addr = Ipv4Addr::new(172, 20, 0, 0);
const POOL_PREFIX: u8 = 16;
const LEASE_PREFIX: u8 = 30;
const LEASE_STEP: u32 = 1 << (32 - LEASE_PREFIX);
const POOL_SIZE: u32 = 1 << (32 - POOL_PREFIX);

/// DNS servers injected into TAP-backed guests.
pub const DEFAULT_DNS: [&str; 2] = ["1.1.1.1", "8.8.8.8"];
/// The fixed libslirp guest address.
pub const USER_NET_GUEST_IP: &str = "10.0.2.15";
/// The fixed libslirp prefix length.
pub const USER_NET_PREFIX: u8 = 24;
/// The fixed libslirp gateway address.
pub const USER_NET_GATEWAY: &str = "10.0.2.2";
/// DNS servers exposed by libslirp.
pub const USER_NET_DNS: [&str; 1] = ["10.0.2.3"];

#[cfg(any(test, target_os = "linux"))]
static START_INSTANT: std::sync::LazyLock<std::time::Instant> =
	std::sync::LazyLock::new(std::time::Instant::now);

#[cfg(target_os = "linux")]
static REFRESHERS: LazyLock<Mutex<HashMap<String, DomainRefresher>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(target_os = "linux")]
static BROKER_SOCKET: LazyLock<Mutex<Option<PathBuf>>> = LazyLock::new(|| Mutex::new(None));
/// A host TAP lease and its forwarding policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TapLease {
	pub name:         String,
	pub guest_ip:     String,
	pub host_ip:      String,
	pub prefix:       u8,
	pub network:      String,
	pub egress_allow: Vec<String>,
}

impl TapLease {
	/// Build and validate a point-to-point TAP lease.
	pub fn new(
		name: &str,
		guest_ip: &str,
		host_ip: &str,
		prefix: u8,
		egress_allow: Option<&[String]>,
	) -> Result<Self> {
		if name.is_empty() || name.len() >= 16 {
			return Err(EngineError::invalid("tap name must be non-empty and shorter than 16 bytes"));
		}
		let guest = parse_ipv4(guest_ip)?;
		let host = parse_ipv4(host_ip)?;
		if prefix != LEASE_PREFIX {
			return Err(EngineError::invalid("sandbox TAP networking requires a /30 prefix"));
		}
		let network = ipv4_network(host, prefix)?;
		if !ipv4_contains(network, prefix, guest) || !ipv4_contains(network, prefix, host) {
			return Err(EngineError::invalid(format!(
				"{guest} and {host} are not both in {network}/{prefix}"
			)));
		}
		if !ipv4_contains(POOL_BASE, POOL_PREFIX, network) {
			return Err(EngineError::invalid(format!(
				"sandbox TAP network must be inside {POOL_BASE}/{POOL_PREFIX}"
			)));
		}
		let network_value = u32::from(network);
		if u32::from(host) != network_value + 1 || u32::from(guest) != network_value + 2 {
			return Err(EngineError::invalid(
				"sandbox TAP addresses must use network+1 for host and network+2 for guest",
			));
		}
		let mut allowed = Vec::new();
		for cidr in egress_allow.unwrap_or(&[]) {
			allowed.push(cidr.parse::<CidrNet>()?.to_string());
		}
		Ok(Self {
			name: name.to_owned(),
			guest_ip: guest.to_string(),
			host_ip: host.to_string(),
			prefix,
			network: format!("{network}/{prefix}"),
			egress_allow: allowed,
		})
	}

	/// The guest address as an iptables host CIDR.
	pub fn guest_cidr(&self) -> String {
		format!("{}/32", self.guest_ip)
	}

	/// The host TAP address with the TAP prefix.
	pub fn host_cidr(&self) -> String {
		format!("{}/{}", self.host_ip, self.prefix)
	}
}

/// A persisted TAP allocation for a sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestConfig {
	pub tap:      String,
	pub host_ip:  String,
	pub guest_ip: String,
	pub prefix:   u8,
	pub network:  String,
	pub dns:      Vec<String>,
}

/// The subset of network config handed to the guest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestNetworkConfig {
	pub tap:      String,
	pub guest_ip: String,
	pub host_ip:  String,
	pub prefix:   u8,
	pub dns:      Vec<String>,
}

impl From<&GuestConfig> for GuestNetworkConfig {
	fn from(value: &GuestConfig) -> Self {
		Self {
			tap:      value.tap.clone(),
			guest_ip: value.guest_ip.clone(),
			host_ip:  value.host_ip.clone(),
			prefix:   value.prefix,
			dns:      value.dns.clone(),
		}
	}
}

/// A configured sandbox network and its loopback port tunnels.
pub struct SandboxNetwork {
	pub name:             String,
	pub config:           GuestConfig,
	pub tap:              TapLease,
	pub guest_config:     GuestNetworkConfig,
	tunnels:              TunnelSet,
	egress_allow:         Option<Vec<String>>,
	egress_allow_domains: Option<Vec<String>>,
}

impl SandboxNetwork {
	/// Return guest-port to local loopback endpoint mappings.
	pub fn tunnels(&self) -> BTreeMap<u16, (String, u16)> {
		self.tunnels.mapping()
	}

	/// Permit only the fixed host credential-gateway TCP port.
	pub fn allow_credential_gateway(&self) -> Result<()> {
		broker_request(broker::Request::AllowCredential { lease: (&self.tap).into() }).map(|_| ())
	}

	/// Tear down tunnels, host policy, and the persisted lease.
	pub fn teardown(self) -> Result<()> {
		let Self {
			name,
			config: _,
			tap,
			guest_config: _,
			tunnels,
			egress_allow,
			egress_allow_domains,
		} = self;
		drop(tunnels);
		teardown_tap(
			&tap.name,
			Some(&tap.guest_ip),
			Some(&tap.host_ip),
			tap.prefix,
			egress_allow.as_deref(),
			egress_allow_domains.as_deref(),
		)?;
		release_guest_config(&name)
	}
}

/// A set of loopback TCP tunnels into a guest.
pub struct TunnelSet {
	mapping:          BTreeMap<u16, (String, u16)>,
	accept_tasks:     Vec<TokioJoinHandle<()>>,
	connection_tasks: Arc<Mutex<Vec<TokioJoinHandle<()>>>>,
}

impl TunnelSet {
	/// Start loopback TCP proxies for the requested guest ports.
	pub async fn new(
		guest_ip: &str,
		ports: &[u16],
		inbound_cidr_allowlist: Option<&[String]>,
	) -> Result<Self> {
		let guest_addr = parse_ip(guest_ip)?;
		let allowed_nets = Arc::new(parse_cidr_allowlist(inbound_cidr_allowlist)?);
		let mut unique = Vec::new();
		let mut seen = BTreeSet::new();
		for &port in ports {
			if port == 0 {
				return Err(EngineError::invalid("invalid TCP port 0"));
			}
			if seen.insert(port) {
				unique.push(port);
			}
		}

		let connection_tasks = Arc::new(Mutex::new(Vec::new()));
		let mut accept_tasks = Vec::new();
		let mut mapping = BTreeMap::new();
		for guest_port in unique {
			let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
			let local = listener.local_addr()?;
			mapping.insert(guest_port, (local.ip().to_string(), local.port()));
			let task_allowed = Arc::clone(&allowed_nets);
			let task_connections = Arc::clone(&connection_tasks);
			let guest = SocketAddr::new(guest_addr, guest_port);
			accept_tasks.push(tokio::spawn(async move {
				accept_tunnel(listener, guest, task_allowed, task_connections).await;
			}));
		}

		Ok(Self { mapping, accept_tasks, connection_tasks })
	}

	/// Return guest-port to local loopback endpoint mappings.
	pub fn mapping(&self) -> BTreeMap<u16, (String, u16)> {
		self.mapping.clone()
	}
}

impl Drop for TunnelSet {
	fn drop(&mut self) {
		for task in &self.accept_tasks {
			task.abort();
		}
		for task in self.connection_tasks.lock().drain(..) {
			task.abort();
		}
	}
}

/// Allocate, configure, and expose host networking for a sandbox.
pub async fn setup_sandbox_network(
	name: &str,
	ports: &[u16],
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
	inbound_cidr_allowlist: Option<&[String]>,
) -> Result<SandboxNetwork> {
	let config = allocate_guest_config(name)?;
	let policy_result = policy::EgressPolicy::new(
		policy::SystemResolver,
		egress_allow_domains.unwrap_or(&[]),
		egress_allow.unwrap_or(&[]),
	)
	.and_then(|p| {
		let dns_ips = config
			.dns
			.iter()
			.map(|s| {
				s.parse::<IpAddr>()
					.map_err(|_| EngineError::invalid(format!("invalid DNS address {s}")))
			})
			.collect::<Result<Vec<IpAddr>>>()?;
		p.approved_dns(&dns_ips)?;
		Ok(())
	});
	if let Err(err) = policy_result {
		let _ = release_guest_config(name);
		return Err(err);
	}
	let tap_result = setup_tap(
		&config.tap,
		&config.guest_ip,
		&config.host_ip,
		config.prefix,
		egress_allow,
		egress_allow_domains,
		None,
	);
	let tap = match tap_result {
		Ok(tap) => tap,
		Err(err) => {
			let _ = release_guest_config(name);
			return Err(err);
		},
	};

	let tunnels = match TunnelSet::new(&config.guest_ip, ports, inbound_cidr_allowlist).await {
		Ok(tunnels) => tunnels,
		Err(err) => {
			let _ = teardown_tap(
				&config.tap,
				Some(&config.guest_ip),
				Some(&config.host_ip),
				config.prefix,
				egress_allow,
				egress_allow_domains,
			);
			let _ = release_guest_config(name);
			return Err(err);
		},
	};
	let guest_config = GuestNetworkConfig::from(&config);
	Ok(SandboxNetwork {
		name: name.to_owned(),
		config,
		tap,
		guest_config,
		tunnels,
		egress_allow: egress_allow.map(<[String]>::to_vec),
		egress_allow_domains: egress_allow_domains.map(<[String]>::to_vec),
	})
}

/// Allocate or return the persisted guest network config for a sandbox.
pub fn allocate_guest_config(name: &str) -> Result<GuestConfig> {
	allocate_guest_config_with_dns(name, &DEFAULT_DNS)
}

/// Allocate or return the persisted guest network config with custom DNS.
pub fn allocate_guest_config_with_dns(name: &str, dns: &[&str]) -> Result<GuestConfig> {
	LeaseStore::default().allocate(name, dns)
}

/// Release a sandbox's persisted guest network config.
pub fn release_guest_config(name: &str) -> Result<()> {
	LeaseStore::default().release(name)
}

/// Return the persisted lease for a sandbox without allocating one.
pub fn lease_for(name: &str) -> Option<GuestConfig> {
	LeaseStore::default().lease_for(name)
}

/// Return the deterministic TAP name for a sandbox name.
pub fn tap_name(name: &str) -> String {
	let mut hasher = Sha1::new();
	hasher.update(name.as_bytes());
	let digest = hasher.finalize();
	format!("tv{}", hex::encode(digest)[..10].to_owned())
}

/// Configure the externally managed privileged network broker socket.
pub fn configure_broker_socket(socket: Option<PathBuf>) {
	#[cfg(target_os = "linux")]
	{
		*BROKER_SOCKET.lock() = socket;
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = socket;
	}
}

/// Return whether a privileged network broker is configured.
#[cfg(target_os = "linux")]
pub fn has_net_admin() -> bool {
	BROKER_SOCKET.lock().is_some()
}

/// Return whether a privileged network broker is configured.
#[cfg(not(target_os = "linux"))]
pub const fn has_net_admin() -> bool {
	false
}
fn broker_request(request: broker::Request) -> Result<Option<TapLease>> {
	#[cfg(target_os = "linux")]
	{
		let socket = BROKER_SOCKET
			.lock()
			.clone()
			.ok_or_else(|| EngineError::engine("network broker is not configured"))?;
		broker::call(&socket, request)
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = request;
		Err(EngineError::unsupported("host TAP networking requires Linux"))
	}
}

#[cfg(target_os = "linux")]
fn has_net_admin_direct() -> bool {
	// SAFETY: geteuid has no preconditions and reads process credentials.
	if unsafe { libc::geteuid() } == 0 {
		return true;
	}
	let Ok(status) = fs::read_to_string("/proc/self/status") else {
		return false;
	};
	for line in status.lines() {
		if let Some(value) = line.strip_prefix("CapEff:") {
			let Some(bits) = value.split_whitespace().next() else {
				return false;
			};
			let Ok(mask) = u64::from_str_radix(bits, 16) else {
				return false;
			};
			return (mask & (1_u64 << CAP_NET_ADMIN)) != 0;
		}
	}
	false
}

#[cfg(target_os = "linux")]
fn require_net_admin() -> Result<()> {
	if has_net_admin_direct() {
		Ok(())
	} else {
		Err(EngineError::engine("network setup needs root or CAP_NET_ADMIN"))
	}
}

/// Create/configure a TAP interface and its NAT/FORWARD rules.
#[cfg(any(test, target_os = "linux"))]
pub const PROTECTED_V4_RANGES: &[&str] = &[
	"0.0.0.0/8",
	"127.0.0.0/8",
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"169.254.0.0/16",
	"224.0.0.0/4",
	"255.255.255.255/32",
	"100.64.0.0/10",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"192.88.99.0/24",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"240.0.0.0/4",
];

#[cfg(any(test, target_os = "linux"))]
fn protected_deny_rule(
	action: IptablesAction,
	name: &str,
	guest_cidr: &str,
	range: &str,
	idx: usize,
) -> Vec<String> {
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		name,
		"-s",
		guest_cidr,
		"-d",
		range,
		"-m",
		"comment",
		"--comment",
		&comment(name, &format!("deny-range-{idx}")),
		"-j",
		"REJECT",
	])
}

#[cfg(target_os = "linux")]
fn credential_gateway_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let port = CREDENTIAL_GATEWAY_PORT.to_string();
	strings([
		action.flag(),
		"INPUT",
		"-i",
		&lease.name,
		"-s",
		&lease.guest_cidr(),
		"-d",
		&lease.host_ip,
		"-p",
		"tcp",
		"--dport",
		&port,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "credential-gateway"),
		"-j",
		"ACCEPT",
	])
}

#[cfg(target_os = "linux")]
fn host_deny_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	strings([
		action.flag(),
		"INPUT",
		"-i",
		&lease.name,
		"-s",
		&lease.guest_cidr(),
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "host-deny"),
		"-j",
		"DROP",
	])
}

#[cfg(target_os = "linux")]
pub(crate) fn allow_credential_gateway_direct(lease: &TapLease) -> Result<()> {
	iptables_ensure(&credential_gateway_rule(IptablesAction::Insert, lease))
}

/// Ask the configured broker to create/configure a TAP and its NAT/FORWARD
/// rules.
pub fn setup_tap(
	name: &str,
	guest_ip: &str,
	host_ip: &str,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
	previous_egress_allow: Option<&[String]>,
) -> Result<TapLease> {
	let response = broker_request(broker::Request::Setup {
		name: name.to_owned(),
		guest_ip: guest_ip.to_owned(),
		host_ip: host_ip.to_owned(),
		prefix,
		egress_allow: egress_allow.map(<[String]>::to_vec),
		egress_allow_domains: egress_allow_domains.map(<[String]>::to_vec),
		previous_egress_allow: previous_egress_allow.map(<[String]>::to_vec),
	})?;
	response.ok_or_else(|| EngineError::engine("network broker returned no TAP lease"))
}

#[cfg(target_os = "linux")]
/// Create one TAP owned by the authenticated unprivileged daemon user.
pub(crate) fn setup_tap_direct(
	name: &str,
	guest_ip: &str,
	host_ip: &str,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
	previous_egress_allow: Option<&[String]>,
	owner_uid: u32,
) -> Result<TapLease> {
	require_net_admin()?;
	let lease = TapLease::new(name, guest_ip, host_ip, prefix, egress_allow)?;

	if !ip_link_exists(name)? {
		run(
			&[
				"ip".to_owned(),
				"tuntap".to_owned(),
				"add".to_owned(),
				"dev".to_owned(),
				name.to_owned(),
				"mode".to_owned(),
				"tap".to_owned(),
				"user".to_owned(),
				owner_uid.to_string(),
			],
			true,
		)?;
	}
	run(
		&[
			"ip".to_owned(),
			"addr".to_owned(),
			"replace".to_owned(),
			lease.host_cidr(),
			"dev".to_owned(),
			name.to_owned(),
		],
		true,
	)?;
	run(&argv(["ip", "link", "set", "dev", name, "up"]), true)?;
	disable_ipv6(name)?;
	enable_ip_forward()?;

	iptables_ensure(&masq_rule(IptablesAction::Append, &lease))?;
	iptables_ensure(&return_rule(IptablesAction::Append, &lease))?;
	iptables_delete(&credential_gateway_rule(IptablesAction::Delete, &lease))?;
	iptables_delete(&host_deny_rule(IptablesAction::Delete, &lease))?;
	iptables_ensure(&host_deny_rule(IptablesAction::Insert, &lease))?;

	let domains = clean_domains(egress_allow_domains);
	let restricted = egress_allow.is_some() || !domains.is_empty();
	let prepared_domains = if restricted {
		let mut policy =
			policy::EgressPolicy::new(policy::SystemResolver, &domains, &lease.egress_allow)?;
		let now = START_INSTANT.elapsed();
		let mut rules = BTreeMap::new();
		let mut next_idx = 0;
		for domain in &domains {
			for ip in policy.resolve_allowed(domain, now)? {
				let ip = ip.to_string();
				if let Entry::Vacant(entry) = rules.entry(ip) {
					entry.insert(next_idx);
					next_idx += 1;
				}
			}
		}
		Some((rules, next_idx))
	} else {
		None
	};
	// Keep a terminal deny in place throughout every policy replacement. New
	// rules are installed around it, and unrestricted mode removes it only
	// after the protected-range denies and public allow are complete.
	iptables_ensure(&deny_egress_rule(IptablesAction::Append, &lease))?;
	let prior_domain_ips = stop_domain_refresher(name);
	iptables_delete(&unrestricted_egress_rule(IptablesAction::Delete, &lease))?;
	for (ip, idx) in &prior_domain_ips {
		iptables_delete(&domain_rule(
			IptablesAction::Delete,
			&lease.name,
			&lease.guest_cidr(),
			ip,
			*idx,
		))?;
	}
	for (idx, cidr) in previous_egress_allow.unwrap_or(&[]).iter().enumerate() {
		iptables_delete(&egress_cidr_rule(
			IptablesAction::Delete,
			&lease.name,
			&lease.guest_cidr(),
			cidr,
			idx,
		))?;
	}
	for (idx, range) in PROTECTED_V4_RANGES.iter().enumerate() {
		iptables_delete(&protected_deny_rule(
			IptablesAction::Delete,
			&lease.name,
			&lease.guest_cidr(),
			range,
			idx,
		))?;
	}

	if restricted {
		for (idx, cidr) in lease.egress_allow.iter().enumerate() {
			iptables_ensure(&egress_cidr_rule(
				IptablesAction::Insert,
				&lease.name,
				&lease.guest_cidr(),
				cidr,
				idx,
			))?;
		}
		let (rules, next_idx) = prepared_domains.expect("restricted policy was prepared");
		for (ip, idx) in &rules {
			iptables_ensure(&domain_rule(
				IptablesAction::Insert,
				&lease.name,
				&lease.guest_cidr(),
				ip,
				*idx,
			))?;
		}
		if !domains.is_empty() {
			start_domain_refresher(
				&lease.name,
				&lease.guest_cidr(),
				domains,
				rules,
				next_idx,
				lease.egress_allow.clone(),
			)?;
		}
	} else {
		for (idx, range) in PROTECTED_V4_RANGES.iter().enumerate() {
			iptables_ensure(&protected_deny_rule(
				IptablesAction::Insert,
				&lease.name,
				&lease.guest_cidr(),
				range,
				idx,
			))?;
		}
		iptables_ensure(&unrestricted_egress_rule(IptablesAction::Append, &lease))?;
		iptables_delete(&deny_egress_rule(IptablesAction::Delete, &lease))?;
	}

	Ok(lease)
}

/// Ask the configured broker to remove a TAP and its exact policy rules.
pub fn teardown_tap(
	name: &str,
	guest_ip: Option<&str>,
	host_ip: Option<&str>,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
) -> Result<()> {
	broker_request(broker::Request::Teardown {
		name: name.to_owned(),
		guest_ip: guest_ip.map(str::to_owned),
		host_ip: host_ip.map(str::to_owned),
		prefix,
		egress_allow: egress_allow.map(<[String]>::to_vec),
		egress_allow_domains: egress_allow_domains.map(<[String]>::to_vec),
	})
	.map(|_| ())
}

#[cfg(target_os = "linux")]
pub(crate) fn teardown_tap_direct(
	name: &str,
	guest_ip: Option<&str>,
	host_ip: Option<&str>,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
) -> Result<()> {
	let seen_rules = stop_domain_refresher(name);
	if !has_net_admin_direct() {
		return Err(EngineError::engine("network broker needs root or CAP_NET_ADMIN"));
	}

	if let (Some(guest_ip), Some(host_ip)) = (guest_ip, host_ip) {
		let lease = TapLease::new(name, guest_ip, host_ip, prefix, egress_allow)?;
		iptables_delete(&masq_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&return_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&credential_gateway_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&host_deny_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&unrestricted_egress_rule(IptablesAction::Delete, &lease))?;
		for (idx, range) in PROTECTED_V4_RANGES.iter().enumerate() {
			iptables_delete(&protected_deny_rule(
				IptablesAction::Delete,
				&lease.name,
				&lease.guest_cidr(),
				range,
				idx,
			))?;
		}
		for (idx, cidr) in lease.egress_allow.iter().enumerate() {
			iptables_delete(&egress_cidr_rule(
				IptablesAction::Delete,
				&lease.name,
				&lease.guest_cidr(),
				cidr,
				idx,
			))?;
		}
		iptables_delete(&deny_egress_rule(IptablesAction::Delete, &lease))?;

		let mut domain_rules = seen_rules;
		if let Some(allowed_domains) = egress_allow_domains {
			let clean = clean_domains(Some(allowed_domains));
			if let Ok(mut policy) =
				policy::EgressPolicy::new(policy::SystemResolver, &clean, &lease.egress_allow)
			{
				let now = START_INSTANT.elapsed();
				for domain in &clean {
					if let Ok(ips) = policy.resolve_allowed(domain, now) {
						for ip in ips {
							let ip_str = ip.to_string();
							domain_rules.entry(ip_str).or_insert(0);
						}
					}
				}
			}
		}
		for (ip, idx) in domain_rules {
			iptables_delete(&domain_rule(
				IptablesAction::Delete,
				&lease.name,
				&lease.guest_cidr(),
				&ip,
				idx,
			))?;
		}
	}

	if ip_link_exists(name)? {
		run(&argv(["ip", "link", "del", "dev", name]), false)?;
	}
	Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaseEntry {
	network: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	tap:     Option<String>,
}

struct LeaseStore {
	path: PathBuf,
}

impl LeaseStore {
	const fn new(path: PathBuf) -> Self {
		Self { path }
	}

	fn default() -> Self {
		Self::new(crate::home::state_dir().join("network").join("leases.json"))
	}

	fn allocate(&self, name: &str, dns: &[&str]) -> Result<GuestConfig> {
		self.update(|leases| {
			if !leases.contains_key(name) {
				let used = leases
					.values()
					.map(|entry| entry.network.as_str())
					.collect::<BTreeSet<_>>();
				let Some(network) = first_free_network(&used) else {
					return Err(EngineError::engine(format!(
						"no free /30 networks left in {POOL_BASE}/{POOL_PREFIX}"
					)));
				};
				leases.insert(name.to_owned(), LeaseEntry { network, tap: Some(tap_name(name)) });
			}
			let Some(entry) = leases.get(name) else {
				return Err(EngineError::engine("lease allocation failed"));
			};
			config_from_entry(name, entry, dns)
		})
	}

	fn release(&self, name: &str) -> Result<()> {
		self.update(|leases| {
			leases.remove(name);
			Ok(())
		})
	}

	fn lease_for(&self, name: &str) -> Option<GuestConfig> {
		let raw = fs::read_to_string(&self.path).ok()?;
		let leases = serde_json::from_str::<BTreeMap<String, LeaseEntry>>(&raw).ok()?;
		let entry = leases.get(name)?;
		config_from_entry(name, entry, &DEFAULT_DNS).ok()
	}

	fn update<T>(
		&self,
		callback: impl FnOnce(&mut BTreeMap<String, LeaseEntry>) -> Result<T>,
	) -> Result<T> {
		if let Some(parent) = self.path.parent() {
			fs::create_dir_all(parent)?;
		}
		let mut file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(&self.path)?;
		let _lock = FileLock::exclusive(&file)?;
		file.seek(SeekFrom::Start(0))?;
		let mut raw = String::new();
		file.read_to_string(&mut raw)?;
		let mut leases = if raw.trim().is_empty() {
			BTreeMap::new()
		} else {
			serde_json::from_str::<BTreeMap<String, LeaseEntry>>(&raw)?
		};
		let result = callback(&mut leases)?;
		file.seek(SeekFrom::Start(0))?;
		file.set_len(0)?;
		let json = serde_json::to_string_pretty(&leases)?;
		file.write_all(json.as_bytes())?;
		file.write_all(b"\n")?;
		Ok(result)
	}
}

struct FileLock {
	#[cfg(unix)]
	fd: std::os::fd::RawFd,
}

impl FileLock {
	fn exclusive(file: &File) -> Result<Self> {
		#[cfg(unix)]
		{
			use std::os::fd::AsRawFd;

			let fd = file.as_raw_fd();
			flock(fd, libc::LOCK_EX)?;
			Ok(Self { fd })
		}
		#[cfg(not(unix))]
		{
			let _ = file;
			Ok(Self {})
		}
	}
}

impl Drop for FileLock {
	fn drop(&mut self) {
		#[cfg(unix)]
		{
			let _ = flock(self.fd, libc::LOCK_UN);
		}
	}
}

#[cfg(unix)]
fn flock(fd: std::os::fd::RawFd, operation: libc::c_int) -> Result<()> {
	// SAFETY: flock only observes the valid file descriptor borrowed from File.
	let rc = unsafe { libc::flock(fd, operation) };
	if rc == 0 {
		Ok(())
	} else {
		Err(std::io::Error::last_os_error().into())
	}
}

async fn accept_tunnel(
	listener: TcpListener,
	guest: SocketAddr,
	allowed_nets: Arc<Vec<CidrNet>>,
	connection_tasks: Arc<Mutex<Vec<TokioJoinHandle<()>>>>,
) {
	loop {
		let Ok((client, peer)) = listener.accept().await else {
			break;
		};
		let nets = Arc::clone(&allowed_nets);
		let task = tokio::spawn(async move {
			proxy_tcp(client, peer, guest, &nets).await;
		});
		connection_tasks.lock().push(task);
	}
}

async fn proxy_tcp(
	mut client: TcpStream,
	peer: SocketAddr,
	guest: SocketAddr,
	allowed_nets: &[CidrNet],
) {
	if !ip_allowed(peer.ip(), allowed_nets) {
		return;
	}
	let Ok(mut guest_stream) = TcpStream::connect(guest).await else {
		return;
	};
	let _ = io::copy_bidirectional(&mut client, &mut guest_stream).await;
}

fn parse_cidr_allowlist(allowlist: Option<&[String]>) -> Result<Vec<CidrNet>> {
	let items = allowlist.map_or_else(|| vec!["127.0.0.0/8".to_owned()], <[String]>::to_vec);
	items.iter().map(|cidr| cidr.parse()).collect()
}

fn ip_allowed(addr: IpAddr, nets: &[CidrNet]) -> bool {
	nets.iter().any(|net| net.contains(addr))
}

fn parse_ip(value: &str) -> Result<IpAddr> {
	value
		.parse::<IpAddr>()
		.map_err(|_| EngineError::invalid(format!("invalid IP address {value}")))
}

fn parse_ipv4(value: &str) -> Result<Ipv4Addr> {
	match parse_ip(value)? {
		IpAddr::V4(addr) => Ok(addr),
		IpAddr::V6(_) => Err(EngineError::invalid("sandbox TAP networking supports IPv4 only")),
	}
}

fn first_free_network(used: &BTreeSet<&str>) -> Option<String> {
	let base = u32::from(POOL_BASE);
	for offset in (0..POOL_SIZE).step_by(LEASE_STEP as usize) {
		let network = Ipv4Addr::from(base + offset);
		let candidate = format!("{network}/{LEASE_PREFIX}");
		if !used.contains(candidate.as_str()) {
			return Some(candidate);
		}
	}
	None
}

fn config_from_entry(name: &str, entry: &LeaseEntry, dns: &[&str]) -> Result<GuestConfig> {
	let cidr = entry.network.parse::<CidrNet>()?;
	let (network, prefix) = match cidr {
		CidrNet::V4 { network, prefix } => (network, prefix),
		CidrNet::V6 { .. } => {
			return Err(EngineError::invalid("sandbox TAP networking supports IPv4 only"));
		},
	};
	let host_ip = Ipv4Addr::from(u32::from(network) + 1);
	let guest_ip = Ipv4Addr::from(u32::from(network) + 2);
	Ok(GuestConfig {
		tap: entry.tap.clone().unwrap_or_else(|| tap_name(name)),
		host_ip: host_ip.to_string(),
		guest_ip: guest_ip.to_string(),
		prefix,
		network: format!("{network}/{prefix}"),
		dns: dns.iter().map(|value| (*value).to_owned()).collect(),
	})
}

fn ipv4_network(addr: Ipv4Addr, prefix: u8) -> Result<Ipv4Addr> {
	if prefix > 32 {
		return Err(EngineError::invalid(format!("invalid IPv4 prefix {prefix}")));
	}
	Ok(Ipv4Addr::from(u32::from(addr) & ipv4_mask(prefix)))
}

fn ipv4_contains(network: Ipv4Addr, prefix: u8, addr: Ipv4Addr) -> bool {
	(u32::from(addr) & ipv4_mask(prefix)) == u32::from(network)
}

const fn ipv4_mask(prefix: u8) -> u32 {
	if prefix == 0 {
		0
	} else {
		u32::MAX << (32 - prefix)
	}
}

fn ipv6_network(addr: Ipv6Addr, prefix: u8) -> Result<Ipv6Addr> {
	if prefix > 128 {
		return Err(EngineError::invalid(format!("invalid IPv6 prefix {prefix}")));
	}
	let raw = u128::from(addr);
	let mask = if prefix == 0 {
		0
	} else {
		u128::MAX << (128 - prefix)
	};
	Ok(Ipv6Addr::from(raw & mask))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CidrNet {
	V4 { network: Ipv4Addr, prefix: u8 },
	V6 { network: Ipv6Addr, prefix: u8 },
}

impl CidrNet {
	fn contains(&self, addr: IpAddr) -> bool {
		match (self, addr) {
			(Self::V4 { network, prefix }, IpAddr::V4(addr)) => ipv4_contains(*network, *prefix, addr),
			(Self::V6 { network, prefix }, IpAddr::V6(addr)) => {
				let mask = if *prefix == 0 {
					0
				} else {
					u128::MAX << (128 - *prefix)
				};
				(u128::from(addr) & mask) == u128::from(*network)
			},
			_ => false,
		}
	}
}

impl std::str::FromStr for CidrNet {
	type Err = EngineError;

	fn from_str(value: &str) -> Result<Self> {
		let value = value.trim();
		if value.is_empty() {
			return Err(EngineError::invalid("empty CIDR"));
		}
		let (addr_s, prefix_s) = value
			.split_once('/')
			.map_or((value, None), |(addr, prefix)| (addr, Some(prefix)));
		let addr = parse_ip(addr_s)?;
		match addr {
			IpAddr::V4(addr) => {
				let prefix = parse_prefix(prefix_s, 32)?;
				Ok(Self::V4 { network: ipv4_network(addr, prefix)?, prefix })
			},
			IpAddr::V6(addr) => {
				let prefix = parse_prefix(prefix_s, 128)?;
				Ok(Self::V6 { network: ipv6_network(addr, prefix)?, prefix })
			},
		}
	}
}

impl fmt::Display for CidrNet {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::V4 { network, prefix } => write!(f, "{network}/{prefix}"),
			Self::V6 { network, prefix } => write!(f, "{network}/{prefix}"),
		}
	}
}

fn parse_prefix(prefix: Option<&str>, max: u8) -> Result<u8> {
	let Some(prefix) = prefix else {
		return Ok(max);
	};
	let parsed = prefix
		.parse::<u8>()
		.map_err(|_| EngineError::invalid(format!("invalid CIDR prefix {prefix}")))?;
	if parsed > max {
		return Err(EngineError::invalid(format!("invalid CIDR prefix {prefix}")));
	}
	Ok(parsed)
}

#[cfg(any(test, target_os = "linux"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IptablesAction {
	Append,
	Delete,
	Insert,
}

#[cfg(any(test, target_os = "linux"))]
impl IptablesAction {
	const fn flag(self) -> &'static str {
		match self {
			Self::Append => "-A",
			Self::Delete => "-D",
			Self::Insert => "-I",
		}
	}
}

#[cfg(any(test, target_os = "linux"))]
fn masq_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	strings([
		"-t",
		"nat",
		action.flag(),
		"POSTROUTING",
		"-s",
		&lease.network,
		"!",
		"-d",
		&lease.network,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "masq"),
		"-j",
		"MASQUERADE",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn return_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let guest_cidr = lease.guest_cidr();
	strings([
		action.flag(),
		"FORWARD",
		"-o",
		&lease.name,
		"-d",
		&guest_cidr,
		"-m",
		"conntrack",
		"--ctstate",
		"ESTABLISHED,RELATED",
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "return"),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn unrestricted_egress_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let guest_cidr = lease.guest_cidr();
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		&lease.name,
		"-s",
		&guest_cidr,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "egress"),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn egress_cidr_rule(
	action: IptablesAction,
	name: &str,
	guest_cidr: &str,
	cidr: &str,
	idx: usize,
) -> Vec<String> {
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		name,
		"-s",
		guest_cidr,
		"-d",
		cidr,
		"-m",
		"comment",
		"--comment",
		&comment(name, &format!("egress-{idx}")),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn deny_egress_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let guest_cidr = lease.guest_cidr();
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		&lease.name,
		"-s",
		&guest_cidr,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "egress-deny"),
		"-j",
		"REJECT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn domain_rule(
	action: IptablesAction,
	name: &str,
	guest_cidr: &str,
	ip: &str,
	idx: usize,
) -> Vec<String> {
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		name,
		"-s",
		guest_cidr,
		"-d",
		&format!("{ip}/32"),
		"-m",
		"comment",
		"--comment",
		&comment(name, &format!("domain-{idx}")),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn comment(name: &str, tag: &str) -> String {
	format!("{RULE_PREFIX}:{name}:{tag}")
}

#[cfg(any(test, target_os = "linux"))]
fn iptables_check_form(rule: &[String]) -> Result<Vec<String>> {
	let mut converted = rule.to_vec();
	for value in &mut converted {
		if value == "-A" || value == "-D" || value == "-I" {
			"-C".clone_into(value);
			return Ok(converted);
		}
	}
	Err(EngineError::invalid(format!("iptables rule missing -A/-D/-I: {}", rule.join(" "))))
}

#[cfg(any(test, target_os = "linux"))]
fn strings<const N: usize>(items: [&str; N]) -> Vec<String> {
	items.into_iter().map(str::to_owned).collect()
}

#[cfg(target_os = "linux")]
fn clean_domains(domains: Option<&[String]>) -> Vec<String> {
	domains
		.unwrap_or(&[])
		.iter()
		.map(|domain| domain.trim())
		.filter(|domain| !domain.is_empty())
		.map(str::to_owned)
		.collect()
}

#[cfg(target_os = "linux")]
fn start_domain_refresher(
	name: &str,
	guest_cidr: &str,
	domains: Vec<String>,
	rules: BTreeMap<String, usize>,
	next_idx: usize,
	trusted: Vec<String>,
) -> Result<()> {
	let refresher =
		DomainRefresher::new(name, guest_cidr, domains, rules, next_idx, trusted).start()?;
	REFRESHERS.lock().insert(name.to_owned(), refresher);
	Ok(())
}

#[cfg(target_os = "linux")]
fn stop_domain_refresher(name: &str) -> BTreeMap<String, usize> {
	let refresher = REFRESHERS.lock().remove(name);
	refresher.map_or_else(BTreeMap::new, DomainRefresher::stop)
}

#[cfg(any(test, target_os = "linux"))]
struct DomainRefresher {
	name:       String,
	guest_cidr: String,
	domains:    Vec<String>,
	trusted:    Vec<String>,
	rules:      Arc<Mutex<BTreeMap<String, usize>>>,
	next_idx:   Arc<Mutex<usize>>,
	stop_tx:    mpsc::Sender<()>,
	stop_rx:    Option<mpsc::Receiver<()>>,
	thread:     Option<JoinHandle<()>>,
}

#[cfg(any(test, target_os = "linux"))]
impl DomainRefresher {
	const INTERVAL: Duration = Duration::from_mins(1);

	fn new(
		name: &str,
		guest_cidr: &str,
		domains: Vec<String>,
		rules: BTreeMap<String, usize>,
		next_idx: usize,
		trusted: Vec<String>,
	) -> Self {
		let (stop_tx, stop_rx) = mpsc::channel();
		Self {
			name: name.to_owned(),
			guest_cidr: guest_cidr.to_owned(),
			domains,
			trusted,
			rules: Arc::new(Mutex::new(rules)),
			next_idx: Arc::new(Mutex::new(next_idx)),
			stop_tx,
			stop_rx: Some(stop_rx),
			thread: None,
		}
	}

	fn start(mut self) -> Result<Self> {
		let Some(stop_rx) = self.stop_rx.take() else {
			return Err(EngineError::engine("domain refresher already started"));
		};
		let name = self.name.clone();
		#[cfg(target_os = "linux")]
		let guest_cidr = self.guest_cidr.clone();
		#[cfg(not(target_os = "linux"))]
		let _ = &self.guest_cidr;
		let domains = self.domains.clone();
		let trusted = self.trusted.clone();
		let rules = Arc::clone(&self.rules);
		let next_idx = Arc::clone(&self.next_idx);
		let thread_name = format!("vmon-egress-dns-{name}");
		let thread = thread::Builder::new().name(thread_name).spawn(move || {
			let Ok(mut policy) = policy::EgressPolicy::new(policy::SystemResolver, &domains, &trusted)
			else {
				return;
			};
			loop {
				match stop_rx.recv_timeout(Self::INTERVAL) {
					Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
					Err(mpsc::RecvTimeoutError::Timeout) => {},
				}
				let now = START_INSTANT.elapsed();
				let mut current_ips = BTreeSet::new();
				let mut refresh_failed = false;
				for domain in &domains {
					if let Ok(ips) = policy.resolve_allowed(domain, now) {
						for ip in ips {
							current_ips.insert(ip.to_string());
						}
					} else {
						refresh_failed = true;
						break;
					}
				}

				if refresh_failed {
					#[cfg(target_os = "linux")]
					{
						let to_delete = {
							let rules_guard = rules.lock();
							rules_guard.clone()
						};
						for (ip, idx) in to_delete {
							let _ = iptables_delete(&domain_rule(
								IptablesAction::Delete,
								&name,
								&guest_cidr,
								&ip,
								idx,
							));
						}
					}
					{
						let mut rules_guard = rules.lock();
						rules_guard.clear();
					}
					break;
				}

				let mut to_add = Vec::new();
				let mut to_remove = Vec::new();

				{
					let rules_guard = rules.lock();
					for ip in rules_guard.keys() {
						if !current_ips.contains(ip) {
							to_remove.push(ip.clone());
						}
					}
					for ip in &current_ips {
						if !rules_guard.contains_key(ip) {
							to_add.push(ip.clone());
						}
					}
				}

				// 1. Add refreshed/new rules FIRST (only on success, update rules map after)
				for ip in to_add {
					let idx = {
						let mut next_idx_guard = next_idx.lock();
						let current = *next_idx_guard;
						*next_idx_guard += 1;
						current
					};
					#[cfg(target_os = "linux")]
					let res = iptables_ensure(&domain_rule(IptablesAction::Insert, &name, &guest_cidr, &ip, idx));
					#[cfg(not(target_os = "linux"))]
					let res: Result<(), EngineError> = Ok(());

					if res.is_ok() {
						let mut rules_guard = rules.lock();
						rules_guard.insert(ip.clone(), idx);
					}
				}

				// 2. Delete stale rules SECOND (only on success, update rules map after)
				for ip in to_remove {
					let idx = {
						let rules_guard = rules.lock();
						rules_guard.get(&ip).copied()
					};
					if let Some(idx) = idx {
						#[cfg(not(target_os = "linux"))]
						let _ = idx;
						#[cfg(target_os = "linux")]
						let res = iptables_delete(&domain_rule(
							IptablesAction::Delete,
							&name,
							&guest_cidr,
							&ip,
							idx,
						));
						#[cfg(not(target_os = "linux"))]
						let res: Result<(), EngineError> = Ok(());

						if res.is_ok() {
							let mut rules_guard = rules.lock();
							rules_guard.remove(&ip);
						}
					}
				}
			}
		})?;
		self.thread = Some(thread);
		Ok(self)
	}

	fn stop(mut self) -> BTreeMap<String, usize> {
		let _ = self.stop_tx.send(());
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
		let rules = self.rules.lock().clone();
		#[cfg(target_os = "linux")]
		for (ip, idx) in &rules {
			let _ = iptables_delete(&domain_rule(
				IptablesAction::Delete,
				&self.name,
				&self.guest_cidr,
				ip,
				*idx,
			));
		}
		rules
	}
}

#[cfg(target_os = "linux")]
fn disable_ipv6(name: &str) -> Result<()> {
	let path = PathBuf::from("/proc/sys/net/ipv6/conf").join(name).join("disable_ipv6");
	fs::write(&path, "1\n").map_err(|error| {
		EngineError::engine(format!("disabling IPv6 on sandbox TAP {name}: {error}"))
	})
}

#[cfg(target_os = "linux")]
fn enable_ip_forward() -> Result<()> {
	let current = fs::read_to_string(IP_FORWARD)
		.map_err(|err| EngineError::engine(format!("enabling IPv4 forwarding: {err}")))?;
	if current.trim() == "1" {
		return Ok(());
	}
	fs::write(IP_FORWARD, "1\n")
		.map_err(|err| EngineError::engine(format!("enabling IPv4 forwarding: {err}")))
}

#[cfg(target_os = "linux")]
fn ip_link_exists(name: &str) -> Result<bool> {
	Ok(run(&argv(["ip", "link", "show", "dev", name]), false)?.success())
}

#[cfg(target_os = "linux")]
fn iptables_ensure(rule: &[String]) -> Result<()> {
	let check_rule = iptables_check_form(rule)?;
	if run_iptables(&check_rule, false)?.success() {
		return Ok(());
	}
	run_iptables(rule, true)?;
	Ok(())
}

#[cfg(target_os = "linux")]
fn iptables_delete(rule: &[String]) -> Result<()> {
	while run_iptables(rule, false)?.success() {}
	Ok(())
}

#[cfg(target_os = "linux")]
fn run_iptables(rule: &[String], check: bool) -> Result<RunOutput> {
	let mut command = Vec::with_capacity(rule.len() + 1);
	command.push("iptables".to_owned());
	command.extend(rule.iter().cloned());
	run(&command, check)
}

#[cfg(target_os = "linux")]
fn argv<const N: usize>(items: [&str; N]) -> Vec<String> {
	items.into_iter().map(str::to_owned).collect()
}

#[cfg(target_os = "linux")]
struct RunOutput {
	code: i32,
}

#[cfg(target_os = "linux")]
impl RunOutput {
	const fn success(&self) -> bool {
		self.code == 0
	}
}

#[cfg(target_os = "linux")]
fn run(argv: &[String], check: bool) -> Result<RunOutput> {
	let Some(program) = argv.first() else {
		return Err(EngineError::engine("empty command"));
	};
	let output = Command::new(program)
		.args(&argv[1..])
		.output()
		.map_err(|err| {
			if err.kind() == std::io::ErrorKind::NotFound {
				EngineError::engine(format!("required command not found: {program}"))
			} else {
				EngineError::engine(err.to_string())
			}
		})?;
	let code = output.status.code().unwrap_or(-1);
	if check && !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		let detail = if stderr.trim().is_empty() {
			stdout.trim()
		} else {
			stderr.trim()
		};
		return Err(EngineError::engine(format!("{} failed: {detail}", argv.join(" "))));
	}
	Ok(RunOutput { code })
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lease_allocation_persistence_release_roundtrip() {
		let dir = tempfile::tempdir().unwrap();
		let store = LeaseStore::new(dir.path().join("network").join("leases.json"));

		let first = store.allocate("alpha", &DEFAULT_DNS).unwrap();
		assert_eq!(first.network, "172.20.0.0/30");
		assert_eq!(first.host_ip, "172.20.0.1");
		assert_eq!(first.guest_ip, "172.20.0.2");
		assert_eq!(first.prefix, 30);
		assert_eq!(first.tap, tap_name("alpha"));

		let second = store.allocate("beta", &DEFAULT_DNS).unwrap();
		assert_eq!(second.network, "172.20.0.4/30");
		assert_eq!(store.allocate("alpha", &DEFAULT_DNS).unwrap(), first);
		assert_eq!(store.lease_for("alpha").as_ref(), Some(&first));

		let raw = fs::read_to_string(dir.path().join("network").join("leases.json")).unwrap();
		let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
		assert_eq!(value["alpha"]["network"], "172.20.0.0/30");
		assert_eq!(value["alpha"]["tap"], tap_name("alpha"));

		store.release("alpha").unwrap();
		assert_eq!(store.lease_for("alpha"), None);
		let reused = store.allocate("gamma", &DEFAULT_DNS).unwrap();
		assert_eq!(reused.network, "172.20.0.0/30");
		assert_eq!(store.lease_for("beta"), Some(second));
	}

	#[test]
	fn tap_name_derivation_matches_python() {
		assert_eq!(tap_name("sandbox"), "tv9ed037b849");
		assert_eq!(tap_name("vm-1"), "tv764646a060");
	}

	#[test]
	fn cidr_allowlist_matches_boundaries() {
		let cidrs =
			["192.168.1.0/24".to_owned(), "10.0.0.5/32".to_owned(), "2001:db8::/126".to_owned()];
		let nets = parse_cidr_allowlist(Some(&cidrs)).unwrap();
		assert!(ip_allowed("192.168.1.0".parse().unwrap(), &nets));
		assert!(ip_allowed("192.168.1.255".parse().unwrap(), &nets));
		assert!(!ip_allowed("192.168.2.0".parse().unwrap(), &nets));
		assert!(ip_allowed("10.0.0.5".parse().unwrap(), &nets));
		assert!(!ip_allowed("10.0.0.6".parse().unwrap(), &nets));
		assert!(ip_allowed("2001:db8::3".parse().unwrap(), &nets));
		assert!(!ip_allowed("2001:db8::4".parse().unwrap(), &nets));

		let default = parse_cidr_allowlist(None).unwrap();
		assert!(ip_allowed("127.255.255.255".parse().unwrap(), &default));
		assert!(!ip_allowed("128.0.0.1".parse().unwrap(), &default));
	}

	#[test]
	fn iptables_rule_argv_is_symmetric() {
		let allow = vec!["0.0.0.0/0".to_owned()];
		let lease = TapLease::new("tvabc123", "172.20.0.2", "172.20.0.1", 30, Some(&allow)).unwrap();

		let masq_add = masq_rule(IptablesAction::Append, &lease);
		assert_eq!(masq_add, vec![
			"-t",
			"nat",
			"-A",
			"POSTROUTING",
			"-s",
			"172.20.0.0/30",
			"!",
			"-d",
			"172.20.0.0/30",
			"-m",
			"comment",
			"--comment",
			"vmon:tvabc123:masq",
			"-j",
			"MASQUERADE",
		]);
		let mut masq_delete = masq_add.clone();
		"-D".clone_into(&mut masq_delete[2]);
		assert_eq!(masq_rule(IptablesAction::Delete, &lease), masq_delete);

		let mut return_delete = return_rule(IptablesAction::Append, &lease);
		"-D".clone_into(&mut return_delete[0]);
		assert_eq!(return_rule(IptablesAction::Delete, &lease), return_delete);

		let mut unrestricted_delete = unrestricted_egress_rule(IptablesAction::Append, &lease);
		"-D".clone_into(&mut unrestricted_delete[0]);
		assert_eq!(unrestricted_egress_rule(IptablesAction::Delete, &lease), unrestricted_delete);

		let mut cidr_delete =
			egress_cidr_rule(IptablesAction::Append, &lease.name, &lease.guest_cidr(), "0.0.0.0/0", 0);
		"-D".clone_into(&mut cidr_delete[0]);
		assert_eq!(
			egress_cidr_rule(IptablesAction::Delete, &lease.name, &lease.guest_cidr(), "0.0.0.0/0", 0,),
			cidr_delete
		);

		let mut deny_delete = deny_egress_rule(IptablesAction::Append, &lease);
		"-D".clone_into(&mut deny_delete[0]);
		assert_eq!(deny_egress_rule(IptablesAction::Delete, &lease), deny_delete);

		let check = iptables_check_form(&masq_add).unwrap();
		let mut expected_check = masq_add;
		"-C".clone_into(&mut expected_check[2]);
		assert_eq!(check, expected_check);

		let domain_insert =
			domain_rule(IptablesAction::Insert, &lease.name, &lease.guest_cidr(), "203.0.113.9", 0);
		assert_eq!(domain_insert[0], "-I");
		assert_eq!(iptables_check_form(&domain_insert).unwrap()[0], "-C");
		let mut domain_delete = domain_insert;
		"-D".clone_into(&mut domain_delete[0]);
		assert_eq!(
			domain_rule(IptablesAction::Delete, &lease.name, &lease.guest_cidr(), "203.0.113.9", 0,),
			domain_delete
		);
	}

	#[test]
	fn test_domain_refresher_rule_indexing_state() {
		let rules = BTreeMap::from([("1.1.1.1".to_owned(), 0), ("8.8.8.8".to_owned(), 1)]);
		let next_idx = 2;
		let refresher = DomainRefresher::new(
			"test-sb",
			"10.0.2.15/32",
			vec!["api.example.com".to_owned()],
			rules,
			next_idx,
			vec![],
		)
		.start()
		.unwrap();
		assert_eq!(refresher.rules.lock().get("1.1.1.1"), Some(&0));
		assert_eq!(refresher.rules.lock().get("8.8.8.8"), Some(&1));
		assert_eq!(*refresher.next_idx.lock(), 2);

		let stopped_rules = refresher.stop();
		assert_eq!(stopped_rules.get("1.1.1.1"), Some(&0));
		assert_eq!(stopped_rules.get("8.8.8.8"), Some(&1));
	}

	#[test]
	fn test_protected_deny_rule_argv() {
		let rule =
			protected_deny_rule(IptablesAction::Append, "test-sb", "10.0.2.15/32", "127.0.0.0/8", 3);
		assert_eq!(rule, vec![
			"-A".to_owned(),
			"FORWARD".to_owned(),
			"-i".to_owned(),
			"test-sb".to_owned(),
			"-s".to_owned(),
			"10.0.2.15/32".to_owned(),
			"-d".to_owned(),
			"127.0.0.0/8".to_owned(),
			"-m".to_owned(),
			"comment".to_owned(),
			"--comment".to_owned(),
			"vmon:test-sb:deny-range-3".to_owned(),
			"-j".to_owned(),
			"REJECT".to_owned(),
		]);
	}

	#[test]
	fn test_domain_rule_argv() {
		let rule = domain_rule(IptablesAction::Insert, "test-sb", "10.0.2.15/32", "1.1.1.1", 5);
		assert_eq!(rule, vec![
			"-I".to_owned(),
			"FORWARD".to_owned(),
			"-i".to_owned(),
			"test-sb".to_owned(),
			"-s".to_owned(),
			"10.0.2.15/32".to_owned(),
			"-d".to_owned(),
			"1.1.1.1/32".to_owned(),
			"-m".to_owned(),
			"comment".to_owned(),
			"--comment".to_owned(),
			"vmon:test-sb:domain-5".to_owned(),
			"-j".to_owned(),
			"ACCEPT".to_owned(),
		]);
	}
}
