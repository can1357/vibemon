//! Mesh state, membership persistence, and local host capability probes.
//!
//! The node-to-node wire keys in this module intentionally mirror
//! `python/vmon/mesh.py`: `mesh.json` never stores the cluster token, and
//! [`NodeState::to_wire`] carries the same placement snapshot fields peers
//! exchange during gossip.

use std::{
	collections::BTreeMap,
	fs, io,
	net::UdpSocket,
	path::{Path, PathBuf},
	sync::{LazyLock, Mutex},
	thread,
};

use base64::{Engine as _, engine::general_purpose};
use serde_json::{Map, Value, json};

use crate::{home::Home, image::normalize_oci_arch};

/// `$VMON_HOME/mesh.json`, the durable membership file.
pub const MESH_STATE_FILE: &str = "mesh.json";

const KNOWN_MESH_CODES: &[&str] = &[
	"invalid",
	"unauthorized",
	"unreachable",
	"ambiguous",
	"conflict",
	"unplaceable",
	"arch_required",
	"record_unreplicated",
	"unsupported",
];

static CPU_BASELINE_CACHE: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));

/// A mesh operation failed with a stable machine-readable code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeshError {
	/// Stable wire code used by mesh HTTP error mapping.
	pub code:    String,
	/// Human-readable detail.
	pub message: String,
}

impl MeshError {
	/// Build a mesh error. When `code` is omitted, Python's rule is preserved:
	/// a known message literal doubles as the code, otherwise the code is
	/// `"invalid"`.
	pub fn new(message: impl Into<String>, code: Option<&str>) -> Self {
		let message = message.into();
		let code = code.map_or_else(
			|| {
				if KNOWN_MESH_CODES.contains(&message.as_str()) {
					message.clone()
				} else {
					"invalid".to_owned()
				}
			},
			ToOwned::to_owned,
		);
		Self { code, message }
	}

	/// Build an `"invalid"` mesh error.
	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new(message, Some("invalid"))
	}

	/// Build an `"unauthorized"` mesh error.
	pub fn unauthorized(message: impl Into<String>) -> Self {
		Self::new(message, Some("unauthorized"))
	}

	/// Build an `"unplaceable"` mesh error.
	pub fn unplaceable(message: impl Into<String>) -> Self {
		Self::new(message, Some("unplaceable"))
	}

	/// Build an `"arch_required"` mesh error.
	pub fn arch_required(message: impl Into<String>) -> Self {
		Self::new(message, Some("arch_required"))
	}
}

impl std::fmt::Display for MeshError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.message)
	}
}

impl std::error::Error for MeshError {}

impl From<io::Error> for MeshError {
	fn from(error: io::Error) -> Self {
		Self::invalid(error.to_string())
	}
}

impl From<serde_json::Error> for MeshError {
	fn from(error: serde_json::Error) -> Self {
		Self::invalid(error.to_string())
	}
}

/// Advertised hard capacity for placement decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeCaps {
	/// Advertised vCPU capacity.
	pub vcpus:   u64,
	/// Advertised memory capacity in MiB.
	pub mem_mib: u64,
}

impl NodeCaps {
	/// Build a capacity snapshot.
	pub const fn new(vcpus: u64, mem_mib: u64) -> Self {
		Self { vcpus, mem_mib }
	}
}

impl Default for NodeCaps {
	fn default() -> Self {
		Self { vcpus: 1, mem_mib: 2048 }
	}
}

/// A gossiped snapshot of one mesh member's placement state.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeState {
	pub node_id:            String,
	pub advertise:          String,
	pub region:             String,
	pub caps:               NodeCaps,
	pub committed_vcpus:    u64,
	pub committed_mem_mib:  u64,
	pub inflight:           u64,
	pub templates:          Vec<String>,
	/// Function package, image-input, and snapshot digests available locally.
	pub function_artifacts: Vec<String>,
	pub pools:              BTreeMap<String, u64>,
	pub owned:              Vec<String>,
	pub owned_epochs:       BTreeMap<String, u64>,
	pub ts:                 f64,
	pub last_seen:          f64,
	pub backend:            String,
	pub arch:               String,
	pub cpu_baseline:       String,
	pub template_index:     BTreeMap<String, String>,
}

impl NodeState {
	/// Build the minimal default peer state for a node id and advertise URL.
	pub fn new(node_id: impl Into<String>, advertise: impl Into<String>) -> Self {
		Self {
			node_id:            node_id.into(),
			advertise:          advertise.into(),
			region:             String::new(),
			caps:               NodeCaps::default(),
			committed_vcpus:    0,
			committed_mem_mib:  0,
			inflight:           0,
			templates:          Vec::new(),
			function_artifacts: Vec::new(),
			pools:              BTreeMap::new(),
			owned:              Vec::new(),
			owned_epochs:       BTreeMap::new(),
			ts:                 0.0,
			last_seen:          0.0,
			backend:            String::new(),
			arch:               String::new(),
			cpu_baseline:       String::new(),
			template_index:     BTreeMap::new(),
		}
	}

	/// Return currently uncommitted vCPU capacity.
	pub const fn free_vcpus(&self) -> u64 {
		self.caps.vcpus.saturating_sub(self.committed_vcpus)
	}

	/// Return currently uncommitted memory capacity in MiB.
	pub const fn free_mem_mib(&self) -> u64 {
		self.caps.mem_mib.saturating_sub(self.committed_mem_mib)
	}

	/// Return whether this peer has been seen within three heartbeat windows.
	pub fn healthy(&self, now: f64, interval: f64) -> bool {
		self.last_seen > 0.0 && now - self.last_seen < 3.0 * interval
	}

	/// Serialize this state for mesh HTTP gossip.
	pub fn to_wire(&self) -> Value {
		json!({
			"node_id": self.node_id,
			"advertise": self.advertise,
			"region": self.region,
			"backend": self.backend,
			"arch": self.arch,
			"cpu_baseline": self.cpu_baseline,
			"vcpus": self.caps.vcpus,
			"mem_mib": self.caps.mem_mib,
			"committed_vcpus": self.committed_vcpus,
			"committed_mem_mib": self.committed_mem_mib,
			"inflight": self.inflight,
			"templates": self.templates,
			"function_artifacts": self.function_artifacts,
			"template_index": self.template_index,
			"pools": self.pools,
			"owned": self.owned,
			"owned_epochs": self.owned_epochs,
			"ts": self.ts,
			"last_seen": self.last_seen,
			"free_vcpus": self.free_vcpus(),
			"free_mem_mib": self.free_mem_mib(),
		})
	}

	/// Deserialize a gossiped node state from JSON-compatible data.
	pub fn from_wire(data: &Value) -> Result<Self, MeshError> {
		let object = data
			.as_object()
			.ok_or_else(|| MeshError::invalid("node state must be an object"))?;
		let caps_raw = object.get("caps").and_then(Value::as_object);
		let caps = NodeCaps {
			vcpus:   object
				.get("vcpus")
				.and_then(value_u64)
				.or_else(|| {
					caps_raw
						.and_then(|caps| caps.get("vcpus"))
						.and_then(value_u64)
				})
				.unwrap_or(1),
			mem_mib: object
				.get("mem_mib")
				.and_then(value_u64)
				.or_else(|| {
					caps_raw
						.and_then(|caps| caps.get("mem_mib"))
						.and_then(value_u64)
				})
				.unwrap_or(2048),
		};

		Ok(Self {
			node_id: string_field(object, "node_id"),
			advertise: string_field(object, "advertise"),
			region: string_field(object, "region"),
			backend: string_field(object, "backend"),
			arch: string_field(object, "arch"),
			cpu_baseline: string_field(object, "cpu_baseline"),
			caps,
			committed_vcpus: object
				.get("committed_vcpus")
				.and_then(value_u64)
				.unwrap_or(0),
			committed_mem_mib: object
				.get("committed_mem_mib")
				.and_then(value_u64)
				.unwrap_or(0),
			inflight: object.get("inflight").and_then(value_u64).unwrap_or(0),
			templates: string_vec(object.get("templates")),
			function_artifacts: string_vec(object.get("function_artifacts")),
			pools: u64_map(object.get("pools")),
			template_index: string_map(object.get("template_index")),
			owned: string_vec(object.get("owned")),
			owned_epochs: u64_map(object.get("owned_epochs")),
			ts: object.get("ts").and_then(value_f64).unwrap_or(0.0),
			last_seen: object.get("last_seen").and_then(value_f64).unwrap_or(0.0),
		})
	}
}

/// Durable membership state from `$VMON_HOME/mesh.json`.
#[derive(Clone, Debug, PartialEq)]
pub struct MembershipState {
	pub enabled:          bool,
	pub node_id:          String,
	pub advertise:        String,
	pub region:           String,
	pub caps:             NodeCaps,
	pub peers:            BTreeMap<String, NodeState>,
	pub expected_members: usize,
}

impl MembershipState {
	/// Build an enabled membership snapshot.
	pub fn enabled(
		node_id: impl Into<String>,
		advertise: impl Into<String>,
		region: impl Into<String>,
		caps: NodeCaps,
	) -> Self {
		Self {
			enabled: true,
			node_id: node_id.into(),
			advertise: advertise.into(),
			region: region.into(),
			caps,
			peers: BTreeMap::new(),
			expected_members: 1,
		}
	}

	/// Return the strict majority required for the expected cluster size.
	pub const fn quorum_needed(&self) -> usize {
		self.expected_members / 2 + 1
	}

	/// Return whether unreachable confirmations meet restore quorum.
	pub const fn restore_quorum_met(&self, confirmations: usize) -> bool {
		confirmations >= self.quorum_needed()
	}

	/// Raise expected cluster size to the current known membership high-water
	/// mark. Reap/partition paths must not shrink it.
	pub fn bump_expected(&mut self) -> bool {
		let before = self.expected_members;
		self.expected_members = self.expected_members.max(1 + self.peers.len());
		self.expected_members != before
	}

	/// Forget a peer that announced it is leaving and decrement expected cluster
	/// size with a floor of one.
	pub fn depart(&mut self, node_id: &str) -> bool {
		let removed = self.peers.remove(node_id).is_some();
		self.expected_members = self.expected_members.saturating_sub(1).max(1);
		removed
	}

	/// Return this node plus peers that are healthy at `now`.
	pub fn live_member_ids(&self, now: f64, interval: f64) -> Vec<String> {
		let mut members = vec![self.node_id.clone()];
		members.extend(
			self
				.peers
				.values()
				.filter(|peer| peer.healthy(now, interval))
				.map(|peer| peer.node_id.clone()),
		);
		members
	}

	/// Return whether `node_id` is this node or a healthy peer.
	pub fn is_member_healthy(&self, node_id: &str, now: f64, interval: f64) -> bool {
		if node_id == self.node_id {
			return true;
		}
		self
			.peers
			.get(node_id)
			.is_some_and(|peer| peer.healthy(now, interval))
	}

	/// Load persisted mesh membership without reading any token from disk.
	/// Malformed, disabled, or absent files return `Ok(None)` to match the
	/// Python server's best-effort startup behavior.
	pub fn load(path: &Path, fallback_caps: NodeCaps, now: f64) -> Result<Option<Self>, MeshError> {
		let raw = match fs::read_to_string(path) {
			Ok(raw) => raw,
			Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(_) => return Ok(None),
		};
		let data: Value = match serde_json::from_str(&raw) {
			Ok(data) => data,
			Err(_) => return Ok(None),
		};
		let Some(object) = data.as_object() else {
			return Ok(None);
		};
		let node_id = object
			.get("node_id")
			.and_then(Value::as_str)
			.unwrap_or_default();
		if node_id.is_empty() || object.get("enabled").and_then(Value::as_bool) != Some(true) {
			return Ok(None);
		}

		let mut peers = BTreeMap::new();
		for item in object
			.get("peers")
			.and_then(Value::as_array)
			.into_iter()
			.flatten()
		{
			let mut node = NodeState::from_wire(item)?;
			if !node.node_id.is_empty() && node.node_id != node_id {
				if node.last_seen <= 0.0 {
					node.last_seen = now;
				}
				peers.insert(node.node_id.clone(), node);
			}
		}

		Ok(Some(Self {
			enabled: true,
			node_id: node_id.to_owned(),
			advertise: object
				.get("advertise")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_owned(),
			region: object
				.get("region")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_owned(),
			caps: NodeCaps {
				vcpus:   object
					.get("vcpus")
					.and_then(value_u64)
					.unwrap_or(fallback_caps.vcpus),
				mem_mib: object
					.get("mem_mib")
					.and_then(value_u64)
					.unwrap_or(fallback_caps.mem_mib),
			},
			peers,
			expected_members: object
				.get("expected_members")
				.and_then(value_u64)
				.and_then(|value| usize::try_from(value).ok())
				.unwrap_or(1)
				.max(1),
		}))
	}

	/// Persist membership state atomically without writing the cluster token.
	/// When `enabled` is false, the membership file is removed best-effort.
	pub fn save(&self, path: &Path) -> Result<(), MeshError> {
		if !self.enabled {
			match fs::remove_file(path) {
				Ok(()) => return Ok(()),
				Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
				Err(error) => return Err(error.into()),
			}
		}
		if let Some(parent) = path.parent() {
			fs::create_dir_all(parent)?;
		}
		let data = json!({
			"enabled": true,
			"node_id": self.node_id,
			"advertise": self.advertise,
			"region": self.region,
			"vcpus": self.caps.vcpus,
			"mem_mib": self.caps.mem_mib,
			"peers": self.peers.values().map(NodeState::to_wire).collect::<Vec<_>>(),
			"expected_members": self.expected_members,
		});
		let tmp = tmp_path(path);
		fs::write(&tmp, serde_json::to_vec(&data)?)?;
		fs::rename(tmp, path)?;
		Ok(())
	}
}

/// Return the durable mesh membership path under a [`Home`].
pub fn mesh_state_path(home: &Home) -> PathBuf {
	home.root().join(MESH_STATE_FILE)
}

/// Return a new compact random node id: exactly 16 lowercase hex characters.
pub fn new_node_id() -> String {
	hex::encode(rand::random::<[u8; 8]>())
}

/// Probe this host's default advertised capacity.
pub fn probe_caps() -> NodeCaps {
	NodeCaps { vcpus: available_cpus(), mem_mib: total_mem_mib() }
}

/// Probe this host's snapshot-restore compatibility class as `(backend, arch)`.
pub fn probe_compat() -> (String, String) {
	let backend = if cfg!(target_os = "macos") {
		"hvf"
	} else {
		"kvm"
	};
	(backend.to_owned(), host_arch())
}

/// Probe this host's restore-relevant CPU feature baseline.
///
/// This calls the shared VMM library API directly; it never shells out to the
/// `vmon vmm --print-cpu-baseline` compatibility command.
pub fn probe_cpu_baseline() -> String {
	let mut cache = CPU_BASELINE_CACHE
		.lock()
		.expect("CPU baseline cache poisoned");
	if let Some(cached) = cache.as_ref() {
		return cached.clone();
	}
	let arch = host_arch();
	let baseline = vmm::cpu_baseline().unwrap_or_else(|_| format!("unknown:{arch}"));
	*cache = Some(baseline.clone());
	baseline
}

/// Clear the process-local CPU baseline cache. This is primarily useful for
/// deterministic tests that stub the underlying VMM probe.
pub fn clear_cpu_baseline_cache() {
	*CPU_BASELINE_CACHE
		.lock()
		.expect("CPU baseline cache poisoned") = None;
}

/// Return whether candidate `have` covers ingress `need`'s restore surface.
pub fn cpu_baseline_covers(have: &str, need: &str) -> bool {
	if need.is_empty() {
		return true;
	}
	if need.starts_with("unknown:") {
		return false;
	}
	if need.starts_with("arch:") {
		return !have.starts_with("arch:") || have == need;
	}
	if have.is_empty() || have.starts_with("arch:") || have.starts_with("unknown:") {
		return false;
	}
	let Ok(have_obj) = serde_json::from_str::<Value>(have) else {
		return have == need;
	};
	let Ok(need_obj) = serde_json::from_str::<Value>(need) else {
		return have == need;
	};
	let (Some(have_obj), Some(need_obj)) = (have_obj.as_object(), need_obj.as_object()) else {
		return false;
	};
	for (key, need_value) in need_obj {
		let Some(have_value) = have_obj.get(key) else {
			return false;
		};
		if key == "v" {
			if have_value != need_value {
				return false;
			}
			continue;
		}
		let (Some(have_int), Some(need_int)) = (json_u128(have_value), json_u128(need_value)) else {
			return false;
		};
		if have_int & need_int != need_int {
			return false;
		}
	}
	true
}

/// Return the URL peers should use for a bound host and port.
pub fn default_advertise(host: &str, port: u16) -> String {
	let mut host = if matches!(host, "0.0.0.0" | "127.0.0.1" | "::" | "localhost") {
		primary_ip()
	} else {
		host.to_owned()
	};
	if host.contains(':') && !host.starts_with('[') {
		host = format!("[{host}]");
	}
	format!("http://{host}:{port}")
}

/// Encode an advertise URL and cluster token into a join blob.
pub fn encode_blob(url: &str, token: &str) -> String {
	let url = serde_json::to_string(url).expect("string serialization cannot fail");
	let token = serde_json::to_string(token).expect("string serialization cannot fail");
	let raw = format!("{{\"url\":{url},\"token\":{token}}}");
	general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

/// Decode a mesh join blob into its advertise URL and cluster token.
pub fn decode_blob(blob: &str) -> Result<(String, String), MeshError> {
	let bytes = general_purpose::URL_SAFE_NO_PAD
		.decode(blob)
		.or_else(|_| general_purpose::URL_SAFE.decode(blob))
		.map_err(|_| MeshError::invalid("invalid join blob"))?;
	let data: Value =
		serde_json::from_slice(&bytes).map_err(|_| MeshError::invalid("invalid join blob"))?;
	let url = data
		.get("url")
		.and_then(Value::as_str)
		.filter(|value| !value.is_empty())
		.ok_or_else(|| MeshError::invalid("invalid join blob"))?;
	let token = data
		.get("token")
		.and_then(Value::as_str)
		.filter(|value| !value.is_empty())
		.ok_or_else(|| MeshError::invalid("invalid join blob"))?;
	Ok((url.to_owned(), token.to_owned()))
}

/// Return this process's normalized vmon architecture name.
pub fn host_arch() -> String {
	normalize_oci_arch(Some(std::env::consts::ARCH))
		.unwrap_or_else(|| std::env::consts::ARCH.replace('-', "_"))
}

fn available_cpus() -> u64 {
	thread::available_parallelism().map_or(1, |count| count.get() as u64)
}

fn total_mem_mib() -> u64 {
	sysconf_total_mem_mib().unwrap_or_else(|| available_cpus() * 2048)
}

#[cfg(unix)]
fn sysconf_total_mem_mib() -> Option<u64> {
	// SAFETY: `sysconf` takes integer names and returns process-global values.
	let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
	// SAFETY: `sysconf` takes integer names and returns process-global values.
	let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
	if pages <= 0 || page_size <= 0 {
		return None;
	}
	u64::try_from(pages)
		.ok()
		.zip(u64::try_from(page_size).ok())
		.map(|(pages, page_size)| pages.saturating_mul(page_size) / 1_048_576)
}

#[cfg(not(unix))]
fn sysconf_total_mem_mib() -> Option<u64> {
	None
}

fn primary_ip() -> String {
	UdpSocket::bind("0.0.0.0:0")
		.and_then(|socket| {
			socket.connect("8.8.8.8:80")?;
			socket.local_addr()
		})
		.map_or_else(|_| "127.0.0.1".to_owned(), |addr| addr.ip().to_string())
}

fn tmp_path(path: &Path) -> PathBuf {
	let mut extension = path
		.extension()
		.and_then(|value| value.to_str())
		.map_or_else(String::new, ToOwned::to_owned);
	extension.push_str(".tmp");
	path.with_extension(extension)
}

fn string_field(object: &Map<String, Value>, key: &str) -> String {
	object
		.get(key)
		.and_then(Value::as_str)
		.unwrap_or_default()
		.to_owned()
}

fn string_vec(value: Option<&Value>) -> Vec<String> {
	value
		.and_then(Value::as_array)
		.map(|items| {
			items
				.iter()
				.filter_map(|item| match item {
					Value::String(text) => Some(text.clone()),
					Value::Null => None,
					other => Some(other.to_string()),
				})
				.collect()
		})
		.unwrap_or_default()
}

fn string_map(value: Option<&Value>) -> BTreeMap<String, String> {
	value
		.and_then(Value::as_object)
		.map(|items| {
			items
				.iter()
				.filter_map(|(key, value)| {
					if key.is_empty() {
						return None;
					}
					let value = match value {
						Value::String(text) if !text.is_empty() => text.clone(),
						Value::Null => return None,
						other => other.to_string(),
					};
					Some((key.clone(), value))
				})
				.collect()
		})
		.unwrap_or_default()
}

fn u64_map(value: Option<&Value>) -> BTreeMap<String, u64> {
	value
		.and_then(Value::as_object)
		.map(|items| {
			items
				.iter()
				.filter_map(|(key, value)| value_u64(value).map(|value| (key.clone(), value)))
				.collect()
		})
		.unwrap_or_default()
}

fn value_u64(value: &Value) -> Option<u64> {
	match value {
		Value::Number(number) => number
			.as_u64()
			.or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok()))
			.or_else(|| {
				number
					.as_f64()
					.filter(|value| value.is_finite() && *value >= 0.0)
					.map(|value| value as u64)
			}),
		Value::String(text) => text.parse().ok(),
		_ => None,
	}
}

fn value_f64(value: &Value) -> Option<f64> {
	match value {
		Value::Number(number) => number.as_f64(),
		Value::String(text) => text.parse().ok(),
		_ => None,
	}
}

fn json_u128(value: &Value) -> Option<u128> {
	match value {
		Value::Number(number) => number
			.as_u64()
			.map(u128::from)
			.or_else(|| number.as_i64().and_then(|value| u128::try_from(value).ok())),
		Value::String(text) => text.parse().ok(),
		_ => None,
	}
}
