//! Distributed writable-volume lease votes.
//!
//! This is the durable, node-local half of Python's `vmon.lease`: each node
//! persists its current vote in `$VMON_HOME/leases/<volume>.json`, while
//! callers collect strict-majority grants across peers.  The on-disk files
//! deliberately omit derived wire fields (`expires_at`, `renew_deadline`) so
//! Python-written state and Rust-written state stay byte-shape compatible.

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt, fs,
	io::ErrorKind,
	os::unix::fs::DirBuilderExt,
	path::{Path, PathBuf},
	process,
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize, ser::SerializeStruct};
use serde_json::Value as JsonValue;

use crate::{EngineError, Result, home::Home};

/// `$VMON_HOME/leases/<volume>.json` TTL used by Python when config has no
/// newer explicit lease knob.
pub const DEFAULT_TTL: f64 = 30.0;

/// Stable mesh error code for a quorum lease miss.
pub const LEASE_UNAVAILABLE_CODE: &str = "lease_unavailable";

const VOLUME_NAME_PATTERN: &str = "^[a-z0-9_][a-z0-9_.-]{0,63}$";
const LEASE_DIR_MODE: u32 = 0o700;

/// One node's persisted vote for a writable-volume lease.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaseRecord {
	pub volume:     String,
	pub holder:     String,
	pub epoch:      u64,
	pub granted_at: f64,
	pub ttl:        f64,
}

impl LeaseRecord {
	pub fn new(
		volume: impl Into<String>,
		holder: impl Into<String>,
		epoch: u64,
		granted_at: f64,
		ttl: f64,
	) -> Result<Self> {
		let volume = validate_volume(volume.into())?;
		let holder = validate_holder(holder.into())?;
		validate_ttl(ttl)?;
		Ok(Self { volume, holder, epoch, granted_at, ttl })
	}

	/// Unix timestamp at which the vote expires.
	pub fn expires_at(&self) -> f64 {
		self.granted_at + self.ttl
	}

	/// Unix timestamp by which a matching holder must renew.
	pub fn renew_deadline(&self) -> f64 {
		self.granted_at + (self.ttl / 2.0)
	}

	/// Disk shape: `holder,epoch,granted_at,ttl` only.
	pub fn to_disk(&self) -> LeaseDiskRecord {
		LeaseDiskRecord {
			holder:     self.holder.clone(),
			epoch:      self.epoch,
			granted_at: self.granted_at,
			ttl:        self.ttl,
		}
	}

	/// Peer wire shape: disk fields plus `volume`, `expires_at`, and
	/// `renew_deadline`.
	pub fn to_wire(&self) -> LeaseWireRecord {
		LeaseWireRecord {
			volume:         self.volume.clone(),
			holder:         self.holder.clone(),
			epoch:          self.epoch,
			granted_at:     self.granted_at,
			ttl:            self.ttl,
			expires_at:     self.expires_at(),
			renew_deadline: self.renew_deadline(),
		}
	}

	/// Rebuild a record from either a disk object or a peer wire object.  The
	/// Python helper ignores any `volume` field in the object and trusts the
	/// path or request volume; this does the same.
	pub fn from_disk(volume: impl Into<String>, disk: LeaseDiskRecord) -> Result<Self> {
		Self::new(volume, disk.holder, disk.epoch, disk.granted_at, disk.ttl)
	}
}

/// Durable `$VMON_HOME/leases/<volume>.json` payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaseDiskRecord {
	pub holder:     String,
	pub epoch:      u64,
	pub granted_at: f64,
	pub ttl:        f64,
}

/// Peer-visible lease record payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaseWireRecord {
	pub volume:         String,
	pub holder:         String,
	pub epoch:          u64,
	pub granted_at:     f64,
	pub ttl:            f64,
	pub expires_at:     f64,
	pub renew_deadline: f64,
}

impl TryFrom<LeaseWireRecord> for LeaseRecord {
	type Error = EngineError;

	fn try_from(wire: LeaseWireRecord) -> Result<Self> {
		Self::new(wire.volume, wire.holder, wire.epoch, wire.granted_at, wire.ttl)
	}
}

/// The result of one node's local vote.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct LeaseDecision {
	pub granted: bool,
	pub record:  Option<LeaseRecord>,
	pub reason:  Option<String>,
}

impl LeaseDecision {
	pub const fn granted(record: LeaseRecord) -> Self {
		Self { granted: true, record: Some(record), reason: None }
	}

	pub fn denied(record: Option<LeaseRecord>, reason: impl Into<String>) -> Self {
		Self { granted: false, record, reason: Some(reason.into()) }
	}

	pub const fn released() -> Self {
		Self { granted: true, record: None, reason: None }
	}

	pub fn reason(&self) -> &str {
		self.reason.as_deref().unwrap_or("")
	}

	pub fn to_wire(&self) -> JsonValue {
		serde_json::to_value(self).unwrap_or_else(|_| JsonValue::Object(Default::default()))
	}
}
impl Serialize for LeaseDecision {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let mut fields = 1;
		if self.reason.is_some() {
			fields += 1;
		}
		if self.record.is_some() {
			fields += 1;
		}
		let mut state = serializer.serialize_struct("LeaseDecision", fields)?;
		state.serialize_field("granted", &self.granted)?;
		if let Some(reason) = &self.reason {
			state.serialize_field("reason", reason)?;
		}
		if let Some(record) = &self.record {
			state.serialize_field("record", &record.to_wire())?;
		}
		state.end()
	}
}

/// POST body used by `/v1/mesh/lease/{grant,renew}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaseVoteRequest {
	pub volume:      String,
	pub holder_node: String,
	pub epoch:       u64,
	pub ttl:         f64,
}

/// POST body used by `/v1/mesh/lease/release`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseReleaseRequest {
	pub volume:      String,
	pub holder_node: String,
	pub epoch:       u64,
}

/// A quorum lease could not be acquired or renewed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseUnavailable {
	pub message: String,
	pub votes:   usize,
	pub needed:  usize,
}

impl LeaseUnavailable {
	pub const CODE: &'static str = LEASE_UNAVAILABLE_CODE;

	pub fn new(message: impl Into<String>, votes: usize, needed: usize) -> Self {
		Self { message: message.into(), votes, needed }
	}

	pub const fn code(&self) -> &'static str {
		Self::CODE
	}
}

impl fmt::Display for LeaseUnavailable {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.message)
	}
}

impl std::error::Error for LeaseUnavailable {}

/// Persist local lease votes and collect strict-majority mesh grants.
pub struct LeaseManager {
	root:  PathBuf,
	clock: Arc<dyn Fn() -> f64 + Send + Sync>,
	lock:  Mutex<()>,
}

impl fmt::Debug for LeaseManager {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("LeaseManager")
			.field("root", &self.root)
			.finish_non_exhaustive()
	}
}

impl Default for LeaseManager {
	fn default() -> Self {
		Self::for_home(Home::default())
	}
}

impl LeaseManager {
	/// Build a manager rooted at an explicit `$VMON_HOME/leases` directory.
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self::with_clock(root, unix_now)
	}

	/// Build a manager from shared Vibemon home path helpers.
	pub fn for_home(home: Home) -> Self {
		Self::new(home.leases_dir())
	}

	/// Build a manager with a deterministic clock for tests.
	pub fn with_clock<F>(root: impl Into<PathBuf>, clock: F) -> Self
	where
		F: Fn() -> f64 + Send + Sync + 'static,
	{
		Self { root: root.into(), clock: Arc::new(clock), lock: Mutex::new(()) }
	}

	pub fn root(&self) -> &Path {
		&self.root
	}

	/// Return this node's current vote for `volume`, if any.
	pub fn current(&self, volume: &str) -> Result<Option<LeaseRecord>> {
		let volume = validate_volume(volume)?;
		let _guard = self.lock.lock();
		self.load_unlocked(&volume)
	}

	/// Return unexpired local vote records in stable volume order.
	pub fn active_votes(&self, now: Option<f64>) -> Vec<LeaseRecord> {
		let at = now.unwrap_or_else(|| (self.clock)());
		let _guard = self.lock.lock();
		let Ok(entries) = fs::read_dir(&self.root) else {
			return Vec::new();
		};
		let mut out = Vec::new();
		for entry in entries.flatten() {
			let path = entry.path();
			if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
				continue;
			}
			let Some(volume) = path.file_stem().and_then(|stem| stem.to_str()) else {
				continue;
			};
			let Ok(record) = self.load_unlocked(volume) else {
				continue;
			};
			if let Some(record) = record
				&& at < record.expires_at()
			{
				out.push(record);
			}
		}
		out.sort_by(|left, right| left.volume.cmp(&right.volume));
		out
	}

	/// Persist a yes vote iff no unexpired conflicting or newer vote exists.
	pub fn vote_grant(
		&self,
		volume: &str,
		holder_node: &str,
		epoch: u64,
		ttl: f64,
	) -> Result<LeaseDecision> {
		let volume = validate_volume(volume)?;
		let holder = validate_holder(holder_node)?;
		validate_ttl(ttl)?;
		let _guard = self.lock.lock();
		let now = (self.clock)();
		let existing = self.load_unlocked(&volume)?;
		if let Some(existing) = existing
			&& now < existing.expires_at()
		{
			if epoch < existing.epoch {
				return Ok(LeaseDecision::denied(Some(existing), "stale_epoch"));
			}
			if existing.holder != holder {
				return Ok(LeaseDecision::denied(Some(existing), "conflict"));
			}
		}
		let record = LeaseRecord::new(volume, holder, epoch, now, ttl)?;
		self.store_unlocked(&record)?;
		Ok(LeaseDecision::granted(record))
	}

	/// Extend a matching vote before its renew deadline.
	pub fn vote_renew(
		&self,
		volume: &str,
		holder_node: &str,
		epoch: u64,
		ttl: f64,
	) -> Result<LeaseDecision> {
		let volume = validate_volume(volume)?;
		let holder = validate_holder(holder_node)?;
		validate_ttl(ttl)?;
		let _guard = self.lock.lock();
		let now = (self.clock)();
		let Some(existing) = self.load_unlocked(&volume)? else {
			return Ok(LeaseDecision::denied(None, "missing"));
		};
		if existing.holder != holder || existing.epoch != epoch {
			let reason = if epoch < existing.epoch {
				"stale_epoch"
			} else {
				"mismatch"
			};
			return Ok(LeaseDecision::denied(Some(existing), reason));
		}
		if now >= existing.expires_at() {
			return Ok(LeaseDecision::denied(Some(existing), "expired"));
		}
		if now > existing.renew_deadline() {
			return Ok(LeaseDecision::denied(Some(existing), "renew_deadline"));
		}
		let record = LeaseRecord::new(volume, holder, epoch, now, ttl)?;
		self.store_unlocked(&record)?;
		Ok(LeaseDecision::granted(record))
	}

	/// Clear a matching local vote without allowing stale releases to fence
	/// successors.
	pub fn vote_release(
		&self,
		volume: &str,
		holder_node: &str,
		epoch: u64,
	) -> Result<LeaseDecision> {
		let volume = validate_volume(volume)?;
		let holder = validate_holder(holder_node)?;
		let _guard = self.lock.lock();
		let Some(existing) = self.load_unlocked(&volume)? else {
			return Ok(LeaseDecision::released());
		};
		if existing.holder == holder && existing.epoch == epoch {
			match fs::remove_file(self.path_for(&volume)) {
				Ok(()) => {},
				Err(err) if err.kind() == ErrorKind::NotFound => {},
				Err(err) => return Err(err.into()),
			}
			return Ok(LeaseDecision::released());
		}
		let reason = if epoch < existing.epoch {
			"stale_epoch"
		} else {
			"mismatch"
		};
		Ok(LeaseDecision::denied(Some(existing), reason))
	}

	/// Collect a strict-majority grant from this node and every known peer.
	pub fn request_grant<F, E>(
		&self,
		args: LeaseRequestArgs<'_>,
		post: F,
	) -> std::result::Result<LeaseRecord, LeaseUnavailable>
	where
		F: FnMut(&str, &str, JsonValue) -> std::result::Result<LeaseDecision, E>,
	{
		self.request_majority("grant", args, post, true)
	}

	/// Renew an existing lease by strict majority.
	pub fn request_renew<F, E>(
		&self,
		args: LeaseRequestArgs<'_>,
		post: F,
	) -> std::result::Result<LeaseRecord, LeaseUnavailable>
	where
		F: FnMut(&str, &str, JsonValue) -> std::result::Result<LeaseDecision, E>,
	{
		self.request_majority("renew", args, post, false)
	}

	/// Best-effort explicit release on every known member, including self.
	pub fn request_release<F, E>(&self, args: LeaseReleaseArgs<'_>, mut post: F) -> Result<()>
	where
		F: FnMut(&str, &str, JsonValue) -> std::result::Result<LeaseDecision, E>,
	{
		let volume = validate_volume(args.volume)?;
		let holder = validate_holder(args.holder_node)?;
		let _ = self.vote_release(&volume, &holder, args.epoch)?;
		let payload = serde_json::to_value(LeaseReleaseRequest {
			volume,
			holder_node: holder,
			epoch: args.epoch,
		})?;
		for (node_id, url) in args.peer_urls {
			if node_id == args.self_node {
				continue;
			}
			let _ = post(url, "/v1/mesh/lease/release", payload.clone());
		}
		Ok(())
	}

	fn request_majority<F, E>(
		&self,
		op: &'static str,
		args: LeaseRequestArgs<'_>,
		mut post: F,
		cleanup_on_failure: bool,
	) -> std::result::Result<LeaseRecord, LeaseUnavailable>
	where
		F: FnMut(&str, &str, JsonValue) -> std::result::Result<LeaseDecision, E>,
	{
		let volume = validate_volume(args.volume).map_err(validation_unavailable)?;
		let holder = validate_holder(args.holder_node).map_err(validation_unavailable)?;
		validate_ttl(args.ttl).map_err(validation_unavailable)?;
		let needed = majority_needed(args.expected_members).map_err(validation_unavailable)?;
		let payload = LeaseVoteRequest {
			volume:      volume.clone(),
			holder_node: holder.clone(),
			epoch:       args.epoch,
			ttl:         args.ttl,
		};
		let local_decision = if op == "grant" {
			self.vote_grant(&volume, &holder, args.epoch, args.ttl)
		} else {
			self.vote_renew(&volume, &holder, args.epoch, args.ttl)
		}
		.map_err(validation_unavailable)?;
		let mut votes = usize::from(local_decision.granted);
		let mut record = local_decision
			.record
			.clone()
			.filter(|_| local_decision.granted);
		let mut granted_nodes = BTreeSet::new();
		if local_decision.granted {
			granted_nodes.insert(args.self_node.to_owned());
		}
		let path = if op == "grant" {
			"/v1/mesh/lease/grant"
		} else {
			"/v1/mesh/lease/renew"
		};
		for (node_id, url) in args.peer_urls {
			if node_id == args.self_node {
				continue;
			}
			let Ok(response) = post(
				url,
				path,
				serde_json::to_value(&payload).map_err(|err| validation_unavailable(err.into()))?,
			) else {
				continue;
			};
			if response.granted {
				votes += 1;
				granted_nodes.insert(node_id.clone());
				if record.is_none() {
					record = response.record;
				}
			}
		}
		if votes >= needed
			&& local_decision.granted
			&& let Some(record) = record
		{
			return Ok(record);
		}
		if cleanup_on_failure && !granted_nodes.is_empty() {
			let _ = self.vote_release(&volume, &holder, args.epoch);
			let release_payload = serde_json::to_value(LeaseReleaseRequest {
				volume:      volume.clone(),
				holder_node: holder,
				epoch:       args.epoch,
			})
			.map_err(|err| validation_unavailable(err.into()))?;
			for (node_id, url) in args.peer_urls {
				if node_id == args.self_node || !granted_nodes.contains(node_id) {
					continue;
				}
				let _ = post(url, "/v1/mesh/lease/release", release_payload.clone());
			}
		}
		let reason = if local_decision.reason().is_empty() {
			"quorum"
		} else {
			local_decision.reason()
		};
		Err(LeaseUnavailable::new(
			format!("lease {op} for volume '{volume}' got {votes}/{needed} votes ({reason})"),
			votes,
			needed,
		))
	}

	fn path_for(&self, volume: &str) -> PathBuf {
		self
			.root
			.join(format!("{}.json", validate_volume_lossy(volume)))
	}

	fn load_unlocked(&self, volume: &str) -> Result<Option<LeaseRecord>> {
		let path = self.path_for(volume);
		let text = match fs::read_to_string(&path) {
			Ok(text) => text,
			Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err.into()),
		};
		let disk: LeaseDiskRecord = serde_json::from_str(&text).map_err(|err| {
			EngineError::invalid(format!("invalid lease record in {}: {err}", path.display()))
		})?;
		LeaseRecord::from_disk(volume, disk).map(Some)
	}

	fn store_unlocked(&self, record: &LeaseRecord) -> Result<()> {
		ensure_private_dir(&self.root)?;
		let path = self.path_for(&record.volume);
		let tmp = temp_path_for(&path);
		let bytes = serde_json::to_vec(&record.to_disk())?;
		fs::write(&tmp, [&bytes[..], b"\n"].concat())?;
		fs::rename(&tmp, path)?;
		Ok(())
	}
}

/// Arguments common to majority grant/renew calls.
#[derive(Clone, Copy)]
pub struct LeaseRequestArgs<'a> {
	pub volume:           &'a str,
	pub holder_node:      &'a str,
	pub epoch:            u64,
	pub ttl:              f64,
	pub self_node:        &'a str,
	pub peer_urls:        &'a BTreeMap<String, String>,
	pub expected_members: usize,
}

/// Arguments common to best-effort release calls.
#[derive(Clone, Copy)]
pub struct LeaseReleaseArgs<'a> {
	pub volume:      &'a str,
	pub holder_node: &'a str,
	pub epoch:       u64,
	pub self_node:   &'a str,
	pub peer_urls:   &'a BTreeMap<String, String>,
}

/// Strict-majority size for a mesh with `expected_members` members.
pub fn majority_needed(expected_members: usize) -> Result<usize> {
	if expected_members < 1 {
		return Err(EngineError::invalid("expected_members must be positive"));
	}
	Ok(expected_members / 2 + 1)
}

pub fn quorum_reached(votes: usize, expected_members: usize) -> Result<bool> {
	Ok(votes >= majority_needed(expected_members)?)
}

pub fn validate_lease_volume(volume: &str) -> Result<String> {
	validate_volume(volume)
}

fn validate_volume(volume: impl AsRef<str>) -> Result<String> {
	let value = volume.as_ref();
	if !is_valid_volume_name(value) {
		return Err(EngineError::invalid(format!(
			"invalid volume name {value:?}: must match {VOLUME_NAME_PATTERN}"
		)));
	}
	Ok(value.to_owned())
}

fn validate_volume_lossy(volume: &str) -> String {
	validate_volume(volume).unwrap_or_else(|_| volume.to_owned())
}

fn validate_holder(holder: impl AsRef<str>) -> Result<String> {
	let value = holder.as_ref();
	if value.is_empty() {
		return Err(EngineError::invalid("lease holder_node must be non-empty"));
	}
	Ok(value.to_owned())
}

fn validate_ttl(ttl: f64) -> Result<()> {
	if !ttl.is_finite() || ttl <= 0.0 {
		return Err(EngineError::invalid("lease ttl must be a positive finite number"));
	}
	Ok(())
}

fn is_valid_volume_name(name: &str) -> bool {
	let bytes = name.as_bytes();
	if bytes.is_empty() || bytes.len() > 64 || !is_volume_name_start(bytes[0]) {
		return false;
	}
	bytes[1..].iter().all(|byte| is_volume_name_rest(*byte))
}

const fn is_volume_name_start(byte: u8) -> bool {
	byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
}

const fn is_volume_name_rest(byte: u8) -> bool {
	is_volume_name_start(byte) || byte == b'.' || byte == b'-'
}

fn validation_unavailable(err: EngineError) -> LeaseUnavailable {
	LeaseUnavailable::new(err.message, 0, 0)
}

fn unix_now() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| {
			duration.as_secs() as f64 + f64::from(duration.subsec_nanos()) / 1_000_000_000.0
		})
}

fn ensure_private_dir(path: &Path) -> Result<()> {
	let mut builder = fs::DirBuilder::new();
	builder.recursive(true).mode(LEASE_DIR_MODE);
	builder.create(path).or_else(|err| {
		if err.kind() == ErrorKind::AlreadyExists {
			Ok(())
		} else {
			Err(err)
		}
	})?;
	Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
	let file_name = path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("lease.json");
	path.with_file_name(format!(".{file_name}.{}.tmp", process::id()))
}
