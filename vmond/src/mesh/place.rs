//! Placement scoring, architecture compatibility, warm-template selection, and
//! rendezvous-hash helpers.
//!
//! Public helpers in this module accept JSON-shaped requests because mesh
//! placement sees create, run, restore, fork, and idempotency payloads before
//! all of them are normalized into one Rust request type.

use std::{cmp::Ordering, collections::BTreeSet};

use serde_json::{Map, Value};

use crate::{
	image::{manifest_arches, normalize_oci_arch, parse_reference},
	mesh::state::{MeshError, NodeState, cpu_baseline_covers},
	models::SandboxCreate,
	pools,
};

pub const PLACEMENT_ARCHES: &[&str] = &["aarch64", "x86_64"];

const BLAKE2B_IV: [u64; 8] = [
	0x6a09_e667_f3bc_c908,
	0xbb67_ae85_84ca_a73b,
	0x3c6e_f372_fe94_f82b,
	0xa54f_f53a_5f1d_36f1,
	0x510e_527f_ade6_82d1,
	0x9b05_688c_2b3e_6c1f,
	0x1f83_d9ab_fb41_bd6b,
	0x5be0_cd19_137e_2179,
];

const BLAKE2B_SIGMA: [[usize; 16]; 12] = [
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
	[14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
	[11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
	[7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
	[9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
	[2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
	[12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
	[13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
	[6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
	[10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
	[14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
];

/// Placement weights keyed like `VMON_MESH_W_*` and Python's
/// `DEFAULT_PLACEMENT_WEIGHTS`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlacementWeights {
	pub warm:     f64,
	pub free:     f64,
	pub local:    f64,
	pub region:   f64,
	pub inflight: f64,
}

/// Default placement weights: warm=1000, free=100, local=50, region=30,
/// inflight=80.
pub const DEFAULT_PLACEMENT_WEIGHTS: PlacementWeights = PlacementWeights {
	warm:     1000.0,
	free:     100.0,
	local:    50.0,
	region:   30.0,
	inflight: 80.0,
};

impl PlacementWeights {
	/// Overlay weight values from `(name, value)` pairs; unknown names are
	/// ignored so config maps can be passed through directly.
	pub fn with_overrides<'a>(
		mut self,
		overrides: impl IntoIterator<Item = (&'a str, f64)>,
	) -> Self {
		for (key, value) in overrides {
			match key {
				"warm" => self.warm = value,
				"free" => self.free = value,
				"local" => self.local = value,
				"region" => self.region = value,
				"inflight" => self.inflight = value,
				_ => {},
			}
		}
		self
	}
}

impl Default for PlacementWeights {
	fn default() -> Self {
		DEFAULT_PLACEMENT_WEIGHTS
	}
}

/// Request-local placement context supplied by the ingress node.
#[derive(Clone, Debug, PartialEq)]
pub struct PlacementContext {
	pub ingress_id:     String,
	pub ingress_region: String,
	pub backend:        String,
	pub arch:           String,
	pub cpu_baseline:   String,
	pub interval:       f64,
	pub weights:        PlacementWeights,
}

impl PlacementContext {
	/// Build context with default placement weights.
	pub fn new(
		ingress_id: impl Into<String>,
		ingress_region: impl Into<String>,
		backend: impl Into<String>,
		arch: impl Into<String>,
		cpu_baseline: impl Into<String>,
		interval: f64,
	) -> Self {
		Self {
			ingress_id: ingress_id.into(),
			ingress_region: ingress_region.into(),
			backend: backend.into(),
			arch: arch.into(),
			cpu_baseline: cpu_baseline.into(),
			interval,
			weights: PlacementWeights::default(),
		}
	}
}

/// JSON-shaped placement request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlacementRequest {
	fields: Map<String, Value>,
}

impl PlacementRequest {
	/// Build a placement request from a JSON object.
	pub fn from_value(value: Value) -> Result<Self, MeshError> {
		let fields = value
			.as_object()
			.cloned()
			.ok_or_else(|| MeshError::invalid("placement request must be an object"))?;
		Ok(Self { fields })
	}

	/// Build a placement request from an object map.
	pub const fn from_map(fields: Map<String, Value>) -> Self {
		Self { fields }
	}

	/// Build a placement request from the public v1 sandbox create body.
	pub fn from_sandbox_create(params: &SandboxCreate) -> Self {
		let value = serde_json::to_value(params).expect("SandboxCreate serialization cannot fail");
		Self::from_value(value).expect("SandboxCreate serializes to an object")
	}

	/// Return one raw request field.
	pub fn get(&self, key: &str) -> Option<&Value> {
		self.fields.get(key)
	}

	/// Return whether a request field is Python-truthy.
	pub fn truthy(&self, key: &str) -> bool {
		self.get(key).is_some_and(value_truthy)
	}

	/// Return whether a request field is present and not JSON null.
	pub fn present(&self, key: &str) -> bool {
		self.get(key).is_some_and(|value| !value.is_null())
	}

	/// Return a non-empty string field with surrounding whitespace stripped.
	pub fn non_empty_str(&self, key: &str) -> Option<String> {
		self
			.get(key)
			.and_then(Value::as_str)
			.map(str::trim)
			.filter(|value| !value.is_empty())
			.map(ToOwned::to_owned)
	}
}

/// Validate and return a canonical request-scoped placement arch selector.
pub fn validate_request_arch(value: Option<&Value>) -> Result<Option<String>, MeshError> {
	let Some(value) = value.filter(|value| !value.is_null()) else {
		return Ok(None);
	};
	let Some(text) = value.as_str() else {
		return Err(arch_validation_error());
	};
	if PLACEMENT_ARCHES.contains(&text) {
		return Ok(Some(text.to_owned()));
	}
	Err(arch_validation_error())
}

/// Return the request-derivable identity for a bootable warm template.
pub fn template_key(
	reference: Option<&str>,
	disk_mb: u64,
	memory: u64,
	cpus: u64,
	fs_slots: u64,
	host_slot: bool,
	nic_slot: bool,
	tap_slot: bool,
) -> Result<String, MeshError> {
	let reference = parse_reference(reference).map_err(|error| MeshError::invalid(error.message))?;
	let Some(reference) = reference else {
		return Err(MeshError::invalid("template key requires an image reference"));
	};
	Ok(pools::template_key(
		&reference, disk_mb, memory, cpus, fs_slots, host_slot, nic_slot, tap_slot,
	))
}

/// Return the cross-node warm-template key for a request, if pull-warm applies.
pub fn request_template_key(request: &PlacementRequest) -> Option<String> {
	let reference = request.non_empty_str("image")?;
	if request.truthy("template")
		|| request.truthy("dockerfile")
		|| context_is_local(request.get("context"))
		|| request.present("fs_dir")
		|| volume_count(request.get("volumes")) != 0
	{
		return None;
	}
	let block_network = request.truthy("block_network");
	let user_net_flavor = !block_network && cfg!(target_os = "macos");
	template_key(
		Some(&reference),
		request.u64_or("disk_mb", 1024)?,
		request.memory_or_mem()?,
		request.u64_or("cpus", 1)?,
		0,
		false,
		user_net_flavor,
		!block_network && !user_net_flavor,
	)
	.ok()
}

/// Return the image/template ref a request would use for a warm pool.
pub fn pool_ref(request: &PlacementRequest) -> Option<String> {
	request
		.truthy_scalar("template")
		.or_else(|| request.truthy_scalar("image"))
}

/// Return whether a create request can claim a warm-pool clone.
pub fn pool_eligible(request: &PlacementRequest) -> bool {
	request.truthy("block_network") && !request.truthy("volumes") && !request.truthy("fs_dir")
}

/// Return whether a request references ingress-host-local input.
pub fn pinned_local(request: &PlacementRequest) -> bool {
	request.truthy("dockerfile")
		|| request.truthy("fs_dir")
		|| request.truthy("volumes")
		|| context_is_local(request.get("context"))
}

/// Score one candidate node for a placement request.
#[allow(
	clippy::suboptimal_flops,
	reason = "placement scoring preserves Python-compatible floating-point rounding order"
)]
pub fn score_node(
	node: &NodeState,
	request: &PlacementRequest,
	ingress_id: &str,
	ingress_region: &str,
	weights: Option<&PlacementWeights>,
) -> f64 {
	let default_weights = PlacementWeights::default();
	let weights = weights.unwrap_or(&default_weights);
	let reference = pool_ref(request);
	let key = request_template_key(request);
	let pool_warm = f64::from(
		pool_eligible(request)
			&& reference
				.as_ref()
				.and_then(|reference| node.pools.get(reference))
				.is_some_and(|ready| *ready > 0),
	);
	let template_warm = f64::from(
		key.as_ref()
			.is_some_and(|key| node.template_index.contains_key(key)),
	);
	let warm = pool_warm.max(template_warm);
	let free_frac = if node.caps.vcpus == 0 {
		0.0
	} else {
		node.free_vcpus() as f64 / node.caps.vcpus as f64
	};
	let local = f64::from(node.node_id == ingress_id);
	let region = f64::from(!ingress_region.is_empty() && node.region == ingress_region);
	let inflight_frac = (node.inflight as f64 / node.caps.vcpus.max(1) as f64).min(1.0);
	weights.warm * warm + weights.free * free_frac + weights.local * local + weights.region * region
		- weights.inflight * inflight_frac
}

/// Resolve the request-scoped arch set before backend/template scoring.
pub fn placement_arches(
	request: &PlacementRequest,
	nodes: &[NodeState],
	source_arch: &str,
	ref_bound: bool,
) -> Result<BTreeSet<String>, MeshError> {
	let live_arches = normalized_live_arches(nodes);
	if let Some(explicit) = validate_request_arch(request.get("arch"))? {
		if !live_arches.contains(&explicit) {
			return Err(explicit_arch_error(&explicit, &live_arches));
		}
		return Ok(BTreeSet::from([explicit]));
	}

	if let Some(image) = request_image_ref(request) {
		let Some(image_arches) = manifest_arches(&image) else {
			if live_arches.len() == 1 {
				return Ok(live_arches);
			}
			return Err(MeshError::arch_required(format!(
				"arch_required: cannot derive image arch; pass arch=... on a mixed-arch mesh (live \
				 arches: {})",
				format_arch_set(&live_arches)
			)));
		};
		let image_arches = image_arches.into_iter().collect::<BTreeSet<_>>();
		let compatible = image_arches
			.iter()
			.filter_map(|arch| normalize_oci_arch(Some(arch)))
			.filter(|arch| live_arches.contains(arch))
			.collect::<BTreeSet<_>>();
		if compatible.is_empty() {
			return Err(manifest_arch_error(&image, &image_arches, &live_arches));
		}
		return Ok(compatible);
	}

	if ref_bound {
		return Ok(normalize_oci_arch(Some(source_arch))
			.into_iter()
			.filter(|arch| live_arches.contains(arch))
			.collect());
	}
	Ok(live_arches)
}

/// Return healthy placement candidates for a create/restore/fork request.
pub fn candidates(
	request: &PlacementRequest,
	context: &PlacementContext,
	live_nodes: &[NodeState],
	arches: Option<&BTreeSet<String>>,
) -> Result<Vec<NodeState>, MeshError> {
	let reference = request_bound_ref(request);
	let placement_arches = match arches {
		Some(arches) => arches.clone(),
		None => placement_arches(request, live_nodes, &context.arch, reference.is_some())?,
	};

	let mut nodes = live_nodes
		.iter()
		.filter(|node| {
			normalize_oci_arch(Some(&node.arch)).is_some_and(|arch| placement_arches.contains(&arch))
		})
		.cloned()
		.collect::<Vec<_>>();

	if let Some(reference) = reference {
		nodes.retain(|node| {
			node.backend == context.backend
				&& node.templates.iter().any(|template| template == &reference)
				&& cpu_baseline_covers(&node.cpu_baseline, &context.cpu_baseline)
		});
	}

	Ok(nodes)
}

/// Filter all known nodes down to live candidates. The caller should pass a
/// freshly built local [`NodeState`] plus peer snapshots.
pub fn live_candidates(
	request: &PlacementRequest,
	context: &PlacementContext,
	nodes: &[NodeState],
	now: f64,
	arches: Option<&BTreeSet<String>>,
) -> Result<Vec<NodeState>, MeshError> {
	let live_nodes = nodes
		.iter()
		.filter(|node| node.node_id == context.ingress_id || node.healthy(now, context.interval))
		.cloned()
		.collect::<Vec<_>>();
	candidates(request, context, &live_nodes, arches)
}

/// Choose the owner node id for a placement request.
pub fn place(
	request: &PlacementRequest,
	context: &PlacementContext,
	nodes: &[NodeState],
	now: f64,
) -> Result<String, MeshError> {
	let explicit = validate_request_arch(request.get("arch"))?;
	if pinned_local(request) {
		let local_arch = normalize_oci_arch(Some(&context.arch));
		if let Some(explicit) = explicit
			&& local_arch.as_deref() != Some(explicit.as_str())
		{
			let live = local_arch.into_iter().collect::<BTreeSet<_>>();
			return Err(explicit_arch_error(&explicit, &live));
		}
		return Ok(context.ingress_id.clone());
	}

	let candidates = live_candidates(request, context, nodes, now, None)?;
	if candidates.is_empty() {
		return Ok(context.ingress_id.clone());
	}

	let req_cpus = request.u64_or("cpus", 1).unwrap_or(1);
	let req_mem = request.memory_or_mem().unwrap_or(512);
	let fit = candidates
		.iter()
		.filter(|node| node.free_vcpus() >= req_cpus && node.free_mem_mib() >= req_mem)
		.cloned()
		.collect::<Vec<_>>();
	let pool = if fit.is_empty() { candidates } else { fit };

	let best = pool
		.iter()
		.max_by(|left, right| compare_rank(left, right, request, context))
		.expect("pool is non-empty");
	Ok(best.node_id.clone())
}

/// Rendezvous-hash weight of `node_id` for `key`; the highest weight wins.
pub fn hrw_score(key: &str, node_id: &str) -> u64 {
	let mut input = Vec::with_capacity(key.len() + 1 + node_id.len());
	input.extend_from_slice(key.as_bytes());
	input.push(0);
	input.extend_from_slice(node_id.as_bytes());
	u64::from_be_bytes(blake2b_8(&input))
}

/// Return the first node id with the highest rendezvous-hash score.
pub fn hrw_winner<'a>(key: &str, node_ids: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
	let mut best = None;
	let mut best_score = 0;
	for node_id in node_ids {
		let score = hrw_score(key, node_id);
		if best.is_none() || score > best_score {
			best = Some(node_id);
			best_score = score;
		}
	}
	best
}

/// Sort node ids by descending rendezvous-hash score for `key`.
pub fn sort_by_hrw_desc(key: &str, node_ids: &mut [String]) {
	node_ids.sort_by(|left, right| {
		hrw_score(key, right)
			.cmp(&hrw_score(key, left))
			.then_with(|| left.cmp(right))
	});
}

/// Return the normalized live arch set advertised by nodes.
pub fn normalized_live_arches(nodes: &[NodeState]) -> BTreeSet<String> {
	nodes
		.iter()
		.filter_map(|node| normalize_oci_arch(Some(&node.arch)))
		.filter(|arch| PLACEMENT_ARCHES.contains(&arch.as_str()))
		.collect()
}

/// Format a sorted arch set the way Python mesh errors do.
pub fn format_arch_set(arches: &BTreeSet<String>) -> String {
	if arches.is_empty() {
		"none".to_owned()
	} else {
		arches.iter().cloned().collect::<Vec<_>>().join(", ")
	}
}

fn compare_rank(
	left: &NodeState,
	right: &NodeState,
	request: &PlacementRequest,
	context: &PlacementContext,
) -> Ordering {
	score_node(left, request, &context.ingress_id, &context.ingress_region, Some(&context.weights))
		.total_cmp(&score_node(
			right,
			request,
			&context.ingress_id,
			&context.ingress_region,
			Some(&context.weights),
		))
		.then_with(|| left.free_vcpus().cmp(&right.free_vcpus()))
		.then_with(|| right.inflight.cmp(&left.inflight))
		.then_with(|| left.node_id.cmp(&right.node_id))
}

fn request_image_ref(request: &PlacementRequest) -> Option<String> {
	request.non_empty_str("image")
}

fn request_bound_ref(request: &PlacementRequest) -> Option<String> {
	request
		.truthy_scalar("template")
		.or_else(|| request.truthy_scalar("snapshot"))
}

fn explicit_arch_error(requested: &str, live_arches: &BTreeSet<String>) -> MeshError {
	MeshError::unplaceable(format!(
		"unplaceable: requested arch {requested:?} has no live nodes (live arches: {})",
		format_arch_set(live_arches)
	))
}

fn manifest_arch_error(
	image: &str,
	image_arches: &BTreeSet<String>,
	live_arches: &BTreeSet<String>,
) -> MeshError {
	MeshError::unplaceable(format!(
		"unplaceable: image {image:?} advertises arches {}; live arches are {}",
		format_arch_set(image_arches),
		format_arch_set(live_arches)
	))
}

fn arch_validation_error() -> MeshError {
	MeshError::invalid("arch must be one of: aarch64, x86_64")
}

fn context_is_local(value: Option<&Value>) -> bool {
	match value {
		None | Some(Value::Null) => false,
		Some(Value::String(text)) => text != ".",
		Some(_) => true,
	}
}

fn volume_count(value: Option<&Value>) -> usize {
	match value {
		Some(Value::Object(items)) => items.len(),
		Some(Value::Array(items)) => items.len(),
		_ => 0,
	}
}

fn value_truthy(value: &Value) -> bool {
	match value {
		Value::Null => false,
		Value::Bool(value) => *value,
		Value::Number(number) => number
			.as_i64()
			.map_or_else(|| number.as_f64().is_some_and(|value| value != 0.0), |value| value != 0),
		Value::String(text) => !text.is_empty(),
		Value::Array(items) => !items.is_empty(),
		Value::Object(items) => !items.is_empty(),
	}
}

impl PlacementRequest {
	fn u64_or(&self, key: &str, default: u64) -> Option<u64> {
		match self.get(key) {
			None | Some(Value::Null) => Some(default),
			Some(value) => value_u64(value),
		}
	}

	fn memory_or_mem(&self) -> Option<u64> {
		if let Some(value) = self.get("memory").filter(|value| !value.is_null()) {
			value_u64(value)
		} else {
			self.u64_or("mem", 512)
		}
	}

	fn truthy_scalar(&self, key: &str) -> Option<String> {
		let value = self.get(key).filter(|value| value_truthy(value))?;
		Some(match value {
			Value::String(text) => text.clone(),
			Value::Number(number) => number.to_string(),
			Value::Bool(value) => value.to_string(),
			other => other.to_string(),
		})
		.filter(|value| !value.is_empty())
	}
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

fn blake2b_8(input: &[u8]) -> [u8; 8] {
	let mut h = BLAKE2B_IV;
	h[0] ^= 0x0101_0008;

	let mut offset = 0;
	while input.len().saturating_sub(offset) > 128 {
		let block = &input[offset..offset + 128];
		blake2b_compress(&mut h, block, (offset + 128) as u128, false);
		offset += 128;
	}

	let remaining = input.len() - offset;
	let mut block = [0_u8; 128];
	block[..remaining].copy_from_slice(&input[offset..]);
	blake2b_compress(&mut h, &block, input.len() as u128, true);

	let mut out = [0_u8; 8];
	out.copy_from_slice(&h[0].to_le_bytes());
	out
}

fn blake2b_compress(h: &mut [u64; 8], block: &[u8], count: u128, last: bool) {
	let mut m = [0_u64; 16];
	for (word, chunk) in m.iter_mut().zip(block.chunks_exact(8)) {
		*word = u64::from_le_bytes(chunk.try_into().expect("exact chunk length"));
	}

	let mut v = [0_u64; 16];
	v[..8].copy_from_slice(h);
	v[8..].copy_from_slice(&BLAKE2B_IV);
	v[12] ^= count as u64;
	v[13] ^= (count >> 64) as u64;
	if last {
		v[14] = !v[14];
	}

	for sigma in BLAKE2B_SIGMA {
		blake2b_g(&mut v, 0, 4, 8, 12, m[sigma[0]], m[sigma[1]]);
		blake2b_g(&mut v, 1, 5, 9, 13, m[sigma[2]], m[sigma[3]]);
		blake2b_g(&mut v, 2, 6, 10, 14, m[sigma[4]], m[sigma[5]]);
		blake2b_g(&mut v, 3, 7, 11, 15, m[sigma[6]], m[sigma[7]]);
		blake2b_g(&mut v, 0, 5, 10, 15, m[sigma[8]], m[sigma[9]]);
		blake2b_g(&mut v, 1, 6, 11, 12, m[sigma[10]], m[sigma[11]]);
		blake2b_g(&mut v, 2, 7, 8, 13, m[sigma[12]], m[sigma[13]]);
		blake2b_g(&mut v, 3, 4, 9, 14, m[sigma[14]], m[sigma[15]]);
	}

	for i in 0..8 {
		h[i] ^= v[i] ^ v[i + 8];
	}
}

#[allow(
	clippy::many_single_char_names,
	reason = "BLAKE2b mixing uses the algorithm's conventional a/b/c/d/x/y notation"
)]
const fn blake2b_g(v: &mut [u64; 16], a: usize, b: usize, c: usize, d: usize, x: u64, y: u64) {
	v[a] = v[a].wrapping_add(v[b]).wrapping_add(x);
	v[d] = (v[d] ^ v[a]).rotate_right(32);
	v[c] = v[c].wrapping_add(v[d]);
	v[b] = (v[b] ^ v[c]).rotate_right(24);
	v[a] = v[a].wrapping_add(v[b]).wrapping_add(y);
	v[d] = (v[d] ^ v[a]).rotate_right(16);
	v[c] = v[c].wrapping_add(v[d]);
	v[b] = (v[b] ^ v[c]).rotate_right(63);
}
