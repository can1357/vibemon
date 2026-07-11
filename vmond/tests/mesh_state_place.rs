use std::{
	collections::BTreeMap,
	path::PathBuf,
	time::{SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use tempfile::tempdir;
use vmond::mesh::{
	place::{PlacementContext, PlacementRequest, pool_eligible, request_template_key, template_key},
	state::{MembershipState, NodeCaps, NodeState, cpu_baseline_covers, decode_blob, encode_blob},
};

fn now() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_secs_f64()
}

fn temp_state_path(name: &str) -> PathBuf {
	tempdir().unwrap().keep().join(name)
}

fn req(value: serde_json::Value) -> PlacementRequest {
	PlacementRequest::from_value(value).unwrap()
}

fn node(node_id: &str, advertise: &str) -> NodeState {
	NodeState::new(node_id, advertise)
}

#[test]
fn join_blob_round_trips_and_rejects_invalid_payloads() {
	let blob = encode_blob("http://node", "tok");
	assert_eq!(decode_blob(&blob).unwrap(), ("http://node".to_owned(), "tok".to_owned()));
	assert!(decode_blob("not-json").is_err());
}

#[test]
fn node_state_wire_tolerates_pre_backend_arch_cpu_baseline_peers() {
	let old = NodeState::from_wire(&json!({"node_id": "old", "advertise": "http://old"})).unwrap();
	assert_eq!((old.backend.as_str(), old.arch.as_str(), old.cpu_baseline.as_str()), ("", "", ""));

	let mut state = NodeState::new("n", "u");
	state.backend = "kvm".to_owned();
	state.arch = "x86_64".to_owned();
	state.cpu_baseline = "base".to_owned();
	let round_trip = NodeState::from_wire(&state.to_wire()).unwrap();
	assert_eq!(
		(round_trip.backend.as_str(), round_trip.arch.as_str(), round_trip.cpu_baseline.as_str()),
		("kvm", "x86_64", "base")
	);
}

#[test]
fn expected_members_grows_persists_and_shrinks_with_departures() {
	let path = temp_state_path("mesh.json");
	let mut state = MembershipState::enabled("self", "http://self", "", NodeCaps::new(2, 1024));

	assert_eq!(state.expected_members, 1);
	state.peers.insert("p1".to_owned(), node("p1", "http://p1"));
	state.peers.insert("p2".to_owned(), node("p2", "http://p2"));
	assert!(state.bump_expected());
	assert_eq!(state.expected_members, 3);
	state.save(&path).unwrap();

	let mut loaded = MembershipState::load(&path, NodeCaps::new(1, 512), now())
		.unwrap()
		.unwrap();
	assert_eq!(loaded.expected_members, 3);
	loaded.depart("p1");
	assert_eq!(loaded.expected_members, 2);
	loaded.enabled = false;
	loaded.save(&path).unwrap();
	let disabled = MembershipState::load(&path, NodeCaps::new(1, 512), now()).unwrap();
	assert!(disabled.is_none());
}

#[test]
fn quorum_needed_uses_strict_majority_of_expected_members() {
	let mut state = MembershipState::enabled("self", "http://self", "", NodeCaps::new(2, 1024));

	state.expected_members = 3;
	assert_eq!(state.quorum_needed(), 2);
	assert!(state.restore_quorum_met(2));
	assert!(!state.restore_quorum_met(1));

	state.expected_members = 5;
	assert_eq!(state.quorum_needed(), 3);
}

#[test]
fn health_uses_receiver_time_not_sender_clock() {
	let mut state = MembershipState::enabled("self", "http://self", "", NodeCaps::new(2, 1024));
	let received_at = 1_000.0;
	let mut peer = NodeState::new("B", "http://b");
	peer.ts = 1_000_000.0;
	peer.last_seen = received_at;
	state.peers.insert("B".to_owned(), peer);

	assert!(state.is_member_healthy("B", 1_029.9, 10.0));
	assert!(!state.is_member_healthy("B", 1_030.1, 10.0));
}

#[test]
fn template_key_canonicalizes_shape_reference_and_network_slots() {
	assert_eq!(
		template_key(Some(" ubuntu:latest "), 1024, 2048, 2, 3, true, false, false).unwrap(),
		"ubuntu:latest|d1024|m2048|c2|s3|h1|n0|t0"
	);
	assert!(
		template_key(Some("ubuntu:latest"), 1024, 512, 1, 0, false, false, true)
			.unwrap()
			.ends_with("|n0|t1")
	);
}

#[test]
fn request_template_key_and_pool_eligibility_match_warm_restore_boundaries() {
	let block = request_template_key(&req(json!({"image": "img", "block_network": true})));
	assert!(block.as_deref().is_some_and(|key| key.ends_with("|n0|t0")));

	let networked = request_template_key(&req(json!({"image": "img", "block_network": false})));
	#[cfg(target_os = "macos")]
	assert!(
		networked
			.as_deref()
			.is_some_and(|key| key.ends_with("|n1|t0"))
	);
	#[cfg(not(target_os = "macos"))]
	assert!(
		networked
			.as_deref()
			.is_some_and(|key| key.ends_with("|n0|t1"))
	);

	assert!(!pool_eligible(&req(json!({"image": "img", "block_network": false}))));
	assert!(request_template_key(&req(json!({"image": "img", "fs_dir": "/x"}))).is_none());
	assert!(request_template_key(&req(json!({"image": "img", "volumes": {"/a": "v"}}))).is_none());
}

#[test]
fn cpu_baseline_covers_bit_surfaces_and_arch_tokens() {
	let need =
		serde_json::to_string(&json!({"1.0.ecx": 0b0011, "D.0.eax": 0b0101, "v": 1})).unwrap();
	let have =
		serde_json::to_string(&json!({"1.0.ecx": 0b1011, "D.0.eax": 0b0111, "v": 1})).unwrap();
	let missing_bit =
		serde_json::to_string(&json!({"1.0.ecx": 0b0001, "D.0.eax": 0b0111, "v": 1})).unwrap();
	let missing_key = serde_json::to_string(&json!({"1.0.ecx": 0b1011, "v": 1})).unwrap();

	assert!(cpu_baseline_covers(&have, &need));
	assert!(!cpu_baseline_covers(&missing_bit, &need));
	assert!(!cpu_baseline_covers(&missing_key, &need));
	assert!(cpu_baseline_covers("arch:aarch64", "arch:aarch64"));
	assert!(!cpu_baseline_covers("arch:x86_64", "arch:aarch64"));
	assert!(!cpu_baseline_covers("", &need));
	assert!(!cpu_baseline_covers("arch:x86_64", &need));
	assert!(cpu_baseline_covers(&have, ""));
	assert!(!cpu_baseline_covers("unknown:x86_64", &need));
	assert!(!cpu_baseline_covers(&have, "unknown:x86_64"));
}

#[test]
fn placement_prefers_warm_capacity_but_pins_local_requests() {
	let context = PlacementContext::new("self", "", "kvm", "x86_64", "arch:x86_64", 3.0);
	let mut local = NodeState::new("self", "http://self");
	local.caps = NodeCaps::new(2, 1024);
	local.committed_vcpus = 1;
	local.backend = "kvm".to_owned();
	local.arch = "x86_64".to_owned();
	local.cpu_baseline = "arch:x86_64".to_owned();
	local.last_seen = now();

	let mut warm = NodeState::new("warm", "http://warm");
	warm.caps = NodeCaps::new(2, 1024);
	warm.committed_vcpus = 1;
	warm.committed_mem_mib = 512;
	warm.backend = "kvm".to_owned();
	warm.arch = "x86_64".to_owned();
	warm.pools = BTreeMap::from([("img:x".to_owned(), 2)]);
	warm.last_seen = now();

	let mut free = NodeState::new("free", "http://free");
	free.caps = NodeCaps::new(8, 4096);
	free.backend = "kvm".to_owned();
	free.arch = "x86_64".to_owned();
	free.last_seen = now();

	let nodes = vec![local, warm, free];
	assert_eq!(
		vmond::mesh::place::place(
			&req(json!({"image": "img:x", "pool_size": 1, "block_network": true})),
			&context,
			&nodes,
			now()
		)
		.unwrap(),
		"warm"
	);
	assert_eq!(
		vmond::mesh::place::place(&req(json!({"image": "plain"})), &context, &nodes, now()).unwrap(),
		"free"
	);
	assert_eq!(
		vmond::mesh::place::place(&req(json!({"dockerfile": "Dockerfile"})), &context, &nodes, now())
			.unwrap(),
		"self"
	);
}

#[test]
fn arch_errors_preserve_machine_readable_codes() {
	let context = PlacementContext::new("self", "", "kvm", "x86_64", "arch:x86_64", 3.0);
	let mut local = NodeState::new("self", "http://self");
	local.backend = "kvm".to_owned();
	local.arch = "x86_64".to_owned();
	local.caps = NodeCaps::new(2, 1024);
	local.last_seen = now();
	let mut arm = NodeState::new("arm", "http://arm");
	arm.backend = "kvm".to_owned();
	arm.arch = "aarch64".to_owned();
	arm.caps = NodeCaps::new(8, 4096);
	arm.last_seen = now();

	let err = vmond::mesh::place::place(
		&req(json!({"image": "unknown:latest"})),
		&context,
		&[local, arm],
		now(),
	)
	.unwrap_err();
	assert_eq!(err.code, "arch_required");
	assert!(err.to_string().contains("cannot derive image arch"));
	assert!(err.to_string().contains("pass arch="));
}
