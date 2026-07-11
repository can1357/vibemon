use std::{
	collections::BTreeMap,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use serde_json::{Map, Value, json};
use tempfile::TempDir;
use vmond::mesh::{
	lease::{
		LeaseDecision, LeaseManager, LeaseRecord, LeaseReleaseArgs, LeaseRequestArgs,
		LeaseUnavailable,
	},
	record::{CreateRecord, RecordStore},
	replica::ReplicaStore,
};

#[derive(Clone)]
struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
	fn new(now: f64) -> Self {
		Self(Arc::new(AtomicU64::new(now.to_bits())))
	}

	fn now(&self) -> f64 {
		f64::from_bits(self.0.load(Ordering::Relaxed))
	}

	fn set(&self, now: f64) {
		self.0.store(now.to_bits(), Ordering::Relaxed);
	}

	fn advance(&self, seconds: f64) {
		self.set(self.now() + seconds);
	}
}

struct LeaseCluster {
	_tmp:     TempDir,
	clock:    ManualClock,
	managers: BTreeMap<&'static str, Arc<LeaseManager>>,
	urls:     BTreeMap<String, String>,
}

impl LeaseCluster {
	fn new(nodes: &[&'static str]) -> Self {
		let tmp = tempfile::tempdir().unwrap();
		let clock = ManualClock::new(100.0);
		let managers = nodes
			.iter()
			.map(|node| {
				let root = tmp.path().join(node).join("leases");
				let manager = LeaseManager::with_clock(root, {
					let clock = clock.clone();
					move || clock.now()
				});
				(*node, Arc::new(manager))
			})
			.collect();
		let urls = nodes
			.iter()
			.map(|node| ((*node).to_owned(), format!("http://{}", node.to_ascii_lowercase())))
			.collect();
		Self { _tmp: tmp, clock, managers, urls }
	}

	fn post(&self, url: &str, path: &str, payload: Value) -> Result<LeaseDecision, ()> {
		let node = self
			.urls
			.iter()
			.find_map(|(node, candidate)| (candidate == url).then_some(node.as_str()))
			.expect("lease test posted to an unknown node");
		let manager = self.managers.get(node).unwrap();
		let decision = match path {
			"/v1/mesh/lease/grant" => manager.vote_grant(
				payload["volume"].as_str().unwrap(),
				payload["holder_node"].as_str().unwrap(),
				payload["epoch"].as_u64().unwrap(),
				payload["ttl"].as_f64().unwrap(),
			),
			"/v1/mesh/lease/renew" => manager.vote_renew(
				payload["volume"].as_str().unwrap(),
				payload["holder_node"].as_str().unwrap(),
				payload["epoch"].as_u64().unwrap(),
				payload["ttl"].as_f64().unwrap(),
			),
			"/v1/mesh/lease/release" => manager.vote_release(
				payload["volume"].as_str().unwrap(),
				payload["holder_node"].as_str().unwrap(),
				payload["epoch"].as_u64().unwrap(),
			),
			route => panic!("unexpected lease route {route}"),
		};
		decision.map_err(|_| ())
	}

	fn peer_urls(&self, nodes: &[&'static str]) -> BTreeMap<String, String> {
		nodes
			.iter()
			.map(|node| ((*node).to_owned(), self.urls.get(*node).unwrap().clone()))
			.collect()
	}

	fn grant(
		&self,
		requester: &'static str,
		volume: &str,
		holder: &'static str,
		epoch: u64,
		ttl: f64,
		expected_members: usize,
		peer_nodes: &[&'static str],
	) -> Result<LeaseRecord, LeaseUnavailable> {
		let peer_urls = self.peer_urls(peer_nodes);
		self.managers.get(requester).unwrap().request_grant(
			LeaseRequestArgs {
				volume,
				holder_node: holder,
				epoch,
				ttl,
				self_node: requester,
				peer_urls: &peer_urls,
				expected_members,
			},
			|url, path, payload| self.post(url, path, payload),
		)
	}

	fn renew(
		&self,
		requester: &'static str,
		volume: &str,
		holder: &'static str,
		epoch: u64,
		ttl: f64,
	) -> Result<LeaseRecord, LeaseUnavailable> {
		let peer_urls = self.peer_urls(&["A", "B", "C"]);
		self.managers.get(requester).unwrap().request_renew(
			LeaseRequestArgs {
				volume,
				holder_node: holder,
				epoch,
				ttl,
				self_node: requester,
				peer_urls: &peer_urls,
				expected_members: self.managers.len(),
			},
			|url, path, payload| self.post(url, path, payload),
		)
	}

	fn release(&self, requester: &'static str, volume: &str, holder: &'static str, epoch: u64) {
		let peer_urls = self.peer_urls(&["A", "B", "C"]);
		self
			.managers
			.get(requester)
			.unwrap()
			.request_release(
				LeaseReleaseArgs {
					volume,
					holder_node: holder,
					epoch,
					self_node: requester,
					peer_urls: &peer_urls,
				},
				|url, path, payload| self.post(url, path, payload),
			)
			.unwrap();
	}

	fn holders(&self, volume: &str) -> BTreeMap<&'static str, Option<String>> {
		self
			.managers
			.iter()
			.map(|(node, manager)| {
				(*node, manager.current(volume).unwrap().map(|record| record.holder))
			})
			.collect()
	}
}

fn object(value: Value) -> Map<String, Value> {
	value.as_object().unwrap().clone()
}

#[test]
fn grant_renew_release_round_trip_persists_majority_votes() {
	let cluster = LeaseCluster::new(&["A", "B", "C"]);

	let granted = cluster
		.grant("A", "data", "A", 1, 20.0, 3, &["A", "B", "C"])
		.unwrap();
	assert_eq!(granted.holder, "A");
	assert_eq!(granted.granted_at, 100.0);
	assert_eq!(granted.renew_deadline(), 110.0);
	assert_eq!(granted.expires_at(), 120.0);
	assert_eq!(
		cluster.holders("data"),
		BTreeMap::from([
			("A", Some("A".to_owned())),
			("B", Some("A".to_owned())),
			("C", Some("A".to_owned()))
		])
	);

	cluster.clock.advance(4.0);
	let renewed = cluster.renew("A", "data", "A", 1, 20.0).unwrap();
	assert_eq!(renewed.holder, "A");
	assert_eq!(renewed.granted_at, 104.0);
	assert_eq!(renewed.renew_deadline(), 114.0);
	assert_eq!(renewed.expires_at(), 124.0);
	let granted_at_by_node: BTreeMap<_, _> = cluster
		.managers
		.iter()
		.map(|(node, manager)| (*node, manager.current("data").unwrap().unwrap().granted_at))
		.collect();
	assert_eq!(granted_at_by_node, BTreeMap::from([("A", 104.0), ("B", 104.0), ("C", 104.0)]));

	cluster.release("A", "data", "A", 1);
	assert_eq!(cluster.holders("data"), BTreeMap::from([("A", None), ("B", None), ("C", None)]));
}

#[test]
fn grant_uses_strict_majority_of_expected_members() {
	let cluster = LeaseCluster::new(&["A", "B", "C"]);

	let granted = cluster
		.grant("A", "two_votes_are_enough_for_three", "A", 1, 20.0, 3, &["A", "B"])
		.unwrap();
	assert_eq!(granted.holder, "A");
	assert_eq!(
		cluster.holders("two_votes_are_enough_for_three"),
		BTreeMap::from([("A", Some("A".to_owned())), ("B", Some("A".to_owned())), ("C", None)])
	);

	let err = cluster
		.grant("A", "two_votes_are_not_enough_for_five", "A", 1, 20.0, 5, &["A", "B"])
		.unwrap_err();
	assert_eq!(err.votes, 2);
	assert_eq!(err.needed, 3);
	assert_eq!(
		cluster.holders("two_votes_are_not_enough_for_five"),
		BTreeMap::from([("A", None), ("B", None), ("C", None)])
	);
}

#[test]
fn conflicting_holder_cannot_receive_grant_until_full_ttl_elapsed() {
	let cluster = LeaseCluster::new(&["A", "B", "C"]);
	cluster
		.grant("A", "shared", "A", 1, 10.0, 3, &["A", "B", "C"])
		.unwrap();

	cluster.clock.set(109.999);
	let early = cluster
		.grant("B", "shared", "B", 2, 10.0, 3, &["A", "B", "C"])
		.unwrap_err();
	assert!(early.message.contains("conflict"));
	assert_eq!(
		cluster.holders("shared"),
		BTreeMap::from([
			("A", Some("A".to_owned())),
			("B", Some("A".to_owned())),
			("C", Some("A".to_owned()))
		])
	);

	cluster.clock.set(110.0);
	let successor = cluster
		.grant("B", "shared", "B", 2, 10.0, 3, &["A", "B", "C"])
		.unwrap();
	assert_eq!(successor.holder, "B");
	assert_eq!(successor.granted_at, 110.0);
	assert_eq!(
		cluster.holders("shared"),
		BTreeMap::from([
			("A", Some("B".to_owned())),
			("B", Some("B".to_owned())),
			("C", Some("B".to_owned()))
		])
	);
}

#[test]
fn persisted_votes_survive_manager_restart_and_fence_conflicts() {
	let tmp = tempfile::tempdir().unwrap();
	let root = tmp.path().join("leases");
	let clock = ManualClock::new(50.0);
	let first = LeaseManager::with_clock(root.clone(), {
		let clock = clock.clone();
		move || clock.now()
	});
	let granted = first.vote_grant("vol", "A", 7, 30.0).unwrap();
	assert!(granted.granted);

	let restarted = LeaseManager::with_clock(root, move || clock.now());
	let record = restarted.current("vol").unwrap().unwrap();
	assert_eq!(record, LeaseRecord::new("vol", "A", 7, 50.0, 30.0).unwrap());
	assert_eq!(restarted.active_votes(Some(50.0)), vec![record.clone()]);

	let conflicting = restarted.vote_grant("vol", "B", 8, 30.0).unwrap();
	assert!(!conflicting.granted);
	assert_eq!(conflicting.reason.as_deref(), Some("conflict"));
	assert_eq!(restarted.current("vol").unwrap(), Some(record));
}

#[test]
fn lower_epoch_renewal_is_fenced_after_higher_epoch_grant() {
	let cluster = LeaseCluster::new(&["A", "B", "C"]);
	cluster
		.grant("A", "epochvol", "A", 1, 10.0, 3, &["A", "B", "C"])
		.unwrap();
	cluster.clock.set(110.0);
	cluster
		.grant("B", "epochvol", "B", 2, 10.0, 3, &["A", "B", "C"])
		.unwrap();

	let err = cluster.renew("A", "epochvol", "A", 1, 10.0).unwrap_err();
	assert_eq!(err.votes, 0);
	assert!(err.message.contains("stale_epoch"));
	assert_eq!(
		cluster.holders("epochvol"),
		BTreeMap::from([
			("A", Some("B".to_owned())),
			("B", Some("B".to_owned())),
			("C", Some("B".to_owned()))
		])
	);
}

#[test]
fn record_json_never_persists_secret_material() {
	let tmp = tempfile::tempdir().unwrap();
	let store = RecordStore::new(tmp.path().join("records"));
	let record = CreateRecord::new(
		"secret-record",
		object(
			json!({"name": "secret-record", "image": "img:x", "secrets": [{"TOKEN": "dont-persist-me"}]}),
		),
		"A",
		0,
		"idem-key",
		"async",
		"none",
		100.0,
	)
	.unwrap();
	store.put(record).unwrap();

	let raw = std::fs::read_to_string(tmp.path().join("records/secret-record.json")).unwrap();
	let data: serde_json::Value = serde_json::from_str(&raw).unwrap();
	assert!(!raw.contains("dont-persist-me"));
	assert!(!raw.contains("TOKEN"));
	assert!(data["params"].get("secrets").is_none());
}

#[test]
fn replica_store_round_trip_never_persists_secret_material_and_restart_loses_readiness() {
	let tmp = tempfile::tempdir().unwrap();
	let root = tmp.path().join("replicas");
	let store = ReplicaStore::new(root.clone());
	let params = object(json!({"name": "ha-vm", "env": {"PUBLIC": "ok"}, "secrets": [{"K": "V"}]}));

	store
		.put("ha-vm", "sha256:digest", "owner-a", "/snapshots/ha-vm", params.clone())
		.unwrap();
	let text = std::fs::read_to_string(root.join("ha-vm.json")).unwrap();
	assert!(!text.contains("\"V\""));
	assert!(!text.contains("\"secrets\""));

	let record = store.get("ha-vm").unwrap();
	assert_eq!(record.params, params);
	assert!(record.needs_secrets);
	assert!(store.secrets_ready("ha-vm"));

	let restarted = ReplicaStore::new(root);
	restarted.load();
	assert!(restarted.holds("ha-vm"));
	let restarted_record = restarted.get("ha-vm").unwrap();
	assert!(restarted_record.params.get("secrets").is_none());
	assert!(restarted_record.needs_secrets);
	assert!(!restarted.secrets_ready("ha-vm"));
}
