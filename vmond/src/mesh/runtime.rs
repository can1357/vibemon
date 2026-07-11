//! Concrete mesh runtime wiring for the live server.
//!
//! The Phase 4 mesh modules are deliberately small and trait-oriented.  This
//! module owns the concrete stores and adapters that make those traits point at
//! the real server engine, on-disk mesh state, peer HTTP transport, and
//! background loops.

use std::{
	collections::{BTreeMap, BTreeSet, HashMap, HashSet},
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use futures_util::{FutureExt, future::BoxFuture};
use parking_lot::{Mutex, RwLock};
use serde_json::{Map, Value, json};
use tokio::{
	sync::{Notify, broadcast},
	task::JoinHandle,
};

use super::{
	gossip::{self, HeartbeatView, MeshError, MeshResult, PeerEndpoint, PeerHttpClient},
	lease::{self, DEFAULT_TTL, LeaseDecision, LeaseRecord},
	place::{self, PlacementContext, PlacementRequest, PlacementWeights},
	reconciler::{self, ReconcileConfig, Reconciler},
	record, replica,
	routes::{
		CreateRecordWire, MeshControl, MeshEngine, MeshLeaseManager, MeshRecordStore,
		MeshReplicaStore, MeshRouteConfig, MeshRouteState, MeshSetupResult, MeshTemplateTransfer,
		MetadataPull, MigratePrepareWire, NodeCapsWire, PeerMember, ReplicaRecordWire,
	},
	state::{self, MembershipState, NodeCaps, NodeState},
	transfer,
};
use crate::{
	EngineError, ErrorCode, Result,
	config::ServeConfig,
	engine::{Engine, EngineApi},
	function::store::Store as FunctionStore,
	home::Home,
	image::cas,
	models::SandboxCreate,
};

const OWNER_EPOCH_FLOOR: u64 = 0;

/// Live mesh runtime shared by API routes, peer routes, and background loops.
pub struct MeshRuntime {
	config:             ServeConfig,
	membership_path:    PathBuf,
	node_id:            String,
	membership:         RwLock<MembershipState>,
	token:              RwLock<String>,
	default_advertise:  String,
	transport:          PeerHttpClient,
	engine:             MeshEngineAdapter,
	records:            record::RecordStore,
	replicas:           replica::ReplicaStore,
	function_store:     FunctionStore,
	leases:             lease::LeaseManager,
	owners:             RwLock<BTreeMap<String, (String, u64)>>,
	orphans:            Mutex<Vec<(String, String)>>,
	idem_pins:          RwLock<BTreeMap<String, String>>,
	workers:            Mutex<HashSet<String>>,
	events:             Mutex<Vec<String>>,
	compat_backend:     String,
	compat_arch:        String,
	cpu_baseline:       String,
	weights:            PlacementWeights,
	replicate_notify:   Notify,
	background_started: AtomicBool,
}

impl MeshRuntime {
	/// Build the concrete mesh runtime and load durable mesh/record/replica
	/// state.
	pub fn new(config: ServeConfig, home: Home, engine: Arc<Engine>) -> Result<Arc<Self>> {
		let caps = state::probe_caps();
		let membership_path = state::mesh_state_path(&home);
		let default_advertise = state::default_advertise(&config.host, config.port);
		let now = unix_now();
		let membership = MembershipState::load(&membership_path, caps, now)
			.map_err(|err| EngineError::invalid(err.to_string()))?
			.unwrap_or_else(|| disabled_membership(caps, &default_advertise));
		let token = config.token.clone().unwrap_or_default();
		let transport = PeerHttpClient::new(token.clone()).map_err(mesh_to_engine)?;
		let records = record::RecordStore::for_home(home.clone());
		records.load();
		let replicas = replica::ReplicaStore::for_home(home.clone());
		replicas.load();
		let function_store = FunctionStore::open(&home)?;
		let leases = lease::LeaseManager::for_home(home);
		let (compat_backend, compat_arch) = state::probe_compat();
		let cpu_baseline = state::probe_cpu_baseline();
		let owners = records
			.list()
			.into_iter()
			.map(|record| (record.sid, (record.owner, i64_to_u64(record.epoch))))
			.collect();
		Ok(Arc::new(Self {
			node_id: membership.node_id.clone(),
			config: config.clone(),
			membership_path,
			membership: RwLock::new(membership),
			token: RwLock::new(token),
			default_advertise,
			transport,
			engine: MeshEngineAdapter::new(engine),
			records,
			replicas,
			function_store,
			leases,
			owners: RwLock::new(owners),
			orphans: Mutex::new(Vec::new()),
			idem_pins: RwLock::new(BTreeMap::new()),
			workers: Mutex::new(HashSet::new()),
			events: Mutex::new(Vec::new()),
			compat_backend,
			compat_arch,
			cpu_baseline,
			weights: placement_weights(&config),
			replicate_notify: Notify::new(),
			background_started: AtomicBool::new(false),
		}))
	}

	/// Build the peer-route state object from the shared concrete runtime.
	pub fn route_state(self: &Arc<Self>) -> MeshRouteState {
		let token = self.token.read().clone();
		MeshRouteState::new(
			token.clone(),
			token,
			self.transport.clone(),
			self.clone() as Arc<dyn MeshControl>,
			Arc::new(self.engine.clone()) as Arc<dyn MeshEngine>,
			self.clone() as Arc<dyn MeshLeaseManager>,
			self.clone() as Arc<dyn MeshRecordStore>,
			self.clone() as Arc<dyn MeshReplicaStore>,
			Arc::new(TemplateTransfer) as Arc<dyn MeshTemplateTransfer>,
			self.route_config(),
		)
	}

	/// Start heartbeat, reconciliation, and replica loops once for the server.
	pub fn start_background(
		self: &Arc<Self>,
		shutdown: &broadcast::Sender<()>,
	) -> Vec<JoinHandle<()>> {
		if self.background_started.swap(true, Ordering::SeqCst) {
			return Vec::new();
		}
		let mut handles = Vec::new();
		let heartbeat_runtime = self.clone();
		let mut heartbeat_shutdown = shutdown.subscribe();
		handles.push(tokio::spawn(async move {
			heartbeat_loop(heartbeat_runtime, &mut heartbeat_shutdown).await;
		}));

		let reconcile_runtime = self.clone();
		let mut reconcile_shutdown = shutdown.subscribe();
		handles.push(tokio::spawn(async move {
			reconcile_loop(reconcile_runtime, &mut reconcile_shutdown).await;
		}));
		handles
	}

	pub async fn migrate_sandbox(self: &Arc<Self>, sid: String, target: String) -> Result<Value> {
		super::routes::migrate_sandbox_to(&self.route_state(), sid, target)
			.await
			.map_err(|err| EngineError::engine(format!("mesh migration failed: {err:?}")))
	}

	pub async fn create_sandbox(self: &Arc<Self>, params: Map<String, Value>) -> Result<Value> {
		let key = params
			.get("idempotency_key")
			.and_then(Value::as_str)
			.map(str::trim)
			.filter(|key| !key.is_empty())
			.map_or_else(mesh_request_idempotency_key, str::to_owned);
		let view =
			super::routes::MeshRouteState::idempotent_sandbox_create(&self.route_state(), key, params)
				.await
				.map_err(|err| EngineError::engine(format!("mesh create failed: {err:?}")))?;
		Ok(view)
	}

	pub const fn peer_client(&self) -> &PeerHttpClient {
		&self.transport
	}

	pub fn mesh_enabled(&self) -> bool {
		self.membership.read().enabled
	}

	pub fn local_node_id(&self) -> String {
		self.membership.read().node_id.clone()
	}

	/// Return the live topology snapshot used to admit function HA policies.
	///
	/// This is deliberately topology only: function workers are not sandbox
	/// owners and must not be inserted into the sandbox owner map.
	pub fn function_placement_nodes(&self) -> Vec<NodeState> {
		let now = unix_now();
		let interval = self.config.mesh_heartbeat_sec;
		self
			.all_nodes()
			.into_iter()
			.filter(|node| node.node_id == self.local_node_id() || node.healthy(now, interval))
			.collect()
	}

	fn route_config(&self) -> MeshRouteConfig {
		MeshRouteConfig {
			replicas:          self.config.replicas,
			create_timeout:    secs(self.config.mesh_create_timeout_sec),
			default_ha_meshed: self.config.default_ha(true).to_owned(),
			default_ha_local:  self.config.default_ha(false).to_owned(),
		}
	}

	fn reconcile_config(&self) -> ReconcileConfig {
		ReconcileConfig::from_serve_config(&self.config, self.enabled(), self.token.read().clone())
	}

	fn save_membership(&self, membership: &MembershipState) -> MeshResult<()> {
		membership.save(&self.membership_path).map_err(Into::into)
	}

	fn local_state(&self) -> NodeState {
		let membership = self.membership.read().clone();
		let mut state = NodeState::new(membership.node_id.clone(), membership.advertise.clone());
		state.region.clone_from(&membership.region);
		state.caps = membership.caps;
		state.ts = unix_now();
		state.last_seen = state.ts;
		state.backend.clone_from(&self.compat_backend);
		state.arch.clone_from(&self.compat_arch);
		state.cpu_baseline.clone_from(&self.cpu_baseline);
		state.function_artifacts = self
			.function_store
			.placement_artifact_digests(4096)
			.unwrap_or_default();

		for view in self.engine.list_views().unwrap_or_default() {
			let Some(object) = view.as_object() else {
				continue;
			};
			let status = object
				.get("status")
				.and_then(Value::as_str)
				.unwrap_or_default();
			if status == "terminated" {
				continue;
			}
			if let Some(id) = object.get("id").and_then(Value::as_str) {
				state.owned.push(id.to_owned());
				state
					.owned_epochs
					.insert(id.to_owned(), self.local_epoch_u64(id));
			}
			if status == "running" {
				state.committed_vcpus += object.get("cpus").and_then(value_u64).unwrap_or(0);
				state.committed_mem_mib += object.get("memory").and_then(value_u64).unwrap_or(0);
			}
		}
		state.owned.sort();

		for (key, ready) in self.local_pools() {
			state.pools.insert(key.clone(), ready);
			state.templates.push(key);
		}
		if let Ok(digests) = cas::list_digests() {
			for (digest, template) in digests {
				state.templates.push(digest.clone());
				state.template_index.insert(digest, template);
			}
		}
		state.templates.sort();
		state.templates.dedup();
		state.inflight = self.workers.lock().len() as u64;
		state
	}

	fn local_pools(&self) -> BTreeMap<String, u64> {
		let Ok(value) = self.engine.engine.pool_list() else {
			return BTreeMap::new();
		};
		let Some(object) = value.as_object() else {
			return BTreeMap::new();
		};
		object
			.iter()
			.map(|(key, value)| {
				let ready = value
					.get("ready")
					.and_then(Value::as_u64)
					.unwrap_or_default();
				(key.clone(), ready)
			})
			.collect()
	}

	fn all_nodes(&self) -> Vec<NodeState> {
		let membership = self.membership.read().clone();
		let mut nodes = vec![self.local_state()];
		nodes.extend(membership.peers.values().cloned());
		nodes
	}

	fn context(&self) -> PlacementContext {
		let membership = self.membership.read();
		PlacementContext {
			ingress_id:     membership.node_id.clone(),
			ingress_region: membership.region.clone(),
			backend:        self.compat_backend.clone(),
			arch:           self.compat_arch.clone(),
			cpu_baseline:   self.cpu_baseline.clone(),
			interval:       self.config.mesh_heartbeat_sec,
			weights:        self.weights,
		}
	}

	fn merge_node_state(&self, value: Value) -> MeshResult<()> {
		let mut node = NodeState::from_wire(&value)?;
		if node.node_id.is_empty() || node.node_id.starts_with("seed-") {
			return Ok(());
		}
		let mut membership = self.membership.write();
		if node.node_id == membership.node_id {
			return Ok(());
		}
		if node.last_seen <= 0.0 {
			node.last_seen = unix_now();
		}
		membership.peers.retain(|peer_id, peer| {
			!(peer_id.starts_with("seed-")
				&& peer.advertise == node.advertise
				&& peer_id != &node.node_id)
		});
		membership.peers.insert(node.node_id.clone(), node);
		membership.bump_expected();
		self.save_membership(&membership)
	}

	fn local_epoch_u64(&self, sid: &str) -> u64 {
		self
			.owners
			.read()
			.get(sid)
			.map_or(OWNER_EPOCH_FLOOR, |(_, epoch)| *epoch)
	}

	async fn acquire_writable_leases(
		&self,
		params: &Map<String, Value>,
		epoch: u64,
	) -> MeshResult<Vec<LeaseRecord>> {
		let volumes = writable_volumes(params)?;
		if volumes.is_empty() {
			return Ok(Vec::new());
		}
		if self.enabled() && self.expected_members() < 3 {
			return Err(MeshError::with_code(
				"writable volumes need >=3 nodes on mesh contexts",
				"unsupported",
			));
		}
		let holder = self.node_id();
		let mut acquired: Vec<LeaseRecord> = Vec::new();
		for volume in volumes {
			match self.acquire_one_lease(&volume, &holder, epoch).await {
				Ok(record) => acquired.push(record),
				Err(err) => {
					let _ = self.release_lease_records(&acquired).await;
					return Err(err);
				},
			}
		}
		Ok(acquired)
	}

	async fn acquire_one_lease(
		&self,
		volume: &str,
		holder: &str,
		epoch: u64,
	) -> MeshResult<LeaseRecord> {
		let local = self
			.leases
			.vote_grant(volume, holder, epoch, DEFAULT_TTL)
			.map_err(engine_to_mesh)?;
		let needed = lease::majority_needed(self.expected_members()).map_err(engine_to_mesh)?;
		let mut votes = usize::from(local.granted);
		let mut record = local.record.clone().filter(|_| local.granted);
		let payload = json!({
			"volume": volume,
			"holder_node": holder,
			"epoch": epoch,
			"ttl": DEFAULT_TTL,
		});
		for peer in self.peers() {
			if !peer.healthy {
				continue;
			}
			let posted = self
				.transport
				.post(&peer.advertise, "/v1/mesh/lease/grant", &payload, None)
				.await;
			let Ok(object) = posted else {
				continue;
			};
			let decision = lease_decision(Value::Object(object))?;
			if decision.granted {
				votes += 1;
				if record.is_none() {
					record = decision.record;
				}
			}
		}
		if votes >= needed && local.granted {
			return record.ok_or_else(|| MeshError::invalid("lease grant missing record"));
		}
		let _ = self
			.leases
			.vote_release(volume, holder, epoch)
			.map_err(engine_to_mesh);
		let release = json!({"volume": volume, "holder_node": holder, "epoch": epoch});
		for peer in self.peers() {
			let _ = self
				.transport
				.post(&peer.advertise, "/v1/mesh/lease/release", &release, None)
				.await;
		}
		Err(MeshError::with_code(
			format!("lease grant for volume '{volume}' got {votes}/{needed} votes"),
			lease::LEASE_UNAVAILABLE_CODE,
		))
	}

	/// Renew one held lease by strict majority. Unlike a failed grant, a
	/// failed renew does NOT release votes: the holder keeps writing until its
	/// renew deadline passes, then stops itself.
	async fn renew_one_lease(
		&self,
		volume: &str,
		holder: &str,
		epoch: u64,
		ttl: f64,
	) -> MeshResult<LeaseRecord> {
		let local = self
			.leases
			.vote_renew(volume, holder, epoch, ttl)
			.map_err(engine_to_mesh)?;
		let needed = lease::majority_needed(self.expected_members()).map_err(engine_to_mesh)?;
		let mut votes = usize::from(local.granted);
		let mut record = local.record.clone().filter(|_| local.granted);
		let payload = json!({
			"volume": volume,
			"holder_node": holder,
			"epoch": epoch,
			"ttl": ttl,
		});
		for peer in self.peers() {
			if !peer.healthy {
				continue;
			}
			let posted = self
				.transport
				.post(&peer.advertise, "/v1/mesh/lease/renew", &payload, None)
				.await;
			let Ok(object) = posted else {
				continue;
			};
			let decision = lease_decision(Value::Object(object))?;
			if decision.granted {
				votes += 1;
				if record.is_none() {
					record = decision.record;
				}
			}
		}
		if votes >= needed && local.granted {
			return record.ok_or_else(|| MeshError::invalid("lease renew missing record"));
		}
		Err(MeshError::with_code(
			format!("lease renew for volume '{volume}' got {votes}/{needed} votes"),
			lease::LEASE_UNAVAILABLE_CODE,
		))
	}

	/// Renew local writable-volume leases, stopping writers that miss their
	/// renew deadline. Port of Python `renew_writable_volume_leases_once`.
	async fn renew_writable_volume_leases(&self) -> MeshResult<()> {
		if !self.enabled() {
			return Ok(());
		}
		let node_id = self.node_id();
		for (name, leases, writable) in self.engine.engine.mesh_volume_lease_records() {
			let mut refreshed: Vec<LeaseRecord> = Vec::new();
			let mut stop_reason: Option<String> = None;
			for volume in writable {
				let Some(data) = leases.get(&volume).and_then(Value::as_object) else {
					continue;
				};
				let holder = data
					.get("holder")
					.and_then(Value::as_str)
					.unwrap_or_default()
					.to_owned();
				if holder != node_id {
					continue;
				}
				let epoch = data.get("epoch").and_then(Value::as_u64).unwrap_or(0);
				let ttl = data
					.get("ttl")
					.and_then(Value::as_f64)
					.unwrap_or(DEFAULT_TTL);
				let granted_at = data
					.get("granted_at")
					.and_then(Value::as_f64)
					.unwrap_or(0.0);
				let renew_deadline = data
					.get("renew_deadline")
					.and_then(Value::as_f64)
					.unwrap_or(granted_at + ttl / 2.0);
				match self.renew_one_lease(&volume, &holder, epoch, ttl).await {
					Ok(lease) => refreshed.push(lease),
					Err(err) => {
						if unix_now() >= renew_deadline {
							stop_reason = Some(err.message.clone());
							break;
						}
						tracing::warn!("lease renew deferred for {name}/{volume}: {}", err.message);
					},
				}
			}
			if let Some(reason) = stop_reason {
				tracing::warn!("stopping {name} after writable-volume lease renewal failure: {reason}");
				let stopped = self.engine.engine.mesh_stop_sandbox(&name);
				let released = self.release_recorded_leases(&name, &leases).await;
				stopped.map_err(engine_to_mesh)?;
				released?;
			} else if !refreshed.is_empty() {
				let values = refreshed
					.iter()
					.map(lease_record_value)
					.collect::<MeshResult<Vec<_>>>()?;
				self
					.engine
					.engine
					.mesh_record_volume_leases(&name, values)
					.map_err(engine_to_mesh)?;
			}
		}
		Ok(())
	}

	/// Release every vote in a recorded `volume_leases` map, then clear the
	/// sandbox's lease metadata. The clear only happens after a successful
	/// release so an incomplete release stays visible on the record.
	async fn release_recorded_leases(
		&self,
		sid: &str,
		leases: &Map<String, Value>,
	) -> MeshResult<()> {
		let values = leases.values().cloned().collect::<Vec<_>>();
		self.release_lease_values(values).await?;
		self
			.engine
			.engine
			.mesh_clear_volume_leases(sid)
			.map_err(engine_to_mesh)
	}

	async fn release_lease_records(&self, leases: &[LeaseRecord]) -> MeshResult<()> {
		let values = leases
			.iter()
			.map(lease_record_value)
			.collect::<MeshResult<Vec<_>>>()?;
		self.release_lease_values(values).await
	}

	async fn release_lease_values(&self, leases: Vec<Value>) -> MeshResult<()> {
		for value in leases {
			let Some((volume, holder, epoch)) = lease_tuple(&value) else {
				continue;
			};
			self
				.leases
				.vote_release(&volume, &holder, epoch)
				.map_err(engine_to_mesh)?;
			let payload = json!({"volume": volume, "holder_node": holder, "epoch": epoch});
			for peer in self.peers() {
				let _ = self
					.transport
					.post(&peer.advertise, "/v1/mesh/lease/release", &payload, None)
					.await;
			}
		}
		Ok(())
	}

	fn record_orphan_for_dead_owner(&self, dead: &str) {
		let owned = self
			.owners
			.read()
			.iter()
			.filter(|(_, (owner, _))| owner.as_str() == dead)
			.map(|(sid, _)| sid.clone())
			.collect::<Vec<_>>();
		let mut orphans = self.orphans.lock();
		for sid in owned {
			if !orphans
				.iter()
				.any(|(seen_sid, seen_dead)| seen_sid == &sid && seen_dead == dead)
			{
				orphans.push((sid, dead.to_owned()));
			}
		}
	}
}

impl MeshControl for MeshRuntime {
	fn node_id(&self) -> String {
		self.membership.read().node_id.clone()
	}

	fn advertise(&self) -> String {
		self.membership.read().advertise.clone()
	}

	fn default_advertise(&self) -> String {
		self.default_advertise.clone()
	}

	fn caps(&self) -> NodeCapsWire {
		caps_wire(self.membership.read().caps)
	}

	fn enabled(&self) -> bool {
		self.membership.read().enabled
	}

	fn peers(&self) -> Vec<PeerMember> {
		let membership = self.membership.read().clone();
		let now = unix_now();
		membership
			.peers
			.values()
			.map(|peer| PeerMember {
				node_id:   peer.node_id.clone(),
				advertise: peer.advertise.clone(),
				healthy:   peer.healthy(now, self.config.mesh_heartbeat_sec),
			})
			.collect()
	}

	fn expected_members(&self) -> usize {
		self.membership.read().expected_members
	}

	fn quorum_needed(&self) -> usize {
		self.membership.read().quorum_needed()
	}

	fn setup(
		&self,
		advertise: String,
		region: String,
		caps: Option<NodeCapsWire>,
	) -> MeshResult<MeshSetupResult> {
		let mut membership = self.membership.write();
		let node_id = self.node_id.clone();
		let caps = caps.map_or(membership.caps, node_caps);
		*membership = MembershipState::enabled(node_id, advertise, region, caps);
		self.save_membership(&membership)?;
		Ok(MeshSetupResult {
			blob:      state::encode_blob(&membership.advertise, &self.token.read()),
			node_id:   membership.node_id.clone(),
			advertise: membership.advertise.clone(),
		})
	}

	fn join(&self, blob: String, advertise: Option<String>, region: String) -> MeshResult<Value> {
		let (peer_url, token) = state::decode_blob(&blob)?;
		if self.token.read().is_empty() && !token.is_empty() {
			*self.token.write() = token;
		}
		let caps = self.membership.read().caps;
		let mut membership = MembershipState::enabled(
			self.node_id.clone(),
			advertise.unwrap_or_else(|| self.default_advertise.clone()),
			region,
			caps,
		);
		let mut peer = NodeState::new("seed", peer_url.clone());
		peer.node_id = format!("seed-{}", state::new_node_id());
		peer.last_seen = unix_now();
		membership.peers.insert(peer.node_id.clone(), peer);
		membership.bump_expected();
		self.save_membership(&membership)?;
		*self.membership.write() = membership.clone();
		Ok(json!({
			"ok": true,
			"node_id": membership.node_id,
			"advertise": membership.advertise,
			"peers": [peer_url],
		}))
	}

	fn request_replication(&self) {
		self.replicate_notify.notify_one();
	}

	fn leave(&self) -> MeshResult<()> {
		let mut membership = self.membership.write();
		membership.enabled = false;
		membership.peers.clear();
		membership.expected_members = 1;
		self.save_membership(&membership)
	}

	fn status(&self) -> MeshResult<Value> {
		let membership = self.membership.read().clone();
		let self_state = self.local_state().to_wire();
		let peers = membership
			.peers
			.values()
			.map(NodeState::to_wire)
			.collect::<Vec<_>>();
		Ok(json!({
			"enabled": membership.enabled,
			"node_id": membership.node_id,
			"advertise": membership.advertise,
			"region": membership.region,
			"expected_members": membership.expected_members,
			"quorum": membership.quorum_needed(),
			"self": self_state,
			"peers": peers,
		}))
	}

	fn register(&self, state: Map<String, Value>) -> MeshResult<Value> {
		self.merge_node_state(Value::Object(state))?;
		Ok(json!({"members": self.known_members_wire()}))
	}

	fn heartbeat(&self, state: Map<String, Value>, known: Vec<Value>) -> MeshResult<Value> {
		self.merge_node_state(Value::Object(state))?;
		for item in known {
			let _ = self.merge_node_state(item);
		}
		Ok(json!({"state": self.self_state_wire(), "known": self.known_members_wire()}))
	}

	fn depart(&self, node_id: &str) {
		let mut membership = self.membership.write();
		if membership.depart(node_id) {
			let _ = self.save_membership(&membership);
			self.record_orphan_for_dead_owner(node_id);
		}
	}

	fn is_peer_healthy(&self, node_id: &str) -> bool {
		self
			.membership
			.read()
			.is_member_healthy(node_id, unix_now(), self.config.mesh_heartbeat_sec)
	}

	fn migration_target(&self, node_id: &str) -> MeshResult<PeerMember> {
		self
			.peers()
			.into_iter()
			.find(|peer| peer.node_id == node_id)
			.ok_or_else(|| MeshError::unreachable(format!("unknown mesh node {node_id}")))
	}

	fn replica_targets(&self, sid: &str, k: usize) -> Vec<String> {
		let members = self.peers();
		let mut peers = members
			.iter()
			.filter(|peer| peer.healthy)
			.map(|peer| peer.node_id.clone())
			.collect::<Vec<_>>();
		if peers.is_empty() {
			peers = members.into_iter().map(|peer| peer.node_id).collect();
		}
		place::sort_by_hrw_desc(sid, &mut peers);
		peers.truncate(k);
		peers
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		self
			.membership
			.read()
			.peers
			.get(node_id)
			.map(|peer| peer.advertise.clone())
	}

	fn find_template_provider(&self, key: &str) -> Option<(String, String)> {
		let now = unix_now();
		self
			.membership
			.read()
			.peers
			.values()
			.filter(|peer| peer.healthy(now, self.config.mesh_heartbeat_sec))
			.find_map(|peer| {
				peer
					.template_index
					.get(key)
					.cloned()
					.map(|digest| (peer.advertise.clone(), digest))
			})
	}

	fn request_template_key(&self, params: &Map<String, Value>) -> Option<String> {
		place::request_template_key(&PlacementRequest::from_map(params.clone()))
	}

	fn has_local_template_key(&self, key: &str) -> bool {
		let local = self.local_state();
		local.templates.iter().any(|template| template == key)
			|| local.template_index.contains_key(key)
	}

	fn authoritative_owner(&self, sid: &str) -> Option<(String, i64)> {
		self
			.owners
			.read()
			.get(sid)
			.map(|(owner, epoch)| (owner.clone(), u64_to_i64(*epoch)))
	}

	fn local_epoch(&self, sid: &str) -> i64 {
		u64_to_i64(self.local_epoch_u64(sid))
	}

	fn owner_of(&self, sid: &str) -> Option<String> {
		self.owners.read().get(sid).map(|(owner, _)| owner.clone())
	}

	fn record_owner(&self, sid: &str, node_id: &str, epoch: i64) {
		let epoch = i64_to_u64(epoch);
		let mut owners = self.owners.write();
		let accept = owners
			.get(sid)
			.is_none_or(|(_, existing_epoch)| epoch >= *existing_epoch);
		if accept {
			owners.insert(sid.to_owned(), (node_id.to_owned(), epoch));
		}
	}

	fn broadcast_owner(&self, sid: &str, node_id: &str, epoch: i64) {
		self.record_owner(sid, node_id, epoch);
	}

	fn forget_owner(&self, sid: &str) {
		self.owners.write().remove(sid);
	}

	fn mark_unhealthy(&self, node_id: &str) {
		if let Some(peer) = self.membership.write().peers.get_mut(node_id) {
			peer.last_seen = 0.0;
		}
		self.record_orphan_for_dead_owner(node_id);
	}

	fn pinned_local(&self, params: &Map<String, Value>) -> bool {
		place::pinned_local(&PlacementRequest::from_map(params.clone()))
	}

	fn place(&self, params: &Map<String, Value>) -> MeshResult<String> {
		if !self.enabled() {
			return Ok(self.node_id());
		}
		Ok(place::place(
			&PlacementRequest::from_map(params.clone()),
			&self.context(),
			&self.all_nodes(),
			unix_now(),
		)?)
	}

	fn coordinator_for(&self, key: &str) -> String {
		let live = self
			.membership
			.read()
			.live_member_ids(unix_now(), self.config.mesh_heartbeat_sec);
		place::hrw_winner(key, live.iter().map(String::as_str))
			.map_or_else(|| MeshControl::node_id(self), str::to_owned)
	}

	fn idem_owner(&self, key: &str) -> Option<String> {
		self.idem_pins.read().get(key).cloned()
	}

	fn idem_pin(&self, key: &str, owner: &str) -> String {
		let mut pins = self.idem_pins.write();
		pins
			.entry(key.to_owned())
			.or_insert_with(|| owner.to_owned())
			.clone()
	}

	fn idem_unpin(&self, key: &str) {
		self.idem_pins.write().remove(key);
	}

	fn worker_begin(&self, key: &str) -> bool {
		self.workers.lock().insert(key.to_owned())
	}

	fn worker_end(&self, key: &str) {
		self.workers.lock().remove(key);
	}
}

impl HeartbeatView for MeshRuntime {
	fn heartbeat_peers(&self) -> Vec<PeerEndpoint> {
		self
			.membership
			.read()
			.peers
			.values()
			.map(|peer| PeerEndpoint {
				node_id:   peer.node_id.clone(),
				advertise: peer.advertise.clone(),
			})
			.collect()
	}

	fn self_state_wire(&self) -> Value {
		self.local_state().to_wire()
	}

	fn known_members_wire(&self) -> Vec<Value> {
		let mut members = vec![self.self_state_wire()];
		members.extend(
			self
				.membership
				.read()
				.peers
				.values()
				.map(NodeState::to_wire),
		);
		members
	}

	fn merge_heartbeat_response(&self, response: Map<String, Value>) -> MeshResult<()> {
		if let Some(state) = response.get("state") {
			self.merge_node_state(state.clone())?;
		}
		if let Some(Value::Array(known)) = response.get("known") {
			for item in known {
				let _ = self.merge_node_state(item.clone());
			}
		}
		Ok(())
	}

	fn reap_stale_peers(&self) {
		let now = unix_now();
		let reap_sec = self.config.mesh_reap_sec;
		let mut dead = Vec::new();
		{
			let mut membership = self.membership.write();
			membership.peers.retain(|node_id, peer| {
				let keep = peer.last_seen <= 0.0 || now - peer.last_seen <= reap_sec;
				if !keep {
					dead.push(node_id.clone());
				}
				keep
			});
			if !dead.is_empty() {
				let _ = self.save_membership(&membership);
			}
		}
		for node_id in dead {
			self.record_orphan_for_dead_owner(&node_id);
		}
	}
}

impl MeshRecordStore for MeshRuntime {
	fn get(&self, sid: &str) -> MeshResult<Option<CreateRecordWire>> {
		Ok(self.records.get(sid).map(record_wire))
	}

	fn put(&self, record: CreateRecordWire) -> MeshResult<()> {
		self
			.records
			.put(create_record(record)?)
			.map_err(engine_to_mesh)
	}

	fn remove(&self, sid: &str) -> MeshResult<()> {
		self.records.remove(sid).map_err(engine_to_mesh)
	}
}

impl MeshReplicaStore for MeshRuntime {
	fn get(&self, sid: &str) -> MeshResult<Option<ReplicaRecordWire>> {
		Ok(self.replicas.get(sid).map(replica_wire))
	}

	fn put(
		&self,
		sid: String,
		digest: String,
		source_node: String,
		snapshot_dir: String,
		params: Map<String, Value>,
	) -> MeshResult<()> {
		self
			.replicas
			.put(sid, digest, source_node, snapshot_dir, params)
			.map_err(engine_to_mesh)
	}

	fn list(&self) -> MeshResult<Vec<String>> {
		Ok(self.replicas.list())
	}

	fn remove(&self, sid: &str) -> MeshResult<()> {
		self.replicas.remove(sid).map_err(engine_to_mesh)
	}
}

impl MeshLeaseManager for MeshRuntime {
	fn vote_grant(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<Map<String, Value>> {
		decision_object(
			self
				.leases
				.vote_grant(&volume, &holder_node, i64_to_u64(epoch), ttl),
		)
	}

	fn vote_renew(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<Map<String, Value>> {
		decision_object(
			self
				.leases
				.vote_renew(&volume, &holder_node, i64_to_u64(epoch), ttl),
		)
	}

	fn vote_release(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
	) -> MeshResult<Map<String, Value>> {
		decision_object(
			self
				.leases
				.vote_release(&volume, &holder_node, i64_to_u64(epoch)),
		)
	}

	fn acquire_writable_volume_leases(
		&self,
		params: Map<String, Value>,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<Vec<Value>>> {
		async move {
			let records = self
				.acquire_writable_leases(&params, i64_to_u64(epoch))
				.await?;
			records
				.iter()
				.map(lease_record_value)
				.collect::<MeshResult<Vec<_>>>()
		}
		.boxed()
	}

	fn release_leases(&self, leases: Vec<Value>) -> BoxFuture<'_, MeshResult<()>> {
		async move { self.release_lease_values(leases).await }.boxed()
	}

	fn release_record_volume_leases(&self, sid: String) -> BoxFuture<'_, MeshResult<()>> {
		async move {
			let view = self.engine.get_view(&sid)?;
			let leases = view
				.get("volume_leases")
				.and_then(Value::as_object)
				.cloned()
				.unwrap_or_default();
			self.release_recorded_leases(&sid, &leases).await
		}
		.boxed()
	}
}

impl reconciler::MeshReconcileState for MeshRuntime {
	fn enabled(&self) -> bool {
		MeshControl::enabled(self)
	}

	fn interval(&self) -> f64 {
		self.config.mesh_heartbeat_sec.max(1.0)
	}

	fn node_id(&self) -> &str {
		&self.node_id
	}

	fn advertise(&self) -> String {
		self.membership.read().advertise.clone()
	}

	fn create_timeout(&self) -> Duration {
		secs(self.config.mesh_create_timeout_sec)
	}

	fn expected_members(&self) -> usize {
		MeshControl::expected_members(self)
	}

	fn live_member_ids(&self) -> Vec<String> {
		self
			.membership
			.read()
			.live_member_ids(unix_now(), self.config.mesh_heartbeat_sec)
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		MeshControl::peer_url(self, node_id)
	}

	fn is_peer_healthy(&self, node_id: &str) -> bool {
		MeshControl::is_peer_healthy(self, node_id)
	}

	fn owner_of(&self, sid: &str) -> Option<String> {
		MeshControl::owner_of(self, sid)
	}

	fn restore_owner(&self, sid: &str, exclude: &HashSet<String>) -> Option<String> {
		let mut candidates = self.live_member_ids();
		candidates.retain(|node_id| !exclude.contains(node_id));
		place::hrw_winner(sid, candidates.iter().map(String::as_str)).map(str::to_owned)
	}

	fn restore_quorum_met(&self, confirmations: usize) -> bool {
		self.membership.read().restore_quorum_met(confirmations)
	}

	fn quorum_needed(&self) -> usize {
		MeshControl::quorum_needed(self)
	}

	fn drain_orphans(&self) -> Vec<(String, String)> {
		std::mem::take(&mut *self.orphans.lock())
	}

	fn requeue_orphan(&self, sid: &str, dead: &str) {
		self.orphans.lock().push((sid.to_owned(), dead.to_owned()));
	}

	fn next_epoch(&self, sid: &str) -> u64 {
		self.local_epoch_u64(sid).saturating_add(1)
	}

	fn broadcast_owner(&self, sid: &str, node_id: &str, epoch: u64) -> Result<()> {
		MeshControl::record_owner(self, sid, node_id, u64_to_i64(epoch));
		Ok(())
	}

	fn update_record_owner(
		&self,
		sid: &str,
		node_id: &str,
		epoch: u64,
		_require_quorum: bool,
	) -> Result<()> {
		let _ = self.records.update_owner(sid, node_id, u64_to_i64(epoch))?;
		MeshControl::record_owner(self, sid, node_id, u64_to_i64(epoch));
		Ok(())
	}

	fn worker_begin(&self, key: &str) -> bool {
		MeshControl::worker_begin(self, key)
	}

	fn worker_end(&self, key: &str) {
		MeshControl::worker_end(self, key);
	}

	fn note_event(&self, event: &str) {
		self.events.lock().push(event.to_owned());
	}

	fn replica_targets(&self, sid: &str, replicas: usize) -> Vec<String> {
		MeshControl::replica_targets(self, sid, replicas)
	}

	fn fenced_local_ids(&self, owned_ids: &[String]) -> Vec<String> {
		let local = reconciler::MeshReconcileState::node_id(self).to_owned();
		owned_ids
			.iter()
			.filter(|sid| {
				reconciler::MeshReconcileState::owner_of(self, sid).is_some_and(|owner| owner != local)
			})
			.cloned()
			.collect()
	}

	fn forget_local_owner(&self, sid: &str) {
		MeshControl::forget_owner(self, sid);
	}
}

impl reconciler::CreateRecordStore for MeshRuntime {
	fn get(&self, sid: &str) -> Option<reconciler::CreateRecord> {
		self.records.get(sid).map(reconcile_record)
	}

	fn reconcile_records(&self) -> Result<()> {
		self.records.load();
		Ok(())
	}
}

impl reconciler::ReplicaStore for MeshRuntime {
	fn get(&self, sid: &str) -> Option<reconciler::ReplicaRecord> {
		self.replicas.get(sid).map(reconcile_replica)
	}

	fn holds(&self, sid: &str) -> bool {
		self.replicas.holds(sid)
	}

	fn secrets_ready(&self, sid: &str) -> bool {
		self.replicas.secrets_ready(sid)
	}

	fn drop_replica(&self, sid: &str) -> Result<()> {
		self.replicas.drop_replica(sid)
	}
}

impl reconciler::LeaseManager for MeshRuntime {
	fn renew_writable_volume_leases_once(&self) -> BoxFuture<'_, Result<()>> {
		async move {
			self
				.renew_writable_volume_leases()
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn acquire_writable_volume_leases<'a>(
		&'a self,
		params: &'a Value,
		epoch: u64,
	) -> BoxFuture<'a, Result<Vec<LeaseRecord>>> {
		async move {
			let params = params.as_object().cloned().unwrap_or_default();
			self
				.acquire_writable_leases(&params, epoch)
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn release_leases<'a>(&'a self, leases: &'a [LeaseRecord]) -> BoxFuture<'a, Result<()>> {
		async move {
			self
				.release_lease_records(leases)
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn release_record_volume_leases<'a>(
		&'a self,
		record: &'a reconciler::SandboxRecord,
	) -> BoxFuture<'a, Result<()>> {
		async move {
			let leases = record
				.detail
				.get("volume_leases")
				.and_then(Value::as_object)
				.cloned()
				.unwrap_or_default();
			self
				.release_recorded_leases(&record.name, &leases)
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}
}

#[derive(Clone)]
pub struct MeshEngineAdapter {
	engine: Arc<Engine>,
}

impl MeshEngineAdapter {
	const fn new(engine: Arc<Engine>) -> Self {
		Self { engine }
	}
}

impl MeshEngine for MeshEngineAdapter {
	fn owned_ids(&self) -> MeshResult<Vec<String>> {
		self.engine.mesh_owned_ids().map_err(engine_to_mesh)
	}

	fn list_views(&self) -> MeshResult<Vec<Value>> {
		self.engine.mesh_list_views().map_err(engine_to_mesh)
	}

	fn has_sandbox(&self, sid: &str) -> bool {
		self.engine.mesh_has_sandbox(sid)
	}

	fn get_view(&self, sid: &str) -> MeshResult<Value> {
		self.engine.mesh_get_view(sid).map_err(engine_to_mesh)
	}

	fn checkpoint_age_sec(&self, sid: &str) -> Option<f64> {
		self.engine.mesh_checkpoint_age_sec(sid)
	}

	fn find_by_idempotency_key(&self, key: &str) -> MeshResult<Option<Value>> {
		self
			.engine
			.mesh_find_by_idempotency_key(key)
			.map_err(engine_to_mesh)
	}

	fn record_idempotency(&self, sid: &str, key: &str) -> MeshResult<()> {
		self
			.engine
			.mesh_record_idempotency(sid, key)
			.map_err(engine_to_mesh)
	}

	fn record_volume_leases(&self, sid: &str, leases: Vec<Value>) -> MeshResult<()> {
		self
			.engine
			.mesh_record_volume_leases(sid, leases)
			.map_err(engine_to_mesh)
	}

	fn create_sandbox(&self, params: Map<String, Value>) -> BoxFuture<'_, MeshResult<Value>> {
		let engine = self.engine.clone();
		async move { spawn_engine(move || engine.mesh_create_from_params(params)).await }.boxed()
	}

	fn run_detached(&self, params: Map<String, Value>) -> BoxFuture<'_, MeshResult<Value>> {
		self.create_sandbox(params)
	}

	fn restore_detached(&self, params: Map<String, Value>) -> BoxFuture<'_, MeshResult<Value>> {
		self.create_sandbox(params)
	}

	fn restore_from_template(
		&self,
		params: Map<String, Value>,
		template_dir: String,
		quorum_ok: bool,
	) -> BoxFuture<'_, MeshResult<Value>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine.mesh_restore_from_template(params, Path::new(&template_dir), quorum_ok)
			})
			.await
		}
		.boxed()
	}

	fn migrate_precopy(&self, sid: String) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				let prep = engine.mesh_migrate_precopy(&sid)?;
				Ok(MigratePrepareWire {
					digest:       prep.digest,
					snapshot_dir: prep.snapshot_dir.to_string_lossy().into_owned(),
					params:       params_object(&prep.params)?,
				})
			})
			.await
		}
		.boxed()
	}

	fn migrate_finalize(
		&self,
		sid: String,
		base_dir: String,
	) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				let prep = engine.mesh_migrate_finalize(&sid, Path::new(&base_dir))?;
				Ok(MigratePrepareWire {
					digest:       prep.digest,
					snapshot_dir: prep.snapshot_dir.to_string_lossy().into_owned(),
					params:       params_object(&prep.params)?,
				})
			})
			.await
		}
		.boxed()
	}

	fn checkpoint_discard(
		&self,
		snapshot_dir: String,
		digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || engine.mesh_replicate_cleanup(&digest, Path::new(&snapshot_dir)))
				.await
		}
		.boxed()
	}

	fn migrate_abort(
		&self,
		sid: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
		params: Map<String, Value>,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine
					.mesh_migrate_abort(&sid, &base_digest, Path::new(&delta_dir), &delta_digest, params)
					.map(|_| ())
			})
			.await
		}
		.boxed()
	}

	fn migrate_commit(
		&self,
		sid: String,
		base_dir: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine.mesh_migrate_commit(
					&sid,
					Path::new(&base_dir),
					&base_digest,
					Path::new(&delta_dir),
					&delta_digest,
				)
			})
			.await
		}
		.boxed()
	}
}

impl reconciler::ReconcileEngine for MeshEngineAdapter {
	fn owned_ids(&self) -> Result<Vec<String>> {
		self.engine.mesh_owned_ids()
	}

	fn get(&self, sid: &str) -> Result<reconciler::SandboxRecord> {
		sandbox_record(self.engine.mesh_get_view(sid)?)
	}

	fn remove(&self, sid: &str) -> Result<()> {
		let _ = EngineApi::remove(&*self.engine, sid)?;
		Ok(())
	}

	fn replicate_prepare<'a>(
		&'a self,
		sid: &'a str,
	) -> futures_util::future::BoxFuture<'a, Result<reconciler::ReplicatePreparation>> {
		let engine = self.engine.clone();
		let sid = sid.to_owned();
		async move {
			tokio::task::spawn_blocking(move || engine.mesh_replicate_prepare(&sid))
				.await
				.map_err(|err| {
					EngineError::engine(format!("mesh replicate prepare task failed: {err}"))
				})?
		}
		.boxed()
	}

	fn replicate_cleanup(&self, digest: &str, snapshot_dir: &Path) -> Result<()> {
		self.engine.mesh_replicate_cleanup(digest, snapshot_dir)
	}

	fn restore_from_template(
		&self,
		params: &Value,
		snapshot_dir: &Path,
		quorum_ok: bool,
	) -> Result<reconciler::SandboxRecord> {
		let params = params.as_object().cloned().unwrap_or_default();
		sandbox_record(
			self
				.engine
				.mesh_restore_from_template(params, snapshot_dir, quorum_ok)?,
		)
	}

	fn record_volume_leases(&self, sid: &str, leases: &[LeaseRecord]) -> Result<()> {
		let values = leases
			.iter()
			.map(lease_record_value)
			.collect::<MeshResult<Vec<_>>>()
			.map_err(mesh_to_engine)?;
		self.engine.mesh_record_volume_leases(sid, values)
	}

	fn record_idempotency(&self, sid: &str, key: &str) -> Result<()> {
		self.engine.mesh_record_idempotency(sid, key)
	}

	fn set_ha_metadata(&self, sid: &str, ha: &str, restart_policy: &str) -> Result<()> {
		self.engine.mesh_set_ha_metadata(sid, ha, restart_policy)
	}

	fn create_from_record(&self, params: &Value) -> Result<reconciler::SandboxRecord> {
		sandbox_record(
			self
				.engine
				.mesh_create_from_params(params_object(params)?)?,
		)
	}

	fn run_from_record(&self, params: &Value) -> Result<Value> {
		self.engine.mesh_create_from_params(params_object(params)?)
	}

	fn restore_from_record(&self, params: &Value) -> Result<Value> {
		self.engine.mesh_create_from_params(params_object(params)?)
	}
}

struct TemplateTransfer;

impl MeshTemplateTransfer for TemplateTransfer {
	fn pull_template<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		async move {
			let path = transfer::pull_template(client, &peer_url, &digest, &token)
				.await
				.map_err(engine_to_mesh)?;
			Ok(path.to_string_lossy().into_owned())
		}
		.boxed()
	}

	fn pull_template_metadata<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<MetadataPull>> {
		async move {
			let path = transfer::pull_template_metadata(client, &peer_url, &digest, &token)
				.await
				.map_err(engine_to_mesh)?;
			let metadata = transfer::remote_page_metadata(&path).map_err(engine_to_mesh)?;
			Ok(MetadataPull {
				template:        path.to_string_lossy().into_owned(),
				remote_page_url: metadata.page_url,
				digest:          metadata.digest,
			})
		}
		.boxed()
	}
}

async fn heartbeat_loop(runtime: Arc<MeshRuntime>, shutdown: &mut broadcast::Receiver<()>) {
	let mut ticker = tokio::time::interval(secs(runtime.config.mesh_heartbeat_sec));
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	loop {
		tokio::select! {
			_ = shutdown.recv() => break,
			_ = ticker.tick() => {
				if runtime.enabled() {
					let _ = gossip::heartbeat_once(&*runtime, runtime.peer_client()).await;
				}
			}
		}
	}
}

async fn reconcile_loop(runtime: Arc<MeshRuntime>, shutdown: &mut broadcast::Receiver<()>) {
	let mut ticker = tokio::time::interval(secs(runtime.config.mesh_heartbeat_sec.max(1.0)));
	// A long pass (peer pulls) must not queue a burst of catch-up passes.
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	let mut last = HashMap::new();
	// One reconciler for the loop's lifetime: its per-sid checkpoint throttle
	// only works if it survives across passes and notify wakeups.
	let ctx = Reconciler::new(
		runtime.reconcile_config(),
		&*runtime,
		&runtime.engine,
		&*runtime,
		&*runtime,
		&*runtime,
		runtime.peer_client().client(),
	);
	loop {
		tokio::select! {
			_ = shutdown.recv() => break,
			_ = ticker.tick() => {
				run_reconcile_pass(&runtime, &ctx, &mut last).await;
			}
			() = runtime.replicate_notify.notified() => {
				run_reconcile_pass(&runtime, &ctx, &mut last).await;
			}
		}
	}
}

async fn run_reconcile_pass(
	runtime: &Arc<MeshRuntime>,
	ctx: &Reconciler<'_>,
	last: &mut HashMap<String, String>,
) {
	if !runtime.enabled() {
		return;
	}
	if let Err(err) = ctx.ha_reconcile_once().await {
		tracing::warn!(error = %err, "mesh HA reconciliation failed");
	}
	if let Err(err) = ctx.replicate_once(last).await {
		tracing::warn!(error = %err, "mesh replica loop failed");
	}
}

async fn spawn_engine<F, T>(call: F) -> MeshResult<T>
where
	F: FnOnce() -> Result<T> + Send + 'static,
	T: Send + 'static,
{
	tokio::task::spawn_blocking(call)
		.await
		.map_err(|err| MeshError::with_code(format!("engine task failed: {err}"), "engine_error"))?
		.map_err(engine_to_mesh)
}

fn disabled_membership(caps: NodeCaps, advertise: &str) -> MembershipState {
	MembershipState {
		enabled: false,
		node_id: state::new_node_id(),
		advertise: advertise.to_owned(),
		region: String::new(),
		caps,
		peers: BTreeMap::new(),
		expected_members: 1,
	}
}

const fn placement_weights(config: &ServeConfig) -> PlacementWeights {
	PlacementWeights {
		warm:     config.mesh_w_warm,
		free:     config.mesh_w_free,
		local:    config.mesh_w_local,
		region:   config.mesh_w_region,
		inflight: config.mesh_w_inflight,
	}
}

fn caps_wire(caps: NodeCaps) -> NodeCapsWire {
	NodeCapsWire { vcpus: caps.vcpus.min(u64::from(u32::MAX)) as u32, mem_mib: caps.mem_mib }
}

fn node_caps(caps: NodeCapsWire) -> NodeCaps {
	NodeCaps { vcpus: u64::from(caps.vcpus), mem_mib: caps.mem_mib }
}

fn secs(value: f64) -> Duration {
	Duration::from_secs_f64(value.max(1.0))
}

fn value_u64(value: &Value) -> Option<u64> {
	value
		.as_u64()
		.or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
		.or_else(|| {
			value
				.as_f64()
				.filter(|value| *value >= 0.0)
				.map(|value| value as u64)
		})
}

#[allow(
	clippy::unnecessary_wraps,
	reason = "callers share MeshResult plumbing with validators even though parsing is infallible"
)]
pub(crate) fn writable_volumes(params: &Map<String, Value>) -> MeshResult<Vec<String>> {
	let mut volumes = BTreeSet::new();
	let Some(raw) = params.get("volumes") else {
		return Ok(Vec::new());
	};
	match raw {
		Value::Object(object) => {
			for value in object.values() {
				if let Some((name, read_only)) = volume_name_and_mode(value)
					&& !read_only
				{
					volumes.insert(name);
				}
			}
		},
		Value::Array(items) => {
			for value in items {
				if let Some((name, read_only)) = volume_name_and_mode(value)
					&& !read_only
				{
					volumes.insert(name);
				}
			}
		},
		_ => {},
	}
	Ok(volumes.into_iter().collect())
}

fn volume_name_and_mode(value: &Value) -> Option<(String, bool)> {
	match value {
		Value::String(name) => Some((name.clone(), false)),
		Value::Object(object) => object.get("name").and_then(Value::as_str).map(|name| {
			let read_only = object
				.get("read_only")
				.or_else(|| object.get("ro"))
				.and_then(Value::as_bool)
				.unwrap_or(false);
			(name.to_owned(), read_only)
		}),
		_ => None,
	}
}

fn decision_object(result: Result<LeaseDecision>) -> MeshResult<Map<String, Value>> {
	let value = serde_json::to_value(result.map_err(engine_to_mesh)?)
		.map_err(|err| MeshError::invalid(format!("lease decision serialization failed: {err}")))?;
	value_object(value)
}

fn lease_decision(value: Value) -> MeshResult<LeaseDecision> {
	serde_json::from_value(value)
		.map_err(|err| MeshError::invalid(format!("invalid lease decision: {err}")))
}

fn lease_record_value(record: &LeaseRecord) -> MeshResult<Value> {
	serde_json::to_value(record.to_wire())
		.map_err(|err| MeshError::invalid(format!("lease record serialization failed: {err}")))
}

fn lease_tuple(value: &Value) -> Option<(String, String, u64)> {
	let object = value.as_object()?;
	let volume = object.get("volume")?.as_str()?.to_owned();
	let holder = object
		.get("holder")
		.or_else(|| object.get("holder_node"))?
		.as_str()?
		.to_owned();
	let epoch = object.get("epoch").and_then(value_u64)?;
	Some((volume, holder, epoch))
}

fn record_wire(record: record::CreateRecord) -> CreateRecordWire {
	CreateRecordWire {
		sid:             record.sid,
		params:          record.params,
		owner:           record.owner,
		epoch:           record.epoch,
		idempotency_key: record.idempotency_key,
		ha:              record.ha,
		restart_policy:  record.restart_policy,
		created_at:      record.created_at,
	}
}

fn create_record(record: CreateRecordWire) -> MeshResult<record::CreateRecord> {
	record::CreateRecord::new(
		record.sid,
		record.params,
		record.owner,
		record.epoch,
		record.idempotency_key,
		record.ha,
		record.restart_policy,
		record.created_at,
	)
	.map_err(engine_to_mesh)
}

fn replica_wire(record: replica::ReplicaRecord) -> ReplicaRecordWire {
	ReplicaRecordWire {
		sid:           record.sid,
		digest:        record.digest,
		source_node:   record.source_node,
		snapshot_dir:  record.snapshot_dir,
		params:        record.params,
		needs_secrets: record.needs_secrets,
	}
}

fn reconcile_record(record: record::CreateRecord) -> reconciler::CreateRecord {
	reconciler::CreateRecord {
		sid:             record.sid,
		params:          Value::Object(record.params),
		owner:           record.owner,
		epoch:           i64_to_u64(record.epoch),
		idempotency_key: record.idempotency_key,
		ha:              record.ha,
		restart_policy:  record.restart_policy,
	}
}

fn reconcile_replica(record: replica::ReplicaRecord) -> reconciler::ReplicaRecord {
	reconciler::ReplicaRecord {
		sid:           record.sid,
		digest:        record.digest,
		source_node:   record.source_node,
		snapshot_dir:  PathBuf::from(record.snapshot_dir),
		params:        Value::Object(record.params),
		needs_secrets: record.needs_secrets,
	}
}

fn sandbox_record(value: Value) -> Result<reconciler::SandboxRecord> {
	let object = value
		.as_object()
		.cloned()
		.ok_or_else(|| EngineError::engine("sandbox view must be an object"))?;
	let name = object
		.get("name")
		.or_else(|| object.get("id"))
		.and_then(Value::as_str)
		.unwrap_or_default()
		.to_owned();
	Ok(reconciler::SandboxRecord { name, detail: object })
}

fn params_object(value: &Value) -> Result<Map<String, Value>> {
	value
		.as_object()
		.cloned()
		.ok_or_else(|| EngineError::invalid("record params must be an object"))
}

fn value_object(value: Value) -> MeshResult<Map<String, Value>> {
	match value {
		Value::Object(object) => Ok(object),
		_ => Err(MeshError::invalid("expected JSON object")),
	}
}

fn mesh_to_engine(error: MeshError) -> EngineError {
	let code = match error.code.as_str() {
		"not_found" => ErrorCode::NotFound,
		"not_running" => ErrorCode::NotRunning,
		"busy" | "conflict" => ErrorCode::Busy,
		"unsupported" => ErrorCode::Unsupported,
		"unauthorized" => ErrorCode::Unauthorized,
		"invalid" | "arch_required" => ErrorCode::Invalid,
		_ => ErrorCode::Engine,
	};
	EngineError::new(code, error.message)
}

fn engine_to_mesh(error: EngineError) -> MeshError {
	MeshError::with_code(error.message, error.code.as_str())
}

fn mesh_request_idempotency_key() -> String {
	hex::encode(rand::random::<[u8; 18]>())
}

fn i64_to_u64(value: i64) -> u64 {
	u64::try_from(value).unwrap_or_default()
}

fn u64_to_i64(value: u64) -> i64 {
	i64::try_from(value).unwrap_or(i64::MAX)
}

fn unix_now() -> f64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

fn sanitize_sandbox_params(mut params: Map<String, Value>) -> Map<String, Value> {
	const KEYS: &[&str] = &[
		"image",
		"template",
		"dockerfile",
		"context",
		"name",
		"cpus",
		"memory",
		"disk_mb",
		"timeout",
		"timeout_secs",
		"workdir",
		"env",
		"secrets",
		"volumes",
		"tags",
		"fs_dir",
		"block_network",
		"ports",
		"egress_allow",
		"egress_allow_domains",
		"inbound_cidr_allowlist",
		"readiness_probe",
		"pool_size",
		"remote_page_url",
		"remote_page_token",
		"remote_page_digest",
		"ha",
		"arch",
		"idempotency_key",
		"command",
	];
	params.retain(|key, _| KEYS.contains(&key.as_str()));
	params
}

pub(crate) fn sandbox_create_from_mesh_params(params: Map<String, Value>) -> Result<SandboxCreate> {
	serde_json::from_value(Value::Object(sanitize_sandbox_params(params))).map_err(EngineError::from)
}
