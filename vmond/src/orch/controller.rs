//! Controller janitor: leader lease, dead-worker lost-marking, orphan sweep.
//!
//! Every scheduler runs these loops, but only the current leader (holder of
//! the `vmon:o:leader` lease) acts. The controller's one job is truth repair:
//! when a worker vanishes, the sandboxes it was running are gone with it, so
//! their routes are rewritten to [`super::STATE_LOST`] instead of lying
//! `running` until the route TTL expires.

use std::{
	sync::Arc,
	time::{Duration, Instant},
};

use super::{
	ROUTE_TTL_MS, SandboxRoute, is_live_state, keys, now_ms, redis::Redis, table::WorkerTable,
};
use crate::{EngineError, Result};

/// How often [`Controller::tick`] additionally runs the full orphan sweep.
/// The sweep SCANs every route key, so it is paced well below the tick rate.
const ORPHAN_SWEEP_INTERVAL: Duration = Duration::from_mins(1);

/// `SET NX PX` leader lease on [`keys::LEADER`].
///
/// One holder wins per TTL window; the winner keeps renewing (`PEXPIRE`) on
/// every tick, and anyone else backs off. Redis trouble means nobody leads
/// (fail closed): a janitor that cannot reach Redis cannot repair routes
/// anyway, and a second concurrent leader would only do duplicate idempotent
/// writes.
pub struct LeaderLease {
	redis:  Redis,
	holder: String,
	ttl_ms: u64,
}

impl LeaderLease {
	pub fn new(redis: Redis, holder: String, ttl: Duration) -> Self {
		let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
		Self { redis, holder, ttl_ms }
	}

	/// Try to acquire or renew the lease; `true` means "I am the leader for
	/// the next TTL window". Never errors: any Redis failure yields `false`.
	pub async fn tick(&self) -> bool {
		match self
			.redis
			.set_nx_px(keys::LEADER, self.holder.as_bytes(), self.ttl_ms)
			.await
		{
			Ok(true) => true,
			Ok(false) => match self.redis.get(keys::LEADER).await {
				Ok(Some(value)) if value == self.holder.as_bytes() => {
					self.redis.pexpire(keys::LEADER, self.ttl_ms).await.is_ok()
				},
				_ => false,
			},
			Err(_) => false,
		}
	}

	pub fn holder(&self) -> &str {
		&self.holder
	}
}

/// Dead-worker janitor. Run only while holding the [`LeaderLease`].
pub struct Controller {
	redis:      Redis,
	table:      Arc<WorkerTable>,
	last_sweep: Option<Instant>,
}

impl Controller {
	pub const fn new(redis: Redis, table: Arc<WorkerTable>) -> Self {
		Self { redis, table, last_sweep: None }
	}

	/// One janitor pass: reap workers the table has given up on and mark
	/// their sandboxes lost; at most every [`ORPHAN_SWEEP_INTERVAL`], also
	/// sweep all routes for orphans no reap ever caught (e.g. a worker that
	/// died while no leader was watching). Returns how many sandboxes were
	/// newly marked lost.
	pub async fn tick(&mut self) -> Result<usize> {
		let mut lost = 0;
		for heartbeat in self.table.reap() {
			lost += self.mark_worker_lost(&heartbeat.wid).await?;
		}
		let now = Instant::now();
		let due = self
			.last_sweep
			.is_none_or(|at| now.saturating_duration_since(at) >= ORPHAN_SWEEP_INTERVAL);
		if due {
			self.last_sweep = Some(now);
			lost += self.sweep_orphans().await?;
		}
		Ok(lost)
	}

	/// Mark every live route owned by a dead worker as lost. Heartbeats no
	/// longer carry per-sandbox digests, so ownership comes from a scan of
	/// the route keyspace: any route with `wid == dead worker` still in a
	/// live state transitions to [`super::STATE_LOST`]; routes already in a
	/// non-live state (lost/terminal) are left untouched. Returns the number
	/// of sandboxes actually transitioned.
	pub async fn mark_worker_lost(&self, wid: &str) -> Result<usize> {
		let mut lost = 0;
		for (key, mut route) in scan_routes(&self.redis).await? {
			if route.wid != wid || !is_live_state(&route.state) {
				continue;
			}
			route.state = super::STATE_LOST.into();
			route.ts_ms = now_ms();
			write_route(&self.redis, &key, &route).await?;
			lost += 1;
		}
		Ok(lost)
	}

	/// Full-scan safety net: any route in a live state whose worker the table
	/// has completely forgotten (not live, not even stale-but-known) is marked
	/// lost. Stale-but-known workers are the reaper's business — orphaning
	/// them here would race a worker that is merely slow to heartbeat.
	/// Per-key Redis trouble is logged and skipped, never fatal to the sweep.
	pub async fn sweep_orphans(&self) -> Result<usize> {
		let known: std::collections::HashSet<String> =
			self.table.live().into_iter().map(|hb| hb.wid).collect();
		let mut lost = 0;
		for (key, mut route) in scan_routes(&self.redis).await? {
			if !is_live_state(&route.state)
				|| known.contains(&route.wid)
				|| self.table.url_of(&route.wid).is_some()
			{
				continue;
			}
			route.state = super::STATE_LOST.into();
			route.ts_ms = now_ms();
			if let Err(error) = write_route(&self.redis, &key, &route).await {
				tracing::warn!(%key, %error, "orphan sweep: route rewrite failed");
				continue;
			}
			lost += 1;
		}
		Ok(lost)
	}
}

/// Scan every sandbox route key (`vmon:o:sb:*`) and decode the snapshot.
/// Shared by dead-worker marking and the orphan sweep. Per-key Redis trouble
/// and undecodable payloads are logged/skipped, never fatal to the scan.
async fn scan_routes(redis: &Redis) -> Result<Vec<(String, SandboxRoute)>> {
	let mut routes = Vec::new();
	for key in redis.scan_prefix(keys::SANDBOX_PREFIX).await? {
		let bytes = match redis.get(&key).await {
			Ok(Some(bytes)) => bytes,
			Ok(None) => continue, // expired between SCAN and GET
			Err(error) => {
				tracing::warn!(%key, %error, "route scan: read failed");
				continue;
			},
		};
		if let Ok(route) = serde_json::from_slice::<SandboxRoute>(&bytes) {
			routes.push((key, route));
		}
	}
	Ok(routes)
}

/// `SET key route-json PX ROUTE_TTL_MS`.
async fn write_route(redis: &Redis, key: &str, route: &SandboxRoute) -> Result<()> {
	let payload = serde_json::to_vec(route)
		.map_err(|error| EngineError::engine(format!("route encode: {error}")))?;
	redis.set_px(key, &payload, ROUTE_TTL_MS).await
}

#[cfg(test)]
mod tests {
	use super::{
		super::{Resources, WIRE_VERSION, WorkerHeartbeat, miniredis::MiniRedis, table::TableConfig},
		*,
	};

	fn heartbeat(wid: &str, running: u64) -> WorkerHeartbeat {
		WorkerHeartbeat {
			ver: WIRE_VERSION,
			wid: wid.to_owned(),
			url: format!("http://{wid}:8000"),
			arch: "aarch64".to_owned(),
			backend: "hvf".to_owned(),
			seq: 1,
			ts_ms: 1,
			caps: Resources { vcpus: 8, mem_mib: 8_192 },
			used: Resources::default(),
			running,
			accepting: true,
			draining: false,
			net_rx_bytes: None,
			net_tx_bytes: None,
		}
	}

	fn route(sid: &str, wid: &str, state: &str, result: Option<i64>) -> SandboxRoute {
		SandboxRoute {
			sid: sid.to_owned(),
			wid: wid.to_owned(),
			url: format!("http://{wid}:8000"),
			state: state.to_owned(),
			ts_ms: 42,
			result,
		}
	}

	async fn put_route(redis: &Redis, route: &SandboxRoute) {
		let key = keys::sandbox(&route.sid);
		let payload = serde_json::to_vec(route).expect("encode");
		redis
			.set_px(&key, &payload, ROUTE_TTL_MS)
			.await
			.expect("set");
	}

	async fn get_route(redis: &Redis, sid: &str) -> SandboxRoute {
		let bytes = redis
			.get(&keys::sandbox(sid))
			.await
			.expect("get")
			.expect("route present");
		serde_json::from_slice(&bytes).expect("decode")
	}

	fn table() -> Arc<WorkerTable> {
		Arc::new(WorkerTable::new(TableConfig::from_dead_after(Duration::from_secs(5))))
	}

	#[tokio::test]
	async fn mark_worker_lost_flips_live_routes_via_wid_scan() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let controller = Controller::new(redis.clone(), table());

		put_route(&redis, &route("a", "w1", "paused", Some(3))).await;
		put_route(&redis, &route("b", "w1", "terminated", Some(0))).await;
		put_route(&redis, &route("c", "w1", "running", None)).await;
		put_route(&redis, &route("other", "w2", "running", None)).await;

		// No digests exist anymore: ownership comes purely from the routes'
		// wid fields.
		let lost = controller.mark_worker_lost("w1").await.expect("mark");
		assert_eq!(lost, 2, "only w1's live routes transition");

		let a = get_route(&redis, "a").await;
		assert_eq!(a.state, super::super::STATE_LOST);
		assert_eq!(a.result, Some(3), "existing result must be preserved");
		assert!(a.ts_ms > 42, "timestamp must be refreshed");

		let c = get_route(&redis, "c").await;
		assert_eq!(c.state, super::super::STATE_LOST);
		assert_eq!(c.wid, "w1");

		let b = get_route(&redis, "b").await;
		assert_eq!(b.state, "terminated", "terminal routes are left untouched");
		let other = get_route(&redis, "other").await;
		assert_eq!(other.state, "running", "another worker's routes are spared");
	}

	#[tokio::test]
	async fn sweep_orphans_spares_known_workers() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let table = table();
		table.upsert(heartbeat("known", 1));
		let controller = Controller::new(redis.clone(), Arc::clone(&table));

		put_route(&redis, &route("kept", "known", "running", None)).await;
		put_route(&redis, &route("orphan", "gone", "running", Some(9))).await;
		put_route(&redis, &route("done", "gone", "terminated", None)).await;

		let lost = controller.sweep_orphans().await.expect("sweep");
		assert_eq!(lost, 1, "only the live orphan transitions");
		assert_eq!(get_route(&redis, "kept").await.state, "running");
		assert_eq!(get_route(&redis, "done").await.state, "terminated");
		let orphan = get_route(&redis, "orphan").await;
		assert_eq!(orphan.state, super::super::STATE_LOST);
		assert_eq!(orphan.result, Some(9), "result survives the lost rewrite");
	}

	#[tokio::test]
	async fn tick_reaps_and_marks() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let table = Arc::new(WorkerTable::new(TableConfig {
			dead_after: Duration::from_millis(10),
			reap_after: Duration::from_millis(10),
		}));
		table.upsert(heartbeat("w1", 1));
		put_route(&redis, &route("sb", "w1", "running", None)).await;
		let mut controller = Controller::new(redis.clone(), Arc::clone(&table));

		tokio::time::sleep(Duration::from_millis(30)).await;
		let lost = controller.tick().await.expect("tick");
		assert_eq!(lost, 1);
		assert!(table.is_empty(), "reaped worker must be forgotten");
		assert_eq!(get_route(&redis, "sb").await.state, super::super::STATE_LOST);
	}

	#[tokio::test]
	async fn leader_lease_excludes_and_expires() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let ttl = Duration::from_millis(100);
		let alpha = LeaderLease::new(redis.clone(), "alpha".to_owned(), ttl);
		let beta = LeaderLease::new(redis.clone(), "beta".to_owned(), ttl);
		assert_eq!(alpha.holder(), "alpha");

		assert!(alpha.tick().await, "first tick acquires");
		assert!(alpha.tick().await, "holder renews its own lease");
		assert!(!beta.tick().await, "second holder is locked out while held");

		tokio::time::sleep(Duration::from_millis(250)).await;
		assert!(beta.tick().await, "lapsed lease is up for grabs");
		assert!(!alpha.tick().await, "old leader must observe the new holder");
	}
}
