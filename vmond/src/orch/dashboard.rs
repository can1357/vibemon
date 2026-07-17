//! Lightweight scheduler dashboard backed by worker heartbeats in Redis.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, extract::State, response::Html, routing::get};
use serde::{Deserialize, Serialize};

use super::{
	Resources, keys,
	redis::{Redis, Resp},
};

const HISTORY: &str = "vmon:o:dashboard:history";
/// Cumulative successful creates, survives scheduler restarts.
const CREATED: &str = "vmon:o:dashboard:created";
const MAX_HISTORY: u64 = 3_600;

#[derive(Clone)]
struct DashboardState {
	redis: Redis,
}

#[derive(Clone, Debug, Deserialize)]
struct Heartbeat {
	#[serde(default)]
	wid:          String,
	#[serde(default)]
	caps:         Resources,
	#[serde(default)]
	used:         Resources,
	#[serde(default)]
	running:      u64,
	#[serde(default)]
	net_rx_bytes: Option<u64>,
	#[serde(default)]
	net_tx_bytes: Option<u64>,
}

/// Per-worker cumulative byte counters from the previous tick; turns worker
/// heartbeat totals into fleet-wide bytes-per-second between samples.
#[derive(Default)]
pub struct NetRates {
	last:  std::collections::HashMap<String, (u64, u64)>,
	at_ms: u64,
}

impl NetRates {
	/// Fold this tick's counters into rates against the previous tick.
	/// Workers that restarted (counters went backwards) or newly appeared
	/// contribute nothing this round. Returns `(ingress_bps, egress_bps,
	/// telemetry_seen)`.
	fn advance(&mut self, now_ms: u64, observed: Vec<(String, u64, u64)>) -> (u64, u64, bool) {
		let telemetry = !observed.is_empty();
		let elapsed_ms = now_ms.saturating_sub(self.at_ms);
		let mut rx_delta = 0_u64;
		let mut tx_delta = 0_u64;
		let mut next = std::collections::HashMap::with_capacity(observed.len());
		for (wid, rx, tx) in observed {
			if let Some((last_rx, last_tx)) = self.last.get(&wid) {
				rx_delta = rx_delta.saturating_add(rx.saturating_sub(*last_rx));
				tx_delta = tx_delta.saturating_add(tx.saturating_sub(*last_tx));
			}
			next.insert(wid, (rx, tx));
		}
		self.last = next;
		self.at_ms = now_ms;
		if elapsed_ms == 0 {
			return (0, 0, telemetry);
		}
		(
			rx_delta.saturating_mul(1000) / elapsed_ms,
			tx_delta.saturating_mul(1000) / elapsed_ms,
			telemetry,
		)
	}
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct Sample {
	timestamp:           u64,
	worker_vms:          u64,
	running:             u64,
	created:             u64,
	created_delta:       u64,
	used_vcpus:          u64,
	reserved_vcpus:      u64,
	used_memory_mib:     u64,
	reserved_memory_mib: u64,
	egress_bps:          u64,
	ingress_bps:         u64,
	/// Whether any worker reported host byte counters this tick.
	network_telemetry:   bool,
}

#[derive(Clone, Debug, Default, Serialize)]
struct Dashboard {
	live: bool,
	updated_at: u64,
	concurrent: u64,
	backing_vms: u64,
	total_created: u64,
	average_created_per_second: f64,
	network_telemetry: bool,
	points: Vec<Sample>,
	#[serde(skip_serializing_if = "Option::is_none")]
	error: Option<String>,
}

pub fn router(redis: Redis) -> Router {
	let state = DashboardState { redis };
	Router::new()
		.route("/", get(index))
		.route("/api/dashboard", get(data))
		.with_state(state)
}

/// Record one fleet sample from worker heartbeats.
///
/// `created_delta` is the number of creates this scheduler forwarded since
/// the previous tick; it lands on the shared cumulative Redis counter so
/// totals survive restarts. `net` carries per-worker counter memory for
/// byte-rate derivation.
pub async fn record(redis: &Redis, created_delta: u64, net: &mut NetRates) {
	let Ok(keys) = redis.scan_prefix(keys::WORKER_PREFIX).await else {
		return;
	};
	let mut sample = Sample { timestamp: now_ms(), ..Sample::default() };
	let mut counters = Vec::new();
	for key in keys {
		let Ok(Some(raw)) = redis.get(&key).await else {
			continue;
		};
		let Ok(worker) = serde_json::from_slice::<Heartbeat>(&raw) else {
			continue;
		};
		sample.worker_vms = sample.worker_vms.saturating_add(1);
		sample.running = sample.running.saturating_add(worker.running);
		sample.used_vcpus = sample.used_vcpus.saturating_add(worker.used.vcpus);
		sample.reserved_vcpus = sample.reserved_vcpus.saturating_add(worker.caps.vcpus);
		sample.used_memory_mib = sample.used_memory_mib.saturating_add(worker.used.mem_mib);
		sample.reserved_memory_mib = sample
			.reserved_memory_mib
			.saturating_add(worker.caps.mem_mib);
		if let (Some(rx), Some(tx)) = (worker.net_rx_bytes, worker.net_tx_bytes) {
			counters.push((worker.wid, rx, tx));
		}
	}
	let (ingress_bps, egress_bps, telemetry) = net.advance(sample.timestamp, counters);
	sample.ingress_bps = ingress_bps;
	sample.egress_bps = egress_bps;
	sample.network_telemetry = telemetry;
	sample.created_delta = created_delta;
	sample.created = match redis.incr_by(CREATED, created_delta).await {
		Ok(total) => u64::try_from(total).unwrap_or(0),
		Err(_) => 0,
	};
	if let Ok(payload) = serde_json::to_vec(&sample) {
		let _ = redis.xadd(HISTORY, MAX_HISTORY, &payload).await;
	}
}

async fn data(State(state): State<DashboardState>) -> Json<Dashboard> {
	let points = match read_history(&state.redis).await {
		Ok(points) if !points.is_empty() => points,
		_ => current_sample(&state.redis).await.into_iter().collect(),
	};
	let latest = points.last().cloned().unwrap_or_default();
	let live = latest.timestamp.saturating_add(15_000) >= now_ms();
	let (first, last) = (points.first(), points.last());
	let average_created_per_second = match (first, last) {
		(Some(first), Some(last)) if last.timestamp > first.timestamp => {
			let created = last.created.saturating_sub(first.created) as f64;
			created * 1000.0 / (last.timestamp - first.timestamp) as f64
		},
		_ => 0.0,
	};
	Json(Dashboard {
		live,
		updated_at: latest.timestamp,
		concurrent: latest.running,
		backing_vms: latest.worker_vms,
		total_created: latest.created,
		average_created_per_second,
		network_telemetry: latest.network_telemetry,
		points,
		error: None,
	})
}

async fn current_sample(redis: &Redis) -> Option<Sample> {
	let keys = redis.scan_prefix(keys::WORKER_PREFIX).await.ok()?;
	let mut sample = Sample { timestamp: now_ms(), ..Sample::default() };
	for key in keys {
		let Ok(Some(raw)) = redis.get(&key).await else {
			continue;
		};
		let Ok(worker) = serde_json::from_slice::<Heartbeat>(&raw) else {
			continue;
		};
		sample.worker_vms = sample.worker_vms.saturating_add(1);
		sample.running = sample.running.saturating_add(worker.running);
		sample.used_vcpus = sample.used_vcpus.saturating_add(worker.used.vcpus);
		sample.reserved_vcpus = sample.reserved_vcpus.saturating_add(worker.caps.vcpus);
		sample.used_memory_mib = sample.used_memory_mib.saturating_add(worker.used.mem_mib);
		sample.reserved_memory_mib = sample
			.reserved_memory_mib
			.saturating_add(worker.caps.mem_mib);
	}
	Some(sample)
}

async fn read_history(redis: &Redis) -> Result<Vec<Sample>, String> {
	let reply = redis
		.call(&[b"XREVRANGE", HISTORY.as_bytes(), b"+", b"-", b"COUNT", b"300"])
		.await
		.map_err(|error| error.to_string())?;
	let Resp::Array(entries) = reply else {
		return Ok(Vec::new());
	};
	let mut points = entries
		.into_iter()
		.filter_map(|entry| {
			let Resp::Array(parts) = entry else {
				return None;
			};
			let Resp::Array(fields) = parts.get(1)?.clone() else {
				return None;
			};
			let payload = fields.chunks(2).find_map(|pair| match pair {
				[Resp::Bulk(key), Resp::Bulk(value)] if key == b"d" => Some(value.clone()),
				_ => None,
			})?;
			serde_json::from_slice::<Sample>(&payload).ok()
		})
		.collect::<Vec<_>>();
	points.sort_by_key(|point| point.timestamp);
	Ok(points)
}

async fn index() -> Html<&'static str> {
	Html(INDEX)
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or(Duration::ZERO)
		.as_millis() as u64
}

/// The chart page: hand-rolled SVG time series over `/api/dashboard`.
const INDEX: &str = include_str!("dashboard.html");
