//! Fair, bounded scheduling primitives and the durable scheduler task.
//!
//! Queue ownership remains in [`super::store::Store`]. This module only keeps
//! ephemeral worker-pool admission state; a daemon restart reconstructs work by
//! asking the store to recover expired leases.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, Notify};

use super::metrics::FunctionMetrics;

/// Immutable worker-pool limits for one function revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolPolicy {
	/// Workers retained even without queued work.
	pub min_workers: usize,
	/// Hard worker count limit for this revision.
	pub max_workers: usize,
	/// Extra idle workers retained for bursts.
	pub buffer_workers: usize,
	/// Inputs executable concurrently by one worker.
	pub capacity: usize,
	/// Queued plus executing inputs admitted per worker.
	pub max_outstanding: usize,
	/// Worker idle retirement threshold.
	pub idle_timeout: Duration,
	/// Calls after which a worker is retired; zero disables call-count retirement.
	pub max_calls: u64,
	/// Largest input group sent to one executor request.
	pub max_batch_size: usize,
	/// Maximum delay used to collect a partial batch.
	pub batch_wait: Duration,
}

impl Default for PoolPolicy {
	fn default() -> Self {
		Self {
			min_workers: 0,
			max_workers: 1,
			buffer_workers: 0,
			capacity: 1,
			max_outstanding: 1,
			idle_timeout: Duration::from_secs(60),
			max_calls: 0,
			max_batch_size: 1,
			batch_wait: Duration::ZERO,
		}
	}
}

impl PoolPolicy {
	/// Validate and normalize zero-valued protobuf defaults.
	pub fn normalized(mut self) -> Self {
		self.max_workers = self.max_workers.max(1);
		self.min_workers = self.min_workers.min(self.max_workers);
		self.capacity = self.capacity.max(1);
		self.max_outstanding = self.max_outstanding.max(self.capacity);
		self.max_batch_size = self.max_batch_size.max(1).min(self.max_outstanding);
		self
	}
}

/// Diagnostic placement preferences used when a mesh-backed executor starts a worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlacementHints {
	/// Prefer hosts other than these current worker hosts.
	pub avoid_hosts: Vec<String>,
	/// Prefer zones other than these current worker zones.
	pub avoid_zones: Vec<String>,
	/// Stable affinity key for actors or instance-lifecycle functions.
	pub affinity_key: Option<String>,
}

/// One store-owned input advertised to the ephemeral admission queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedInput {
	/// Immutable function revision identifier and pool key.
	pub revision_id: String,
	/// Durable call identifier.
	pub call_id: String,
	/// Durable input index.
	pub input_index: u64,
	/// Earliest retry admission time in Unix milliseconds.
	pub available_at_ms: u64,
	/// Durable actor identifier, when this is an actor method call.
	pub actor_id: Option<String>,
}

#[derive(Debug)]
struct RevisionQueue {
	policy: PoolPolicy,
	items: VecDeque<QueuedInput>,
}

/// In-memory, bounded round-robin queue. Durable work remains authoritative in the store.
#[derive(Debug, Default)]
pub struct FairQueue {
	revisions: HashMap<String, RevisionQueue>,
	ready: VecDeque<String>,
	ready_set: HashSet<String>,
	queued: usize,
	global_limit: usize,
}

impl FairQueue {
	/// Create a fair queue with a process-wide outstanding-input cap.
	pub fn new(global_limit: usize) -> Self {
		Self { global_limit: global_limit.max(1), ..Self::default() }
	}

	/// Register or replace the immutable policy for a revision pool.
	pub fn set_policy(&mut self, revision_id: String, policy: PoolPolicy) {
		let policy = policy.normalized();
		self.revisions
			.entry(revision_id)
			.and_modify(|queue| queue.policy = policy)
			.or_insert_with(|| RevisionQueue { policy, items: VecDeque::new() });
	}

	/// Advertise a durable input. Returns false when backpressure must be applied.
	pub fn push(&mut self, input: QueuedInput) -> bool {
		if self.queued >= self.global_limit {
			return false;
		}
		let Some(queue) = self.revisions.get_mut(&input.revision_id) else {
			return false;
		};
		let revision_limit = queue.policy.max_workers.saturating_mul(queue.policy.max_outstanding);
		if queue.items.len() >= revision_limit {
			return false;
		}
		let revision_id = input.revision_id.clone();
		queue.items.push_back(input);
		self.queued += 1;
		if self.ready_set.insert(revision_id.clone()) {
			self.ready.push_back(revision_id);
		}
		true
	}

	/// Pop one fair batch whose retry time is eligible.
	pub fn pop(&mut self, now_ms: u64) -> Option<Vec<QueuedInput>> {
		let turns = self.ready.len();
		for _ in 0..turns {
			let revision_id = self.ready.pop_front()?;
			self.ready_set.remove(&revision_id);
			let queue = self.revisions.get_mut(&revision_id)?;
			let eligible = queue.items.front().is_some_and(|item| item.available_at_ms <= now_ms);
			if !eligible {
				self.ready.push_back(revision_id.clone());
				self.ready_set.insert(revision_id);
				continue;
			}
			let count = queue.policy.max_batch_size.min(queue.items.len());
			let mut batch = Vec::with_capacity(count);
			for _ in 0..count {
				if queue.items.front().is_none_or(|item| item.available_at_ms > now_ms) {
					break;
				}
				batch.push(queue.items.pop_front().expect("front checked"));
				self.queued -= 1;
			}
			if !queue.items.is_empty() {
				self.ready.push_back(revision_id.clone());
				self.ready_set.insert(revision_id);
			}
			return Some(batch);
		}
		None
	}

	/// Number of ephemeral advertisements currently admitted.
	pub fn len(&self) -> usize {
		self.queued
	}

	/// Whether no inputs are currently advertised.
	pub fn is_empty(&self) -> bool {
		self.queued == 0
	}
}

/// Compute a bounded exponential delay for a one-based retry attempt.
pub fn retry_backoff(policy_initial_ms: u64, policy_max_ms: u64, multiplier: f64, attempt: u32) -> Duration {
	let exponent = attempt.saturating_sub(1) as i32;
	let factor = if multiplier.is_finite() && multiplier >= 1.0 {
		multiplier.powi(exponent)
	} else {
		1.0
	};
	let delay = (policy_initial_ms as f64 * factor).min(policy_max_ms.max(policy_initial_ms) as f64);
	Duration::from_millis(delay as u64)
}

/// Wake-up and shutdown handles shared with the scheduler background task.
#[derive(Debug)]
pub struct SchedulerControl {
	/// Notifies the scheduler after a transaction makes new work eligible.
	pub notify: Notify,
	/// Process-local scheduler measurements.
	pub metrics: Arc<FunctionMetrics>,
}

impl SchedulerControl {
	/// Create empty scheduler control state.
	pub fn new(metrics: Arc<FunctionMetrics>) -> Self {
		Self { notify: Notify::new(), metrics }
	}
}

/// Run periodic lease recovery and schedule maintenance until shutdown.
///
/// Concrete store/executor dispatch is deliberately driven by
/// [`super::FunctionDomain`], which calls its transactional scheduling tick.
pub async fn background_loop(
	control: Arc<SchedulerControl>,
	mut shutdown: broadcast::Receiver<()>,
	mut tick: impl FnMut() + Send + 'static,
) {
	let mut maintenance = tokio::time::interval(Duration::from_millis(250));
	maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	loop {
		tokio::select! {
			_ = shutdown.recv() => break,
			_ = control.notify.notified() => tick(),
			_ = maintenance.tick() => tick(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn item(revision: &str, index: u64) -> QueuedInput {
		QueuedInput {
			revision_id: revision.into(),
			call_id: format!("call-{revision}-{index}"),
			input_index: index,
			available_at_ms: 0,
			actor_id: None,
		}
	}

	#[test]
	fn bounded_and_round_robin() {
		let mut queue = FairQueue::new(4);
		queue.set_policy("a".into(), PoolPolicy { max_workers: 2, ..PoolPolicy::default() });
		queue.set_policy("b".into(), PoolPolicy { max_workers: 2, ..PoolPolicy::default() });
		assert!(queue.push(item("a", 0)));
		assert!(queue.push(item("a", 1)));
		assert!(queue.push(item("b", 0)));
		assert!(queue.push(item("b", 1)));
		assert!(!queue.push(item("b", 2)));
		assert_eq!(queue.pop(0).unwrap()[0].revision_id, "a");
		assert_eq!(queue.pop(0).unwrap()[0].revision_id, "b");
		assert_eq!(queue.pop(0).unwrap()[0].revision_id, "a");
		assert_eq!(queue.pop(0).unwrap()[0].revision_id, "b");
	}

	#[test]
	fn batches_only_eligible_inputs_and_preserves_indexes() {
		let mut queue = FairQueue::new(8);
		queue.set_policy(
			"r".into(),
			PoolPolicy { max_batch_size: 3, max_outstanding: 4, ..PoolPolicy::default() },
		);
		for index in 0..3 {
			let mut input = item("r", index);
			input.available_at_ms = if index == 2 { 20 } else { 10 };
			assert!(queue.push(input));
		}
		let first = queue.pop(10).unwrap();
		assert_eq!(first.iter().map(|input| input.input_index).collect::<Vec<_>>(), vec![0, 1]);
		assert!(queue.pop(10).is_none());
		assert_eq!(queue.pop(20).unwrap()[0].input_index, 2);
	}

	#[test]
	fn backoff_is_separate_attempt_based_and_bounded() {
		assert_eq!(retry_backoff(100, 1_000, 2.0, 1), Duration::from_millis(100));
		assert_eq!(retry_backoff(100, 1_000, 2.0, 3), Duration::from_millis(400));
		assert_eq!(retry_backoff(100, 1_000, 2.0, 8), Duration::from_millis(1_000));
	}
}
