//! Lock-free scheduler counters and timing aggregates.
//!
//! These values are process-local observations. Durable call timing remains in
//! [`vmon_proto::v1::CallStats`]; restarting the daemon intentionally resets
//! this registry rather than pretending that metrics are transactional state.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use vmon_proto::v1 as pb;

/// A snapshot of the function scheduler's process-local measurements.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetricsSnapshot {
	/// Inputs admitted to an executor.
	pub inputs_started: u64,
	/// Inputs whose result was committed successfully.
	pub inputs_succeeded: u64,
	/// User-code attempts that failed.
	pub user_failures: u64,
	/// Infrastructure attempts that failed.
	pub infrastructure_failures: u64,
	/// Workers created during this daemon lifetime.
	pub workers_started: u64,
	/// Workers retired cleanly during this daemon lifetime.
	pub workers_retired: u64,
	/// Cumulative milliseconds spent queued by started inputs.
	pub queue_millis: u64,
	/// Cumulative milliseconds spent preparing workers.
	pub startup_millis: u64,
	/// Cumulative milliseconds spent executing user code.
	pub execution_millis: u64,
}

/// Non-secret immutable build provenance suitable for diagnostics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReproducibilityDescription {
	/// Immutable function revision identifier.
	pub revision_id: String,
	/// Canonical function specification digest in lowercase hexadecimal.
	pub specification_digest: String,
	/// Normalized build-input digest in lowercase hexadecimal.
	pub build_inputs_digest: String,
	/// Builder implementation identifier.
	pub builder_id: String,
	/// Exact builder implementation version.
	pub builder_version: String,
	/// Reproducible build timestamp.
	pub source_date_epoch: u64,
	/// Sorted non-secret build environment.
	pub environment: BTreeMap<String, String>,
}

/// Describe reproducible inputs without exposing secret references or values.
pub fn describe_reproducibility(revision: &pb::FunctionRevision) -> ReproducibilityDescription {
	let reproducibility = revision.spec.as_ref().and_then(|spec| spec.reproducibility.as_ref());
	ReproducibilityDescription {
		revision_id: revision.r#ref.as_ref().map(|reference| reference.revision_id.clone()).unwrap_or_default(),
		specification_digest: revision
			.spec_digest
			.as_ref()
			.map(|digest| hex::encode(&digest.value))
			.unwrap_or_default(),
		build_inputs_digest: reproducibility
			.and_then(|value| value.build_inputs_digest.as_ref())
			.map(|digest| hex::encode(&digest.value))
			.unwrap_or_default(),
		builder_id: reproducibility.map(|value| value.builder_id.clone()).unwrap_or_default(),
		builder_version: reproducibility.map(|value| value.builder_version.clone()).unwrap_or_default(),
		source_date_epoch: reproducibility.map_or(0, |value| value.source_date_epoch),
		environment: reproducibility
			.map(|value| value.environment.iter().map(|(key, value)| (key.clone(), value.clone())).collect())
			.unwrap_or_default(),
	}
}

/// Process-local function runtime metrics.
///
/// Counters use relaxed atomics because no counter guards scheduler state and
/// readers only require a self-consistent value for each individual field.
#[derive(Debug, Default)]
pub struct FunctionMetrics {
	inputs_started: AtomicU64,
	inputs_succeeded: AtomicU64,
	user_failures: AtomicU64,
	infrastructure_failures: AtomicU64,
	workers_started: AtomicU64,
	workers_retired: AtomicU64,
	queue_millis: AtomicU64,
	startup_millis: AtomicU64,
	execution_millis: AtomicU64,
}

impl FunctionMetrics {
	/// Record admission and the time the input spent awaiting admission.
	pub fn input_started(&self, queue_millis: u64) {
		self.inputs_started.fetch_add(1, Ordering::Relaxed);
		self.queue_millis.fetch_add(queue_millis, Ordering::Relaxed);
	}

	/// Record a committed successful result and its execution duration.
	pub fn input_succeeded(&self, execution_millis: u64) {
		self.inputs_succeeded.fetch_add(1, Ordering::Relaxed);
		self.execution_millis.fetch_add(execution_millis, Ordering::Relaxed);
	}

	/// Record a user-code attempt failure and its execution duration.
	pub fn user_failure(&self, execution_millis: u64) {
		self.user_failures.fetch_add(1, Ordering::Relaxed);
		self.execution_millis.fetch_add(execution_millis, Ordering::Relaxed);
	}

	/// Record a retryable infrastructure failure.
	pub fn infrastructure_failure(&self) {
		self.infrastructure_failures.fetch_add(1, Ordering::Relaxed);
	}

	/// Record creation of a worker and its startup duration.
	pub fn worker_started(&self, startup_millis: u64) {
		self.workers_started.fetch_add(1, Ordering::Relaxed);
		self.startup_millis.fetch_add(startup_millis, Ordering::Relaxed);
	}

	/// Record clean retirement of a worker.
	pub fn worker_retired(&self) {
		self.workers_retired.fetch_add(1, Ordering::Relaxed);
	}

	/// Return the current counter values.
	pub fn snapshot(&self) -> MetricsSnapshot {
		MetricsSnapshot {
			inputs_started: self.inputs_started.load(Ordering::Relaxed),
			inputs_succeeded: self.inputs_succeeded.load(Ordering::Relaxed),
			user_failures: self.user_failures.load(Ordering::Relaxed),
			infrastructure_failures: self.infrastructure_failures.load(Ordering::Relaxed),
			workers_started: self.workers_started.load(Ordering::Relaxed),
			workers_retired: self.workers_retired.load(Ordering::Relaxed),
			queue_millis: self.queue_millis.load(Ordering::Relaxed),
			startup_millis: self.startup_millis.load(Ordering::Relaxed),
			execution_millis: self.execution_millis.load(Ordering::Relaxed),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn records_timing_and_outcomes() {
		let metrics = FunctionMetrics::default();
		metrics.input_started(7);
		metrics.worker_started(11);
		metrics.input_succeeded(13);
		metrics.input_started(17);
		metrics.user_failure(19);
		metrics.infrastructure_failure();
		metrics.worker_retired();

		assert_eq!(
			metrics.snapshot(),
			MetricsSnapshot {
				inputs_started: 2,
				inputs_succeeded: 1,
				user_failures: 1,
				infrastructure_failures: 1,
				workers_started: 1,
				workers_retired: 1,
				queue_millis: 24,
				startup_millis: 11,
				execution_millis: 32,
			}
		);
	}

	#[test]
	fn reproducibility_description_excludes_secrets() {
		let revision = pb::FunctionRevision {
			r#ref: Some(pb::RevisionRef { revision_id: "revision".into(), ..Default::default() }),
			spec: Some(pb::FunctionSpec {
				reproducibility: Some(pb::ReproducibilitySpec {
					builder_id: "builder".into(),
					builder_version: "1.2.3".into(),
					environment: [("LANG".into(), "C".into())].into(),
					..Default::default()
				}),
				secrets: vec![pb::SecretRef { name: "must-not-appear".into(), ..Default::default() }],
				..Default::default()
			}),
			..Default::default()
		};
		let description = describe_reproducibility(&revision);
		assert_eq!(description.builder_id, "builder");
		assert_eq!(description.environment["LANG"], "C");
		assert!(!format!("{description:?}").contains("must-not-appear"));
	}
}
