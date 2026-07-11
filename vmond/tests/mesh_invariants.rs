use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

fn field<'a>(row: &'a Value, names: &[&str]) -> Option<&'a Value> {
	names.iter().find_map(|name| row.get(*name))
}

fn field_str(row: &Value, names: &[&str]) -> String {
	field(row, names)
		.and_then(Value::as_str)
		.expect("invariant fixture missing string field")
		.to_owned()
}

fn field_f64(row: &Value, names: &[&str]) -> f64 {
	field(row, names)
		.and_then(Value::as_f64)
		.expect("invariant fixture missing numeric field")
}

fn field_i64(row: &Value, names: &[&str]) -> i64 {
	field(row, names)
		.and_then(Value::as_i64)
		.expect("invariant fixture missing integer field")
}

fn assert_no_epoch_overlap(history: &[Value]) {
	let mut intervals: BTreeMap<String, Vec<(f64, f64, String, i64)>> = BTreeMap::new();
	for row in history {
		let sid = field_str(row, &["sid", "sandbox_id"]);
		let epoch = field_i64(row, &["epoch"]);
		let owner = field_str(row, &["owner", "node", "node_id"]);
		let start = field_f64(row, &["start", "started_at", "from_ts"]);
		let end = field(row, &["end", "ended_at", "until_ts"])
			.and_then(Value::as_f64)
			.unwrap_or(f64::INFINITY);
		assert!(start < end, "invalid owner interval for {sid} epoch {epoch}: {start} >= {end}");
		intervals
			.entry(sid)
			.or_default()
			.push((start, end, owner, epoch));
	}

	for (sid, claims) in &mut intervals {
		claims.sort_by(|left, right| left.0.total_cmp(&right.0));
		for index in 0..claims.len() {
			let (left_start, left_end, left_owner, left_epoch) = &claims[index];
			for (right_start, right_end, right_owner, right_epoch) in &claims[index + 1..] {
				if right_start >= left_end {
					break;
				}
				if left_owner == right_owner {
					continue;
				}
				panic!(
					"overlapping owners for {sid}: {left_owner}@epoch{left_epoch}[{left_start}, \
					 {left_end}) and {right_owner}@epoch{right_epoch}[{right_start}, {right_end})"
				);
			}
		}
	}
}

fn assert_checkpoint_age_within_bound(records: &[Value], now: f64, cadence: f64, push_bound: f64) {
	let max_age = cadence + push_bound;
	assert!(max_age >= 0.0, "checkpoint age bound must be non-negative");
	for row in records {
		let sid = field(row, &["sid", "sandbox_id", "id"])
			.and_then(Value::as_str)
			.unwrap_or("<unknown>");
		let ts = field(row, &["checkpointed_at", "checkpoint_at", "pushed_at", "created_at", "ts"])
			.and_then(Value::as_f64)
			.unwrap_or_else(|| panic!("checkpoint record for {sid} has no timestamp"));
		let age = now - ts;
		assert!(
			(0.0..=max_age).contains(&age),
			"checkpoint for {sid} is {age:.3}s old; bound is {max_age:.3}s"
		);
	}
}

fn assert_idempotent_create_same_sid(responses: &[Value]) {
	assert!(!responses.is_empty(), "no create responses supplied");
	let mut sids = BTreeSet::new();
	for payload in responses {
		let sid = payload
			.get("id")
			.or_else(|| payload.get("name"))
			.and_then(Value::as_str)
			.unwrap_or_default();
		assert!(!sid.is_empty(), "create response has no sandbox id: {payload:?}");
		sids.insert(sid.to_owned());
	}
	assert_eq!(sids.len(), 1, "idempotent create returned multiple sids: {sids:?}");
}

fn assert_rerun_at_least_once(history: &[Value]) {
	let mut acked = BTreeSet::new();
	let mut failed = BTreeSet::new();
	let mut rerun = BTreeSet::new();
	for row in history {
		let sid = field_str(row, &["sid", "sandbox_id"]);
		match field_str(row, &["event", "kind", "type"]).as_str() {
			"create_ack" | "acked" => {
				acked.insert(sid);
			},
			"owner_failed_without_checkpoint" | "owner_lost_no_checkpoint" => {
				failed.insert(sid);
			},
			"rerun" | "rerun_started" | "rerun_completed" => {
				rerun.insert(sid);
			},
			_ => {},
		}
	}
	let missing: Vec<_> = acked
		.intersection(&failed)
		.filter(|sid| !rerun.contains(*sid))
		.collect();
	assert!(missing.is_empty(), "acked sandboxes were not re-run after owner loss: {missing:?}");
}

fn assert_volume_lease_non_overlap(history: &[Value]) {
	let mut leases: BTreeMap<String, Vec<(f64, f64, String)>> = BTreeMap::new();
	for row in history {
		let volume = field_str(row, &["volume", "volume_id", "name"]);
		let holder = field_str(row, &["holder", "owner", "node", "node_id"]);
		let start = field_f64(row, &["start", "granted_at", "from_ts"]);
		let end = field(row, &["end", "expires_at", "until_ts"])
			.and_then(Value::as_f64)
			.unwrap_or(f64::INFINITY);
		assert!(start < end, "invalid lease interval for {volume}: {start} >= {end}");
		leases.entry(volume).or_default().push((start, end, holder));
	}

	for (volume, intervals) in &mut leases {
		intervals.sort_by(|left, right| left.0.total_cmp(&right.0));
		for pair in intervals.windows(2) {
			let (left_start, left_end, left_holder) = &pair[0];
			let (right_start, right_end, right_holder) = &pair[1];
			if left_holder == right_holder {
				continue;
			}
			assert!(
				left_end <= right_start || right_end <= left_start,
				"overlapping writable leases for {volume}: {left_holder}@[{left_start}, {left_end}) \
				 and {right_holder}@[{right_start}, {right_end})"
			);
		}
	}
}

#[test]
fn no_epoch_overlap_rejects_same_or_new_epoch_split_brain() {
	assert_no_epoch_overlap(&[
		serde_json::json!({"sid": "vm", "epoch": 1, "owner": "A", "start": 0.0, "end": 10.0}),
		serde_json::json!({"sid": "vm", "epoch": 2, "owner": "B", "start": 10.0, "end": null}),
	]);

	let same_epoch = std::panic::catch_unwind(|| {
		assert_no_epoch_overlap(&[
			serde_json::json!({"sid": "vm", "epoch": 3, "owner": "A", "start": 20.0, "end": 30.0}),
			serde_json::json!({"sid": "vm", "epoch": 3, "owner": "B", "start": 29.0, "end": 40.0}),
		]);
	});
	assert!(same_epoch.is_err(), "same-epoch overlap must be split-brain");

	let cross_epoch = std::panic::catch_unwind(|| {
		assert_no_epoch_overlap(&[
			serde_json::json!({"sid": "vm", "epoch": 1, "owner": "A", "start": 0.0, "end": null}),
			serde_json::json!({"sid": "vm", "epoch": 2, "owner": "B", "start": 5.0, "end": null}),
		]);
	});
	assert!(cross_epoch.is_err(), "cross-epoch overlap must also be split-brain");
}

#[test]
fn checkpoint_age_allows_cadence_plus_push_bound_only() {
	assert_checkpoint_age_within_bound(
		&[
			serde_json::json!({"sid": "fresh", "checkpointed_at": 94.0}),
			serde_json::json!({"sid": "edge", "checkpointed_at": 90.0}),
		],
		100.0,
		8.0,
		2.0,
	);

	let stale = std::panic::catch_unwind(|| {
		assert_checkpoint_age_within_bound(
			&[serde_json::json!({"sid": "stale", "checkpointed_at": 89.99})],
			100.0,
			8.0,
			2.0,
		);
	});
	assert!(stale.is_err(), "checkpoint older than cadence plus push bound must fail");
}

#[test]
fn idempotent_create_retry_must_return_one_sid() {
	assert_idempotent_create_same_sid(&[
		serde_json::json!({"id": "same"}),
		serde_json::json!({"name": "same"}),
	]);

	let duplicate = std::panic::catch_unwind(|| {
		assert_idempotent_create_same_sid(&[
			serde_json::json!({"id": "left"}),
			serde_json::json!({"id": "right"}),
		]);
	});
	assert!(duplicate.is_err(), "same idempotency key producing two sandboxes must fail");
}

#[test]
fn acked_no_checkpoint_owner_loss_requires_rerun() {
	assert_rerun_at_least_once(&[
		serde_json::json!({"sid": "acked", "event": "create_ack"}),
		serde_json::json!({"sid": "acked", "event": "owner_failed_without_checkpoint"}),
		serde_json::json!({"sid": "acked", "event": "rerun_started"}),
	]);

	let missing = std::panic::catch_unwind(|| {
		assert_rerun_at_least_once(&[
			serde_json::json!({"sid": "lost", "event": "create_ack"}),
			serde_json::json!({"sid": "lost", "event": "owner_failed_without_checkpoint"}),
		]);
	});
	assert!(missing.is_err(), "acked lost owner without checkpoint must rerun");
}

#[test]
fn writable_volume_leases_do_not_overlap_between_holders() {
	assert_volume_lease_non_overlap(&[
		serde_json::json!({"volume": "shared", "holder": "A", "granted_at": 100.0, "expires_at": 110.0}),
		serde_json::json!({"volume": "shared", "holder": "B", "granted_at": 110.0, "expires_at": 120.0}),
	]);

	let overlap = std::panic::catch_unwind(|| {
		assert_volume_lease_non_overlap(&[
			serde_json::json!({"volume": "shared", "holder": "A", "granted_at": 100.0, "expires_at": 110.0}),
			serde_json::json!({"volume": "shared", "holder": "B", "granted_at": 109.999, "expires_at": 120.0}),
		]);
	});
	assert!(overlap.is_err(), "distinct writable holders must not overlap");
}
