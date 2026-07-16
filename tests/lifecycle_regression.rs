//! Black-box lifecycle regressions.  These intentionally boot real guests and,
//! for portable recovery, use the configured `PostgreSQL` and S3 services
//! rather than an in-process substitute.

mod common;
mod lifecycle_support;

use std::{
	thread,
	time::{Duration, Instant},
};

use lifecycle_support::{
	Server, create, exec, history, production_env, require_lifecycle_e2e, require_portable_e2e,
	short_home, view,
};
use vmon_proto::v1 as pb;

fn recovery_point(server: &Server, id: &str, kind: &str) -> pb::RecoveryPoint {
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		if let Some(point) = history(server, id)
			.into_iter()
			.find(|point| point.kind == kind)
		{
			return point;
		}
		assert!(Instant::now() < deadline, "timed out waiting for a {kind} recovery point");
		thread::sleep(Duration::from_millis(100));
	}
}

fn history_env() -> Vec<(&'static str, String)> {
	vec![
		("VMON_HISTORY_DISK_SEC", "0.1".to_owned()),
		("VMON_HISTORY_CHECKPOINT_SEC", "0.2".to_owned()),
	]
}

fn portable_history_env() -> Vec<(&'static str, String)> {
	let mut env = production_env();
	env.extend(history_env());
	env
}

fn rollback(server: &Server, id: &str, point: &str) {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	grpc
		.block_on(sandboxes.rollback(pb::RollbackSandboxRequest {
			id:             id.to_owned(),
			recovery_point: point.to_owned(),
		}))
		.unwrap_or_else(|status| {
			panic!("rollback {point} failed: {}", common::api::status_detail(&status))
		});
}

#[test]
fn daemon_restart_keeps_management_and_recovery_history() {
	if !require_lifecycle_e2e() {
		return;
	}
	let mut server = Server::start(short_home("restart"), history_env());
	let id = create(&server, "lifecycle-restart");
	exec(&server, &id, "echo durable > /root/restart-marker && sync");
	let point = recovery_point(&server, &id, "disk").name;
	server.restart();
	assert!(
		history(&server, &id)
			.iter()
			.any(|entry| entry.name == point && entry.kind == "disk"),
		"recovery history disappeared across daemon restart"
	);
	assert_eq!(exec(&server, &id, "cat /root/restart-marker").trim(), "durable");
}

#[test]
fn disk_rollback_cold_boots_but_checkpoint_resumes_processes() {
	if !require_lifecycle_e2e() {
		return;
	}
	let server = Server::start(short_home("tiers"), history_env());
	let id = create(&server, "lifecycle-tiers");
	exec(
		&server,
		&id,
		"rm -f /root/tier-marker /root/disk-process /root/checkpoint-process \
		 /root/checkpoint-release /root/checkpoint-pid; echo disk > /root/tier-marker; sync",
	);
	let disk = recovery_point(&server, &id, "disk").name;
	exec(
		&server,
		&id,
		"(sleep 3; echo leaked > /root/disk-process) & echo changed > /root/tier-marker; sync",
	);
	rollback(&server, &id, &disk);
	assert_eq!(
		exec(&server, &id, "cat /root/tier-marker").trim(),
		"disk",
		"disk recovery did not restore the captured filesystem"
	);
	thread::sleep(Duration::from_secs(4));
	assert_eq!(
		exec(&server, &id, "if test -e /root/disk-process; then echo present; else echo absent; fi")
			.trim(),
		"absent",
		"disk recovery resumed a process that was not in its disk point"
	);

	let existing_checkpoints: std::collections::HashSet<_> = history(&server, &id)
		.into_iter()
		.filter(|point| point.kind == "checkpoint")
		.map(|point| point.name)
		.collect();
	exec(
		&server,
		&id,
		"nohup sh -c 'while ! test -e /root/checkpoint-release; do sleep 1; done; echo resumed > \
		 /root/checkpoint-process' </dev/null >/dev/null 2>&1 & echo $! > /root/checkpoint-pid; sync",
	);
	exec(&server, &id, "kill -0 $(cat /root/checkpoint-pid)");
	let deadline = Instant::now() + Duration::from_secs(30);
	let full = loop {
		let unseen = history(&server, &id)
			.into_iter()
			.filter(|point| point.kind == "checkpoint" && !existing_checkpoints.contains(&point.name))
			.collect::<Vec<_>>();
		if unseen.len() >= 2 {
			break unseen.last().expect("unseen checkpoint").name.clone();
		}
		assert!(
			Instant::now() < deadline,
			"timed out waiting for two checkpoints after starting the process"
		);
		thread::sleep(Duration::from_millis(100));
	};
	let wait_for_checkpoint_process = || {
		let deadline = Instant::now() + Duration::from_secs(10);
		loop {
			let state = exec(
				&server,
				&id,
				"if test -e /root/checkpoint-process; then cat /root/checkpoint-process; else echo \
				 pending; fi",
			);
			if state.trim() == "resumed" {
				break;
			}
			assert!(Instant::now() < deadline, "captured process did not resume");
			thread::sleep(Duration::from_millis(100));
		}
	};
	exec(&server, &id, "touch /root/checkpoint-release; sync");
	wait_for_checkpoint_process();
	rollback(&server, &id, &full);
	assert_eq!(
		exec(
			&server,
			&id,
			"if test -e /root/checkpoint-process; then echo present; else echo absent; fi"
		)
		.trim(),
		"absent",
		"checkpoint rollback did not restore the captured process-era filesystem"
	);
	exec(&server, &id, "touch /root/checkpoint-release; sync");
	wait_for_checkpoint_process();

	let points = history(&server, &id);
	assert!(
		points
			.iter()
			.any(|entry| entry.name == disk && entry.kind == "disk"),
		"disk point missing or relabelled: {points:?}"
	);
	assert!(
		points
			.iter()
			.any(|entry| entry.name == full && entry.kind == "checkpoint"),
		"full point missing or relabelled: {points:?}"
	);
}

#[test]
fn failed_durable_suspend_keeps_source_running_and_unpublished() {
	if !require_portable_e2e() {
		return;
	}
	let Some(failure_endpoint) = std::env::var_os("VMON_LIFECYCLE_FAILURE_S3_ENDPOINT") else {
		eprintln!(
			"SKIP lifecycle_regression: VMON_LIFECYCLE_FAILURE_S3_ENDPOINT must be an unreachable \
			 real S3 endpoint"
		);
		return;
	};
	let mut env = production_env();
	env.retain(|(name, _)| *name != "VMON_S3_ENDPOINT");
	env.push(("VMON_S3_ENDPOINT", failure_endpoint.to_string_lossy().into_owned()));
	let server = Server::start(short_home("failed-suspend"), env);
	let id = create(&server, "lifecycle-failed-suspend");
	exec(&server, &id, "echo source-survives > /root/source-marker && sync");

	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	assert!(
		grpc
			.block_on(sandboxes.suspend(pb::SandboxRef { id: id.clone() }))
			.is_err(),
		"suspend unexpectedly succeeded with an unreachable production S3 endpoint"
	);
	assert_eq!(
		exec(&server, &id, "cat /root/source-marker").trim(),
		"source-survives",
		"failed durable suspend tore down the source VM"
	);
	assert!(
		history(&server, &id).is_empty(),
		"a partial durable suspend became visible in committed history"
	);
}

#[test]
fn restart_after_persisted_suspend_intent_converges_to_suspended() {
	if !require_portable_e2e() {
		return;
	}
	let mut server = Server::start(short_home("interrupt"), production_env());
	let id = create(&server, "lifecycle-interrupt");
	exec(&server, &id, "dd if=/dev/zero of=/root/suspend-payload bs=1M count=32 && sync");
	let socket = server.socket();
	let suspended_id = id.clone();
	let request = thread::spawn(move || {
		let grpc = common::api::Grpc::connect_uds(&socket).expect("connect suspend client");
		let mut sandboxes = grpc.sandboxes();
		grpc.block_on(sandboxes.suspend(pb::SandboxRef { id: suspended_id }))
	});
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		let current = view(&server, &id);
		if current["desired_state"].as_str() == Some("suspended")
			&& current["observed_state"].as_str() != Some("suspended")
		{
			break;
		}
		assert!(
			Instant::now() < deadline,
			"suspend never exposed a persisted intent before observed completion: {current}"
		);
		thread::sleep(Duration::from_millis(10));
	}
	server.kill_hard();
	let _ = request.join();
	server.restart();

	let current = view(&server, &id);
	assert_eq!(
		current["desired_state"].as_str(),
		Some("suspended"),
		"restart lost the requested suspend state: {current}"
	);
	assert_eq!(
		current["observed_state"].as_str(),
		Some("suspended"),
		"restart did not converge the interrupted suspend: {current}"
	);
	let points = history(&server, &id);
	assert!(
		!points.is_empty(),
		"restart did not converge the interrupted suspend to a committed recovery point"
	);
	let selected = points.last().expect("recovery point").name.clone();
	rollback(&server, &id, &selected);
	assert_eq!(
		exec(&server, &id, "wc -c < /root/suspend-payload").trim(),
		(32 * 1024 * 1024).to_string(),
		"converged recovery point was not restorable"
	);
}

#[test]
fn rejected_rollback_preserves_the_live_source_identity() {
	if !require_lifecycle_e2e() {
		return;
	}
	let server = Server::start(short_home("rollback-source"), Vec::new());
	let id = create(&server, "lifecycle-rollback-source");
	exec(&server, &id, "echo still-live > /root/source-identity && sync");
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	assert!(
		grpc
			.block_on(sandboxes.rollback(pb::RollbackSandboxRequest {
				id:             id.clone(),
				recovery_point: "does-not-exist".to_owned(),
			}))
			.is_err(),
		"rollback unexpectedly accepted an absent recovery point"
	);
	assert_eq!(
		exec(&server, &id, "cat /root/source-identity").trim(),
		"still-live",
		"failed rollback destroyed or replaced the source VM"
	);
}

#[test]
fn selected_portable_history_restores_on_a_second_node() {
	if !require_portable_e2e() {
		return;
	}
	let mut source = Server::start(short_home("portable-source"), portable_history_env());
	let id = create(&source, "lifecycle-portable-history");
	exec(&source, &id, "echo older > /root/portable-marker && sync");
	let older = recovery_point(&source, &id, "disk").name;
	exec(&source, &id, "echo newer > /root/portable-marker && sync");
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		let newest = history(&source, &id)
			.into_iter()
			.rev()
			.find(|point| point.kind == "disk");
		if let Some(point) = newest
			&& point.name != older
		{
			break;
		}
		assert!(Instant::now() < deadline, "timed out waiting for a newer disk recovery point");
		thread::sleep(Duration::from_millis(100));
	}
	let grpc = source.grpc();
	let mut sandboxes = grpc.sandboxes();
	let suspended = grpc
		.block_on(sandboxes.suspend(pb::SandboxRef { id: id.clone() }))
		.unwrap_or_else(|status| {
			panic!("portable suspend failed: {}", common::api::status_detail(&status))
		})
		.into_inner();
	let suspended: serde_json::Value =
		serde_json::from_str(&suspended.json).expect("suspend view JSON");
	assert_eq!(
		suspended["status"].as_str(),
		Some("suspended"),
		"suspend acknowledged before the sandbox was suspended: {suspended}"
	);
	source.kill_hard();

	let destination = Server::start(short_home("portable-destination"), portable_history_env());
	assert!(
		history(&destination, &id)
			.iter()
			.any(|entry| entry.name == older && entry.kind == "disk"),
		"destination did not expose the selected committed source history"
	);
	rollback(&destination, &id, &older);
	assert_eq!(
		exec(&destination, &id, "cat /root/portable-marker").trim(),
		"older",
		"destination did not restore the explicitly selected remote history point"
	);
}

#[test]
fn retry_after_unobserved_migration_result_has_one_committed_owner() {
	if std::env::var("VMON_LIFECYCLE_MIGRATION_E2E").as_deref() != Ok("1") {
		eprintln!("SKIP lifecycle_regression: VMON_LIFECYCLE_MIGRATION_E2E=1 is not set");
		return;
	}
	let source_socket =
		std::env::var("VMON_LIFECYCLE_MIGRATION_SOURCE_SOCK").expect("migration e2e source socket");
	let destination_socket = std::env::var("VMON_LIFECYCLE_MIGRATION_DESTINATION_SOCK")
		.expect("migration e2e destination socket");
	let id = std::env::var("VMON_LIFECYCLE_MIGRATION_SANDBOX").expect("migration e2e sandbox id");
	let target =
		std::env::var("VMON_LIFECYCLE_MIGRATION_TARGET").expect("migration e2e target node");

	// The first caller intentionally discards the response. The retry therefore
	// sees precisely the client-visible state of a response lost after commit.
	let first_socket = source_socket.clone();
	let first_id = id.clone();
	let first_target = target.clone();
	let first = thread::spawn(move || {
		let grpc = common::api::Grpc::connect_uds(std::path::Path::new(&first_socket))
			.expect("connect first migration client");
		let mut sandboxes = grpc.sandboxes();
		let _ = grpc
			.block_on(sandboxes.migrate(pb::MigrateRequest { id: first_id, target: first_target }));
	});
	first.join().expect("issue migration request");

	let retry_grpc = common::api::Grpc::connect_uds(std::path::Path::new(&source_socket))
		.expect("connect retry migration client");
	let mut retry_sandboxes = retry_grpc.sandboxes();
	let retry =
		retry_grpc.block_on(retry_sandboxes.migrate(pb::MigrateRequest { id: id.clone(), target }));
	assert!(retry.is_ok(), "migration retry did not resolve the original commit: {retry:?}");

	let destination_grpc = common::api::Grpc::connect_uds(std::path::Path::new(&destination_socket))
		.expect("connect destination client");
	let mut destination_sandboxes = destination_grpc.sandboxes();
	let destination =
		destination_grpc.block_on(destination_sandboxes.exec_capture(pb::ExecCaptureRequest {
			id:   id.clone(),
			exec: Some(pb::ExecStart {
				cmd: vec!["/bin/sh".into(), "-c".into(), "echo destination-owner".into()],
				timeout: Some(30.0),
				..Default::default()
			}),
		}));
	assert!(destination.is_ok(), "destination never became the migration owner: {destination:?}");

	let source_grpc = common::api::Grpc::connect_uds(std::path::Path::new(&source_socket))
		.expect("connect source status client");
	let mut source_sandboxes = source_grpc.sandboxes();
	let source = source_grpc.block_on(source_sandboxes.exec_capture(pb::ExecCaptureRequest {
		id,
		exec: Some(pb::ExecStart {
			cmd: vec!["/bin/sh".into(), "-c".into(), "echo source-owner".into()],
			timeout: Some(30.0),
			..Default::default()
		}),
	}));
	assert!(source.is_err(), "migration left both source and destination running: {source:?}");
}
