mod common;

use std::{
	fs,
	io::{Read, Write},
	net::TcpListener,
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use serde_json::json;

#[test]
fn mem_target_pager_evicts_and_reports_metrics() {
	if !common::require_hv() || !common::supports_linux_isolation() {
		return;
	}

	let dir = common::test_dir("pager-mem-target");
	let sock = dir.join("control.sock");
	let mut args = common::base_args("soak");
	if let Some(mem) = args
		.iter_mut()
		.skip_while(|arg| arg.as_str() != "--mem")
		.nth(1)
	{
		*mem = "128".into();
	}
	args.push("--mem-target-mib".into());
	args.push("32".into());
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SOAK_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	let deadline = Instant::now() + Duration::from_secs(10);
	let metrics = loop {
		let metrics = control.request("metrics", json!({}));
		let evictions = metrics["pager"]["evictions"].as_u64().unwrap_or(0);
		if evictions > 0 || Instant::now() >= deadline {
			break metrics;
		}
		thread::sleep(Duration::from_millis(200));
	};

	assert!(
		metrics["pager"]["evictions"].as_u64().unwrap_or(0) > 0,
		"pager did not evict before deadline: {metrics}"
	);
	assert!(
		metrics["pager"]["resident_pages"]
			.as_u64()
			.unwrap_or(u64::MAX)
			<= (32 * 1024 * 1024 / 4096) + 8192,
		"pager resident_pages not near target: {metrics}"
	);
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}

/// `--zram-store-max-mib`/`--zram-swap-file`: capping the compressed in-RAM
/// store forces evicted pages to spill into the operator-supplied swap file.
#[test]
fn zram_cap_spills_to_swap_file() {
	if !common::require_hv() || !common::supports_linux_isolation() {
		return;
	}

	let dir = common::test_dir("pager-zram-swap");
	let sock = dir.join("control.sock");
	let swap = dir.join("swap.bin");
	let mut args = common::base_args("soak");
	if let Some(mem) = args
		.iter_mut()
		.skip_while(|arg| arg.as_str() != "--mem")
		.nth(1)
	{
		*mem = "128".into();
	}
	args.push("--mem-target-mib".into());
	args.push("16".into());
	args.push("--zram-store-max-mib".into());
	args.push("4".into());
	args.push("--zram-swap-file".into());
	args.push(swap.display().to_string());
	args.push("--api-sock".into());
	args.push(sock.display().to_string());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SOAK_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	let deadline = Instant::now() + Duration::from_secs(30);
	let metrics = loop {
		let metrics = control.request("metrics", json!({}));
		let swapped = metrics["pager"]["swapped_pages"].as_u64().unwrap_or(0);
		if swapped > 0 || Instant::now() >= deadline {
			break metrics;
		}
		thread::sleep(Duration::from_millis(200));
	};

	assert!(
		metrics["pager"]["swapped_pages"].as_u64().unwrap_or(0) > 0,
		"no pages spilled to swap before deadline: {metrics}"
	);
	let swap_len = fs::metadata(&swap)
		.unwrap_or_else(|e| panic!("stat {}: {e}", swap.display()))
		.len();
	assert!(swap_len > 0, "swap file {} is empty", swap.display());

	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}

/// `--remote-page-url`: a restore with no locally-loaded memory faults every
/// touched page in over HTTP with the bearer token, mirroring the mesh
/// template page server (`GET <base>/<page>`, 4096 bytes at page*4096).
#[test]
fn remote_pager_faults_pages_in_over_http() {
	if !common::require_hv() || !common::supports_linux_isolation() {
		return;
	}

	// Phase 1: take a plain snapshot (same shape as tests/snapshot.rs).
	let dir = common::test_dir("pager-remote");
	let sock = dir.join("control.sock");
	let snap = dir.join("snap");
	{
		let mut args = common::base_args("snapshot");
		args.push("--api-sock".into());
		args.push(sock.display().to_string());
		args.push("--snapshot-root".into());
		args.push(dir.display().to_string());
		let refs = common::as_refs(&args);
		let mut vm = common::spawn_vmm(&refs);
		vm.wait_for("SNAPSHOT_READY", Duration::from_mins(1));
		let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
		assert_eq!(control.command("pause"), "OK");
		assert_eq!(control.command("snapshot snap"), "OK");
		assert_eq!(control.command("quit"), "OK");
		let (status, output) = vm.wait(Duration::from_secs(30));
		assert!(status.success(), "snapshot source exited with {status}; output:\n{output}");
		common::assert_snapshot_written(&snap);
	}

	// Phase 2: serve snapshot memory pages over HTTP with a bearer check.
	let generation = fs::read_to_string(snap.join("current-generation"))
		.unwrap_or_else(|e| panic!("reading current-generation: {e}"));
	let memory_path = snap.join(format!("memory.{}.bin", generation.trim()));
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind page server");
	let addr = listener.local_addr().expect("page server address");
	let served = Arc::new(AtomicUsize::new(0));
	let stop = Arc::new(AtomicBool::new(false));
	let server = {
		let served = Arc::clone(&served);
		let stop = Arc::clone(&stop);
		listener
			.set_nonblocking(true)
			.expect("nonblocking page listener");
		thread::spawn(move || {
			page_server_loop(&listener, &memory_path, &served, &stop);
		})
	};

	// Phase 3: restore with every page marked remote; the guest resumes and
	// runs to completion purely off faulted-in pages.
	let restore_sock = dir.join("restore.sock");
	let args = vec![
		"--restore".to_string(),
		snap.display().to_string(),
		"--remote-page-url".to_string(),
		format!("http://{addr}/pages"),
		"--api-sock".to_string(),
		restore_sock.display().to_string(),
	];
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm_with_env(&refs, &[("VMON_REMOTE_PAGE_TOKEN", "secret")]);
	vm.wait_for("SNAPSHOT_AFTER_RESTORE", Duration::from_mins(2));
	let mut control = common::ControlClient::connect(&restore_sock, Duration::from_secs(10));
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "restored vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);

	stop.store(true, Ordering::SeqCst);
	server.join().expect("page server thread");
	assert!(served.load(Ordering::SeqCst) >= 1, "remote page server never served a page request");
}

/// Accept sequential page requests until `stop`: parse `GET /pages/<n>`,
/// require the bearer token, and answer with the 4096-byte page at n*4096
/// from the snapshot memory image (the format `server/templates.py` serves).
fn page_server_loop(
	listener: &TcpListener,
	memory_path: &PathBuf,
	served: &AtomicUsize,
	stop: &AtomicBool,
) {
	let memory =
		fs::read(memory_path).unwrap_or_else(|e| panic!("reading {}: {e}", memory_path.display()));
	while !stop.load(Ordering::SeqCst) {
		let mut stream = match listener.accept() {
			Ok((stream, _)) => stream,
			Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
				thread::sleep(Duration::from_millis(5));
				continue;
			},
			Err(e) => panic!("page server accept: {e}"),
		};
		stream.set_nonblocking(false).expect("blocking page stream");
		stream
			.set_read_timeout(Some(Duration::from_secs(5)))
			.expect("page stream read timeout");
		let mut request = Vec::new();
		let mut buf = [0u8; 1024];
		while !request.windows(4).any(|w| w == b"\r\n\r\n") {
			match stream.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => request.extend_from_slice(&buf[..n]),
				Err(_) => break,
			}
		}
		let request = String::from_utf8_lossy(&request);
		let page = request
			.lines()
			.next()
			.and_then(|line| line.strip_prefix("GET /pages/"))
			.and_then(|rest| rest.split(' ').next())
			.and_then(|n| n.parse::<usize>().ok());
		let authorized = request.contains("Authorization: Bearer secret");
		let body = page.and_then(|page| memory.get(page * 4096..(page + 1) * 4096));
		let response = match (authorized, body) {
			(true, Some(body)) => {
				served.fetch_add(1, Ordering::SeqCst);
				let mut response =
					b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n".to_vec();
				response.extend_from_slice(body);
				response
			},
			(false, _) => b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".to_vec(),
			(true, None) => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
		};
		let _ = stream.write_all(&response);
	}
}
