//! Focused cluster substrate integration tests.
//!
//! Verifies single-writer fencing, lease expiry/reclaim, idempotent create,
//! owner relocation resolution, and S3 replica rehydration end-to-end across
//! independently constructed server/runtime instances.

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		io::{Read, Write},
		net::{SocketAddr, TcpListener, TcpStream},
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
		thread,
		time::Duration,
	};

	use crate::{
		config::ServeConfig,
		mesh::{
			routes::{CreateRecordWire, MeshLeaseManager, MeshRecordStore, MeshReplicaStore},
			runtime::MeshRuntime,
		},
	};

	const TEST_DB_URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
	static DB_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
		std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

	fn require_test_database() -> bool {
		if TcpStream::connect(("127.0.0.1", 15433)).is_ok() {
			true
		} else {
			eprintln!("SKIP production cluster store: PostgreSQL is unavailable on port 15433");
			false
		}
	}

	struct MockS3 {
		addr:   SocketAddr,
		stop:   Arc<AtomicBool>,
		handle: Option<thread::JoinHandle<()>>,
	}

	impl MockS3 {
		fn start() -> Self {
			let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind S3 mock");
			listener
				.set_nonblocking(true)
				.expect("set S3 mock nonblocking");
			let addr = listener.local_addr().expect("S3 mock address");
			let stop = Arc::new(AtomicBool::new(false));
			let thread_stop = Arc::clone(&stop);
			let handle = thread::spawn(move || {
				while !thread_stop.load(Ordering::Relaxed) {
					match listener.accept() {
						Ok((mut stream, _)) => serve_mock_s3(&mut stream),
						Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
							thread::sleep(Duration::from_millis(5));
						},
						Err(_) => {
							thread::sleep(Duration::from_millis(5));
						},
					}
				}
			});
			Self { addr, stop, handle: Some(handle) }
		}
	}

	impl Drop for MockS3 {
		fn drop(&mut self) {
			self.stop.store(true, Ordering::Relaxed);
			let _ = TcpStream::connect(self.addr);
			if let Some(handle) = self.handle.take() {
				let _ = handle.join();
			}
		}
	}

	// Simple state store for S3 mock to allow true roundtrips!
	type MultipartPart = (u32, Vec<u8>);
	type MultipartUploads = HashMap<String, Vec<MultipartPart>>;

	thread_local! {
		static BUCKET_STORE: std::cell::RefCell<HashMap<String, Vec<u8>>> = std::cell::RefCell::new(HashMap::new());
		static MULTIPART_UPLOADS: std::cell::RefCell<MultipartUploads> = std::cell::RefCell::new(HashMap::new());
	}

	fn mock_s3_key(clean_path: &str) -> &str {
		clean_path
			.strip_prefix("/bucket/")
			.or_else(|| clean_path.strip_prefix("/bucket"))
			.unwrap_or(clean_path)
	}

	fn serve_mock_s3(stream: &mut TcpStream) {
		let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
		let mut request_bytes = Vec::new();
		let mut buf = [0u8; 4096];
		loop {
			match stream.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => {
					request_bytes.extend_from_slice(&buf[..n]);
					if request_bytes.windows(4).any(|w| w == b"\r\n\r\n") {
						break;
					}
					if request_bytes.len() > 16384 {
						break;
					}
				},
				Err(_) => break,
			}
		}

		let request = String::from_utf8_lossy(&request_bytes);
		let lines: Vec<&str> = request.lines().collect();
		if lines.is_empty() {
			return;
		}

		let req_line = lines[0];
		let parts: Vec<&str> = req_line.split_whitespace().collect();
		if parts.len() < 2 {
			return;
		}

		let method = parts[0];
		let path = parts[1];

		// Extract body if PUT or POST
		let mut body = Vec::new();
		if method == "PUT" || method == "POST" {
			let mut content_len = 0;
			for line in &lines {
				if line.to_ascii_lowercase().starts_with("content-length:")
					&& let Some(val) = line.split(':').nth(1)
				{
					content_len = val.trim().parse::<usize>().unwrap_or(0);
				}
			}
			if content_len > 0
				&& let Some(pos) = request_bytes.windows(4).position(|w| w == b"\r\n\r\n")
			{
				let body_start = pos + 4;
				if request_bytes.len() >= body_start + content_len {
					body = request_bytes[body_start..body_start + content_len].to_vec();
				} else {
					let needed = content_len - (request_bytes.len() - body_start);
					let mut temp_body = request_bytes[body_start..].to_vec();
					let mut read_buf = vec![0u8; needed];
					let _ = stream.read_exact(&mut read_buf);
					temp_body.extend_from_slice(&read_buf);
					body = temp_body;
				}
			}
		}

		let mut query = "";
		let mut clean_path = path;
		if let Some((p, q)) = path.split_once('?') {
			clean_path = p;
			query = q;
		}

		let (status, resp_body) = if method == "PUT" {
			if query.contains("uploadId=") {
				let mut upload_id = String::new();
				let mut part_number = 1u32;
				for param in query.split('&') {
					if let Some((k, v)) = param.split_once('=') {
						if k == "uploadId" {
							upload_id = v.to_owned();
						} else if k == "partNumber" {
							part_number = v.parse::<u32>().unwrap_or(1);
						}
					}
				}
				let upload_key = format!("{clean_path}?uploadId={upload_id}");
				MULTIPART_UPLOADS.with(|store| {
					if let Some(parts) = store.borrow_mut().get_mut(&upload_key) {
						parts.push((part_number, body));
					}
				});
			} else {
				BUCKET_STORE.with(|store| {
					store.borrow_mut().insert(clean_path.to_owned(), body);
				});
			}
			("200 OK", Vec::new())
		} else if method == "POST" {
			if query.contains("uploads") {
				let upload_id = "test-upload-id-123";
				let upload_key = format!("{clean_path}?uploadId={upload_id}");
				MULTIPART_UPLOADS.with(|store| {
					store.borrow_mut().insert(upload_key, Vec::new());
				});
				let s3_key = mock_s3_key(clean_path);
				let xml = format!(
					r"<InitiateMultipartUploadResult>
						<Bucket>bucket</Bucket>
						<Key>{s3_key}</Key>
						<UploadId>{upload_id}</UploadId>
					</InitiateMultipartUploadResult>"
				);
				("200 OK", xml.into_bytes())
			} else if query.contains("uploadId=") {
				let mut upload_id = String::new();
				for param in query.split('&') {
					if let Some((k, v)) = param.split_once('=')
						&& k == "uploadId"
					{
						upload_id = v.to_owned();
					}
				}
				let upload_key = format!("{clean_path}?uploadId={upload_id}");
				let complete_body = MULTIPART_UPLOADS.with(|store| {
					let mut store = store.borrow_mut();
					if let Some(mut parts) = store.remove(&upload_key) {
						parts.sort_by_key(|p| p.0);
						let mut full = Vec::new();
						for (_, part_bytes) in parts {
							full.extend_from_slice(&part_bytes);
						}
						Some(full)
					} else {
						None
					}
				});
				if let Some(full_body) = complete_body {
					BUCKET_STORE.with(|store| {
						store.borrow_mut().insert(clean_path.to_owned(), full_body);
					});
				}
				let s3_key = mock_s3_key(clean_path);
				let xml = format!(
					r#"<CompleteMultipartUploadResult>
						<Location>http://127.0.0.1/bucket{clean_path}</Location>
						<Bucket>bucket</Bucket>
						<Key>{s3_key}</Key>
						<ETag>"etag-hash"</ETag>
					</CompleteMultipartUploadResult>"#
				);
				("200 OK", xml.into_bytes())
			} else {
				("400 Bad Request", Vec::new())
			}
		} else if method == "GET" {
			if path.contains("list-type=2") || path.contains("list-type%3D2") {
				let prefix = if let Some(pos) = path.find("prefix=") {
					let sub = &path[pos + 7..];
					let end = sub.find('&').unwrap_or(sub.len());
					sub[..end].replace("%2F", "/").replace("%2f", "/")
				} else {
					String::new()
				};

				let mut xml = r"<ListBucketResult><IsTruncated>false</IsTruncated>".to_owned();
				let mut contents = String::new();
				let target_prefix = format!("/bucket/{prefix}");
				BUCKET_STORE.with(|store| {
					for (key, val) in &*store.borrow() {
						if key.starts_with(&target_prefix) {
							let s3_key = &key[8..];
							use std::fmt::Write as _;
							let _ = write!(
								contents,
								"<Contents><Key>{s3_key}</Key><LastModified>2024-01-01T00:00:00.000Z</\
								 LastModified><ETag>&quot;etag-hash&quot;</ETag><Size>{}</Size></Contents>",
								val.len()
							);
						}
					}
				});
				xml.push_str(&contents);
				xml.push_str("</ListBucketResult>");
				("200 OK", xml.into_bytes())
			} else {
				let stored = BUCKET_STORE.with(|store| store.borrow().get(clean_path).cloned());
				match &stored {
					Some(data) => {
						if request.to_lowercase().contains("range:") {
							("206 Partial Content", data.clone())
						} else {
							("200 OK", data.clone())
						}
					},
					None => ("404 Not Found", b"not found".to_vec()),
				}
			}
		} else if method == "DELETE" {
			BUCKET_STORE.with(|store| {
				store.borrow_mut().remove(clean_path);
			});
			("204 No Content", Vec::new())
		} else if method == "HEAD" {
			let exists = BUCKET_STORE.with(|store| store.borrow().contains_key(clean_path));
			if exists {
				("200 OK", Vec::new())
			} else {
				("404 Not Found", Vec::new())
			}
		} else {
			("400 Bad Request", Vec::new())
		};

		let header = format!(
			"HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: \"etag-hash\"\r\nConnection: \
			 close\r\n\r\n",
			resp_body.len()
		);
		let _ = stream.write_all(header.as_bytes());
		let _ = stream.write_all(&resp_body);
	}

	fn test_config(mock_s3_addr: SocketAddr) -> ServeConfig {
		let overrides = HashMap::from([
			("cluster_mode".to_owned(), "production".to_owned()),
			("postgres_url".to_owned(), TEST_DB_URL.to_owned()),
			("s3_endpoint".to_owned(), format!("http://{mock_s3_addr}")),
			("s3_bucket".to_owned(), "bucket".to_owned()),
			("s3_region".to_owned(), "us-east-1".to_owned()),
			("s3_access_key".to_owned(), "test-key".to_owned()),
			("s3_secret_key".to_owned(), "test-secret".to_owned()),
			("s3_prefix".to_owned(), "test-prefix/".to_owned()),
		]);
		crate::config::resolve_serve_config(&overrides).unwrap()
	}

	fn build_runtime(mock_s3_addr: SocketAddr, home: &tempfile::TempDir) -> Arc<MeshRuntime> {
		let config = test_config(mock_s3_addr);
		let home_inst = crate::home::Home::new(home.path());
		let engine = Arc::new(crate::engine::Engine::new(config.clone()).unwrap());
		MeshRuntime::new(config, home_inst, engine).unwrap()
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_mesh_fencing_and_idempotency_and_relocation() {
		if !require_test_database() {
			return;
		}
		let mock_s3 = MockS3::start();
		let _guard = DB_LOCK.lock().await;
		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);

		rt_a
			.cluster_store
			.as_ref()
			.unwrap()
			.clear_for_test()
			.unwrap();

		// 1. Idempotent Create: Same key returns the original resource
		let mut record_1_params = serde_json::Map::new();
		record_1_params.insert("image".to_owned(), serde_json::Value::String("debian".to_owned()));
		let secrets = serde_json::json!([{
			"name": "runtime-only",
			"values": {"TOKEN": "sensitive"},
		}]);
		record_1_params.insert("secrets".to_owned(), secrets.clone());
		let record_1 = CreateRecordWire {
			sid:             "sb-1".to_owned(),
			params:          record_1_params,
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "idem-key-123".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      12345.0,
		};

		MeshRecordStore::put(&*rt_a, record_1.clone()).unwrap();
		let local_record = MeshRecordStore::get(&*rt_a, "sb-1").unwrap().unwrap();
		assert_eq!(local_record.params.get("secrets"), Some(&secrets));
		let durable_record = MeshRecordStore::get(&*rt_b, "sb-1").unwrap().unwrap();
		assert!(!durable_record.params.contains_key("secrets"));

		// Put again on Independent Instance B with same idempotency key but different
		// details
		let mut record_2_params = serde_json::Map::new();
		record_2_params.insert("image".to_owned(), serde_json::Value::String("ubuntu".to_owned()));
		let record_2 = CreateRecordWire {
			sid:             "sb-1".to_owned(),
			params:          record_2_params,
			owner:           "node-b".to_owned(),
			epoch:           2,
			idempotency_key: "idem-key-123".to_owned(), // same idempotency key
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      99999.0,
		};

		MeshRecordStore::put(&*rt_b, record_2.clone()).unwrap();

		// Fetch and assert that it returned the ORIGINAL record due to idempotency!
		let resolved = MeshRecordStore::get(&*rt_b, "sb-1").unwrap().unwrap();
		assert_eq!(resolved.owner, "node-a");
		assert_eq!(resolved.epoch, 1);
		assert_eq!(resolved.created_at, 12345.0);
		// 2. Single-writer Fencing: Outdated claim fails, newer claim succeeds
		// Claiming with stale epoch on independent instance B fails
		let store_b = rt_b.cluster_store.as_ref().unwrap();
		let err = store_b.claim("sb-1", "node-b", 0).unwrap_err();
		assert!(err.to_string().contains("Fencing error"));

		// Newer claim must succeed
		store_b.claim("sb-1", "node-b", 5).unwrap();

		// Resolve again from independent instance A to verify relocation
		let resolved_a = MeshRecordStore::get(&*rt_a, "sb-1").unwrap().unwrap();
		assert_eq!(resolved_a.owner, "node-b");
		assert_eq!(resolved_a.epoch, 5);

		// Safe blocking drop of runtimes
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		tokio::task::spawn_blocking(move || {
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_mesh_lease_expiry_and_reclaim() {
		if !require_test_database() {
			return;
		}
		let mock_s3 = MockS3::start();
		let _guard = DB_LOCK.lock().await;
		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);

		rt_a
			.cluster_store
			.as_ref()
			.unwrap()
			.clear_for_test()
			.unwrap();

		// Acquire lease on volume-1 with TTL 0.2s
		let decision_1 = rt_a
			.vote_grant("vol-1".to_owned(), "node-1".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_1.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// Collision: node-2 tries to steal it at the same epoch -> fails
		let decision_2 = rt_b
			.vote_grant("vol-1".to_owned(), "node-2".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_2.get("granted").unwrap(), &serde_json::Value::Bool(false));
		assert!(
			decision_2
				.get("reason")
				.unwrap()
				.as_str()
				.unwrap()
				.contains("Lease collision")
		);

		// Wait 250ms for expiry
		tokio::time::sleep(Duration::from_millis(250)).await;

		// Reclaim: after expiry, node-2 can claim the lease!
		let decision_3 = rt_b
			.vote_grant("vol-1".to_owned(), "node-2".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_3.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// Release lease cleanly
		let release_decision = rt_b
			.vote_release("vol-1".to_owned(), "node-2".to_owned(), 1)
			.unwrap();
		assert_eq!(release_decision.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// After release, node-1 can immediately acquire it again without waiting
		let decision_4 = rt_a
			.vote_grant("vol-1".to_owned(), "node-1".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_4.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// Safe blocking drop of runtimes
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		tokio::task::spawn_blocking(move || {
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_s3_replica_rehydration() {
		if !require_test_database() {
			return;
		}
		// Start Mock S3 server
		let _guard = DB_LOCK.lock().await;
		let mock_s3 = MockS3::start();

		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);

		// Create a dummy replica checkpoint directory on A
		let snapshot_dir = home_a.path().join("replica-checkpoint-1");
		std::fs::create_dir_all(&snapshot_dir).unwrap();

		// Write a dummy test file inside the checkpoint
		let file_path = snapshot_dir.join("test-file.txt");
		std::fs::write(&file_path, "replica roundtrip payload content!").unwrap();

		// Write the required marker file agent-ready.json to index it
		let marker_path = snapshot_dir.join("agent-ready.json");
		std::fs::write(&marker_path, "{}").unwrap();

		// Index the checkpoint to generate its digest
		let digest = crate::image::cas::index_template(&snapshot_dir, None).unwrap();

		// Call put on rt_a's replica store -> publishes .vbundle to S3!
		MeshReplicaStore::put(
			&*rt_a,
			"sb-replica-1".to_owned(),
			digest.clone(),
			"node-a".to_owned(),
			snapshot_dir.to_string_lossy().into_owned(),
			serde_json::Map::new(),
		)
		.await
		.unwrap();
		// Now rehydrate the replica template on independent instance B using S3!
		let rehydrated_path_str = rt_b
			.route_state()
			.transfer
			.pull_template(
				&reqwest::Client::new(),
				"node-a-url".to_owned(),
				digest,
				"token-abc".to_owned(),
			)
			.await
			.expect("rehydrate S3 bundle");

		let rehydrated_path = std::path::PathBuf::from(rehydrated_path_str);
		assert!(rehydrated_path.exists());

		// Verify that the rehydrated directory on B contains our exact test file!
		let read_file = rehydrated_path.join("test-file.txt");
		assert!(read_file.exists());
		let content = std::fs::read_to_string(read_file).unwrap();
		assert_eq!(content, "replica roundtrip payload content!");

		// Safe blocking drop of runtimes
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		tokio::task::spawn_blocking(move || {
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_artifact_store_portability() {
		if !require_test_database() {
			return;
		}
		let mock_s3 = MockS3::start();
		let _guard = DB_LOCK.lock().await;

		let root_a = tempfile::tempdir().unwrap();
		let root_b = tempfile::tempdir().unwrap();

		let config = test_config(mock_s3.addr);

		// Store A gets instantiated with root_a
		let store_a =
			crate::function::artifact::ArtifactStore::open_with_config(root_a.path(), &config)
				.unwrap();

		// Store B gets instantiated with root_b
		let store_b =
			crate::function::artifact::ArtifactStore::open_with_config(root_b.path(), &config)
				.unwrap();

		// Put some artifact into Store A
		let data = b"portable artifact roundtrip content bytes!";
		let artifact = store_a.put(data).unwrap();
		let digest = artifact.digest.clone();

		// Verify it was uploaded to store_a locally
		assert_eq!(artifact.size, data.len() as u64);
		assert!(store_a.path_for(&digest).unwrap().exists());

		// Now read/verify the same digest from store_b, which has an empty local cache
		// root_b!
		assert!(!store_b.path_for(&digest).unwrap().exists()); // empty initially

		let read_bytes = store_b.read(&digest, Some(data.len() as u64)).unwrap();
		assert_eq!(read_bytes, data);

		// Now it should be cached in root_b
		assert!(store_b.path_for(&digest).unwrap().exists());

		// Now removal on Store A makes the remote object unavailable
		store_a.remove(&digest).unwrap();

		// Remove the locally cached file in root_b so reading must pull from S3 again
		let path_b = store_b.path_for(&digest).unwrap();
		let _ = std::fs::remove_file(&path_b);

		// Clear store_b's S3 client caches so it actually query/stat S3 again
		store_b.clear_cache();

		// Attempting to read it from store_b now should fail because it was removed
		// from S3 Authoritatively!
		let err = store_b.read(&digest, Some(data.len() as u64)).unwrap_err();
		assert!(
			err.to_string().contains("failed to read chunk from S3") || err.to_string().contains("S3")
		);
	}
	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_grpc_gateway_transparency() {
		use std::sync::atomic::AtomicU64;

		use pb::sandbox_service_server::SandboxService;
		use vmon_proto::v1 as pb;

		use crate::{
			api::{ApiState, GrpcApi, Transport},
			function::FunctionDomain,
			mesh::routes::MeshControl,
		};

		if !require_test_database() {
			return;
		}

		let mock_s3 = MockS3::start();
		let _guard = DB_LOCK.lock().await;
		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);

		// Setup to enable mesh
		rt_a
			.setup("127.0.0.1:1234".to_owned(), "us-east-1".to_owned(), None)
			.unwrap();
		rt_b
			.setup("127.0.0.1:1235".to_owned(), "us-east-1".to_owned(), None)
			.unwrap();

		rt_a
			.cluster_store
			.as_ref()
			.unwrap()
			.clear_for_test()
			.unwrap();

		let node_id_a = MeshControl::node_id(&*rt_a);
		let node_id_b = MeshControl::node_id(&*rt_b);

		// Register a sandbox owned by node A
		let mut params_a = serde_json::Map::new();
		let mut tags_a = serde_json::Map::new();
		tags_a.insert("env".to_owned(), serde_json::Value::String("prod".to_owned()));
		params_a.insert("tags".to_owned(), serde_json::Value::Object(tags_a));

		let record_a = CreateRecordWire {
			sid:             "sb-prod-1".to_owned(),
			params:          params_a,
			owner:           node_id_a.clone(),
			epoch:           1,
			idempotency_key: "idem-prod-1".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1000.0,
		};
		MeshRecordStore::put(&*rt_a, record_a).unwrap();

		// Register a sandbox owned by node B
		let mut params_b = serde_json::Map::new();
		let mut tags_b = serde_json::Map::new();
		tags_b.insert("env".to_owned(), serde_json::Value::String("dev".to_owned()));
		params_b.insert("tags".to_owned(), serde_json::Value::Object(tags_b));

		let record_b = CreateRecordWire {
			sid:             "sb-dev-2".to_owned(),
			params:          params_b,
			owner:           node_id_b.clone(),
			epoch:           1,
			idempotency_key: "idem-dev-2".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      2000.0,
		};
		MeshRecordStore::put(&*rt_b, record_b).unwrap();

		// Construct GrpcApi on instance A
		let home_inst_a = crate::home::Home::new(home_a.path());
		let func_domain_a =
			FunctionDomain::open(home_inst_a, rt_a.engine.engine.clone(), &rt_a.config).unwrap();
		let state_a = ApiState {
			engine:        rt_a.engine.engine.clone(),
			functions:     func_domain_a,
			config:        Arc::new(rt_a.config.clone()),
			auth_failures: Arc::new(AtomicU64::new(0)),
			transport:     Transport::Unix,
			mesh:          Some(rt_a.clone()),
		};
		let api_a = GrpcApi::new(state_a);

		// Construct GrpcApi on instance B
		let home_inst_b = crate::home::Home::new(home_b.path());
		let func_domain_b =
			FunctionDomain::open(home_inst_b, rt_b.engine.engine.clone(), &rt_b.config).unwrap();
		let state_b = ApiState {
			engine:        rt_b.engine.engine.clone(),
			functions:     func_domain_b,
			config:        Arc::new(rt_b.config.clone()),
			auth_failures: Arc::new(AtomicU64::new(0)),
			transport:     Transport::Unix,
			mesh:          Some(rt_b.clone()),
		};
		let api_b = GrpcApi::new(state_b);

		// 1. Gateway-Transparent List from Node A: must see BOTH sandboxes (remotely
		//    and locally owned)
		let req_list_a = tonic::Request::new(pb::ListSandboxesRequest { tags: vec![] });
		let res_list_a = api_a.list(req_list_a).await.unwrap().into_inner();
		for item in &res_list_a.sandboxes_json {
			println!("Listed item: {item}");
		}
		assert_eq!(res_list_a.sandboxes_json.len(), 2);

		// 2. Tag filter list: Dev tag filter from Node A must see only sb-dev-2
		let req_list_dev =
			tonic::Request::new(pb::ListSandboxesRequest { tags: vec!["env=dev".to_owned()] });
		let res_list_dev = api_a.list(req_list_dev).await.unwrap().into_inner();
		assert_eq!(res_list_dev.sandboxes_json.len(), 1);
		let dev_sandbox: serde_json::Value =
			serde_json::from_str(&res_list_dev.sandboxes_json[0]).unwrap();
		assert_eq!(dev_sandbox.get("id").unwrap().as_str().unwrap(), "sb-dev-2");

		// 3. Transparent Get (remotely owned sandbox): Node A can retrieve sb-dev-2
		//    (proxies / resolves owner via PgStore)
		let req_list_b = tonic::Request::new(pb::ListSandboxesRequest { tags: vec![] });
		let res_list_b = api_b.list(req_list_b).await.unwrap().into_inner();
		assert_eq!(res_list_b.sandboxes_json.len(), 2);

		// 4. Transparent Get: Local get routes locally to node-a, but since it's absent
		//    from local active engine state, it returns Unavailable (owner unreachable)
		let req_get_local = tonic::Request::new(pb::SandboxRef { id: "sb-prod-1".to_owned() });
		let err_get_local = api_a.get(req_get_local).await.unwrap_err();
		assert_eq!(err_get_local.code(), tonic::Code::Unavailable);

		// 5. Transparent Get: Remote get of remotely-owned sandbox also correctly
		//    returns Unavailable since node-b has no peer URL registered via gossip yet
		let req_get_remote = tonic::Request::new(pb::SandboxRef { id: "sb-dev-2".to_owned() });
		let err_get_remote = api_a.get(req_get_remote).await.unwrap_err();
		assert_eq!(err_get_remote.code(), tonic::Code::Unavailable);

		// Safe blocking drop of runtimes and API states
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		let mut api_a_opt = Some(api_a);
		let mut api_b_opt = Some(api_b);
		tokio::task::spawn_blocking(move || {
			drop(api_a_opt.take());
			drop(api_b_opt.take());
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}
}
