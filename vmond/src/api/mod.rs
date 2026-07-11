//! v1 API surface of vmond.
//!
//! gRPC services (vmon.v1, proto/vmon/v1/api.proto) over native h2c and the
//! `/grpc` WebSocket bridge, plus the kept HTTP surface — healthz, metrics,
//! ports proxy, and the static web UI. Port of python/vmon/server/.

mod bridge;
mod error;
mod grpc;
mod routes;
mod server;
mod state;
mod validation;
mod ws;

use std::{collections::HashMap, hash::BuildHasher};

pub use error::{ApiError, ErrorBody};

/// Serve the v1 API over `$VMON_HOME/vmond.sock` and optional TCP.
pub fn serve<S>(overrides: HashMap<String, String, S>) -> crate::Result<()>
where
	S: BuildHasher,
{
	server::serve(overrides)
}

#[cfg(test)]
pub(crate) use routes::router as test_router;
#[cfg(test)]
pub(crate) use state::{ApiState, Transport};

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		fs,
		sync::{Arc, Mutex},
	};

	use axum::http::StatusCode;
	use serde_json::{Value, json};
	use sha2::{Digest as _, Sha256};
	use tokio::{io::AsyncWriteExt as _, net::TcpStream};
	use tokio_stream::StreamExt as _;
	use tonic::{
		Code, Request,
		metadata::MetadataValue,
		transport::{Channel, Endpoint},
	};
	use vmon_proto::{prost::Message as _, v1 as pb};

	use super::*;
	use crate::{
		EngineError, ErrorCode, Result,
		config::ServeConfig,
		engine::{EngineApi, ExecCapture, ExecRequest, ExecStream, ShellSession},
		function::FunctionDomain,
		home::Home,
		models::{ForkBody, NetworkBody, PoolPutBody, RestoreBody, SandboxCreate},
	};

	#[derive(Default)]
	struct CapturedInputs {
		creates:    Vec<SandboxCreate>,
		list_tags:  Vec<Option<HashMap<String, String>>>,
		gets:       Vec<String>,
		pool_sets:  Vec<(String, PoolPutBody)>,
		migrations: Vec<(String, String)>,
	}

	struct ScriptedEngine {
		create_response: Mutex<Result<Value>>,
		list_response:   Mutex<Result<Vec<Value>>>,
		get_response:    Mutex<Result<Value>>,
		info_response:   Mutex<Result<Value>>,
		events:          flume::Receiver<Value>,
		captured:        Mutex<CapturedInputs>,
	}

	impl ScriptedEngine {
		fn new() -> Self {
			let (_tx, rx) = flume::unbounded();
			Self {
				create_response: Mutex::new(Ok(json!({"id":"created"}))),
				list_response:   Mutex::new(Ok(Vec::new())),
				get_response:    Mutex::new(Ok(json!({"id":"sandbox"}))),
				info_response:   Mutex::new(Ok(json!({"version":"test"}))),
				events:          rx,
				captured:        Mutex::new(CapturedInputs::default()),
			}
		}

		fn set_create_response(&self, response: Value) {
			*self.create_response.lock().expect("create response") = Ok(response);
		}

		fn set_list_response(&self, response: Vec<Value>) {
			*self.list_response.lock().expect("list response") = Ok(response);
		}

		fn set_get_error(&self, code: ErrorCode, message: &str) {
			*self.get_response.lock().expect("get response") = Err(EngineError::new(code, message));
		}

		fn captures(&self) -> CapturedInputs {
			self.captured.lock().expect("captured inputs").clone()
		}

		fn unexpected<T>(method: &str) -> Result<T> {
			Err(EngineError::engine(format!("unexpected EngineApi::{method} call")))
		}
	}

	impl Clone for CapturedInputs {
		fn clone(&self) -> Self {
			Self {
				creates:    self.creates.clone(),
				list_tags:  self.list_tags.clone(),
				gets:       self.gets.clone(),
				pool_sets:  self.pool_sets.clone(),
				migrations: self.migrations.clone(),
			}
		}
	}

	impl EngineApi for ScriptedEngine {
		fn create(&self, params: SandboxCreate) -> Result<Value> {
			self
				.captured
				.lock()
				.expect("captured inputs")
				.creates
				.push(params);
			self
				.create_response
				.lock()
				.expect("create response")
				.clone()
		}

		fn list(&self, tags: Option<HashMap<String, String>>) -> Result<Vec<Value>> {
			self
				.captured
				.lock()
				.expect("captured inputs")
				.list_tags
				.push(tags);
			self.list_response.lock().expect("list response").clone()
		}

		fn get(&self, id: &str) -> Result<Value> {
			self
				.captured
				.lock()
				.expect("captured inputs")
				.gets
				.push(id.to_owned());
			self.get_response.lock().expect("get response").clone()
		}

		fn stop(&self, _id: &str) -> Result<Value> {
			Self::unexpected("stop")
		}

		fn remove(&self, _id: &str) -> Result<Value> {
			Self::unexpected("remove")
		}

		fn terminate(&self, _id: &str, _reason: &str) -> Result<Value> {
			Self::unexpected("terminate")
		}

		fn pause(&self, _id: &str) -> Result<Value> {
			Self::unexpected("pause")
		}

		fn resume(&self, _id: &str) -> Result<Value> {
			Self::unexpected("resume")
		}

		fn extend(&self, _id: &str, _secs: u64) -> Result<Value> {
			Self::unexpected("extend")
		}

		fn metrics(&self, _id: &str) -> Result<Value> {
			Self::unexpected("metrics")
		}

		fn logs(&self, _id: &str) -> Result<Vec<u8>> {
			Self::unexpected("logs")
		}

		fn logs_follow(&self, _id: &str) -> Result<flume::Receiver<Vec<u8>>> {
			Self::unexpected("logs_follow")
		}

		fn exec_capture(&self, _id: &str, _req: ExecRequest) -> Result<ExecCapture> {
			Self::unexpected("exec_capture")
		}

		fn exec_stream(&self, _id: &str, _req: ExecRequest) -> Result<ExecStream> {
			Self::unexpected("exec_stream")
		}

		fn shell_start(&self, _params: Value) -> Result<ShellSession> {
			Self::unexpected("shell_start")
		}

		fn shell_cleanup(&self, _name: &str) {}

		fn file_read(&self, _id: &str, _path: &str) -> Result<Vec<u8>> {
			Self::unexpected("file_read")
		}

		fn file_write(&self, _id: &str, _path: &str, _data: &[u8]) -> Result<()> {
			Self::unexpected("file_write")
		}

		fn file_delete(&self, _id: &str, _path: &str, _recursive: bool) -> Result<()> {
			Self::unexpected("file_delete")
		}

		fn file_list(&self, _id: &str, _path: &str) -> Result<Value> {
			Self::unexpected("file_list")
		}

		fn file_stat(&self, _id: &str, _path: &str) -> Result<Value> {
			Self::unexpected("file_stat")
		}

		fn network_get(&self, _id: &str) -> Result<Value> {
			Self::unexpected("network_get")
		}

		fn network_set(&self, _id: &str, _policy: NetworkBody) -> Result<Value> {
			Self::unexpected("network_set")
		}

		fn tunnels(&self, _id: &str) -> Result<Value> {
			Self::unexpected("tunnels")
		}

		fn tunnel_target(&self, _id: &str, _port: u16) -> Result<(String, u16)> {
			Self::unexpected("tunnel_target")
		}

		fn snapshot(&self, _id: &str, _name: Option<String>, _stop: bool) -> Result<Value> {
			Self::unexpected("snapshot")
		}

		fn snapshot_fs(&self, _id: &str, _name: Option<String>) -> Result<Value> {
			Self::unexpected("snapshot_fs")
		}

		fn snapshots(&self) -> Result<Vec<String>> {
			Self::unexpected("snapshots")
		}

		fn restore(&self, _snapshot: &str, _body: RestoreBody) -> Result<Value> {
			Self::unexpected("restore")
		}

		fn fork(&self, _snapshot: &str, _body: ForkBody) -> Result<Value> {
			Self::unexpected("fork")
		}

		fn volume_list(&self) -> Result<Vec<String>> {
			Self::unexpected("volume_list")
		}

		fn volume_create(&self, _name: &str) -> Result<()> {
			Self::unexpected("volume_create")
		}

		fn volume_delete(&self, _name: &str) -> Result<()> {
			Self::unexpected("volume_delete")
		}

		fn pool_list(&self) -> Result<Value> {
			Self::unexpected("pool_list")
		}

		fn pool_set(&self, reference: &str, body: PoolPutBody) -> Result<Value> {
			self
				.captured
				.lock()
				.expect("captured inputs")
				.pool_sets
				.push((reference.to_owned(), body));
			Self::unexpected("pool_set")
		}

		fn pool_delete(&self, _reference: &str) -> Result<()> {
			Self::unexpected("pool_delete")
		}

		fn info(&self) -> Result<Value> {
			self.info_response.lock().expect("info response").clone()
		}

		fn subscribe_events(&self) -> flume::Receiver<Value> {
			self.events.clone()
		}

		fn prometheus_metrics(&self) -> String {
			String::new()
		}

		fn migrate(&self, id: &str, target: &str) -> Result<Value> {
			self
				.captured
				.lock()
				.expect("captured inputs")
				.migrations
				.push((id.to_owned(), target.to_owned()));
			Self::unexpected("migrate")
		}
	}

	struct ApiHarness {
		base_url:          String,
		client:            reqwest::Client,
		template_revision: String,
		handle:            tokio::task::JoinHandle<()>,
		_temp:             tempfile::TempDir,
	}

	impl ApiHarness {
		async fn start(engine: Arc<ScriptedEngine>) -> Self {
			let mut config = ServeConfig::default();
			config.token = Some("admin-token".to_owned());
			config.client_token = Some("client-token".to_owned());
			let temp = tempfile::tempdir().expect("function domain tempdir");
			let home = Home::new(temp.path());
			let template = home.templates_dir().join("api-test");
			fs::create_dir_all(&template).expect("create test template");
			fs::write(template.join("rootfs.img"), b"test rootfs").expect("write test rootfs");
			fs::write(template.join("agent-ready.json"), b"{}").expect("write test marker");
			let template_revision =
				crate::image::cas::template_digest(&template).expect("digest test template");
			fs::create_dir_all(home.cas_dir()).expect("create test CAS");
			let pointer = crate::image::cas::CasPointer {
				template_dir: template.to_string_lossy().into_owned(),
				tpl_name:     "api-test".to_owned(),
				created_unix: 0,
			};
			fs::write(
				home.cas_dir().join(format!("{template_revision}.json")),
				serde_json::to_vec(&pointer).expect("encode test CAS pointer"),
			)
			.expect("write test CAS pointer");
			let engine_api: Arc<dyn EngineApi> = engine.clone();
			let functions = FunctionDomain::open(home, engine_api).expect("open function domain");
			let state = ApiState::new(engine, functions.clone(), config, Transport::Tcp);
			let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
				.await
				.expect("bind test api listener");
			let addr = listener.local_addr().expect("test api listener address");
			let router = test_router(state);
			let handle = tokio::spawn(async move {
				axum::serve(listener, router).await.expect("serve test api");
			});
			Self {
				base_url: format!("http://{addr}"),
				client: reqwest::Client::new(),
				template_revision,
				handle,
				_temp: temp,
			}
		}

		fn url(&self, path: &str) -> String {
			format!("{}{}", self.base_url, path)
		}

		fn channel(&self) -> Channel {
			Endpoint::from_shared(self.base_url.clone())
				.expect("test grpc endpoint")
				.connect_lazy()
		}
	}

	impl Drop for ApiHarness {
		fn drop(&mut self) {
			self.handle.abort();
		}
	}

	fn authed<T>(message: T, token: &str) -> Request<T> {
		let mut request = Request::new(message);
		let value = MetadataValue::try_from(format!("Bearer {token}")).expect("valid auth metadata");
		request.metadata_mut().insert("authorization", value);
		request
	}

	fn vmon_code(status: &tonic::Status) -> Option<String> {
		status
			.metadata()
			.get("vmon-code")
			.and_then(|value| value.to_str().ok())
			.map(str::to_owned)
	}

	fn test_function_spec(name: &str, template_revision: &str) -> pb::FunctionSpec {
		pb::FunctionSpec {
			function: Some(pb::FunctionRef {
				namespace: "tests".to_owned(),
				name:      name.to_owned(),
			}),
			package: Some(pb::PackageSpec::default()),
			image: Some(pb::ImageSpec {
				source: Some(pb::image_spec::Source::Template(pb::TemplateImageSource {
					name:     "api-test".to_owned(),
					revision: template_revision.to_owned(),
				})),
				..Default::default()
			}),
			resources: Some(pb::ResourceSpec {
				architecture: if std::env::consts::ARCH == "aarch64" {
					pb::CpuArchitecture::Arm64 as i32
				} else {
					pb::CpuArchitecture::Amd64 as i32
				},
				..Default::default()
			}),
			retry: Some(pb::RetryPolicy { max_attempts: 1, ..Default::default() }),
			timeouts: Some(pb::TimeoutSpec::default()),
			workers: Some(pb::WorkerSpec::default()),
			concurrency: Some(pb::ConcurrencySpec::default()),
			batching: Some(pb::BatchingSpec::default()),
			serializer: Some(pb::SerializerSpec {
				input_serializer:     pb::ValueSerializer::Json as i32,
				result_serializer:    pb::ValueSerializer::Json as i32,
				compression:          pb::ValueCompression::None as i32,
				allow_trusted_python: false,
			}),
			..Default::default()
		}
	}

	fn test_json_value(value: Value) -> pb::ValueEnvelope {
		let bytes = serde_json::to_vec(&value).expect("test json");
		pb::ValueEnvelope {
			schema_version: 1,
			serializer: pb::ValueSerializer::Json as i32,
			compression: pb::ValueCompression::None as i32,
			checksum: Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     Sha256::digest(&bytes).to_vec(),
			}),
			uncompressed_size_bytes: bytes.len() as u64,
			storage: Some(pb::value_envelope::Storage::InlineData(bytes)),
			..Default::default()
		}
	}

	#[tokio::test]
	async fn api_bearer_admin_succeeds_and_missing_token_is_unauthenticated() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;
		let mut client = pb::system_service_client::SystemServiceClient::new(api.channel());

		let response = client
			.info(authed(pb::InfoRequest {}, "admin-token"))
			.await
			.expect("admin info response");
		let body: Value = serde_json::from_str(&response.into_inner().json).expect("admin info json");
		assert_eq!(body, json!({"version":"test"}));

		let status = client
			.info(Request::new(pb::InfoRequest {}))
			.await
			.expect_err("missing token rejected");
		assert_eq!(status.code(), Code::Unauthenticated);

		// The kept HTTP surface stays behind the same bearer guard with the JSON
		// error envelope.
		let response = api
			.client
			.get(api.url("/metrics"))
			.send()
			.await
			.expect("missing token metrics response");
		assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
		assert_eq!(
			response.headers().get(reqwest::header::WWW_AUTHENTICATE),
			Some(&reqwest::header::HeaderValue::from_static("Bearer"))
		);
		let body: Value = response.json().await.expect("missing token json");
		assert_eq!(body, json!({"code":"unauthorized","message":"unauthorized"}));
	}

	#[tokio::test]
	async fn api_client_token_is_forbidden_from_admin_pool_and_migrate_rpcs() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine.clone()).await;

		let mut pools = pb::pool_service_client::PoolServiceClient::new(api.channel());
		let status = pools
			.set(authed(
				pb::PoolSetRequest {
					reference: "x".to_owned(),
					body_json: json!({"size": 1}).to_string(),
				},
				"client-token",
			))
			.await
			.expect_err("client pool set rejected");
		assert_eq!(status.code(), Code::PermissionDenied);

		let mut sandboxes = pb::sandbox_service_client::SandboxServiceClient::new(api.channel());
		let status = sandboxes
			.migrate(authed(
				pb::MigrateRequest { id: "sb".to_owned(), target: "node-b".to_owned() },
				"client-token",
			))
			.await
			.expect_err("client migrate rejected");
		assert_eq!(status.code(), Code::PermissionDenied);

		let captures = engine.captures();
		assert!(captures.pool_sets.is_empty());
		assert!(captures.migrations.is_empty());
	}

	#[tokio::test]
	async fn api_create_validation_rejects_python_compatible_bad_ports_cidrs_and_ha() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine.clone()).await;
		let mut client = pb::sandbox_service_client::SandboxServiceClient::new(api.channel());
		let cases = [
			(
				json!({"name": "bad-port", "ports": [65536]}),
				"ports must be TCP port numbers from 1 to 65535",
			),
			(
				json!({"name": "bad-cidr", "egress_allow": ["not-a-cidr"]}),
				"egress_allow entries must be valid CIDR networks",
			),
			(
				json!({"name": "bad-ha", "ha": "sync"}),
				"ha must be one of: async, async+rerun, off, rerun",
			),
		];

		for (payload, message) in cases {
			let status = client
				.create(authed(
					pb::CreateSandboxRequest { spec_json: payload.to_string() },
					"admin-token",
				))
				.await
				.expect_err("invalid create rejected");
			assert_eq!(status.code(), Code::InvalidArgument);
			assert_eq!(status.message(), message);
			assert_eq!(vmon_code(&status).as_deref(), Some("invalid"));
		}

		assert!(engine.captures().creates.is_empty());
	}

	#[tokio::test]
	async fn api_engine_errors_from_get_map_to_grpc_codes_and_vmon_code_metadata() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine.clone()).await;
		let mut client = pb::sandbox_service_client::SandboxServiceClient::new(api.channel());
		let cases = [
			(ErrorCode::NotFound, Code::NotFound),
			(ErrorCode::NotRunning, Code::FailedPrecondition),
			(ErrorCode::Busy, Code::Aborted),
			(ErrorCode::Invalid, Code::InvalidArgument),
			(ErrorCode::Unsupported, Code::Unimplemented),
			(ErrorCode::Unauthorized, Code::Unauthenticated),
			(ErrorCode::Engine, Code::Unavailable),
		];

		for (code, expected) in cases {
			let message = format!("mapped {}", code.as_str());
			engine.set_get_error(code, &message);
			let status = client
				.get(authed(pb::SandboxRef { id: "sb".to_owned() }, "admin-token"))
				.await
				.expect_err("scripted engine error surfaces");
			assert_eq!(status.code(), expected, "grpc code for {code:?}");
			assert_eq!(status.message(), message);
			assert_eq!(vmon_code(&status).as_deref(), Some(code.as_str()));
		}
	}

	#[tokio::test]
	async fn api_create_returns_engine_view_passthrough() {
		let engine = Arc::new(ScriptedEngine::new());
		let view = json!({"id":"sb-1","state":"running","details":{"from":"fake"}});
		engine.set_create_response(view.clone());
		let api = ApiHarness::start(engine.clone()).await;
		let mut client = pb::sandbox_service_client::SandboxServiceClient::new(api.channel());

		let response = client
			.create(authed(
				pb::CreateSandboxRequest {
					spec_json: json!({"name": "sb-1", "ports": [8080]}).to_string(),
				},
				"admin-token",
			))
			.await
			.expect("create response");
		let body: Value = serde_json::from_str(&response.into_inner().json).expect("create json");
		assert_eq!(body, view);

		let captures = engine.captures();
		assert_eq!(captures.creates.len(), 1);
		assert_eq!(captures.creates[0].name.as_deref(), Some("sb-1"));
		assert_eq!(captures.creates[0].ports.as_deref(), Some(&[8080][..]));
	}

	#[tokio::test]
	async fn api_list_tag_filters_pass_hash_map_to_engine() {
		let engine = Arc::new(ScriptedEngine::new());
		engine.set_list_response(vec![json!({"id":"sb-api"})]);
		let api = ApiHarness::start(engine.clone()).await;
		let mut client = pb::sandbox_service_client::SandboxServiceClient::new(api.channel());

		let response = client
			.list(authed(
				pb::ListSandboxesRequest { tags: vec!["role=api".to_owned()] },
				"admin-token",
			))
			.await
			.expect("list response");
		let sandboxes = response.into_inner().sandboxes_json;
		assert_eq!(sandboxes.len(), 1);
		let row: Value = serde_json::from_str(&sandboxes[0]).expect("list row json");
		assert_eq!(row, json!({"id":"sb-api"}));

		let captures = engine.captures();
		assert_eq!(captures.list_tags.len(), 1);
		assert_eq!(
			captures.list_tags[0]
				.as_ref()
				.and_then(|tags| tags.get("role")),
			Some(&"api".to_owned())
		);
	}

	async fn bridge_handshake(api: &ApiHarness, query: &str) -> std::io::Result<TcpStream> {
		let addr = api
			.base_url
			.strip_prefix("http://")
			.expect("http base url")
			.to_owned();
		let (host, port) = addr.rsplit_once(':').expect("host:port");
		let port: u16 = port.parse().expect("port number");
		let mut stream = TcpStream::connect((host, port)).await?;
		ws::websocket_client_handshake(&mut stream, host, port, "grpc", query).await?;
		Ok(stream)
	}

	async fn send_bridge_frame(stream: &mut TcpStream, frame: pb::bridge_frame::Frame) {
		let frame = pb::BridgeFrame { frame: Some(frame) };
		stream
			.write_all(&ws::encode_ws_frame(2, &frame.encode_to_vec()))
			.await
			.expect("bridge frame written");
	}

	async fn recv_bridge_frame(stream: &mut TcpStream) -> pb::bridge_frame::Frame {
		loop {
			let (opcode, payload) = ws::read_ws_frame(stream).await.expect("bridge frame read");
			match opcode {
				0x2 => {
					return pb::BridgeFrame::decode(payload.as_slice())
						.expect("bridge frame decodes")
						.frame
						.expect("bridge frame set");
				},
				0x9 | 0xa => {},
				other => panic!("unexpected websocket opcode {other}"),
			}
		}
	}

	#[tokio::test]
	async fn api_bridge_drives_unary_info_rpc_end_to_end() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;
		let mut stream = bridge_handshake(&api, "token=admin-token")
			.await
			.expect("bridge upgrade");

		send_bridge_frame(
			&mut stream,
			pb::bridge_frame::Frame::Call(pb::BridgeCall {
				method:   "/vmon.v1.SystemService/Info".to_owned(),
				metadata: HashMap::new(),
			}),
		)
		.await;
		// InfoRequest{} encodes to zero bytes.
		send_bridge_frame(&mut stream, pb::bridge_frame::Frame::Message(Vec::new())).await;
		send_bridge_frame(&mut stream, pb::bridge_frame::Frame::HalfClose(pb::BridgeHalfClose {}))
			.await;

		match recv_bridge_frame(&mut stream).await {
			pb::bridge_frame::Frame::Message(payload) => {
				let view = pb::JsonView::decode(payload.as_slice()).expect("info view decodes");
				let body: Value = serde_json::from_str(&view.json).expect("info json");
				assert_eq!(body, json!({"version":"test"}));
			},
			other => panic!("unexpected bridge frame: {other:?}"),
		}
		match recv_bridge_frame(&mut stream).await {
			pb::bridge_frame::Frame::End(end) => {
				assert_eq!(end.status, Code::Ok as i32);
				assert!(end.message.is_empty());
			},
			other => panic!("unexpected bridge frame: {other:?}"),
		}
	}

	#[tokio::test]
	async fn api_bridge_upgrade_requires_a_token() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;
		let err = bridge_handshake(&api, "")
			.await
			.expect_err("unauthenticated bridge upgrade rejected");
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
	}

	#[tokio::test]
	async fn api_bridge_enforces_admin_rpcs_for_client_tokens() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine.clone()).await;
		let mut stream = bridge_handshake(&api, "token=client-token")
			.await
			.expect("client bridge upgrade");

		send_bridge_frame(
			&mut stream,
			pb::bridge_frame::Frame::Call(pb::BridgeCall {
				method:   "/vmon.v1.PoolService/Set".to_owned(),
				metadata: HashMap::new(),
			}),
		)
		.await;

		match recv_bridge_frame(&mut stream).await {
			pb::bridge_frame::Frame::End(end) => {
				assert_eq!(end.status, Code::PermissionDenied as i32);
				assert_eq!(end.trailers.get("vmon-code").map(String::as_str), Some("unauthorized"));
			},
			other => panic!("unexpected bridge frame: {other:?}"),
		}
		assert!(engine.captures().pool_sets.is_empty());
	}

	#[tokio::test]
	async fn function_app_schedule_and_call_rpcs_round_trip() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;
		let mut functions = pb::function_service_client::FunctionServiceClient::new(api.channel());

		let revision = functions
			.register(authed(
				pb::RegisterFunctionRequest {
					spec:              Some(test_function_spec("echo", &api.template_revision)),
					request_id:        "register-echo".to_owned(),
					transient_secrets: Vec::new(),
				},
				"admin-token",
			))
			.await
			.expect("register function")
			.into_inner();
		let revision_ref = revision.r#ref.clone().expect("revision ref");
		functions
			.activate(authed(
				pb::ActivateFunctionRequest {
					revision:                  Some(revision_ref.clone()),
					expected_current_presence: None,
				},
				"admin-token",
			))
			.await
			.expect("activate function");
		let current = functions
			.get(authed(
				pb::GetFunctionRequest {
					function: Some(pb::FunctionSelector {
						selection: Some(pb::function_selector::Selection::Current(
							revision_ref.function.clone().expect("function ref"),
						)),
					}),
				},
				"client-token",
			))
			.await
			.expect("get active function")
			.into_inner();
		assert_eq!(current.r#ref, Some(revision_ref.clone()));
		let listed = functions
			.list(authed(pb::ListFunctionsRequest::default(), "client-token"))
			.await
			.expect("list functions")
			.into_inner();
		assert_eq!(listed.revisions.len(), 1);

		let inactive = functions
			.register(authed(
				pb::RegisterFunctionRequest {
					spec:              Some(test_function_spec("delete-me", &api.template_revision)),
					request_id:        "register-delete".to_owned(),
					transient_secrets: Vec::new(),
				},
				"admin-token",
			))
			.await
			.expect("register inactive function")
			.into_inner()
			.r#ref
			.expect("inactive revision ref");
		functions
			.delete(authed(pb::DeleteFunctionRequest { revision: Some(inactive) }, "admin-token"))
			.await
			.expect("delete inactive revision");

		let app = pb::AppRef { namespace: "tests".to_owned(), name: "app".to_owned() };
		let binding = pb::AppFunctionBinding {
			name:     "echo".to_owned(),
			revision: Some(revision_ref.clone()),
		};
		let first_app = functions
			.activate_app(authed(
				pb::ActivateAppRequest {
					app: Some(app.clone()),
					functions: vec![binding.clone()],
					expected_current_presence: None,
					request_id: "app-1".to_owned(),
				},
				"admin-token",
			))
			.await
			.expect("activate app")
			.into_inner();
		let second_app = functions
			.activate_app(authed(
				pb::ActivateAppRequest {
					app: Some(app.clone()),
					functions: vec![binding],
					expected_current_presence: None,
					request_id: "app-2".to_owned(),
				},
				"admin-token",
			))
			.await
			.expect("second app activation")
			.into_inner();
		assert_ne!(first_app.r#ref, second_app.r#ref);
		let current_app = functions
			.get_app(authed(
				pb::GetAppRequest {
					app: Some(pb::AppSelector {
						selection: Some(pb::app_selector::Selection::Current(app.clone())),
					}),
				},
				"client-token",
			))
			.await
			.expect("get app")
			.into_inner();
		assert_eq!(current_app.r#ref, second_app.r#ref);
		functions
			.rollback_app(authed(
				pb::RollbackAppRequest {
					target:                    first_app.r#ref.clone(),
					expected_current_presence: None,
					request_id:                "rollback".to_owned(),
				},
				"admin-token",
			))
			.await
			.expect("rollback app");

		let schedule = functions
			.create_schedule(authed(
				pb::CreateScheduleRequest {
					schedule_id_presence: Some(
						pb::create_schedule_request::ScheduleIdPresence::ScheduleId(
							"periodic".to_owned(),
						),
					),
					spec:                 Some(pb::ScheduleSpec {
						name:   "periodic".to_owned(),
						app:    first_app.r#ref.clone(),
						target: Some(pb::ScheduleTarget {
							function: Some(revision_ref.clone()),
							input:    Some(test_json_value(json!({"scheduled": true}))),
						}),
						timing: Some(pb::schedule_spec::Timing::Period(pb::PeriodSchedule {
							period_millis:      60_000,
							anchor_unix_millis: 1,
						})),
						status: pb::ScheduleStatus::Active as i32,
						labels: Default::default(),
					}),
					request_id:           "schedule".to_owned(),
				},
				"admin-token",
			))
			.await
			.expect("create schedule")
			.into_inner();
		let schedule_ref = schedule.r#ref.expect("schedule ref");
		functions
			.get_schedule(authed(schedule_ref.clone(), "client-token"))
			.await
			.expect("get schedule");
		assert_eq!(
			functions
				.list_schedules(authed(pb::ListSchedulesRequest::default(), "client-token"))
				.await
				.expect("list schedules")
				.into_inner()
				.schedules
				.len(),
			1
		);
		functions
			.delete_schedule(authed(schedule_ref, "admin-token"))
			.await
			.expect("delete schedule");

		let mut calls = pb::call_service_client::CallServiceClient::new(api.channel());
		let call = calls
			.create(authed(
				pb::CreateCallRequest {
					r#type: pb::CallType::Batch as i32,
					target: Some(pb::CallTarget { function: Some(revision_ref), receiver: None }),
					inputs_closed: false,
					request_id: "open-call".to_owned(),
					..Default::default()
				},
				"client-token",
			))
			.await
			.expect("create call")
			.into_inner();
		let call_ref = call.r#ref.expect("call ref");
		let malformed = pb::StreamCallInputsRequest {
			frame: Some(pb::stream_call_inputs_request::Frame::Input(pb::CallInput {
				index:    0,
				payload:  Some(pb::call_input::Payload::Value(test_json_value(json!(1)))),
				input_id: "malformed-input".to_owned(),
			})),
		};
		let status = calls
			.stream_inputs(authed(tokio_stream::iter([malformed]), "client-token"))
			.await
			.expect_err("input before call opener rejected");
		assert_eq!(status.code(), Code::InvalidArgument);
		let opener = pb::StreamCallInputsRequest {
			frame: Some(pb::stream_call_inputs_request::Frame::Call(call_ref.clone())),
		};
		let input = pb::StreamCallInputsRequest {
			frame: Some(pb::stream_call_inputs_request::Frame::Input(pb::CallInput {
				index:    0,
				payload:  Some(pb::call_input::Payload::Arguments(pb::InvocationArguments {
					positional: vec![test_json_value(json!(1))],
					named:      HashMap::from([("named".to_owned(), test_json_value(json!(2)))]),
				})),
				input_id: "input-0".to_owned(),
			})),
		};
		let mut acknowledgements = calls
			.stream_inputs(authed(tokio_stream::iter([opener, input]), "client-token"))
			.await
			.expect("stream input")
			.into_inner();
		let opener_ack = acknowledgements
			.message()
			.await
			.expect("opener ack")
			.expect("opener frame");
		assert_eq!(opener_ack.committed_input_count, 0);
		let committed = acknowledgements
			.message()
			.await
			.expect("input ack")
			.expect("input frame");
		assert_eq!(committed.committed_input_count, 1);
		calls
			.close_inputs(authed(
				pb::CloseCallInputsRequest {
					call:                 Some(call_ref.clone()),
					expected_input_count: 1,
				},
				"client-token",
			))
			.await
			.expect("close call inputs");
		assert_eq!(
			calls
				.list(authed(pb::ListCallsRequest::default(), "client-token"))
				.await
				.expect("list calls")
				.into_inner()
				.calls
				.len(),
			1
		);
		let watched: Vec<_> = calls
			.watch(authed(
				pb::WatchCallRequest {
					cursor: Some(pb::EventCursor {
						call:           Some(call_ref.clone()),
						after_sequence: 0,
					}),
					follow: false,
				},
				"client-token",
			))
			.await
			.expect("watch replay")
			.into_inner()
			.collect::<Result<Vec<_>, _>>()
			.await
			.expect("watch events");
		assert!(!watched.is_empty());
		calls
			.cancel(authed(
				pb::CancelCallRequest {
					call:       Some(call_ref.clone()),
					reason:     "test".to_owned(),
					request_id: "cancel".to_owned(),
				},
				"client-token",
			))
			.await
			.expect("cancel call");
		calls
			.get(authed(call_ref.clone(), "client-token"))
			.await
			.expect("get call");
		let missing = calls
			.get_result(authed(
				pb::GetCallResultRequest { call: Some(call_ref.clone()), index: 99 },
				"client-token",
			))
			.await
			.expect_err("missing result");
		assert_eq!(missing.code(), Code::NotFound);
		let result_page = calls
			.list_results(authed(
				pb::ListCallResultsRequest {
					cursor:    Some(pb::ResultCursor {
						call:           Some(call_ref),
						after_sequence: 0,
					}),
					page_size: 10,
				},
				"client-token",
			))
			.await
			.expect("list empty results")
			.into_inner();
		assert!(result_page.results.is_empty());
		assert!(result_page.end);

		let response = api
			.client
			.post(api.url("/v1/functions/tests/echo/invoke"))
			.header(reqwest::header::AUTHORIZATION, "Bearer client-token")
			.header(reqwest::header::CONTENT_TYPE, "application/x-python-cloudpickle")
			.body(vec![0_u8; 8])
			.send()
			.await
			.expect("cloudpickle request");
		assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
	}

	#[tokio::test]
	async fn actor_rpc_surface_returns_stable_missing_resource_errors() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;
		let mut actors = pb::actor_service_client::ActorServiceClient::new(api.channel());
		let missing_actor = pb::ActorRef { actor_id: "missing".to_owned() };
		let missing_checkpoint = pb::ActorCheckpointRef { checkpoint_id: "missing".to_owned() };
		let missing_revision = pb::RevisionRef {
			function:    Some(pb::FunctionRef {
				namespace: "tests".to_owned(),
				name:      "actor".to_owned(),
			}),
			revision_id: "missing".to_owned(),
		};

		let create = actors
			.create(authed(
				pb::CreateActorRequest {
					function: Some(missing_revision),
					request_id: "create-missing".to_owned(),
					..Default::default()
				},
				"client-token",
			))
			.await
			.expect_err("missing actor revision");
		assert_eq!(create.code(), Code::NotFound);
		assert_eq!(
			actors
				.get(authed(missing_actor.clone(), "client-token"))
				.await
				.expect_err("missing actor")
				.code(),
			Code::NotFound
		);
		assert_eq!(
			actors
				.checkpoint(authed(
					pb::CheckpointActorRequest {
						actor:      Some(missing_actor.clone()),
						request_id: "checkpoint".to_owned(),
					},
					"client-token",
				))
				.await
				.expect_err("missing checkpoint actor")
				.code(),
			Code::NotFound
		);
		assert_eq!(
			actors
				.restore(authed(
					pb::RestoreActorRequest {
						actor:      Some(missing_actor.clone()),
						checkpoint: Some(missing_checkpoint.clone()),
						request_id: "restore".to_owned(),
					},
					"client-token",
				))
				.await
				.expect_err("missing restore actor")
				.code(),
			Code::NotFound
		);
		assert_eq!(
			actors
				.fork(authed(
					pb::ForkActorRequest {
						checkpoint: Some(missing_checkpoint),
						request_id: "fork".to_owned(),
						labels:     Default::default(),
					},
					"client-token",
				))
				.await
				.expect_err("missing fork checkpoint")
				.code(),
			Code::NotFound
		);
		assert_eq!(
			actors
				.delete(authed(missing_actor, "client-token"))
				.await
				.expect_err("missing actor delete")
				.code(),
			Code::NotFound
		);
	}
	#[tokio::test]
	async fn artifact_rpc_enforces_stream_contract_checksum_and_round_trips() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;
		let mut client = pb::artifact_service_client::ArtifactServiceClient::new(api.channel())
			.max_decoding_message_size(64 * 1024 * 1024)
			.max_encoding_message_size(64 * 1024 * 1024);

		let malformed = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Data(b"bad".to_vec())),
		};
		let status = client
			.put(authed(tokio_stream::iter([malformed]), "admin-token"))
			.await
			.expect_err("data before header is rejected");
		assert_eq!(status.code(), Code::InvalidArgument);

		let bytes = b"durable artifact".to_vec();
		let bad_header = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Header(pb::PutArtifactHeader {
				expected_digest:     Some(pb::Digest {
					algorithm: pb::DigestAlgorithm::Sha256 as i32,
					value:     vec![0; 32],
				}),
				expected_size_bytes: bytes.len() as u64,
				media_type_presence: None,
				ttl_millis_presence: None,
			})),
		};
		let data = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Data(bytes.clone())),
		};
		let status = client
			.put(authed(tokio_stream::iter([bad_header, data]), "admin-token"))
			.await
			.expect_err("checksum mismatch is rejected");
		assert_eq!(status.code(), Code::DataLoss);
		assert_eq!(vmon_code(&status).as_deref(), Some("checksum"));

		let digest = pb::Digest {
			algorithm: pb::DigestAlgorithm::Sha256 as i32,
			value:     Sha256::digest(&bytes).to_vec(),
		};
		let header = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Header(pb::PutArtifactHeader {
				expected_digest:     Some(digest.clone()),
				expected_size_bytes: bytes.len() as u64,
				media_type_presence: Some(pb::put_artifact_header::MediaTypePresence::MediaType(
					"application/octet-stream".to_owned(),
				)),
				ttl_millis_presence: None,
			})),
		};
		let data = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Data(bytes.clone())),
		};
		let record = client
			.put(authed(tokio_stream::iter([header, data]), "admin-token"))
			.await
			.expect("artifact upload")
			.into_inner();
		assert_eq!(record.size_bytes, bytes.len() as u64);
		let conflicting_header = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Header(pb::PutArtifactHeader {
				expected_digest:     Some(digest.clone()),
				expected_size_bytes: bytes.len() as u64,
				media_type_presence: Some(pb::put_artifact_header::MediaTypePresence::MediaType(
					"application/octet-stream".to_owned(),
				)),
				ttl_millis_presence: Some(pb::put_artifact_header::TtlMillisPresence::TtlMillis(
					60_000,
				)),
			})),
		};
		let conflicting_data = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Data(bytes.clone())),
		};
		let deduped = client
			.put(authed(tokio_stream::iter([conflicting_header, conflicting_data]), "admin-token"))
			.await
			.expect("dedupe returns canonical metadata")
			.into_inner();
		assert_eq!(deduped, record);

		let reference = pb::ArtifactRef { digest: Some(digest) };
		let stat = client
			.stat(authed(reference.clone(), "client-token"))
			.await
			.expect("client token may stat")
			.into_inner();
		assert_eq!(stat.size_bytes, bytes.len() as u64);
		assert!(matches!(
			stat.media_type_presence,
			Some(pb::artifact_record::MediaTypePresence::MediaType(value))
				if value == "application/octet-stream"
		));

		let mut download = client
			.get(authed(
				pb::GetArtifactRequest { artifact: Some(reference), range_presence: None },
				"client-token",
			))
			.await
			.expect("artifact download")
			.into_inner();
		let mut downloaded = Vec::new();
		let mut saw_eof = false;
		while let Some(chunk) = download.message().await.expect("download frame") {
			downloaded.extend_from_slice(&chunk.data);
			saw_eof = chunk.eof;
		}
		assert_eq!(downloaded, bytes);
		assert!(saw_eof);

		let chunk_size = 1024 * 1024;
		let chunk_count = 65_u64;
		let chunk = vec![0xa5; chunk_size];
		let mut hasher = Sha256::new();
		for _ in 0..chunk_count {
			hasher.update(&chunk);
		}
		let large_digest = pb::Digest {
			algorithm: pb::DigestAlgorithm::Sha256 as i32,
			value:     hasher.finalize().to_vec(),
		};
		let interrupted_header = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Header(pb::PutArtifactHeader {
				expected_digest:     Some(large_digest.clone()),
				expected_size_bytes: chunk_count * chunk_size as u64,
				media_type_presence: None,
				ttl_millis_presence: None,
			})),
		};
		let interrupted = [
			interrupted_header.clone(),
			pb::PutArtifactRequest {
				frame: Some(pb::put_artifact_request::Frame::Data(chunk.clone())),
			},
			interrupted_header,
		];
		let status = client
			.put(authed(tokio_stream::iter(interrupted), "admin-token"))
			.await
			.expect_err("interrupted upload rejected");
		assert_eq!(status.code(), Code::InvalidArgument);
		let missing = client
			.stat(authed(pb::ArtifactRef { digest: Some(large_digest.clone()) }, "client-token"))
			.await
			.expect_err("interrupted upload is not committed");
		assert_eq!(missing.code(), Code::NotFound);
		let large_header = pb::PutArtifactRequest {
			frame: Some(pb::put_artifact_request::Frame::Header(pb::PutArtifactHeader {
				expected_digest:     Some(large_digest),
				expected_size_bytes: chunk_count * chunk_size as u64,
				media_type_presence: None,
				ttl_millis_presence: None,
			})),
		};
		let frames = std::iter::once(large_header).chain((0..chunk_count).map(move |_| {
			pb::PutArtifactRequest {
				frame: Some(pb::put_artifact_request::Frame::Data(vec![0xa5; chunk_size])),
			}
		}));
		let large = client
			.put(authed(tokio_stream::iter(frames), "admin-token"))
			.await
			.expect("multi-message upload larger than one gRPC message")
			.into_inner();
		assert_eq!(large.size_bytes, chunk_count * chunk_size as u64);
	}
}
