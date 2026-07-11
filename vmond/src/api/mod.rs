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
		sync::{Arc, Mutex},
	};

	use axum::http::StatusCode;
	use serde_json::{Value, json};
	use tokio::{io::AsyncWriteExt as _, net::TcpStream};
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
		base_url: String,
		client:   reqwest::Client,
		handle:   tokio::task::JoinHandle<()>,
	}

	impl ApiHarness {
		async fn start(engine: Arc<ScriptedEngine>) -> Self {
			let mut config = ServeConfig::default();
			config.token = Some("admin-token".to_owned());
			config.client_token = Some("client-token".to_owned());
			let state = ApiState::new(engine, config, Transport::Tcp);
			let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
				.await
				.expect("bind test api listener");
			let addr = listener.local_addr().expect("test api listener address");
			let router = test_router(state);
			let handle = tokio::spawn(async move {
				axum::serve(listener, router).await.expect("serve test api");
			});
			Self { base_url: format!("http://{addr}"), client: reqwest::Client::new(), handle }
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
		let value = MetadataValue::try_from(format!("Bearer {token}")).expect("bearer metadata");
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
}
