//! v1 HTTP API: axum router, auth tiers, WS exec, SSE. Port of
//! python/vmon/server/.

mod error;
mod routes;
mod server;
mod state;
mod validation;
mod ws;

use std::{collections::HashMap, hash::BuildHasher};

pub use error::{ApiError, ErrorBody};

/// Serve the v1 HTTP API over `$VMON_HOME/vmond.sock` and optional TCP.
pub fn serve<S>(overrides: HashMap<String, String, S>) -> crate::Result<()>
where
	S: BuildHasher,
{
	server::serve(overrides)
}

/// Return the generated v1 `OpenAPI` document as pretty JSON.
pub fn openapi_json() -> String {
	routes::openapi_json()
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
	}

	impl Drop for ApiHarness {
		fn drop(&mut self) {
			self.handle.abort();
		}
	}

	fn bearer(request: reqwest::RequestBuilder, token: &str) -> reqwest::RequestBuilder {
		request.bearer_auth(token)
	}

	async fn assert_error(
		response: reqwest::Response,
		status: StatusCode,
		code: &str,
		message: &str,
	) {
		assert_eq!(response.status(), status);
		let body: Value = response.json().await.expect("error json body");
		assert_eq!(body, json!({"code": code, "message": message}));
	}

	#[tokio::test]
	async fn api_bearer_admin_succeeds_and_missing_token_gets_401_envelope() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;

		let response = bearer(api.client.get(api.url("/v1/info")), "admin-token")
			.send()
			.await
			.expect("admin info response");
		assert_eq!(response.status(), StatusCode::OK);
		let body: Value = response.json().await.expect("admin info json");
		assert_eq!(body, json!({"version":"test"}));

		let response = api
			.client
			.get(api.url("/v1/info"))
			.send()
			.await
			.expect("missing token response");
		assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
		assert_eq!(
			response.headers().get(reqwest::header::WWW_AUTHENTICATE),
			Some(&reqwest::header::HeaderValue::from_static("Bearer"))
		);
		let body: Value = response.json().await.expect("missing token json");
		assert_eq!(body, json!({"code":"unauthorized","message":"unauthorized"}));
	}

	#[tokio::test]
	async fn api_client_token_is_forbidden_from_admin_pool_and_migrate_routes() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine.clone()).await;

		let pool_response = bearer(
			api.client
				.put(api.url("/v1/pools/x"))
				.json(&json!({"size": 1})),
			"client-token",
		)
		.send()
		.await
		.expect("client pool response");
		assert_error(pool_response, StatusCode::FORBIDDEN, "unauthorized", "forbidden").await;

		let migrate_response = bearer(
			api.client
				.post(api.url("/v1/sandboxes/sb/migrate"))
				.json(&json!({"target": "node-b"})),
			"client-token",
		)
		.send()
		.await
		.expect("client migrate response");
		assert_error(migrate_response, StatusCode::FORBIDDEN, "unauthorized", "forbidden").await;

		let captures = engine.captures();
		assert!(captures.pool_sets.is_empty());
		assert!(captures.migrations.is_empty());
	}

	#[tokio::test]
	async fn api_create_validation_rejects_python_compatible_bad_ports_cidrs_and_ha() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine.clone()).await;
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
			let response =
				bearer(api.client.post(api.url("/v1/sandboxes")).json(&payload), "admin-token")
					.send()
					.await
					.expect("validation response");
			assert_error(response, StatusCode::BAD_REQUEST, "invalid", message).await;
		}

		assert!(engine.captures().creates.is_empty());
	}

	#[tokio::test]
	async fn api_engine_errors_from_get_map_to_http_status_and_envelope_codes() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine.clone()).await;
		let cases = [
			(ErrorCode::NotFound, StatusCode::NOT_FOUND),
			(ErrorCode::NotRunning, StatusCode::CONFLICT),
			(ErrorCode::Busy, StatusCode::CONFLICT),
			(ErrorCode::Invalid, StatusCode::BAD_REQUEST),
			(ErrorCode::Unsupported, StatusCode::NOT_IMPLEMENTED),
			(ErrorCode::Unauthorized, StatusCode::UNAUTHORIZED),
			(ErrorCode::Engine, StatusCode::SERVICE_UNAVAILABLE),
		];

		for (code, status) in cases {
			let message = format!("mapped {}", code.as_str());
			engine.set_get_error(code, &message);
			let response = bearer(api.client.get(api.url("/v1/sandboxes/sb")), "admin-token")
				.send()
				.await
				.expect("engine error response");
			assert_eq!(response.status(), status);
			if code == ErrorCode::Unauthorized {
				assert_eq!(
					response.headers().get(reqwest::header::WWW_AUTHENTICATE),
					Some(&reqwest::header::HeaderValue::from_static("Bearer"))
				);
			}
			let body: Value = response.json().await.expect("engine error json");
			assert_eq!(body, json!({"code": code.as_str(), "message": message}));
		}
	}

	#[tokio::test]
	async fn api_create_returns_201_with_engine_view_passthrough() {
		let engine = Arc::new(ScriptedEngine::new());
		let view = json!({"id":"sb-1","state":"running","details":{"from":"fake"}});
		engine.set_create_response(view.clone());
		let api = ApiHarness::start(engine.clone()).await;

		let response = bearer(
			api.client
				.post(api.url("/v1/sandboxes"))
				.json(&json!({"name": "sb-1", "ports": [8080]})),
			"admin-token",
		)
		.send()
		.await
		.expect("create response");
		assert_eq!(response.status(), StatusCode::CREATED);
		let body: Value = response.json().await.expect("create json");
		assert_eq!(body, view);

		let captures = engine.captures();
		assert_eq!(captures.creates.len(), 1);
		assert_eq!(captures.creates[0].name.as_deref(), Some("sb-1"));
		assert_eq!(captures.creates[0].ports.as_deref(), Some(&[8080][..]));
	}

	#[tokio::test]
	async fn api_list_tag_query_passes_hash_map_to_engine() {
		let engine = Arc::new(ScriptedEngine::new());
		engine.set_list_response(vec![json!({"id":"sb-api"})]);
		let api = ApiHarness::start(engine.clone()).await;

		let response = bearer(api.client.get(api.url("/v1/sandboxes?tag=role=api")), "admin-token")
			.send()
			.await
			.expect("list response");
		assert_eq!(response.status(), StatusCode::OK);
		let body: Value = response.json().await.expect("list json");
		assert_eq!(body, json!({"sandboxes":[{"id":"sb-api"}]}));

		let captures = engine.captures();
		assert_eq!(captures.list_tags.len(), 1);
		assert_eq!(
			captures.list_tags[0]
				.as_ref()
				.and_then(|tags| tags.get("role")),
			Some(&"api".to_owned())
		);
	}

	#[tokio::test]
	async fn api_events_is_sse_and_starts_with_keepalive_comment() {
		let engine = Arc::new(ScriptedEngine::new());
		let api = ApiHarness::start(engine).await;

		let mut response = bearer(api.client.get(api.url("/v1/events")), "admin-token")
			.send()
			.await
			.expect("events response");
		assert_eq!(response.status(), StatusCode::OK);
		assert_eq!(
			response.headers().get(reqwest::header::CONTENT_TYPE),
			Some(&reqwest::header::HeaderValue::from_static("text/event-stream"))
		);

		let first = response
			.chunk()
			.await
			.expect("first sse chunk")
			.expect("first sse chunk bytes");
		assert!(first.starts_with(b": keepalive"));
	}
}
