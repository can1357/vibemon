use std::{
	collections::HashMap,
	io::{Read, Write},
	net::TcpStream,
	os::unix::net::UnixStream,
	path::{Path, PathBuf},
	process::{Command, Stdio},
	sync::{Arc, LazyLock, OnceLock},
	thread,
	time::{Duration, Instant},
};

use serde_json::Value;
use tonic::{
	metadata::{Ascii, MetadataValue},
	service::interceptor::InterceptedService,
	transport::{Channel, Endpoint as GrpcEndpoint, Uri},
};
use vmon_proto::v1::{
	pool_service_client::PoolServiceClient, sandbox_service_client::SandboxServiceClient,
	snapshot_service_client::SnapshotServiceClient, system_service_client::SystemServiceClient,
	volume_service_client::VolumeServiceClient,
};

use crate::error::{CliError, Result, err};

/// 64 MiB cap on encoded/decoded gRPC messages (file transfers, capture
/// output).
const GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub type SandboxClient = SandboxServiceClient<InterceptedService<Channel, AuthInterceptor>>;
pub type SnapshotClient = SnapshotServiceClient<InterceptedService<Channel, AuthInterceptor>>;
pub type VolumeClient = VolumeServiceClient<InterceptedService<Channel, AuthInterceptor>>;
pub type PoolClient = PoolServiceClient<InterceptedService<Channel, AuthInterceptor>>;
pub type SystemClient = SystemServiceClient<InterceptedService<Channel, AuthInterceptor>>;

#[derive(Clone, Debug)]
pub struct ApiClient {
	endpoints: Vec<Endpoint>,
	autostart: bool,
	grpc:      Arc<OnceLock<Grpc>>,
}

/// Shared handle to the lazily-connected gRPC channel plus the background
/// runtime that drives it. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Grpc {
	handle:  tokio::runtime::Handle,
	channel: Channel,
	auth:    AuthInterceptor,
}

/// Injects `authorization: Bearer <token>` metadata on every RPC.
#[derive(Clone, Debug)]
pub struct AuthInterceptor {
	token: Option<MetadataValue<Ascii>>,
}

impl tonic::service::Interceptor for AuthInterceptor {
	fn call(
		&mut self,
		mut request: tonic::Request<()>,
	) -> std::result::Result<tonic::Request<()>, tonic::Status> {
		if let Some(token) = &self.token {
			request
				.metadata_mut()
				.insert("authorization", token.clone());
		}
		Ok(request)
	}
}

impl Grpc {
	/// Runs `future` to completion on the transport runtime.
	pub fn block_on<F: Future>(&self, future: F) -> F::Output {
		self.handle.block_on(future)
	}

	pub fn sandboxes(&self) -> SandboxClient {
		self.client(SandboxServiceClient::with_interceptor)
	}

	pub fn snapshots(&self) -> SnapshotClient {
		self.client(SnapshotServiceClient::with_interceptor)
	}

	pub fn volumes(&self) -> VolumeClient {
		self.client(VolumeServiceClient::with_interceptor)
	}

	pub fn pools(&self) -> PoolClient {
		self.client(PoolServiceClient::with_interceptor)
	}

	pub fn system(&self) -> SystemClient {
		self.client(SystemServiceClient::with_interceptor)
	}

	fn client<T>(&self, build: fn(Channel, AuthInterceptor) -> T) -> T
	where
		T: WithMessageLimits,
	{
		build(self.channel.clone(), self.auth.clone()).with_message_limits()
	}
}

/// Uniform 64 MiB limit application across the generated clients.
trait WithMessageLimits {
	fn with_message_limits(self) -> Self;
}

macro_rules! impl_message_limits {
	($($client:ident),+ $(,)?) => {
		$(impl WithMessageLimits for $client {
			fn with_message_limits(self) -> Self {
				self
					.max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
					.max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
			}
		})+
	};
}

impl_message_limits!(SandboxClient, SnapshotClient, VolumeClient, PoolClient, SystemClient);

/// Rebuilds a `CliError` from a gRPC status: the stable vmond code travels in
/// `vmon-code` metadata; the gRPC code is the fallback mapping.
pub fn status_error(status: tonic::Status) -> CliError {
	let code = status
		.metadata()
		.get("vmon-code")
		.and_then(|value| value.to_str().ok())
		.filter(|code| !code.is_empty())
		.map_or_else(|| fallback_code(status.code()), str::to_owned);
	CliError::new(format!("{code}: {}", status.message()))
}

fn fallback_code(code: tonic::Code) -> String {
	match code {
		tonic::Code::NotFound => "not_found",
		tonic::Code::InvalidArgument => "invalid",
		tonic::Code::Unauthenticated => "unauthorized",
		tonic::Code::FailedPrecondition => "not_running",
		tonic::Code::Aborted => "busy",
		tonic::Code::Unimplemented => "unsupported",
		tonic::Code::Unavailable => "engine",
		_ => "error",
	}
	.to_owned()
}

#[derive(Clone, Debug)]
pub enum Endpoint {
	Uds { sock: PathBuf },
	Tcp { base: BaseUrl, token: Option<String> },
}

#[derive(Clone, Debug)]
pub struct BaseUrl {
	scheme:      String,
	host:        String,
	port:        u16,
	host_header: String,
	prefix:      String,
}

pub enum Connection {
	Unix(UnixStream),
	Tcp(TcpStream),
}

pub struct HttpResponse {
	pub status: u16,
	pub body:   Vec<u8>,
}

impl ApiClient {
	pub fn local(autostart: bool) -> Self {
		let home = vmond::home::Home::new(vmond::home::state_dir());
		Self {
			endpoints: vec![Endpoint::Uds { sock: home.vmond_sock() }],
			autostart,
			grpc: Arc::default(),
		}
	}

	pub fn remote(endpoints: Vec<String>, token: Option<String>) -> Result<Self> {
		if endpoints.is_empty() {
			return err("remote context has no endpoints");
		}
		let endpoints = endpoints
			.into_iter()
			.map(|endpoint| {
				Ok(Endpoint::Tcp { base: BaseUrl::parse(&endpoint)?, token: token.clone() })
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(Self { endpoints, autostart: false, grpc: Arc::default() })
	}

	pub fn request_json(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value> {
		let body = match body {
			Some(value) => Some(("application/json", serde_json::to_vec(&value)?)),
			None => None,
		};
		let response = self.request(method, path, body)?;
		if response.body.is_empty() {
			return Ok(Value::Object(Default::default()));
		}
		serde_json::from_slice(&response.body).map_err(Into::into)
	}

	pub fn request_text(&self, method: &str, path: &str) -> Result<String> {
		let response = self.request(method, path, None)?;
		Ok(String::from_utf8_lossy(&response.body).into_owned())
	}

	pub fn request(
		&self,
		method: &str,
		path: &str,
		body: Option<(&str, Vec<u8>)>,
	) -> Result<HttpResponse> {
		let mut last_error = None;
		for endpoint in &self.endpoints {
			if matches!(endpoint, Endpoint::Uds { .. }) && self.autostart {
				self.ensure_local_running()?;
			}
			match send_request(endpoint, method, path, body.as_ref()) {
				Ok(response) if (200..300).contains(&response.status) => return Ok(response),
				Ok(response) => return api_status_error(response),
				Err(error) => last_error = Some(error),
			}
		}
		Err(last_error.unwrap_or_else(|| CliError::new("no API endpoint configured")))
	}

	/// Lazily connects the gRPC channel (trying each endpoint in order) and
	/// returns a cheap clone of the shared transport.
	pub fn grpc(&self) -> Result<Grpc> {
		if let Some(grpc) = self.grpc.get() {
			return Ok(grpc.clone());
		}
		let grpc = self.connect_grpc()?;
		Ok(self.grpc.get_or_init(|| grpc).clone())
	}

	fn connect_grpc(&self) -> Result<Grpc> {
		let handle = GRPC_RUNTIME.clone();
		let mut last_error = None;
		for endpoint in &self.endpoints {
			if matches!(endpoint, Endpoint::Uds { .. }) && self.autostart {
				self.ensure_local_running()?;
			}
			match connect_channel(&handle, endpoint) {
				Ok(channel) => {
					let auth = AuthInterceptor { token: bearer(endpoint.token())? };
					return Ok(Grpc { handle, channel, auth });
				},
				Err(error) => last_error = Some(error),
			}
		}
		Err(last_error.unwrap_or_else(|| CliError::new("no API endpoint configured")))
	}

	pub fn ensure_local_running(&self) -> Result<()> {
		let Some(Endpoint::Uds { sock }) = self.endpoints.first() else {
			return Ok(());
		};
		if healthz(sock).is_ok() {
			return Ok(());
		}
		let exe = std::env::current_exe()?;
		Command::new(exe)
			.arg("serve")
			.arg("--port")
			.arg("0")
			.stdin(Stdio::null())
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.map_err(|error| CliError::new(format!("failed to start vmond: {error}")))?;
		let deadline = Instant::now() + Duration::from_secs(10);
		while Instant::now() < deadline {
			if healthz(sock).is_ok() {
				return Ok(());
			}
			thread::sleep(Duration::from_millis(100));
		}
		err(format!("vmond did not become ready at {}", sock.display()))
	}
}

/// Background current-thread runtime driving all gRPC IO; unary calls
/// `block_on` against its handle from the CLI thread.
static GRPC_RUNTIME: LazyLock<tokio::runtime::Handle> = LazyLock::new(|| {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.expect("failed to build the gRPC runtime");
	let handle = runtime.handle().clone();
	thread::Builder::new()
		.name("vmon-grpc".to_owned())
		.spawn(move || runtime.block_on(std::future::pending::<()>()))
		.expect("failed to spawn the gRPC runtime thread");
	handle
});

fn bearer(token: Option<&str>) -> Result<Option<MetadataValue<Ascii>>> {
	token
		.map(|token| {
			format!("Bearer {token}")
				.parse()
				.map_err(|_| CliError::new("API token contains characters not valid in a header"))
		})
		.transpose()
}

fn connect_channel(handle: &tokio::runtime::Handle, endpoint: &Endpoint) -> Result<Channel> {
	match endpoint {
		Endpoint::Uds { sock } => {
			let sock = sock.clone();
			let connector = tower::service_fn(move |_: Uri| {
				let sock = sock.clone();
				async move {
					let stream = tokio::net::UnixStream::connect(sock).await?;
					Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
				}
			});
			handle
				.block_on(
					GrpcEndpoint::from_static("http://vmon.uds").connect_with_connector(connector),
				)
				.map_err(|error| CliError::new(format!("gRPC connect failed: {error}")))
		},
		Endpoint::Tcp { base, .. } => {
			if base.scheme != "http" {
				return err(format!(
					"{} contexts are not supported by this CLI transport yet; use http://",
					base.scheme
				));
			}
			let uri = format!("http://{}:{}", base.host, base.port);
			let grpc_endpoint = GrpcEndpoint::from_shared(uri)
				.map_err(|error| CliError::new(format!("invalid gRPC endpoint: {error}")))?;
			handle
				.block_on(grpc_endpoint.connect())
				.map_err(|error| CliError::new(format!("gRPC connect failed: {error}")))
		},
	}
}

impl Endpoint {
	pub fn connect(&self) -> Result<Connection> {
		match self {
			Self::Uds { sock } => Ok(Connection::Unix(UnixStream::connect(sock)?)),
			Self::Tcp { base, .. } => {
				if base.scheme != "http" {
					return err(format!(
						"{} contexts are not supported by this CLI transport yet; use http://",
						base.scheme
					));
				}
				Ok(Connection::Tcp(TcpStream::connect((base.host.as_str(), base.port))?))
			},
		}
	}

	pub fn request_path(&self, path: &str) -> String {
		match self {
			Self::Uds { .. } => path.to_owned(),
			Self::Tcp { base, .. } => base.path(path),
		}
	}

	pub fn host_header(&self) -> String {
		match self {
			Self::Uds { .. } => "vmon".to_owned(),
			Self::Tcp { base, .. } => base.host_header.clone(),
		}
	}

	pub fn token(&self) -> Option<&str> {
		match self {
			Self::Uds { .. } => None,
			Self::Tcp { token, .. } => token.as_deref(),
		}
	}
}

impl BaseUrl {
	pub fn parse(input: &str) -> Result<Self> {
		let normalized = if input.contains("://") {
			input.trim().trim_end_matches('/').to_owned()
		} else {
			format!("http://{}", input.trim().trim_end_matches('/'))
		};
		let Some((scheme, rest)) = normalized.split_once("://") else {
			return err(format!("invalid server URL {input:?}"));
		};
		let (authority, prefix) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
		if authority.is_empty() {
			return err(format!("invalid server URL {input:?}: missing host"));
		}
		let (host, port) = split_authority(authority, scheme)?;
		let default_port = default_port(scheme)?;
		let port = port.unwrap_or(default_port);
		let host_header = if port == default_port {
			host.clone()
		} else {
			format!("{host}:{port}")
		};
		let prefix = if prefix.is_empty() {
			String::new()
		} else {
			format!("/{prefix}")
		};
		Ok(Self { scheme: scheme.to_owned(), host, port, host_header, prefix })
	}

	fn path(&self, path: &str) -> String {
		if self.prefix.is_empty() {
			path.to_owned()
		} else if path == "/" {
			self.prefix.clone()
		} else {
			format!("{}{}", self.prefix, path)
		}
	}
}

impl Read for Connection {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		match self {
			Self::Unix(stream) => stream.read(buf),
			Self::Tcp(stream) => stream.read(buf),
		}
	}
}

impl Write for Connection {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		match self {
			Self::Unix(stream) => stream.write(buf),
			Self::Tcp(stream) => stream.write(buf),
		}
	}

	fn flush(&mut self) -> std::io::Result<()> {
		match self {
			Self::Unix(stream) => stream.flush(),
			Self::Tcp(stream) => stream.flush(),
		}
	}
}

fn healthz(sock: &Path) -> Result<()> {
	let endpoint = Endpoint::Uds { sock: sock.to_path_buf() };
	let response = send_request(&endpoint, "GET", "/healthz", None)?;
	if (200..300).contains(&response.status) {
		Ok(())
	} else {
		err(format!("healthz returned HTTP {}", response.status))
	}
}

fn send_request(
	endpoint: &Endpoint,
	method: &str,
	path: &str,
	body: Option<&(&str, Vec<u8>)>,
) -> Result<HttpResponse> {
	let mut stream = endpoint.connect()?;
	let request_path = endpoint.request_path(path);
	let mut request = format!(
		"{method} {request_path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: vmon/{}\r\nAccept: \
		 */*\r\nConnection: close\r\n",
		endpoint.host_header(),
		env!("CARGO_PKG_VERSION")
	);
	if let Some(token) = endpoint.token() {
		request.push_str("Authorization: Bearer ");
		request.push_str(token);
		request.push_str("\r\n");
	}
	if let Some((content_type, bytes)) = body {
		request.push_str("Content-Type: ");
		request.push_str(content_type);
		request.push_str("\r\nContent-Length: ");
		request.push_str(&bytes.len().to_string());
		request.push_str("\r\n");
	} else {
		request.push_str("Content-Length: 0\r\n");
	}
	request.push_str("\r\n");
	stream.write_all(request.as_bytes())?;
	if let Some((_, bytes)) = body {
		stream.write_all(bytes)?;
	}
	stream.flush()?;
	let mut raw = Vec::new();
	stream.read_to_end(&mut raw)?;
	parse_response(raw)
}

fn parse_response(raw: Vec<u8>) -> Result<HttpResponse> {
	let Some(header_end) = find_header_end(&raw) else {
		return err("malformed HTTP response: missing header terminator");
	};
	let header = String::from_utf8_lossy(&raw[..header_end]);
	let mut lines = header.split("\r\n");
	let status_line = lines.next().unwrap_or_default();
	let status = status_line
		.split_whitespace()
		.nth(1)
		.and_then(|part| part.parse::<u16>().ok())
		.ok_or_else(|| CliError::new(format!("malformed HTTP status line: {status_line}")))?;
	let mut headers = HashMap::new();
	for line in lines {
		if let Some((key, value)) = line.split_once(':') {
			headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
		}
	}
	let mut body = raw[header_end + 4..].to_vec();
	if headers
		.get("transfer-encoding")
		.is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
	{
		body = decode_chunked(&body)?;
	}
	Ok(HttpResponse { status, body })
}

fn api_status_error<T>(response: HttpResponse) -> Result<T> {
	let detail = serde_json::from_slice::<Value>(&response.body)
		.ok()
		.and_then(|value| {
			let code = value.get("code").and_then(Value::as_str).unwrap_or("error");
			let message = value.get("message").and_then(Value::as_str)?;
			Some(format!("{code}: {message}"))
		})
		.unwrap_or_else(|| String::from_utf8_lossy(&response.body).trim().to_owned());
	let suffix = if detail.is_empty() {
		String::new()
	} else {
		format!(": {detail}")
	};
	err(format!("HTTP {}{}", response.status, suffix))
}

fn find_header_end(raw: &[u8]) -> Option<usize> {
	raw.windows(4).position(|window| window == b"\r\n\r\n")
}

fn decode_chunked(mut body: &[u8]) -> Result<Vec<u8>> {
	let mut out = Vec::new();
	loop {
		let Some(line_end) = body.windows(2).position(|window| window == b"\r\n") else {
			return err("malformed chunked response");
		};
		let line = String::from_utf8_lossy(&body[..line_end]);
		let size_text = line.split(';').next().unwrap_or_default().trim();
		let size =
			usize::from_str_radix(size_text, 16).map_err(|_| CliError::new("malformed chunk size"))?;
		body = &body[line_end + 2..];
		if size == 0 {
			return Ok(out);
		}
		if body.len() < size + 2 {
			return err("truncated chunked response");
		}
		out.extend_from_slice(&body[..size]);
		body = &body[size + 2..];
	}
}

fn split_authority(authority: &str, _scheme: &str) -> Result<(String, Option<u16>)> {
	if authority.starts_with('[') {
		let Some(end) = authority.find(']') else {
			return err(format!("invalid IPv6 authority {authority:?}"));
		};
		let host = authority[..=end].to_owned();
		let rest = &authority[end + 1..];
		let port = if let Some(port) = rest.strip_prefix(':') {
			Some(parse_port(port)?)
		} else {
			None
		};
		return Ok((host, port));
	}
	if let Some((host, port)) = authority.rsplit_once(':')
		&& !port.is_empty()
		&& port.chars().all(|ch| ch.is_ascii_digit())
	{
		return Ok((host.to_owned(), Some(parse_port(port)?)));
	}
	Ok((authority.to_owned(), None))
}

fn parse_port(port: &str) -> Result<u16> {
	port
		.parse::<u16>()
		.map_err(|_| CliError::new(format!("invalid port {port:?}")))
}

fn default_port(scheme: &str) -> Result<u16> {
	match scheme {
		"http" => Ok(80),
		"https" => Ok(443),
		_ => err(format!("unsupported URL scheme {scheme:?}")),
	}
}
