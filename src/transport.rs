use std::{
	collections::HashMap,
	io::{Read, Write},
	net::TcpStream,
	os::unix::net::UnixStream,
	path::{Path, PathBuf},
	process::{Command, Stdio},
	thread,
	time::{Duration, Instant},
};

use serde_json::Value;

use crate::{
	error::{CliError, Result, err},
	ws::WebSocket,
};

#[derive(Clone, Debug)]
pub struct ApiClient {
	endpoints: Vec<Endpoint>,
	autostart: bool,
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
		Self { endpoints: vec![Endpoint::Uds { sock: home.vmond_sock() }], autostart }
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
		Ok(Self { endpoints, autostart: false })
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

	pub fn request_bytes(&self, method: &str, path: &str, body: Option<Vec<u8>>) -> Result<Vec<u8>> {
		let body = body.map(|bytes| ("application/octet-stream", bytes));
		Ok(self.request(method, path, body)?.body)
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

	pub fn websocket(&self, path: &str) -> Result<WebSocket> {
		let mut last_error = None;
		for endpoint in &self.endpoints {
			if matches!(endpoint, Endpoint::Uds { .. }) && self.autostart {
				self.ensure_local_running()?;
			}
			match WebSocket::connect(endpoint, path) {
				Ok(socket) => return Ok(socket),
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

impl Connection {
	pub fn try_clone(&self) -> Result<Self> {
		match self {
			Self::Unix(stream) => Ok(Self::Unix(stream.try_clone()?)),
			Self::Tcp(stream) => Ok(Self::Tcp(stream.try_clone()?)),
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

pub fn percent_encode(input: &str) -> String {
	const HEX: &[u8; 16] = b"0123456789ABCDEF";
	let mut out = String::new();
	for byte in input.bytes() {
		if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
			out.push(byte as char);
		} else {
			out.push('%');
			out.push(HEX[(byte >> 4) as usize] as char);
			out.push(HEX[(byte & 0x0f) as usize] as char);
		}
	}
	out
}

pub fn query(params: &[(&str, String)]) -> String {
	let mut first = true;
	let mut out = String::new();
	for (key, value) in params {
		if !first {
			out.push('&');
		}
		first = false;
		out.push_str(&percent_encode(key));
		out.push('=');
		out.push_str(&percent_encode(value));
	}
	out
}

pub fn path_segment(segment: &str) -> String {
	percent_encode(segment)
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
