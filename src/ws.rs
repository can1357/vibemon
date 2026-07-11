use std::{
	io::{ErrorKind, Read, Write},
	sync::atomic::{AtomicU32, Ordering},
	time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde_json::Value;
use sha1::{Digest, Sha1};

use crate::{
	error::{CliError, Result, err},
	transport::{Connection, Endpoint},
};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
static MASK_COUNTER: AtomicU32 = AtomicU32::new(1);

pub struct WebSocket {
	stream: Connection,
}

pub enum Message {
	Text(String),
	Binary(Vec<u8>),
}

impl WebSocket {
	pub fn connect(endpoint: &Endpoint, path: &str) -> Result<Self> {
		let mut stream = endpoint.connect()?;
		let key = websocket_key();
		let request_path = endpoint.request_path(path);
		let mut request = format!(
			"GET {request_path} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: \
			 Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: \
			 vmon/{}\r\n",
			endpoint.host_header(),
			env!("CARGO_PKG_VERSION")
		);
		if let Some(token) = endpoint.token() {
			request.push_str("Authorization: Bearer ");
			request.push_str(token);
			request.push_str("\r\n");
		}
		request.push_str("\r\n");
		stream.write_all(request.as_bytes())?;
		stream.flush()?;
		let header = read_handshake_header(&mut stream)?;
		let mut lines = header.split("\r\n");
		let status_line = lines.next().unwrap_or_default();
		let status = status_line
			.split_whitespace()
			.nth(1)
			.and_then(|part| part.parse::<u16>().ok())
			.ok_or_else(|| CliError::new(format!("malformed WebSocket status: {status_line}")))?;
		if status != 101 {
			return err(format!("WebSocket upgrade failed with HTTP {status}"));
		}
		let mut headers = std::collections::HashMap::new();
		for line in lines {
			if let Some((name, value)) = line.split_once(':') {
				headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
			}
		}
		let expected = websocket_accept(&key);
		if headers
			.get("sec-websocket-accept")
			.is_none_or(|value| value != &expected)
		{
			return err("WebSocket upgrade returned an invalid accept key");
		}
		Ok(Self { stream })
	}

	pub fn try_clone(&self) -> Result<Self> {
		Ok(Self { stream: self.stream.try_clone()? })
	}

	pub fn send_json(&mut self, value: &Value) -> Result<()> {
		self.send_text(&serde_json::to_string(value)?)
	}

	pub fn send_text(&mut self, text: &str) -> Result<()> {
		self.write_frame(0x1, text.as_bytes())
	}

	pub fn send_close(&mut self) -> Result<()> {
		self.write_frame(0x8, &[])
	}

	pub fn next_message(&mut self) -> Result<Option<Message>> {
		loop {
			let Some((opcode, payload)) = self.read_frame()? else {
				return Ok(None);
			};
			match opcode {
				0x1 => return Ok(Some(Message::Text(String::from_utf8_lossy(&payload).into_owned()))),
				0x2 => return Ok(Some(Message::Binary(payload))),
				0x8 => return Ok(None),
				0x9 => self.write_frame(0xa, &payload)?,
				0xa => {},
				_ => {},
			}
		}
	}

	fn write_frame(&mut self, opcode: u8, payload: &[u8]) -> Result<()> {
		let mut frame = Vec::with_capacity(payload.len() + 14);
		frame.push(0x80 | opcode);
		let mask_bit = 0x80;
		if payload.len() < 126 {
			frame.push(mask_bit | payload.len() as u8);
		} else if u16::try_from(payload.len()).is_ok() {
			frame.push(mask_bit | 0x7e);
			frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
		} else {
			frame.push(mask_bit | 0x7f);
			frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
		}
		let mask = mask_key();
		frame.extend_from_slice(&mask);
		for (idx, byte) in payload.iter().enumerate() {
			frame.push(byte ^ mask[idx % 4]);
		}
		self.stream.write_all(&frame)?;
		self.stream.flush()?;
		Ok(())
	}

	fn read_frame(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
		let mut first = [0_u8; 2];
		if let Err(error) = self.stream.read_exact(&mut first) {
			if error.kind() == ErrorKind::UnexpectedEof {
				return Ok(None);
			}
			return Err(error.into());
		}
		let opcode = first[0] & 0x0f;
		let masked = first[1] & 0x80 != 0;
		let mut len = u64::from(first[1] & 0x7f);
		if len == 126 {
			let mut ext = [0_u8; 2];
			self.stream.read_exact(&mut ext)?;
			len = u64::from(u16::from_be_bytes(ext));
		} else if len == 127 {
			let mut ext = [0_u8; 8];
			self.stream.read_exact(&mut ext)?;
			len = u64::from_be_bytes(ext);
		}
		if len > 64 * 1024 * 1024 {
			return err("WebSocket frame exceeds 64 MiB");
		}
		let mut mask = [0_u8; 4];
		if masked {
			self.stream.read_exact(&mut mask)?;
		}
		let mut payload = vec![0_u8; len as usize];
		self.stream.read_exact(&mut payload)?;
		if masked {
			for (idx, byte) in payload.iter_mut().enumerate() {
				*byte ^= mask[idx % 4];
			}
		}
		Ok(Some((opcode, payload)))
	}
}

fn read_handshake_header(stream: &mut Connection) -> Result<String> {
	let mut raw = Vec::new();
	let mut buf = [0_u8; 1];
	while raw.len() < 64 * 1024 {
		stream.read_exact(&mut buf)?;
		raw.push(buf[0]);
		if raw.ends_with(b"\r\n\r\n") {
			return Ok(String::from_utf8_lossy(&raw[..raw.len() - 4]).into_owned());
		}
	}
	err("WebSocket handshake header is too large")
}

fn websocket_key() -> String {
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_nanos();
	let pid = u128::from(std::process::id());
	B64.encode((now ^ (pid << 32)).to_be_bytes())
}

fn websocket_accept(key: &str) -> String {
	let mut sha = Sha1::new();
	sha.update(key.as_bytes());
	sha.update(WS_GUID.as_bytes());
	B64.encode(sha.finalize())
}

fn mask_key() -> [u8; 4] {
	let counter = MASK_COUNTER.fetch_add(1, Ordering::Relaxed);
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.subsec_nanos();
	(counter ^ nanos).to_be_bytes()
}
