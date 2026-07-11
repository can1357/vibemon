use std::{
	collections::HashMap,
	sync::Arc,
	thread::{self, JoinHandle},
};

use axum::extract::ws::{Message, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	sync::mpsc,
};

use super::{
	error::{ApiError, ApiResult},
	validation,
};
use crate::{
	engine::{EngineApi, ExecExit, ExecStream},
	models::ExecBody,
};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug)]
pub enum ClientExecFrame {
	Stdin(Vec<u8>),
	Eof,
	Resize(u16, u16),
	Ignored,
}

pub fn parse_client_exec_frame(text: &str) -> ApiResult<ClientExecFrame> {
	let value: Value =
		serde_json::from_str(text).map_err(|_| ApiError::invalid("invalid exec frame"))?;
	let Some(object) = value.as_object() else {
		return Err(ApiError::invalid("invalid exec frame"));
	};
	if let Some(stdin) = object.get("stdin_b64") {
		let Some(stdin) = stdin.as_str() else {
			return Err(ApiError::invalid("stdin_b64 must be a string"));
		};
		return B64
			.decode(stdin)
			.map(ClientExecFrame::Stdin)
			.map_err(|_| ApiError::invalid("stdin_b64 must be base64"));
	}
	if object.get("eof").and_then(Value::as_bool).unwrap_or(false) {
		return Ok(ClientExecFrame::Eof);
	}
	if let Some(resize) = object.get("resize") {
		let Some(items) = resize.as_array() else {
			return Err(ApiError::invalid("resize must be [rows, cols]"));
		};
		if items.len() != 2 {
			return Err(ApiError::invalid("resize must be [rows, cols]"));
		}
		let rows = resize_dim(&items[0])?;
		let cols = resize_dim(&items[1])?;
		return Ok(ClientExecFrame::Resize(rows, cols));
	}
	Ok(ClientExecFrame::Ignored)
}

pub fn encode_stream_frame(stream: &str, data: &[u8]) -> Value {
	json!({"stream": stream, "b64": B64.encode(data)})
}

pub fn encode_exit_frame(exit: ExecExit) -> Value {
	json!({"exit": exit.code, "signal": exit.signal})
}

pub async fn exec_socket(engine: Arc<dyn EngineApi>, id: String, mut socket: WebSocket) {
	let Some(first) = receive_text(&mut socket).await else {
		let _ = socket.close().await;
		return;
	};
	let body = match parse_exec_body(&first) {
		Ok(body) => body,
		Err(err) => {
			let _ = send_error(&mut socket, err).await;
			let _ = socket.close().await;
			return;
		},
	};
	let request = match validation::validate_exec(&body) {
		Ok(request) => request,
		Err(err) => {
			let _ = send_error(&mut socket, err).await;
			let _ = socket.close().await;
			return;
		},
	};
	let stream = match tokio::task::spawn_blocking(move || engine.exec_stream(&id, request)).await {
		Ok(Ok(stream)) => stream,
		Ok(Err(err)) => {
			let _ = send_error(&mut socket, err.into()).await;
			let _ = socket.close().await;
			return;
		},
		Err(err) => {
			let _ = send_error(&mut socket, super::error::join_error(err)).await;
			let _ = socket.close().await;
			return;
		},
	};
	pump_exec_socket(stream, socket).await;
}

pub async fn shell_socket(engine: Arc<dyn EngineApi>, params: Value, mut socket: WebSocket) {
	let params = if is_empty_object(&params) {
		if let Some(text) = receive_text(&mut socket).await {
			match serde_json::from_str::<Value>(&text) {
				Ok(value) if value.is_object() => value,
				Ok(_) => {
					let _ = send_error(&mut socket, ApiError::invalid("invalid shell request")).await;
					let _ = socket.close().await;
					return;
				},
				Err(err) => {
					let _ = send_json(
						&mut socket,
						&json!({"error": {"code": "invalid", "message": format!("invalid shell request: {err}")}}),
					)
					.await;
					let _ = socket.close().await;
					return;
				},
			}
		} else {
			let _ = socket.close().await;
			return;
		}
	} else {
		params
	};
	let engine_for_start = engine.clone();
	let session =
		match tokio::task::spawn_blocking(move || engine_for_start.shell_start(params)).await {
			Ok(Ok(session)) => session,
			Ok(Err(err)) => {
				let _ = send_error(&mut socket, err.into()).await;
				let _ = socket.close().await;
				return;
			},
			Err(err) => {
				let _ = send_error(&mut socket, super::error::join_error(err)).await;
				let _ = socket.close().await;
				return;
			},
		};
	let name = session.name.clone();
	let ephemeral = session.ephemeral;
	if send_json(&mut socket, &json!({"ready": name}))
		.await
		.is_err()
	{
		if ephemeral {
			engine.shell_cleanup(&session.name);
		}
		return;
	}
	let cleanup_name = session.name.clone();
	pump_exec_socket(session.stream, socket).await;
	if ephemeral {
		engine.shell_cleanup(&cleanup_name);
	}
}

pub async fn attach_socket(engine: Arc<dyn EngineApi>, id: String, mut socket: WebSocket) {
	let logs = match tokio::task::spawn_blocking(move || engine.logs_follow(&id)).await {
		Ok(Ok(logs)) => logs,
		Ok(Err(err)) => {
			let _ = send_error(&mut socket, err.into()).await;
			let _ = socket.close().await;
			return;
		},
		Err(err) => {
			let _ = send_error(&mut socket, super::error::join_error(err)).await;
			let _ = socket.close().await;
			return;
		},
	};
	let (tx, mut rx) = mpsc::channel::<Vec<u8>>(32);
	std::thread::spawn(move || {
		while let Ok(chunk) = logs.recv() {
			if tx.blocking_send(chunk).is_err() {
				break;
			}
		}
	});
	while let Some(chunk) = rx.recv().await {
		if send_json(&mut socket, &json!({"stream": "console", "b64": B64.encode(chunk)}))
			.await
			.is_err()
		{
			return;
		}
	}
	let _ = socket.close().await;
}

pub async fn proxy_websocket(
	mut public: WebSocket,
	target: (String, u16),
	rest: String,
	query: String,
) {
	let Ok(mut guest) = TcpStream::connect((target.0.as_str(), target.1)).await else {
		let _ = public.close().await;
		return;
	};
	if websocket_client_handshake(&mut guest, &target.0, target.1, &rest, &query)
		.await
		.is_err()
	{
		let _ = public.close().await;
		return;
	}
	let (mut public_sender, mut public_receiver) = public.split();
	let (mut guest_reader, mut guest_writer) = guest.into_split();
	let client_to_guest = async move {
		while let Some(message) = public_receiver.next().await {
			match message {
				Ok(Message::Text(text)) => {
					guest_writer
						.write_all(&encode_ws_frame(1, text.as_bytes()))
						.await?;
				},
				Ok(Message::Binary(bytes)) => {
					guest_writer.write_all(&encode_ws_frame(2, &bytes)).await?;
				},
				Ok(Message::Close(_)) | Err(_) => {
					guest_writer.write_all(&encode_ws_frame(8, &[])).await?;
					break;
				},
				Ok(Message::Ping(bytes)) => {
					guest_writer.write_all(&encode_ws_frame(9, &bytes)).await?;
				},
				Ok(Message::Pong(bytes)) => {
					guest_writer.write_all(&encode_ws_frame(10, &bytes)).await?;
				},
			}
			guest_writer.flush().await?;
		}
		Ok::<(), std::io::Error>(())
	};
	let guest_to_client = async move {
		loop {
			let (opcode, payload) = read_ws_frame(&mut guest_reader).await?;
			match opcode {
				1 => public_sender
					.send(Message::Text(String::from_utf8_lossy(&payload).into_owned().into()))
					.await
					.map_err(|err| std::io::Error::new(std::io::ErrorKind::BrokenPipe, err))?,
				2 => public_sender
					.send(Message::Binary(payload.into()))
					.await
					.map_err(|err| std::io::Error::new(std::io::ErrorKind::BrokenPipe, err))?,
				8 => {
					let _ = public_sender.send(Message::Close(None)).await;
					break;
				},
				9 => public_sender
					.send(Message::Pong(payload.into()))
					.await
					.map_err(|err| std::io::Error::new(std::io::ErrorKind::BrokenPipe, err))?,
				_ => {},
			}
		}
		Ok::<(), std::io::Error>(())
	};
	tokio::select! {
		result = client_to_guest => drop(result),
		result = guest_to_client => drop(result),
	}
}

pub fn shell_params_from_query(params: &HashMap<String, String>) -> Value {
	let mut object = serde_json::Map::new();
	for (key, value) in params {
		object.insert(key.clone(), Value::String(value.clone()));
	}
	if let Some(command) = object.remove("command")
		&& !object.contains_key("cmd")
	{
		object
			.insert("cmd".to_owned(), json!(["/bin/sh", "-c", command.as_str().unwrap_or_default()]));
	}
	for key in ["cmd", "env"] {
		if let Some(Value::String(text)) = object.get(key)
			&& let Ok(value) = serde_json::from_str::<Value>(text)
		{
			object.insert(key.to_owned(), value);
		}
	}
	for key in ["mem", "cpus", "disk_mb"] {
		if let Some(Value::String(text)) = object.get(key)
			&& let Ok(value) = text.parse::<i64>()
		{
			object.insert(key.to_owned(), json!(value));
		}
	}
	if let Some(Value::String(text)) = object.get("timeout")
		&& let Ok(value) = text.parse::<f64>()
	{
		object.insert("timeout".to_owned(), json!(value));
	}
	if let Some(pty) = object.remove("pty") {
		let enabled = pty
			.as_str()
			.is_none_or(|text| !matches!(text.to_ascii_lowercase().as_str(), "0" | "false" | "no"));
		object.insert("tty".to_owned(), json!(enabled));
	}
	Value::Object(object)
}

async fn pump_exec_socket(stream: ExecStream, socket: WebSocket) {
	let ExecStream { mut control, stdout, stderr, exit } = stream;
	let (mut sender, mut receiver) = socket.split();
	let (events_tx, mut events_rx) = mpsc::channel::<Value>(32);
	let stdout_forward = spawn_stream_forward(stdout, "stdout", events_tx.clone());
	let stderr_forward = spawn_stream_forward(stderr, "stderr", events_tx.clone());
	spawn_exit_forward(exit, [stdout_forward, stderr_forward], events_tx.clone());
	let output = async move {
		while let Some(frame) = events_rx.recv().await {
			let is_exit = frame.get("exit").is_some();
			if sender
				.send(Message::Text(frame.to_string().into()))
				.await
				.is_err()
			{
				break;
			}
			if is_exit {
				break;
			}
		}
		let _ = sender.send(Message::Close(None)).await;
	};
	let input_tx = events_tx.clone();
	let input = async move {
		while let Some(message) = receiver.next().await {
			let text = match message {
				Ok(Message::Text(text)) => text.to_string(),
				Ok(Message::Binary(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
				Ok(Message::Close(_)) | Err(_) => {
					let _ = control.kill(15);
					break;
				},
				Ok(Message::Ping(_) | Message::Pong(_)) => continue,
			};
			match parse_client_exec_frame(&text) {
				Ok(ClientExecFrame::Stdin(data)) => {
					if let Err(err) = control.write_stdin(&data) {
						let _ = input_tx.send(error_value(err.into())).await;
						break;
					}
				},
				Ok(ClientExecFrame::Eof) => {
					if let Err(err) = control.close_stdin() {
						let _ = input_tx.send(error_value(err.into())).await;
						break;
					}
				},
				Ok(ClientExecFrame::Resize(rows, cols)) => {
					if let Err(err) = control.resize(rows, cols) {
						let _ = input_tx.send(error_value(err.into())).await;
						break;
					}
				},
				Ok(ClientExecFrame::Ignored) => {},
				Err(err) => {
					let _ = input_tx.send(error_value(err)).await;
					break;
				},
			}
		}
	};
	tokio::select! {
		() = output => {},
		() = input => {},
	}
}

fn spawn_stream_forward(
	rx: flume::Receiver<Vec<u8>>,
	name: &'static str,
	tx: mpsc::Sender<Value>,
) -> JoinHandle<()> {
	thread::spawn(move || {
		while let Ok(chunk) = rx.recv() {
			if tx.blocking_send(encode_stream_frame(name, &chunk)).is_err() {
				break;
			}
		}
	})
}

fn spawn_exit_forward(
	rx: flume::Receiver<ExecExit>,
	streams: [JoinHandle<()>; 2],
	tx: mpsc::Sender<Value>,
) {
	thread::spawn(move || {
		let exit = rx.recv();
		for stream in streams {
			let _ = stream.join();
		}
		if let Ok(exit) = exit {
			let _ = tx.blocking_send(encode_exit_frame(exit));
		}
	});
}

async fn receive_text(socket: &mut WebSocket) -> Option<String> {
	while let Some(message) = socket.recv().await {
		match message.ok()? {
			Message::Text(text) => return Some(text.to_string()),
			Message::Binary(bytes) => return Some(String::from_utf8_lossy(&bytes).into_owned()),
			Message::Close(_) => return None,
			Message::Ping(_) | Message::Pong(_) => {},
		}
	}
	None
}

fn parse_exec_body(text: &str) -> ApiResult<ExecBody> {
	let mut value: Value =
		serde_json::from_str(text).map_err(|_| ApiError::invalid("invalid exec request"))?;
	if value.get("exec").is_some_and(Value::is_object)
		&& let Some(exec) = value.get_mut("exec")
	{
		value = exec.take();
	}
	serde_json::from_value(value).map_err(|_| ApiError::invalid("invalid exec request"))
}

async fn send_error(socket: &mut WebSocket, err: ApiError) -> Result<(), axum::Error> {
	send_json(socket, &error_value(err)).await
}

async fn send_json(socket: &mut WebSocket, value: &Value) -> Result<(), axum::Error> {
	socket.send(Message::Text(value.to_string().into())).await
}

fn error_value(err: ApiError) -> Value {
	json!({"error": {"code": err.code(), "message": err.message()}})
}

fn is_empty_object(value: &Value) -> bool {
	value.as_object().is_none_or(serde_json::Map::is_empty)
}

fn resize_dim(value: &Value) -> ApiResult<u16> {
	let Some(raw) = value.as_u64() else {
		return Err(ApiError::invalid("resize must be [rows, cols]"));
	};
	u16::try_from(raw)
		.ok()
		.filter(|dim| *dim > 0)
		.ok_or_else(|| ApiError::invalid("resize must be [rows, cols]"))
}

async fn websocket_client_handshake(
	stream: &mut TcpStream,
	host: &str,
	port: u16,
	rest: &str,
	query: &str,
) -> std::io::Result<()> {
	let key = B64.encode(rand::random::<[u8; 16]>());
	let mut path = format!("/{}", rest.trim_start_matches('/'));
	if !query.is_empty() {
		path.push('?');
		path.push_str(query);
	}
	let request = format!(
		"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: \
		 Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
	);
	stream.write_all(request.as_bytes()).await?;
	stream.flush().await?;
	let mut response = Vec::new();
	let mut buf = [0_u8; 1024];
	while !response.windows(4).any(|window| window == b"\r\n\r\n") {
		let read = stream.read(&mut buf).await?;
		if read == 0 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::UnexpectedEof,
				"websocket upgrade closed",
			));
		}
		response.extend_from_slice(&buf[..read]);
	}
	let accept = B64.encode(Sha1::digest(format!("{key}{WS_GUID}").as_bytes()));
	let header = String::from_utf8_lossy(&response);
	if !header.starts_with("HTTP/1.1 101") && !header.starts_with("HTTP/1.0 101") {
		return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "websocket upgrade failed"));
	}
	if !header.contains(&accept) {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"websocket accept mismatch",
		));
	}
	Ok(())
}

fn encode_ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
	let mut out = vec![0b1000_0000 | (opcode & 0x0f)];
	let len = payload.len();
	if len < 126 {
		out.push(0b1000_0000 | len as u8);
	} else if len <= 0xffff {
		out.push(0b1000_0000 | 0b0111_1110);
		out.extend_from_slice(&(len as u16).to_be_bytes());
	} else {
		out.push(0b1000_0000 | 127);
		out.extend_from_slice(&(len as u64).to_be_bytes());
	}
	let mask = rand::random::<[u8; 4]>();
	out.extend_from_slice(&mask);
	for (idx, byte) in payload.iter().enumerate() {
		out.push(byte ^ mask[idx % mask.len()]);
	}
	out
}

async fn read_ws_frame<R>(reader: &mut R) -> std::io::Result<(u8, Vec<u8>)>
where
	R: AsyncRead + Unpin,
{
	let mut head = [0_u8; 2];
	reader.read_exact(&mut head).await?;
	let opcode = head[0] & 0x0f;
	let masked = head[1] & 0x80 != 0;
	let mut len = u64::from(head[1] & 0x7f);
	if len == 126 {
		let mut buf = [0_u8; 2];
		reader.read_exact(&mut buf).await?;
		len = u64::from(u16::from_be_bytes(buf));
	} else if len == 127 {
		let mut buf = [0_u8; 8];
		reader.read_exact(&mut buf).await?;
		len = u64::from_be_bytes(buf);
	}
	let mut mask = [0_u8; 4];
	if masked {
		reader.read_exact(&mut mask).await?;
	}
	let len = usize::try_from(len).map_err(|_| {
		std::io::Error::new(std::io::ErrorKind::InvalidData, "websocket frame too large")
	})?;
	let mut payload = vec![0_u8; len];
	if len > 0 {
		reader.read_exact(&mut payload).await?;
	}
	if masked {
		for (idx, byte) in payload.iter_mut().enumerate() {
			*byte ^= mask[idx % mask.len()];
		}
	}
	Ok((opcode, payload))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_exec_control_frames() {
		match parse_client_exec_frame(r#"{"stdin_b64":"aGk="}"#).unwrap() {
			ClientExecFrame::Stdin(data) => assert_eq!(data, b"hi"),
			other => panic!("unexpected frame: {other:?}"),
		}
		assert!(matches!(parse_client_exec_frame(r#"{"eof":true}"#).unwrap(), ClientExecFrame::Eof));
		assert!(matches!(
			parse_client_exec_frame(r#"{"resize":[24,80]}"#).unwrap(),
			ClientExecFrame::Resize(24, 80)
		));
	}

	#[test]
	fn encodes_stream_and_exit_frames() {
		assert_eq!(encode_stream_frame("stdout", b"ok"), json!({"stream":"stdout","b64":"b2s="}));
		assert_eq!(
			encode_exit_frame(ExecExit { code: 7, signal: None }),
			json!({"exit":7,"signal":null})
		);
	}

	#[test]
	fn exit_frame_follows_all_stream_frames() {
		let (stdout_tx, stdout_rx) = flume::unbounded();
		let (stderr_tx, stderr_rx) = flume::unbounded();
		let (exit_tx, exit_rx) = flume::bounded(1);
		let (events_tx, mut events_rx) = mpsc::channel(4);
		let streams = [
			spawn_stream_forward(stdout_rx, "stdout", events_tx.clone()),
			spawn_stream_forward(stderr_rx, "stderr", events_tx.clone()),
		];
		spawn_exit_forward(exit_rx, streams, events_tx);

		stdout_tx.send(b"out".to_vec()).unwrap();
		stderr_tx.send(b"err".to_vec()).unwrap();
		drop(stdout_tx);
		drop(stderr_tx);
		exit_tx.send(ExecExit { code: 0, signal: None }).unwrap();

		let frames = [
			events_rx.blocking_recv().unwrap(),
			events_rx.blocking_recv().unwrap(),
			events_rx.blocking_recv().unwrap(),
		];
		assert!(
			frames[..2]
				.iter()
				.any(|frame| frame.get("stream") == Some(&json!("stdout")))
		);
		assert!(
			frames[..2]
				.iter()
				.any(|frame| frame.get("stream") == Some(&json!("stderr")))
		);
		assert_eq!(frames[2], json!({"exit": 0, "signal": null}));
	}
}
