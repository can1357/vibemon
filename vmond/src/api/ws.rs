//! Raw WebSocket tunnel for the sandbox ports proxy
//! (`GET /v1/sandboxes/{id}/ports/{port}/ws/...`).
//!
//! This is an opaque byte relay between the public axum WebSocket and a plain
//! RFC-6455 client connection into the guest — text/binary/close/ping/pong
//! opcodes are preserved verbatim. The interactive exec/shell/attach API is
//! gRPC (see `grpc.rs`), reachable from browsers via the `/grpc` bridge
//! (`bridge.rs`).

use axum::extract::ws::{Message, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use futures_util::{SinkExt, StreamExt};
use sha1::{Digest, Sha1};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

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

pub(super) async fn websocket_client_handshake(
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

pub(super) fn encode_ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
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

pub(super) async fn read_ws_frame<R>(reader: &mut R) -> std::io::Result<(u8, Vec<u8>)>
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
