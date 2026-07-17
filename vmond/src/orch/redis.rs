//! Minimal async RESP2 Redis client for the orchestration layer.
//!
//! Hand-rolled on purpose: the orch layer needs eleven commands, and the
//! server core should not grow a general Redis dependency tree for them.
//! One [`Redis`] handle multiplexes request/response commands behind a
//! mutex (control-plane volume only — never the sandbox create hot path);
//! [`StreamFollower`] owns a dedicated connection because blocking `XREAD`
//! must not stall unrelated commands.
//!
//! Compatible with real Redis (`redis://[:password@]host:port[/db]`) and the
//! in-process [`super::miniredis`] subset used by tests and `--embed-redis`.

use std::{sync::Arc, time::Duration};

use tokio::{
	io::{AsyncReadExt, AsyncWriteExt, BufReader},
	net::{
		TcpStream,
		tcp::{OwnedReadHalf, OwnedWriteHalf},
	},
	sync::Mutex,
	time::timeout,
};

use crate::{EngineError, Result};

/// Deadline applied to every non-blocking command round trip.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
/// Additional slack granted to `XREAD BLOCK` beyond its server-side block.
const BLOCK_SLACK: Duration = Duration::from_secs(5);

/// One decoded RESP2 reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resp {
	/// `+OK` style simple string.
	Simple(String),
	/// `:1` integer.
	Int(i64),
	/// `$n` bulk string payload.
	Bulk(Vec<u8>),
	/// `$-1` / `*-1` null.
	Null,
	/// `*n` array.
	Array(Vec<Self>),
}

impl Resp {
	/// Bulk or simple payload as owned bytes.
	pub fn into_bytes(self) -> Option<Vec<u8>> {
		match self {
			Self::Bulk(bytes) => Some(bytes),
			Self::Simple(text) => Some(text.into_bytes()),
			_ => None,
		}
	}

	/// Bulk or simple payload as UTF-8 text.
	pub fn into_string(self) -> Option<String> {
		self
			.into_bytes()
			.and_then(|bytes| String::from_utf8(bytes).ok())
	}

	fn into_array(self) -> Option<Vec<Self>> {
		match self {
			Self::Array(items) => Some(items),
			_ => None,
		}
	}
}

/// Parsed `redis://` endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisAddr {
	/// `host:port` dial target.
	pub authority: String,
	/// Optional `AUTH` password.
	pub password:  Option<String>,
}

impl RedisAddr {
	/// Parse `redis://[:password@]host[:port][/db]`; bare `host:port` is also
	/// accepted. Only database 0 is supported.
	pub fn parse(url: &str) -> Result<Self> {
		let rest = url.strip_prefix("redis://").unwrap_or(url);
		let (auth, hostpart) = match rest.rsplit_once('@') {
			Some((auth, host)) => (Some(auth), host),
			None => (None, rest),
		};
		let password = auth.map(|auth| {
			auth
				.split_once(':')
				.map_or(auth, |(_user, password)| password)
				.to_owned()
		});
		let hostpart = hostpart.split('/').next().unwrap_or(hostpart);
		if hostpart.is_empty() {
			return Err(EngineError::invalid(format!("invalid redis url: {url}")));
		}
		let authority = if hostpart.contains(':') {
			hostpart.to_owned()
		} else {
			format!("{hostpart}:6379")
		};
		Ok(Self { authority, password: password.filter(|password| !password.is_empty()) })
	}

	async fn dial(&self) -> Result<Conn> {
		let stream = TcpStream::connect(&self.authority).await.map_err(|error| {
			EngineError::engine(format!("redis connect {}: {error}", self.authority))
		})?;
		stream.set_nodelay(true).ok();
		let (read, write) = stream.into_split();
		let mut conn = Conn { read: BufReader::new(read), write };
		if let Some(password) = &self.password {
			let reply = conn.roundtrip(&[b"AUTH", password.as_bytes()]).await?;
			if !matches!(reply, Resp::Simple(_)) {
				return Err(EngineError::unauthorized("redis AUTH rejected"));
			}
		}
		Ok(conn)
	}
}

struct Conn {
	read:  BufReader<OwnedReadHalf>,
	write: OwnedWriteHalf,
}

impl Conn {
	async fn roundtrip(&mut self, args: &[&[u8]]) -> Result<Resp> {
		self.send(args).await?;
		self.receive().await
	}

	async fn set_px_pipeline(&mut self, entries: &[(String, Vec<u8>)], px: &[u8]) -> Result<()> {
		for (key, value) in entries {
			self
				.send(&[b"SET", key.as_bytes(), value, b"PX", px])
				.await?;
		}

		let mut redis_error = None;
		for _ in entries {
			match self.receive().await {
				Ok(_) => {},
				Err(error) if error.message.starts_with("redis error:") => {
					redis_error.get_or_insert(error);
				},
				Err(error) => return Err(error),
			}
		}
		redis_error.map_or(Ok(()), Err)
	}

	async fn send(&mut self, args: &[&[u8]]) -> Result<Resp> {
		let mut frame = Vec::with_capacity(args.iter().map(|arg| arg.len() + 16).sum());
		frame.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
		for arg in args {
			frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
			frame.extend_from_slice(arg);
			frame.extend_from_slice(b"\r\n");
		}
		self
			.write
			.write_all(&frame)
			.await
			.map_err(|error| EngineError::engine(format!("redis write: {error}")))?;
		Ok(Resp::Null)
	}

	async fn receive(&mut self) -> Result<Resp> {
		let line = self.read_line().await?;
		let (kind, rest) = line.split_at(1);
		match kind {
			"+" => Ok(Resp::Simple(rest.to_owned())),
			"-" => Err(EngineError::engine(format!("redis error: {rest}"))),
			":" => Ok(Resp::Int(parse_int(rest)?)),
			"$" => {
				let len = parse_int(rest)?;
				if len < 0 {
					return Ok(Resp::Null);
				}
				let mut payload = vec![0_u8; len as usize + 2];
				self
					.read
					.read_exact(&mut payload)
					.await
					.map_err(|error| EngineError::engine(format!("redis read: {error}")))?;
				payload.truncate(len as usize);
				Ok(Resp::Bulk(payload))
			},
			"*" => {
				let len = parse_int(rest)?;
				if len < 0 {
					return Ok(Resp::Null);
				}
				let mut items = Vec::with_capacity(len as usize);
				for _ in 0..len {
					items.push(Box::pin(self.receive()).await?);
				}
				Ok(Resp::Array(items))
			},
			other => Err(EngineError::engine(format!("redis protocol: unexpected type {other:?}"))),
		}
	}

	async fn read_line(&mut self) -> Result<String> {
		let mut line = Vec::with_capacity(32);
		loop {
			let byte = self
				.read
				.read_u8()
				.await
				.map_err(|error| EngineError::engine(format!("redis read: {error}")))?;
			if byte == b'\n' {
				if line.last() == Some(&b'\r') {
					line.pop();
				}
				return String::from_utf8(line)
					.map_err(|_| EngineError::engine("redis protocol: non-utf8 line"));
			}
			line.push(byte);
		}
	}
}

fn parse_int(text: &str) -> Result<i64> {
	text
		.parse()
		.map_err(|_| EngineError::engine(format!("redis protocol: bad integer {text:?}")))
}

/// Cheap-to-clone command handle with one lazily-connected shared connection.
///
/// Every command is bounded by [`COMMAND_TIMEOUT`] and retried once on a
/// transport error (fresh connection), so callers never wedge on a dead
/// Redis and transient blips self-heal.
#[derive(Clone)]
pub struct Redis {
	addr: Arc<RedisAddr>,
	conn: Arc<Mutex<Option<Conn>>>,
}

impl Redis {
	/// Build a handle from a `redis://` URL. No I/O happens until first use.
	pub fn new(url: &str) -> Result<Self> {
		Ok(Self { addr: Arc::new(RedisAddr::parse(url)?), conn: Arc::new(Mutex::new(None)) })
	}

	/// The parsed endpoint (for follower connections and diagnostics).
	pub fn addr(&self) -> &RedisAddr {
		&self.addr
	}

	/// Run one command; transport failures retry once on a new connection.
	pub async fn call(&self, args: &[&[u8]]) -> Result<Resp> {
		let mut guard = self.conn.lock().await;
		let mut last_error = EngineError::engine("redis: command failed");
		for _attempt in 0..2 {
			if guard.is_none() {
				*guard = Some(self.addr.dial().await?);
			}
			let conn = guard.as_mut().expect("connection just installed");
			match timeout(COMMAND_TIMEOUT, conn.roundtrip(args)).await {
				Ok(Ok(reply)) => return Ok(reply),
				// Redis-level errors (`-ERR`) are deterministic, not
				// transport failures; never retry them.
				Ok(Err(error)) if error.message.starts_with("redis error:") => {
					return Err(error);
				},
				Ok(Err(error)) => {
					*guard = None;
					last_error = error;
				},
				Err(_) => {
					*guard = None;
					last_error = EngineError::engine("redis: command timed out");
				},
			}
		}
		Err(last_error)
	}

	/// `PING` health probe.
	pub async fn ping(&self) -> Result<()> {
		self.call(&[b"PING"]).await.map(drop)
	}

	/// `GET key`.
	pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
		Ok(self.call(&[b"GET", key.as_bytes()]).await?.into_bytes())
	}

	/// `SET key value PX px`.
	pub async fn set_px(&self, key: &str, value: &[u8], px: u64) -> Result<()> {
		let px = px.to_string();
		self
			.call(&[b"SET", key.as_bytes(), value, b"PX", px.as_bytes()])
			.await
			.map(drop)
	}

	/// Pipeline a bounded batch of `SET key value PX px` commands.
	///
	/// All commands are written before any replies are read. Transport failures
	/// reconnect and retry the whole (idempotent) batch once.
	pub(crate) async fn set_px_pipeline(
		&self,
		entries: &[(String, Vec<u8>)],
		px: u64,
	) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}
		let px = px.to_string();
		let mut guard = self.conn.lock().await;
		let mut last_error = EngineError::engine("redis: pipeline failed");
		for _attempt in 0..2 {
			if guard.is_none() {
				*guard = Some(self.addr.dial().await?);
			}
			let conn = guard.as_mut().expect("connection just installed");
			match timeout(COMMAND_TIMEOUT, conn.set_px_pipeline(entries, px.as_bytes())).await {
				Ok(Ok(())) => return Ok(()),
				Ok(Err(error)) if error.message.starts_with("redis error:") => return Err(error),
				Ok(Err(error)) => {
					*guard = None;
					last_error = error;
				},
				Err(_) => {
					*guard = None;
					last_error = EngineError::engine("redis: pipeline timed out");
				},
			}
		}
		Err(last_error)
	}

	/// `SET key value PX px NX`; true when the key was newly set.
	pub async fn set_nx_px(&self, key: &str, value: &[u8], px: u64) -> Result<bool> {
		let px = px.to_string();
		let reply = self
			.call(&[b"SET", key.as_bytes(), value, b"PX", px.as_bytes(), b"NX"])
			.await?;
		Ok(matches!(reply, Resp::Simple(_)))
	}

	/// `DEL key`.
	pub async fn del(&self, key: &str) -> Result<()> {
		self.call(&[b"DEL", key.as_bytes()]).await.map(drop)
	}

	/// `INCRBY key delta`; returns the new value.
	pub async fn incr_by(&self, key: &str, delta: u64) -> Result<i64> {
		let delta = delta.to_string();
		match self
			.call(&[b"INCRBY", key.as_bytes(), delta.as_bytes()])
			.await?
		{
			Resp::Int(value) => Ok(value),
			other => Err(EngineError::engine(format!("unexpected INCRBY reply: {other:?}"))),
		}
	}

	/// `PEXPIRE key ms`.
	pub async fn pexpire(&self, key: &str, ms: u64) -> Result<()> {
		let ms = ms.to_string();
		self
			.call(&[b"PEXPIRE", key.as_bytes(), ms.as_bytes()])
			.await
			.map(drop)
	}

	/// Full cursor walk of `SCAN 0 MATCH {prefix}* COUNT 512`.
	pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
		let pattern = format!("{prefix}*");
		let mut cursor = "0".to_owned();
		let mut keys = Vec::new();
		loop {
			let reply = self
				.call(&[b"SCAN", cursor.as_bytes(), b"MATCH", pattern.as_bytes(), b"COUNT", b"512"])
				.await?;
			let mut parts = reply
				.into_array()
				.ok_or_else(|| EngineError::engine("redis: malformed SCAN reply"))?
				.into_iter();
			cursor = parts
				.next()
				.and_then(Resp::into_string)
				.ok_or_else(|| EngineError::engine("redis: malformed SCAN cursor"))?;
			if let Some(batch) = parts.next().and_then(Resp::into_array) {
				keys.extend(batch.into_iter().filter_map(Resp::into_string));
			}
			if cursor == "0" {
				return Ok(keys);
			}
		}
	}

	/// `XADD stream MAXLEN ~ maxlen * d payload`; returns the entry id.
	pub async fn xadd(&self, stream: &str, maxlen: u64, payload: &[u8]) -> Result<String> {
		let maxlen = maxlen.to_string();
		let reply = self
			.call(&[
				b"XADD",
				stream.as_bytes(),
				b"MAXLEN",
				b"~",
				maxlen.as_bytes(),
				b"*",
				b"d",
				payload,
			])
			.await?;
		reply
			.into_string()
			.ok_or_else(|| EngineError::engine("redis: malformed XADD reply"))
	}
}

/// Dedicated `XREAD BLOCK` follower over one stream.
///
/// Owns its own connection (a blocked read must not serialize behind the
/// shared [`Redis`] handle) and reconnects transparently; entries published
/// before the follower starts are skipped (`$`) because live state is
/// bootstrapped from the self-expiring worker keys instead.
pub struct StreamFollower {
	addr:    Arc<RedisAddr>,
	stream:  String,
	conn:    Option<Conn>,
	last_id: String,
}

impl StreamFollower {
	/// Follow `stream` starting from new entries only.
	pub fn new(redis: &Redis, stream: &str) -> Self {
		Self {
			addr:    Arc::clone(&redis.addr),
			stream:  stream.to_owned(),
			conn:    None,
			last_id: "$".to_owned(),
		}
	}

	/// Wait up to `block_ms` for new entries; returns their `d` payloads.
	/// Transport errors surface after resetting the connection, so callers
	/// can sleep and retry without special cases.
	pub async fn next_batch(&mut self, block_ms: u64) -> Result<Vec<Vec<u8>>> {
		if self.conn.is_none() {
			self.conn = Some(self.addr.dial().await?);
			// A reconnect may have missed entries; restart from new ones.
			// Callers re-bootstrap from keys on follower errors.
			if self.last_id.is_empty() {
				"$".clone_into(&mut self.last_id);
			}
		}
		let conn = self.conn.as_mut().expect("connection just installed");
		let block = block_ms.to_string();
		let outcome = timeout(Duration::from_millis(block_ms) + BLOCK_SLACK, async {
			conn
				.roundtrip(&[
					b"XREAD",
					b"BLOCK",
					block.as_bytes(),
					b"STREAMS",
					self.stream.as_bytes(),
					self.last_id.as_bytes(),
				])
				.await
		})
		.await;
		let reply = match outcome {
			Ok(Ok(reply)) => reply,
			Ok(Err(error)) => {
				self.conn = None;
				return Err(error);
			},
			Err(_) => {
				self.conn = None;
				return Err(EngineError::engine("redis: XREAD timed out"));
			},
		};
		let mut payloads = Vec::new();
		let Some(streams) = reply.into_array() else {
			return Ok(payloads); // null: block elapsed with no entries
		};
		for stream in streams {
			let Some(mut stream_parts) = stream.into_array() else {
				continue;
			};
			if stream_parts.len() != 2 {
				continue;
			}
			let entries = stream_parts
				.pop()
				.and_then(Resp::into_array)
				.unwrap_or_default();
			for entry in entries {
				let Some(mut entry_parts) = entry.into_array() else {
					continue;
				};
				if entry_parts.len() != 2 {
					continue;
				}
				let fields = entry_parts
					.pop()
					.and_then(Resp::into_array)
					.unwrap_or_default();
				if let Some(id) = entry_parts.pop().and_then(Resp::into_string) {
					self.last_id = id;
				}
				let mut fields = fields.into_iter();
				while let (Some(name), Some(value)) = (fields.next(), fields.next()) {
					if name.into_bytes().as_deref() == Some(b"d")
						&& let Some(payload) = value.into_bytes()
					{
						payloads.push(payload);
					}
				}
			}
		}
		Ok(payloads)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn set_px_pipeline_persists_every_entry() {
		let server = super::super::miniredis::MiniRedis::spawn()
			.await
			.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let entries = vec![
			("route:a".to_owned(), b"worker-a".to_vec()),
			("route:b".to_owned(), b"worker-b".to_vec()),
			("route:c".to_owned(), b"worker-c".to_vec()),
		];

		redis
			.set_px_pipeline(&entries, 60_000)
			.await
			.expect("pipeline");

		for (key, value) in entries {
			assert_eq!(redis.get(&key).await.expect("get"), Some(value));
		}
	}

	#[tokio::test]
	async fn set_px_pipeline_applies_ttls() {
		let server = super::super::miniredis::MiniRedis::spawn()
			.await
			.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let entries = vec![("ttl:a".to_owned(), b"1".to_vec()), ("ttl:b".to_owned(), b"2".to_vec())];

		redis.set_px_pipeline(&entries, 25).await.expect("pipeline");

		tokio::time::sleep(Duration::from_millis(60)).await;
		for (key, _value) in entries {
			assert_eq!(redis.get(&key).await.expect("get"), None, "{key} must expire");
		}
	}

	#[test]
	fn parses_redis_urls() {
		let plain = RedisAddr::parse("redis://cache.internal:6380").expect("plain");
		assert_eq!(plain.authority, "cache.internal:6380");
		assert_eq!(plain.password, None);

		let defaulted = RedisAddr::parse("redis://cache.internal").expect("default port");
		assert_eq!(defaulted.authority, "cache.internal:6379");

		let auth = RedisAddr::parse("redis://:hunter2@10.0.0.5:7000/0").expect("auth");
		assert_eq!(auth.authority, "10.0.0.5:7000");
		assert_eq!(auth.password.as_deref(), Some("hunter2"));

		let bare = RedisAddr::parse("127.0.0.1:6379").expect("bare");
		assert_eq!(bare.authority, "127.0.0.1:6379");

		assert!(RedisAddr::parse("redis://").is_err());
	}
}
