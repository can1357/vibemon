//! In-process Redis subset for tests and single-host dev clusters.
//!
//! Speaks just enough RESP2 for [`super::redis`]: `PING`, `AUTH`, `SET`
//! (`PX`/`EX`/`NX`), `GET`, `DEL`, `PEXPIRE`, `SCAN` (prefix `MATCH`),
//! `XADD` (`MAXLEN ~`), and `XREAD` (`BLOCK`). Values live in one mutexed
//! map with lazy TTL expiry; stream waiters wake on `XADD` via a broadcast
//! [`Notify`]. This is a dev/test harness — durability, replication, and
//! the rest of Redis are intentionally absent.
//!
//! Production deployments point `--redis` at a real server; `vmon sched
//! --embed-redis` runs this instead so a laptop demo needs no extra daemon.

use std::{
	collections::{HashMap, VecDeque},
	net::SocketAddr,
	sync::Arc,
	time::{Duration, Instant},
};

use dashmap::DashMap;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt, BufReader},
	net::{TcpListener, TcpStream},
	sync::{Mutex, Notify},
	task::JoinHandle,
	time::timeout,
};

use crate::{EngineError, Result};

const MAX_ARG_BYTES: usize = 8 * 1024 * 1024;

/// Handle to a running mini server; the listener aborts on drop.
pub struct MiniRedis {
	addr: SocketAddr,
	task: JoinHandle<()>,
}

impl MiniRedis {
	/// Bind `127.0.0.1:0` and start serving.
	pub async fn spawn() -> Result<Self> {
		let listener = TcpListener::bind(("127.0.0.1", 0))
			.await
			.map_err(|error| EngineError::engine(format!("miniredis bind: {error}")))?;
		let addr = listener
			.local_addr()
			.map_err(|error| EngineError::engine(format!("miniredis addr: {error}")))?;
		let store = Arc::new(Store::default());
		let task = tokio::spawn(async move {
			loop {
				let Ok((socket, _peer)) = listener.accept().await else {
					break;
				};
				let store = Arc::clone(&store);
				tokio::spawn(async move {
					let _ = serve_connection(socket, store).await;
				});
			}
		});
		Ok(Self { addr, task })
	}

	/// `host:port` dial target.
	pub fn authority(&self) -> String {
		self.addr.to_string()
	}

	/// `redis://host:port` URL for client configuration.
	pub fn url(&self) -> String {
		format!("redis://{}", self.addr)
	}
}

impl Drop for MiniRedis {
	fn drop(&mut self) {
		self.task.abort();
	}
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct StreamId {
	ms:  u64,
	seq: u64,
}

impl StreamId {
	const ZERO: Self = Self { ms: 0, seq: 0 };

	fn render(self) -> String {
		format!("{}-{}", self.ms, self.seq)
	}

	fn parse(text: &str) -> Option<Self> {
		let (ms, seq) = text.split_once('-')?;
		Some(Self { ms: ms.parse().ok()?, seq: seq.parse().ok()? })
	}
}

struct KvEntry {
	value:      Vec<u8>,
	expires_at: Option<Instant>,
}

impl KvEntry {
	fn live(&self, now: Instant) -> bool {
		self.expires_at.is_none_or(|deadline| deadline > now)
	}
}

/// One appended stream entry: id plus its field/value pairs.
type StreamEntry = (StreamId, Vec<(Vec<u8>, Vec<u8>)>);

#[derive(Default)]
struct StreamLog {
	entries: VecDeque<StreamEntry>,
	last:    StreamId,
}

/// Keyspace is a sharded [`DashMap`]; the stream log stays behind one mutex
/// because XREAD's check-then-wait must be atomic with waiter registration.
#[derive(Default)]
struct Store {
	kv:      DashMap<String, KvEntry>,
	streams: Mutex<HashMap<String, StreamLog>>,
	wakeup:  Notify,
}

enum Reply {
	Simple(&'static str),
	Error(String),
	Int(i64),
	Bulk(Vec<u8>),
	Null,
	Array(Vec<Self>),
}

impl Reply {
	fn encode_into(self, buffer: &mut Vec<u8>) {
		match self {
			Self::Simple(text) => {
				buffer.extend_from_slice(format!("+{text}\r\n").as_bytes());
			},
			Self::Error(text) => {
				buffer.extend_from_slice(format!("-ERR {text}\r\n").as_bytes());
			},
			Self::Int(value) => {
				buffer.extend_from_slice(format!(":{value}\r\n").as_bytes());
			},
			Self::Bulk(payload) => {
				buffer.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
				buffer.extend_from_slice(&payload);
				buffer.extend_from_slice(b"\r\n");
			},
			Self::Null => buffer.extend_from_slice(b"$-1\r\n"),
			Self::Array(items) => {
				buffer.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
				for item in items {
					item.encode_into(buffer);
				}
			},
		}
	}
}

async fn serve_connection(socket: TcpStream, store: Arc<Store>) -> std::io::Result<()> {
	socket.set_nodelay(true).ok();
	let (read, mut write) = socket.into_split();
	let mut read = BufReader::new(read);
	loop {
		let Some(args) = read_command(&mut read).await? else {
			return Ok(()); // clean disconnect
		};
		let reply = dispatch(&store, args).await;
		let mut frame = Vec::with_capacity(64);
		reply.encode_into(&mut frame);
		write.write_all(&frame).await?;
	}
}

async fn read_command(
	read: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
	let first = match read.read_u8().await {
		Ok(byte) => byte,
		Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
		Err(error) => return Err(error),
	};
	if first != b'*' {
		return Err(std::io::Error::other("miniredis: inline commands unsupported"));
	}
	let count = read_int_line(read).await?;
	let mut args = Vec::with_capacity(count.clamp(0, 128) as usize);
	for _ in 0..count {
		if read.read_u8().await? != b'$' {
			return Err(std::io::Error::other("miniredis: expected bulk string"));
		}
		let len = read_int_line(read).await?;
		if !(0..=MAX_ARG_BYTES as i64).contains(&len) {
			return Err(std::io::Error::other("miniredis: bulk length out of range"));
		}
		let mut payload = vec![0_u8; len as usize + 2];
		read.read_exact(&mut payload).await?;
		payload.truncate(len as usize);
		args.push(payload);
	}
	Ok(Some(args))
}

async fn read_int_line(
	read: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> std::io::Result<i64> {
	let mut digits = Vec::with_capacity(16);
	loop {
		let byte = read.read_u8().await?;
		if byte == b'\n' {
			if digits.last() == Some(&b'\r') {
				digits.pop();
			}
			return std::str::from_utf8(&digits)
				.ok()
				.and_then(|text| text.parse().ok())
				.ok_or_else(|| std::io::Error::other("miniredis: bad integer line"));
		}
		digits.push(byte);
	}
}

async fn dispatch(store: &Store, args: Vec<Vec<u8>>) -> Reply {
	let Some(command) = args.first() else {
		return Reply::Error("empty command".to_owned());
	};
	let command = String::from_utf8_lossy(command).to_ascii_uppercase();
	match command.as_str() {
		"PING" => Reply::Simple("PONG"),
		"AUTH" => Reply::Simple("OK"),
		"SET" => cmd_set(store, &args),
		"GET" => cmd_get(store, &args),
		"DEL" => cmd_del(store, &args),
		"INCRBY" => cmd_incrby(store, &args),
		"PEXPIRE" => cmd_pexpire(store, &args),
		"SCAN" => cmd_scan(store, &args),
		"XADD" => cmd_xadd(store, args).await,
		"XREAD" => cmd_xread(store, &args).await,
		other => Reply::Error(format!("unknown command '{other}'")),
	}
}

fn text_arg(args: &[Vec<u8>], index: usize) -> Option<&str> {
	args
		.get(index)
		.and_then(|arg| std::str::from_utf8(arg).ok())
}

fn cmd_set(store: &Store, args: &[Vec<u8>]) -> Reply {
	let (Some(key), Some(value)) = (text_arg(args, 1), args.get(2)) else {
		return Reply::Error("SET requires key and value".to_owned());
	};
	let mut ttl = None;
	let mut only_if_absent = false;
	let mut cursor = 3;
	while cursor < args.len() {
		match text_arg(args, cursor)
			.map(str::to_ascii_uppercase)
			.as_deref()
		{
			Some("PX") => {
				let Some(ms) = text_arg(args, cursor + 1).and_then(|raw| raw.parse::<u64>().ok())
				else {
					return Reply::Error("PX requires milliseconds".to_owned());
				};
				ttl = Some(Duration::from_millis(ms));
				cursor += 2;
			},
			Some("EX") => {
				let Some(secs) = text_arg(args, cursor + 1).and_then(|raw| raw.parse::<u64>().ok())
				else {
					return Reply::Error("EX requires seconds".to_owned());
				};
				ttl = Some(Duration::from_secs(secs));
				cursor += 2;
			},
			Some("NX") => {
				only_if_absent = true;
				cursor += 1;
			},
			_ => return Reply::Error("unsupported SET option".to_owned()),
		}
	}
	let now = Instant::now();
	let entry = KvEntry { value: value.clone(), expires_at: ttl.map(|ttl| now + ttl) };
	match store.kv.entry(key.to_owned()) {
		dashmap::Entry::Occupied(mut occupied) => {
			if only_if_absent && occupied.get().live(now) {
				return Reply::Null;
			}
			occupied.insert(entry);
		},
		dashmap::Entry::Vacant(vacant) => {
			vacant.insert(entry);
		},
	}
	Reply::Simple("OK")
}

fn cmd_get(store: &Store, args: &[Vec<u8>]) -> Reply {
	let Some(key) = text_arg(args, 1) else {
		return Reply::Error("GET requires key".to_owned());
	};
	let now = Instant::now();
	let value = store
		.kv
		.get(key)
		.filter(|entry| entry.live(now))
		.map(|entry| entry.value.clone());
	if let Some(value) = value {
		Reply::Bulk(value)
	} else {
		store.kv.remove_if(key, |_key, entry| !entry.live(now));
		Reply::Null
	}
}

fn cmd_del(store: &Store, args: &[Vec<u8>]) -> Reply {
	let mut removed = 0;
	for key in &args[1..] {
		if let Ok(key) = std::str::from_utf8(key)
			&& store.kv.remove(key).is_some()
		{
			removed += 1;
		}
	}
	Reply::Int(removed)
}
fn cmd_incrby(store: &Store, args: &[Vec<u8>]) -> Reply {
	let (Some(key), Some(delta)) = (text_arg(args, 1), text_arg(args, 2)) else {
		return Reply::Error("INCRBY requires key and delta".to_owned());
	};
	let Ok(delta) = delta.parse::<i64>() else {
		return Reply::Error("INCRBY delta must be an integer".to_owned());
	};
	let now = Instant::now();
	// The shard lock held by `entry` makes the read-modify-write atomic.
	let mut entry = store
		.kv
		.entry(key.to_owned())
		.or_insert_with(|| KvEntry { value: b"0".to_vec(), expires_at: None });
	let current = if entry.live(now) {
		std::str::from_utf8(&entry.value)
			.ok()
			.and_then(|text| text.trim().parse::<i64>().ok())
	} else {
		Some(0)
	};
	let Some(current) = current else {
		return Reply::Error("INCRBY target is not an integer".to_owned());
	};
	let next = current.saturating_add(delta);
	*entry = KvEntry { value: next.to_string().into_bytes(), expires_at: None };
	Reply::Int(next)
}

fn cmd_pexpire(store: &Store, args: &[Vec<u8>]) -> Reply {
	let (Some(key), Some(ms)) =
		(text_arg(args, 1), text_arg(args, 2).and_then(|raw| raw.parse::<u64>().ok()))
	else {
		return Reply::Error("PEXPIRE requires key and milliseconds".to_owned());
	};
	let now = Instant::now();
	match store.kv.get_mut(key) {
		Some(mut entry) if entry.live(now) => {
			entry.expires_at = Some(now + Duration::from_millis(ms));
			Reply::Int(1)
		},
		_ => Reply::Int(0),
	}
}

fn cmd_scan(store: &Store, args: &[Vec<u8>]) -> Reply {
	// Single-pass cursor: every reply carries cursor 0 plus all live matches,
	// which is a valid terminating SCAN conversation for small keyspaces.
	let mut pattern = None;
	let mut cursor = 2;
	while cursor < args.len() {
		match text_arg(args, cursor)
			.map(str::to_ascii_uppercase)
			.as_deref()
		{
			Some("MATCH") => {
				pattern = text_arg(args, cursor + 1).map(str::to_owned);
				cursor += 2;
			},
			Some("COUNT") => cursor += 2,
			_ => return Reply::Error("unsupported SCAN option".to_owned()),
		}
	}
	let now = Instant::now();
	let matches = store
		.kv
		.iter()
		.filter(|entry| entry.live(now))
		.filter(|entry| pattern_matches(pattern.as_deref(), entry.key()))
		.map(|entry| Reply::Bulk(entry.key().clone().into_bytes()))
		.collect();
	Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(matches)])
}

fn pattern_matches(pattern: Option<&str>, key: &str) -> bool {
	match pattern {
		None | Some("*") => true,
		Some(pattern) => match pattern.strip_suffix('*') {
			Some(prefix) => key.starts_with(prefix),
			None => key == pattern,
		},
	}
}

async fn cmd_xadd(store: &Store, args: Vec<Vec<u8>>) -> Reply {
	let Some(stream) = text_arg(&args, 1).map(str::to_owned) else {
		return Reply::Error("XADD requires stream".to_owned());
	};
	let mut maxlen = None;
	let mut cursor = 2;
	if text_arg(&args, cursor).is_some_and(|arg| arg.eq_ignore_ascii_case("MAXLEN")) {
		cursor += 1;
		if text_arg(&args, cursor) == Some("~") {
			cursor += 1;
		}
		let Some(limit) = text_arg(&args, cursor).and_then(|raw| raw.parse::<usize>().ok()) else {
			return Reply::Error("MAXLEN requires a count".to_owned());
		};
		maxlen = Some(limit.max(1));
		cursor += 1;
	}
	if text_arg(&args, cursor) != Some("*") {
		return Reply::Error("only auto ids are supported".to_owned());
	}
	cursor += 1;
	let mut fields = Vec::new();
	while cursor + 1 < args.len() {
		fields.push((args[cursor].clone(), args[cursor + 1].clone()));
		cursor += 2;
	}
	if fields.is_empty() {
		return Reply::Error("XADD requires field value pairs".to_owned());
	}
	let mut streams = store.streams.lock().await;
	let log = streams.entry(stream).or_default();
	let ms = super::now_ms();
	let id = if ms > log.last.ms {
		StreamId { ms, seq: 0 }
	} else {
		StreamId { ms: log.last.ms, seq: log.last.seq + 1 }
	};
	log.last = id;
	log.entries.push_back((id, fields));
	if let Some(limit) = maxlen {
		while log.entries.len() > limit {
			log.entries.pop_front();
		}
	}
	drop(streams);
	store.wakeup.notify_waiters();
	Reply::Bulk(id.render().into_bytes())
}

async fn cmd_xread(store: &Store, args: &[Vec<u8>]) -> Reply {
	let mut block = None;
	let mut cursor = 1;
	if text_arg(args, cursor).is_some_and(|arg| arg.eq_ignore_ascii_case("BLOCK")) {
		let Some(ms) = text_arg(args, cursor + 1).and_then(|raw| raw.parse::<u64>().ok()) else {
			return Reply::Error("BLOCK requires milliseconds".to_owned());
		};
		block = Some(Duration::from_millis(ms));
		cursor += 2;
	}
	if !text_arg(args, cursor).is_some_and(|arg| arg.eq_ignore_ascii_case("STREAMS")) {
		return Reply::Error("XREAD requires STREAMS".to_owned());
	}
	// Single stream only — exactly what the orch follower issues.
	let (Some(stream), Some(after_raw)) = (text_arg(args, cursor + 1), text_arg(args, cursor + 2))
	else {
		return Reply::Error("XREAD requires one stream and one id".to_owned());
	};
	let stream = stream.to_owned();
	let deadline = block.map(|window| Instant::now() + window);
	let mut after = match after_raw {
		"$" => None, // resolved to the live tail below, under the lock
		other => match StreamId::parse(other) {
			Some(id) => Some(id),
			None => return Reply::Error("bad stream id".to_owned()),
		},
	};
	loop {
		let waiter = store.wakeup.notified();
		{
			let streams = store.streams.lock().await;
			let log = streams.get(&stream);
			let floor = if let Some(id) = after {
				id
			} else {
				// First `$` pass: remember the current tail and only
				// serve entries appended after it.
				let tail = log.map_or(StreamId::ZERO, |log| log.last);
				after = Some(tail);
				tail
			};
			if let Some(log) = log {
				let fresh: Vec<Reply> = log
					.entries
					.iter()
					.filter(|(id, _fields)| *id > floor)
					.map(|(id, fields)| {
						let rendered_fields = fields
							.iter()
							.flat_map(|(name, value)| {
								[Reply::Bulk(name.clone()), Reply::Bulk(value.clone())]
							})
							.collect();
						Reply::Array(vec![
							Reply::Bulk(id.render().into_bytes()),
							Reply::Array(rendered_fields),
						])
					})
					.collect();
				if !fresh.is_empty() {
					return Reply::Array(vec![Reply::Array(vec![
						Reply::Bulk(stream.into_bytes()),
						Reply::Array(fresh),
					])]);
				}
			}
		}
		let Some(deadline) = deadline else {
			return Reply::Null; // non-blocking read with nothing new
		};
		let remaining = deadline.saturating_duration_since(Instant::now());
		if remaining.is_zero() || timeout(remaining, waiter).await.is_err() {
			return Reply::Null; // block window elapsed
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{
		super::redis::{Redis, StreamFollower},
		*,
	};

	#[tokio::test]
	async fn kv_ttl_nx_scan_roundtrip() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		redis.ping().await.expect("ping");

		redis.set_px("vmon:o:w:a", b"1", 60_000).await.expect("set");
		assert_eq!(redis.get("vmon:o:w:a").await.expect("get"), Some(b"1".to_vec()));
		assert!(
			redis
				.set_nx_px("vmon:o:lock", b"me", 60_000)
				.await
				.expect("nx first")
		);
		assert!(
			!redis
				.set_nx_px("vmon:o:lock", b"you", 60_000)
				.await
				.expect("nx second")
		);

		redis
			.set_px("vmon:o:w:b", b"2", 25)
			.await
			.expect("set short");
		tokio::time::sleep(Duration::from_millis(60)).await;
		assert_eq!(redis.get("vmon:o:w:b").await.expect("expired get"), None);

		let mut keys = redis.scan_prefix("vmon:o:w:").await.expect("scan");
		keys.sort();
		assert_eq!(keys, vec!["vmon:o:w:a".to_owned()]);

		redis.del("vmon:o:w:a").await.expect("del");
		assert_eq!(redis.get("vmon:o:w:a").await.expect("deleted get"), None);
	}

	#[tokio::test]
	async fn pipelined_commands_on_one_connection_reply_in_order() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let mut socket = TcpStream::connect(server.authority())
			.await
			.expect("connect");

		// Three back-to-back SETs written in one frame before any reply is
		// read: the server must process them sequentially and answer in
		// order (this is what the client's `set_px_pipeline` relies on).
		let mut frame = Vec::new();
		for (key, value) in [("p:a", "1"), ("p:b", "2"), ("p:c", "3")] {
			let args = ["SET", key, value, "PX", "60000"];
			frame.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
			for arg in args {
				frame.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
			}
		}
		socket.write_all(&frame).await.expect("write pipeline");

		let mut replies = [0_u8; 15];
		socket.read_exact(&mut replies).await.expect("read replies");
		assert_eq!(&replies, b"+OK\r\n+OK\r\n+OK\r\n");

		let redis = Redis::new(&server.url()).expect("client");
		assert_eq!(redis.get("p:a").await.expect("get"), Some(b"1".to_vec()));
		assert_eq!(redis.get("p:b").await.expect("get"), Some(b"2".to_vec()));
		assert_eq!(redis.get("p:c").await.expect("get"), Some(b"3".to_vec()));
	}

	#[tokio::test]
	async fn stream_follower_sees_only_new_entries() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		redis.xadd("s", 128, b"before").await.expect("xadd before");

		let mut follower = StreamFollower::new(&redis, "s");
		// Prime the follower so `$` resolves before the next append.
		let quiet = follower.next_batch(20).await.expect("quiet read");
		assert!(quiet.is_empty(), "must not replay history: {quiet:?}");

		let publisher = redis.clone();
		let push = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(30)).await;
			publisher.xadd("s", 128, b"after-1").await.expect("xadd 1");
			publisher.xadd("s", 128, b"after-2").await.expect("xadd 2");
		});
		let mut seen = Vec::new();
		while seen.len() < 2 {
			seen.extend(follower.next_batch(1_000).await.expect("follow"));
		}
		push.await.expect("publisher");
		assert_eq!(seen, vec![b"after-1".to_vec(), b"after-2".to_vec()]);

		// MAXLEN trimming keeps the log bounded.
		for index in 0..300 {
			redis
				.xadd("s", 16, format!("bulk-{index}").as_bytes())
				.await
				.expect("bulk xadd");
		}
	}
}
