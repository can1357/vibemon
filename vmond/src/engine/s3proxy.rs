//! Per-VM local-transport proxy between remote virtio-fs devices and S3.
//!
//! The proxy owns no S3 credentials itself. It routes one framed request at a
//! time to the mount-specific [`S3Client`] selected during sandbox creation.

use std::{
	collections::HashMap,
	fs, io,
	path::{Path, PathBuf},
	sync::Arc,
};

use serde::Serialize;
#[cfg(not(target_os = "windows"))]
use tokio::net::UnixListener;
#[cfg(target_os = "windows")]
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
	runtime::Runtime,
	sync::oneshot,
	task::{JoinHandle, JoinSet},
};
use tracing::warn;
use vmm::remotefs::proto;

use crate::{
	EngineError, Result,
	s3::{ObjKind, S3Client, S3Error},
};

/// A live per-VM S3 proxy listener and its owned accept task.
pub struct S3Proxy {
	accept_task: JoinHandle<()>,
	sock:        PathBuf,
}

impl S3Proxy {
	/// Binds `sock` on `runtime` and starts serving the supplied tagged mounts.
	///
	/// # Errors
	///
	/// Returns an engine error when the socket parent cannot be prepared, a
	/// stale socket cannot be removed, or the listener fails to bind.
	pub fn start(
		runtime: &Runtime,
		sock: &Path,
		mounts: HashMap<String, Arc<S3Client>>,
	) -> Result<Self> {
		if !sock.is_absolute() {
			return Err(EngineError::invalid(format!(
				"S3 proxy socket must be absolute: {}",
				sock.display()
			)));
		}
		prepare_endpoint(sock)?;

		let sock = sock.to_path_buf();
		let listener_sock = sock.clone();
		let mounts = Arc::new(mounts);
		let (ready_tx, ready_rx) = oneshot::channel::<std::result::Result<(), String>>();
		let accept_task = runtime.spawn(async move {
			match bind_listener(&listener_sock) {
				Ok(listener) => {
					let _ = ready_tx.send(Ok(()));
					accept_loop(listener, listener_sock, mounts).await;
				},
				Err(error) => {
					let _ = ready_tx.send(Err(error.to_string()));
				},
			}
		});

		match runtime.block_on(ready_rx) {
			Ok(Ok(())) => Ok(Self { accept_task, sock }),
			Ok(Err(error)) => {
				accept_task.abort();
				cleanup_endpoint(&sock);
				Err(EngineError::engine(format!("binding S3 proxy socket {}: {error}", sock.display())))
			},
			Err(error) => {
				accept_task.abort();
				cleanup_endpoint(&sock);
				Err(EngineError::engine(format!(
					"starting S3 proxy listener {}: {error}",
					sock.display()
				)))
			},
		}
	}
}

impl Drop for S3Proxy {
	fn drop(&mut self) {
		self.accept_task.abort();
		cleanup_endpoint(&self.sock);
	}
}

#[cfg(not(target_os = "windows"))]
type ProxyListener = UnixListener;
#[cfg(target_os = "windows")]
type ProxyListener = NamedPipeServer;

#[cfg(not(target_os = "windows"))]
fn prepare_endpoint(sock: &Path) -> Result<()> {
	let parent = sock
		.parent()
		.filter(|parent| !parent.as_os_str().is_empty())
		.ok_or_else(|| {
			EngineError::invalid(format!("S3 proxy socket has no parent: {}", sock.display()))
		})?;
	fs::create_dir_all(parent)
		.map_err(|error| EngineError::engine(format!("creating S3 proxy directory: {error}")))?;
	match fs::remove_file(sock) {
		Ok(()) => {},
		Err(error) if error.kind() == io::ErrorKind::NotFound => {},
		Err(error) => {
			return Err(EngineError::engine(format!(
				"removing stale S3 proxy socket {}: {error}",
				sock.display()
			)));
		},
	}
	Ok(())
}

#[cfg(target_os = "windows")]
fn prepare_endpoint(_sock: &Path) -> Result<()> {
	Ok(())
}

#[cfg(not(target_os = "windows"))]
fn cleanup_endpoint(sock: &Path) {
	let _ = fs::remove_file(sock);
}

#[cfg(target_os = "windows")]
fn cleanup_endpoint(_sock: &Path) {}

#[cfg(not(target_os = "windows"))]
fn bind_listener(sock: &Path) -> io::Result<ProxyListener> {
	UnixListener::bind(sock)
}

#[cfg(target_os = "windows")]
fn bind_listener(sock: &Path) -> io::Result<ProxyListener> {
	ServerOptions::new().first_pipe_instance(true).create(sock)
}

#[cfg(not(target_os = "windows"))]
async fn accept_loop(
	listener: ProxyListener,
	_endpoint: PathBuf,
	mounts: Arc<HashMap<String, Arc<S3Client>>>,
) {
	let mut connections = JoinSet::new();
	loop {
		tokio::select! {
			accepted = listener.accept() => match accepted {
				Ok((stream, _)) => {
					let mounts = Arc::clone(&mounts);
					connections.spawn(async move { serve_connection(stream, mounts).await });
				},
				Err(error) => {
					warn!(%error, "S3 proxy accept failed");
					return;
				},
			},
			joined = connections.join_next(), if !connections.is_empty() => {
				match joined {
					Some(Ok(Err(error))) => warn!(%error, "S3 proxy connection ended with an error"),
					Some(Err(error)) => warn!(%error, "S3 proxy connection task failed"),
					Some(Ok(Ok(()))) | None => {},
				}
			},
		}
	}
}

#[cfg(target_os = "windows")]
async fn accept_loop(
	mut listener: ProxyListener,
	endpoint: PathBuf,
	mounts: Arc<HashMap<String, Arc<S3Client>>>,
) {
	let mut connections = JoinSet::new();
	loop {
		if let Err(error) = listener.connect().await {
			warn!(%error, "S3 proxy accept failed");
			return;
		}
		let connected = listener;
		listener = match ServerOptions::new().create(&endpoint) {
			Ok(listener) => listener,
			Err(error) => {
				warn!(%error, "S3 proxy listener recreation failed");
				return;
			},
		};
		let mounts = Arc::clone(&mounts);
		connections.spawn(async move { serve_connection(connected, mounts).await });
		while let Some(joined) = connections.try_join_next() {
			match joined {
				Ok(Err(error)) => warn!(%error, "S3 proxy connection ended with an error"),
				Err(error) => warn!(%error, "S3 proxy connection task failed"),
				Ok(Ok(())) => {},
			}
		}
	}
}

async fn serve_connection<S>(
	mut stream: S,
	mounts: Arc<HashMap<String, Arc<S3Client>>>,
) -> io::Result<()>
where
	S: AsyncRead + AsyncWrite + Unpin,
{
	loop {
		let (ty, id, payload) = match read_frame(&mut stream).await {
			Ok(frame) => frame,
			Err(error)
				if matches!(
					error.kind(),
					io::ErrorKind::UnexpectedEof
						| io::ErrorKind::ConnectionReset
						| io::ErrorKind::BrokenPipe
				) =>
			{
				return Ok(());
			},
			Err(error) => return Err(error),
		};
		let (response_ty, response) = if ty == proto::REQ {
			match serde_json::from_slice::<proto::Request>(&payload) {
				Ok(request) => dispatch(request, &mounts).await?,
				Err(error) => {
					error_response("bad_request", &format!("invalid S3 proxy request: {error}"))?
				},
			}
		} else {
			error_response("bad_request", "expected S3 proxy request frame")?
		};
		write_frame(&mut stream, response_ty, id, &response).await?;
	}
}

async fn dispatch(
	request: proto::Request,
	mounts: &HashMap<String, Arc<S3Client>>,
) -> io::Result<(u8, Vec<u8>)> {
	match request {
		proto::Request::Stat { tag, path } => {
			let Some(client) = mounts.get(&tag) else {
				return error_response("bad_request", "unknown S3 mount tag");
			};
			match client.stat(&path).await {
				Ok(stat) => json_response(&proto::StatReply {
					kind:  obj_kind(stat.kind),
					size:  stat.size,
					mtime: stat.mtime,
					etag:  stat.etag,
				}),
				Err(error) => s3_error_response(error),
			}
		},
		proto::Request::List { tag, path } => {
			let Some(client) = mounts.get(&tag) else {
				return error_response("bad_request", "unknown S3 mount tag");
			};
			match client.list_dir(&path).await {
				Ok(entries) => json_response(&proto::ListReply {
					entries: entries
						.iter()
						.map(|entry| proto::Entry {
							name:  entry.name.clone(),
							kind:  obj_kind(entry.kind),
							size:  entry.size,
							mtime: entry.mtime,
						})
						.collect(),
				}),
				Err(error) => s3_error_response(error),
			}
		},
		proto::Request::Read { tag, path, offset, len } => {
			let Some(client) = mounts.get(&tag) else {
				return error_response("bad_request", "unknown S3 mount tag");
			};
			match client.read(&path, offset, len).await {
				Ok(bytes) => Ok((proto::OK_DATA, bytes.to_vec())),
				Err(error) => s3_error_response(error),
			}
		},
	}
}

const fn obj_kind(kind: ObjKind) -> proto::Kind {
	match kind {
		ObjKind::File => proto::Kind::File,
		ObjKind::Dir => proto::Kind::Dir,
	}
}

fn s3_error_response(error: S3Error) -> io::Result<(u8, Vec<u8>)> {
	let code = error.code();
	error_response(code, &error.to_string())
}

fn json_response<T: Serialize>(value: &T) -> io::Result<(u8, Vec<u8>)> {
	Ok((proto::OK_JSON, json(value)?))
}

fn error_response(code: &str, msg: &str) -> io::Result<(u8, Vec<u8>)> {
	Ok((proto::ERR, json(&proto::ErrReply { code: code.to_owned(), msg: msg.to_owned() })?))
}

fn json<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
	serde_json::to_vec(value).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn read_frame<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<(u8, u32, Vec<u8>)> {
	let mut header = [0u8; proto::HEADER_LEN];
	stream.read_exact(&mut header).await?;
	let payload_len =
		u32::from_le_bytes(header[..4].try_into().expect("fixed frame header")) as usize;
	if payload_len > proto::MAX_FRAME {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("S3 proxy frame payload length {payload_len} exceeds {}", proto::MAX_FRAME),
		));
	}
	let ty = header[4];
	let id = u32::from_le_bytes(header[5..].try_into().expect("fixed frame header"));
	let mut payload = vec![0; payload_len];
	stream.read_exact(&mut payload).await?;
	Ok((ty, id, payload))
}

async fn write_frame<S: AsyncWrite + Unpin>(
	stream: &mut S,
	ty: u8,
	id: u32,
	payload: &[u8],
) -> io::Result<()> {
	if payload.len() > proto::MAX_FRAME {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("S3 proxy frame payload length {} exceeds {}", payload.len(), proto::MAX_FRAME),
		));
	}
	let mut header = [0u8; proto::HEADER_LEN];
	header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
	header[4] = ty;
	header[5..].copy_from_slice(&id.to_le_bytes());
	stream.write_all(&header).await?;
	stream.write_all(payload).await
}

#[cfg(test)]
mod tests {
	use std::{
		io::{Read, Write},
		net::{SocketAddr, TcpListener, TcpStream},
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
		thread,
		time::Duration,
	};

	use tempfile::tempdir;

	use super::*;
	use crate::s3::{S3Auth, S3MountConfig};

	struct StubS3 {
		addr:   SocketAddr,
		stop:   Arc<AtomicBool>,
		handle: Option<thread::JoinHandle<()>>,
	}

	impl StubS3 {
		fn start() -> Self {
			let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind S3 stub");
			listener
				.set_nonblocking(true)
				.expect("set S3 stub nonblocking");
			let addr = listener.local_addr().expect("S3 stub address");
			let stop = Arc::new(AtomicBool::new(false));
			let thread_stop = Arc::clone(&stop);
			let handle = thread::spawn(move || {
				while !thread_stop.load(Ordering::Relaxed) {
					match listener.accept() {
						Ok((mut stream, _)) => serve_s3(&mut stream),
						Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
							thread::sleep(Duration::from_millis(5));
						},
						Err(_) => return,
					}
				}
			});
			Self { addr, stop, handle: Some(handle) }
		}
	}

	impl Drop for StubS3 {
		fn drop(&mut self) {
			self.stop.store(true, Ordering::Relaxed);
			let _ = TcpStream::connect(self.addr);
			if let Some(handle) = self.handle.take() {
				let _ = handle.join();
			}
		}
	}

	fn serve_s3(stream: &mut TcpStream) {
		let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
		let mut request = [0u8; 4096];
		let read = stream.read(&mut request).unwrap_or(0);
		let request = String::from_utf8_lossy(&request[..read]);
		let (status, body) = if request.starts_with("GET /bucket?") {
			(
				"200 OK",
				r"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;etag&quot;</ETag><Size>5</Size></Contents></ListBucketResult>".as_bytes(),
			)
		} else if request.starts_with("GET /bucket/a.txt") {
			assert!(request.contains("Range: bytes=0-4") || request.contains("range: bytes=0-4"));
			("206 Partial Content", b"hello".as_slice())
		} else {
			("404 Not Found", b"missing".as_slice())
		};
		let header = format!(
			"HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
			body.len()
		);
		let _ = stream.write_all(header.as_bytes());
		let _ = stream.write_all(body);
	}

	fn request(
		stream: &mut std::os::unix::net::UnixStream,
		id: u32,
		request: proto::Request,
	) -> (u8, u32, Vec<u8>) {
		let payload = serde_json::to_vec(&request).expect("serialize proxy request");
		proto::write_frame(stream, proto::REQ, id, &payload).expect("write proxy request");
		proto::read_frame(stream).expect("read proxy response")
	}

	#[test]
	fn proxies_stat_list_read_and_errors() {
		let server = StubS3::start();
		let runtime = Runtime::new().expect("network runtime");
		let endpoint = format!("http://{}", server.addr);
		let client = Arc::new(
			S3Client::new(S3MountConfig {
				bucket:    "bucket".to_owned(),
				prefix:    String::new(),
				region:    "us-east-1".to_owned(),
				endpoint:  Some(endpoint),
				read_only: false,
				creds:     None,
				auth:      S3Auth::Anonymous,
			})
			.expect("S3 client"),
		);
		let mut mounts = HashMap::new();
		mounts.insert("data".to_owned(), client);
		let dir = tempdir().expect("temporary proxy directory");
		let sock = dir.path().join("s3.sock");
		let proxy = S3Proxy::start(&runtime, &sock, mounts).expect("start S3 proxy");
		let mut stream = std::os::unix::net::UnixStream::connect(&sock).expect("connect to S3 proxy");

		let (ty, id, payload) = request(&mut stream, 7, proto::Request::Stat {
			tag:  "data".to_owned(),
			path: "a.txt".to_owned(),
		});
		assert_eq!((ty, id), (proto::OK_JSON, 7));
		let stat: proto::StatReply = serde_json::from_slice(&payload).expect("stat reply");
		assert_eq!(stat.kind, proto::Kind::File);
		assert_eq!(stat.size, 5);
		assert_eq!(stat.etag.as_deref(), Some("\"etag\""));

		let (ty, id, payload) = request(&mut stream, 8, proto::Request::List {
			tag:  "data".to_owned(),
			path: String::new(),
		});
		assert_eq!((ty, id), (proto::OK_JSON, 8));
		let list: proto::ListReply = serde_json::from_slice(&payload).expect("list reply");
		assert_eq!(list.entries.len(), 1);
		assert_eq!(list.entries[0].name, "a.txt");

		let (ty, id, payload) = request(&mut stream, 9, proto::Request::Read {
			tag:    "data".to_owned(),
			path:   "a.txt".to_owned(),
			offset: 0,
			len:    5,
		});
		assert_eq!((ty, id), (proto::OK_DATA, 9));
		assert_eq!(payload, b"hello");

		let (ty, id, payload) = request(&mut stream, 10, proto::Request::Stat {
			tag:  "data".to_owned(),
			path: "missing".to_owned(),
		});
		assert_eq!((ty, id), (proto::ERR, 10));
		let error: proto::ErrReply = serde_json::from_slice(&payload).expect("error reply");
		assert_eq!(error.code, "not_found");

		drop(stream);
		drop(proxy);
		assert!(!sock.exists(), "proxy socket removed on drop");
	}
}
