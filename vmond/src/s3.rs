//! Lazy, signed S3 object access for remote virtio-fs mounts.
//!
//! [`S3Client`] owns all S3-specific behavior: addressing, `SigV4` signing,
//! `ListObjectsV2` parsing, short-lived metadata caches, and bounded
//! ranged-read caching. The VMM only exchanges the compact proxy protocol
//! defined by its remote filesystem device.

use std::{
	collections::{BTreeMap, HashMap},
	fmt,
	sync::Arc,
	time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use quick_xml::de::from_str;
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use tracing::warn;

use crate::{EngineError, Result};

const LIST_CACHE_TTL: Duration = Duration::from_secs(5);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const CHUNK_SIZE: usize = 1 << 20;
const CHUNK_CACHE_CAP: usize = 256 << 20;
const MAX_LIST_ENTRIES: usize = 100_000;
type ListCacheEntry = (Instant, Arc<Vec<ObjEntry>>);
type ListCache = HashMap<String, ListCacheEntry>;

const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Resolved S3 connection settings for one sandbox mount.
#[derive(Clone, Debug)]
pub struct S3MountConfig {
	/// Bucket name from the mount URI.
	pub bucket:    String,
	/// Normalized key prefix relative to `bucket`.
	pub prefix:    String,
	/// AWS signing region.
	pub region:    String,
	/// Optional S3-compatible endpoint using path-style addressing.
	pub endpoint:  Option<String>,
	/// Whether the guest mounts the remote layer without an overlay.
	pub read_only: bool,
	/// Credentials selected by the engine, absent for anonymous requests.
	pub creds:     Option<S3Credentials>,
	/// Provenance of the selected credential mode.
	pub auth:      S3Auth,
}

/// Credentials used to sign S3 requests.
#[derive(Clone)]
pub struct S3Credentials {
	/// Access-key identifier.
	pub access_key:    String,
	/// Secret access key.
	pub secret_key:    String,
	/// Temporary-credential session token, when present.
	pub session_token: Option<String>,
}

impl fmt::Debug for S3Credentials {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("S3Credentials(redacted)")
	}
}

/// How a mount obtained the credentials it uses for S3 requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3Auth {
	/// Credentials were supplied in the sandbox-create request.
	Inline,
	/// Credentials were read from the server environment.
	Env,
	/// Requests carry no Authorization header.
	Anonymous,
}

impl S3Auth {
	/// Returns the stable metadata spelling for this credential provenance.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Inline => "inline",
			Self::Env => "env",
			Self::Anonymous => "anonymous",
		}
	}
}

/// A failure returned by S3 or by the S3 request machinery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3Error {
	/// The bucket or object does not exist.
	NotFound,
	/// Credentials lack access to the requested resource.
	Access,
	/// The object `ETag` changed during a ranged read.
	Stale,
	/// The caller supplied an invalid S3 request.
	BadRequest,
	/// A network, protocol, or unexpected service failure.
	Io(String),
}

impl S3Error {
	/// Returns the stable remote-filesystem proxy error code.
	pub const fn code(&self) -> &'static str {
		match self {
			Self::NotFound => "not_found",
			Self::Access => "access",
			Self::Stale => "stale",
			Self::BadRequest => "bad_request",
			Self::Io(_) => "io",
		}
	}
}

impl fmt::Display for S3Error {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NotFound => formatter.write_str("S3 object was not found"),
			Self::Access => formatter.write_str("S3 access was denied"),
			Self::Stale => formatter.write_str("S3 object changed during read"),
			Self::BadRequest => formatter.write_str("invalid S3 request"),
			Self::Io(message) => formatter.write_str(message),
		}
	}
}

impl std::error::Error for S3Error {}

/// The kind of object exposed through a mount.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjKind {
	/// A regular S3 object.
	File,
	/// A directory synthesized from a common prefix.
	Dir,
}

/// One direct child in a mounted S3 directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjEntry {
	/// One child name, without the parent path.
	pub name:  String,
	/// Child kind.
	pub kind:  ObjKind,
	/// Object length in bytes, or zero for directories.
	pub size:  u64,
	/// Last modification time as Unix seconds.
	pub mtime: u64,
	/// Object `ETag`, absent for directories.
	pub etag:  Option<String>,
}

/// Metadata for an S3 mount-relative path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjStat {
	/// Path kind.
	pub kind:  ObjKind,
	/// Object length in bytes, or zero for directories.
	pub size:  u64,
	/// Last modification time as Unix seconds.
	pub mtime: u64,
	/// Object `ETag`, absent for directories.
	pub etag:  Option<String>,
}

/// Signed S3 client with bounded directory and ranged-data caches.
pub struct S3Client {
	http:       reqwest::Client,
	cfg:        S3MountConfig,
	list_cache: Mutex<ListCache>,
	chunks:     Mutex<ChunkLru>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct ChunkKey {
	path:  String,
	etag:  String,
	index: u64,
}

struct CachedChunk {
	data:      Bytes,
	last_used: u64,
}

struct ChunkLru {
	entries: HashMap<ChunkKey, CachedChunk>,
	bytes:   usize,
	clock:   u64,
}

impl ChunkLru {
	fn get(&mut self, key: &ChunkKey) -> Option<Bytes> {
		self.clock = self.clock.wrapping_add(1);
		let entry = self.entries.get_mut(key)?;
		entry.last_used = self.clock;
		Some(entry.data.clone())
	}

	fn insert(&mut self, key: ChunkKey, data: Bytes) {
		if data.len() > CHUNK_CACHE_CAP {
			return;
		}
		if let Some(old) = self.entries.remove(&key) {
			self.bytes = self.bytes.saturating_sub(old.data.len());
		}
		while self.bytes.saturating_add(data.len()) > CHUNK_CACHE_CAP {
			let Some(victim) = self
				.entries
				.iter()
				.min_by_key(|(_, entry)| entry.last_used)
				.map(|(key, _)| key.clone())
			else {
				break;
			};
			if let Some(old) = self.entries.remove(&victim) {
				self.bytes = self.bytes.saturating_sub(old.data.len());
			}
		}
		self.clock = self.clock.wrapping_add(1);
		self.bytes += data.len();
		self
			.entries
			.insert(key, CachedChunk { data, last_used: self.clock });
	}
}

impl S3Client {
	/// Creates a bounded, ten-second-timeout client for one resolved mount.
	///
	/// # Errors
	///
	/// Returns [`S3Error::BadRequest`] for an invalid endpoint or an I/O error
	/// when reqwest cannot initialize its rustls client.
	pub fn new(cfg: S3MountConfig) -> std::result::Result<Self, S3Error> {
		if cfg.bucket.is_empty() || cfg.region.is_empty() {
			return Err(S3Error::BadRequest);
		}
		if let Some(endpoint) = &cfg.endpoint {
			validate_endpoint(endpoint)?;
		}
		let http = reqwest::Client::builder()
			.timeout(HTTP_TIMEOUT)
			.build()
			.map_err(|error| S3Error::Io(format!("building S3 HTTP client: {error}")))?;
		Ok(Self {
			http,
			cfg,
			list_cache: Mutex::new(ListCache::new()),
			chunks: Mutex::new(ChunkLru { entries: HashMap::new(), bytes: 0, clock: 0 }),
		})
	}

	/// Verifies bucket access with a one-key `ListObjectsV2` request.
	pub async fn probe(&self) -> std::result::Result<(), S3Error> {
		let prefix = self.list_prefix("")?;
		self.list_page(&prefix, None, 1).await.map(|_| ())
	}

	/// Lists direct children of `dir`, caching the normalized result for five
	/// seconds.
	pub async fn list_dir(&self, dir: &str) -> std::result::Result<Arc<Vec<ObjEntry>>, S3Error> {
		if !valid_relative_path(dir) {
			return Err(S3Error::BadRequest);
		}
		if let Some((stored, entries)) = self.list_cache.lock().get(dir)
			&& stored.elapsed() < LIST_CACHE_TTL
		{
			return Ok(entries.clone());
		}

		let prefix = self.list_prefix(dir)?;
		let mut entries = BTreeMap::new();
		let mut continuation = None;
		loop {
			let mut page = self
				.list_page(&prefix, continuation.as_deref(), 1000)
				.await?;
			let truncated = page.is_truncated;
			let next_continuation = page.next_continuation_token.take();
			merge_listing(&mut entries, &prefix, page)?;
			if entries.len() >= MAX_LIST_ENTRIES {
				warn!(dir, "S3 directory listing reached the 100000-entry cap; truncating");
				break;
			}
			if !truncated {
				break;
			}
			continuation = next_continuation;
			if continuation.is_none() {
				return Err(S3Error::Io(
					"truncated S3 ListObjectsV2 response lacks a continuation token".to_owned(),
				));
			}
		}
		let entries: Arc<Vec<ObjEntry>> =
			Arc::new(entries.into_values().take(MAX_LIST_ENTRIES).collect());
		self
			.list_cache
			.lock()
			.insert(dir.to_owned(), (Instant::now(), entries.clone()));
		Ok(entries)
	}

	/// Resolves metadata from the parent listing instead of issuing a HEAD
	/// request.
	pub async fn stat(&self, path: &str) -> std::result::Result<ObjStat, S3Error> {
		if !valid_relative_path(path) {
			return Err(S3Error::BadRequest);
		}
		if path.is_empty() {
			return Ok(ObjStat { kind: ObjKind::Dir, size: 0, mtime: 0, etag: None });
		}
		let (parent, name) = path
			.rsplit_once('/')
			.map_or(("", path), |(parent, name)| (parent, name));
		let entries = self.list_dir(parent).await?;
		let entry = entries
			.iter()
			.find(|entry| entry.name == name)
			.ok_or(S3Error::NotFound)?;
		Ok(ObjStat {
			kind:  entry.kind,
			size:  entry.size,
			mtime: entry.mtime,
			etag:  entry.etag.clone(),
		})
	}

	/// Reads up to `len` bytes using cached one-mebibyte ranged GETs.
	pub async fn read(
		&self,
		path: &str,
		offset: u64,
		len: u32,
	) -> std::result::Result<Bytes, S3Error> {
		if len as usize > CHUNK_SIZE {
			return Err(S3Error::BadRequest);
		}
		let stat = self.stat(path).await?;
		if stat.kind != ObjKind::File {
			return Err(S3Error::BadRequest);
		}
		if offset >= stat.size || len == 0 {
			return Ok(Bytes::new());
		}
		let etag = stat
			.etag
			.ok_or_else(|| S3Error::Io("S3 file listing omitted its ETag".to_owned()))?;
		let wanted = u64::from(len).min(stat.size - offset) as usize;
		let first_chunk = offset / CHUNK_SIZE as u64;
		let last_chunk = (offset + wanted as u64 - 1) / CHUNK_SIZE as u64;
		let mut data = BytesMut::with_capacity(wanted);
		let mut remaining = wanted;
		for index in first_chunk..=last_chunk {
			let chunk = self.chunk(path, &etag, index, stat.size).await?;
			let begin = if index == first_chunk {
				(offset % CHUNK_SIZE as u64) as usize
			} else {
				0
			};
			let available = chunk.get(begin..).ok_or_else(|| {
				S3Error::Io("S3 ranged response ended before the requested offset".to_owned())
			})?;
			let take = available.len().min(remaining);
			if take == 0 {
				return Err(S3Error::Io("S3 ranged response ended before EOF".to_owned()));
			}
			data.extend_from_slice(&available[..take]);
			remaining -= take;
		}
		if remaining != 0 {
			return Err(S3Error::Io("S3 ranged response ended before EOF".to_owned()));
		}
		Ok(data.freeze())
	}

	async fn chunk(
		&self,
		path: &str,
		etag: &str,
		index: u64,
		size: u64,
	) -> std::result::Result<Bytes, S3Error> {
		let key = ChunkKey { path: path.to_owned(), etag: etag.to_owned(), index };
		let cached_chunk = self.chunks.lock().get(&key);
		if let Some(chunk) = cached_chunk {
			return Ok(chunk);
		}
		let start = index.saturating_mul(CHUNK_SIZE as u64);
		if start >= size {
			return Ok(Bytes::new());
		}
		let end = start.saturating_add(CHUNK_SIZE as u64 - 1).min(size - 1);
		let chunk = self.fetch_chunk(path, etag, start, end).await?;
		let expected = usize::try_from(end - start + 1).expect("S3 chunk fits usize");
		if chunk.len() > expected {
			return Err(S3Error::Io("S3 ranged response exceeded its requested range".to_owned()));
		}
		self.chunks.lock().insert(key, chunk.clone());
		Ok(chunk)
	}

	async fn fetch_chunk(
		&self,
		path: &str,
		etag: &str,
		start: u64,
		end: u64,
	) -> std::result::Result<Bytes, S3Error> {
		let key = self.object_key(path)?;
		let range = format!("bytes={start}-{end}");
		let response = self
			.request(Method::GET, &key, &[], Some(&range), Some(etag))
			.await?;
		if response.status() == StatusCode::PRECONDITION_FAILED {
			return Err(S3Error::Stale);
		}
		if response.status() != StatusCode::PARTIAL_CONTENT {
			return Err(response_error(response.status()));
		}
		response
			.bytes()
			.await
			.map_err(|error| S3Error::Io(format!("reading S3 range response: {error}")))
	}

	async fn list_page(
		&self,
		prefix: &str,
		continuation: Option<&str>,
		max_keys: u16,
	) -> std::result::Result<ListBucketResult, S3Error> {
		let mut query = vec![
			("delimiter".to_owned(), "/".to_owned()),
			("list-type".to_owned(), "2".to_owned()),
			("max-keys".to_owned(), max_keys.to_string()),
			("prefix".to_owned(), prefix.to_owned()),
		];
		if let Some(token) = continuation {
			query.push(("continuation-token".to_owned(), token.to_owned()));
		}
		let response = self.request(Method::GET, "", &query, None, None).await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let body = response
			.text()
			.await
			.map_err(|error| S3Error::Io(format!("reading S3 listing response: {error}")))?;
		from_str(&body).map_err(|error| S3Error::Io(format!("parsing S3 ListObjectsV2 XML: {error}")))
	}

	async fn request(
		&self,
		method: Method,
		key: &str,
		query: &[(String, String)],
		range: Option<&str>,
		if_match: Option<&str>,
	) -> std::result::Result<reqwest::Response, S3Error> {
		let (url, canonical_uri, canonical_query) = self.request_url(key, query)?;
		let host = request_host(&url)?;
		let mut request = self.http.request(method.clone(), url);
		if let Some(range) = range {
			request = request.header("range", range);
		}
		if let Some(etag) = if_match {
			request = request.header("if-match", etag);
		}
		if self.cfg.auth != S3Auth::Anonymous {
			let creds = self.cfg.creds.as_ref().ok_or(S3Error::BadRequest)?;
			let now = Utc::now();
			let date = now.format("%Y%m%dT%H%M%SZ").to_string();
			let mut headers = vec![
				("host".to_owned(), host.clone()),
				("x-amz-content-sha256".to_owned(), UNSIGNED_PAYLOAD.to_owned()),
				("x-amz-date".to_owned(), date.clone()),
			];
			if let Some(token) = &creds.session_token {
				headers.push(("x-amz-security-token".to_owned(), token.clone()));
			}
			let authorization = sigv4::authorization(
				method.as_str(),
				&canonical_uri,
				&canonical_query,
				&headers,
				UNSIGNED_PAYLOAD,
				now,
				&self.cfg.region,
				creds,
			);
			request = request
				.header("host", host)
				.header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
				.header("x-amz-date", date)
				.header("authorization", authorization);
			if let Some(token) = &creds.session_token {
				request = request.header("x-amz-security-token", token);
			}
		}
		request
			.send()
			.await
			.map_err(|error| S3Error::Io(format!("sending S3 request: {error}")))
	}

	fn request_url(
		&self,
		key: &str,
		query: &[(String, String)],
	) -> std::result::Result<(Url, String, String), S3Error> {
		let canonical_query = canonical_query(query);
		let encoded_key = encode_path(key);
		let canonical_uri = if let Some(endpoint) = &self.cfg.endpoint {
			let endpoint = endpoint_url(endpoint)?;
			let mut path = endpoint.path().trim_end_matches('/').to_owned();
			path.push('/');
			path.push_str(&encode_component(&self.cfg.bucket));
			if !encoded_key.is_empty() {
				path.push('/');
				path.push_str(&encoded_key);
			}
			path
		} else if encoded_key.is_empty() {
			"/".to_owned()
		} else {
			format!("/{encoded_key}")
		};
		let url = if let Some(endpoint) = &self.cfg.endpoint {
			let base = endpoint.trim_end_matches('/');
			format!("{base}{canonical_uri}")
		} else {
			format!("https://{}.s3.{}.amazonaws.com{canonical_uri}", self.cfg.bucket, self.cfg.region)
		};
		let url = if canonical_query.is_empty() {
			url
		} else {
			format!("{url}?{canonical_query}")
		};
		let url = Url::parse(&url).map_err(|_| S3Error::BadRequest)?;
		Ok((url, canonical_uri, canonical_query))
	}

	fn object_key(&self, path: &str) -> std::result::Result<String, S3Error> {
		if !valid_relative_path(path) {
			return Err(S3Error::BadRequest);
		}
		Ok(match (self.cfg.prefix.is_empty(), path.is_empty()) {
			(true, true) => String::new(),
			(true, false) => path.to_owned(),
			(false, true) => self.cfg.prefix.clone(),
			(false, false) => format!("{}/{}", self.cfg.prefix, path),
		})
	}

	fn list_prefix(&self, dir: &str) -> std::result::Result<String, S3Error> {
		let key = self.object_key(dir)?;
		Ok(if key.is_empty() {
			key
		} else {
			format!("{key}/")
		})
	}
}

/// Parses an `s3://bucket[/prefix]` mount URI into bucket and normalized
/// prefix.
///
/// # Errors
///
/// Returns [`EngineError::invalid`] when the URI lacks an `s3://` scheme or a
/// bucket.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
	let value = uri
		.strip_prefix("s3://")
		.ok_or_else(|| EngineError::invalid(format!("S3 URI must start with s3:// (got {uri:?})")))?;
	let (bucket, prefix) = value
		.split_once('/')
		.map_or((value, ""), |(bucket, prefix)| (bucket, prefix));
	if bucket.is_empty() {
		return Err(EngineError::invalid("S3 URI must include a bucket"));
	}
	Ok((bucket.to_owned(), prefix.trim_matches('/').to_owned()))
}

fn validate_endpoint(endpoint: &str) -> std::result::Result<(), S3Error> {
	endpoint_url(endpoint).map(|_| ())
}

fn endpoint_url(endpoint: &str) -> std::result::Result<Url, S3Error> {
	let url = Url::parse(endpoint).map_err(|_| S3Error::BadRequest)?;
	if !matches!(url.scheme(), "http" | "https")
		|| url.host_str().is_none()
		|| url.query().is_some()
		|| url.fragment().is_some()
		|| !url.username().is_empty()
		|| url.password().is_some()
	{
		return Err(S3Error::BadRequest);
	}
	Ok(url)
}

fn request_host(url: &Url) -> std::result::Result<String, S3Error> {
	let host = url.host_str().ok_or(S3Error::BadRequest)?;
	Ok(url
		.port()
		.map_or_else(|| host.to_owned(), |port| format!("{host}:{port}")))
}

fn response_error(status: StatusCode) -> S3Error {
	match status {
		StatusCode::FORBIDDEN => S3Error::Access,
		StatusCode::NOT_FOUND => S3Error::NotFound,
		StatusCode::PRECONDITION_FAILED => S3Error::Stale,
		_ => S3Error::Io(format!("unexpected S3 HTTP status {status}")),
	}
}

fn valid_relative_path(path: &str) -> bool {
	path.is_empty()
		|| path
			.split('/')
			.all(|part| !part.is_empty() && part != "." && part != ".." && !part.contains('\0'))
}

fn direct_file_name<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
	let name = key.strip_prefix(prefix)?;
	if valid_segment(name) {
		Some(name)
	} else {
		None
	}
}

fn direct_dir_name<'a>(prefix_value: &'a str, prefix: &str) -> Option<&'a str> {
	let name = prefix_value.strip_prefix(prefix)?.strip_suffix('/')?;
	if valid_segment(name) {
		Some(name)
	} else {
		None
	}
}

fn valid_segment(segment: &str) -> bool {
	!segment.is_empty()
		&& segment != "."
		&& segment != ".."
		&& !segment.contains('/')
		&& !segment.contains('\0')
}

fn parse_mtime(value: &str) -> std::result::Result<u64, S3Error> {
	let timestamp = DateTime::parse_from_rfc3339(value)
		.map_err(|error| {
			S3Error::Io(format!("invalid S3 LastModified timestamp {value:?}: {error}"))
		})?
		.timestamp();
	u64::try_from(timestamp)
		.map_err(|_| S3Error::Io(format!("S3 LastModified timestamp predates Unix epoch: {value:?}")))
}

fn merge_listing(
	entries: &mut BTreeMap<String, ObjEntry>,
	prefix: &str,
	page: ListBucketResult,
) -> std::result::Result<(), S3Error> {
	for object in page.contents {
		let Some(name) = direct_file_name(&object.key, prefix) else {
			continue;
		};
		if entries
			.get(name)
			.is_some_and(|entry| entry.kind == ObjKind::Dir)
		{
			continue;
		}
		entries.insert(name.to_owned(), ObjEntry {
			name:  name.to_owned(),
			kind:  ObjKind::File,
			size:  object.size,
			mtime: parse_mtime(&object.last_modified)?,
			etag:  object.etag,
		});
	}
	for prefix_value in page.common_prefixes {
		let Some(name) = direct_dir_name(&prefix_value.prefix, prefix) else {
			continue;
		};
		entries.insert(name.to_owned(), ObjEntry {
			name:  name.to_owned(),
			kind:  ObjKind::Dir,
			size:  0,
			mtime: 0,
			etag:  None,
		});
	}
	Ok(())
}

fn canonical_query(query: &[(String, String)]) -> String {
	let mut query = query
		.iter()
		.map(|(key, value)| (encode_component(key), encode_component(value)))
		.collect::<Vec<_>>();
	query.sort_unstable();
	query
		.into_iter()
		.map(|(key, value)| format!("{key}={value}"))
		.collect::<Vec<_>>()
		.join("&")
}

fn encode_path(path: &str) -> String {
	path
		.split('/')
		.map(encode_component)
		.collect::<Vec<_>>()
		.join("/")
}

fn encode_component(value: &str) -> String {
	let mut encoded = String::with_capacity(value.len());
	for byte in value.bytes() {
		if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
			encoded.push(char::from(byte));
		} else {
			use std::fmt::Write as _;
			let _ = write!(encoded, "%{byte:02X}");
		}
	}
	encoded
}

#[derive(Deserialize)]
#[serde(rename = "ListBucketResult")]
struct ListBucketResult {
	#[serde(rename = "Contents", default)]
	contents:                Vec<ListContents>,
	#[serde(rename = "CommonPrefixes", default)]
	common_prefixes:         Vec<ListPrefix>,
	#[serde(rename = "IsTruncated", default)]
	is_truncated:            bool,
	#[serde(rename = "NextContinuationToken")]
	next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
struct ListContents {
	#[serde(rename = "Key")]
	key:           String,
	#[serde(rename = "Size")]
	size:          u64,
	#[serde(rename = "LastModified")]
	last_modified: String,
	#[serde(rename = "ETag")]
	etag:          Option<String>,
}

#[derive(Deserialize)]
struct ListPrefix {
	#[serde(rename = "Prefix")]
	prefix: String,
}

mod sigv4 {
	use chrono::{DateTime, Utc};
	use hmac::{Hmac, Mac};
	use sha2::{Digest, Sha256};

	use super::S3Credentials;

	type HmacSha256 = Hmac<Sha256>;

	pub(super) fn authorization(
		method: &str,
		canonical_uri: &str,
		canonical_query: &str,
		headers: &[(String, String)],
		payload_hash: &str,
		timestamp: DateTime<Utc>,
		region: &str,
		credentials: &S3Credentials,
	) -> String {
		let mut headers = headers.to_vec();
		headers.sort_unstable_by(|left, right| left.0.cmp(&right.0));
		use std::fmt::Write as _;

		let mut canonical_headers = String::new();
		for (name, value) in &headers {
			let _ = writeln!(canonical_headers, "{name}:{}", normalize_header(value));
		}
		let signed_headers = headers
			.iter()
			.map(|(name, _)| name.as_str())
			.collect::<Vec<_>>()
			.join(";");
		let canonical_request = [
			method,
			canonical_uri,
			canonical_query,
			&canonical_headers,
			&signed_headers,
			payload_hash,
		]
		.join("\n");
		let date = timestamp.format("%Y%m%d").to_string();
		let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
		let scope = format!("{date}/{region}/s3/aws4_request");
		let string_to_sign = format!(
			"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
			hex::encode(Sha256::digest(canonical_request.as_bytes()))
		);
		let date_key = hmac(format!("AWS4{}", credentials.secret_key).as_bytes(), &date);
		let region_key = hmac(&date_key, region);
		let service_key = hmac(&region_key, "s3");
		let signing_key = hmac(&service_key, "aws4_request");
		let signature = hex::encode(hmac(&signing_key, &string_to_sign));
		format!(
			"AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, \
			 Signature={signature}",
			credentials.access_key
		)
	}

	fn normalize_header(value: &str) -> String {
		value.split_whitespace().collect::<Vec<_>>().join(" ")
	}

	fn hmac(key: &[u8], value: &str) -> [u8; 32] {
		let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary key sizes");
		mac.update(value.as_bytes());
		let output = mac.finalize().into_bytes();
		let mut bytes = [0; 32];
		bytes.copy_from_slice(&output);
		bytes
	}
}

#[cfg(test)]
mod tests {
	use chrono::{TimeZone, Utc};

	use super::*;

	#[test]
	fn parses_s3_uri_and_normalizes_prefix() {
		assert_eq!(parse_s3_uri("s3://bucket").unwrap(), ("bucket".to_owned(), String::new()));
		assert_eq!(
			parse_s3_uri("s3://bucket//prefix/nested/").unwrap(),
			("bucket".to_owned(), "prefix/nested".to_owned())
		);
		assert!(parse_s3_uri("https://bucket/prefix").is_err());
		assert!(parse_s3_uri("s3://").is_err());
	}

	#[test]
	fn signs_published_s3_get_object_vector() {
		let credentials = S3Credentials {
			access_key:    "AKIAIOSFODNN7EXAMPLE".to_owned(),
			secret_key:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
			session_token: None,
		};
		let headers = vec![
			("host".to_owned(), "examplebucket.s3.amazonaws.com".to_owned()),
			("range".to_owned(), "bytes=0-9".to_owned()),
			(
				"x-amz-content-sha256".to_owned(),
				"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
			),
			("x-amz-date".to_owned(), "20130524T000000Z".to_owned()),
		];
		let timestamp = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).single().unwrap();
		let authorization = sigv4::authorization(
			"GET",
			"/test.txt",
			"",
			&headers,
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			timestamp,
			"us-east-1",
			&credentials,
		);
		assert_eq!(
			authorization.split("Signature=").nth(1),
			Some("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
		);
	}

	#[test]
	fn parses_truncated_listing_xml_and_merges_directory_over_file() {
		let page: ListBucketResult = from_str(
			r"<ListBucketResult>
				<IsTruncated>true</IsTruncated>
				<NextContinuationToken>next-token</NextContinuationToken>
				<Contents><Key>prefix/a.txt</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;a&quot;</ETag><Size>5</Size></Contents>
				<Contents><Key>prefix/dir</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;file-dir&quot;</ETag><Size>7</Size></Contents>
				<CommonPrefixes><Prefix>prefix/dir/</Prefix></CommonPrefixes>
			</ListBucketResult>",
		)
		.expect("valid ListObjectsV2 fixture");
		assert!(page.is_truncated);
		assert_eq!(page.next_continuation_token.as_deref(), Some("next-token"));
		let mut entries = BTreeMap::new();
		merge_listing(&mut entries, "prefix/", page).unwrap();
		assert_eq!(entries["a.txt"].kind, ObjKind::File);
		assert_eq!(entries["dir"].kind, ObjKind::Dir);
	}

	#[test]
	fn credential_debug_output_is_redacted() {
		let credentials = S3Credentials {
			access_key:    "access-secret".to_owned(),
			secret_key:    "secret-secret".to_owned(),
			session_token: Some("session-secret".to_owned()),
		};
		let debug = format!("{credentials:?}");
		assert_eq!(debug, "S3Credentials(redacted)");
		assert!(!debug.contains("secret"));
	}
}
