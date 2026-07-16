//! Atomic content-addressed storage for durable function payloads.

use std::{
	collections::HashSet,
	fs::{self, File, OpenOptions},
	future::Future,
	io::{Read, Seek, SeekFrom, Write},
	os::unix::fs::OpenOptionsExt,
	path::{Path, PathBuf},
	sync::{Arc, Condvar, LazyLock, Mutex},
	thread,
	time::{Duration, SystemTime},
};

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
	EngineError, Result,
	config::{ClusterMode, ServeConfig},
	postgres::{self as pg, Client as PgClient},
	s3::{S3Auth, S3Client, S3Credentials, S3MountConfig},
};

/// SHA-256 digest length in bytes.
pub const SHA256_BYTES: usize = 32;

/// A verified, published artifact which still owns its digest publication
/// lease. It can only be committed through [`super::store::Store`].
pub struct StoredArtifact {
	/// Lowercase hexadecimal SHA-256 digest.
	pub digest:       String,
	/// Exact byte length of the content.
	pub size:         u64,
	/// Absolute path of the immutable local cache file.
	pub path:         PathBuf,
	pub(crate) lease: ArtifactDigestLease,
}

impl std::fmt::Debug for StoredArtifact {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("StoredArtifact")
			.field("digest", &self.digest)
			.field("size", &self.size)
			.field("path", &self.path)
			.finish_non_exhaustive()
	}
}

impl PartialEq for StoredArtifact {
	fn eq(&self, other: &Self) -> bool {
		self.digest == other.digest && self.size == other.size && self.path == other.path
	}
}

impl Eq for StoredArtifact {}

/// A single-digest publication/deletion lease. Its private construction keeps
/// metadata commits and destructive operations fenced by the same lock domain.
pub(crate) struct ArtifactDigestLease {
	digest: String,
	_guard: ArtifactLeaseGuard,
}

enum ArtifactLeaseGuard {
	Local { _guard: LocalArtifactLease },
	Postgres { _client: Box<PgClient> },
}

impl ArtifactDigestLease {
	pub(crate) fn digest(&self) -> &str {
		&self.digest
	}
}

#[derive(Clone)]
enum ArtifactLeaseProvider {
	Local,
	Postgres(String),
}

struct LocalArtifactLease {
	digest: String,
	locks:  Arc<LocalArtifactLocks>,
}

struct LocalArtifactLocks {
	held: Mutex<HashSet<String>>,
	wake: Condvar,
}

static LOCAL_ARTIFACT_LOCKS: LazyLock<Arc<LocalArtifactLocks>> = LazyLock::new(|| {
	Arc::new(LocalArtifactLocks { held: Mutex::new(HashSet::new()), wake: Condvar::new() })
});

impl Drop for LocalArtifactLease {
	fn drop(&mut self) {
		if let Ok(mut held) = self.locks.held.lock() {
			held.remove(&self.digest);
			self.locks.wake.notify_all();
		}
	}
}

impl ArtifactLeaseProvider {
	fn acquire(&self, digest: &str) -> Result<ArtifactDigestLease> {
		let key = artifact_lock_key(digest);
		match self {
			Self::Local => {
				let locks = Arc::clone(&LOCAL_ARTIFACT_LOCKS);
				let mut held = locks
					.held
					.lock()
					.map_err(|_| EngineError::engine("artifact lock registry poisoned"))?;
				while held.contains(digest) {
					held = locks
						.wake
						.wait(held)
						.map_err(|_| EngineError::engine("artifact lock registry poisoned"))?;
				}
				held.insert(digest.to_owned());
				drop(held);
				Ok(ArtifactDigestLease {
					digest: digest.to_owned(),
					_guard: ArtifactLeaseGuard::Local {
						_guard: LocalArtifactLease { digest: digest.to_owned(), locks },
					},
				})
			},
			Self::Postgres(url) => {
				let mut client = pg::connect(url, "function artifact digest guard")?;
				pg::blocking(|| {
					client
						.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key])
						.map_err(|error| {
							EngineError::engine(format!("function artifact lock: {error}"))
						})?;
					Ok::<(), EngineError>(())
				})?;
				Ok(ArtifactDigestLease {
					digest: digest.to_owned(),
					_guard: ArtifactLeaseGuard::Postgres { _client: Box::new(client) },
				})
			},
		}
	}

	fn try_acquire(&self, digest: &str) -> Result<Option<ArtifactDigestLease>> {
		let key = artifact_lock_key(digest);
		match self {
			Self::Local => {
				let locks = Arc::clone(&LOCAL_ARTIFACT_LOCKS);
				let mut held = locks
					.held
					.lock()
					.map_err(|_| EngineError::engine("artifact lock registry poisoned"))?;
				if !held.insert(digest.to_owned()) {
					return Ok(None);
				}
				drop(held);
				Ok(Some(ArtifactDigestLease {
					digest: digest.to_owned(),
					_guard: ArtifactLeaseGuard::Local {
						_guard: LocalArtifactLease { digest: digest.to_owned(), locks },
					},
				}))
			},
			Self::Postgres(url) => {
				let mut client = pg::connect(url, "function artifact staging guard")?;
				let acquired = pg::blocking(|| {
					client
						.query_one("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", &[&key])
						.map(|row| row.get::<_, bool>(0))
						.map_err(|error| EngineError::engine(format!("function artifact lock: {error}")))
				})?;
				Ok(acquired.then_some(ArtifactDigestLease {
					digest: digest.to_owned(),
					_guard: ArtifactLeaseGuard::Postgres { _client: Box::new(client) },
				}))
			},
		}
	}
}

fn artifact_lock_key(digest: &str) -> String {
	format!("function-artifact:{digest}")
}

/// Immutable SHA-256 content store. Production uses S3 as the authoritative
/// object store and keeps verified bytes in `root` only as a local cache.
#[derive(Clone)]
pub struct ArtifactStore {
	root:           PathBuf,
	s3:             Option<Arc<S3Client>>,
	leases:         ArtifactLeaseProvider,
	staging_grace:  Duration,
	staging_prefix: String,
}

impl std::fmt::Debug for ArtifactStore {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("ArtifactStore")
			.field("root", &self.root)
			.field("authoritative_s3", &self.s3.is_some())
			.finish()
	}
}

/// A digest-verified artifact positioned at its first byte.
///
/// Verification is completed before this reader is returned, so callers can
/// consume quota-sized artifacts with a fixed-size buffer without observing
/// unverified content.
#[derive(Debug)]
pub struct VerifiedArtifactReader {
	file: File,
	size: u64,
}

impl VerifiedArtifactReader {
	/// Exact verified byte length.
	pub const fn len(&self) -> u64 {
		self.size
	}

	/// Whether this artifact contains no bytes.
	pub const fn is_empty(&self) -> bool {
		self.size == 0
	}
}

impl Read for VerifiedArtifactReader {
	fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
		self.file.read(buffer)
	}
}

impl Seek for VerifiedArtifactReader {
	fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
		self.file.seek(position)
	}
}

/// Incremental, quota-bounded artifact upload. Dropping it aborts and removes
/// the confined temporary file.
pub struct ArtifactWriter {
	temporary:       PathBuf,
	final_path:      PathBuf,
	file:            Option<File>,
	hasher:          Sha256,
	expected_digest: [u8; SHA256_BYTES],
	expected_size:   u64,
	max_size:        u64,
	written:         u64,
	committed:       bool,
	s3:              Option<Arc<S3Client>>,
	object_key:      String,
	staging_prefix:  String,
	leases:          ArtifactLeaseProvider,
}

impl ArtifactWriter {
	/// Append one chunk while enforcing declared size and server quota before
	/// writing.
	pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<()> {
		let next = self
			.written
			.checked_add(chunk.len() as u64)
			.ok_or_else(|| EngineError::invalid("artifact size overflow"))?;
		if next > self.expected_size {
			return Err(EngineError::invalid("artifact upload exceeds declared size"));
		}
		if next > self.max_size {
			return Err(EngineError::invalid("artifact upload exceeds quota"));
		}
		self
			.file
			.as_mut()
			.ok_or_else(|| EngineError::invalid("artifact writer is finalized"))?
			.write_all(chunk)?;
		self.hasher.update(chunk);
		self.written = next;
		Ok(())
	}

	/// Verify, fsync, and atomically publish the completed artifact.
	pub fn finalize(mut self) -> Result<StoredArtifact> {
		if self.written != self.expected_size {
			return Err(EngineError::invalid(format!(
				"artifact size mismatch: expected {}, received {}",
				self.expected_size, self.written
			)));
		}
		let actual = self.hasher.clone().finalize();
		if actual.as_slice() != self.expected_digest {
			return Err(EngineError::invalid("artifact SHA-256 digest mismatch"));
		}
		let file = self
			.file
			.take()
			.ok_or_else(|| EngineError::invalid("artifact writer is finalized"))?;
		file.sync_all()?;
		drop(file);
		let digest = hex::encode(self.expected_digest);
		// This session/process lease remains inside the returned value until
		// Store::record_stored_artifact has committed the metadata reference.
		let lease = self.leases.acquire(&digest)?;
		let parent = self
			.final_path
			.parent()
			.ok_or_else(|| EngineError::engine("artifact path has no parent"))?;

		if let Some(s3) = &self.s3 {
			let staging_key = format!("{}/{}-{}", self.staging_prefix, digest, Uuid::new_v4());
			let (tx, rx) = mpsc::channel(4);
			let temp_path = self.temporary.clone();
			let s3_clone = s3.clone();
			let upload_key = staging_key.clone();
			let upload_task = async move {
				s3_clone
					.put_multipart(&upload_key, rx)
					.await
					.map_err(|err| EngineError::engine(format!("failed S3 multipart upload: {err:?}")))
			};
			let read_thread = thread::spawn(move || -> std::io::Result<()> {
				stream_reader_to_channel(File::open(&temp_path)?, tx)
			});
			let upload_result = block_on(upload_task);
			let reader_result = read_thread
				.join()
				.map_err(|_| EngineError::engine("multipart reader thread panicked"))?;
			match (reader_result, upload_result) {
				(Ok(()), Ok(())) => {},
				(Err(error), _) => {
					let _ = block_on(s3.remove(&staging_key));
					return Err(error.into());
				},
				(_, Err(error)) => {
					let _ = block_on(s3.remove(&staging_key));
					return Err(error);
				},
			}
			if let Err(error) =
				verify_s3_artifact(s3, &staging_key, &self.expected_digest, self.expected_size)
			{
				let _ = block_on(s3.remove(&staging_key));
				return Err(error);
			}
			let publish_result =
				block_on(s3.copy_object(&staging_key, &self.object_key, self.expected_size))
					.map_err(|error| EngineError::engine(format!("publishing artifact to S3: {error}")));
			if let Err(error) = publish_result {
				let _ = block_on(s3.remove(&staging_key));
				return Err(error);
			}
			let final_result =
				verify_s3_artifact(s3, &self.object_key, &self.expected_digest, self.expected_size);
			let _ = block_on(s3.remove(&staging_key));
			final_result?;
		}

		if self.final_path.exists() {
			verify_file(&self.final_path, &digest, self.expected_size)?;
			fs::remove_file(&self.temporary)?;
		} else {
			match fs::rename(&self.temporary, &self.final_path) {
				Ok(()) => {},
				Err(_error) if self.final_path.exists() => {
					verify_file(&self.final_path, &digest, self.expected_size)?;
					let _ = fs::remove_file(&self.temporary);
				},
				Err(error) => return Err(error.into()),
			}
		}
		File::open(parent)?.sync_all()?;
		self.committed = true;
		Ok(StoredArtifact { digest, size: self.expected_size, path: self.final_path.clone(), lease })
	}
}
impl Drop for ArtifactWriter {
	fn drop(&mut self) {
		if !self.committed {
			self.file.take();
			let _ = fs::remove_file(&self.temporary);
		}
	}
}

impl ArtifactStore {
	/// Open explicitly configured durable metadata backend.
	pub fn open_with_config(root: impl Into<PathBuf>, config: &ServeConfig) -> Result<Self> {
		let root = root.into();
		let (s3, leases, staging_prefix) = if config.cluster_mode == ClusterMode::Production {
			let postgres_url = config
				.postgres_url
				.as_deref()
				.ok_or_else(|| EngineError::invalid("production mode requires postgres_url"))?;
			let namespace = crate::mesh::cluster_store::production_object_namespace(postgres_url)?;
			let creds =
				if let (Some(key), Some(secret)) = (&config.s3_access_key, &config.s3_secret_key) {
					Some(S3Credentials {
						access_key:    key.clone(),
						secret_key:    secret.clone(),
						session_token: None,
					})
				} else {
					None
				};
			let s3_cfg = S3MountConfig {
				bucket: config.s3_bucket.clone().unwrap_or_default(),
				prefix: config.s3_prefix.clone().unwrap_or_default(),
				region: config
					.s3_region
					.clone()
					.unwrap_or_else(|| "us-east-1".to_owned()),
				endpoint: config.s3_endpoint.clone(),
				read_only: false,
				creds,
				auth: if config.s3_access_key.is_some() {
					S3Auth::Inline
				} else {
					S3Auth::Anonymous
				},
			};
			let client = S3Client::new(s3_cfg).map_err(|err| {
				EngineError::engine(format!("failed to initialize S3 client: {err:?}"))
			})?;
			(
				Some(Arc::new(client)),
				ArtifactLeaseProvider::Postgres(postgres_url.to_owned()),
				format!("{namespace}/artifacts/.staging"),
			)
		} else {
			(None, ArtifactLeaseProvider::Local, "artifacts/.staging".to_owned())
		};
		let staging_grace = Duration::from_secs_f64(config.s3_multipart_stale_sec);
		Ok(Self { root, s3, leases, staging_grace, staging_prefix })
	}

	/// Open (and create) an artifact directory.
	pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
		let root = root.into();
		fs::create_dir_all(&root)?;
		Ok(Self {
			root,
			s3: None,
			leases: ArtifactLeaseProvider::Local,
			staging_grace: Duration::from_secs(0),
			staging_prefix: "artifacts/.staging".to_owned(),
		})
	}

	/// Return the artifact root.
	pub fn root(&self) -> &Path {
		&self.root
	}

	/// Clear the authoritative S3 client caches if configured.
	pub fn clear_cache(&self) {
		if let Some(s3) = &self.s3 {
			s3.clear_cache();
		}
	}

	/// Remove abandoned remote staging objects. A candidate is only deleted
	/// after its grace period and after a non-blocking acquisition of the
	/// final digest lease, so a live finalizer is never disturbed.
	pub fn gc_staging(&self, now: SystemTime, limit: u32) -> Result<u64> {
		let Some(s3) = &self.s3 else {
			return Ok(0);
		};
		let now_secs = now
			.duration_since(SystemTime::UNIX_EPOCH)
			.map_err(|error| {
				EngineError::engine(format!("system clock precedes Unix epoch: {error}"))
			})?
			.as_secs();
		let cutoff = now_secs.saturating_sub(self.staging_grace.as_secs());
		let mut continuation = None;
		let mut removed = 0;
		while removed < u64::from(limit) {
			let page = block_on(s3.scan_keys_page(
				&self.staging_prefix,
				continuation.as_deref(),
				limit.saturating_sub(removed as u32).clamp(1, 1000) as u16,
			))
			.map_err(|error| EngineError::engine(format!("listing artifact staging: {error:?}")))?;
			for entry in page.entries {
				if entry.mtime > cutoff {
					continue;
				}
				let Some(digest) = staging_digest(&self.staging_prefix, &entry.key) else {
					continue;
				};
				let Some(_lease) = self.leases.try_acquire(digest)? else {
					continue;
				};
				block_on(s3.remove(&entry.key)).map_err(|error| {
					EngineError::engine(format!("removing artifact staging object: {error:?}"))
				})?;
				removed += 1;
			}
			let Some(next) = page.next_continuation_token else {
				break;
			};
			continuation = Some(next);
		}
		Ok(removed)
	}

	fn final_object_key(&self, digest: &str) -> String {
		format!(
			"{}/{}",
			self
				.staging_prefix
				.strip_suffix("/.staging")
				.expect("artifact staging prefix is a sibling of final objects"),
			digest
		)
	}

	/// Begin a confined incremental upload with an exact digest, size, and
	/// quota.
	pub fn begin_put(
		&self,
		expected_digest: &[u8],
		expected_size: u64,
		max_size: u64,
	) -> Result<ArtifactWriter> {
		if expected_digest.len() != SHA256_BYTES {
			return Err(EngineError::invalid("artifact digest must be a 32-byte SHA-256 digest"));
		}
		if expected_size > max_size {
			return Err(EngineError::invalid("artifact declared size exceeds quota"));
		}
		let mut digest = [0u8; SHA256_BYTES];
		digest.copy_from_slice(expected_digest);
		let digest_hex = hex::encode(digest);
		let final_path = self.path_for(&digest_hex)?;
		let parent = final_path
			.parent()
			.ok_or_else(|| EngineError::engine("artifact path has no parent"))?;
		fs::create_dir_all(parent)?;
		let temporary = parent.join(format!(".{digest_hex}.tmp-{}", Uuid::new_v4()));
		let file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(0o600)
			.open(&temporary)?;
		Ok(ArtifactWriter {
			temporary,
			final_path,
			file: Some(file),
			hasher: Sha256::new(),
			expected_digest: digest,
			expected_size,
			max_size,
			written: 0,
			committed: false,
			s3: self.s3.clone(),
			object_key: self.final_object_key(&digest_hex),
			staging_prefix: self.staging_prefix.clone(),
			leases: self.leases.clone(),
		})
	}

	/// Commit bytes through the same incremental verification path.
	pub fn put_verified(
		&self,
		expected_digest: &[u8],
		expected_size: u64,
		bytes: &[u8],
	) -> Result<StoredArtifact> {
		let mut writer = self.begin_put(expected_digest, expected_size, expected_size)?;
		writer.write_chunk(bytes)?;
		writer.finalize()
	}

	/// Hash and atomically commit bytes.
	pub fn put(&self, bytes: &[u8]) -> Result<StoredArtifact> {
		let digest = Sha256::digest(bytes);
		self.put_verified(&digest, bytes.len() as u64, bytes)
	}

	/// Open an artifact for bounded-memory or range consumption.
	///
	/// Size and digest validation finish before the reader is returned. The
	/// returned reader supports seeking for callers that consume ranges.
	pub fn open_verified(
		&self,
		digest: &str,
		expected_size: Option<u64>,
	) -> Result<VerifiedArtifactReader> {
		let mut file = self.open_file(digest)?;
		let size = verify_open_file(&mut file, digest, expected_size)?;
		Ok(VerifiedArtifactReader { file, size })
	}

	/// Read a small artifact into memory, rejecting it before allocation when
	/// its stored size exceeds `max_size`.
	pub fn read_bounded(
		&self,
		digest: &str,
		expected_size: Option<u64>,
		max_size: u64,
	) -> Result<Vec<u8>> {
		let mut file = self.open_file(digest)?;
		let size = file.metadata()?.len();
		if size > max_size {
			return Err(EngineError::invalid(format!(
				"artifact {digest} exceeds in-memory read limit"
			)));
		}
		if let Some(expected_size) = expected_size
			&& size != expected_size
		{
			return Err(EngineError::engine(format!("artifact {digest} has corrupt size")));
		}
		let capacity = usize::try_from(size)
			.map_err(|_| EngineError::invalid("artifact exceeds addressable memory"))?;
		let mut bytes = Vec::with_capacity(capacity);
		file.read_to_end(&mut bytes)?;
		if bytes.len() as u64 != size {
			return Err(EngineError::engine(format!("artifact {digest} has corrupt size")));
		}
		if hex::encode(Sha256::digest(&bytes)) != digest {
			return Err(EngineError::engine(format!("artifact {digest} failed digest verification")));
		}
		Ok(bytes)
	}

	/// Read an artifact and verify both its path digest and bytes.
	///
	/// Prefer [`Self::open_verified`] for quota-sized content and
	/// [`Self::read_bounded`] when a caller has an explicit in-memory limit.
	pub fn read(&self, digest: &str, expected_size: Option<u64>) -> Result<Vec<u8>> {
		self.read_bounded(digest, expected_size, u64::MAX)
	}

	/// Return the canonical sharded path for a validated digest.
	pub fn path_for(&self, digest: &str) -> Result<PathBuf> {
		validate_digest_hex(digest)?;
		Ok(self.root.join(&digest[..2]).join(&digest[2..]))
	}

	/// Remove an artifact while retaining its digest lease throughout remote
	/// and local deletion.
	pub fn remove(&self, digest: &str) -> Result<()> {
		let lease = self.leases.acquire(digest)?;
		let path = self.path_for(digest)?;
		self.remove_remote_locked(&lease)?;
		match fs::remove_file(path) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error.into()),
		}
	}

	fn remove_remote_locked(&self, lease: &ArtifactDigestLease) -> Result<()> {
		let Some(s3) = &self.s3 else {
			return Ok(());
		};
		let s3 = Arc::clone(s3);
		let key = self.final_object_key(lease.digest());
		block_on(async move { s3.remove(&key).await })
			.map_err(|error| EngineError::engine(format!("failed to remove S3 artifact: {error:?}")))
	}

	/// Delete expired, unreferenced artifacts using a recoverable trash phase.
	///
	/// Files are atomically renamed into the store's trash directory before
	/// metadata is deleted. A later GC restores trashed files whose metadata
	/// still exists and sweeps files whose metadata deletion committed.
	pub fn gc_expired(&self, store: &super::store::Store, now_ms: u64, limit: u32) -> Result<u64> {
		self.gc_expired_with_remove(store, now_ms, limit, |path| fs::remove_file(path))
	}

	fn gc_expired_with_remove(
		&self,
		store: &super::store::Store,
		now_ms: u64,
		limit: u32,
		mut remove_file: impl FnMut(&Path) -> std::io::Result<()>,
	) -> Result<u64> {
		self.reconcile_trash(store, &mut remove_file)?;
		let candidates = store.unreferenced_expired_artifacts(now_ms, limit)?;
		let trash_dir = self.root.join(".trash");
		fs::create_dir_all(&trash_dir)?;
		let mut removed = 0;
		for (digest, _logical_key) in candidates {
			let lease = self.leases.acquire(&digest)?;
			let expected = self.path_for(&digest)?;
			let cached_trash = trash_dir.join(&digest);
			let (trash, cached) = match fs::rename(&expected, &cached_trash) {
				Ok(()) => (cached_trash, true),
				Err(error) if error.kind() == std::io::ErrorKind::NotFound && self.s3.is_some() => {
					let marker = trash_dir.join(format!("{digest}.delete"));
					OpenOptions::new()
						.write(true)
						.create_new(true)
						.mode(0o600)
						.open(&marker)?
						.sync_all()?;
					File::open(&trash_dir)?.sync_all()?;
					(marker, false)
				},
				Err(error) => return Err(error.into()),
			};
			if store.delete_unreferenced_artifact_locked(&lease, now_ms)? {
				self.remove_remote_locked(&lease)?;
				remove_if_present(&trash, &mut remove_file)?;
				removed += 1;
			} else if cached {
				fs::rename(&trash, &expected)?;
			} else {
				remove_if_present(&trash, &mut remove_file)?;
			}
		}
		Ok(removed)
	}

	fn reconcile_trash(
		&self,
		store: &super::store::Store,
		remove_file: &mut impl FnMut(&Path) -> std::io::Result<()>,
	) -> Result<()> {
		let trash_dir = self.root.join(".trash");
		let entries = match fs::read_dir(&trash_dir) {
			Ok(entries) => entries,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
			Err(error) => return Err(error.into()),
		};
		for entry in entries {
			let entry = entry?;
			if !entry.file_type()?.is_file() {
				continue;
			}
			let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
				continue;
			};
			let (digest, delete_only) = name
				.strip_suffix(".delete")
				.map_or((name.as_str(), false), |digest| (digest, true));
			if validate_digest_hex(digest).is_err() {
				continue;
			}
			let lease = self.leases.acquire(digest)?;
			let trash = entry.path();
			match store.stat_artifact(digest) {
				Ok(_) if delete_only => remove_if_present(&trash, remove_file)?,
				Ok(_) => {
					let expected = self.path_for(digest)?;
					if expected.exists() {
						remove_if_present(&trash, remove_file)?;
					} else {
						fs::rename(&trash, &expected)?;
					}
				},
				Err(error) if error.code == crate::ErrorCode::NotFound => {
					self.remove_remote_locked(&lease)?;
					remove_if_present(&trash, remove_file)?;
				},
				Err(error) => return Err(error),
			}
		}
		Ok(())
	}

	fn fetch_s3_to_cache(&self, digest: &str) -> Result<PathBuf> {
		let s3 = self
			.s3
			.as_ref()
			.ok_or_else(|| EngineError::engine("S3 is not configured"))?;
		let final_path = self.path_for(digest)?;
		let parent = final_path
			.parent()
			.ok_or_else(|| EngineError::engine("path has no parent"))?;
		fs::create_dir_all(parent)?;
		let temp_path = parent.join(format!(".{digest}.download-{}", Uuid::new_v4()));
		let mut file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(0o600)
			.open(&temp_path)?;

		let mut hasher = Sha256::new();
		let mut offset = 0u64;
		let chunk_size = 1024 * 1024;
		let s3_clone = s3.clone();
		let key = self.final_object_key(digest);
		loop {
			let s3_inner = s3_clone.clone();
			let key_inner = key.clone();
			let chunk = block_on(async move {
				s3_inner
					.read(&key_inner, offset, chunk_size)
					.await
					.map_err(|err| EngineError::engine(format!("failed to read chunk from S3: {err:?}")))
			})?;
			if chunk.is_empty() {
				break;
			}
			file.write_all(&chunk)?;
			hasher.update(&chunk);
			offset += chunk.len() as u64;
		}
		file.sync_all()?;
		drop(file);

		let actual = hex::encode(hasher.finalize());
		if actual != digest {
			let _ = fs::remove_file(&temp_path);
			return Err(EngineError::engine("S3 downloaded artifact digest mismatch"));
		}

		if final_path.exists() {
			let _ = fs::remove_file(&temp_path);
		} else {
			fs::rename(&temp_path, &final_path)?;
		}
		Ok(final_path)
	}

	fn open_file(&self, digest: &str) -> Result<File> {
		let path = self.path_for(digest)?;
		match File::open(&path) {
			Ok(file) => Ok(file),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound && self.s3.is_some() => {
				let cached_path = self.fetch_s3_to_cache(digest)?;
				File::open(cached_path).map_err(Into::into)
			},
			Err(error) => Err(error.into()),
		}
	}
}
static ARTIFACT_BLOCKING_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.worker_threads(1)
		.thread_name("artifact-blocking")
		.build()
		.expect("create artifact blocking runtime")
});

fn block_on<F: Future>(future: F) -> F::Output {
	match tokio::runtime::Handle::try_current() {
		Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
		Err(_) => ARTIFACT_BLOCKING_RUNTIME.block_on(future),
	}
}

fn stream_reader_to_channel(
	mut reader: impl Read,
	tx: mpsc::Sender<std::io::Result<Bytes>>,
) -> std::io::Result<()> {
	let mut buffer = vec![0u8; 8 * 1024 * 1024];
	loop {
		let count = match reader.read(&mut buffer) {
			Ok(count) => count,
			Err(error) => {
				let _ = tx.blocking_send(Err(std::io::Error::new(error.kind(), error.to_string())));
				return Err(error);
			},
		};
		if count == 0 {
			return Ok(());
		}
		if tx
			.blocking_send(Ok(Bytes::copy_from_slice(&buffer[..count])))
			.is_err()
		{
			return Ok(());
		}
	}
}

fn staging_digest<'a>(staging_prefix: &str, key: &'a str) -> Option<&'a str> {
	let name = key.strip_prefix(staging_prefix)?.strip_prefix('/')?;
	if name.len() < SHA256_BYTES * 2 {
		return None;
	}
	let (digest, attempt) = name.split_at(SHA256_BYTES * 2);
	if !attempt.starts_with('-') || Uuid::parse_str(&attempt[1..]).is_err() {
		return None;
	}
	validate_digest_hex(digest).ok().map(|()| digest)
}

fn verify_s3_artifact(
	s3: &S3Client,
	key: &str,
	expected_digest: &[u8; SHA256_BYTES],
	expected_size: u64,
) -> Result<()> {
	let stat = block_on(s3.head_object(key))
		.map_err(|error| EngineError::engine(format!("verifying uploaded artifact HEAD: {error}")))?;
	if stat.size != expected_size {
		return Err(EngineError::engine(format!(
			"uploaded artifact size mismatch: expected {expected_size}, got {}",
			stat.size
		)));
	}
	let mut hasher = Sha256::new();
	let mut offset = 0;
	while offset < expected_size {
		let wanted = (expected_size - offset).min(1 << 20) as u32;
		let bytes =
			block_on(s3.read_range_if_match(key, &stat, offset, wanted)).map_err(|error| {
				EngineError::engine(format!("verifying uploaded artifact bytes: {error}"))
			})?;
		hasher.update(&bytes);
		offset += bytes.len() as u64;
	}
	if hasher.finalize().as_slice() != expected_digest {
		return Err(EngineError::engine("uploaded artifact SHA-256 digest mismatch"));
	}
	Ok(())
}

fn remove_if_present(
	path: &Path,
	remove_file: &mut impl FnMut(&Path) -> std::io::Result<()>,
) -> Result<()> {
	match remove_file(path) {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(error.into()),
	}
}

fn verify_file(path: &Path, digest: &str, expected_size: u64) -> Result<()> {
	let mut file = File::open(path)?;
	verify_open_file(&mut file, digest, Some(expected_size))?;
	Ok(())
}

fn verify_open_file(file: &mut File, digest: &str, expected_size: Option<u64>) -> Result<u64> {
	let size = file.metadata()?.len();
	if let Some(expected_size) = expected_size
		&& size != expected_size
	{
		return Err(EngineError::engine(format!("artifact {digest} has corrupt size")));
	}
	let mut hasher = Sha256::new();
	let mut buffer = vec![0u8; 64 * 1024];
	loop {
		let read = file.read(&mut buffer)?;
		if read == 0 {
			break;
		}
		hasher.update(&buffer[..read]);
	}
	if hex::encode(hasher.finalize()) != digest {
		return Err(EngineError::engine(format!("artifact {digest} failed digest verification")));
	}
	file.seek(SeekFrom::Start(0))?;
	Ok(size)
}

fn validate_digest_hex(digest: &str) -> Result<()> {
	if digest.len() != SHA256_BYTES * 2
		|| !digest
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
	{
		return Err(EngineError::invalid(
			"artifact digest must be 64 lowercase hexadecimal characters",
		));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::os::unix::fs::PermissionsExt;

	use super::*;

	#[test]
	fn producer_forwards_reader_error_instead_of_signaling_eof() {
		struct FailingReader {
			read_once: bool,
		}
		impl Read for FailingReader {
			fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
				if self.read_once {
					return Err(std::io::Error::other("injected reader failure"));
				}
				self.read_once = true;
				buffer[..3].copy_from_slice(b"abc");
				Ok(3)
			}
		}

		let (sender, mut receiver) = mpsc::channel(2);
		let error = stream_reader_to_channel(FailingReader { read_once: false }, sender).unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::Other);
		assert_eq!(receiver.blocking_recv().unwrap().unwrap(), Bytes::from_static(b"abc"));
		assert!(receiver.blocking_recv().unwrap().is_err());
	}

	#[tokio::test]
	async fn remote_verification_rejects_completed_digest_mismatch() {
		use axum::{
			Router,
			body::Body,
			http::{HeaderValue, Request, StatusCode},
			response::{IntoResponse, Response},
			routing::any,
		};

		async fn corrupt_object(request: Request<Body>) -> Response {
			let mut response = match *request.method() {
				reqwest::Method::HEAD => (StatusCode::OK, "").into_response(),
				reqwest::Method::GET => (StatusCode::OK, "evil").into_response(),
				_ => (StatusCode::METHOD_NOT_ALLOWED, "").into_response(),
			};
			response
				.headers_mut()
				.insert(reqwest::header::CONTENT_LENGTH, HeaderValue::from_static("4"));
			response.headers_mut().insert(
				reqwest::header::LAST_MODIFIED,
				HeaderValue::from_static("Mon, 01 Jan 2024 00:00:00 GMT"),
			);
			response
				.headers_mut()
				.insert(reqwest::header::ETAG, HeaderValue::from_static("\"etag\""));
			response
		}

		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = format!("http://{}", listener.local_addr().unwrap());
		let server = tokio::spawn(async move {
			axum::serve(listener, Router::new().fallback(any(corrupt_object)))
				.await
				.unwrap();
		});
		let s3 = S3Client::new(S3MountConfig {
			bucket:    "bucket".to_owned(),
			prefix:    String::new(),
			region:    "us-east-1".to_owned(),
			endpoint:  Some(endpoint),
			read_only: false,
			creds:     None,
			auth:      S3Auth::Anonymous,
		})
		.unwrap();
		let expected: [u8; SHA256_BYTES] = Sha256::digest(b"good").into();
		let result =
			tokio::task::spawn_blocking(move || verify_s3_artifact(&s3, "digest-key", &expected, 4))
				.await
				.unwrap();
		assert!(result.is_err());
		server.abort();
	}
	#[test]
	fn atomic_round_trip_and_validation() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let bytes = b"immutable payload";
		let digest = Sha256::digest(bytes);
		let record = store
			.put_verified(&digest, bytes.len() as u64, bytes)
			.unwrap();
		assert_eq!(store.read(&record.digest, Some(record.size)).unwrap(), bytes);
		let expected = (record.digest.clone(), record.size, record.path.clone());
		drop(record);
		let duplicate = store.put(bytes).unwrap();
		assert_eq!((duplicate.digest.clone(), duplicate.size, duplicate.path.clone()), expected);
		drop(duplicate);
		assert!(store.put_verified(&digest, 1, bytes).is_err());
		assert!(
			store
				.put_verified(&[0; 32], bytes.len() as u64, bytes)
				.is_err()
		);
		assert!(store.read("../state.sqlite3", None).is_err());
	}

	#[test]
	fn incremental_large_upload_enforces_quota_and_abort_cleanup() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let bytes = vec![0x5a; 3 * 1024 * 1024 + 17];
		let digest = Sha256::digest(&bytes);
		assert!(
			store
				.begin_put(&digest, bytes.len() as u64, (bytes.len() - 1) as u64)
				.is_err()
		);
		{
			let mut writer = store
				.begin_put(&digest, bytes.len() as u64, bytes.len() as u64)
				.unwrap();
			writer.write_chunk(&bytes[..1024]).unwrap();
		}
		let mut writer = store
			.begin_put(&digest, bytes.len() as u64, bytes.len() as u64)
			.unwrap();
		assert_eq!(
			fs::metadata(&writer.temporary)
				.unwrap()
				.permissions()
				.mode() & 0o777,
			0o600
		);
		for chunk in bytes.chunks(128 * 1024) {
			writer.write_chunk(chunk).unwrap();
		}
		let record = writer.finalize().unwrap();
		assert_eq!(store.read(&record.digest, Some(record.size)).unwrap(), bytes);
		let parent = record.path.parent().unwrap();
		assert!(
			!fs::read_dir(parent)
				.unwrap()
				.flatten()
				.any(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
		);
	}

	#[test]
	fn concurrent_incremental_writers_dedupe_canonically() {
		let temp = tempfile::tempdir().unwrap();
		let home = crate::home::Home::new(temp.path());
		let metadata = super::super::store::Store::open(&home).unwrap();
		let store = ArtifactStore::open(temp.path().join("artifacts")).unwrap();
		let bytes = vec![7u8; 256 * 1024];
		let digest = Sha256::digest(&bytes);
		let records = std::thread::scope(|scope| {
			let mut threads = Vec::new();
			for _ in 0..4 {
				threads.push(scope.spawn(|| {
					let mut writer = store
						.begin_put(&digest, bytes.len() as u64, bytes.len() as u64)
						.unwrap();
					for chunk in bytes.chunks(8192) {
						writer.write_chunk(chunk).unwrap();
					}
					let stored = writer.finalize().unwrap();
					metadata
						.record_stored_artifact(&stored, None, 1, None)
						.unwrap();
					(stored.digest, stored.size, stored.path)
				}));
			}
			threads
				.into_iter()
				.map(|thread| thread.join().unwrap())
				.collect::<Vec<_>>()
		});
		assert!(records.windows(2).all(|pair| pair[0] == pair[1]));
		assert_eq!(store.read(&records[0].0, Some(records[0].1)).unwrap(), bytes);
	}

	#[test]
	fn verified_reader_streams_large_artifact_in_caller_sized_chunks() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let bytes: Vec<u8> = (0..5 * 1024 * 1024 + 31)
			.map(|index| (index % 251) as u8)
			.collect();
		let record = store.put(&bytes).unwrap();

		let mut reader = store
			.open_verified(&record.digest, Some(record.size))
			.unwrap();
		assert_eq!(reader.len(), record.size);
		assert!(!reader.is_empty());
		let mut buffer = [0u8; 8191];
		let mut consumed = 0u64;
		let mut largest_read = 0usize;
		let mut consumed_hash = Sha256::new();
		loop {
			let count = reader.read(&mut buffer).unwrap();
			if count == 0 {
				break;
			}
			largest_read = largest_read.max(count);
			consumed += count as u64;
			consumed_hash.update(&buffer[..count]);
		}
		assert_eq!(consumed, record.size);
		assert!(largest_read <= buffer.len());
		assert_eq!(hex::encode(consumed_hash.finalize()), record.digest);

		let range_start = 2 * 1024 * 1024 + 7;
		reader.seek(SeekFrom::Start(range_start as u64)).unwrap();
		let mut range = [0u8; 4093];
		reader.read_exact(&mut range).unwrap();
		assert_eq!(&range, &bytes[range_start..range_start + range.len()]);
	}

	#[test]
	fn bounded_read_rejects_oversize_artifact_before_digest_consumption() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let record = store.put(&vec![0x6b; 1024 * 1024]).unwrap();
		fs::write(&record.path, vec![0x7c; record.size as usize]).unwrap();

		let error = store
			.read_bounded(&record.digest, Some(record.size), 64 * 1024)
			.unwrap_err();
		assert!(error.to_string().contains("exceeds in-memory read limit"));
	}

	#[test]
	fn gc_retries_trashed_file_after_remove_failure_and_reopen() {
		let temp = tempfile::tempdir().unwrap();
		let home = crate::home::Home::new(temp.path());
		let metadata = super::super::store::Store::open(&home).unwrap();
		let artifact_root = temp.path().join("artifact-cas");
		let artifacts = ArtifactStore::open(&artifact_root).unwrap();
		let record = artifacts.put(b"expired").unwrap();
		metadata
			.record_artifact(
				&record.digest,
				record.size,
				None,
				record.path.to_str().unwrap(),
				1,
				Some(2),
			)
			.unwrap();
		let digest = record.digest.clone();
		let size = record.size;
		let path = record.path.clone();
		drop(record);

		let error = artifacts
			.gc_expired_with_remove(&metadata, 3, 100, |_| {
				Err(std::io::Error::new(
					std::io::ErrorKind::PermissionDenied,
					"injected remove failure",
				))
			})
			.unwrap_err();
		assert!(error.to_string().contains("injected remove failure"));
		assert_eq!(metadata.stat_artifact(&digest).unwrap_err().code, crate::ErrorCode::NotFound);
		assert!(!path.exists());
		let trash = artifact_root.join(".trash").join(&digest);
		assert!(trash.is_file());

		drop(artifacts);
		let reopened = ArtifactStore::open(&artifact_root).unwrap();
		assert_eq!(reopened.gc_expired(&metadata, 4, 100).unwrap(), 0);
		assert!(!trash.exists());
		assert!(reopened.read(&digest, Some(size)).is_err());
	}

	#[test]
	fn verified_reader_rejects_corrupt_and_truncated_content() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let corrupt = store.put(b"good").unwrap();
		fs::write(&corrupt.path, b"evil").unwrap();
		assert!(
			store
				.open_verified(&corrupt.digest, Some(corrupt.size))
				.is_err()
		);

		let truncated = store.put(&vec![0x42; 128 * 1024]).unwrap();
		File::options()
			.write(true)
			.open(&truncated.path)
			.unwrap()
			.set_len(truncated.size - 1)
			.unwrap();
		assert!(
			store
				.open_verified(&truncated.digest, Some(truncated.size))
				.is_err()
		);
		assert!(store.open_verified(&truncated.digest, None).is_err());
	}
}
