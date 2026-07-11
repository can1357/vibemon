//! Atomic content-addressed storage for durable function payloads.

use std::{
	fs::{self, File, OpenOptions},
	io::{Read, Seek, SeekFrom, Write},
	os::unix::fs::OpenOptionsExt,
	path::{Path, PathBuf},
};

use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{EngineError, Result};

/// SHA-256 digest length in bytes.
pub const SHA256_BYTES: usize = 32;

/// Metadata returned after an artifact has been durably committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredArtifact {
	/// Lowercase hexadecimal SHA-256 digest.
	pub digest: String,
	/// Exact byte length of the content.
	pub size:   u64,
	/// Absolute path of the immutable file.
	pub path:   PathBuf,
}

/// Filesystem-backed immutable SHA-256 content store.
#[derive(Clone, Debug)]
pub struct ArtifactStore {
	root: PathBuf,
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
		let parent = self
			.final_path
			.parent()
			.ok_or_else(|| EngineError::engine("artifact path has no parent"))?;
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
		Ok(StoredArtifact { digest, size: self.expected_size, path: self.final_path.clone() })
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
	/// Open (and create) an artifact directory.
	pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
		let root = root.into();
		fs::create_dir_all(&root)?;
		Ok(Self { root })
	}

	/// Return the artifact root.
	pub fn root(&self) -> &Path {
		&self.root
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

	/// Remove an artifact file. Missing files are treated as already removed.
	pub fn remove(&self, digest: &str) -> Result<()> {
		let path = self.path_for(digest)?;
		match fs::remove_file(path) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error.into()),
		}
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
		for (digest, persisted_path) in candidates {
			let expected = self.path_for(&digest)?;
			if Path::new(&persisted_path) != expected {
				return Err(EngineError::engine(format!(
					"artifact {digest} has an invalid persisted path"
				)));
			}
			let trash = trash_dir.join(&digest);
			fs::rename(&expected, &trash)?;
			if store.delete_unreferenced_artifact(&digest, now_ms)? {
				remove_if_present(&trash, &mut remove_file)?;
				removed += 1;
			} else {
				fs::rename(&trash, &expected)?;
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
			let Some(digest) = entry.file_name().to_str().map(str::to_owned) else {
				continue;
			};
			if validate_digest_hex(&digest).is_err() {
				continue;
			}
			let trash = entry.path();
			match store.stat_artifact(&digest) {
				Ok(_) => {
					let expected = self.path_for(&digest)?;
					if expected.exists() {
						remove_if_present(&trash, remove_file)?;
					} else {
						fs::rename(&trash, &expected)?;
					}
				},
				Err(error) if error.code == crate::ErrorCode::NotFound => {
					remove_if_present(&trash, remove_file)?;
				},
				Err(error) => return Err(error),
			}
		}
		Ok(())
	}

	fn open_file(&self, digest: &str) -> Result<File> {
		let path = self.path_for(digest)?;
		File::open(path).map_err(|error| {
			if error.kind() == std::io::ErrorKind::NotFound {
				EngineError::not_found(format!("artifact {digest} not found"))
			} else {
				error.into()
			}
		})
	}
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
	fn atomic_round_trip_and_validation() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let bytes = b"immutable payload";
		let digest = Sha256::digest(bytes);
		let record = store
			.put_verified(&digest, bytes.len() as u64, bytes)
			.unwrap();
		assert_eq!(store.read(&record.digest, Some(record.size)).unwrap(), bytes);
		assert_eq!(store.put(bytes).unwrap(), record);
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
		let store = ArtifactStore::open(temp.path()).unwrap();
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
					writer.finalize().unwrap()
				}));
			}
			threads
				.into_iter()
				.map(|thread| thread.join().unwrap())
				.collect::<Vec<_>>()
		});
		assert!(records.windows(2).all(|pair| pair[0] == pair[1]));
		assert_eq!(
			store
				.read(&records[0].digest, Some(records[0].size))
				.unwrap(),
			bytes
		);
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

		let error = artifacts
			.gc_expired_with_remove(&metadata, 3, 100, |_| {
				Err(std::io::Error::new(
					std::io::ErrorKind::PermissionDenied,
					"injected remove failure",
				))
			})
			.unwrap_err();
		assert!(error.to_string().contains("injected remove failure"));
		assert_eq!(
			metadata.stat_artifact(&record.digest).unwrap_err().code,
			crate::ErrorCode::NotFound
		);
		assert!(!record.path.exists());
		let trash = artifact_root.join(".trash").join(&record.digest);
		assert!(trash.is_file());

		drop(artifacts);
		let reopened = ArtifactStore::open(&artifact_root).unwrap();
		assert_eq!(reopened.gc_expired(&metadata, 4, 100).unwrap(), 0);
		assert!(!trash.exists());
		assert!(reopened.read(&record.digest, Some(record.size)).is_err());
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
