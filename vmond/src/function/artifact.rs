//! Atomic content-addressed storage for durable function payloads.

use std::{
	fs::{self, File, OpenOptions},
	io::{Read, Write},
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

	/// Read an artifact and verify both its path digest and bytes.
	pub fn read(&self, digest: &str, expected_size: Option<u64>) -> Result<Vec<u8>> {
		let path = self.path_for(digest)?;
		let mut file = File::open(&path).map_err(|error| {
			if error.kind() == std::io::ErrorKind::NotFound {
				EngineError::not_found(format!("artifact {digest} not found"))
			} else {
				error.into()
			}
		})?;
		let metadata = file.metadata()?;
		if let Some(size) = expected_size {
			if metadata.len() != size {
				return Err(EngineError::engine(format!("artifact {digest} has corrupt size")));
			}
		}
		let mut bytes = Vec::with_capacity(metadata.len().try_into().unwrap_or(0));
		file.read_to_end(&mut bytes)?;
		let actual = hex::encode(Sha256::digest(&bytes));
		if actual != digest {
			return Err(EngineError::engine(format!("artifact {digest} failed digest verification")));
		}
		Ok(bytes)
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

	/// Delete expired, unreferenced artifacts using metadata compare-and-delete.
	///
	/// The database path is required to match the digest-derived path before
	/// metadata is removed, preventing a corrupt row from escaping this root.
	pub fn gc_expired(&self, store: &super::store::Store, now_ms: u64, limit: u32) -> Result<u64> {
		let candidates = store.unreferenced_expired_artifacts(now_ms, limit)?;
		let mut removed = 0;
		for (digest, persisted_path) in candidates {
			let expected = self.path_for(&digest)?;
			if Path::new(&persisted_path) != expected {
				return Err(EngineError::engine(format!(
					"artifact {digest} has an invalid persisted path"
				)));
			}
			if store.delete_unreferenced_artifact(&digest, now_ms)? {
				self.remove(&digest)?;
				removed += 1;
			}
		}
		Ok(removed)
	}
}

fn verify_file(path: &Path, digest: &str, expected_size: u64) -> Result<()> {
	let mut file = File::open(path)?;
	if file.metadata()?.len() != expected_size {
		return Err(EngineError::engine(format!("artifact {digest} has corrupt size")));
	}
	let mut hasher = Sha256::new();
	let mut buffer = [0u8; 64 * 1024];
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
	Ok(())
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
		let mut threads = Vec::new();
		for _ in 0..4 {
			let store = store.clone();
			let bytes = bytes.clone();
			let digest = digest.to_vec();
			threads.push(std::thread::spawn(move || {
				let mut writer = store
					.begin_put(&digest, bytes.len() as u64, bytes.len() as u64)
					.unwrap();
				for chunk in bytes.chunks(8192) {
					writer.write_chunk(chunk).unwrap();
				}
				writer.finalize().unwrap()
			}));
		}
		let records: Vec<_> = threads
			.into_iter()
			.map(|thread| thread.join().unwrap())
			.collect();
		assert!(records.windows(2).all(|pair| pair[0] == pair[1]));
		assert_eq!(
			store
				.read(&records[0].digest, Some(records[0].size))
				.unwrap(),
			bytes
		);
	}

	#[test]
	fn detects_corrupt_content() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let record = store.put(b"good").unwrap();
		fs::write(&record.path, b"evil").unwrap();
		assert!(store.read(&record.digest, Some(record.size)).is_err());
	}
}
