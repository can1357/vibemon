//! Atomic content-addressed storage for durable function payloads.

use std::{
	fs::{self, File, OpenOptions},
	io::{Read, Write},
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
	pub size: u64,
	/// Absolute path of the immutable file.
	pub path: PathBuf,
}

/// Filesystem-backed immutable SHA-256 content store.
#[derive(Clone, Debug)]
pub struct ArtifactStore {
	root: PathBuf,
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

	/// Commit bytes after verifying the caller-provided digest and size.
	///
	/// The temporary file is synced before an atomic rename. Existing content is
	/// opened and verified instead of overwritten.
	pub fn put_verified(&self, expected_digest: &[u8], expected_size: u64, bytes: &[u8]) -> Result<StoredArtifact> {
		if expected_digest.len() != SHA256_BYTES {
			return Err(EngineError::invalid("artifact digest must be a 32-byte SHA-256 digest"));
		}
		if bytes.len() as u64 != expected_size {
			return Err(EngineError::invalid(format!(
				"artifact size mismatch: expected {expected_size}, received {}",
				bytes.len()
			)));
		}
		let actual = Sha256::digest(bytes);
		if actual.as_slice() != expected_digest {
			return Err(EngineError::invalid("artifact SHA-256 digest mismatch"));
		}
		self.put_digest_hex(&hex::encode(expected_digest), bytes)
	}

	/// Hash and atomically commit bytes.
	pub fn put(&self, bytes: &[u8]) -> Result<StoredArtifact> {
		let digest = Sha256::digest(bytes);
		self.put_digest_hex(&hex::encode(digest), bytes)
	}

	/// Read an artifact and verify both its path digest and bytes.
	pub fn read(&self, digest: &str, expected_size: Option<u64>) -> Result<Vec<u8>> {
		let path = self.path_for(digest)?;
		let mut file = File::open(&path)
			.map_err(|error| if error.kind() == std::io::ErrorKind::NotFound {
				EngineError::not_found(format!("artifact {digest} not found"))
			} else { error.into() })?;
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
				return Err(EngineError::engine(format!("artifact {digest} has an invalid persisted path")));
			}
			if store.delete_unreferenced_artifact(&digest, now_ms)? {
				self.remove(&digest)?;
				removed += 1;
			}
		}
		Ok(removed)
	}

	fn put_digest_hex(&self, digest: &str, bytes: &[u8]) -> Result<StoredArtifact> {
		let path = self.path_for(digest)?;
		let parent = path.parent().ok_or_else(|| EngineError::engine("artifact path has no parent"))?;
		fs::create_dir_all(parent)?;
		if path.exists() {
			let existing = self.read(digest, Some(bytes.len() as u64))?;
			if existing != bytes {
				return Err(EngineError::engine("content-address collision or artifact corruption"));
			}
			return Ok(StoredArtifact { digest: digest.to_owned(), size: bytes.len() as u64, path });
		}

		let temporary = parent.join(format!(".{}.tmp-{}", digest, Uuid::new_v4()));
		let result = (|| -> Result<()> {
			let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
			file.write_all(bytes)?;
			file.sync_all()?;
			match fs::rename(&temporary, &path) {
				Ok(()) => {}
				Err(_error) if path.exists() => {
					let _ = fs::remove_file(&temporary);
					let existing = self.read(digest, Some(bytes.len() as u64))?;
					if existing != bytes {
						return Err(EngineError::engine("content-address collision or artifact corruption"));
					}
				}
				Err(error) => return Err(error.into()),
			}
			File::open(parent)?.sync_all()?;
			Ok(())
		})();
		if result.is_err() {
			let _ = fs::remove_file(&temporary);
		}
		result?;
		Ok(StoredArtifact { digest: digest.to_owned(), size: bytes.len() as u64, path })
	}
}

fn validate_digest_hex(digest: &str) -> Result<()> {
	if digest.len() != SHA256_BYTES * 2
		|| !digest.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
	{
		return Err(EngineError::invalid("artifact digest must be 64 lowercase hexadecimal characters"));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn atomic_round_trip_and_validation() {
		let temp = tempfile::tempdir().unwrap();
		let store = ArtifactStore::open(temp.path()).unwrap();
		let bytes = b"immutable payload";
		let digest = Sha256::digest(bytes);
		let record = store.put_verified(&digest, bytes.len() as u64, bytes).unwrap();
		assert_eq!(store.read(&record.digest, Some(record.size)).unwrap(), bytes);
		assert_eq!(store.put(bytes).unwrap(), record);
		assert!(store.put_verified(&digest, 1, bytes).is_err());
		assert!(store.put_verified(&[0; 32], bytes.len() as u64, bytes).is_err());
		assert!(store.read("../state.sqlite3", None).is_err());
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
