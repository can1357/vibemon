//! Startup reclamation for plaintext capture and extraction staging.
//!
//! Only direct children of the security runtime directory are eligible.  The
//! caller supplies paths rehydrated from durable lifecycle records; canonical
//! paths at or below a candidate keep that candidate intact.

use std::{
	fs, io,
	path::{Path, PathBuf},
};

/// Remove crash-left plaintext extraction and capture staging directories that
/// no live record uses. Migration roots deliberately belong to the later mesh
/// reconciliation pass and are not candidates here.
pub fn reclaim_orphaned_staging(runtime_root: &Path, preserve_paths: &[PathBuf]) -> io::Result<()> {
	reclaim_orphaned_directories(runtime_root, preserve_paths, is_plaintext_staging_name)
}

/// Remove mesh migration roots after mesh state has identified its live paths.
///
/// A migration root is a direct `migration-<lowercase-hyphenated-uuid>`
/// directory. Preserve paths may name that root or any descendant.
pub fn reclaim_orphaned_migration_staging(
	runtime_root: &Path,
	preserve_paths: &[PathBuf],
) -> io::Result<()> {
	reclaim_orphaned_directories(runtime_root, preserve_paths, is_migration_staging_name)
}

/// Reclaim direct managed children of `runtime_root`.
///
/// Existing preserve paths are canonicalized. Missing preserve paths are
/// normalized lexically only when already beneath the canonical runtime root;
/// this retains a live staging root while its named child is not yet present.
/// Preserve paths outside the root are ignored. Other validation failures are
/// returned so startup does not silently reclaim a path it could not validate.
fn reclaim_orphaned_directories(
	runtime_root: &Path,
	preserve_paths: &[PathBuf],
	is_managed_name: fn(&str) -> bool,
) -> io::Result<()> {
	let root_metadata = match fs::symlink_metadata(runtime_root) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(error),
	};
	if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"staging runtime root must be a real directory",
		));
	}
	let runtime_root = fs::canonicalize(runtime_root)?;
	let preserve_paths = preserve_paths
		.iter()
		.filter_map(|path| normalize_preserve_path(&runtime_root, path).transpose())
		.collect::<io::Result<Vec<_>>>()?;

	let mut removed = false;
	for entry in fs::read_dir(&runtime_root)? {
		let entry = entry?;
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			continue;
		};
		if !is_managed_name(name) {
			continue;
		}

		let kind = entry.file_type()?;
		if kind.is_symlink() || !kind.is_dir() {
			continue;
		}
		let candidate = fs::canonicalize(entry.path())?;
		if !candidate.starts_with(&runtime_root)
			|| preserve_paths
				.iter()
				.any(|preserve| preserve.starts_with(&candidate))
		{
			continue;
		}

		// Recheck immediately before removal: directory entries may have changed
		// since `read_dir`, and links are never reclamation targets.
		let metadata = fs::symlink_metadata(&candidate)?;
		if metadata.file_type().is_symlink() || !metadata.is_dir() {
			continue;
		}
		fs::remove_dir_all(&candidate)?;
		removed = true;
	}
	if removed {
		fsync_directory(&runtime_root)?;
	}
	Ok(())
}

fn normalize_preserve_path(runtime_root: &Path, path: &Path) -> io::Result<Option<PathBuf>> {
	match fs::canonicalize(path) {
		Ok(path) => Ok(path.starts_with(runtime_root).then_some(path)),
		Err(error) if error.kind() == io::ErrorKind::NotFound => {
			let Some(path) = lexically_normalize(path) else {
				return Ok(None);
			};
			Ok(path.starts_with(runtime_root).then_some(path))
		},
		Err(error) => Err(error),
	}
}

fn lexically_normalize(path: &Path) -> Option<PathBuf> {
	let mut normalized = PathBuf::new();
	for component in path.components() {
		match component {
			std::path::Component::Normal(component) => normalized.push(component),
			std::path::Component::CurDir => {},
			std::path::Component::ParentDir => {
				if !normalized.pop() {
					return None;
				}
			},
			std::path::Component::RootDir | std::path::Component::Prefix(_) => {
				normalized.push(component.as_os_str());
			},
		}
	}
	Some(normalized)
}

fn is_plaintext_staging_name(name: &str) -> bool {
	name.starts_with("capture-") || name.starts_with("snapshot-") || name.starts_with("recovery-")
}

fn is_migration_staging_name(name: &str) -> bool {
	let Some(uuid) = name.strip_prefix("migration-") else {
		return false;
	};
	uuid::Uuid::parse_str(uuid).is_ok_and(|parsed| parsed.hyphenated().to_string() == uuid)
}

#[cfg(unix)]
fn fsync_directory(path: &Path) -> io::Result<()> {
	fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn fsync_directory(_: &Path) -> io::Result<()> {
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::{fs, io, path::Path};

	use tempfile::TempDir;

	use super::{reclaim_orphaned_migration_staging, reclaim_orphaned_staging};

	fn runtime(temp: &TempDir) -> std::path::PathBuf {
		let runtime = temp.path().join("runtime");
		fs::create_dir(&runtime).expect("runtime directory");
		runtime
	}

	fn directory(path: &Path) {
		fs::create_dir_all(path).expect("directory");
	}

	#[test]
	fn deletes_orphaned_managed_directories() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		for name in ["capture-old", "snapshot-old", "recovery-old"] {
			directory(&runtime.join(name));
		}

		reclaim_orphaned_staging(&runtime, &[]).expect("cleanup");

		for name in ["capture-old", "snapshot-old", "recovery-old"] {
			assert!(!runtime.join(name).exists(), "{name} should be reclaimed");
		}
	}

	#[test]
	fn early_cleanup_preserves_migration_roots() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		let migration = runtime.join("migration-6c2f7e0b-71a5-4c15-98c1-4e8712e4f6c8");
		directory(&migration);

		reclaim_orphaned_staging(&runtime, &[]).expect("early cleanup");

		assert!(migration.is_dir());
	}

	#[test]
	fn late_cleanup_deletes_orphans_and_preserves_delta_children() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		let orphan = runtime.join("migration-6c2f7e0b-71a5-4c15-98c1-4e8712e4f6c8");
		let live_root = runtime.join("migration-1c856b8e-5c8c-46bf-9c18-c5d5ad19d837");
		let delta = live_root.join("delta");
		directory(&orphan);
		directory(&delta);

		reclaim_orphaned_migration_staging(&runtime, std::slice::from_ref(&delta))
			.expect("late cleanup");

		assert!(!orphan.exists());
		assert!(delta.is_dir());
	}

	#[test]
	fn late_cleanup_ignores_malformed_migration_names() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		let malformed = runtime.join("migration-NOT-A-UUID");
		directory(&malformed);

		reclaim_orphaned_migration_staging(&runtime, &[]).expect("late cleanup");

		assert!(malformed.is_dir());
	}

	#[cfg(unix)]
	#[test]
	fn late_cleanup_skips_migration_symlinks() {
		use std::os::unix::fs::symlink;

		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		let outside = temp.path().join("outside");
		directory(&outside);
		let link = runtime.join("migration-6c2f7e0b-71a5-4c15-98c1-4e8712e4f6c8");
		symlink(&outside, &link).expect("symlink");

		reclaim_orphaned_migration_staging(&runtime, &[]).expect("late cleanup");

		assert!(
			fs::symlink_metadata(&link)
				.expect("link remains")
				.file_type()
				.is_symlink()
		);
	}

	#[test]
	fn preserves_exact_and_nested_live_sources() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		let exact = runtime.join("capture-active");
		let nested = runtime.join("snapshot-active").join("disk.img");
		directory(&exact);
		directory(nested.parent().expect("parent"));
		fs::write(&nested, b"active").expect("live source");

		reclaim_orphaned_staging(&runtime, &[exact.clone(), nested.clone()]).expect("cleanup");

		assert!(exact.is_dir());
		assert!(nested.is_file());
	}

	#[test]
	fn leaves_unrelated_entries_untouched() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		let ordinary_dir = runtime.join("volume-data");
		let managed_file = runtime.join("capture-not-a-directory");
		directory(&ordinary_dir);
		fs::write(&managed_file, b"do not remove").expect("file");

		reclaim_orphaned_staging(&runtime, &[]).expect("cleanup");

		assert!(ordinary_dir.is_dir());
		assert_eq!(fs::read(&managed_file).expect("file remains"), b"do not remove");
	}

	#[cfg(unix)]
	#[test]
	fn leaves_symlink_escape_untouched() {
		use std::os::unix::fs::symlink;

		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		let outside = temp.path().join("outside");
		directory(&outside);
		fs::write(outside.join("secret"), b"keep").expect("outside file");
		let link = runtime.join("recovery-escape");
		symlink(&outside, &link).expect("symlink");

		reclaim_orphaned_staging(&runtime, &[]).expect("cleanup");

		assert!(
			fs::symlink_metadata(&link)
				.expect("link remains")
				.file_type()
				.is_symlink()
		);
		assert_eq!(fs::read(outside.join("secret")).expect("outside remains"), b"keep");
	}

	#[test]
	fn is_idempotent() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		directory(&runtime.join("capture-orphan"));

		reclaim_orphaned_staging(&runtime, &[]).expect("first cleanup");
		reclaim_orphaned_staging(&runtime, &[]).expect("second cleanup");
		assert!(!runtime.join("capture-orphan").exists());
	}

	#[test]
	fn ignores_missing_preserve_paths() {
		let temp = tempfile::tempdir().expect("temporary directory");
		let runtime = runtime(&temp);
		directory(&runtime.join("recovery-orphan"));

		reclaim_orphaned_staging(&runtime, &[runtime.join("recovery-missing")]).expect("cleanup");
		assert!(!runtime.join("recovery-orphan").exists());
	}

	#[test]
	fn rejects_a_symlink_runtime_root() {
		#[cfg(unix)]
		{
			use std::os::unix::fs::symlink;

			let temp = tempfile::tempdir().expect("temporary directory");
			let real_root = temp.path().join("real-runtime");
			directory(&real_root);
			let linked_root = temp.path().join("runtime");
			symlink(&real_root, &linked_root).expect("symlink");
			let error = reclaim_orphaned_staging(&linked_root, &[]).expect_err("link root rejected");
			assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
		}
	}
}
