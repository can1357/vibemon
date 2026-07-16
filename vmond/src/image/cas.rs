//! Template CAS pointers and digests. Port of python/vmon/cas.py.

use std::{
	collections::BTreeMap,
	fs::{self, File},
	io::{Read, Write},
	os::unix::fs::PermissionsExt,
	path::{Path, PathBuf},
	sync::{LazyLock, Mutex},
	time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{EngineError, Result};

const CHUNK_SIZE: usize = 1024 * 1024;
const MARKER_NAME: &str = "agent-ready.json";
const VOLATILE_MARKER_FIELD: &str = "content_digest";

/// Serializes pointer publication with exact-pointer cleanup in this process.
static POINTER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn pointer_lock() -> std::sync::MutexGuard<'static, ()> {
	POINTER_LOCK
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
}
/// CAS pointer payload stored under `$VMON_HOME/cas/<digest>.json`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CasPointer {
	pub template_dir: String,
	pub tpl_name:     String,
	pub created_unix: u64,
}

/// Return the on-disk root for template digest pointers.
pub fn cas_root() -> Result<PathBuf> {
	let root = crate::home::state_dir().join("cas");
	fs::create_dir_all(&root)?;
	Ok(root)
}

/// Return the stable legacy SHA-256 digest for a template's bootable files.
///
/// This content-only digest identifies template CAS pointers and deliberately
/// remains compatible with existing pointer names. Zero runs are encoded
/// independently of physical holes.
pub fn template_digest(template_dir: &Path) -> Result<String> {
	const ZERO_BLOCK: usize = 64 * 1024;
	let mut digest = Sha256::new();
	for path in regular_files(template_dir)? {
		let rel = template_rel_path(template_dir, &path)?;
		digest.update(rel.as_bytes());
		digest.update(b"\0");
		if path.file_name().and_then(|name| name.to_str()) == Some(MARKER_NAME)
			&& path.parent() == Some(template_dir)
		{
			digest.update(marker_bytes(&path)?);
			continue;
		}
		let mut file = File::open(&path)?;
		let mut buf = vec![0_u8; CHUNK_SIZE];
		let mut zero_run = 0u64;
		loop {
			let mut n = 0usize;
			while n < buf.len() {
				let got = file.read(&mut buf[n..])?;
				if got == 0 {
					break;
				}
				n += got;
			}
			if n == 0 {
				break;
			}
			let mut cursor = 0usize;
			while cursor < n {
				let end = n.min(cursor + ZERO_BLOCK);
				if crate::mesh::bundle::is_zero(&buf[cursor..end]) {
					zero_run += (end - cursor) as u64;
				} else {
					if zero_run > 0 {
						digest.update([0u8]);
						digest.update(zero_run.to_le_bytes());
						zero_run = 0;
					}
					digest.update([1u8]);
					digest.update(((end - cursor) as u64).to_le_bytes());
					digest.update(&buf[cursor..end]);
				}
				cursor = end;
			}
		}
		if zero_run > 0 {
			digest.update([0u8]);
			digest.update(zero_run.to_le_bytes());
		}
	}
	Ok(hex::encode(digest.finalize()))
}

/// Return a canonical SHA-256 digest for the complete transfer bundle tree.
///
/// The digest covers every directory (including empty ones), regular file, and
/// symlink. Entry records are length- and type-framed so distinct trees cannot
/// be concatenated into the same hash input. File data encodes zero runs
/// independently of physical holes, keeping sparse and dense copies equal.
pub fn snapshot_digest(template_dir: &Path) -> Result<String> {
	let mut digest = Sha256::new();
	for entry in template_entries(template_dir)? {
		digest.update([b'E', entry.kind.tag()]);
		hash_field(&mut digest, entry.rel.as_bytes());

		match entry.kind {
			EntryKind::Directory => {
				digest.update(b"M");
				digest.update(entry.meta.permissions().mode().to_le_bytes());
			},
			EntryKind::Symlink => {
				let target = fs::read_link(&entry.path)?;
				let target = target
					.to_str()
					.ok_or_else(|| EngineError::invalid("template symlink target is not utf-8"))?;
				digest.update(b"T");
				hash_field(&mut digest, target.as_bytes());
			},
			EntryKind::File => {
				digest.update(b"M");
				digest.update(entry.meta.permissions().mode().to_le_bytes());
				digest.update(b"L");
				digest.update(entry.meta.len().to_le_bytes());
				hash_file_bytes(&mut digest, &entry.path)?;
			},
		}
	}
	Ok(hex::encode(digest.finalize()))
}

/// Record a digest-to-template pointer and return the digest.
pub fn index_template(template_dir: &Path, digest: Option<&str>) -> Result<String> {
	let digest = match digest {
		Some(value) => validate_digest(value)?.to_owned(),
		None => template_digest(template_dir)?,
	};
	let pointer = pointer_path(&digest)?;
	let _guard = pointer_lock();
	let mut created_unix = unix_now();
	let existing = read_pointer_file(&pointer).ok();
	if let Some(existing) = &existing {
		created_unix = existing.created_unix;
	}
	let payload = CasPointer {
		template_dir: template_dir.to_string_lossy().into_owned(),
		tpl_name: template_dir
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or_default()
			.to_owned(),
		created_unix,
	};
	if existing.as_ref() != Some(&payload) {
		write_pointer(&pointer, &payload)?;
	}
	Ok(digest)
}

/// Resolve a live template directory for a digest, pruning stale pointers.
pub fn lookup(digest: &str) -> Result<Option<PathBuf>> {
	let pointer = pointer_path(digest)?;
	let Ok(payload) = read_pointer_file(&pointer) else {
		let _ = fs::remove_file(&pointer);
		return Ok(None);
	};
	let template_dir = PathBuf::from(payload.template_dir);
	if template_present(&template_dir) {
		Ok(Some(template_dir))
	} else {
		let _ = fs::remove_file(&pointer);
		Ok(None)
	}
}

/// Remove a digest's CAS pointer without deleting the template directory.
pub fn drop_pointer(digest: &str) -> Result<()> {
	let Ok(pointer) = pointer_path(digest) else {
		return Ok(());
	};
	let _guard = pointer_lock();
	match fs::remove_file(pointer) {
		Ok(()) => Ok(()),
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(e) => Err(e.into()),
	}
}

/// Remove a pointer only when it still names `template_dir`.
///
/// This prevents an abandoned duplicate checkpoint from unpublishing a newer
/// identical-content checkpoint that has reused the same digest pointer.
pub fn drop_pointer_exact(digest: &str, template_dir: &Path) -> Result<bool> {
	let Ok(pointer) = pointer_path(digest) else {
		return Ok(false);
	};
	let _guard = pointer_lock();
	let Ok(payload) = read_pointer_file(&pointer) else {
		return Ok(false);
	};
	if Path::new(&payload.template_dir) != template_dir {
		return Ok(false);
	}
	match fs::remove_file(pointer) {
		Ok(()) => Ok(true),
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
		Err(e) => Err(e.into()),
	}
}

/// Return digest-to-template mappings for all live CAS pointers.
pub fn list_digests() -> Result<BTreeMap<String, String>> {
	let mut live = BTreeMap::new();
	for entry in fs::read_dir(cas_root()?)? {
		let entry = entry?;
		let path = entry.path();
		if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
			continue;
		}
		let Some(digest) = path.file_stem().and_then(|stem| stem.to_str()) else {
			continue;
		};
		if let Ok(Some(template_dir)) = lookup(digest) {
			live.insert(digest.to_owned(), template_dir.to_string_lossy().into_owned());
		}
	}
	Ok(live)
}

fn validate_digest(digest: &str) -> Result<&str> {
	let valid = digest.len() == 64
		&& digest
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
	if valid {
		Ok(digest)
	} else {
		Err(EngineError::invalid("template digest must be a lowercase SHA-256 hex digest"))
	}
}

fn pointer_path(digest: &str) -> Result<PathBuf> {
	Ok(cas_root()?.join(format!("{}.json", validate_digest(digest)?)))
}

fn regular_files(template_dir: &Path) -> Result<Vec<PathBuf>> {
	let mut out = Vec::new();
	collect_regular_files(template_dir, &mut out)?;
	out.sort_by_key(|path| template_rel_path(template_dir, path).unwrap_or_default());
	Ok(out)
}

fn collect_regular_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
	for entry in fs::read_dir(dir)? {
		let path = entry?.path();
		let meta = fs::symlink_metadata(&path)?;
		if meta.file_type().is_dir() {
			collect_regular_files(&path, out)?;
		} else if meta.file_type().is_file() {
			out.push(path);
		}
	}
	Ok(())
}

fn template_rel_path(root: &Path, path: &Path) -> Result<String> {
	let rel = path
		.strip_prefix(root)
		.map_err(|e| EngineError::engine(e.to_string()))?;
	Ok(rel
		.components()
		.map(|component| component.as_os_str().to_string_lossy())
		.collect::<Vec<_>>()
		.join("/"))
}

#[derive(Clone, Copy)]
enum EntryKind {
	Directory,
	File,
	Symlink,
}

impl EntryKind {
	const fn tag(self) -> u8 {
		match self {
			Self::Directory => 1,
			Self::File => 2,
			Self::Symlink => 3,
		}
	}
}

struct TemplateEntry {
	path: PathBuf,
	rel:  String,
	meta: fs::Metadata,
	kind: EntryKind,
}

fn template_entries(template_dir: &Path) -> Result<Vec<TemplateEntry>> {
	let mut out = Vec::new();
	collect_template_entries(template_dir, template_dir, &mut out)?;
	out.sort_by(|left, right| left.rel.cmp(&right.rel));
	Ok(out)
}

fn collect_template_entries(root: &Path, path: &Path, out: &mut Vec<TemplateEntry>) -> Result<()> {
	let meta = fs::symlink_metadata(path)?;
	let kind = if meta.file_type().is_dir() {
		EntryKind::Directory
	} else if meta.file_type().is_file() {
		EntryKind::File
	} else if meta.file_type().is_symlink() {
		EntryKind::Symlink
	} else {
		return Ok(());
	};
	out.push(TemplateEntry {
		path: path.to_owned(),
		rel: snapshot_rel_path(root, path)?,
		meta,
		kind,
	});
	if matches!(kind, EntryKind::Directory) {
		for entry in fs::read_dir(path)? {
			collect_template_entries(root, &entry?.path(), out)?;
		}
	}
	Ok(())
}

fn hash_field(digest: &mut Sha256, bytes: &[u8]) {
	digest.update((bytes.len() as u64).to_le_bytes());
	digest.update(bytes);
}

fn hash_file_bytes(digest: &mut Sha256, path: &Path) -> Result<()> {
	let mut file = File::open(path)?;
	let mut buf = vec![0_u8; CHUNK_SIZE];
	loop {
		let mut n = 0usize;
		while n < buf.len() {
			let got = file.read(&mut buf[n..])?;
			if got == 0 {
				break;
			}
			n += got;
		}
		if n == 0 {
			break;
		}
		hash_logical_bytes(digest, &buf[..n]);
	}
	digest.update(b"X");
	Ok(())
}

fn hash_logical_bytes(digest: &mut Sha256, bytes: &[u8]) {
	const ZERO_BLOCK: usize = 64 * 1024;
	let mut cursor = 0usize;
	let mut zero_run = 0u64;
	while cursor < bytes.len() {
		let end = bytes.len().min(cursor + ZERO_BLOCK);
		if crate::mesh::bundle::is_zero(&bytes[cursor..end]) {
			zero_run += (end - cursor) as u64;
		} else {
			if zero_run > 0 {
				digest.update(b"Z");
				digest.update(zero_run.to_le_bytes());
				zero_run = 0;
			}
			digest.update(b"D");
			digest.update(((end - cursor) as u64).to_le_bytes());
			digest.update(&bytes[cursor..end]);
		}
		cursor = end;
	}
	if zero_run > 0 {
		digest.update(b"Z");
		digest.update(zero_run.to_le_bytes());
	}
}

fn snapshot_rel_path(root: &Path, path: &Path) -> Result<String> {
	let rel = path
		.strip_prefix(root)
		.map_err(|e| EngineError::engine(e.to_string()))?;
	let mut components = Vec::new();
	for component in rel.components() {
		components.push(
			component
				.as_os_str()
				.to_str()
				.ok_or_else(|| EngineError::invalid("template path is not utf-8"))?,
		);
	}
	Ok(components.join("/"))
}
fn marker_bytes(path: &Path) -> Result<Vec<u8>> {
	let bytes = fs::read(path)?;
	let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
		return Ok(bytes);
	};
	let Value::Object(map) = value else {
		return Ok(bytes);
	};
	let stable: BTreeMap<String, Value> = map
		.into_iter()
		.filter(|(key, _)| key != VOLATILE_MARKER_FIELD)
		.collect();
	Ok(serde_json::to_vec(&stable)?)
}

/// A pullable directory holds the marker plus either a full `rootfs.img` or
/// a live-migration block delta standing in for it.
fn template_present(template_dir: &Path) -> bool {
	template_dir.is_dir()
		&& template_dir.join(MARKER_NAME).is_file()
		&& (template_dir.join("rootfs.img").is_file()
			|| template_dir
				.join(crate::engine::diskdelta::DISK_DELTA_FILE)
				.is_file())
}

fn read_pointer_file(path: &Path) -> Result<CasPointer> {
	let text = fs::read_to_string(path)?;
	Ok(serde_json::from_str(&text)?)
}

fn write_pointer(path: &Path, payload: &CasPointer) -> Result<()> {
	let tmp = path.with_file_name(format!(
		".{}.{}.tmp",
		path
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or("pointer"),
		std::process::id()
	));
	let result = (|| {
		let mut file = File::create(&tmp)?;
		let mut stable = BTreeMap::new();
		stable.insert("created_unix", Value::from(payload.created_unix));
		stable.insert("template_dir", Value::from(payload.template_dir.clone()));
		stable.insert("tpl_name", Value::from(payload.tpl_name.clone()));
		file.write_all(&serde_json::to_vec(&stable)?)?;
		fs::rename(&tmp, path)?;
		Ok(())
	})();
	let _ = fs::remove_file(&tmp);
	result
}

fn unix_now() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		os::unix::fs::{PermissionsExt, symlink},
	};

	use sha2::{Digest, Sha256};

	type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

	use super::*;

	#[test]
	fn digest_ignores_marker_content_digest() -> TestResult {
		let tmp = tempfile::tempdir()?;
		let tpl = tmp.path().join("tpl");
		fs::create_dir(&tpl)?;
		fs::write(tpl.join("rootfs.img"), b"root")?;
		fs::create_dir(tpl.join("nested"))?;
		fs::write(tpl.join("nested/file.txt"), b"data")?;
		fs::write(
			tpl.join(MARKER_NAME),
			br#"{"boot_version":6,"content_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","memory":512}"#,
		)?;

		let digest_a = template_digest(&tpl)?;
		let snapshot_a = snapshot_digest(&tpl)?;
		fs::write(
			tpl.join(MARKER_NAME),
			br#"{"boot_version":6,"content_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","memory":512}"#,
		)?;
		let digest_b = template_digest(&tpl)?;
		let snapshot_b = snapshot_digest(&tpl)?;
		assert_eq!(digest_a, digest_b);
		assert_ne!(snapshot_a, snapshot_b);

		let mut expected = Sha256::new();
		expected.update(b"agent-ready.json\0");
		expected.update(br#"{"boot_version":6,"memory":512}"#);
		expected.update(b"nested/file.txt\0");
		expected.update([1u8]);
		expected.update(4u64.to_le_bytes());
		expected.update(b"data");
		expected.update(b"rootfs.img\0");
		expected.update([1u8]);
		expected.update(4u64.to_le_bytes());
		expected.update(b"root");
		assert_eq!(digest_a, hex::encode(expected.finalize()));

		Ok(())
	}

	/// The digest is content-defined: a materialized run of zero bytes and a
	/// filesystem hole of the same length hash identically.
	#[test]
	fn digest_is_independent_of_physical_hole_layout() -> TestResult {
		use std::io::{Seek, SeekFrom, Write as _};
		let tmp = tempfile::tempdir()?;
		let dense = tmp.path().join("dense");
		let sparse = tmp.path().join("sparse");
		for dir in [&dense, &sparse] {
			fs::create_dir(dir)?;
			fs::write(dir.join(MARKER_NAME), b"{}")?;
		}
		let mut payload = vec![0u8; 8 * 1024 * 1024];
		payload[4 * 1024 * 1024] = 0xaa;
		fs::write(dense.join("memory.1.bin"), &payload)?;
		let mut holey = fs::File::create(sparse.join("memory.1.bin"))?;
		holey.seek(SeekFrom::Start(4 * 1024 * 1024))?;
		holey.write_all(&[0xaa])?;
		holey.set_len(8 * 1024 * 1024)?;
		drop(holey);

		assert_eq!(snapshot_digest(&dense)?, snapshot_digest(&sparse)?);
		assert_eq!(template_digest(&dense)?, template_digest(&sparse)?);
		Ok(())
	}

	#[test]
	fn snapshot_digest_reflects_all_bundle_entry_attributes() -> TestResult {
		let tmp = tempfile::tempdir()?;
		let tpl = tmp.path().join("tpl");
		fs::create_dir(&tpl)?;
		let file = tpl.join("file");
		fs::write(&file, b"same bytes")?;

		let initial = snapshot_digest(&tpl)?;
		fs::set_permissions(&file, fs::Permissions::from_mode(0o755))?;
		assert_ne!(initial, snapshot_digest(&tpl)?);
		fs::set_permissions(&file, fs::Permissions::from_mode(0o644))?;

		fs::create_dir(tpl.join("empty"))?;
		let with_empty_dir = snapshot_digest(&tpl)?;
		assert_ne!(initial, with_empty_dir);
		fs::set_permissions(tpl.join("empty"), fs::Permissions::from_mode(0o700))?;
		assert_ne!(with_empty_dir, snapshot_digest(&tpl)?);
		fs::remove_dir(tpl.join("empty"))?;

		symlink("first-target", tpl.join("link"))?;
		let first_target = snapshot_digest(&tpl)?;
		fs::remove_file(tpl.join("link"))?;
		symlink("second-target", tpl.join("link"))?;
		assert_ne!(first_target, snapshot_digest(&tpl)?);
		fs::remove_file(tpl.join("link"))?;

		let file_tree = tmp.path().join("file-tree");
		let dir_tree = tmp.path().join("dir-tree");
		fs::create_dir(&file_tree)?;
		fs::create_dir(&dir_tree)?;
		fs::write(file_tree.join("entry"), b"")?;
		fs::create_dir(dir_tree.join("entry"))?;
		assert_ne!(snapshot_digest(&file_tree)?, snapshot_digest(&dir_tree)?);

		let left_path = tmp.path().join("left-path");
		let right_path = tmp.path().join("right-path");
		fs::create_dir(&left_path)?;
		fs::create_dir(&right_path)?;
		fs::write(left_path.join("left"), b"same bytes")?;
		fs::write(right_path.join("right"), b"same bytes")?;
		assert_ne!(snapshot_digest(&left_path)?, snapshot_digest(&right_path)?);
		Ok(())
	}

	#[test]
	fn pointer_roundtrip_and_stale_prune() -> TestResult {
		let tmp = tempfile::tempdir()?;
		let home = tmp.path().join("home");
		let _home = crate::home::test_home::set(&home);
		let tpl = tmp.path().join("tpl-name");
		fs::create_dir(&tpl)?;
		fs::write(tpl.join("rootfs.img"), b"root")?;
		fs::write(tpl.join(MARKER_NAME), b"{}")?;

		let digest = index_template(&tpl, None)?;
		assert_eq!(lookup(&digest)?, Some(tpl.clone()));
		let pointer = cas_root()?.join(format!("{digest}.json"));
		let payload: CasPointer = serde_json::from_str(&fs::read_to_string(&pointer)?)?;
		assert_eq!(payload.template_dir, tpl.to_string_lossy().as_ref());
		assert_eq!(payload.tpl_name, "tpl-name");

		fs::remove_file(tpl.join("rootfs.img"))?;
		assert_eq!(lookup(&digest)?, None);
		assert!(!pointer.exists());
		Ok(())
	}

	#[test]
	fn lookup_accepts_delta_checkpoint_without_rootfs() -> TestResult {
		let tmp = tempfile::tempdir()?;
		let home = tmp.path().join("home");
		let _home = crate::home::test_home::set(&home);
		let tpl = tmp.path().join("migrate-delta");
		fs::create_dir(&tpl)?;
		fs::write(tpl.join(MARKER_NAME), b"{}")?;
		fs::write(tpl.join(crate::engine::diskdelta::DISK_DELTA_FILE), b"VMONDSK1")?;

		let digest = index_template(&tpl, None)?;
		assert_eq!(lookup(&digest)?, Some(tpl));
		Ok(())
	}

	#[test]
	fn drop_pointer_ignores_invalid_digest() -> TestResult {
		drop_pointer("not-a-digest")?;
		Ok(())
	}
}
