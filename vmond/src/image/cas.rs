//! Template CAS pointers and digests. Port of python/vmon/cas.py.

use std::{
	collections::BTreeMap,
	fs::{self, File},
	io::{Read, Write},
	path::{Path, PathBuf},
	time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{EngineError, Result};

const CHUNK_SIZE: usize = 1024 * 1024;
const MARKER_NAME: &str = "agent-ready.json";
const VOLATILE_MARKER_FIELD: &str = "content_digest";

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

/// Return a stable SHA-256 digest for a template's bootable content.
pub fn template_digest(template_dir: &Path) -> Result<String> {
	let mut digest = Sha256::new();
	for path in regular_files(template_dir)? {
		let rel = rel_path(template_dir, &path)?;
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
		loop {
			let n = file.read(&mut buf)?;
			if n == 0 {
				break;
			}
			digest.update(&buf[..n]);
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
	match fs::remove_file(pointer) {
		Ok(()) => Ok(()),
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
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
	out.sort_by_key(|path| rel_path(template_dir, path).unwrap_or_default());
	Ok(out)
}

fn collect_regular_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
	for entry in fs::read_dir(dir)? {
		let entry = entry?;
		let path = entry.path();
		let meta = fs::symlink_metadata(&path)?;
		if meta.file_type().is_dir() {
			collect_regular_files(&path, out)?;
		} else if meta.file_type().is_file() {
			out.push(path);
		}
	}
	Ok(())
}

fn rel_path(root: &Path, path: &Path) -> Result<String> {
	let rel = path
		.strip_prefix(root)
		.map_err(|e| EngineError::engine(e.to_string()))?;
	Ok(rel
		.components()
		.map(|component| component.as_os_str().to_string_lossy())
		.collect::<Vec<_>>()
		.join("/"))
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
	use std::fs;

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
		fs::write(
			tpl.join(MARKER_NAME),
			br#"{"boot_version":6,"content_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","memory":512}"#,
		)?;
		let digest_b = template_digest(&tpl)?;
		assert_eq!(digest_a, digest_b);

		let mut expected = Sha256::new();
		expected.update(b"agent-ready.json\0");
		expected.update(br#"{"boot_version":6,"memory":512}"#);
		expected.update(b"nested/file.txt\0data");
		expected.update(b"rootfs.img\0root");
		assert_eq!(digest_a, hex::encode(expected.finalize()));
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
