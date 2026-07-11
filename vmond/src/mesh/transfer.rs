//! Template/CAS transfer and remote-page metadata.
//!
//! Port of `python/vmon/server/templates.py` plus the CAS rules from
//! `python/vmon/cas.py`.  The route layer can serve the archive helpers under
//! `/v1/templates/{digest}` and use `pull_template*` for peer fetches.  Full
//! template pulls verify the content-addressed digest before installing and
//! indexing; metadata pulls install a lazy restore stub with
//! `remote-page.json`.

use std::{
	ffi::OsStr,
	fs,
	io::{Read, Seek, SeekFrom, Write},
	net::ToSocketAddrs,
	path::{Path, PathBuf},
	process::Command,
	time::{SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{EngineError, Result, home::state_dir, image::cas};

const MARKER_NAME: &str = "agent-ready.json";
const ROOTFS_NAME: &str = "rootfs.img";
const GENERATION_NAME: &str = "current-generation";
const PAGE_SIZE: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplateArchive {
	pub path:       PathBuf,
	pub filename:   String,
	pub media_type: &'static str,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemotePageMetadata {
	pub peer_url: String,
	pub digest:   String,
	pub page_url: String,
}

/// Create a gzip tarball containing the whole bootable template directory.
pub fn template_archive(template_dir: &Path, digest: &str) -> Result<TemplateArchive> {
	let archive = temp_path_in(&state_dir(), "vmon-template-", ".tar.gz")?;
	create_tar_gz(template_dir, &archive)?;
	Ok(TemplateArchive {
		path:       archive,
		filename:   format!("{digest}.tar.gz"),
		media_type: "application/gzip",
	})
}

/// Create a metadata-only gzip tarball by excluding `memory.*.bin` pages.  This
/// matches the Python filter used for lazy remote page-in restore.
pub fn template_metadata_archive(template_dir: &Path, digest: &str) -> Result<TemplateArchive> {
	let state = state_dir();
	fs::create_dir_all(&state)?;
	let staging = temp_dir_in(&state, ".template-meta-stage-")?;
	let root_name = file_name(template_dir)?;
	let staged_root = staging.join(root_name);
	copy_tree_filtered(template_dir, &staged_root, &|path| !is_memory_page(path))?;
	let archive = temp_path_in(&state, "vmon-template-meta-", ".tar.gz")?;
	let result = create_tar_gz(&staged_root, &archive).map(|()| TemplateArchive {
		path:       archive,
		filename:   format!("{digest}.metadata.tar.gz"),
		media_type: "application/gzip",
	});
	let _ = fs::remove_dir_all(&staging);
	result
}

/// Fetch a metadata-only archive from a peer and install a lazy remote-page
/// restore stub under `$VMON_HOME/templates/remote-*`.
pub async fn pull_template_metadata(
	client: &Client,
	peer_url: &str,
	digest: &str,
	token: &str,
) -> Result<PathBuf> {
	let archive = pull_archive(
		client,
		&format!("{}/v1/templates/{digest}/metadata", peer_url.trim_end_matches('/')),
		digest,
		token,
		".template-meta-pull-",
	)
	.await?;
	let archive_for_install = archive.clone();
	let peer_url = peer_url.to_owned();
	let digest = digest.to_owned();
	let result = tokio::task::spawn_blocking(move || {
		install_lazy_template_stub(&archive_for_install, &digest, &peer_url)
	})
	.await
	.map_err(|err| EngineError::engine(format!("mesh metadata install task failed: {err}")))?;
	let _ = fs::remove_file(&archive);
	result
}

/// Fetch, verify, install, and CAS-index a full template archive from a peer.
pub async fn pull_template(
	client: &Client,
	peer_url: &str,
	digest: &str,
	token: &str,
) -> Result<PathBuf> {
	let archive = pull_archive(
		client,
		&format!("{}/v1/templates/{digest}", peer_url.trim_end_matches('/')),
		digest,
		token,
		".template-pull-",
	)
	.await?;
	let archive_for_install = archive.clone();
	let digest = digest.to_owned();
	let result =
		tokio::task::spawn_blocking(move || install_pulled_template(&archive_for_install, &digest))
			.await
			.map_err(|err| EngineError::engine(format!("mesh template install task failed: {err}")))?;
	let _ = fs::remove_file(&archive);
	result
}

/// Install a full pulled template archive, verifying that its stable CAS digest
/// equals `digest` before replacing any local target.
pub fn install_pulled_template(archive: &Path, digest: &str) -> Result<PathBuf> {
	validate_digest(digest)?;
	let templates = state_dir().join("templates");
	fs::create_dir_all(&templates)?;
	let extract_root = temp_dir_in(&templates, ".pull-extract-")?;
	let result = (|| {
		extract_tar_gz(archive, &extract_root)?;
		let source = extracted_template_root(&extract_root)?;
		let actual = cas::template_digest(&source)?;
		if actual != digest {
			return Err(EngineError::invalid("pulled template digest mismatch"));
		}
		let target = templates.join(template_name_from_marker(&source, digest));
		if target.exists() {
			if target.is_dir() && cas::template_digest(&target).ok().as_deref() == Some(digest) {
				cas::index_template(&target, Some(digest))?;
				return Ok(target);
			}
			remove_path(&target)?;
		}
		fs::rename(&source, &target)?;
		cas::index_template(&target, Some(digest))?;
		Ok(target)
	})();
	let _ = fs::remove_dir_all(&extract_root);
	result
}

/// Install a metadata-only lazy template stub and write `remote-page.json`.
///
/// The stub records `{peer_url,digest,page_url}` and is intentionally not
/// CAS-indexed; it exists to feed `--remote-page-url` during restore.
pub fn install_lazy_template_stub(archive: &Path, digest: &str, peer_url: &str) -> Result<PathBuf> {
	validate_digest(digest)?;
	let templates = state_dir().join("templates");
	fs::create_dir_all(&templates)?;
	let extract_root = temp_dir_in(&templates, ".pull-meta-")?;
	let result = (|| {
		extract_tar_gz(archive, &extract_root)?;
		let source = extracted_template_root(&extract_root)?;
		if marker_digest(&source).as_deref() != Some(digest) {
			return Err(EngineError::invalid("pulled template metadata digest mismatch"));
		}
		let generation = fs::read_to_string(source.join(GENERATION_NAME))?
			.trim()
			.to_owned();
		if !generation.chars().all(|ch| ch.is_ascii_digit())
			|| !source.join(format!("vmstate.{generation}.bin")).is_file()
		{
			return Err(EngineError::invalid("pulled template metadata is missing snapshot state"));
		}
		if !source.join(ROOTFS_NAME).is_file() {
			return Err(EngineError::invalid("pulled template metadata is missing rootfs.img"));
		}
		let target = templates.join(format!("remote-{}", template_name_from_marker(&source, digest)));
		if target.exists() {
			remove_path(&target)?;
		}
		let metadata = RemotePageMetadata {
			peer_url: peer_url.trim_end_matches('/').to_owned(),
			digest:   digest.to_owned(),
			page_url: remote_page_url(peer_url, digest)?,
		};
		fs::write(
			source.join("remote-page.json"),
			serde_json::to_vec_pretty(&metadata).map_err(EngineError::from)?,
		)?;
		fs::rename(&source, &target)?;
		Ok(target)
	})();
	let _ = fs::remove_dir_all(&extract_root);
	result
}

/// Build the peer memory-page base URL.  For plain HTTP peers, resolve the
/// hostname to an IP address like Python does so the VMM pager receives a URL
/// it can fetch without relying on guest DNS.
pub fn remote_page_url(peer_url: &str, digest: &str) -> Result<String> {
	validate_digest(digest)?;
	let parsed = reqwest::Url::parse(peer_url.trim_end_matches('/'))
		.map_err(|_| EngineError::invalid("invalid peer URL"))?;
	let mut host = parsed
		.host_str()
		.ok_or_else(|| EngineError::invalid("invalid peer URL"))?
		.to_owned();
	let port = parsed.port_or_known_default().unwrap_or(80);
	if parsed.scheme() == "http"
		&& let Ok(mut addrs) = (host.as_str(), port).to_socket_addrs()
		&& let Some(addr) = addrs.next()
	{
		host = addr.ip().to_string();
	}
	let host = if host.contains(':') {
		format!("[{host}]")
	} else {
		host
	};
	let netloc = if parsed.port().is_some() {
		format!("{host}:{port}")
	} else {
		host
	};
	Ok(format!("{}://{netloc}/v1/templates/{digest}/pages", parsed.scheme()))
}

/// Return one 4 KiB memory page from a CAS-resolved template.
pub fn template_memory_page(template_dir: &Path, page: u64) -> Result<Vec<u8>> {
	let generation = fs::read_to_string(template_dir.join(GENERATION_NAME))?
		.trim()
		.to_owned();
	if !generation.chars().all(|ch| ch.is_ascii_digit()) {
		return Err(EngineError::not_found("template has invalid snapshot generation"));
	}
	let memory = template_dir.join(format!("memory.{generation}.bin"));
	let mut file = fs::File::open(memory)?;
	file.seek(SeekFrom::Start(page.saturating_mul(PAGE_SIZE as u64)))?;
	let mut data = vec![0_u8; PAGE_SIZE];
	file.read_exact(&mut data).map_err(|_| {
		EngineError::not_found(format!("template page {page} is outside the memory image"))
	})?;
	Ok(data)
}

/// Resolve and archive a digest for `GET /v1/templates/{digest}` route wiring.
pub fn archive_for_digest(digest: &str) -> Result<TemplateArchive> {
	let template_dir =
		cas::lookup(digest)?.ok_or_else(|| EngineError::not_found("unknown template"))?;
	template_archive(&template_dir, digest)
}

/// Resolve and archive metadata for `GET /v1/templates/{digest}/metadata`.
pub fn metadata_archive_for_digest(digest: &str) -> Result<TemplateArchive> {
	let template_dir =
		cas::lookup(digest)?.ok_or_else(|| EngineError::not_found("unknown template"))?;
	template_metadata_archive(&template_dir, digest)
}

/// Resolve and read one memory page for `GET
/// /v1/templates/{digest}/pages/{page}`.
pub fn page_for_digest(digest: &str, page: u64) -> Result<Vec<u8>> {
	let template_dir =
		cas::lookup(digest)?.ok_or_else(|| EngineError::not_found("unknown template"))?;
	template_memory_page(&template_dir, page)
}

async fn pull_archive(
	client: &Client,
	url: &str,
	digest: &str,
	token: &str,
	prefix: &str,
) -> Result<PathBuf> {
	validate_digest(digest)?;
	let archive_dir = state_dir();
	fs::create_dir_all(&archive_dir)?;
	let archive = temp_path_in(&archive_dir, prefix, ".tar.gz")?;
	let mut response = client
		.get(url)
		.bearer_auth(token)
		.header("X-Vmon-Mesh-Hop", "1")
		.send()
		.await
		.map_err(|err| EngineError::engine(format!("failed to fetch template: {err}")))?;
	if response.status() == StatusCode::NOT_FOUND {
		let _ = fs::remove_file(&archive);
		return Err(EngineError::not_found(format!("unknown template digest {digest}")));
	}
	if let Err(err) = response.error_for_status_ref() {
		let _ = fs::remove_file(&archive);
		return Err(EngineError::engine(format!("failed to fetch template: {err}")));
	}
	let mut file = fs::File::create(&archive)?;
	while let Some(chunk) = response
		.chunk()
		.await
		.map_err(|err| EngineError::engine(format!("failed to stream template: {err}")))?
	{
		if !chunk.is_empty() {
			file.write_all(&chunk)?;
		}
	}
	Ok(archive)
}

fn extracted_template_root(root: &Path) -> Result<PathBuf> {
	if root.join(MARKER_NAME).is_file() {
		return Ok(root.to_owned());
	}
	for entry in fs::read_dir(root)? {
		let path = entry?.path();
		if path.is_dir() && path.join(MARKER_NAME).is_file() {
			return Ok(path);
		}
	}
	Err(EngineError::invalid("template archive did not contain an agent-ready marker"))
}

fn template_name_from_marker(template_dir: &Path, digest: &str) -> String {
	let data = read_marker_json(template_dir).unwrap_or(Value::Null);
	let raw = data
		.get("tpl_name")
		.and_then(Value::as_str)
		.filter(|value| !value.is_empty());
	let name = raw.unwrap_or_else(|| {
		template_dir
			.file_name()
			.and_then(OsStr::to_str)
			.unwrap_or("")
	});
	let safe = Path::new(name)
		.file_name()
		.and_then(OsStr::to_str)
		.unwrap_or("");
	if safe.is_empty() || matches!(safe, "." | "..") {
		format!("tpl-{}", &digest[..12])
	} else {
		safe.to_owned()
	}
}

fn marker_digest(template_dir: &Path) -> Option<String> {
	let data = read_marker_json(template_dir).ok()?;
	let value = data.get("content_digest")?.as_str()?;
	(value.len() == 64
		&& value
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
	.then_some(value.to_owned())
}

fn read_marker_json(template_dir: &Path) -> Result<Value> {
	let raw = fs::read_to_string(template_dir.join(MARKER_NAME))?;
	serde_json::from_str(&raw).map_err(EngineError::from)
}

fn create_tar_gz(template_dir: &Path, archive: &Path) -> Result<()> {
	let parent = template_dir
		.parent()
		.ok_or_else(|| EngineError::invalid("template directory has no parent"))?;
	let name = file_name(template_dir)?;
	run_tar([OsStr::new("-czf"), archive.as_os_str(), OsStr::new("-C"), parent.as_os_str(), name])
}

fn extract_tar_gz(archive: &Path, output_dir: &Path) -> Result<()> {
	fs::create_dir_all(output_dir)?;
	run_tar([OsStr::new("-xzf"), archive.as_os_str(), OsStr::new("-C"), output_dir.as_os_str()])
}

fn run_tar<const N: usize>(args: [&OsStr; N]) -> Result<()> {
	let output = Command::new("tar").args(args).output()?;
	if output.status.success() {
		Ok(())
	} else {
		let stderr = String::from_utf8_lossy(&output.stderr);
		Err(EngineError::engine(format!("tar failed: {stderr}")))
	}
}

fn copy_tree_filtered(src: &Path, dst: &Path, include: &dyn Fn(&Path) -> bool) -> Result<()> {
	if !include(src) {
		return Ok(());
	}
	let meta = fs::symlink_metadata(src)?;
	if meta.is_dir() {
		fs::create_dir_all(dst)?;
		for entry in fs::read_dir(src)? {
			let entry = entry?;
			copy_tree_filtered(&entry.path(), &dst.join(entry.file_name()), include)?;
		}
	} else if meta.file_type().is_symlink() {
		copy_symlink(src, dst)?;
	} else if meta.is_file() {
		if let Some(parent) = dst.parent() {
			fs::create_dir_all(parent)?;
		}
		fs::copy(src, dst)?;
	}
	Ok(())
}

#[cfg(unix)]
fn copy_symlink(src: &Path, dst: &Path) -> Result<()> {
	use std::os::unix::fs as unix_fs;
	let target = fs::read_link(src)?;
	if let Some(parent) = dst.parent() {
		fs::create_dir_all(parent)?;
	}
	unix_fs::symlink(target, dst)?;
	Ok(())
}

#[cfg(not(unix))]
fn copy_symlink(src: &Path, dst: &Path) -> Result<()> {
	let target = fs::read_link(src)?;
	if target.is_file() {
		fs::copy(target, dst)?;
	}
	Ok(())
}

#[allow(
	clippy::case_sensitive_file_extension_comparisons,
	reason = "checkpoint page files are generated with exact lowercase .bin names"
)]
fn is_memory_page(path: &Path) -> bool {
	path
		.file_name()
		.and_then(OsStr::to_str)
		.is_some_and(|name| name.starts_with("memory.") && name.ends_with(".bin"))
}

fn remove_path(path: &Path) -> Result<()> {
	if path.is_dir() && !fs::symlink_metadata(path)?.file_type().is_symlink() {
		fs::remove_dir_all(path)?;
	} else {
		fs::remove_file(path)?;
	}
	Ok(())
}

fn file_name(path: &Path) -> Result<&OsStr> {
	path
		.file_name()
		.ok_or_else(|| EngineError::invalid("path has no file name"))
}

fn validate_digest(digest: &str) -> Result<()> {
	let valid = digest.len() == 64
		&& digest
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
	if valid {
		Ok(())
	} else {
		Err(EngineError::invalid("template digest must be a lowercase SHA-256 hex digest"))
	}
}

fn temp_dir_in(parent: &Path, prefix: &str) -> Result<PathBuf> {
	fs::create_dir_all(parent)?;
	for _ in 0..128 {
		let path = parent.join(format!("{prefix}{}", unique_suffix()));
		match fs::create_dir(&path) {
			Ok(()) => return Ok(path),
			Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {},
			Err(err) => return Err(err.into()),
		}
	}
	Err(EngineError::engine("failed to allocate temporary directory"))
}

fn temp_path_in(parent: &Path, prefix: &str, suffix: &str) -> Result<PathBuf> {
	fs::create_dir_all(parent)?;
	for _ in 0..128 {
		let path = parent.join(format!("{prefix}{}{suffix}", unique_suffix()));
		match fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&path)
		{
			Ok(_) => return Ok(path),
			Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {},
			Err(err) => return Err(err.into()),
		}
	}
	Err(EngineError::engine("failed to allocate temporary archive"))
}

fn unique_suffix() -> String {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_nanos());
	format!("{}-{nanos}", std::process::id())
}

/// Return the JSON marker value used by tests and route wiring to assert lazy
/// metadata installs without reading the file directly.
pub fn remote_page_metadata(template_dir: &Path) -> Result<RemotePageMetadata> {
	let raw = fs::read_to_string(template_dir.join("remote-page.json"))?;
	serde_json::from_str(&raw).map_err(EngineError::from)
}

/// Build the exact metadata JSON payload for a peer/digest pair.
pub fn remote_page_metadata_value(peer_url: &str, digest: &str) -> Result<Value> {
	Ok(json!({
		"peer_url": peer_url.trim_end_matches('/'),
		"digest": digest,
		"page_url": remote_page_url(peer_url, digest)?,
	}))
}
