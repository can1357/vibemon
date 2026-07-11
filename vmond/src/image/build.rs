//! Dockerfile builds via buildah/docker-buildx. Port of python/vmon/build.py.

use std::{
	collections::BTreeSet,
	fs::{self, File},
	io::{Read, Seek, SeekFrom},
	path::{Path, PathBuf},
	process::{Command, Stdio},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{EngineError, Result};

const BUILD_TIMEOUT: Duration = Duration::from_mins(30);
const TAR_BLOCK: usize = 512;

const EXTRA_TOOL_DIRS: &[&str] = &[
	"/opt/homebrew/bin",
	"/usr/local/bin",
	"/opt/homebrew/opt/e2fsprogs/sbin",
	"/usr/local/opt/e2fsprogs/sbin",
	"/opt/homebrew/sbin",
	"/usr/local/sbin",
	"/sbin",
	"/usr/sbin",
];

#[derive(Clone, Debug, Eq, PartialEq)]
enum Backend {
	Buildah(PathBuf),
	Docker(PathBuf),
}

/// Build a Dockerfile into a content-addressed OCI layout and return its skopeo
/// ref.
pub fn build_image(
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	arch: Option<&str>,
) -> Result<String> {
	if tag.trim().is_empty() {
		return Err(EngineError::invalid("image tag must not be empty"));
	}
	let backend = detect_backend()?;
	let builds = builds_dir();
	fs::create_dir_all(&builds)?;
	let tmp = TempDir::new_in(&builds, ".tmp-")?;
	let layout = tmp.path().join("layout");
	match backend {
		Backend::Buildah(binary) => {
			build_with_buildah(&binary, dockerfile, context, tag, &layout, arch)?;
		},
		Backend::Docker(binary) => {
			build_with_docker(&binary, dockerfile, context, tag, &layout, arch)?;
		},
	}
	let digest = digest_layout(&layout)?;
	let final_dir = builds.join(&digest);
	if final_dir.exists() {
		if !final_dir.is_dir() {
			fs::remove_file(&final_dir)?;
			fs::rename(&layout, &final_dir)?;
		}
	} else {
		fs::rename(&layout, &final_dir)?;
	}
	let canonical = final_dir.canonicalize().unwrap_or(final_dir);
	Ok(oci_ref(&canonical, tag))
}

/// Best-effort pruning for old local build layouts after a template consumes
/// one.
pub fn prune_build_layouts(keep_ref: Option<&str>, keep_recent: usize) -> Result<()> {
	let builds = builds_dir();
	let keep = keep_ref
		.and_then(layout_from_ref)
		.and_then(|path| path.canonicalize().ok());
	let mut entries = Vec::new();
	if let Ok(read_dir) = fs::read_dir(&builds) {
		for entry in read_dir {
			let entry = entry?;
			let path = entry.path();
			if path.is_dir()
				&& path
					.file_name()
					.and_then(|name| name.to_str())
					.is_some_and(|name| !name.starts_with('.'))
			{
				let mtime = entry
					.metadata()
					.and_then(|meta| meta.modified())
					.unwrap_or(UNIX_EPOCH);
				entries.push((mtime, path));
			}
		}
	}
	entries.sort_by_key(|entry| std::cmp::Reverse(entry.0));
	let mut protected = BTreeSet::new();
	for (_, path) in entries.iter().take(keep_recent) {
		if let Ok(path) = path.canonicalize() {
			protected.insert(path);
		}
	}
	if let Some(keep) = keep {
		protected.insert(keep);
	}
	for (_, path) in entries {
		let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
		if !protected.contains(&canonical) {
			let _ = fs::remove_dir_all(path);
		}
	}
	Ok(())
}

/// Return the buildah build argv used for Dockerfile builds.
pub fn buildah_build_args(
	buildah: &Path,
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	arch: Option<&str>,
) -> Vec<String> {
	let mut args =
		vec![path_string(buildah), "build".to_owned(), "--format".to_owned(), "oci".to_owned()];
	if let Some(platform) = platform_arg(arch) {
		args.extend(["--platform".to_owned(), platform]);
	}
	args.extend([
		"-f".to_owned(),
		path_string(dockerfile),
		"-t".to_owned(),
		tag.to_owned(),
		path_string(context),
	]);
	args
}

/// Return the buildah push argv used to export an OCI layout.
pub fn buildah_push_args(buildah: &Path, tag: &str, layout: &Path) -> Vec<String> {
	vec![
		path_string(buildah),
		"push".to_owned(),
		tag.to_owned(),
		format!("oci:{}:{tag}", path_string(layout)),
	]
}

/// Return the docker buildx argv used for Dockerfile builds.
pub fn docker_buildx_args(
	docker: &Path,
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	archive: &Path,
	arch: Option<&str>,
) -> Vec<String> {
	let mut args = vec![path_string(docker), "buildx".to_owned(), "build".to_owned()];
	if let Some(platform) = platform_arg(arch) {
		args.extend(["--platform".to_owned(), platform]);
	}
	args.extend([
		"-f".to_owned(),
		path_string(dockerfile),
		"-t".to_owned(),
		tag.to_owned(),
		"-o".to_owned(),
		format!("type=oci,dest={}", path_string(archive)),
		path_string(context),
	]);
	args
}

fn builds_dir() -> PathBuf {
	crate::home::state_dir().join("builds")
}

fn normalize_arch(arch: &str) -> String {
	match arch.trim().to_ascii_lowercase().replace('-', "_").as_str() {
		"x86_64" | "amd64" | "x64" => "amd64".to_owned(),
		"aarch64" | "arm64" => "arm64".to_owned(),
		other => other.to_owned(),
	}
}

fn platform_arg(arch: Option<&str>) -> Option<String> {
	arch
		.map(normalize_arch)
		.filter(|arch| !arch.is_empty())
		.map(|arch| format!("linux/{arch}"))
}

fn unsupported_message() -> String {
	let mut message = "Dockerfile builds require buildah (rootless) or docker buildx with an OCI \
	                   builder; install buildah or Docker with buildx enabled"
		.to_owned();
	if cfg!(target_os = "macos") {
		message
			.push_str("; on macOS builds need a Linux host or a docker CLI with a running builder");
	}
	message
}

fn detect_backend() -> Result<Backend> {
	if let Some(buildah) = find_tool("buildah")
		&& !cfg!(target_os = "macos")
	{
		return Ok(Backend::Buildah(buildah));
	}
	if let Some(docker) = find_tool("docker") {
		return Ok(Backend::Docker(docker));
	}
	Err(EngineError::unsupported(unsupported_message()))
}

fn build_with_buildah(
	buildah: &Path,
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	layout: &Path,
	arch: Option<&str>,
) -> Result<()> {
	run_inherited(
		&buildah_build_args(buildah, dockerfile, context, tag, arch),
		BUILD_TIMEOUT,
		"Dockerfile build",
	)?;
	run_inherited(&buildah_push_args(buildah, tag, layout), BUILD_TIMEOUT, "Dockerfile build")?;
	tag_oci_layout(layout, tag)
}

fn build_with_docker(
	docker: &Path,
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	layout: &Path,
	arch: Option<&str>,
) -> Result<()> {
	let archive = layout.with_extension("tar");
	run_inherited(
		&docker_buildx_args(docker, dockerfile, context, tag, &archive, arch),
		BUILD_TIMEOUT,
		"Dockerfile build",
	)?;
	fs::create_dir_all(layout)?;
	extract_tar(&archive, layout)?;
	tag_oci_layout(layout, tag)
}

fn run_inherited(cmd: &[String], timeout: Duration, label: &str) -> Result<()> {
	let Some((program, args)) = cmd.split_first() else {
		return Err(EngineError::engine("empty command"));
	};
	let mut child = Command::new(program)
		.args(args)
		.stdin(Stdio::null())
		.stdout(Stdio::inherit())
		.stderr(Stdio::inherit())
		.spawn()
		.map_err(|e| EngineError::unsupported(format!("{}: {e}", unsupported_message())))?;
	let start = Instant::now();
	loop {
		if let Some(status) = child.try_wait()? {
			if status.success() {
				return Ok(());
			}
			let hint = if cfg!(target_os = "macos") {
				"; Dockerfile builds need a Linux host or a docker CLI with a running builder"
			} else {
				""
			};
			return Err(EngineError::engine(format!(
				"{program} {label} failed with exit code {}{hint}",
				status
					.code()
					.map_or_else(|| "signal".to_owned(), |code| code.to_string())
			)));
		}
		if start.elapsed() >= timeout {
			let _ = child.kill();
			let _ = child.wait();
			return Err(EngineError::engine(format!("{program} {label} timed out")));
		}
		std::thread::sleep(Duration::from_millis(100));
	}
}

fn tag_oci_layout(layout: &Path, tag: &str) -> Result<()> {
	let index_path = layout.join("index.json");
	let mut data: Value = serde_json::from_str(&fs::read_to_string(&index_path)?)?;
	let Some(manifests) = data.get_mut("manifests").and_then(Value::as_array_mut) else {
		return Ok(());
	};
	let Some(manifest) = manifests.first_mut().and_then(Value::as_object_mut) else {
		return Ok(());
	};
	let annotations = manifest
		.entry("annotations")
		.or_insert_with(|| Value::Object(serde_json::Map::new()));
	if !annotations.is_object() {
		*annotations = Value::Object(serde_json::Map::new());
	}
	let Some(annotations) = annotations.as_object_mut() else {
		return Ok(());
	};
	annotations
		.insert("org.opencontainers.image.ref.name".to_owned(), Value::String(tag.to_owned()));
	annotations.insert("io.containerd.image.name".to_owned(), Value::String(tag.to_owned()));
	fs::write(index_path, format!("{}\n", serde_json::to_string(&data)?))?;
	Ok(())
}

fn digest_layout(layout: &Path) -> Result<String> {
	let mut hasher = Sha256::new();
	digest_walk(layout, layout, &mut hasher)?;
	Ok(hex::encode(hasher.finalize()))
}

fn digest_walk(root: &Path, dir: &Path, hasher: &mut Sha256) -> Result<()> {
	let mut dirs = Vec::new();
	let mut files = Vec::new();
	for entry in fs::read_dir(dir)? {
		let entry = entry?;
		let path = entry.path();
		let meta = fs::symlink_metadata(&path)?;
		if meta.file_type().is_dir() {
			dirs.push(path);
		} else if meta.file_type().is_file() {
			files.push(path);
		}
	}
	dirs.sort_by_key(|path| rel_path(root, path).unwrap_or_default());
	files.sort_by_key(|path| rel_path(root, path).unwrap_or_default());
	for dir_path in &dirs {
		let rel = rel_path(root, dir_path)?;
		hasher.update(format!("dir\0{rel}\0").as_bytes());
	}
	for file_path in &files {
		let rel = rel_path(root, file_path)?;
		hasher.update(format!("file\0{rel}\0").as_bytes());
		let mut file = File::open(file_path)?;
		let mut buf = vec![0_u8; 1024 * 1024];
		loop {
			let n = file.read(&mut buf)?;
			if n == 0 {
				break;
			}
			hasher.update(&buf[..n]);
		}
		hasher.update(b"\0");
	}
	for dir_path in dirs {
		digest_walk(root, &dir_path, hasher)?;
	}
	Ok(())
}

fn layout_from_ref(reference: &str) -> Option<PathBuf> {
	let body = reference.strip_prefix("oci:")?;
	let builds = builds_dir().canonicalize().ok()?;
	let prefix = format!("{}{}", builds.to_string_lossy(), std::path::MAIN_SEPARATOR);
	let rest = body.strip_prefix(&prefix)?;
	let digest = rest.split_once(':').map_or(rest, |(digest, _)| digest);
	if digest.is_empty()
		|| !digest
			.bytes()
			.all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
	{
		return None;
	}
	Some(builds.join(digest))
}

fn oci_ref(layout: &Path, tag: &str) -> String {
	format!("oci:{}:{tag}", path_string(layout))
}

fn extract_tar(archive: &Path, dest: &Path) -> Result<()> {
	let mut file = File::open(archive)?;
	loop {
		let mut header = [0_u8; TAR_BLOCK];
		let n = file.read(&mut header)?;
		if n == 0 {
			break;
		}
		if n != TAR_BLOCK {
			return Err(EngineError::engine("truncated OCI tar header"));
		}
		if header.iter().all(|byte| *byte == 0) {
			break;
		}
		let name = tar_name(&header)?;
		let size = tar_size(&header)?;
		let kind = header[156];
		let out = safe_join(dest, &name)?;
		match kind {
			b'0' | 0 => {
				if let Some(parent) = out.parent() {
					fs::create_dir_all(parent)?;
				}
				let mut out_file = File::create(out)?;
				std::io::copy(&mut file.by_ref().take(size), &mut out_file)?;
			},
			b'5' => {
				fs::create_dir_all(out)?;
				file.seek(SeekFrom::Current(
					i64::try_from(size).map_err(|e| EngineError::engine(e.to_string()))?,
				))?;
			},
			_ => {
				file.seek(SeekFrom::Current(
					i64::try_from(size).map_err(|e| EngineError::engine(e.to_string()))?,
				))?;
			},
		}
		let padding = (TAR_BLOCK as u64 - (size % TAR_BLOCK as u64)) % TAR_BLOCK as u64;
		if padding != 0 {
			file.seek(SeekFrom::Current(
				i64::try_from(padding).map_err(|e| EngineError::engine(e.to_string()))?,
			))?;
		}
	}
	Ok(())
}

fn tar_name(header: &[u8; TAR_BLOCK]) -> Result<String> {
	let name = nul_string(&header[0..100]);
	let prefix = nul_string(&header[345..500]);
	let full = if prefix.is_empty() {
		name
	} else {
		format!("{prefix}/{name}")
	};
	if full.is_empty() {
		Err(EngineError::engine("empty path in OCI tar"))
	} else {
		Ok(full)
	}
}

fn tar_size(header: &[u8; TAR_BLOCK]) -> Result<u64> {
	let text = nul_string(&header[124..136]);
	u64::from_str_radix(text.trim(), 8).map_err(|e| EngineError::engine(e.to_string()))
}

fn nul_string(bytes: &[u8]) -> String {
	let end = bytes
		.iter()
		.position(|byte| *byte == 0)
		.unwrap_or(bytes.len());
	String::from_utf8_lossy(&bytes[..end]).trim().to_owned()
}

fn safe_join(root: &Path, name: &str) -> Result<PathBuf> {
	let path = Path::new(name);
	if path.is_absolute()
		|| path
			.components()
			.any(|component| matches!(component, std::path::Component::ParentDir))
	{
		return Err(EngineError::engine(format!("unsafe path in OCI tar: {name}")));
	}
	Ok(root.join(path))
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

fn find_tool(name: &str) -> Option<PathBuf> {
	std::env::var_os("PATH")
		.into_iter()
		.flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
		.chain(EXTRA_TOOL_DIRS.iter().map(PathBuf::from))
		.map(|dir| dir.join(name))
		.find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
	if !path.is_file() {
		return false;
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		path
			.metadata()
			.is_ok_and(|meta| meta.permissions().mode() & 0o111 != 0)
	}
	#[cfg(not(unix))]
	{
		true
	}
}

fn path_string(path: &Path) -> String {
	path.to_string_lossy().into_owned()
}

struct TempDir {
	path: PathBuf,
}

impl TempDir {
	fn new_in(parent: &Path, prefix: &str) -> Result<Self> {
		fs::create_dir_all(parent)?;
		for attempt in 0..100_u32 {
			let path =
				parent.join(format!("{prefix}{}-{attempt}-{}", std::process::id(), time_nanos()));
			match fs::create_dir(&path) {
				Ok(()) => return Ok(Self { path }),
				Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {},
				Err(e) => return Err(e.into()),
			}
		}
		Err(EngineError::engine("failed to create temporary build directory"))
	}

	fn path(&self) -> &Path {
		&self.path
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.path);
	}
}

fn time_nanos() -> u128 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_nanos())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn buildah_argv_matches_python() {
		assert_eq!(
			buildah_build_args(
				Path::new("/bin/buildah"),
				Path::new("Dockerfile"),
				Path::new("."),
				"vmon-build:latest",
				Some("aarch64"),
			),
			vec![
				"/bin/buildah",
				"build",
				"--format",
				"oci",
				"--platform",
				"linux/arm64",
				"-f",
				"Dockerfile",
				"-t",
				"vmon-build:latest",
				".",
			]
		);
		assert_eq!(
			buildah_push_args(Path::new("/bin/buildah"), "tag", Path::new("/tmp/layout")),
			vec!["/bin/buildah", "push", "tag", "oci:/tmp/layout:tag"]
		);
	}

	#[test]
	fn docker_argv_matches_python() {
		assert_eq!(
			docker_buildx_args(
				Path::new("docker"),
				Path::new("Containerfile"),
				Path::new("ctx"),
				"tag",
				Path::new("layout.tar"),
				Some("x86_64"),
			),
			vec![
				"docker",
				"buildx",
				"build",
				"--platform",
				"linux/amd64",
				"-f",
				"Containerfile",
				"-t",
				"tag",
				"-o",
				"type=oci,dest=layout.tar",
				"ctx",
			]
		);
	}
}
