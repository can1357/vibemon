//! Dockerfile builds through a configured, rootless `BuildKit` worker.

use std::{
	collections::BTreeSet,
	fs::{self, File},
	io::{Read, Seek, SeekFrom},
	path::{Path, PathBuf},
	process::{Command, Stdio},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{EngineError, Result};

const BUILD_TIMEOUT: Duration = Duration::from_mins(30);
const TAR_BLOCK: usize = 512;
const MAX_CONTEXT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_OUTPUT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const BUILDKIT_ADDR_ENV: &str = "VMON_BUILDKIT_ADDR";
const BUILDKIT_BIN_ENV: &str = "VMON_BUILDKIT_BIN";

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

/// Build a Dockerfile into a content-addressed OCI layout.
///
/// The configured worker must be a disposable, rootless `BuildKit` daemon; the
/// serving host never executes Dockerfile instructions itself.
pub fn build_image(
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	arch: Option<&str>,
) -> Result<String> {
	build_image_in(&builds_dir(), dockerfile, context, tag, arch)
}

/// Build a Dockerfile into an explicitly owned content-addressed OCI directory.
pub(crate) fn build_image_in(
	builds: &Path,
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	arch: Option<&str>,
) -> Result<String> {
	let worker = BuildkitWorker::from_environment()?;
	build_image_with_worker(builds, dockerfile, context, tag, arch, &worker)
}

fn build_image_with_worker(
	builds: &Path,
	dockerfile: &Path,
	context: &Path,
	tag: &str,
	arch: Option<&str>,
	worker: &BuildkitWorker,
) -> Result<String> {
	if tag.trim().is_empty() {
		return Err(EngineError::invalid("image tag must not be empty"));
	}
	let context = validate_context(context, dockerfile, MAX_CONTEXT_BYTES)?;
	let dockerfile = dockerfile
		.canonicalize()
		.map_err(|e| EngineError::invalid(format!("invalid Dockerfile: {e}")))?;
	let filename = dockerfile
		.strip_prefix(&context)
		.map_err(|_| EngineError::invalid("Dockerfile must be inside the build context"))?;

	fs::create_dir_all(builds)?;
	let tmp = TempDir::new_in(builds, ".tmp-")?;
	let layout = tmp.path().join("layout");
	let archive = tmp.path().join("output.oci.tar");
	worker.build(&context, filename, &archive, arch)?;
	if fs::metadata(&archive)
		.map_err(|e| EngineError::engine(format!("BuildKit did not produce OCI output: {e}")))?
		.len()
		> MAX_OUTPUT_BYTES
	{
		return Err(EngineError::engine("BuildKit OCI output exceeds size limit"));
	}
	fs::create_dir(&layout)?;
	extract_tar(&archive, &layout, MAX_OUTPUT_BYTES)?;
	validate_oci_layout(&layout)?;
	tag_oci_layout(&layout, tag)?;

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

/// Return the `BuildKit` worker argv for a Dockerfile build. `buildctl` talks
/// to a separately managed rootless worker; it does not evaluate the
/// Dockerfile.
pub fn buildkit_build_args(
	buildctl: &Path,
	address: &str,
	context: &Path,
	dockerfile_name: &Path,
	archive: &Path,
	arch: Option<&str>,
) -> Vec<String> {
	let mut args = vec![
		path_string(buildctl),
		"--addr".to_owned(),
		address.to_owned(),
		"build".to_owned(),
		"--frontend".to_owned(),
		"dockerfile.v0".to_owned(),
		"--local".to_owned(),
		format!("context={}", path_string(context)),
		"--local".to_owned(),
		format!("dockerfile={}", path_string(context)),
		"--opt".to_owned(),
		format!("filename={}", path_string(dockerfile_name)),
	];
	if let Some(platform) = platform_arg(arch) {
		args.extend(["--opt".to_owned(), format!("platform={platform}")]);
	}
	args.extend(["--output".to_owned(), format!("type=oci,dest={}", path_string(archive))]);
	args
}

struct BuildkitWorker {
	binary:  PathBuf,
	address: String,
}

impl BuildkitWorker {
	fn from_environment() -> Result<Self> {
		let address = std::env::var(BUILDKIT_ADDR_ENV).map_err(|_| {
			EngineError::unsupported(format!(
				"Dockerfile builds require a disposable rootless BuildKit worker; set \
				 {BUILDKIT_ADDR_ENV}"
			))
		})?;
		if address.trim().is_empty() {
			return Err(EngineError::invalid(format!("{BUILDKIT_ADDR_ENV} must not be empty")));
		}
		let binary = std::env::var_os(BUILDKIT_BIN_ENV)
			.map(PathBuf::from)
			.or_else(|| find_tool("buildctl"))
			.ok_or_else(|| {
				EngineError::unsupported(format!(
					"Dockerfile builds require buildctl; install it or set {BUILDKIT_BIN_ENV}"
				))
			})?;
		if !is_executable(&binary) {
			return Err(EngineError::invalid(format!(
				"{BUILDKIT_BIN_ENV} must name an executable buildctl binary"
			)));
		}
		Ok(Self { binary, address })
	}

	fn build(
		&self,
		context: &Path,
		dockerfile_name: &Path,
		archive: &Path,
		arch: Option<&str>,
	) -> Result<()> {
		let home = archive
			.parent()
			.ok_or_else(|| EngineError::invalid("BuildKit output must have a parent directory"))?;
		let args =
			buildkit_build_args(&self.binary, &self.address, context, dockerfile_name, archive, arch);
		run_worker(&args, BUILD_TIMEOUT, home)
	}
}

fn validate_context(context: &Path, dockerfile: &Path, limit: u64) -> Result<PathBuf> {
	let context = context
		.canonicalize()
		.map_err(|e| EngineError::invalid(format!("invalid build context: {e}")))?;
	if !context.is_dir() {
		return Err(EngineError::invalid("build context must be a directory"));
	}
	let dockerfile_meta = fs::symlink_metadata(dockerfile)
		.map_err(|e| EngineError::invalid(format!("invalid Dockerfile: {e}")))?;
	if !dockerfile_meta.file_type().is_file() || dockerfile_meta.file_type().is_symlink() {
		return Err(EngineError::invalid("Dockerfile must be a regular file"));
	}
	let dockerfile = dockerfile
		.canonicalize()
		.map_err(|e| EngineError::invalid(format!("invalid Dockerfile: {e}")))?;
	if !dockerfile.starts_with(&context) {
		return Err(EngineError::invalid("Dockerfile must be inside the build context"));
	}
	let mut total = 0;
	validate_context_tree(&context, &mut total, limit)?;
	Ok(context)
}

fn validate_context_tree(path: &Path, total: &mut u64, limit: u64) -> Result<()> {
	for entry in fs::read_dir(path)? {
		let entry = entry?;
		let child = entry.path();
		let meta = fs::symlink_metadata(&child)?;
		if meta.file_type().is_symlink() {
			return Err(EngineError::invalid(format!(
				"build context must not contain symlinks: {}",
				child.display()
			)));
		}
		if meta.is_dir() {
			validate_context_tree(&child, total, limit)?;
		} else if meta.is_file() {
			*total = total
				.checked_add(meta.len())
				.ok_or_else(|| EngineError::invalid("build context exceeds size limit"))?;
			if *total > limit {
				return Err(EngineError::invalid("build context exceeds size limit"));
			}
		} else {
			return Err(EngineError::invalid(format!(
				"build context contains unsupported entry: {}",
				child.display()
			)));
		}
	}
	Ok(())
}

fn run_worker(cmd: &[String], timeout: Duration, home: &Path) -> Result<()> {
	let Some((program, args)) = cmd.split_first() else {
		return Err(EngineError::engine("empty BuildKit worker command"));
	};
	let mut child = Command::new(program)
		.args(args)
		.env_clear()
		.env("HOME", home)
		.env("PATH", "/usr/bin:/bin")
		.env("BUILDKIT_PROGRESS", "plain")
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.spawn()
		.map_err(|e| EngineError::unsupported(format!("failed to start BuildKit worker: {e}")))?;
	let start = SystemTime::now();
	loop {
		if let Some(status) = child.try_wait()? {
			if status.success() {
				return Ok(());
			}
			return Err(EngineError::engine(format!(
				"BuildKit worker failed with exit code {}",
				status
					.code()
					.map_or_else(|| "signal".to_owned(), |code| code.to_string())
			)));
		}
		if start.elapsed().unwrap_or_default() >= timeout {
			let _ = child.kill();
			let _ = child.wait();
			return Err(EngineError::engine("BuildKit worker timed out"));
		}
		std::thread::sleep(Duration::from_millis(100));
	}
}

fn validate_oci_layout(layout: &Path) -> Result<()> {
	let layout_marker: Value =
		serde_json::from_str(&fs::read_to_string(layout.join("oci-layout"))?)?;
	if layout_marker
		.get("imageLayoutVersion")
		.and_then(Value::as_str)
		.is_none()
	{
		return Err(EngineError::engine("BuildKit output is not an OCI image layout"));
	}
	let index: Value = serde_json::from_str(&fs::read_to_string(layout.join("index.json"))?)?;
	if index
		.get("manifests")
		.and_then(Value::as_array)
		.is_none_or(Vec::is_empty)
	{
		return Err(EngineError::engine("BuildKit OCI output has no manifests"));
	}
	Ok(())
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

fn oci_ref(layout: &Path, tag: &str) -> String {
	format!("oci:{}:{tag}", path_string(layout))
}

fn extract_tar(archive: &Path, dest: &Path, max_bytes: u64) -> Result<()> {
	let mut file = File::open(archive)?;
	let mut total_bytes = 0_u64;
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
				total_bytes = total_bytes
					.checked_add(size)
					.ok_or_else(|| EngineError::engine("OCI tar size exceeds limit"))?;
				if total_bytes > max_bytes {
					return Err(EngineError::engine("OCI tar size exceeds limit"));
				}
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

fn create_private_dir(path: &Path) -> std::io::Result<()> {
	#[cfg(unix)]
	{
		use std::os::unix::fs::DirBuilderExt;

		fs::DirBuilder::new().mode(0o700).create(path)
	}
	#[cfg(not(unix))]
	{
		fs::create_dir(path)
	}
}

impl TempDir {
	fn new_in(parent: &Path, prefix: &str) -> Result<Self> {
		fs::create_dir_all(parent)?;
		for attempt in 0..100_u32 {
			let path =
				parent.join(format!("{prefix}{}-{attempt}-{}", std::process::id(), time_nanos()));
			match create_private_dir(&path) {
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
	use std::io::Write;

	use super::*;

	fn create_mock_worker(temp_dir: &Path) -> PathBuf {
		let script_path = temp_dir.join("mock_buildctl.sh");
		let content = r#"#!/bin/sh
dest=""
for arg in "$@"; do
  case "$arg" in
    *dest=*)
      dest=$(echo "$arg" | sed 's/.*dest=//')
      ;;
  esac
done

if [ -z "$dest" ]; then
  exit 1
fi

for arg in "$@"; do
  case "$arg" in
    *fail*)
      exit 1
      ;;
  esac
done

mkdir -p "$(dirname "$dest")/.."
env > "$(dirname "$dest")/../test.env"

tmpdir="/tmp/vmon-mock-tar-$$"
mkdir -p "$tmpdir"
echo '{"imageLayoutVersion": "1.0.0"}' > "$tmpdir/oci-layout"
echo '{"imageLayoutVersion": "1.0.0", "manifests": [{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "size": 0}]}' > "$tmpdir/index.json"

tar -cf "$dest" -C "$tmpdir" oci-layout index.json
rm -rf "$tmpdir"
"#;
		fs::write(&script_path, content).unwrap();
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			let mut perms = fs::metadata(&script_path).unwrap().permissions();
			perms.set_mode(0o755);
			fs::set_permissions(&script_path, perms).unwrap();
		}
		script_path
	}

	#[test]
	fn test_buildkit_args() {
		let args = buildkit_build_args(
			Path::new("/bin/buildctl"),
			"unix:///run/buildkit/buildkitd.sock",
			Path::new("/tmp/ctx"),
			Path::new("Dockerfile"),
			Path::new("/tmp/out.tar"),
			Some("aarch64"),
		);
		assert_eq!(args, vec![
			"/bin/buildctl",
			"--addr",
			"unix:///run/buildkit/buildkitd.sock",
			"build",
			"--frontend",
			"dockerfile.v0",
			"--local",
			"context=/tmp/ctx",
			"--local",
			"dockerfile=/tmp/ctx",
			"--opt",
			"filename=Dockerfile",
			"--opt",
			"platform=linux/arm64",
			"--output",
			"type=oci,dest=/tmp/out.tar"
		]);
	}

	#[test]
	fn test_env_isolation_and_success() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-build-").unwrap();
		let mock_bin = create_mock_worker(temp.path());

		let worker = BuildkitWorker { binary: mock_bin, address: "unix:///mock/address".to_owned() };

		let builds_root = temp.path().join("builds");
		let context_dir = temp.path().join("ctx");
		fs::create_dir(&context_dir).unwrap();
		let dockerfile = context_dir.join("Dockerfile");
		fs::write(&dockerfile, "FROM scratch").unwrap();

		let result_ref = build_image_with_worker(
			&builds_root,
			&dockerfile,
			&context_dir,
			"test-tag:latest",
			None,
			&worker,
		)
		.unwrap();

		assert!(result_ref.starts_with("oci:"));
		let body = result_ref.strip_prefix("oci:").unwrap();
		let prefix = format!(
			"{}{}",
			builds_root.canonicalize().unwrap().to_string_lossy(),
			std::path::MAIN_SEPARATOR
		);
		let rest = body.strip_prefix(&prefix).unwrap();
		let (digest, tag) = rest.split_once(':').unwrap();
		assert_eq!(tag, "test-tag:latest");
		let layout = builds_root.join(digest);
		let index: Value =
			serde_json::from_str(&fs::read_to_string(layout.join("index.json")).unwrap()).unwrap();
		assert_eq!(
			index["manifests"][0]["annotations"]["org.opencontainers.image.ref.name"],
			"test-tag:latest"
		);
		assert!(
			fs::read_dir(&builds_root).unwrap().all(|entry| !entry
				.unwrap()
				.file_name()
				.to_string_lossy()
				.starts_with(".tmp-")),
			"successful handoff must not retain staging directories"
		);

		// Read the environment dump created by the isolated worker
		let env_dump_path = builds_root.join("test.env");
		assert!(env_dump_path.is_file(), "Env dump was not written");
		let env_content = fs::read_to_string(env_dump_path).unwrap();

		// Assert precise environment isolation
		let lines: Vec<&str> = env_content.lines().collect();
		assert!(lines.contains(&"BUILDKIT_PROGRESS=plain"));
		let home = lines
			.iter()
			.find_map(|line| line.strip_prefix("HOME="))
			.expect("isolated HOME");
		assert!(Path::new(home).starts_with(&builds_root));
		assert!(!Path::new(home).exists(), "temporary HOME must be removed");
		assert!(lines.contains(&"PATH=/usr/bin:/bin"));
		// Assert that ambient cargo/runner environment did not leak
		assert!(!lines.iter().any(|l| l.starts_with("CARGO")));
	}

	#[test]
	fn test_failed_worker_cleanup() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-build-").unwrap();
		let mock_bin = create_mock_worker(temp.path());

		let worker = BuildkitWorker { binary: mock_bin, address: "unix:///mock/address".to_owned() };

		let builds_root = temp.path().join("builds");
		let context_dir = temp.path().join("ctx");
		fs::create_dir(&context_dir).unwrap();
		let dockerfile = context_dir.join("Dockerfile-fail");
		fs::write(&dockerfile, "FROM scratch").unwrap();

		let result = build_image_with_worker(
			&builds_root,
			&dockerfile,
			&context_dir,
			"test-tag:latest",
			None,
			&worker,
		);

		assert!(result.is_err());

		// Verify deterministic cleanup on failure (no tmp or partial layouts left)
		if builds_root.exists() {
			let entries: Vec<_> = fs::read_dir(&builds_root)
				.unwrap()
				.map(|e| e.unwrap().path())
				.collect();
			assert!(entries.is_empty(), "Residual files found: {entries:?}");
		}
	}

	#[test]
	fn test_output_bound_rejection() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-tar-").unwrap();
		let tar_path = temp.path().join("evil.tar");
		let dest_dir = temp.path().join("extracted");
		fs::create_dir(&dest_dir).unwrap();

		let mut header = [0_u8; 512];
		header[0..4].copy_from_slice(b"file");
		header[124..135].copy_from_slice(b"00000000200");
		header[156] = b'0';

		let mut tar_file = File::create(&tar_path).unwrap();
		tar_file.write_all(&header).unwrap();
		tar_file.write_all(&[0; 128]).unwrap();
		tar_file.write_all(&[0; 512]).unwrap();
		drop(tar_file);

		let res = extract_tar(&tar_path, &dest_dir, 100);
		assert!(res.is_err(), "should reject archive exceeding output bounds");
	}

	#[test]
	fn test_validate_context_success() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-ctx-").unwrap();
		let dockerfile = temp.path().join("Dockerfile");
		fs::write(&dockerfile, "FROM scratch").unwrap();
		let validated = validate_context(temp.path(), &dockerfile, 1024).unwrap();
		assert_eq!(validated.canonicalize().unwrap(), temp.path().canonicalize().unwrap());
	}

	#[test]
	fn test_validate_context_blocks_symlink() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-ctx-").unwrap();
		let dockerfile = temp.path().join("Dockerfile");
		fs::write(&dockerfile, "FROM scratch").unwrap();

		#[cfg(unix)]
		{
			let target = temp.path().join("evil");
			let symlink = temp.path().join("sym");
			fs::write(&target, "secret").unwrap();
			std::os::unix::fs::symlink(&target, &symlink).unwrap();
			let res = validate_context(temp.path(), &dockerfile, 1024);
			assert!(res.is_err(), "should reject context containing symlinks");
		}
	}

	#[test]
	fn test_validate_context_size_limit() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-ctx-").unwrap();
		let dockerfile = temp.path().join("Dockerfile");
		fs::write(&dockerfile, vec![0; 2000]).unwrap();
		let res = validate_context(temp.path(), &dockerfile, 1024);
		assert!(res.is_err(), "should reject context exceeding limit");
	}

	#[test]
	fn test_validate_context_outside_dockerfile() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-ctx-").unwrap();
		let ext_temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-ext-").unwrap();
		let dockerfile = ext_temp.path().join("Dockerfile");
		fs::write(&dockerfile, "FROM scratch").unwrap();
		let res = validate_context(temp.path(), &dockerfile, 1024);
		assert!(res.is_err(), "should reject Dockerfile outside build context");
	}

	#[test]
	fn test_validate_context_non_regular_dockerfile() {
		let temp = TempDir::new_in(&std::env::temp_dir(), "vmon-test-ctx-").unwrap();
		let dockerfile = temp.path().join("Dockerfile");
		fs::create_dir(&dockerfile).unwrap();
		let res = validate_context(temp.path(), &dockerfile, 1024);
		assert!(res.is_err(), "should reject non-regular Dockerfile");
	}
}
