//! Deterministic resolution of typed function image specifications.

use std::{
	collections::{BTreeMap, BTreeSet},
	fs::{self, OpenOptions},
	io::Write,
	os::unix::{
		fs::{OpenOptionsExt, PermissionsExt},
		io::AsRawFd,
	},
	path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use vmon_proto::v1 as pb;

use super::artifact::ArtifactStore;
use crate::{
	EngineError, Result,
	home::Home,
	image::{self, ResolvedOciImage},
};

const PYTHON_REPOSITORY: &str = "docker.io/library/python";
const MAX_CONTEXT_BYTES: usize = 512 * 1024 * 1024;
const TAR_BLOCK: usize = 512;

/// Image fields ready to copy into an engine `SandboxCreate` request.
#[derive(Clone, Debug, PartialEq)]
pub struct RealizedImage {
	pub image:             Option<String>,
	pub template:          Option<String>,
	pub dockerfile:        Option<String>,
	pub context:           Option<String>,
	pub environment:       BTreeMap<String, String>,
	pub resolved_spec:     pb::ImageSpec,
	pub provenance_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceResolution {
	launch:   Launch,
	digest:   String,
	platform: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Launch {
	Image(String),
	Template(String),
}

trait Backend {
	fn resolve_oci(&self, reference: &str, arch: &str) -> Result<ResolvedOciImage>;
	fn build(
		&self,
		dockerfile: &Path,
		context: &Path,
		tag: &str,
		arch: &str,
	) -> Result<ResolvedOciImage>;
	fn template(&self, home: &Home, name: &str, revision: &str) -> Result<PathBuf>;
}

struct SystemBackend;

impl Backend for SystemBackend {
	fn resolve_oci(&self, reference: &str, arch: &str) -> Result<ResolvedOciImage> {
		image::resolve_oci_reference(reference, Some(arch))
	}

	fn build(
		&self,
		dockerfile: &Path,
		context: &Path,
		tag: &str,
		arch: &str,
	) -> Result<ResolvedOciImage> {
		let reference =
			image::build::build_image(dockerfile, context, tag, Some(arch)).map_err(|error| {
				EngineError::engine(format!(
					"function image build failed; verify pinned apt/uv packages and build commands: \
					 {error}"
				))
			})?;
		image::resolve_oci_reference(&reference, Some(arch))
	}

	fn template(&self, home: &Home, name: &str, revision: &str) -> Result<PathBuf> {
		validate_sha256_hex(revision, "template revision")?;
		let pointer = home.cas_dir().join(format!("{revision}.json"));
		let bytes = fs::read(&pointer).map_err(|error| {
			if error.kind() == std::io::ErrorKind::NotFound {
				EngineError::not_found(format!(
					"template {name}@{revision} is not present in the server CAS"
				))
			} else {
				error.into()
			}
		})?;
		let value: serde_json::Value = serde_json::from_slice(&bytes)?;
		let template_name = value
			.get("tpl_name")
			.and_then(|value| value.as_str())
			.unwrap_or_default();
		if template_name != name {
			return Err(EngineError::invalid(format!(
				"template revision {revision} belongs to {template_name:?}, not {name:?}"
			)));
		}
		let path = value
			.get("template_dir")
			.and_then(|value| value.as_str())
			.map(PathBuf::from)
			.ok_or_else(|| EngineError::engine("template CAS pointer is missing template_dir"))?;
		if !path.is_dir() || !path.join("agent-ready.json").is_file() {
			return Err(EngineError::not_found(format!("template {name}@{revision} is incomplete")));
		}
		let actual = image::cas::template_digest(&path)?;
		if actual != revision {
			return Err(EngineError::engine(format!(
				"template {name}@{revision} failed immutable digest verification"
			)));
		}
		Ok(path)
	}
}

/// Resolve, validate, and (when needed) build a finalized function image.
///
/// This function performs no secret resolution. Secret values must only be
/// attached to the sandbox after this non-secret build plan has completed.
pub fn realize(
	home: &Home,
	spec: &pb::ImageSpec,
	architecture: pb::CpuArchitecture,
) -> Result<RealizedImage> {
	realize_with(&SystemBackend, home, spec, architecture)
}

fn realize_with(
	backend: &impl Backend,
	home: &Home,
	spec: &pb::ImageSpec,
	architecture: pb::CpuArchitecture,
) -> Result<RealizedImage> {
	let arch = architecture_name(architecture)?;
	validate_inputs(spec)?;
	let mut normalized = spec.clone();
	normalize_spec(&mut normalized);
	let artifacts = ArtifactStore::open(home.function_artifacts_dir())?;
	let source = resolve_source(backend, home, &artifacts, &normalized, arch)?;
	verify_caller_digest(normalized.resolved_oci_digest.as_ref(), &source.digest)?;
	if source.platform != format!("linux/{arch}") {
		return Err(EngineError::unsupported(format!(
			"image platform {} does not match requested linux/{arch}",
			source.platform
		)));
	}

	normalized.platform.clone_from(&source.platform);
	normalized.resolved_oci_digest = Some(pb::Digest {
		algorithm: pb::DigestAlgorithm::Sha256 as i32,
		value:     hex::decode(&source.digest)
			.map_err(|_| EngineError::engine("resolved image digest is invalid"))?,
	});
	let provenance_digest = manifest_digest(&normalized, &source)?;
	if matches!(
		normalized.source.as_ref(),
		Some(pb::image_spec::Source::Python(_) | pb::image_spec::Source::Registry(_))
	) && let Launch::Image(reference) = &source.launch
	{
		normalized.source = Some(pb::image_spec::Source::Registry(pb::RegistryImageSource {
			reference: reference.clone(),
		}));
	}
	let environment = normalized
		.environment
		.iter()
		.map(|(k, v)| (k.clone(), v.clone()))
		.collect();
	let (image, template) = match source.launch {
		Launch::Image(reference) => (Some(reference), None),
		Launch::Template(reference) => (None, Some(reference)),
	};
	let (dockerfile, context) = (None, None);
	Ok(RealizedImage {
		image,
		template,
		dockerfile,
		context,
		environment,
		resolved_spec: normalized,
		provenance_digest,
	})
}

fn resolve_source(
	backend: &impl Backend,
	home: &Home,
	artifacts: &ArtifactStore,
	spec: &pb::ImageSpec,
	arch: &str,
) -> Result<SourceResolution> {
	match spec
		.source
		.as_ref()
		.ok_or_else(|| EngineError::invalid("image source is required"))?
	{
		pb::image_spec::Source::Python(source) => {
			validate_python_source(source)?;
			let reference =
				format!("{PYTHON_REPOSITORY}:{}-{}", source.python_version, source.variant);
			let resolved = backend.resolve_oci(&reference, arch)?;
			resolve_layered(backend, home, artifacts, spec, resolved, arch)
		},
		pb::image_spec::Source::Registry(source) => {
			if source.reference.starts_with("oci:") {
				validate_local_oci_reference(home, &source.reference)?;
				let resolved = backend.resolve_oci(&source.reference, arch)?;
				Ok(SourceResolution {
					launch:   Launch::Image(resolved.reference),
					digest:   resolved.digest,
					platform: resolved.platform,
				})
			} else {
				let resolved = backend.resolve_oci(&source.reference, arch)?;
				resolve_layered(backend, home, artifacts, spec, resolved, arch)
			}
		},
		pb::image_spec::Source::Dockerfile(source) => {
			let context_digest = artifact_digest(source.context.as_ref(), "Dockerfile context")?;
			let root = materialize_archive(home, artifacts, &context_digest)?;
			let dockerfile_rel = safe_relative(&source.dockerfile_path, "Dockerfile path")?;
			let dockerfile = root.join(&dockerfile_rel);
			ensure_confined_regular(&root, &dockerfile, "Dockerfile")?;
			let plan = prepare_build_context(home, artifacts, spec, Some((&root, &dockerfile)), None)?;
			let tag = format!("vmon-function:{}", plan.digest);
			let built = build_cached(backend, &plan, &tag, arch)?;
			Ok(SourceResolution {
				launch:   Launch::Image(built.reference),
				digest:   built.digest,
				platform: built.platform,
			})
		},
		pb::image_spec::Source::Template(source) => {
			if has_layers(spec) {
				return Err(EngineError::unsupported(
					"apt, uv, commands, and local artifacts cannot be layered onto a template source",
				));
			}
			if source.name.trim().is_empty() {
				return Err(EngineError::invalid("template name is required"));
			}
			let host_arch = match std::env::consts::ARCH {
				"x86_64" => "amd64",
				"aarch64" => "arm64",
				other => other,
			};
			if arch != host_arch {
				return Err(EngineError::unsupported(format!(
					"template architecture linux/{host_arch} does not match requested linux/{arch}"
				)));
			}
			backend.template(home, &source.name, &source.revision)?;
			Ok(SourceResolution {
				launch:   Launch::Template(format!("{}@{}", source.name, source.revision)),
				digest:   source.revision.clone(),
				platform: format!("linux/{arch}"),
			})
		},
	}
}

fn resolve_layered(
	backend: &impl Backend,
	home: &Home,
	artifacts: &ArtifactStore,
	spec: &pb::ImageSpec,
	base: ResolvedOciImage,
	arch: &str,
) -> Result<SourceResolution> {
	if !has_layers(spec) {
		return Ok(SourceResolution {
			launch:   Launch::Image(base.reference),
			digest:   base.digest,
			platform: base.platform,
		});
	}
	let plan = prepare_build_context(home, artifacts, spec, None, Some(&base.reference))?;
	let tag = format!("vmon-function:{}", plan.digest);
	let built = build_cached(backend, &plan, &tag, arch)?;
	Ok(SourceResolution {
		launch:   Launch::Image(built.reference),
		digest:   built.digest,
		platform: built.platform,
	})
}

struct BuildPlan {
	dockerfile: PathBuf,
	context:    PathBuf,
	digest:     String,
}

#[derive(Deserialize, Serialize)]
struct CachedBuild {
	reference: String,
	digest:    String,
	platform:  String,
}

struct CacheLock(fs::File);

impl CacheLock {
	fn acquire(path: &Path) -> Result<Self> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.mode(0o600)
			.open(path)?;
		// SAFETY: `file` owns this valid descriptor for the lifetime of the lock.
		if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
			return Err(std::io::Error::last_os_error().into());
		}
		Ok(Self(file))
	}
}

impl Drop for CacheLock {
	fn drop(&mut self) {
		// SAFETY: the descriptor remains valid until this drop completes.
		let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
	}
}

fn build_cached(
	backend: &impl Backend,
	plan: &BuildPlan,
	tag: &str,
	arch: &str,
) -> Result<ResolvedOciImage> {
	let cache = plan
		.context
		.parent()
		.ok_or_else(|| EngineError::engine("function build context has no cache directory"))?
		.join(format!("resolved-{arch}.json"));
	let lock = cache.with_extension(format!("{arch}.lock"));
	let _guard = CacheLock::acquire(&lock)?;
	if let Ok(bytes) = fs::read(&cache)
		&& let Ok(cached) = serde_json::from_slice::<CachedBuild>(&bytes)
		&& validate_sha256_hex(&cached.digest, "cached image digest").is_ok()
		&& cached.platform == format!("linux/{arch}")
		&& let Ok(resolved) = backend.resolve_oci(&cached.reference, arch)
		&& resolved.digest == cached.digest
		&& resolved.platform == cached.platform
	{
		return Ok(ResolvedOciImage {
			reference: cached.reference,
			digest:    cached.digest,
			platform:  cached.platform,
		});
	}
	let built = backend.build(&plan.dockerfile, &plan.context, tag, arch)?;
	validate_sha256_hex(&built.digest, "built image digest")?;
	if built.platform != format!("linux/{arch}") {
		return Err(EngineError::unsupported(format!(
			"built image platform {} does not match requested linux/{arch}",
			built.platform
		)));
	}
	let payload = serde_json::to_vec(&CachedBuild {
		reference: built.reference.clone(),
		digest:    built.digest.clone(),
		platform:  built.platform.clone(),
	})?;
	let temporary = cache.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
	fs::write(&temporary, payload)?;
	match fs::rename(&temporary, &cache) {
		Ok(()) => {},
		Err(error) if cache.is_file() => {
			let _ = fs::remove_file(&temporary);
			let _ = error;
		},
		Err(error) => return Err(error.into()),
	}
	Ok(built)
}

fn prepare_build_context(
	home: &Home,
	artifacts: &ArtifactStore,
	spec: &pb::ImageSpec,
	existing: Option<(&Path, &Path)>,
	base: Option<&str>,
) -> Result<BuildPlan> {
	let mut recipe = dockerfile_recipe(spec, existing.map(|(_, dockerfile)| dockerfile), base)?;
	if let Some((root, _)) = existing {
		let context_digest = fs::read_to_string(root.join(".complete"))?;
		recipe.push_str("# vmon-context-sha256=");
		recipe.push_str(&context_digest);
		recipe.push('\n');
	}
	let digest = hex::encode(Sha256::digest(recipe.as_bytes()));
	let context = home
		.images_dir()
		.join("function-builds")
		.join(&digest)
		.join("context");
	let dockerfile = context.join(".vmon.Dockerfile");
	if dockerfile.is_file() {
		return Ok(BuildPlan { dockerfile, context, digest });
	}
	let temporary = context.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
	fs::create_dir_all(&temporary)?;
	if let Some((source, _)) = existing {
		copy_tree_confined(source, &temporary)?;
	}
	let artifact_dir = temporary.join(".vmon-artifacts");
	for mount in &spec.local_artifact_mounts {
		let digest = artifact_digest(mount.artifact.as_ref(), "local artifact mount")?;
		let destination = artifact_dir.join(&digest);
		if !destination.is_file() {
			let bytes = artifacts.read(&digest, None)?;
			fs::create_dir_all(&artifact_dir)?;
			write_new(&destination, &bytes, 0o400)?;
		}
	}
	write_new(&temporary.join(".vmon.Dockerfile"), recipe.as_bytes(), 0o600)?;
	if let Some(parent) = context.parent() {
		fs::create_dir_all(parent)?;
	}
	match fs::rename(&temporary, &context) {
		Ok(()) => {},
		Err(_) if context.join(".vmon.Dockerfile").is_file() => {
			let _ = fs::remove_dir_all(&temporary);
		},
		Err(error) => return Err(error.into()),
	}
	Ok(BuildPlan { dockerfile, context, digest })
}

fn dockerfile_recipe(
	spec: &pb::ImageSpec,
	original: Option<&Path>,
	base: Option<&str>,
) -> Result<String> {
	let mut out = String::from("# syntax=docker/dockerfile:1\n");
	if let Some(base) = base {
		out.push_str("FROM ");
		out.push_str(base);
		out.push('\n');
	} else if let Some(original) = original {
		let body = fs::read_to_string(original)
			.map_err(|_| EngineError::invalid("Dockerfile must be UTF-8 text"))?;
		if contains_secret_text(&body) {
			return Err(EngineError::invalid(
				"Dockerfile contains secret-looking input; use runtime secrets",
			));
		}
		validate_dockerfile(&body)?;
		out.push_str(&body);
		if !out.ends_with('\n') {
			out.push('\n');
		}
	} else {
		return Err(EngineError::engine("build recipe has no base source"));
	}
	if !spec.apt_packages.is_empty() {
		let packages = spec
			.apt_packages
			.iter()
			.map(|package| format!("{}={}", package.name, package.version))
			.collect::<Vec<_>>()
			.join(" ");
		out.push_str(
			"RUN [\"/bin/sh\",\"-c\",\"apt-get update && apt-get install -y --no-install-recommends ",
		);
		out.push_str(&packages);
		out.push_str(" && rm -rf /var/lib/apt/lists/*\"]\n");
	}
	for package in &spec.uv_packages {
		let mut argv =
			vec!["python".to_owned(), "-m".to_owned(), "pip".to_owned(), "install".to_owned()];
		if let Some(pb::uv_package::IndexUrlPresence::IndexUrl(index)) = &package.index_url_presence {
			argv.push("--index-url".to_owned());
			argv.push(index.clone());
		}
		argv.push(format!("{}=={}", package.name, package.version));
		out.push_str("RUN ");
		out.push_str(&serde_json::to_string(&argv)?);
		out.push('\n');
	}
	for mount in &spec.local_artifact_mounts {
		let digest = artifact_digest(mount.artifact.as_ref(), "local artifact mount")?;
		out.push_str("COPY --chown=0:0 .vmon-artifacts/");
		out.push_str(&digest);
		out.push(' ');
		out.push_str(&serde_json::to_string(&mount.path)?);
		out.push('\n');
	}
	for command in &spec.commands {
		out.push_str("RUN ");
		out.push_str(&serde_json::to_string(&command.argv)?);
		out.push('\n');
	}
	for (key, value) in sorted_environment(&spec.environment) {
		out.push_str("ENV ");
		out.push_str(key);
		out.push('=');
		out.push_str(&serde_json::to_string(value)?);
		out.push('\n');
	}
	Ok(out)
}

fn validate_dockerfile(body: &str) -> Result<()> {
	let mut from_count = 0usize;
	let mut stages = BTreeSet::new();
	for line in body.lines() {
		let line = line.trim();
		if line.is_empty() || line.starts_with('#') {
			continue;
		}
		let (directive, arguments) = line
			.split_once(char::is_whitespace)
			.map_or((line, ""), |(directive, arguments)| (directive, arguments.trim()));
		match directive.to_ascii_uppercase().as_str() {
			"FROM" => {
				let words = arguments.split_whitespace().collect::<Vec<_>>();
				let image = words
					.iter()
					.find(|argument| !argument.starts_with("--"))
					.ok_or_else(|| EngineError::invalid("Dockerfile FROM is missing an image"))?;
				if arguments.contains("--platform=") {
					return Err(EngineError::unsupported(
						"Dockerfile must not override the server-selected platform",
					));
				}
				if *image != "scratch" {
					let Some(digest) = image.rsplit_once("@sha256:").map(|(_, digest)| digest) else {
						return Err(EngineError::invalid(
							"Dockerfile FROM images must be pinned with @sha256:<manifest>",
						));
					};
					validate_sha256_hex(digest, "Dockerfile FROM digest")?;
				}
				if let Some(position) = words
					.iter()
					.position(|word| word.eq_ignore_ascii_case("AS"))
					&& let Some(alias) = words.get(position + 1)
				{
					stages.insert(alias.to_ascii_lowercase());
				}
				from_count += 1;
			},
			"RUN" => {
				return Err(EngineError::unsupported(
					"Dockerfile RUN is not reproducible because build commands may access the network; \
					 use typed image build inputs",
				));
			},
			"COPY" => {
				if let Some(source) = arguments.split_whitespace().find_map(|argument| {
					argument
						.get(.."--from=".len())
						.filter(|prefix| prefix.eq_ignore_ascii_case("--from="))
						.map(|_| &argument["--from=".len()..])
				}) {
					let local_number = source
						.parse::<usize>()
						.is_ok_and(|index| index < from_count);
					let local_alias = stages.contains(&source.to_ascii_lowercase());
					if !local_number && !local_alias {
						let Some(digest) = source.rsplit_once("@sha256:").map(|(_, digest)| digest)
						else {
							return Err(EngineError::invalid(
								"Dockerfile external COPY --from source must be pinned with \
								 @sha256:<manifest>",
							));
						};
						validate_sha256_hex(digest, "Dockerfile COPY --from digest")?;
					}
				}
			},
			"ADD" if arguments.contains("://") => {
				return Err(EngineError::unsupported(
					"Dockerfile ADD may not fetch remote mutable content; upload an artifact",
				));
			},
			"ONBUILD" => {
				return Err(EngineError::unsupported(
					"Dockerfile ONBUILD is not reproducible because deferred instructions are not \
					 validated",
				));
			},
			"ARG" | "ENV" => {
				let name = arguments
					.split(|character: char| character == '=' || character.is_whitespace())
					.next()
					.unwrap_or_default();
				if secret_name(name) {
					return Err(EngineError::invalid(
						"Dockerfile contains secret-looking ARG/ENV; use runtime secrets",
					));
				}
			},
			_ => {},
		}
	}
	if from_count == 0 {
		return Err(EngineError::invalid("Dockerfile requires at least one FROM instruction"));
	}
	Ok(())
}

fn validate_inputs(spec: &pb::ImageSpec) -> Result<()> {
	let mut apt_names = BTreeSet::new();
	let mut uv_names = BTreeSet::new();
	let mut mount_paths = BTreeSet::new();
	for package in &spec.apt_packages {
		if !valid_package_name(&package.name) || !pinned_version(&package.version) {
			return Err(EngineError::invalid(format!(
				"apt package {:?} requires an exact version",
				package.name
			)));
		}
		if !apt_names.insert(package.name.as_str()) {
			return Err(EngineError::invalid(format!(
				"apt package {:?} is declared more than once",
				package.name
			)));
		}
	}
	for package in &spec.uv_packages {
		if !valid_python_name(&package.name) || !pinned_version(&package.version) {
			return Err(EngineError::invalid(format!(
				"uv package {:?} requires an exact version",
				package.name
			)));
		}
		if !uv_names.insert(package.name.to_ascii_lowercase().replace('_', "-")) {
			return Err(EngineError::invalid(format!(
				"uv package {:?} is declared more than once",
				package.name
			)));
		}
		if let Some(url) = package
			.index_url_presence
			.as_ref()
			.map(|value| match value {
				pb::uv_package::IndexUrlPresence::IndexUrl(url) => url.as_str(),
			}) && (!url.starts_with("https://")
			|| url.contains('@')
			|| url.contains('?')
			|| url.contains('#'))
		{
			return Err(EngineError::invalid(
				"uv index URL must be immutable HTTPS without credentials or query parameters",
			));
		}
	}
	for command in &spec.commands {
		if command.argv.is_empty()
			|| command
				.argv
				.iter()
				.any(|arg| arg.contains('\0') || contains_secret_text(arg) || secret_argument(arg))
		{
			return Err(EngineError::invalid("build command requires non-secret, NUL-free argv"));
		}
		let executable = Path::new(&command.argv[0])
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or_default();
		if matches!(
			executable,
			"sh"
				| "bash" | "dash"
				| "zsh" | "env"
				| "sudo" | "su"
				| "curl" | "wget"
				| "git" | "pip"
				| "pip3" | "uv"
				| "apt" | "apt-get"
				| "apk" | "dnf"
				| "yum"
		) {
			return Err(EngineError::unsupported(format!(
				"build command executable {executable:?} is unsupported; provide direct deterministic \
				 argv"
			)));
		}
	}
	for (key, value) in &spec.environment {
		if !valid_env_name(key) || secret_name(key) || contains_secret_text(value) {
			return Err(EngineError::invalid(format!(
				"environment variable {key:?} is invalid or secret-looking; use SecretRef"
			)));
		}
		if value.contains('\0') || value.contains('\n') || value.contains('\r') {
			return Err(EngineError::invalid(format!(
				"environment variable {key:?} contains a forbidden control character"
			)));
		}
	}
	for mount in &spec.local_artifact_mounts {
		artifact_digest(mount.artifact.as_ref(), "local artifact mount")?;
		validate_absolute_destination(&mount.path)?;
		if !mount_paths.insert(mount.path.as_str()) {
			return Err(EngineError::invalid(format!(
				"local artifact destination {} is declared more than once",
				mount.path
			)));
		}
		if !mount.read_only {
			return Err(EngineError::unsupported(format!(
				"local artifact mount {} must be read-only",
				mount.path
			)));
		}
	}
	Ok(())
}

fn validate_python_source(source: &pb::PythonImageSource) -> Result<()> {
	let parts = source.python_version.split('.').collect::<Vec<_>>();
	if !matches!(parts.len(), 2 | 3)
		|| parts
			.iter()
			.any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
		|| parts[0] != "3"
	{
		return Err(EngineError::invalid(
			"Python image version must be a supported Python 3 major.minor or major.minor.patch \
			 release",
		));
	}
	if source.variant.is_empty()
		|| !source
			.variant
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
	{
		return Err(EngineError::invalid("Python image variant contains unsupported characters"));
	}
	Ok(())
}

fn validate_local_oci_reference(home: &Home, reference: &str) -> Result<()> {
	let location = reference
		.strip_prefix("oci:")
		.and_then(|value| value.rsplit_once(':').map(|(path, _)| path))
		.ok_or_else(|| EngineError::invalid("resolved local OCI reference is malformed"))?;
	let path = Path::new(location);
	let canonical = path
		.canonicalize()
		.map_err(|_| EngineError::not_found("resolved local OCI build cache entry is missing"))?;
	let builds = home
		.root()
		.join("builds")
		.canonicalize()
		.map_err(|_| EngineError::not_found("server OCI build cache is missing"))?;
	if !canonical.is_dir() || !canonical.starts_with(builds) {
		return Err(EngineError::invalid(
			"resolved local OCI reference escapes the server build cache",
		));
	}
	Ok(())
}

fn verify_caller_digest(caller: Option<&pb::Digest>, actual: &str) -> Result<()> {
	let Some(caller) = caller else {
		return Ok(());
	};
	if caller.algorithm != pb::DigestAlgorithm::Sha256 as i32 || caller.value.len() != 32 {
		return Err(EngineError::invalid("resolved OCI digest must be a 32-byte SHA-256 digest"));
	}
	if hex::encode(&caller.value) != actual {
		return Err(EngineError::invalid(format!(
			"resolved OCI digest does not match the current immutable manifest (expected \
			 sha256:{actual})"
		)));
	}
	Ok(())
}

fn normalize_spec(spec: &mut pb::ImageSpec) {
	spec
		.apt_packages
		.sort_by(|a, b| (&a.name, &a.version).cmp(&(&b.name, &b.version)));
	spec
		.uv_packages
		.sort_by(|a, b| (&a.name, &a.version).cmp(&(&b.name, &b.version)));
	spec
		.local_artifact_mounts
		.sort_by(|a, b| a.path.cmp(&b.path));
}

#[derive(Serialize)]
struct Manifest<'a> {
	version:         u8,
	source:          String,
	resolved_digest: &'a str,
	platform:        &'a str,
	apt:             Vec<(&'a str, &'a str)>,
	uv:              Vec<(&'a str, &'a str, Option<&'a str>)>,
	commands:        Vec<&'a [String]>,
	environment:     BTreeMap<&'a str, &'a str>,
	artifacts:       Vec<(String, &'a str, bool)>,
}

fn manifest_digest(spec: &pb::ImageSpec, source: &SourceResolution) -> Result<String> {
	let source_name = match spec.source.as_ref().expect("validated source") {
		pb::image_spec::Source::Python(value) => {
			format!("python:{}-{}", value.python_version, value.variant)
		},
		pb::image_spec::Source::Registry(_) => match &source.launch {
			Launch::Image(reference) => reference.clone(),
			_ => unreachable!(),
		},
		pb::image_spec::Source::Dockerfile(value) => format!(
			"dockerfile:{}:{}",
			artifact_digest(value.context.as_ref(), "Dockerfile context")?,
			value.dockerfile_path
		),
		pb::image_spec::Source::Template(value) => {
			format!("template:{}@{}", value.name, value.revision)
		},
	};
	let manifest = Manifest {
		version:         1,
		source:          source_name,
		resolved_digest: &source.digest,
		platform:        &source.platform,
		apt:             spec
			.apt_packages
			.iter()
			.map(|p| (p.name.as_str(), p.version.as_str()))
			.collect(),
		uv:              spec
			.uv_packages
			.iter()
			.map(|p| {
				let index = p.index_url_presence.as_ref().map(|value| match value {
					pb::uv_package::IndexUrlPresence::IndexUrl(url) => url.as_str(),
				});
				(p.name.as_str(), p.version.as_str(), index)
			})
			.collect(),
		commands:        spec
			.commands
			.iter()
			.map(|command| command.argv.as_slice())
			.collect(),
		environment:     sorted_environment(&spec.environment),
		artifacts:       spec
			.local_artifact_mounts
			.iter()
			.map(|mount| {
				Ok((
					artifact_digest(mount.artifact.as_ref(), "local artifact mount")?,
					mount.path.as_str(),
					mount.read_only,
				))
			})
			.collect::<Result<Vec<_>>>()?,
	};
	Ok(hex::encode(Sha256::digest(serde_json::to_vec(&manifest)?)))
}

fn artifact_digest(reference: Option<&pb::ArtifactRef>, label: &str) -> Result<String> {
	let digest = reference
		.and_then(|reference| reference.digest.as_ref())
		.ok_or_else(|| EngineError::invalid(format!("{label} digest is required")))?;
	if digest.algorithm != pb::DigestAlgorithm::Sha256 as i32 || digest.value.len() != 32 {
		return Err(EngineError::invalid(format!("{label} must use a 32-byte SHA-256 digest")));
	}
	Ok(hex::encode(&digest.value))
}

fn materialize_archive(home: &Home, artifacts: &ArtifactStore, digest: &str) -> Result<PathBuf> {
	let final_dir = home.images_dir().join("function-contexts").join(digest);
	if final_dir.join(".complete").is_file() {
		return Ok(final_dir);
	}
	let bytes = artifacts.read(digest, None)?;
	if bytes.len() > MAX_CONTEXT_BYTES {
		return Err(EngineError::invalid("Dockerfile context exceeds 512 MiB extraction limit"));
	}
	let temporary = final_dir.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
	fs::create_dir_all(&temporary)?;
	extract_tar(&bytes, &temporary)?;
	write_new(&temporary.join(".complete"), digest.as_bytes(), 0o400)?;
	if let Some(parent) = final_dir.parent() {
		fs::create_dir_all(parent)?;
	}
	match fs::rename(&temporary, &final_dir) {
		Ok(()) => {},
		Err(_) if final_dir.join(".complete").is_file() => {
			let _ = fs::remove_dir_all(&temporary);
		},
		Err(error) => return Err(error.into()),
	}
	Ok(final_dir)
}

fn extract_tar(bytes: &[u8], destination: &Path) -> Result<()> {
	let mut offset = 0usize;
	while offset
		.checked_add(TAR_BLOCK)
		.is_some_and(|end| end <= bytes.len())
	{
		let header = &bytes[offset..offset + TAR_BLOCK];
		if header.iter().all(|byte| *byte == 0) {
			return Ok(());
		}
		let stored = tar_octal(&header[148..156])?;
		let mut checksum_header = header.to_vec();
		checksum_header[148..156].fill(b' ');
		let actual: u64 = checksum_header.iter().map(|byte| u64::from(*byte)).sum();
		if stored != actual {
			return Err(EngineError::invalid("Dockerfile context has an invalid tar checksum"));
		}
		let name = tar_text(&header[0..100])?;
		let prefix = tar_text(&header[345..500])?;
		let relative = if prefix.is_empty() {
			name
		} else {
			format!("{prefix}/{name}")
		};
		let relative = safe_relative(&relative, "archive entry")?;
		let size = usize::try_from(tar_octal(&header[124..136])?)
			.map_err(|_| EngineError::invalid("archive entry is too large"))?;
		let data_start = offset + TAR_BLOCK;
		let data_end = data_start
			.checked_add(size)
			.ok_or_else(|| EngineError::invalid("archive size overflow"))?;
		if data_end > bytes.len() {
			return Err(EngineError::invalid("Dockerfile context is truncated"));
		}
		let path = destination.join(relative);
		match header[156] {
			0 | b'0' => {
				if let Some(parent) = path.parent() {
					fs::create_dir_all(parent)?;
				}
				write_new(&path, &bytes[data_start..data_end], 0o600)?;
			},
			b'5' => fs::create_dir_all(&path)?,
			b'1' | b'2' => {
				return Err(EngineError::invalid("Dockerfile context may not contain links"));
			},
			kind => {
				return Err(EngineError::unsupported(format!(
					"Dockerfile context contains unsupported tar entry type {kind}"
				)));
			},
		}
		offset = data_start + size.div_ceil(TAR_BLOCK) * TAR_BLOCK;
	}
	Err(EngineError::invalid("Dockerfile context is not a complete tar archive"))
}

fn copy_tree_confined(source: &Path, destination: &Path) -> Result<()> {
	for entry in fs::read_dir(source)? {
		let entry = entry?;
		let source_path = entry.path();
		let destination_path = destination.join(entry.file_name());
		let metadata = fs::symlink_metadata(&source_path)?;
		if metadata.file_type().is_symlink() {
			return Err(EngineError::invalid("Dockerfile context may not contain symlinks"));
		}
		if metadata.is_dir() {
			fs::create_dir_all(&destination_path)?;
			copy_tree_confined(&source_path, &destination_path)?;
		} else if metadata.is_file() {
			fs::copy(&source_path, &destination_path)?;
		} else {
			return Err(EngineError::unsupported("Dockerfile context contains a non-regular file"));
		}
	}
	Ok(())
}

fn ensure_confined_regular(root: &Path, path: &Path, label: &str) -> Result<()> {
	let metadata = fs::symlink_metadata(path).map_err(|error| {
		if error.kind() == std::io::ErrorKind::NotFound {
			EngineError::not_found(format!("{label} is missing from the verified context"))
		} else {
			error.into()
		}
	})?;
	if !metadata.is_file() || metadata.file_type().is_symlink() {
		return Err(EngineError::invalid(format!("{label} must be a regular file")));
	}
	let canonical_root = root.canonicalize()?;
	let canonical_path = path.canonicalize()?;
	if !canonical_path.starts_with(canonical_root) {
		return Err(EngineError::invalid(format!("{label} escapes its context")));
	}
	Ok(())
}

fn safe_relative(value: &str, label: &str) -> Result<PathBuf> {
	let path = Path::new(value);
	if value.is_empty() || path.is_absolute() {
		return Err(EngineError::invalid(format!("{label} must be a safe relative path")));
	}
	let mut normalized = PathBuf::new();
	for component in path.components() {
		match component {
			Component::Normal(component) => normalized.push(component),
			Component::CurDir => {},
			_ => return Err(EngineError::invalid(format!("{label} must be a safe relative path"))),
		}
	}
	if normalized.as_os_str().is_empty() {
		return Err(EngineError::invalid(format!("{label} must name a relative path")));
	}
	Ok(normalized)
}

fn validate_absolute_destination(value: &str) -> Result<()> {
	let path = Path::new(value);
	if !path.is_absolute()
		|| path
			.components()
			.any(|component| matches!(component, Component::ParentDir | Component::CurDir))
		|| value == "/"
	{
		return Err(EngineError::invalid(
			"local artifact destination must be a normalized absolute non-root path",
		));
	}
	Ok(())
}

fn write_new(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(mode)
		.open(path)?;
	file.write_all(bytes)?;
	file.sync_all()?;
	fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
	Ok(())
}

fn architecture_name(architecture: pb::CpuArchitecture) -> Result<&'static str> {
	match architecture {
		pb::CpuArchitecture::Amd64 => Ok("amd64"),
		pb::CpuArchitecture::Arm64 => Ok("arm64"),
		pb::CpuArchitecture::Unspecified => {
			#[cfg(target_arch = "x86_64")]
			return Ok("amd64");
			#[cfg(target_arch = "aarch64")]
			return Ok("arm64");
			#[allow(unreachable_code)]
			Err(EngineError::unsupported(
				"server compile-target architecture is unsupported for function images",
			))
		},
	}
}

const fn has_layers(spec: &pb::ImageSpec) -> bool {
	!spec.apt_packages.is_empty()
		|| !spec.uv_packages.is_empty()
		|| !spec.commands.is_empty()
		|| !spec.local_artifact_mounts.is_empty()
}

fn sorted_environment<K, V>(environment: &std::collections::HashMap<K, V>) -> BTreeMap<&str, &str>
where
	K: AsRef<str> + Eq + std::hash::Hash,
	V: AsRef<str>,
{
	environment
		.iter()
		.map(|(key, value)| (key.as_ref(), value.as_ref()))
		.collect()
}

fn valid_package_name(value: &str) -> bool {
	!value.is_empty()
		&& value.bytes().all(|byte| {
			byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'+' | b'.' | b'-')
		})
}
fn valid_python_name(value: &str) -> bool {
	!value.is_empty()
		&& value
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
fn pinned_version(value: &str) -> bool {
	!value.is_empty()
		&& value.bytes().all(|byte| {
			byte.is_ascii_alphanumeric()
				|| matches!(byte, b'.' | b'+' | b'-' | b'_' | b':' | b'!' | b'~')
		})
}
fn valid_env_name(value: &str) -> bool {
	let mut bytes = value.bytes();
	bytes
		.next()
		.is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_')
		&& bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
fn secret_name(value: &str) -> bool {
	let upper = value.to_ascii_uppercase();
	["SECRET", "TOKEN", "PASSWORD", "PASSWD", "PRIVATE_KEY", "API_KEY", "ACCESS_KEY", "CREDENTIAL"]
		.iter()
		.any(|needle| upper.contains(needle))
}
fn secret_argument(value: &str) -> bool {
	let normalized = value
		.trim_start_matches('-')
		.split('=')
		.next()
		.unwrap_or_default()
		.replace('-', "_");
	secret_name(&normalized)
}
fn contains_secret_text(value: &str) -> bool {
	let lower = value.to_ascii_lowercase();
	lower.contains("-----begin private key")
		|| lower.contains("://")
			&& lower.split("://").nth(1).is_some_and(|rest| {
				rest
					.split('/')
					.next()
					.is_some_and(|authority| authority.contains('@'))
			})
}
fn validate_sha256_hex(value: &str, label: &str) -> Result<()> {
	if value.len() != 64
		|| !value
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
	{
		return Err(EngineError::invalid(format!("{label} must be a lowercase SHA-256 digest")));
	}
	Ok(())
}
fn tar_text(field: &[u8]) -> Result<String> {
	let end = field
		.iter()
		.position(|byte| *byte == 0)
		.unwrap_or(field.len());
	std::str::from_utf8(&field[..end])
		.map(str::to_owned)
		.map_err(|_| EngineError::invalid("archive path is not UTF-8"))
}
fn tar_octal(field: &[u8]) -> Result<u64> {
	let text = std::str::from_utf8(field)
		.map_err(|_| EngineError::invalid("archive header is invalid"))?
		.trim_matches(['\0', ' ']);
	if text.is_empty() {
		return Ok(0);
	}
	u64::from_str_radix(text, 8)
		.map_err(|_| EngineError::invalid("archive numeric field is invalid"))
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::*;

	struct FakeBackend {
		digest: String,
		arch:   &'static str,
		builds: AtomicUsize,
	}
	impl FakeBackend {
		fn new(byte: u8) -> Self {
			Self { digest: hex::encode([byte; 32]), arch: "amd64", builds: AtomicUsize::new(0) }
		}
	}
	impl Backend for FakeBackend {
		fn resolve_oci(&self, reference: &str, _arch: &str) -> Result<ResolvedOciImage> {
			Ok(ResolvedOciImage {
				reference: format!("{}@sha256:{}", reference.split(':').next().unwrap(), self.digest),
				digest:    self.digest.clone(),
				platform:  format!("linux/{}", self.arch),
			})
		}

		fn build(
			&self,
			_dockerfile: &Path,
			_context: &Path,
			_tag: &str,
			_arch: &str,
		) -> Result<ResolvedOciImage> {
			self.builds.fetch_add(1, Ordering::SeqCst);
			Ok(ResolvedOciImage {
				reference: format!("build@sha256:{}", self.digest),
				digest:    self.digest.clone(),
				platform:  format!("linux/{}", self.arch),
			})
		}

		fn template(&self, home: &Home, _name: &str, revision: &str) -> Result<PathBuf> {
			let path = home.templates_dir().join(revision);
			fs::create_dir_all(&path)?;
			Ok(path)
		}
	}
	fn digest(byte: u8) -> pb::Digest {
		pb::Digest { algorithm: pb::DigestAlgorithm::Sha256 as i32, value: vec![byte; 32] }
	}
	fn artifact_ref(byte: u8) -> pb::ArtifactRef {
		pb::ArtifactRef { digest: Some(digest(byte)) }
	}
	#[test]
	fn sdk_defaults_use_host_architecture_and_accept_major_minor_python() {
		let (_temp, home) = home();
		let arch = architecture_name(pb::CpuArchitecture::Unspecified).unwrap();
		let backend =
			FakeBackend { digest: hex::encode([7u8; 32]), arch, builds: AtomicUsize::new(0) };
		let spec = pb::ImageSpec {
			source: Some(pb::image_spec::Source::Python(pb::PythonImageSource {
				python_version: "3.14".into(),
				variant:        "slim".into(),
			})),
			..Default::default()
		};
		let realized =
			realize_with(&backend, &home, &spec, pb::CpuArchitecture::Unspecified).unwrap();
		assert_eq!(realized.resolved_spec.platform, format!("linux/{arch}"));
		assert!(realized.image.unwrap().starts_with(PYTHON_REPOSITORY));

		let mut unsupported = spec;
		if let Some(pb::image_spec::Source::Python(source)) = &mut unsupported.source {
			source.python_version = "2.7".into();
		}
		assert!(
			realize_with(&backend, &home, &unsupported, pb::CpuArchitecture::Unspecified)
				.unwrap_err()
				.to_string()
				.contains("supported Python 3")
		);
	}

	#[test]
	fn dockerfile_rejects_network_run_and_unpinned_external_copy() {
		let digest = hex::encode([8u8; 32]);
		assert!(
			validate_dockerfile("FROM scratch\nRUN curl https://example.invalid/x\n")
				.unwrap_err()
				.to_string()
				.contains("network")
		);
		assert!(
			validate_dockerfile("FROM scratch\nCOPY --from=alpine:latest /bin/tool /bin/tool\n")
				.unwrap_err()
				.to_string()
				.contains("must be pinned")
		);
		assert!(
			validate_dockerfile(&format!(
				"FROM scratch AS local\nCOPY --from=local /x /x\nCOPY \
				 --from=repo/tool@sha256:{digest} /y /y\n"
			))
			.is_ok()
		);
	}
	#[test]
	fn stale_build_cache_is_verified_before_reuse() {
		let (_temp, home) = home();
		let mut spec = registry();
		spec.uv_packages.push(pb::UvPackage {
			name:               "httpx".into(),
			version:            "0.27.0".into(),
			index_url_presence: None,
		});
		spec.environment.insert("MODE".into(), "stable".into());
		let first = FakeBackend::new(10);
		let first_result = realize_with(&first, &home, &spec, pb::CpuArchitecture::Amd64).unwrap();
		let replacement = FakeBackend::new(11);
		let replacement_result =
			realize_with(&replacement, &home, &spec, pb::CpuArchitecture::Amd64).unwrap();
		assert_ne!(
			first_result.resolved_spec.resolved_oci_digest,
			replacement_result.resolved_spec.resolved_oci_digest
		);
		assert_eq!(replacement.builds.load(Ordering::SeqCst), 1);
	}

	fn registry() -> pb::ImageSpec {
		pb::ImageSpec {
			source: Some(pb::image_spec::Source::Registry(pb::RegistryImageSource {
				reference: "registry.example/app:latest".into(),
			})),
			..Default::default()
		}
	}
	fn home() -> (tempfile::TempDir, Home) {
		let temp = tempfile::tempdir().unwrap();
		let home = Home::new(temp.path());
		(temp, home)
	}

	#[test]
	fn registry_and_python_are_digest_pinned_and_mutation_is_detected() {
		let (_temp, home) = home();
		let first = FakeBackend::new(1);
		let spec = registry();
		let one = realize_with(&first, &home, &spec, pb::CpuArchitecture::Amd64).unwrap();
		assert!(one.image.unwrap().contains("@sha256:"));
		let mut claimed = spec;
		claimed.resolved_oci_digest = Some(digest(1));
		let moved = FakeBackend::new(2);
		assert!(
			realize_with(&moved, &home, &claimed, pb::CpuArchitecture::Amd64)
				.unwrap_err()
				.to_string()
				.contains("does not match")
		);
		let python = pb::ImageSpec {
			source: Some(pb::image_spec::Source::Python(pb::PythonImageSource {
				python_version: "3.12.4".into(),
				variant:        "slim".into(),
			})),
			..Default::default()
		};
		assert!(
			realize_with(&first, &home, &python, pb::CpuArchitecture::Amd64)
				.unwrap()
				.image
				.unwrap()
				.starts_with(PYTHON_REPOSITORY)
		);
	}

	#[test]
	fn layered_inputs_are_stable_complete_and_cache_context() {
		let (_temp, home) = home();
		let store = ArtifactStore::open(home.function_artifacts_dir()).unwrap();
		let artifact = store.put(b"payload").unwrap();
		let mut spec = registry();
		spec
			.apt_packages
			.push(pb::AptPackage { name: "curl".into(), version: "8.1.2-1".into() });
		spec.uv_packages.push(pb::UvPackage {
			name:               "httpx".into(),
			version:            "0.27.0".into(),
			index_url_presence: None,
		});
		spec.commands.push(pb::ImageBuildCommand {
			argv: vec!["python".into(), "-m".into(), "compileall".into(), "/app".into()],
		});
		spec.environment.insert("MODE".into(), "prod".into());
		spec.local_artifact_mounts.push(pb::LocalArtifactMount {
			artifact:  Some(pb::ArtifactRef {
				digest: Some(pb::Digest {
					algorithm: pb::DigestAlgorithm::Sha256 as i32,
					value:     hex::decode(artifact.digest).unwrap(),
				}),
			}),
			path:      "/opt/model".into(),
			read_only: true,
		});
		spec.local_artifact_mounts.push(pb::LocalArtifactMount {
			artifact:  spec.local_artifact_mounts[0].artifact.clone(),
			path:      "/opt/model-copy".into(),
			read_only: true,
		});
		spec.uv_packages[0].index_url_presence =
			Some(pb::uv_package::IndexUrlPresence::IndexUrl("https://packages.example/simple".into()));
		let backend = FakeBackend::new(3);
		let one = realize_with(&backend, &home, &spec, pb::CpuArchitecture::Amd64).unwrap();
		let two = realize_with(&backend, &home, &spec, pb::CpuArchitecture::Amd64).unwrap();
		assert_eq!(one.provenance_digest, two.provenance_digest);
		assert_eq!(one.resolved_spec.environment.get("MODE").unwrap(), "prod");
		let contexts = fs::read_dir(home.images_dir().join("function-builds"))
			.unwrap()
			.count();
		assert_eq!(contexts, 1);
		assert_eq!(backend.builds.load(Ordering::SeqCst), 1);
		let context = fs::read_dir(home.images_dir().join("function-builds"))
			.unwrap()
			.next()
			.unwrap()
			.unwrap()
			.path()
			.join("context/.vmon.Dockerfile");
		let recipe = fs::read_to_string(context).unwrap();
		for expected in [
			"curl=8.1.2-1",
			"httpx==0.27.0",
			"python\",\"-m\",\"pip\",\"install",
			"https://packages.example/simple",
			"compileall",
			"MODE=\"prod\"",
			".vmon-artifacts/",
		] {
			assert!(recipe.contains(expected), "{expected} missing from recipe");
		}
		spec.environment.insert("MODE".into(), "dev".into());
		let changed = realize_with(&backend, &home, &spec, pb::CpuArchitecture::Amd64).unwrap();
		assert_ne!(one.provenance_digest, changed.provenance_digest);
	}

	fn tar(entries: &[(&str, u8, &[u8])]) -> Vec<u8> {
		let mut out = Vec::new();
		for (name, kind, data) in entries {
			let mut h = [0u8; 512];
			h[..name.len()].copy_from_slice(name.as_bytes());
			h[100..108].copy_from_slice(b"0000600\0");
			h[108..116].copy_from_slice(b"0000000\0");
			h[116..124].copy_from_slice(b"0000000\0");
			let size = format!("{:011o}\0", data.len());
			h[124..136].copy_from_slice(size.as_bytes());
			h[136..148].copy_from_slice(b"00000000000\0");
			h[148..156].fill(b' ');
			h[156] = *kind;
			h[257..263].copy_from_slice(b"ustar\0");
			let sum: u64 = h.iter().map(|b| u64::from(*b)).sum();
			let checksum = format!("{sum:06o}\0 ");
			h[148..156].copy_from_slice(checksum.as_bytes());
			out.extend(h);
			out.extend(*data);
			out.resize(out.len().div_ceil(512) * 512, 0);
		}
		out.extend([0u8; 1024]);
		out
	}

	#[test]
	fn dockerfile_context_is_verified_and_traversal_or_links_rejected() {
		let (_temp, home) = home();
		let store = ArtifactStore::open(home.function_artifacts_dir()).unwrap();
		let bytes = tar(&[("Dockerfile", b'0', b"FROM scratch\n")]);
		let stored = store.put(&bytes).unwrap();
		let context = pb::ArtifactRef {
			digest: Some(pb::Digest {
				algorithm: pb::DigestAlgorithm::Sha256 as i32,
				value:     hex::decode(stored.digest).unwrap(),
			}),
		};
		let spec = pb::ImageSpec {
			source: Some(pb::image_spec::Source::Dockerfile(pb::DockerfileImageSource {
				context:         Some(context),
				dockerfile_path: "Dockerfile".into(),
			})),
			..Default::default()
		};
		assert!(realize_with(&FakeBackend::new(4), &home, &spec, pb::CpuArchitecture::Amd64).is_ok());
		let bad = tar(&[("../escape", b'0', b"x")]);
		let bad = store.put(&bad).unwrap();
		let mut traversal = spec;
		if let Some(pb::image_spec::Source::Dockerfile(source)) = &mut traversal.source {
			source.context = Some(pb::ArtifactRef {
				digest: Some(pb::Digest {
					algorithm: pb::DigestAlgorithm::Sha256 as i32,
					value:     hex::decode(bad.digest).unwrap(),
				}),
			});
		}
		assert!(
			realize_with(&FakeBackend::new(4), &home, &traversal, pb::CpuArchitecture::Amd64).is_err()
		);
		let link = tar(&[("Dockerfile", b'2', b"")]);
		let link = store.put(&link).unwrap();
		if let Some(pb::image_spec::Source::Dockerfile(source)) = &mut traversal.source {
			source.context = Some(pb::ArtifactRef {
				digest: Some(pb::Digest {
					algorithm: pb::DigestAlgorithm::Sha256 as i32,
					value:     hex::decode(link.digest).unwrap(),
				}),
			});
		}
		assert!(
			realize_with(&FakeBackend::new(4), &home, &traversal, pb::CpuArchitecture::Amd64).is_err()
		);
	}

	#[test]
	fn template_arch_and_secret_rejections_are_actionable() {
		let (_temp, home) = home();
		let revision = hex::encode([5u8; 32]);
		let template = pb::ImageSpec {
			source: Some(pb::image_spec::Source::Template(pb::TemplateImageSource {
				name:     "warm".into(),
				revision: revision.clone(),
			})),
			..Default::default()
		};
		let architecture = if cfg!(target_arch = "aarch64") {
			pb::CpuArchitecture::Arm64
		} else {
			pb::CpuArchitecture::Amd64
		};
		let realized = realize_with(&FakeBackend::new(5), &home, &template, architecture).unwrap();
		assert_eq!(realized.template.as_deref(), Some(format!("warm@{revision}").as_str()));
		let wrong = FakeBackend {
			digest: hex::encode([1u8; 32]),
			arch:   "arm64",
			builds: AtomicUsize::new(0),
		};
		assert!(
			realize_with(&wrong, &home, &registry(), pb::CpuArchitecture::Amd64)
				.unwrap_err()
				.to_string()
				.contains("does not match")
		);
		let mut secret = registry();
		secret
			.environment
			.insert("API_TOKEN".into(), "not-logged".into());
		let error = realize_with(&FakeBackend::new(1), &home, &secret, pb::CpuArchitecture::Amd64)
			.unwrap_err()
			.to_string();
		assert!(!error.contains("not-logged"));
		assert!(error.contains("SecretRef"));
		assert!(!home.images_dir().exists(), "rejected secrets must not create a build context");
	}

	#[test]
	fn unpinned_and_unsupported_fields_are_never_ignored() {
		let (_temp, home) = home();
		let mut spec = registry();
		spec
			.apt_packages
			.push(pb::AptPackage { name: "curl".into(), version: "*".into() });
		assert!(
			realize_with(&FakeBackend::new(1), &home, &spec, pb::CpuArchitecture::Amd64).is_err()
		);
		spec.apt_packages.clear();
		spec
			.commands
			.push(pb::ImageBuildCommand { argv: vec!["sh".into(), "-c".into(), "echo x".into()] });
		assert!(
			realize_with(&FakeBackend::new(1), &home, &spec, pb::CpuArchitecture::Amd64).is_err()
		);
		spec.commands.clear();
		spec.local_artifact_mounts.push(pb::LocalArtifactMount {
			artifact:  Some(artifact_ref(9)),
			path:      "../bad".into(),
			read_only: false,
		});
		assert!(
			realize_with(&FakeBackend::new(1), &home, &spec, pb::CpuArchitecture::Amd64).is_err()
		);
	}
}
