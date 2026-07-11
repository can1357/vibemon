//! Pinned kernel download. Port of python/vmon/assets.py.

use std::{
	fs,
	path::{Path, PathBuf},
	process::Command,
};

use sha2::{Digest, Sha256};

use crate::error::{EngineError, Result};

const KERNELS: &[KernelPin] = &[
	KernelPin {
		arch:   "aarch64",
		name:   "Image-aarch64",
		url:    "https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.16.9-20260508/Image-arm64",
		sha256: "69d1b1235381ec50f1b45cf771a7dff4a9013d452833ab34682d6283e2114010",
	},
	KernelPin {
		arch:   "x86_64",
		name:   "bzImage-x86_64",
		url:    "https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.12.8-20250613/bzImage-x86_64",
		sha256: "d4af401aa859e4659d4b08a153ac608eb6a315c6918e567daa46981af5d2e5ef",
	},
];

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

struct KernelPin {
	arch:   &'static str,
	name:   &'static str,
	url:    &'static str,
	sha256: &'static str,
}

/// Return the auto-provisioned guest asset directory.
pub fn assets_dir() -> Result<PathBuf> {
	let dir = crate::home::state_dir().join("assets");
	fs::create_dir_all(&dir)?;
	Ok(dir)
}

/// Return a guest kernel for the host architecture, downloading it if needed.
pub fn ensure_kernel() -> Result<PathBuf> {
	let arch = host_arch();
	let pin = KERNELS.iter().find(|pin| pin.arch == arch).ok_or_else(|| {
		EngineError::engine(format!(
			"no pinned guest kernel for arch {arch:?}; set VMON_KERNEL=/path/to/Image-or-bzImage"
		))
	})?;
	let dest = assets_dir()?.join(pin.name);
	if dest.is_file() && sha256_file(&dest)? == pin.sha256 {
		return Ok(dest);
	}
	eprintln!("vmon: downloading guest kernel {} (one-time)...", pin.name);
	download_verified(pin.url, &dest, pin.sha256)?;
	Ok(dest)
}

/// Resolve the default guest kernel path.
pub fn default_kernel() -> Result<PathBuf> {
	if let Ok(env) = std::env::var("VMON_KERNEL")
		&& !env.is_empty()
	{
		return Ok(expand_home(&env));
	}
	if cfg!(target_os = "linux") {
		let release = fs::read_to_string("/proc/sys/kernel/osrelease")
			.map(|text| text.trim().to_owned())
			.unwrap_or_default();
		if !release.is_empty() {
			let kernel = PathBuf::from(format!("/boot/vmlinuz-{release}"));
			if kernel.is_file() {
				return Ok(kernel);
			}
		}
	}
	ensure_kernel()
}

fn normalize_arch(arch: &str) -> String {
	match arch.trim().to_ascii_lowercase().replace('-', "_").as_str() {
		"arm64" => "aarch64".to_owned(),
		"amd64" | "x64" => "x86_64".to_owned(),
		other => other.to_owned(),
	}
}

fn host_arch() -> String {
	normalize_arch(std::env::consts::ARCH)
}

fn sha256_file(path: &Path) -> Result<String> {
	let bytes = fs::read(path)?;
	let digest = Sha256::digest(bytes);
	Ok(hex::encode(digest))
}

fn download_verified(url: &str, dest: &Path, expected_sha256: &str) -> Result<()> {
	let tmp = dest.with_file_name(format!(
		"{}.part.{}",
		dest
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or("download"),
		std::process::id()
	));
	let result = (|| {
		// reqwest is compiled without its blocking client in this crate; using curl
		// matches the repository asset scripts while keeping the dependency set fixed.
		let curl = find_tool("curl").ok_or_else(|| {
			EngineError::unsupported(
				"curl not found; install curl or set VMON_KERNEL=/path/to/Image-or-bzImage",
			)
		})?;
		let status = Command::new(&curl)
			.args(["--fail", "--location", "--user-agent", "vmon", "--output"])
			.arg(&tmp)
			.arg(url)
			.status()
			.map_err(|e| {
				EngineError::engine(format!("failed to download guest kernel from {url}: {e}"))
			})?;
		if !status.success() {
			return Err(EngineError::engine(format!(
				"failed to download guest kernel from {url}: curl exited with {status}"
			)));
		}
		let got = sha256_file(&tmp)?;
		if got != expected_sha256 {
			return Err(EngineError::engine(format!(
				"checksum mismatch for {url}: expected {expected_sha256}, got {got}"
			)));
		}
		fs::rename(&tmp, dest)?;
		Ok(())
	})();
	let _ = fs::remove_file(&tmp);
	result
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

fn expand_home(value: &str) -> PathBuf {
	if let Some(rest) = value.strip_prefix("~/")
		&& let Some(home) = std::env::var_os("HOME")
	{
		return PathBuf::from(home).join(rest);
	}
	PathBuf::from(value)
}
