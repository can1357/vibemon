//! Environment diagnostics. Port of python/vmon/doctor.py.

use std::{
	env, fs,
	io::{Read, Write},
	net::Shutdown,
	os::unix::{fs::PermissionsExt, net::UnixStream},
	path::{Path, PathBuf},
	process::Command,
	time::Duration,
};

use serde::Serialize;

use crate::{
	config::{SERVE_CONFIG_KEYS, ServeConfig, cli_option_for, env_var_for, resolve_serve_config},
	home, image,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
	Ok,
	Warn,
	Fail,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Check {
	pub name:   String,
	pub status: Status,
	pub detail: String,
	pub hint:   String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ServeConfigRow {
	pub key:    String,
	pub value:  String,
	pub source: String,
	pub env:    String,
	pub flag:   String,
}

impl Check {
	pub fn ok(name: impl Into<String>, detail: impl Into<String>) -> Self {
		Self::new(name, Status::Ok, detail, "")
	}

	pub fn warn(
		name: impl Into<String>,
		detail: impl Into<String>,
		hint: impl Into<String>,
	) -> Self {
		Self::new(name, Status::Warn, detail, hint)
	}

	pub fn fail(
		name: impl Into<String>,
		detail: impl Into<String>,
		hint: impl Into<String>,
	) -> Self {
		Self::new(name, Status::Fail, detail, hint)
	}

	fn new(
		name: impl Into<String>,
		status: Status,
		detail: impl Into<String>,
		hint: impl Into<String>,
	) -> Self {
		Self { name: name.into(), status, detail: detail.into(), hint: hint.into() }
	}
}

pub fn collect_checks() -> Vec<Check> {
	let mut checks = Vec::new();
	let (binary_check, binary) = check_vmon_binary();
	checks.push(binary_check);
	if cfg!(target_os = "macos") {
		checks.push(check_codesign(binary.as_deref()));
	}
	checks.push(check_hypervisor());
	checks.push(check_image_tools());
	checks.push(check_mkfs());
	checks.push(check_guest_kernel());
	checks.push(check_bundled_agent());
	checks.push(check_daemon());
	checks.push(check_environment());
	checks
}

pub fn collect_serve_config_checks(config: &ServeConfig) -> Vec<Check> {
	let mut checks = Vec::new();
	if config
		.token
		.as_deref()
		.is_some_and(|token| !token.trim().is_empty())
	{
		checks.push(Check::ok("serve token", "operator bearer token configured"));
	} else if is_loopback_host(&config.host) {
		checks.push(Check::warn(
			"serve token",
			"missing operator bearer token; loopback TCP and UDS are local-only",
			"set VMON_API_TOKEN before binding a non-loopback host",
		));
	} else {
		checks.push(Check::fail(
			"serve token",
			"missing operator bearer token for a non-loopback bind",
			"pass --token, set VMON_API_TOKEN, or set token in the config file",
		));
	}
	if config.tls_cert.is_some() == config.tls_key.is_some() {
		let detail = if config.tls_cert.is_some() {
			"certificate and key configured"
		} else {
			"disabled"
		};
		checks.push(Check::ok("serve TLS", detail));
	} else {
		checks.push(Check::fail(
			"serve TLS",
			"TLS cert/key must be configured together",
			"set both tls_cert and tls_key, or neither",
		));
	}
	if config.replicas > 0 && config.replicate_sec == Some(0.0) {
		checks.push(Check::fail(
			"replication cadence",
			"replicas > 0 but replicate_sec is 0",
			"set replicate_sec to a positive value, leave it unset for auto, or set replicas=0",
		));
	} else {
		let cadence = config
			.replicate_sec
			.map_or_else(|| "auto (60s when mesh is enabled)".to_owned(), |secs| format!("{secs}s"));
		checks.push(Check::ok(
			"replication cadence",
			format!("cadence={cadence}; K={}", config.replicas),
		));
	}
	checks.push(check_advertise(config));
	checks.push(check_restore_quorum(config));
	let bad_refs = config
		.warm_images
		.iter()
		.filter(|item| {
			item.reference.trim().is_empty() || item.reference.chars().any(char::is_whitespace)
		})
		.map(|item| item.reference.clone())
		.collect::<Vec<_>>();
	if bad_refs.is_empty() {
		checks.push(Check::ok("warm images", format!("{} configured", config.warm_images.len())));
	} else {
		checks.push(Check::fail(
			"warm images",
			format!("invalid image ref(s): {}", bad_refs.join(", ")),
			"use OCI refs without whitespace, e.g. alpine:latest=2",
		));
	}
	checks
}

pub fn serve_config_rows(config: &ServeConfig) -> Vec<ServeConfigRow> {
	SERVE_CONFIG_KEYS
		.iter()
		.map(|key| ServeConfigRow {
			key:    (*key).to_owned(),
			value:  serve_value(key, config),
			source: config
				.source(key)
				.map_or_else(|| "default".to_owned(), |source| source.as_str().to_owned()),
			env:    env_var_for(key).unwrap_or("-").to_owned(),
			flag:   cli_option_for(key).unwrap_or("-").to_owned(),
		})
		.collect()
}

pub fn collect_serve_doctor<S>(
	config_overrides: &std::collections::HashMap<String, String, S>,
) -> (Vec<ServeConfigRow>, Vec<Check>)
where
	S: std::hash::BuildHasher,
{
	let config_overrides = config_overrides
		.iter()
		.map(|(key, value)| (key.clone(), value.clone()))
		.collect::<std::collections::HashMap<_, _>>();
	match resolve_serve_config(&config_overrides) {
		Ok(config) => (serve_config_rows(&config), collect_serve_config_checks(&config)),
		Err(err) => (Vec::new(), vec![Check::fail(
			"serve config",
			err.to_string(),
			"fix the serve configuration",
		)]),
	}
}

fn check_vmon_binary() -> (Check, Option<PathBuf>) {
	match find_vmon_binary() {
		Some(path) => (Check::ok("vmm binary", path.display().to_string()), Some(path)),
		None => (
			Check::fail(
				"vmm binary",
				"not found",
				"run `cargo build` from the repository root or set VMON_BIN=/path/to/vmon",
			),
			None,
		),
	}
}

fn check_codesign(binary: Option<&Path>) -> Check {
	let Some(binary) = binary else {
		return Check::fail(
			"codesign entitlement",
			"cannot inspect entitlements because the vmm binary is missing",
			"run `cargo build`, then `just codesign` or `just build`",
		);
	};
	let output = Command::new("codesign")
		.args(["-d", "--entitlements", "-"])
		.arg(binary)
		.output();
	let Ok(output) = output else {
		return Check::fail(
			"codesign entitlement",
			"codesign failed",
			"run `just build` or `just codesign` to grant the Hypervisor entitlement",
		);
	};
	let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
	text.push_str(&String::from_utf8_lossy(&output.stderr));
	if text.contains("com.apple.security.hypervisor") {
		Check::ok("codesign entitlement", "Hypervisor entitlement present")
	} else {
		let detail = if output.status.success() {
			"Hypervisor entitlement missing".to_owned()
		} else {
			format!(
				"codesign exited {}; Hypervisor entitlement missing",
				output.status.code().unwrap_or(-1)
			)
		};
		Check::fail(
			"codesign entitlement",
			detail,
			"run `just build` or `just codesign` to grant the Hypervisor entitlement",
		)
	}
}

fn check_hypervisor() -> Check {
	if cfg!(target_os = "macos") {
		let output = Command::new("sysctl")
			.args(["-n", "kern.hv_support"])
			.output();
		return match output {
			Ok(output) if String::from_utf8_lossy(&output.stdout).trim() == "1" => {
				Check::ok("hypervisor", "kern.hv_support=1")
			},
			Ok(output) => {
				let detail = String::from_utf8_lossy(&output.stderr).trim().to_owned();
				Check::fail(
					"hypervisor",
					if detail.is_empty() {
						"kern.hv_support is not 1".to_owned()
					} else {
						detail
					},
					"run on Apple hardware with Hypervisor.framework support enabled",
				)
			},
			Err(err) => Check::fail(
				"hypervisor",
				format!("could not query kern.hv_support: {err}"),
				"run on Apple hardware with Hypervisor.framework support enabled",
			),
		};
	}
	if cfg!(target_os = "linux") {
		let kvm = Path::new("/dev/kvm");
		return if kvm.exists() && readable_writable(kvm) {
			Check::ok("hypervisor", "/dev/kvm is present and writable")
		} else {
			Check::fail(
				"hypervisor",
				"/dev/kvm is missing or not writable",
				"enable KVM and add your user to the kvm group, then re-login",
			)
		};
	}
	Check::warn(
		"hypervisor",
		format!("host platform {} is not checked", env::consts::OS),
		"vmon expects macOS HVF or Linux KVM for local microVMs",
	)
}

fn check_image_tools() -> Check {
	match image::detect_image_tools() {
		Ok(tools) => Check::ok(
			"image tools",
			format!("skopeo at {}; umoci at {}", tools.skopeo.display(), tools.umoci.display()),
		),
		Err(err) => Check::warn(
			"image tools",
			err.to_string(),
			"install skopeo and umoci before using `vmon run` with image references",
		),
	}
}

fn check_mkfs() -> Check {
	if let Some(path) = find_tool("mkfs.ext4") {
		return Check::ok("mkfs.ext4", path.display().to_string());
	}
	let homebrew = Path::new("/opt/homebrew/opt/e2fsprogs/sbin/mkfs.ext4");
	if cfg!(target_os = "macos") && homebrew.is_file() {
		return Check::ok("mkfs.ext4", homebrew.display().to_string());
	}
	let hint = if cfg!(target_os = "macos") {
		"brew install e2fsprogs"
	} else {
		"install e2fsprogs"
	};
	Check::warn("mkfs.ext4", "mkfs.ext4 not found", hint)
}

fn check_guest_kernel() -> Check {
	if let Ok(kernel) = env::var("VMON_KERNEL") {
		let path = expand_home(&kernel);
		return if path.exists() {
			Check::ok("guest kernel", format!("VMON_KERNEL={}", path.display()))
		} else {
			Check::warn(
				"guest kernel",
				format!("configured kernel path does not exist: {}", path.display()),
				"set VMON_KERNEL to a bootable guest kernel or let vmon auto-provision one",
			)
		};
	}
	if let Some(path) = cached_kernel() {
		return Check::ok("guest kernel", format!("cached kernel at {}", path.display()));
	}
	Check::warn(
		"guest kernel",
		"not available yet",
		"first boot auto-downloads a pinned kernel on macOS; otherwise set VMON_KERNEL",
	)
}

fn check_bundled_agent() -> Check {
	match image::find_agent_binary(Some(&arch())) {
		Ok(path) => Check::ok("bundled agent", path.display().to_string()),
		Err(err) => Check::warn(
			"bundled agent",
			err.to_string(),
			"run `just agent-musl` to build the static guest agent",
		),
	}
}

fn check_daemon() -> Check {
	let sock = home::state_dir().join("vmond.sock");
	if !sock.exists() {
		return Check::warn(
			"daemon",
			format!("vmond socket not present at {}", sock.display()),
			"run `vmon serve`",
		);
	}
	match probe_healthz(&sock) {
		Ok(detail) => Check::ok("daemon", detail),
		Err(err) => Check::warn(
			"daemon",
			format!("socket is present but not responsive: {err}"),
			"run `vmon serve`",
		),
	}
}

fn check_environment() -> Check {
	Check::ok("environment", format!("Rust server on {} {}", env::consts::OS, arch()))
}

fn check_advertise(config: &ServeConfig) -> Check {
	if config.host.trim().is_empty() {
		return Check::fail(
			"advertise URL",
			"host is empty",
			"set a concrete host/port or configure mesh setup with an explicit advertise URL",
		);
	}
	Check::ok("advertise URL", format!("http://{}:{}", config.host, config.port))
}

fn check_restore_quorum(config: &ServeConfig) -> Check {
	let expected_members = expected_members_from_home(&config.home);
	let quorum_on = config.restore_quorum_enabled(expected_members);
	if expected_members == 2 {
		Check::warn(
			"restore quorum",
			"2 expected members cannot form a post-failure majority; quorum restore is off by default",
			"use at least 3 expected members for quorum-gated restore",
		)
	} else if quorum_on && expected_members < 3 {
		Check::warn(
			"restore quorum",
			format!("forced on with only {expected_members} expected member(s)"),
			"use at least 3 expected members for quorum-gated restore",
		)
	} else {
		let state = if quorum_on { "on" } else { "off" };
		Check::ok("restore quorum", format!("{state}; expected_members={expected_members}"))
	}
}

fn serve_value(key: &str, config: &ServeConfig) -> String {
	match key {
		"home" => config.home.display().to_string(),
		"host" => config.host.clone(),
		"port" => config.port.to_string(),
		"token" => secret_value(config.token.as_deref()),
		"client_token" => secret_value(config.client_token.as_deref()),
		"tls_cert" => config
			.tls_cert
			.clone()
			.unwrap_or_else(|| "unset".to_owned()),
		"tls_key" => config.tls_key.clone().unwrap_or_else(|| "unset".to_owned()),
		"idle_timeout" => config.idle_timeout.to_string(),
		"replicate_sec" => config
			.replicate_sec
			.map_or_else(|| "auto".to_owned(), |value| value.to_string()),
		"replicas" => config.replicas.to_string(),
		"replicate_concurrency" => config.replicate_concurrency.to_string(),
		"restore_quorum" => config
			.restore_quorum
			.map_or_else(|| "auto".to_owned(), |value| value.to_string()),
		"warm_pool_size" => config.warm_pool_size.to_string(),
		"warm_images" => {
			if config.warm_images.is_empty() {
				"[]".to_owned()
			} else {
				config
					.warm_images
					.iter()
					.map(|item| format!("{}={}", item.reference, item.count))
					.collect::<Vec<_>>()
					.join(",")
			}
		},
		"mesh_heartbeat_sec" => config.mesh_heartbeat_sec.to_string(),
		"mesh_reap_sec" => config.mesh_reap_sec.to_string(),
		"mesh_idem_ttl_sec" => config.mesh_idem_ttl_sec.to_string(),
		"mesh_create_timeout_sec" => config.mesh_create_timeout_sec.to_string(),
		"mesh_w_warm" => config.mesh_w_warm.to_string(),
		"mesh_w_free" => config.mesh_w_free.to_string(),
		"mesh_w_local" => config.mesh_w_local.to_string(),
		"mesh_w_region" => config.mesh_w_region.to_string(),
		"mesh_w_inflight" => config.mesh_w_inflight.to_string(),
		_ => String::new(),
	}
}

fn secret_value(value: Option<&str>) -> String {
	if value.is_some_and(|value| !value.is_empty()) {
		"<set>".to_owned()
	} else {
		"unset".to_owned()
	}
}

fn expected_members_from_home(home: &Path) -> usize {
	let path = home.join("mesh.json");
	let Ok(text) = fs::read_to_string(path) else {
		return 1;
	};
	let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
		return 1;
	};
	value
		.get("expected_members")
		.and_then(serde_json::Value::as_u64)
		.and_then(|value| usize::try_from(value).ok())
		.unwrap_or(1)
		.max(1)
}

fn find_vmon_binary() -> Option<PathBuf> {
	if let Some(path) = env::var_os("VMON_BIN").map(PathBuf::from) {
		return executable_file(&path).then_some(path);
	}
	let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
		.parent()
		.map_or_else(|| PathBuf::from("."), Path::to_path_buf);
	let arch = arch();
	let triples = [
		String::new(),
		format!("{arch}-unknown-linux-gnu"),
		format!("{arch}-unknown-linux-musl"),
		format!("{arch}-apple-darwin"),
	];
	let mut roots = vec![repo.join("target")];
	if let Some(target_dir) = env::var_os("CARGO_TARGET_DIR") {
		roots.push(PathBuf::from(target_dir));
	}
	let mut best: Option<(std::time::SystemTime, u8, PathBuf)> = None;
	for root in roots {
		for triple in &triples {
			for (rank, profile) in ["debug", "release"].iter().enumerate() {
				let candidate = if triple.is_empty() {
					root.join(profile).join("vmon")
				} else {
					root.join(triple).join(profile).join("vmon")
				};
				if !executable_file(&candidate) {
					continue;
				}
				let modified = fs::metadata(&candidate)
					.and_then(|metadata| metadata.modified())
					.unwrap_or(std::time::SystemTime::UNIX_EPOCH);
				let key = (modified, rank as u8, candidate);
				if best.as_ref().is_none_or(|current| key > *current) {
					best = Some(key);
				}
			}
		}
	}
	best.map(|(_, _, path)| path).or_else(|| find_tool("vmon"))
}

fn cached_kernel() -> Option<PathBuf> {
	let assets = home::state_dir().join("assets");
	let entries = fs::read_dir(assets).ok()?;
	entries
		.filter_map(Result::ok)
		.map(|entry| entry.path())
		.find(|path| {
			path.is_file()
				&& path
					.file_name()
					.and_then(|name| name.to_str())
					.is_some_and(|name| {
						name.starts_with("Image")
							|| name.starts_with("bzImage")
							|| name.starts_with("vmlinuz")
					})
		})
}

fn probe_healthz(sock: &Path) -> std::io::Result<String> {
	let mut stream = UnixStream::connect(sock)?;
	stream.set_read_timeout(Some(Duration::from_secs(2)))?;
	stream.set_write_timeout(Some(Duration::from_secs(2)))?;
	stream.write_all(b"GET /healthz HTTP/1.1\r\nHost: vmon\r\nConnection: close\r\n\r\n")?;
	let _ = stream.shutdown(Shutdown::Write);
	let mut response = String::new();
	stream.read_to_string(&mut response)?;
	if response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200") {
		Ok("responsive".to_owned())
	} else {
		Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "healthz did not return 200"))
	}
}

fn arch() -> String {
	match env::consts::ARCH.replace('-', "_").as_str() {
		"arm64" => "aarch64".to_owned(),
		"amd64" | "x64" => "x86_64".to_owned(),
		other => other.to_owned(),
	}
}

fn find_tool(name: &str) -> Option<PathBuf> {
	let path = env::var_os("PATH")?;
	env::split_paths(&path)
		.map(|dir| dir.join(name))
		.find(|path| executable_file(path))
}

fn executable_file(path: impl AsRef<Path>) -> bool {
	let path = path.as_ref();
	fs::metadata(path)
		.is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

fn readable_writable(path: &Path) -> bool {
	fs::OpenOptions::new()
		.read(true)
		.write(true)
		.open(path)
		.is_ok()
}

fn expand_home(value: &str) -> PathBuf {
	if let Some(rest) = value.strip_prefix("~/")
		&& let Some(home) = env::var_os("HOME")
	{
		return PathBuf::from(home).join(rest);
	}
	PathBuf::from(value)
}

fn is_loopback_host(host: &str) -> bool {
	let host = host.trim();
	if host.eq_ignore_ascii_case("localhost") {
		return true;
	}
	host
		.parse::<std::net::IpAddr>()
		.is_ok_and(|addr| addr.is_loopback())
}
