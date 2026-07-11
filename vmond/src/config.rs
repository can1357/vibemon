//! `ServeConfig`: TOML file / env / CLI precedence. Port of
//! python/vmon/config.py.

use std::{
	collections::{BTreeSet, HashMap},
	env, fs,
	path::PathBuf,
};

use crate::{EngineError, Result};

/// Configuration sources in increasing precedence order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigSource {
	/// Built-in dataclass default.
	Default,
	/// TOML configuration file.
	File,
	/// `VMON_*` environment variable.
	Env,
	/// Explicit CLI flag.
	Flag,
}

impl ConfigSource {
	/// Return the Python `_sources` spelling.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Default => "default",
			Self::File => "file",
			Self::Env => "env",
			Self::Flag => "flag",
		}
	}
}

/// One image reference to prewarm and its requested pool size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmImage {
	/// Image, template, or Dockerfile reference.
	pub reference: String,
	/// Requested ready-instance count.
	pub count:     usize,
}

/// Resolved configuration consumed by `vmon serve` and doctor checks.
#[derive(Clone, Debug, PartialEq)]
pub struct ServeConfig {
	/// Vibemon home directory.
	pub home: PathBuf,
	/// TCP bind host.
	pub host: String,
	/// TCP bind port.
	pub port: u16,
	/// Admin bearer token.
	pub token: Option<String>,
	/// Restricted client bearer token.
	pub client_token: Option<String>,
	/// Maximum accepted size of one streamed function artifact.
	pub function_artifact_max_bytes: u64,
	/// TLS certificate path.
	pub tls_cert: Option<String>,
	/// TLS private-key path.
	pub tls_key: Option<String>,
	/// Idle sandbox timeout in seconds.
	pub idle_timeout: f64,
	/// Replica checkpoint cadence override.
	pub replicate_sec: Option<f64>,
	/// Desired async replica count.
	pub replicas: usize,
	/// Concurrent replica transfer limit.
	pub replicate_concurrency: usize,
	/// Restore-quorum override.
	pub restore_quorum: Option<bool>,
	/// Default warm-pool size.
	pub warm_pool_size: usize,
	/// Images to prewarm at startup.
	pub warm_images: Vec<WarmImage>,
	/// Mesh heartbeat cadence in seconds.
	pub mesh_heartbeat_sec: f64,
	/// Mesh reap age in seconds.
	pub mesh_reap_sec: f64,
	/// Mesh idempotency cache TTL in seconds.
	pub mesh_idem_ttl_sec: f64,
	/// Mesh create dispatch timeout in seconds.
	pub mesh_create_timeout_sec: f64,
	/// Placement weight for warm capacity.
	pub mesh_w_warm: f64,
	/// Placement weight for free capacity.
	pub mesh_w_free: f64,
	/// Placement weight for local node bias.
	pub mesh_w_local: f64,
	/// Placement weight for region affinity.
	pub mesh_w_region: f64,
	/// Placement penalty for inflight creates.
	pub mesh_w_inflight: f64,
	sources: HashMap<String, ConfigSource>,
}

impl ServeConfig {
	/// Return the source that supplied a field's final value.
	pub fn source(&self, key: &str) -> Option<ConfigSource> {
		if !SERVE_CONFIG_KEYS.contains(&key) {
			return None;
		}
		Some(
			self
				.sources
				.get(key)
				.copied()
				.unwrap_or(ConfigSource::Default),
		)
	}

	/// Return the actual checkpoint cadence for the current mesh state.
	pub fn effective_replicate_sec(&self, mesh_enabled: bool) -> f64 {
		self
			.replicate_sec
			.unwrap_or(if mesh_enabled { 60.0 } else { 0.0 })
	}

	/// Return whether automatic restore should require majority confirmation.
	pub fn restore_quorum_enabled(&self, expected_members: usize) -> bool {
		self.restore_quorum.unwrap_or(expected_members >= 3)
	}

	/// Return the default per-sandbox HA tier for a create request.
	pub const fn default_ha(&self, mesh_enabled: bool) -> &'static str {
		if mesh_enabled { "async" } else { "off" }
	}

	/// Return placement scoring weights in mesh names.
	pub fn placement_weights(&self) -> HashMap<&'static str, f64> {
		HashMap::from([
			("warm", self.mesh_w_warm),
			("free", self.mesh_w_free),
			("local", self.mesh_w_local),
			("region", self.mesh_w_region),
			("inflight", self.mesh_w_inflight),
		])
	}

	fn set_source(&mut self, key: &str, source: ConfigSource) {
		self.sources.insert(key.to_owned(), source);
	}
}

impl Default for ServeConfig {
	fn default() -> Self {
		let sources = SERVE_CONFIG_KEYS
			.iter()
			.map(|key| ((*key).to_owned(), ConfigSource::Default))
			.collect();
		Self {
			home: default_home(),
			host: "127.0.0.1".to_owned(),
			port: 8000,
			token: None,
			client_token: None,
			function_artifact_max_bytes: 4 * 1024 * 1024 * 1024,
			tls_cert: None,
			tls_key: None,
			idle_timeout: 300.0,
			replicate_sec: None,
			replicas: 1,
			replicate_concurrency: 2,
			restore_quorum: None,
			warm_pool_size: 1,
			warm_images: Vec::new(),
			mesh_heartbeat_sec: 3.0,
			mesh_reap_sec: 300.0,
			mesh_idem_ttl_sec: 900.0,
			mesh_create_timeout_sec: 120.0,
			mesh_w_warm: 1000.0,
			mesh_w_free: 100.0,
			mesh_w_local: 50.0,
			mesh_w_region: 30.0,
			mesh_w_inflight: 80.0,
			sources,
		}
	}
}

/// Field names in Python dataclass order.
pub const SERVE_CONFIG_KEYS: &[&str] = &[
	"home",
	"host",
	"port",
	"token",
	"client_token",
	"function_artifact_max_bytes",
	"tls_cert",
	"tls_key",
	"idle_timeout",
	"replicate_sec",
	"replicas",
	"replicate_concurrency",
	"restore_quorum",
	"warm_pool_size",
	"warm_images",
	"mesh_heartbeat_sec",
	"mesh_reap_sec",
	"mesh_idem_ttl_sec",
	"mesh_create_timeout_sec",
	"mesh_w_warm",
	"mesh_w_free",
	"mesh_w_local",
	"mesh_w_region",
	"mesh_w_inflight",
];

/// Environment variable names for serve config fields.
pub const ENV_KEYS: &[(&str, &str)] = &[
	("home", "VMON_HOME"),
	("host", "VMON_SERVE_HOST"),
	("port", "VMON_SERVE_PORT"),
	("token", "VMON_API_TOKEN"),
	("client_token", "VMON_CLIENT_TOKEN"),
	("function_artifact_max_bytes", "VMON_FUNCTION_ARTIFACT_MAX_BYTES"),
	("tls_cert", "VMON_TLS_CERT"),
	("tls_key", "VMON_TLS_KEY"),
	("idle_timeout", "VMON_IDLE_TIMEOUT"),
	("replicate_sec", "VMON_REPLICATE_SEC"),
	("replicas", "VMON_REPLICAS"),
	("replicate_concurrency", "VMON_REPLICATE_CONCURRENCY"),
	("restore_quorum", "VMON_RESTORE_QUORUM"),
	("warm_pool_size", "VMON_WARM_POOL_SIZE"),
	("warm_images", "VMON_WARM_IMAGES"),
	("mesh_heartbeat_sec", "VMON_MESH_HEARTBEAT_SEC"),
	("mesh_reap_sec", "VMON_MESH_REAP_SEC"),
	("mesh_idem_ttl_sec", "VMON_MESH_IDEM_TTL_SEC"),
	("mesh_create_timeout_sec", "VMON_MESH_CREATE_TIMEOUT_SEC"),
	("mesh_w_warm", "VMON_MESH_W_WARM"),
	("mesh_w_free", "VMON_MESH_W_FREE"),
	("mesh_w_local", "VMON_MESH_W_LOCAL"),
	("mesh_w_region", "VMON_MESH_W_REGION"),
	("mesh_w_inflight", "VMON_MESH_W_INFLIGHT"),
];

/// CLI option spellings for serve config fields.
pub const CLI_OPTIONS: &[(&str, &str)] = &[
	("home", "--home"),
	("host", "--host"),
	("port", "--port"),
	("token", "--token"),
	("client_token", "--client-token"),
	("function_artifact_max_bytes", "--function-artifact-max-bytes"),
	("tls_cert", "--tls-cert"),
	("tls_key", "--tls-key"),
	("idle_timeout", "--idle-timeout"),
	("replicate_sec", "--replicate-sec"),
	("replicas", "--replicas"),
	("replicate_concurrency", "--replicate-concurrency"),
	("restore_quorum", "--restore-quorum/--no-restore-quorum"),
	("warm_pool_size", "--warm-pool-size"),
	("warm_images", "--warm-images"),
	("mesh_heartbeat_sec", "--mesh-heartbeat-sec"),
	("mesh_reap_sec", "--mesh-reap-sec"),
	("mesh_idem_ttl_sec", "--mesh-idem-ttl-sec"),
	("mesh_create_timeout_sec", "--mesh-create-timeout-sec"),
	("mesh_w_warm", "--mesh-w-warm"),
	("mesh_w_free", "--mesh-w-free"),
	("mesh_w_local", "--mesh-w-local"),
	("mesh_w_region", "--mesh-w-region"),
	("mesh_w_inflight", "--mesh-w-inflight"),
];

const CONFIG_PATH_KEYS: &[&str] = &["config", "config_path"];
const BOOL_TRUE: &[&str] = &["1", "true", "yes", "on"];
const BOOL_FALSE: &[&str] = &["0", "false", "no", "off"];

/// Resolve `vmon serve` configuration from defaults, file, env, then flags.
#[allow(
	clippy::implicit_hasher,
	reason = "The migration contract fixes this public entry point to HashMap<String, String>."
)]
pub fn resolve_serve_config(cli_overrides: &HashMap<String, String>) -> Result<ServeConfig> {
	let overrides = clean_cli_overrides(cli_overrides)?;
	let config_path = resolve_config_path(&overrides);
	let file_values = match config_path {
		Some(path) => read_config_file(&path)?,
		None => HashMap::new(),
	};
	let env_values = env_values()?;
	let mut config = ServeConfig::default();
	overlay_toml(&mut config, &file_values, ConfigSource::File)?;
	overlay_strings(&mut config, &env_values, ConfigSource::Env)?;
	overlay_strings(&mut config, &overrides, ConfigSource::Flag)?;
	Ok(config)
}

/// Return the environment variable name for a config field.
pub fn env_var_for(key: &str) -> Option<&'static str> {
	ENV_KEYS
		.iter()
		.find_map(|(field, env)| (*field == key).then_some(*env))
}

/// Return the CLI option spelling for a config field.
pub fn cli_option_for(key: &str) -> Option<&'static str> {
	CLI_OPTIONS
		.iter()
		.find_map(|(field, option)| (*field == key).then_some(*option))
}

/// Verify field metadata covers every serve config key.
pub fn validate_knob_inventory() -> Result<()> {
	let fields = SERVE_CONFIG_KEYS.iter().copied().collect::<BTreeSet<_>>();
	let env = ENV_KEYS
		.iter()
		.map(|(key, _)| *key)
		.collect::<BTreeSet<_>>();
	let cli = CLI_OPTIONS
		.iter()
		.map(|(key, _)| *key)
		.collect::<BTreeSet<_>>();
	let missing_env = fields.difference(&env).copied().collect::<Vec<_>>();
	let missing_cli = fields.difference(&cli).copied().collect::<Vec<_>>();
	if missing_env.is_empty() && missing_cli.is_empty() {
		return Ok(());
	}
	Err(EngineError::invalid(format!(
		"incomplete serve config metadata: missing env={missing_env:?}, cli={missing_cli:?}"
	)))
}

fn default_home() -> PathBuf {
	env::var_os("HOME")
		.map_or_else(|| PathBuf::from(".vmon"), |home| PathBuf::from(home).join(".vmon"))
}

fn clean_cli_overrides(overrides: &HashMap<String, String>) -> Result<HashMap<String, String>> {
	let unknown = overrides
		.keys()
		.filter(|key| {
			let key = key.as_str();
			!SERVE_CONFIG_KEYS.contains(&key) && !CONFIG_PATH_KEYS.contains(&key)
		})
		.cloned()
		.collect::<BTreeSet<_>>();
	if !unknown.is_empty() {
		let joined = unknown.into_iter().collect::<Vec<_>>().join(", ");
		return Err(EngineError::invalid(format!("unknown CLI override key(s): {joined}")));
	}
	let mut cleaned = HashMap::new();
	for (key, value) in overrides {
		if CONFIG_PATH_KEYS.contains(&key.as_str()) {
			if !value.is_empty() {
				cleaned.insert(key.clone(), value.clone());
			}
			continue;
		}
		if key == "warm_images" && value.is_empty() {
			continue;
		}
		cleaned.insert(key.clone(), value.clone());
	}
	Ok(cleaned)
}

fn resolve_config_path(overrides: &HashMap<String, String>) -> Option<PathBuf> {
	let raw = overrides
		.get("config")
		.or_else(|| overrides.get("config_path"))
		.cloned()
		.or_else(|| env::var("VMON_CONFIG").ok())?;
	let trimmed = raw.trim();
	if trimmed.is_empty() {
		None
	} else {
		Some(expand_user(trimmed))
	}
}

fn read_config_file(path: &PathBuf) -> Result<HashMap<String, toml::Value>> {
	let text = fs::read_to_string(path).map_err(|err| {
		EngineError::invalid(format!("could not read config file {}: {err}", path.display()))
	})?;
	let root = text.parse::<toml::Value>().map_err(|err| {
		EngineError::invalid(format!("could not parse config file {}: {err}", path.display()))
	})?;
	let table = root.as_table().ok_or_else(|| {
		EngineError::invalid(format!("config file {} must contain a TOML table", path.display()))
	})?;
	let data = if let Some(serve) = table.get("serve") {
		let extra = table
			.keys()
			.filter(|key| key.as_str() != "serve")
			.cloned()
			.collect::<BTreeSet<_>>();
		if !extra.is_empty() {
			let joined = extra.into_iter().collect::<Vec<_>>().join(", ");
			return Err(EngineError::invalid(format!("unknown top-level config key(s): {joined}")));
		}
		serve
			.as_table()
			.ok_or_else(|| EngineError::invalid("config key 'serve' must be a TOML table"))?
	} else {
		table
	};
	let unknown = data
		.keys()
		.filter(|key| !SERVE_CONFIG_KEYS.contains(&key.as_str()))
		.cloned()
		.collect::<BTreeSet<_>>();
	if !unknown.is_empty() {
		let keys = unknown.into_iter().collect::<Vec<_>>().join(", ");
		return Err(EngineError::invalid(format!("unknown serve config key(s): {keys}")));
	}
	Ok(data
		.iter()
		.map(|(key, value)| (key.clone(), value.clone()))
		.collect())
}

fn env_values() -> Result<HashMap<String, String>> {
	let mut values = HashMap::new();
	for (key, env_key) in ENV_KEYS {
		match env::var(env_key) {
			Ok(value) => {
				values.insert((*key).to_owned(), value);
			},
			Err(env::VarError::NotPresent) => {},
			Err(env::VarError::NotUnicode(_)) => {
				return Err(EngineError::invalid(format!(
					"environment variable {env_key} must be UTF-8"
				)));
			},
		}
	}
	Ok(values)
}

fn overlay_toml(
	config: &mut ServeConfig,
	incoming: &HashMap<String, toml::Value>,
	source: ConfigSource,
) -> Result<()> {
	for key in SERVE_CONFIG_KEYS {
		if let Some(value) = incoming.get(*key) {
			apply_value(config, key, RawValue::Toml(value), source)?;
		}
	}
	Ok(())
}

fn overlay_strings(
	config: &mut ServeConfig,
	incoming: &HashMap<String, String>,
	source: ConfigSource,
) -> Result<()> {
	for key in SERVE_CONFIG_KEYS {
		if let Some(value) = incoming.get(*key) {
			apply_value(config, key, RawValue::String(value), source)?;
		}
	}
	Ok(())
}

fn apply_value(
	config: &mut ServeConfig,
	key: &str,
	value: RawValue<'_>,
	source: ConfigSource,
) -> Result<()> {
	match key {
		"home" => config.home = expand_user(&coerce_string(key, value)?),
		"token" => config.token = empty_string_as_none(key, value)?,
		"client_token" => config.client_token = empty_string_as_none(key, value)?,
		"function_artifact_max_bytes" => {
			const MAX_ARTIFACT_BYTES: i64 = 1_i64 << 50;
			let parsed = coerce_int(key, value)?;
			if !(1..=MAX_ARTIFACT_BYTES).contains(&parsed) {
				return Err(EngineError::invalid(format!(
					"{key} must be between 1 and {MAX_ARTIFACT_BYTES}"
				)));
			}
			config.function_artifact_max_bytes = parsed as u64;
		},
		"tls_cert" => config.tls_cert = empty_string_as_none(key, value)?,
		"tls_key" => config.tls_key = empty_string_as_none(key, value)?,
		"host" => {
			let host = coerce_string(key, value)?.trim().to_owned();
			if host.is_empty() {
				return Err(EngineError::invalid("host must not be empty"));
			}
			config.host = host;
		},
		"port" => {
			let port = coerce_int(key, value)?;
			if !(0..=65535).contains(&port) {
				return Err(EngineError::invalid("port must be between 0 and 65535"));
			}
			config.port = u16::try_from(port)
				.map_err(|_| EngineError::invalid("port must be between 0 and 65535"))?;
		},
		"replicas" => config.replicas = non_negative_usize(key, value)?,
		"warm_pool_size" => config.warm_pool_size = non_negative_usize(key, value)?,
		"replicate_concurrency" => {
			let parsed = coerce_int(key, value)?;
			if parsed <= 0 {
				return Err(EngineError::invalid("replicate_concurrency must be positive"));
			}
			config.replicate_concurrency = usize::try_from(parsed)
				.map_err(|_| EngineError::invalid("replicate_concurrency must be positive"))?;
		},
		"restore_quorum" => config.restore_quorum = Some(coerce_bool(key, value)?),
		"warm_images" => {
			config.warm_images = coerce_warm_images(value, config.warm_pool_size)?;
		},
		"idle_timeout" => config.idle_timeout = positive_float(key, value)?,
		"replicate_sec" => {
			let parsed = coerce_float(key, value)?;
			if parsed < 0.0 {
				return Err(EngineError::invalid("replicate_sec must be positive"));
			}
			config.replicate_sec = Some(parsed);
		},
		"mesh_heartbeat_sec" => config.mesh_heartbeat_sec = positive_float(key, value)?,
		"mesh_reap_sec" => config.mesh_reap_sec = positive_float(key, value)?,
		"mesh_idem_ttl_sec" => config.mesh_idem_ttl_sec = positive_float(key, value)?,
		"mesh_create_timeout_sec" => {
			config.mesh_create_timeout_sec = positive_float(key, value)?;
		},
		"mesh_w_warm" => config.mesh_w_warm = positive_float(key, value)?,
		"mesh_w_free" => config.mesh_w_free = positive_float(key, value)?,
		"mesh_w_local" => config.mesh_w_local = positive_float(key, value)?,
		"mesh_w_region" => config.mesh_w_region = positive_float(key, value)?,
		"mesh_w_inflight" => config.mesh_w_inflight = positive_float(key, value)?,
		_ => return Err(EngineError::invalid(format!("unsupported serve config key {key:?}"))),
	}
	config.set_source(key, source);
	Ok(())
}

#[derive(Clone, Copy)]
enum RawValue<'a> {
	String(&'a str),
	Toml(&'a toml::Value),
}

fn coerce_string(key: &str, value: RawValue<'_>) -> Result<String> {
	match value {
		RawValue::String(value) => Ok(value.to_owned()),
		RawValue::Toml(toml::Value::String(value)) => Ok(value.clone()),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be a string"))),
	}
}

fn empty_string_as_none(key: &str, value: RawValue<'_>) -> Result<Option<String>> {
	let text = coerce_string(key, value)?.trim().to_owned();
	Ok((!text.is_empty()).then_some(text))
}

fn coerce_int(key: &str, value: RawValue<'_>) -> Result<i64> {
	match value {
		RawValue::String(value) => {
			let text = value.trim();
			if text.is_empty() {
				return Err(EngineError::invalid(format!("{key} must be an integer")));
			}
			text
				.parse::<i64>()
				.map_err(|_| EngineError::invalid(format!("{key} has invalid value {value:?}")))
		},
		RawValue::Toml(toml::Value::Integer(value)) => Ok(*value),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be an integer"))),
	}
}

fn coerce_float(key: &str, value: RawValue<'_>) -> Result<f64> {
	match value {
		RawValue::String(value) => {
			let text = value.trim();
			if text.is_empty() {
				return Err(EngineError::invalid(format!("{key} must be a number")));
			}
			text
				.parse::<f64>()
				.map_err(|_| EngineError::invalid(format!("{key} has invalid value {value:?}")))
		},
		RawValue::Toml(toml::Value::Integer(value)) => Ok(*value as f64),
		RawValue::Toml(toml::Value::Float(value)) => Ok(*value),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be a number"))),
	}
}

fn coerce_bool(key: &str, value: RawValue<'_>) -> Result<bool> {
	match value {
		RawValue::String(value) => {
			let text = value.trim().to_ascii_lowercase();
			if BOOL_TRUE.contains(&text.as_str()) {
				Ok(true)
			} else if BOOL_FALSE.contains(&text.as_str()) {
				Ok(false)
			} else {
				Err(EngineError::invalid(format!("{key} must be a boolean")))
			}
		},
		RawValue::Toml(toml::Value::Boolean(value)) => Ok(*value),
		RawValue::Toml(toml::Value::Integer(0)) => Ok(false),
		RawValue::Toml(toml::Value::Integer(1)) => Ok(true),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be a boolean"))),
	}
}

fn non_negative_usize(key: &str, value: RawValue<'_>) -> Result<usize> {
	let parsed = coerce_int(key, value)?;
	if parsed < 0 {
		return Err(EngineError::invalid(format!("{key} must be non-negative")));
	}
	usize::try_from(parsed).map_err(|_| EngineError::invalid(format!("{key} must be non-negative")))
}

fn positive_float(key: &str, value: RawValue<'_>) -> Result<f64> {
	let parsed = coerce_float(key, value)?;
	if parsed <= 0.0 {
		return Err(EngineError::invalid(format!("{key} must be positive")));
	}
	Ok(parsed)
}

fn coerce_warm_images(value: RawValue<'_>, default_count: usize) -> Result<Vec<WarmImage>> {
	let entries = warm_image_entries(value)?;
	let mut parsed = Vec::new();
	for entry in entries {
		let item = entry.trim();
		if item.is_empty() {
			continue;
		}
		let (raw_ref, count) = split_warm_image_count(item, default_count)?;
		let reference = raw_ref.trim();
		if reference.is_empty() {
			return Err(EngineError::invalid("warm_images entries must include an image reference"));
		}
		parsed.push(WarmImage { reference: reference.to_owned(), count });
	}
	Ok(parsed)
}

fn warm_image_entries(value: RawValue<'_>) -> Result<Vec<String>> {
	match value {
		RawValue::String(value) => Ok(value
			.split(|ch: char| ch == ',' || ch.is_whitespace())
			.map(ToOwned::to_owned)
			.collect()),
		RawValue::Toml(toml::Value::String(value)) => Ok(value
			.split(|ch: char| ch == ',' || ch.is_whitespace())
			.map(ToOwned::to_owned)
			.collect()),
		RawValue::Toml(toml::Value::Array(values)) => values
			.iter()
			.map(|value| match value {
				toml::Value::String(value) => Ok(value.clone()),
				_ => Err(EngineError::invalid("warm_images entries must be strings")),
			})
			.collect(),
		RawValue::Toml(_) => Err(EngineError::invalid("warm_images must be a string or list")),
	}
}

fn split_warm_image_count(item: &str, default_count: usize) -> Result<(&str, usize)> {
	if let Some((reference, raw_count)) = item.rsplit_once('=') {
		return Ok((reference, warm_image_count(raw_count)?));
	}
	if let Some((reference, raw_count)) = item.rsplit_once(':') {
		let count = raw_count.trim();
		if looks_like_integer(count) {
			return Ok((reference, warm_image_count(count)?));
		}
	}
	Ok((item, default_count))
}

fn looks_like_integer(text: &str) -> bool {
	let digits = text
		.strip_prefix('-')
		.or_else(|| text.strip_prefix('+'))
		.unwrap_or(text);
	!digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())
}

fn warm_image_count(raw_count: &str) -> Result<usize> {
	let count = coerce_int("warm_images count", RawValue::String(raw_count))?;
	if count < 0 {
		return Err(EngineError::invalid("warm_images counts must be non-negative integers"));
	}
	usize::try_from(count)
		.map_err(|_| EngineError::invalid("warm_images counts must be non-negative integers"))
}

fn expand_user(text: &str) -> PathBuf {
	if text == "~" {
		return env::var_os("HOME").map_or_else(|| PathBuf::from(text), PathBuf::from);
	}
	if let Some(rest) = text.strip_prefix("~/") {
		return env::var_os("HOME")
			.map_or_else(|| PathBuf::from(text), |home| PathBuf::from(home).join(rest));
	}
	PathBuf::from(text)
}

#[cfg(test)]
mod tests {
	use std::ffi::OsString;

	use super::*;
	use crate::home::test_home;

	struct EnvGuard {
		saved: Vec<(String, Option<OsString>)>,
	}

	impl EnvGuard {
		fn clear() -> Self {
			let keys = ENV_KEYS
				.iter()
				.map(|(_, env_key)| (*env_key).to_owned())
				.chain(["VMON_CONFIG".to_owned()])
				.collect::<Vec<_>>();
			let saved = keys
				.iter()
				.map(|key| (key.clone(), env::var_os(key)))
				.collect::<Vec<_>>();
			for key in keys {
				// SAFETY: config tests serialize all environment mutation with the shared
				// test_home lock.
				unsafe { env::remove_var(key) };
			}
			Self { saved }
		}

		fn set(key: &str, value: &str) {
			// SAFETY: config tests serialize all environment mutation with the shared
			// test_home lock.
			unsafe { env::set_var(key, value) };
		}
	}

	impl Drop for EnvGuard {
		fn drop(&mut self) {
			for (key, value) in &self.saved {
				match value {
					Some(value) => {
						// SAFETY: config tests serialize all environment mutation with the shared
						// test_home lock.
						unsafe { env::set_var(key, value) };
					},
					None => {
						// SAFETY: config tests serialize all environment mutation with the shared
						// test_home lock.
						unsafe { env::remove_var(key) };
					},
				}
			}
		}
	}

	#[test]
	fn precedence_tracks_default_file_env_flag_sources() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let tmp = tempfile::tempdir().expect("tempdir");
		let config_path = tmp.path().join("serve.toml");
		fs::write(
			&config_path,
			"[serve]\nhost = '0.0.0.0'\nport = 9001\nclient_token = 'from-file'\nreplicas = 2\n",
		)
		.expect("write config");
		EnvGuard::set("VMON_CONFIG", config_path.to_str().expect("utf8 path"));
		EnvGuard::set("VMON_SERVE_PORT", "9002");
		let overrides = HashMap::from([
			("host".to_owned(), "192.0.2.1".to_owned()),
			("replicas".to_owned(), "5".to_owned()),
		]);
		let config = resolve_serve_config(&overrides).expect("resolve config");
		assert_eq!(config.host, "192.0.2.1");
		assert_eq!(config.port, 9002);
		assert_eq!(config.client_token.as_deref(), Some("from-file"));
		assert_eq!(config.replicas, 5);
		assert_eq!(config.idle_timeout, 300.0);
		assert_eq!(config.source("host"), Some(ConfigSource::Flag));
		assert_eq!(config.source("port"), Some(ConfigSource::Env));
		assert_eq!(config.source("client_token"), Some(ConfigSource::File));
		assert_eq!(config.source("idle_timeout"), Some(ConfigSource::Default));
	}

	#[test]
	fn coercion_accepts_bool_numbers_floats_and_warm_images() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let overrides = HashMap::from([
			("restore_quorum".to_owned(), "yes".to_owned()),
			("port".to_owned(), "0".to_owned()),
			("idle_timeout".to_owned(), "1.5".to_owned()),
			("warm_pool_size".to_owned(), "3".to_owned()),
			("warm_images".to_owned(), "alpine:2 ubuntu=4,debian".to_owned()),
		]);
		let config = resolve_serve_config(&overrides).expect("resolve config");
		assert_eq!(config.restore_quorum, Some(true));
		assert_eq!(config.port, 0);
		assert_eq!(config.idle_timeout, 1.5);
		assert_eq!(config.warm_images[0], WarmImage { reference: "alpine".to_owned(), count: 2 });
		assert_eq!(config.warm_images[1], WarmImage { reference: "ubuntu".to_owned(), count: 4 });
		assert_eq!(config.warm_images[2], WarmImage { reference: "debian".to_owned(), count: 3 });
	}

	#[test]
	fn function_artifact_quota_defaults_and_honors_env_and_cli() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		assert_eq!(
			resolve_serve_config(&HashMap::new())
				.expect("default config")
				.function_artifact_max_bytes,
			4 * 1024 * 1024 * 1024
		);
		EnvGuard::set("VMON_FUNCTION_ARTIFACT_MAX_BYTES", "67108864");
		let from_env = resolve_serve_config(&HashMap::new()).expect("env quota");
		assert_eq!(from_env.function_artifact_max_bytes, 67_108_864);
		assert_eq!(from_env.source("function_artifact_max_bytes"), Some(ConfigSource::Env));
		let from_cli = resolve_serve_config(&HashMap::from([(
			"function_artifact_max_bytes".to_owned(),
			"134217728".to_owned(),
		)]))
		.expect("CLI quota");
		assert_eq!(from_cli.function_artifact_max_bytes, 134_217_728);
		assert_eq!(from_cli.source("function_artifact_max_bytes"), Some(ConfigSource::Flag));
	}

	#[test]
	fn malformed_values_name_the_key() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		for (key, value) in
			[("port", "abc"), ("restore_quorum", "maybe"), ("warm_images", "alpine=-1")]
		{
			let overrides = HashMap::from([(key.to_owned(), value.to_owned())]);
			let err = resolve_serve_config(&overrides).expect_err("bad value");
			assert!(err.message.contains(key), "expected {key:?} in error {:?}", err.message);
		}
	}

	#[test]
	fn function_artifact_quota_rejects_zero_and_excessive_values() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		for value in ["0", "-1", "1125899906842625"] {
			let error = resolve_serve_config(&HashMap::from([(
				"function_artifact_max_bytes".to_owned(),
				value.to_owned(),
			)]))
			.expect_err("invalid artifact quota");
			assert!(error.message.contains("function_artifact_max_bytes"));
		}
	}

	#[test]
	fn rejects_unknown_config_keys() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let overrides = HashMap::from([("bogus".to_owned(), "1".to_owned())]);
		let err = resolve_serve_config(&overrides).expect_err("unknown key");
		assert!(err.message.contains("unknown CLI override"));
		validate_knob_inventory().expect("metadata covers fields");
	}
}
