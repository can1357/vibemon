//! Encrypted, tenant-scoped credentials resolved only by the host gateway.

use std::{
	collections::BTreeMap,
	fmt, fs,
	path::{Path, PathBuf},
	sync::Arc,
};

use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use super::crypto::{EncryptedArchive, Keyring};
use crate::{EngineError, Result, home::Home};

const MAX_RECORD_BYTES: usize = 1024 * 1024;
const DEFAULT_RATE_LIMIT: u32 = 600;

/// Secret HTTP credential material and its host-enforced service scope.
#[derive(Clone, Serialize, Deserialize)]
pub struct Credential {
	/// Tenant-local stable name.
	pub name:                   String,
	/// Exact domains or leading-wildcard suffixes that may receive the headers.
	pub allowed_domains:        Vec<String>,
	/// Header names and raw values injected after guest headers are validated.
	pub headers:                BTreeMap<String, Vec<u8>>,
	/// Optional absolute expiry time in Unix milliseconds.
	pub expires_at_unix_millis: Option<u64>,
	/// Maximum brokered requests in a rolling minute.
	pub requests_per_minute:    u32,
	/// Immutable version changed by every write.
	pub version:                String,
}

impl Credential {
	/// Validate and normalize a credential before encrypted persistence.
	pub fn validate(mut self) -> Result<Self> {
		validate_name(&self.name)?;
		if self.allowed_domains.is_empty() {
			return Err(EngineError::invalid("credential requires at least one allowed domain"));
		}
		self.allowed_domains = std::mem::take(&mut self.allowed_domains)
			.into_iter()
			.map(|domain| normalize_domain(&domain))
			.collect::<Result<Vec<_>>>()?;
		self.allowed_domains.sort();
		self.allowed_domains.dedup();
		if self.headers.is_empty() {
			return Err(EngineError::invalid("credential requires at least one injected header"));
		}
		for (name, value) in &self.headers {
			validate_header(name, value)?;
		}
		if self.requests_per_minute == 0 {
			self.requests_per_minute = DEFAULT_RATE_LIMIT;
		}
		if self.version.is_empty() {
			self.version = hex::encode(rand::random::<[u8; 16]>());
		}
		Ok(self)
	}

	/// Non-secret metadata safe for API responses and audit records.
	pub fn metadata(&self) -> CredentialMetadata {
		CredentialMetadata {
			name:                   self.name.clone(),
			allowed_domains:        self.allowed_domains.clone(),
			header_names:           self.headers.keys().cloned().collect(),
			expires_at_unix_millis: self.expires_at_unix_millis,
			requests_per_minute:    self.requests_per_minute,
			version:                self.version.clone(),
		}
	}

	/// Whether a target host is inside this credential's service scope.
	pub fn permits_domain(&self, host: &str) -> bool {
		let Ok(host) = normalize_domain(host) else {
			return false;
		};
		self.allowed_domains.iter().any(|allowed| {
			allowed == &host
				|| allowed.strip_prefix("*.").is_some_and(|suffix| {
					host.len() > suffix.len()
						&& host.ends_with(suffix)
						&& host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
				})
		})
	}

	/// Whether this credential is still usable at `now_unix_millis`.
	pub fn active_at(&self, now_unix_millis: u64) -> bool {
		self
			.expires_at_unix_millis
			.is_none_or(|expiry| now_unix_millis < expiry)
	}
}

impl fmt::Debug for Credential {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("Credential")
			.field("name", &self.name)
			.field("allowed_domains", &self.allowed_domains)
			.field("header_names", &self.headers.keys().collect::<Vec<_>>())
			.field("expires_at_unix_millis", &self.expires_at_unix_millis)
			.field("requests_per_minute", &self.requests_per_minute)
			.field("version", &self.version)
			.finish()
	}
}

impl Drop for Credential {
	fn drop(&mut self) {
		for value in self.headers.values_mut() {
			value.zeroize();
		}
	}
}

/// Public credential fields that never reveal injected values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CredentialMetadata {
	pub name:                   String,
	pub allowed_domains:        Vec<String>,
	pub header_names:           Vec<String>,
	pub expires_at_unix_millis: Option<u64>,
	pub requests_per_minute:    u32,
	pub version:                String,
}

/// Pluggable host-side credential lookup used by sandbox gateways.
pub trait CredentialProvider: Send + Sync {
	/// Resolve one tenant-scoped credential; secret bytes remain host-side.
	fn get(&self, tenant: &str, name: &str) -> Result<Credential>;

	/// List non-secret metadata visible to one tenant.
	fn list(&self, tenant: &str) -> Result<Vec<CredentialMetadata>>;
}

/// Encrypted file-backed credential provider.
#[derive(Clone)]
pub struct CredentialStore {
	root:    PathBuf,
	keyring: Arc<Keyring>,
}

impl fmt::Debug for CredentialStore {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("CredentialStore")
			.field("root", &self.root)
			.finish_non_exhaustive()
	}
}

impl CredentialStore {
	/// Open the encrypted credential hierarchy.
	pub fn open(home: &Home, keyring: Arc<Keyring>) -> Result<Self> {
		let root = home.credentials_dir();
		fs::create_dir_all(&root)?;
		set_private(&root)?;
		Ok(Self { root, keyring })
	}

	/// Encrypt and atomically replace one credential under a customer key.
	pub fn put(
		&self,
		tenant: &str,
		key_id: &str,
		credential: Credential,
	) -> Result<CredentialMetadata> {
		validate_tenant(tenant)?;
		let credential = credential.validate()?;
		let metadata = credential.metadata();
		let bytes = serde_json::to_vec(&credential)?;
		EncryptedArchive::seal_bytes(
			&bytes,
			&self.path(tenant, &credential.name)?,
			&self.keyring,
			key_id,
		)?;
		Ok(metadata)
	}

	/// Permanently remove a tenant credential.
	pub fn delete(&self, tenant: &str, name: &str) -> Result<()> {
		let path = self.path(tenant, name)?;
		match fs::remove_file(&path) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				Err(EngineError::not_found(format!("credential {name:?} does not exist")))
			},
			Err(error) => Err(error.into()),
		}
	}

	fn path(&self, tenant: &str, name: &str) -> Result<PathBuf> {
		validate_tenant(tenant)?;
		validate_name(name)?;
		let dir = self.root.join(tenant);
		fs::create_dir_all(&dir)?;
		set_private(&dir)?;
		Ok(dir.join(format!("{name}.venc")))
	}
}

impl CredentialProvider for CredentialStore {
	fn get(&self, tenant: &str, name: &str) -> Result<Credential> {
		let path = self.path(tenant, name)?;
		if !path.is_file() {
			return Err(EngineError::not_found(format!("credential {name:?} does not exist")));
		}
		let mut bytes = EncryptedArchive::open_bytes(&path, &self.keyring, MAX_RECORD_BYTES)?;
		let credential = serde_json::from_slice::<Credential>(&bytes)
			.map_err(|_| EngineError::invalid(format!("credential {name:?} is corrupt")))?;
		bytes.zeroize();
		credential.validate()
	}

	fn list(&self, tenant: &str) -> Result<Vec<CredentialMetadata>> {
		validate_tenant(tenant)?;
		let dir = self.root.join(tenant);
		if !dir.is_dir() {
			return Ok(Vec::new());
		}
		let mut names = fs::read_dir(dir)?
			.filter_map(std::result::Result::ok)
			.filter_map(|entry| {
				entry
					.file_name()
					.to_str()
					.and_then(|name| name.strip_suffix(".venc"))
					.map(str::to_owned)
			})
			.collect::<Vec<_>>();
		names.sort();
		names
			.into_iter()
			.map(|name| self.get(tenant, &name).map(|item| item.metadata()))
			.collect()
	}
}

fn validate_tenant(tenant: &str) -> Result<()> {
	if valid_component(tenant, 64) {
		Ok(())
	} else {
		Err(EngineError::invalid("tenant IDs must match [A-Za-z0-9_-]{1,64}"))
	}
}

fn validate_name(name: &str) -> Result<()> {
	if valid_component(name, 128) {
		Ok(())
	} else {
		Err(EngineError::invalid("credential names must match [A-Za-z0-9_-]{1,128}"))
	}
}

fn valid_component(value: &str, max: usize) -> bool {
	!value.is_empty()
		&& value.len() <= max
		&& value
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn normalize_domain(value: &str) -> Result<String> {
	let value = value.trim().trim_end_matches('.').to_ascii_lowercase();
	let host = value.strip_prefix("*.").unwrap_or(&value);
	if host.is_empty()
		|| host.len() > 253
		|| host.starts_with('-')
		|| host.ends_with('-')
		|| host.split('.').any(|label| {
			label.is_empty()
				|| label.len() > 63
				|| !label
					.bytes()
					.all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
		}) {
		return Err(EngineError::invalid(format!("invalid credential domain {value:?}")));
	}
	Ok(value)
}

fn validate_header(name: &str, value: &[u8]) -> Result<()> {
	if name.is_empty()
		|| name.len() > 128
		|| !name
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
	{
		return Err(EngineError::invalid(format!("invalid credential header {name:?}")));
	}
	if matches!(name.to_ascii_lowercase().as_str(), "host" | "content-length" | "connection") {
		return Err(EngineError::invalid(format!("credential may not inject header {name:?}")));
	}
	if value.is_empty()
		|| value.len() > 16 * 1024
		|| value.contains(&b'\r')
		|| value.contains(&b'\n')
	{
		return Err(EngineError::invalid(format!("invalid value for credential header {name:?}")));
	}
	Ok(())
}

fn set_private(path: &Path) -> Result<()> {
	let metadata = fs::symlink_metadata(path)?;
	if metadata.file_type().is_symlink() || !metadata.is_dir() {
		return Err(EngineError::invalid(format!("{} must be a directory", path.display())));
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::{collections::BTreeMap, fs, sync::Arc};

	use tempfile::TempDir;

	use super::{Credential, CredentialProvider, CredentialStore};
	use crate::{home::Home, security::crypto::Keyring};

	#[test]
	fn credentials_are_encrypted_scoped_and_revocable() {
		let temp = TempDir::new().unwrap();
		let home = Home::new(temp.path());
		let keyring = Arc::new(Keyring::open(&home).unwrap());
		let store = CredentialStore::open(&home, keyring).unwrap();
		let metadata = store
			.put("acme", "default", Credential {
				name:                   "github".into(),
				allowed_domains:        vec!["api.github.com".into()],
				headers:                BTreeMap::from([(
					"Authorization".into(),
					b"Bearer top-secret".to_vec(),
				)]),
				expires_at_unix_millis: None,
				requests_per_minute:    10,
				version:                String::new(),
			})
			.unwrap();
		assert_eq!(metadata.header_names, vec!["Authorization"]);
		let raw = fs::read(home.credentials_dir().join("acme/github.venc")).unwrap();
		assert!(!raw.windows(10).any(|window| window == b"top-secret"));
		let loaded = store.get("acme", "github").unwrap();
		assert!(loaded.permits_domain("api.github.com"));
		assert!(!loaded.permits_domain("github.example"));
		assert!(store.get("other", "github").is_err());
	}
}
