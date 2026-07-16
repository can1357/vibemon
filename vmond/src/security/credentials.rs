//! Encrypted, tenant-scoped credentials resolved only by the host gateway.

use std::{
	collections::BTreeMap,
	fmt, fs,
	io::{Read, Write},
	path::{Path, PathBuf},
	sync::Arc,
};

use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use super::crypto::{EncryptedArchive, Keyring};
use crate::{
	EngineError, Result,
	home::Home,
	mesh::cluster_store::{ProductionStore, TenantCredential},
};

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
	/// Tenant-local stable credential name.
	pub name:                   String,
	/// Exact domains or wildcard suffixes permitted for injection.
	pub allowed_domains:        Vec<String>,
	/// Validated header names; header values remain encrypted and private.
	pub header_names:           Vec<String>,
	/// Optional UTC expiration time in Unix milliseconds.
	pub expires_at_unix_millis: Option<u64>,
	/// Maximum host-enforced requests per minute.
	pub requests_per_minute:    u32,
	/// Immutable version refreshed on each successful write.
	pub version:                String,
}

/// Private payload whose authenticated ciphertext is bound to its persistence
/// location, preventing an otherwise valid ciphertext from being replayed
/// under another tenant or credential name.
#[derive(Serialize, Deserialize)]
struct StoredCredential {
	tenant:     String,
	credential: Credential,
}

/// Pluggable host-side credential lookup used by sandbox gateways.
pub trait CredentialProvider: Send + Sync {
	/// Resolve one tenant-scoped credential; secret bytes remain host-side.
	fn get(&self, tenant: &str, name: &str) -> Result<Credential>;

	/// List non-secret metadata visible to one tenant.
	fn list(&self, tenant: &str) -> Result<Vec<CredentialMetadata>>;
}
/// Encrypted credential provider backed by local files or `PostgreSQL`.
#[derive(Clone)]
pub struct CredentialStore {
	backend:      CredentialBackend,
	keyring:      Arc<Keyring>,
	fallback_key: String,
}

#[derive(Clone)]
enum CredentialBackend {
	Local { root: PathBuf },
	Production(Arc<ProductionStore>),
}

impl fmt::Debug for CredentialStore {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("CredentialStore")
			.finish_non_exhaustive()
	}
}

impl CredentialStore {
	/// Open the single-node encrypted credential hierarchy.
	pub fn open(home: &Home, keyring: Arc<Keyring>) -> Result<Self> {
		let root = home.credentials_dir();
		fs::create_dir_all(&root)?;
		set_private(&root)?;
		Ok(Self {
			backend: CredentialBackend::Local { root },
			keyring,
			fallback_key: "default".to_owned(),
		})
	}

	/// Open the shared `PostgreSQL` credential backend. Default key selection is
	/// replaced with the verified portable-history fallback key.
	pub(crate) fn open_production(
		store: Arc<ProductionStore>,
		keyring: Arc<Keyring>,
		portable_history_key_id: &str,
	) -> Result<Self> {
		if portable_history_key_id.is_empty() || portable_history_key_id == "default" {
			return Err(EngineError::invalid(
				"production credentials require a non-default portable history key",
			));
		}
		keyring.load(portable_history_key_id)?;
		Ok(Self {
			backend: CredentialBackend::Production(store),
			keyring,
			fallback_key: portable_history_key_id.to_owned(),
		})
	}

	/// Encrypt and atomically persist a credential. `key_id` selects a tenant
	/// key; `default` resolves to the deployment's verified fallback key.
	///
	/// Returns only [`CredentialMetadata`]; injected header values never leave
	/// this method except as encrypted ciphertext.
	pub fn put(
		&self,
		tenant: &str,
		key_id: &str,
		credential: Credential,
	) -> Result<CredentialMetadata> {
		validate_tenant(tenant)?;
		let credential = credential.validate()?;
		let metadata = credential.metadata();
		let selected_key = if key_id == "default" {
			self.fallback_key.as_str()
		} else {
			key_id
		};
		let stored = StoredCredential { tenant: tenant.to_owned(), credential };
		let plaintext = Zeroizing::new(serde_json::to_vec(&stored)?);
		match &self.backend {
			CredentialBackend::Local { .. } => EncryptedArchive::seal_bytes(
				&plaintext,
				&self.path(tenant, &stored.credential.name)?,
				&self.keyring,
				selected_key,
			)?,
			CredentialBackend::Production(store) => {
				let mut ciphertext = Vec::new();
				{
					let mut encrypted =
						EncryptedArchive::encrypt(&mut ciphertext, &self.keyring, selected_key)?;
					encrypted.write_all(&plaintext)?;
					encrypted.finish()?;
				}
				store.put_tenant_credential(&tenant_record(
					tenant,
					selected_key,
					&stored.credential,
					ciphertext,
				)?)?;
			},
		}
		Ok(metadata)
	}

	/// Permanently remove one tenant-local credential and its ciphertext.
	///
	/// A missing credential is reported as not found rather than silently
	/// succeeding, so callers cannot mistake a failed cleanup for completion.
	pub fn delete(&self, tenant: &str, name: &str) -> Result<()> {
		validate_tenant(tenant)?;
		validate_name(name)?;
		match &self.backend {
			CredentialBackend::Local { .. } => {
				let path = self.path(tenant, name)?;
				match fs::remove_file(&path) {
					Ok(()) => Ok(()),
					Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
						Err(EngineError::not_found(format!("credential {name:?} does not exist")))
					},
					Err(error) => Err(error.into()),
				}
			},
			CredentialBackend::Production(store) => {
				if store.delete_tenant_credential(tenant, name)? {
					Ok(())
				} else {
					Err(EngineError::not_found(format!("credential {name:?} does not exist")))
				}
			},
		}
	}

	fn path(&self, tenant: &str, name: &str) -> Result<PathBuf> {
		let CredentialBackend::Local { root } = &self.backend else {
			return Err(EngineError::engine("production credential backend has no local path"));
		};
		validate_tenant(tenant)?;
		validate_name(name)?;
		let dir = root.join(tenant);
		fs::create_dir_all(&dir)?;
		set_private(&dir)?;
		Ok(dir.join(format!("{name}.venc")))
	}
}

impl CredentialProvider for CredentialStore {
	fn get(&self, tenant: &str, name: &str) -> Result<Credential> {
		validate_tenant(tenant)?;
		validate_name(name)?;
		let bytes = match &self.backend {
			CredentialBackend::Local { .. } => {
				let path = self.path(tenant, name)?;
				if !path.is_file() {
					return Err(EngineError::not_found(format!("credential {name:?} does not exist")));
				}
				Zeroizing::new(EncryptedArchive::open_bytes(&path, &self.keyring, MAX_RECORD_BYTES)?)
			},
			CredentialBackend::Production(store) => {
				let record = store.tenant_credential(tenant, name)?.ok_or_else(|| {
					EngineError::not_found(format!("credential {name:?} does not exist"))
				})?;
				EncryptedArchive::decrypt(record.ciphertext.as_slice(), &self.keyring, |reader| {
					let mut plaintext = Zeroizing::new(Vec::new());
					reader
						.take(u64::try_from(MAX_RECORD_BYTES).unwrap_or(u64::MAX) + 1)
						.read_to_end(&mut plaintext)?;
					if plaintext.len() > MAX_RECORD_BYTES {
						return Err(EngineError::invalid("encrypted record exceeds size limit"));
					}
					Ok(plaintext)
				})?
			},
		};
		let stored = serde_json::from_slice::<StoredCredential>(&bytes)
			.map_err(|_| EngineError::invalid(format!("credential {name:?} is corrupt")))?;
		if stored.tenant != tenant || stored.credential.name != name {
			return Err(EngineError::invalid(format!(
				"credential {name:?} is replayed at the wrong location"
			)));
		}
		stored.credential.validate()
	}

	fn list(&self, tenant: &str) -> Result<Vec<CredentialMetadata>> {
		validate_tenant(tenant)?;
		match &self.backend {
			CredentialBackend::Local { root } => {
				let dir = root.join(tenant);
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
			},
			CredentialBackend::Production(store) => store
				.list_tenant_credentials(tenant)?
				.into_iter()
				.map(metadata_from_record)
				.collect(),
		}
	}
}

fn tenant_record(
	tenant: &str,
	key_id: &str,
	credential: &Credential,
	ciphertext: Vec<u8>,
) -> Result<TenantCredential> {
	Ok(TenantCredential {
		tenant: tenant.to_owned(),
		name: credential.name.clone(),
		key_id: key_id.to_owned(),
		ciphertext,
		allowed_domains: serde_json::to_string(&credential.allowed_domains)?,
		header_names: serde_json::to_string(&credential.headers.keys().collect::<Vec<_>>())?,
		expires_at_unix_millis: credential
			.expires_at_unix_millis
			.map(i64::try_from)
			.transpose()
			.map_err(|_| EngineError::invalid("credential expiry exceeds PostgreSQL range"))?,
		requests_per_minute: i64::from(credential.requests_per_minute),
		version: credential.version.clone(),
	})
}

fn metadata_from_record(record: TenantCredential) -> Result<CredentialMetadata> {
	Ok(CredentialMetadata {
		name:                   record.name,
		allowed_domains:        serde_json::from_str(&record.allowed_domains)
			.map_err(|_| EngineError::engine("stored credential domains are corrupt"))?,
		header_names:           serde_json::from_str(&record.header_names)
			.map_err(|_| EngineError::engine("stored credential headers are corrupt"))?,
		expires_at_unix_millis: record
			.expires_at_unix_millis
			.map(u64::try_from)
			.transpose()
			.map_err(|_| EngineError::engine("stored credential expiry is negative"))?,
		requests_per_minute:    u32::try_from(record.requests_per_minute)
			.map_err(|_| EngineError::engine("stored credential rate is invalid"))?,
		version:                record.version,
	})
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
	if matches!(
		name.to_ascii_lowercase().as_str(),
		"host"
			| "content-length"
			| "connection"
			| "keep-alive"
			| "proxy-authenticate"
			| "proxy-authorization"
			| "te" | "trailer"
			| "transfer-encoding"
			| "upgrade"
			| "accept-encoding"
	) {
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
		assert_eq!(store.list("acme").unwrap(), vec![metadata]);

		fs::copy(
			home.credentials_dir().join("acme/github.venc"),
			home.credentials_dir().join("acme/other.venc"),
		)
		.unwrap();
		assert!(store.get("acme", "other").is_err());
		fs::create_dir_all(home.credentials_dir().join("other")).unwrap();
		fs::copy(
			home.credentials_dir().join("acme/github.venc"),
			home.credentials_dir().join("other/github.venc"),
		)
		.unwrap();
		assert!(store.get("other", "github").is_err());
		fs::remove_file(home.credentials_dir().join("acme/other.venc")).unwrap();
		fs::remove_file(home.credentials_dir().join("other/github.venc")).unwrap();
		fs::remove_dir(home.credentials_dir().join("other")).unwrap();

		let reopened = CredentialStore::open(&home, Arc::new(Keyring::open(&home).unwrap())).unwrap();
		assert_eq!(
			reopened.get("acme", "github").unwrap().headers["Authorization"],
			b"Bearer top-secret"
		);
		fs::remove_file(home.keys_dir().join("default.key")).unwrap();
		assert!(reopened.get("acme", "github").is_err());
		store.delete("acme", "github").unwrap();
		assert!(reopened.get("acme", "github").is_err());
		assert!(reopened.list("acme").unwrap().is_empty());
	}

	#[test]
	fn credentials_reject_gateway_hop_headers() {
		for header in [
			"Host",
			"Content-Length",
			"Connection",
			"Keep-Alive",
			"Proxy-Authenticate",
			"Proxy-Authorization",
			"TE",
			"Trailer",
			"Transfer-Encoding",
			"Upgrade",
			"Accept-Encoding",
		] {
			assert!(
				Credential {
					name:                   "blocked".into(),
					allowed_domains:        vec!["api.example.test".into()],
					headers:                BTreeMap::from([(header.into(), b"value".to_vec())]),
					expires_at_unix_millis: None,
					requests_per_minute:    1,
					version:                String::new(),
				}
				.validate()
				.is_err()
			);
		}
	}
}
