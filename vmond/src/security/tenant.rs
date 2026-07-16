//! Authenticated tenant identities and customer-key selection.

use std::collections::HashMap;

use crate::{EngineError, Result};

/// Authorization role attached to an authenticated request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
	/// Cluster administrator allowed to cross tenant boundaries.
	Admin,
	/// Tenant client confined to resources owned by one tenant.
	Client,
}

/// Non-secret identity propagated through the API and mesh layers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Principal {
	/// Stable tenant identifier, or `system` for administrators.
	pub tenant: String,
	/// Authorization role.
	pub role:   Role,
	/// Customer-managed encryption key selected for newly persisted data.
	pub key_id: String,
}

impl Principal {
	/// The local administrative identity used by authenticated UDS callers.
	pub fn local_admin() -> Self {
		Self { tenant: "system".to_owned(), role: Role::Admin, key_id: "default".to_owned() }
	}

	/// Build a validated tenant client identity.
	pub fn client(tenant: impl Into<String>, key_id: impl Into<String>) -> Result<Self> {
		let tenant = tenant.into();
		validate_tenant(&tenant)?;
		let key_id = key_id.into();
		if key_id.is_empty() || key_id.len() > 128 {
			return Err(EngineError::invalid("encryption key IDs must contain 1 to 128 bytes"));
		}
		Ok(Self { tenant, role: Role::Client, key_id })
	}

	/// Whether this identity may access every tenant.
	pub const fn is_admin(&self) -> bool {
		matches!(self.role, Role::Admin)
	}

	/// Reject access to a resource owned by a different tenant.
	pub fn require_tenant(&self, owner: &str) -> Result<()> {
		if self.is_admin() || owner == self.tenant {
			Ok(())
		} else {
			Err(EngineError::unauthorized("resource belongs to another tenant"))
		}
	}
}

/// Constant-time bearer-token resolver for administrators and tenant clients.
#[derive(Clone, Debug)]
pub struct TenantDirectory {
	admin_tokens:  Vec<Vec<u8>>,
	client_tokens: Vec<Vec<u8>>,
	tenant_tokens: Vec<(Vec<u8>, Principal)>,
	tenant_keys:   HashMap<String, String>,
}

impl TenantDirectory {
	/// Build a directory from global tokens and tenant-token/key mappings.
	///
	/// `tenant_tokens` maps opaque bearer tokens to tenant IDs. `tenant_keys`
	/// maps tenant IDs to key IDs present in the host keyring.
	pub fn new(
		admin_token: Option<String>,
		client_token: Option<String>,
		tenant_tokens: HashMap<String, String>,
		tenant_keys: HashMap<String, String>,
	) -> Result<Self> {
		for tenant in tenant_tokens.values().chain(tenant_keys.keys()) {
			validate_tenant(tenant)?;
		}
		let tenant_tokens = tenant_tokens
			.into_iter()
			.map(|(token, tenant)| {
				if token.is_empty() {
					return Err(EngineError::invalid("tenant bearer tokens must not be empty"));
				}
				let key_id = tenant_keys
					.get(&tenant)
					.cloned()
					.unwrap_or_else(|| "default".to_owned());
				Ok((token.into_bytes(), Principal { tenant, role: Role::Client, key_id }))
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(Self {
			admin_tokens: split_tokens(admin_token),
			client_tokens: split_tokens(client_token),
			tenant_tokens,
			tenant_keys,
		})
	}

	/// Resolve a bearer token without leaking which candidate matched.
	pub fn authenticate(&self, supplied: Option<&str>) -> Option<Principal> {
		let supplied = supplied?.as_bytes();
		if self
			.admin_tokens
			.iter()
			.any(|expected| constant_time_eq(supplied, expected))
		{
			return Some(Principal::local_admin());
		}
		if self
			.client_tokens
			.iter()
			.any(|expected| constant_time_eq(supplied, expected))
		{
			return Some(Principal {
				tenant: "default".to_owned(),
				role:   Role::Client,
				key_id: self.key_for("default"),
			});
		}
		self.tenant_tokens.iter().find_map(|(token, principal)| {
			constant_time_eq(supplied, token).then(|| principal.clone())
		})
	}

	/// Whether any TCP credential is configured.
	pub const fn tokens_configured(&self) -> bool {
		!self.admin_tokens.is_empty()
			|| !self.client_tokens.is_empty()
			|| !self.tenant_tokens.is_empty()
	}

	/// Return the configured key for a tenant, defaulting to the host key.
	pub fn key_for(&self, tenant: &str) -> String {
		self
			.tenant_keys
			.get(tenant)
			.cloned()
			.unwrap_or_else(|| "default".to_owned())
	}
}

fn validate_tenant(tenant: &str) -> Result<()> {
	if tenant.is_empty()
		|| tenant.len() > 64
		|| !tenant
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
	{
		return Err(EngineError::invalid("tenant IDs must match [A-Za-z0-9_-]{1,64}"));
	}
	Ok(())
}

fn split_tokens(tokens: Option<String>) -> Vec<Vec<u8>> {
	tokens
		.unwrap_or_default()
		.split(',')
		.map(str::trim)
		.filter(|token| !token.is_empty())
		.map(|token| token.as_bytes().to_vec())
		.collect()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
	let mut different = left.len() ^ right.len();
	let width = left.len().max(right.len());
	for index in 0..width {
		let a = left.get(index).copied().unwrap_or(0);
		let b = right.get(index).copied().unwrap_or(0);
		different |= usize::from(a ^ b);
	}
	different == 0
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::{Role, TenantDirectory};

	#[test]
	fn tokens_resolve_to_isolated_tenants_and_keys() {
		let directory = TenantDirectory::new(
			Some("admin,second-admin".into()),
			Some("client".into()),
			HashMap::from([("tenant-token".into(), "acme".into())]),
			HashMap::from([("acme".into(), "acme-kms".into())]),
		)
		.unwrap();
		let admin = directory.authenticate(Some("second-admin")).unwrap();
		assert_eq!(admin.role, Role::Admin);
		let tenant = directory.authenticate(Some("tenant-token")).unwrap();
		assert_eq!(tenant.tenant, "acme");
		assert_eq!(tenant.key_id, "acme-kms");
		assert!(tenant.require_tenant("other").is_err());
	}
}
