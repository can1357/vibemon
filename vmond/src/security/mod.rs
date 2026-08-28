//! Tenant isolation, customer-managed encryption, credential brokering, and
//! audit logging.

pub mod audit;
pub mod credentials;
pub mod crypto;
pub mod gateway;
pub mod host_gateway;
pub mod tenant;

pub use audit::{AuditEvent, AuditLog};
pub use credentials::{Credential, CredentialProvider, CredentialStore};
pub use crypto::{EncryptedArchive, Keyring};
pub use gateway::{CREDENTIAL_GATEWAY_PORT, CredentialGateway};
pub use host_gateway::{HostGateway, HostGatewayClaims, HostGatewayCommand, HostGatewayEvent};
pub use tenant::{Principal, TenantDirectory};
