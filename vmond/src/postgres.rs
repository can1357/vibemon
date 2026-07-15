//! Shared TLS-capable `PostgreSQL` connection setup.

use native_tls::TlsConnector;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;

use crate::{EngineError, Result};

/// Connect to `PostgreSQL` with system-root TLS when requested by the URL.
pub fn connect(url: &str, context: &str) -> Result<Client> {
	let tls = TlsConnector::builder()
		.build()
		.map(MakeTlsConnector::new)
		.map_err(|error| {
			EngineError::engine(format!("{context}: TLS initialization failed: {error}"))
		})?;
	blocking(|| Client::connect(url, tls))
		.map_err(|error| EngineError::engine(format!("{context}: {error}")))
}

/// Run a synchronous `PostgreSQL` operation without nesting its private
/// runtime.
pub fn blocking<T>(operation: impl FnOnce() -> T) -> T {
	if tokio::runtime::Handle::try_current()
		.is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
	{
		tokio::task::block_in_place(operation)
	} else {
		operation()
	}
}
