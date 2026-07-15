//! Shared TLS-capable `PostgreSQL` connection setup.

use postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::{EngineError, Result};

/// Connect to `PostgreSQL` with system-root TLS when requested by the URL.
pub fn connect(url: &str, context: &str) -> Result<Client> {
	let (tls, errors) = MakeRustlsConnect::with_native_certs().map_err(|errors| {
		EngineError::engine(format!("{context}: no usable system TLS roots: {errors:?}"))
	})?;
	if !errors.is_empty() {
		tracing::warn!(context, ?errors, "some PostgreSQL TLS roots could not be loaded");
	}
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
