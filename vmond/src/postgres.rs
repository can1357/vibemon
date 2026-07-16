//! Shared TLS-capable `PostgreSQL` connection setup.

use std::ops::{Deref, DerefMut};

use postgres::Client as SyncClient;
use tokio::runtime::Handle;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::{EngineError, Result};
/// Synchronous `PostgreSQL` client whose destructor is safe inside Tokio.
///
/// The upstream client owns a private runtime and cannot be dropped while a
/// Tokio runtime is entered. This wrapper transfers destruction to a plain OS
/// thread when necessary.
pub struct Client {
	inner: Option<SyncClient>,
}

impl Deref for Client {
	type Target = SyncClient;

	fn deref(&self) -> &Self::Target {
		self.inner.as_ref().expect("live PostgreSQL client")
	}
}

impl DerefMut for Client {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.inner.as_mut().expect("live PostgreSQL client")
	}
}

impl Drop for Client {
	fn drop(&mut self) {
		let Some(client) = self.inner.take() else {
			return;
		};
		if Handle::try_current().is_err() {
			drop(client);
			return;
		}
		std::thread::scope(|scope| match scope.spawn(move || drop(client)).join() {
			Ok(()) => {},
			Err(payload) if std::thread::panicking() => {
				tracing::error!("PostgreSQL client destructor panicked during unwinding");
				drop(payload);
			},
			Err(payload) => std::panic::resume_unwind(payload),
		});
	}
}

/// Connect to `PostgreSQL` with system-root TLS when requested by the URL.
pub fn connect(url: &str, context: &str) -> Result<Client> {
	let (tls, errors) = MakeRustlsConnect::with_native_certs().map_err(|errors| {
		EngineError::engine(format!("{context}: no usable system TLS roots: {errors:?}"))
	})?;
	if !errors.is_empty() {
		tracing::warn!(context, ?errors, "some PostgreSQL TLS roots could not be loaded");
	}
	blocking(|| SyncClient::connect(url, tls))
		.map(|client| Client { inner: Some(client) })
		.map_err(|error| EngineError::engine(format!("{context}: {error}")))
}

/// Run a synchronous `PostgreSQL` operation without nesting its private
/// runtime.
///
/// The operation and its result must be `Send`: under a current-thread Tokio
/// runtime it runs on a scoped OS thread, which permits borrowing from the
/// caller without requiring a `'static` task. Panics from that thread are
/// resumed on the caller.
pub fn blocking<T: Send>(operation: impl FnOnce() -> T + Send) -> T {
	match tokio::runtime::Handle::try_current() {
		Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
			tokio::task::block_in_place(operation)
		},
		Ok(_) => std::thread::scope(|scope| match scope.spawn(operation).join() {
			Ok(result) => result,
			Err(payload) => std::panic::resume_unwind(payload),
		}),
		Err(_) => operation(),
	}
}

#[cfg(test)]
/// Serializes tests that mutate the shared production `PostgreSQL` fixture.
pub static TEST_DATABASE_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
	std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

#[cfg(test)]
mod tests {
	use std::thread;

	use tokio::runtime::Builder;

	use super::blocking;

	#[test]
	fn current_thread_runtime_runs_blocking_work_on_a_scoped_thread() {
		let runtime = Builder::new_current_thread()
			.enable_all()
			.build()
			.expect("runtime");
		let caller = thread::current().id();
		let worker = runtime.block_on(async { blocking(|| thread::current().id()) });
		assert_ne!(worker, caller);
	}
}
