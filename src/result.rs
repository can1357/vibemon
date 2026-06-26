//! Crate-wide error and result types.
//!
//! Most of the underlying crates (`kvm-ioctls`, `vm-memory`, `linux-loader`,
//! `virtio-queue`) expose errors that already implement
//! [`std::error::Error`] + `Send` + `Sync`, so a boxed trait object is the
//! least-friction way to thread them through `?` without a giant bespoke enum.

/// Boxed, thread-safe error type used throughout the VMM.
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Result alias defaulting to the boxed [`Error`]; callers may override `E`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Build a boxed error from anything that is `Display` (typically a string).
pub fn err<S: Into<String>>(msg: S) -> Error {
    Error::from(msg.into())
}

/// Convenience: `bail!("something {x}")` returns early with a formatted error.
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::result::err(format!($($arg)*)))
    };
}
