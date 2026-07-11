//! vmond — the Vibemon server core.
//!
//! Owns the sandbox registry and every VM lifecycle operation (`engine/`),
//! the OCI→ext4→template image pipeline (`image/`), host networking (`net`),
//! warm pools, named volumes, and the v1 HTTP API served by `vmon serve`
//! over `$VMON_HOME/vmond.sock` (HTTP-over-UDS) and optionally TCP.
//!
//! The engine is synchronous (threads + blocking syscalls, like the VMM
//! core); `tokio`/`axum` live only in the API layer, which reaches the
//! engine through `tokio::task::spawn_blocking`.

pub mod api;
pub mod config;
pub mod doctor;
pub mod engine;
pub mod error;
pub mod home;
pub mod function;
pub mod image;
pub mod mesh;
pub mod models;
pub mod net;
pub mod pools;
pub mod registry;
pub mod volumes;

pub use error::{EngineError, ErrorCode, Result};
