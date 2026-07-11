//! Generated protobuf/tonic types for the vmon v1 gRPC API
//! (proto/vmon/v1/{api,bridge}.proto).
pub use prost;
pub use tonic;

pub mod v1 {
	#![allow(
		clippy::pedantic,
		clippy::nursery,
		clippy::style,
		clippy::allow_attributes_without_reason,
		reason = "prost-generated code"
	)]
	include!(concat!(env!("OUT_DIR"), "/vmon.v1.rs"));
}
