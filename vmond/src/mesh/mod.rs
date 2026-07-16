//! Cluster membership, placement, leases, replication, and peer routes.
//!
//! Phase 4 ports the Python mesh/cluster layer into these focused modules.

pub mod bundle;
pub mod cluster_store;
#[cfg(test)]
pub mod cluster_tests;
pub mod gossip;
pub mod lease;
pub mod place;
pub mod proxy;
pub mod reconciler;
pub mod record;
pub mod replica;
pub mod replica_cache;
pub mod routes;
pub mod runtime;
#[cfg(test)]
mod runtime_acceptance;
pub mod state;
pub mod transfer;
