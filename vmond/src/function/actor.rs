//! Actor pinning and serialized admission.
//!
//! The durable actor record and checkpoints are owned by
//! [`super::store::Store`]. This registry contains only live worker pins and
//! serialization permits. A missing worker never implies fresh actor state:
//! callers must restore the returned checkpoint or persist an explicit lost
//! state.

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::worker::Worker;
use crate::{EngineError, Result};

/// Required action after an actor's pinned worker is lost.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActorRecovery {
	/// Restore this immutable checkpoint before accepting another method.
	Restore { checkpoint_id: String },
	/// No checkpoint exists; the actor must remain explicitly lost.
	Lost,
}

struct ActorSlot {
	worker:        Option<Arc<dyn Worker>>,
	checkpoint_id: Option<String>,
	gate:          Arc<Semaphore>,
	lost:          bool,
}

impl ActorSlot {
	fn new(checkpoint_id: Option<String>) -> Self {
		Self { worker: None, checkpoint_id, gate: Arc::new(Semaphore::new(1)), lost: false }
	}
}

/// Process-local actor worker pins and per-actor serialization gates.
#[derive(Default)]
pub struct ActorManager {
	actors: RwLock<HashMap<String, ActorSlot>>,
}

impl ActorManager {
	/// Create an empty registry. Durable actors are loaded lazily from the
	/// store.
	pub fn new() -> Self {
		Self::default()
	}

	/// Register a durable actor without inventing in-memory state.
	pub fn register(&self, actor_id: String, checkpoint_id: Option<String>) {
		self
			.actors
			.write()
			.entry(actor_id)
			.or_insert_with(|| ActorSlot::new(checkpoint_id));
	}

	/// Pin an initialized or restored worker to an actor.
	pub fn pin(
		&self,
		actor_id: &str,
		worker: Arc<dyn Worker>,
		checkpoint_id: Option<String>,
	) -> Result<()> {
		let mut actors = self.actors.write();
		let slot = actors
			.entry(actor_id.to_owned())
			.or_insert_with(|| ActorSlot::new(None));
		if slot.lost && checkpoint_id.as_ref() != slot.checkpoint_id.as_ref() {
			return Err(EngineError::not_running(format!(
				"actor {actor_id} must be restored from its latest checkpoint"
			)));
		}
		slot.worker = Some(worker);
		if checkpoint_id.is_some() {
			slot.checkpoint_id = checkpoint_id;
		}
		slot.lost = false;
		Ok(())
	}

	/// Acquire the actor's serialization gate and current pinned worker.
	pub async fn acquire(&self, actor_id: &str) -> Result<ActorPermit> {
		let (gate, worker) = {
			let actors = self.actors.read();
			let slot = actors
				.get(actor_id)
				.ok_or_else(|| EngineError::not_found(format!("actor {actor_id} not found")))?;
			if slot.lost {
				return Err(EngineError::not_running(format!("actor {actor_id} is lost")));
			}
			let worker = slot.worker.clone().ok_or_else(|| {
				EngineError::not_running(format!("actor {actor_id} has no restored worker"))
			})?;
			(Arc::clone(&slot.gate), worker)
		};
		let permit = gate
			.acquire_owned()
			.await
			.map_err(|_| EngineError::not_running(format!("actor {actor_id} is shutting down")))?;
		Ok(ActorPermit { actor_id: actor_id.to_owned(), worker, _permit: permit })
	}

	/// Commit a new checkpoint frontier after the store transaction succeeds.
	pub fn checkpoint_committed(&self, actor_id: &str, checkpoint_id: String) -> Result<()> {
		let mut actors = self.actors.write();
		let slot = actors
			.get_mut(actor_id)
			.ok_or_else(|| EngineError::not_found(format!("actor {actor_id} not found")))?;
		slot.checkpoint_id = Some(checkpoint_id);
		Ok(())
	}

	/// Remove a failed worker pin and report whether recovery is possible.
	pub fn worker_lost(&self, actor_id: &str) -> Result<ActorRecovery> {
		let mut actors = self.actors.write();
		let slot = actors
			.get_mut(actor_id)
			.ok_or_else(|| EngineError::not_found(format!("actor {actor_id} not found")))?;
		slot.worker = None;
		slot.lost = true;
		Ok(match &slot.checkpoint_id {
			Some(checkpoint_id) => ActorRecovery::Restore { checkpoint_id: checkpoint_id.clone() },
			None => ActorRecovery::Lost,
		})
	}

	/// Register a fork at a source checkpoint without sharing a live worker.
	pub fn fork(&self, source_actor_id: &str, target_actor_id: String) -> Result<()> {
		let checkpoint_id = {
			let actors = self.actors.read();
			let source = actors
				.get(source_actor_id)
				.ok_or_else(|| EngineError::not_found(format!("actor {source_actor_id} not found")))?;
			source.checkpoint_id.clone().ok_or_else(|| {
				EngineError::invalid(format!("actor {source_actor_id} has no checkpoint to fork"))
			})?
		};
		let mut actors = self.actors.write();
		if actors.contains_key(&target_actor_id) {
			return Err(EngineError::busy(format!("actor {target_actor_id} already exists")));
		}
		actors.insert(target_actor_id, ActorSlot::new(Some(checkpoint_id)));
		Ok(())
	}

	/// Remove an actor pin after durable deletion.
	pub fn remove(&self, actor_id: &str) {
		self.actors.write().remove(actor_id);
	}
}

/// Exclusive admission to one actor's currently pinned worker.
pub struct ActorPermit {
	actor_id: String,
	worker:   Arc<dyn Worker>,
	_permit:  OwnedSemaphorePermit,
}

impl ActorPermit {
	/// Durable actor identifier protected by this permit.
	pub fn actor_id(&self) -> &str {
		&self.actor_id
	}

	/// Pinned worker on which the actor method must execute.
	pub fn worker(&self) -> &Arc<dyn Worker> {
		&self.worker
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn loss_requires_checkpoint_and_forks_diverge() {
		let actors = ActorManager::new();
		actors.register("unchecked".into(), None);
		assert_eq!(actors.worker_lost("unchecked").unwrap(), ActorRecovery::Lost);

		actors.register("source".into(), Some("checkpoint-1".into()));
		actors.fork("source", "fork".into()).unwrap();
		assert_eq!(actors.worker_lost("source").unwrap(), ActorRecovery::Restore {
			checkpoint_id: "checkpoint-1".into(),
		});
		actors
			.checkpoint_committed("fork", "checkpoint-2".into())
			.unwrap();
		assert_eq!(actors.worker_lost("fork").unwrap(), ActorRecovery::Restore {
			checkpoint_id: "checkpoint-2".into(),
		});
	}

	#[test]
	fn never_silently_resets_lost_actor() {
		let actors = ActorManager::new();
		actors.register("actor".into(), None);
		assert_eq!(actors.worker_lost("actor").unwrap(), ActorRecovery::Lost);
		assert!(actors.fork("actor", "fork".into()).is_err());
	}
}
