"""Per-app dependency bundle passed to route registration modules."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi.security import HTTPBearer

from ..config import ServeConfig
from ..lease import LeaseManager
from ..mesh import Mesh
from ..records import RecordStore
from ..replica import ReplicaStore
from .supervisor import Supervisor


@dataclass(slots=True)
class ServerRuntime:
    supervisor: Supervisor
    mesh: Mesh
    replica_store: ReplicaStore
    record_store: RecordStore
    lease_manager: LeaseManager
    expected_token: str | None
    outbound_token: str | None
    client_token: str | None
    bearer: HTTPBearer
    config: ServeConfig
