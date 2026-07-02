from __future__ import annotations

import contextlib
import os
import secrets
import shutil
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("VMON_CLUSTER_E2E") != "1",
    reason="set VMON_CLUSTER_E2E=1 to run the SDK cluster failover e2e",
)


def test_sdk_connect_exec_survives_first_gateway_loss(monkeypatch: pytest.MonkeyPatch) -> None:
    """P1 exit criterion: from a machine outside the mesh, the SDK creates a
    sandbox, exec streams, and both survive killing the gateway the client
    first contacted (HA restores the sandbox on the survivor)."""
    from cluster_harness import (
        api_json,
        eventually,
        free_port,
        kill_node,
        mesh_join,
        mesh_setup,
        restart_node,
        sandbox_row,
        start_node,
        wait_healthy,
    )

    from vmon.remote import connect

    token = "sdk-cluster-e2e-" + secrets.token_urlsafe(24)
    # Short /tmp homes: macOS $TMPDIR paths overflow SUN_LEN for the per-VM
    # control/agent Unix sockets under $VMON_HOME.
    home_a = Path(tempfile.mkdtemp(prefix="vsdk-", dir="/tmp"))
    home_b = Path(tempfile.mkdtemp(prefix="vsdk-", dir="/tmp"))
    node_a = start_node(
        home_a,
        free_port(),
        {"VMON_REPLICATE_SEC": "1"},
        name="sdk-node-a",
        token=token,
        log_path=Path("/tmp/sdk-a.log"),
    )
    node_b = start_node(
        home_b,
        free_port(),
        {"VMON_REPLICATE_SEC": "1"},
        name="sdk-node-b",
        token=token,
        log_path=Path("/tmp/sdk-b.log"),
    )
    sandbox_id = f"sdk-e2e-{os.getpid()}"
    image = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")

    try:
        setup = mesh_setup(node_a)
        join = mesh_join(node_b, str(setup["blob"]))
        node_a_id = str(setup["node_id"])
        node_b_id = str(join["node_id"])
        wait_healthy([node_a, node_b], timeout=60.0)
        # Mesh setup persists membership; restart so HA replication/reconcile
        # tasks run from lifespan startup on both gateways.
        restart_node(node_b)
        restart_node(node_a)
        wait_healthy([node_a, node_b], timeout=60.0)

        # Warm the image template on the ingress node so the SDK create below
        # is not subject to cold image-import latency; the SDK path itself is
        # exercised against the warm template.
        warm_id = f"{sandbox_id}-warm"
        api_json(
            "POST",
            node_a.url,
            "/v1/run?detach=1",
            token,
            payload={
                "image": image,
                "name": warm_id,
                "cmd": ["sleep", "60"],
                "block_network": True,
                "timeout": 300,
                "idempotency_key": f"{warm_id}-run",
            },
            timeout=300.0,
        )
        api_json("DELETE", node_a.url, f"/v1/sandboxes/{warm_id}/remove", token, timeout=60.0)

        monkeypatch.setenv("VMON_API_TOKEN", token)
        client = connect(endpoints=[node_a.url, node_b.url], token=token)
        sandbox = client.sandboxes.create(
            image=image,
            name=sandbox_id,
            block_network=True,
            timeout=900,
            cmd=["sh", "-c", "sleep 600"],
        )

        # Idempotent creates are placed by the rendezvous coordinator for the
        # key, so the owner may be either node. Rebind the SDK roster so its
        # FIRST endpoint is the owner: killing the owner then kills exactly the
        # gateway the client first contacted (the P1 exit criterion), and HA
        # must restore the sandbox on the survivor for exec to keep working.
        owner_row = sandbox_row(
            api_json("GET", node_a.url, "/v1/sandboxes", token, timeout=10.0), sandbox_id
        )
        assert owner_row is not None, "created sandbox missing from mesh listing"
        if owner_row.get("node") == node_a_id:
            owner, survivor, survivor_id = node_a, node_b, node_b_id
        else:
            owner, survivor, survivor_id = node_b, node_a, node_a_id
        client = connect(endpoints=[owner.url, survivor.url], token=token)
        sandbox = client.sandboxes.get(sandbox_id)

        first = sandbox.exec("cat", "/etc/hostname", _track_entry=False)
        assert first.wait(timeout=30) == 0
        assert first.stdout.read()

        eventually(
            f"{survivor_id} to hold a replica for {sandbox_id}",
            lambda: (
                sandbox_id
                in api_json(
                    "GET", survivor.url, "/v1/mesh/replica/list", token, timeout=5.0
                ).get("sids", [])
            ),
            timeout=90.0,
            interval=1.0,
        )

        kill_node(owner)
        eventually(
            f"{sandbox_id} to be restored and running on {survivor_id}",
            lambda: (
                (row := sandbox_row(
                    api_json("GET", survivor.url, "/v1/sandboxes", token, timeout=5.0),
                    sandbox_id,
                ))
                is not None
                and row.get("status") == "running"
                and row.get("node") == survivor_id
            ),
            timeout=120.0,
            interval=2.0,
        )

        # The client's first endpoint (the owner it first contacted) is dead;
        # the SDK must fail over to the survivor for the sandbox it created.
        second = sandbox.exec("cat", "/etc/hostname", _track_entry=False)
        assert second.wait(timeout=60) == 0
        assert second.stdout.read()
    finally:
        for node in (node_a, node_b):
            with contextlib.suppress(Exception):
                connect(endpoints=[node.url], token=token).sandboxes.get(sandbox_id).terminate()
                break
        for node in (node_a, node_b):
            with contextlib.suppress(Exception):
                kill_node(node)
        for home in (home_a, home_b):
            shutil.rmtree(home, ignore_errors=True)
