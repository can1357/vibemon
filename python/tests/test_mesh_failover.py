"""Real-socket MeshClient failover integration tests using stub HTTP servers.

These tests exercise the real urllib-backed client and CLI against lightweight
stdlib HTTP servers. They are not a hypervisor multi-host end-to-end test.
"""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager, suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from click.testing import CliRunner

from vmon import cli
from vmon.client import DaemonError, MeshClient
from vmon.context import ContextStore, contexts_path


def _handler(node_id: str, roster: list[str]) -> type[BaseHTTPRequestHandler]:
    class H(BaseHTTPRequestHandler):
        def log_message(self, *args: object) -> None:
            pass

        def _send(self, obj: object, code: int = 200) -> None:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            if self.path == "/healthz":
                return self._send({})
            if self.path.startswith("/v1/mesh/status"):
                return self._send(
                    {
                        "node_id": node_id,
                        "self": {"advertise": roster[0]},
                        "peers": [{"advertise": peer, "healthy": True} for peer in roster[1:]],
                    }
                )
            if self.path.startswith("/v1/sandboxes"):
                return self._send(
                    {
                        "sandboxes": [
                            {
                                "id": "sb",
                                "status": "running",
                                "pid": 1,
                                "source": "img",
                                "node": node_id,
                            }
                        ]
                    }
                )
            self._send({}, 404)

        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(content_length)
            self._send({"served_by": node_id})

    return H


def _stop_server(srv: ThreadingHTTPServer) -> None:
    with suppress(Exception):
        srv.shutdown()
    with suppress(Exception):
        srv.server_close()


@contextmanager
def server(node_id: str, roster: list[str]):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _handler(node_id, roster))
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    roster.insert(0, url)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield url, srv
    finally:
        _stop_server(srv)
        thread.join(timeout=2)


def test_meshclient_fails_over_to_live_socket() -> None:
    with server("A", []) as (url_a, srv_a), server("B", []) as (url_b, _srv_b):
        captured: list[list[str]] = []
        mc = MeshClient([url_a, url_b], "tok", on_roster=captured.append)

        assert mc.call("ps")["vms"][0]["node"] == "A"

        _stop_server(srv_a)

        assert mc.call("ps")["vms"][0]["node"] == "B"
        assert captured[-1] == [url_b, url_a]


def test_meshclient_all_endpoints_down_raises() -> None:
    with server("A", []) as (dead_url, srv):
        _stop_server(srv)

        with pytest.raises(DaemonError) as excinfo:
            MeshClient([dead_url], "tok").call("ps")

        assert excinfo.value.code == "unreachable"


def test_meshclient_single_live_probes_then_runs_once() -> None:
    with server("A", []) as (dead_url, srv_a), server("B", []) as (url_b, _srv_b):
        _stop_server(srv_a)

        result = MeshClient([dead_url, url_b], "tok").call("extend", name="x", secs=5)

        assert result == {"served_by": "B"}


def test_context_create_over_real_socket(tmp_path, monkeypatch) -> None:
    roster_a: list[str] = []
    with server("A", roster_a) as (url_a, _srv_a), server("B", []) as (url_b, _srv_b):
        roster_a.append(url_b)
        monkeypatch.setenv("VMON_HOME", str(tmp_path))
        monkeypatch.setenv("VMON_API_TOKEN", "tok")
        runner = CliRunner()

        result = runner.invoke(cli.cli, ["context", "create", "prod", "--server", url_a])

        assert result.exit_code == 0, result.output
        store = ContextStore(contexts_path(tmp_path))
        store.load()
        ctx = store.get("prod")
        assert ctx is not None
        assert ctx.endpoints == [url_a, url_b]
