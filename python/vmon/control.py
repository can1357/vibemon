"""Client for vmon's newline-delimited JSON Unix control socket."""

from __future__ import annotations

import itertools
import json
import socket
import time


class Control:
    """Talks to a running microVM's ``--api-sock`` JSON lifecycle API."""

    _ids = itertools.count(1)

    def __init__(self, sock_path: str):
        self.sock_path = str(sock_path)

    def _request(self, method: str, params: dict | None = None) -> dict:
        request_id = next(self._ids)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(self.sock_path)
            f = s.makefile("rwb", buffering=0)
            banner_line = f.readline()
            if not banner_line:
                raise RuntimeError("control socket closed before banner")
            try:
                banner = json.loads(banner_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise RuntimeError(f"invalid control banner: {e}") from e
            if banner.get("api") != 1:
                raise RuntimeError(f"unsupported control API banner: {banner!r}")

            request = {"id": request_id, "method": method, "params": params}
            f.write(json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n")
            reply_line = f.readline()
            if not reply_line:
                raise RuntimeError("control socket closed before reply")
        try:
            reply = json.loads(reply_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise RuntimeError(f"invalid control reply: {e}") from e
        if reply.get("id") != request_id:
            raise RuntimeError(f"control reply id mismatch: expected {request_id}, got {reply.get('id')!r}")
        if not reply.get("ok", False):
            error = reply.get("error") or {}
            code = error.get("code", "internal")
            message = error.get("message", "control request failed")
            raise RuntimeError(f"{code}: {message}")
        result = reply.get("result")
        return result if isinstance(result, dict) else {}

    def wait_ready(self, timeout: float = 10.0) -> None:
        """Block until the control socket accepts a ping round-trip."""
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                self.ping()
                return
            except (OSError, RuntimeError) as e:
                last = e
                time.sleep(0.005)
        raise TimeoutError(f"control socket {self.sock_path} not ready: {last}")

    def ping(self) -> dict:
        return self._request("ping")

    def info(self) -> dict:
        return self._request("info")

    def pause(self) -> dict:
        return self._request("pause")

    def resume(self) -> dict:
        return self._request("resume")

    def snapshot(self, name: str, base: str | None = None) -> dict:
        """Snapshot guest state into ``name``; with ``base`` write a delta against it."""
        params: dict[str, str] = {"name": name}
        if base is not None:
            params["base"] = base
        return self._request("snapshot", params)

    def extend(self, secs: int) -> dict:
        """Reset the VMM-enforced wall-clock deadline to ``secs`` from now."""
        return self._request("extend", {"secs": int(secs)})

    def metrics(self) -> dict:
        return self._request("metrics")

    def quit(self) -> dict:
        return self._request("quit")
