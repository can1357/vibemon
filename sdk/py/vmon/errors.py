"""Stable error categories exposed by the Python SDK."""


class APIError(Exception):
    """A structured error response returned by the vmon API."""

    def __init__(self, message: str, *, code: str = "engine", status: int | None = None) -> None:
        super().__init__(message)
        self.message: str = message
        self.code: str = code
        self.status: int | None = status


class TransportError(Exception):
    """A connection or I/O failure that may be retried on another endpoint."""

    def __init__(self, message: str, *, endpoint: str | None = None) -> None:
        super().__init__(message)
        self.message: str = message
        self.endpoint: str | None = endpoint


class ProtocolError(Exception):
    """A malformed HTTP or WebSocket payload received from a vmon endpoint."""


class RemoteFunctionError(RuntimeError):
    """Structured durable function execution failure."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "remote_error",
        remote_type: str = "",
        traceback_text: str = "",
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.remote_type = remote_type
        self.traceback = traceback_text


class ActorLostError(RuntimeError):
    """A durable actor can no longer accept calls.

    This is raised only for terminal actor states (failed, stopped, or
    deleted).  The SDK never hides actor loss by replaying its constructor.
    """

    def __init__(self, actor_id: str, status: str = "lost") -> None:
        super().__init__(f"actor {actor_id!r} is {status}")
        self.actor_id = actor_id
        self.status = status
