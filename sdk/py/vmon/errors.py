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
    """A remote function call failed in its sandbox for infrastructure reasons.

    Raised for session/protocol failures and for remote exceptions that cannot
    be reconstructed locally; reconstructable remote exceptions re-raise as
    their original type instead.
    """

    def __init__(
        self,
        message: str,
        *,
        remote_type: str = "",
        traceback_text: str = "",
    ) -> None:
        super().__init__(message)
        self.remote_type = remote_type
        self.traceback = traceback_text
