"""Bearer-token helpers for HTTP and WebSocket server routes."""

from __future__ import annotations

import re
import secrets
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Request, WebSocket, status
from fastapi.security import HTTPAuthorizationCredentials

from ..core import VMRecord

if TYPE_CHECKING:
    from .runtime import ServerRuntime

def _tokens_match(supplied: str | None, expected: str) -> bool:
    return supplied is not None and secrets.compare_digest(supplied, expected)


def _token_set(value: str | None) -> frozenset[str]:
    if value is None:
        return frozenset()
    return frozenset(token for token in (part.strip() for part in value.split(",")) if token)


def _primary_token(value: str | None) -> str:
    for part in (value or "").split(","):
        token = part.strip()
        if token:
            return token
    return ""


def _token_matches_any(supplied: str | None, expected: frozenset[str]) -> bool:
    matched = False
    for token in expected:
        matched = _tokens_match(supplied, token) or matched
    return matched


def _bearer_token_authorized(
    supplied: str | None, expected_token: str | None, client_token: str | None = None
) -> bool:
    full_tokens = _token_set(expected_token)
    client_tokens = _token_set(client_token)
    if not full_tokens and not client_tokens:
        return True
    return _token_matches_any(supplied, full_tokens) or _token_matches_any(supplied, client_tokens)


def _ws_bearer_token(websocket: WebSocket) -> str | None:
    """Extract a bearer token from the Authorization header or a token query param."""
    header = websocket.headers.get("authorization")
    if header:
        scheme, _, value = header.partition(" ")
        if scheme.lower() == "bearer" and value:
            return value.strip()
    params = websocket.query_params
    return params.get("token") or params.get("access_token")


def _request_bearer_token(request: Request) -> str | None:
    header = request.headers.get("authorization")
    if not header:
        return None
    scheme, _, value = header.partition(" ")
    return value.strip() if scheme.lower() == "bearer" and value else None


_ADMIN_MIGRATE_PATH = re.compile(r"^/v1/sandboxes/[^/]+/migrate/?$")


def _is_admin_path(path: str) -> bool:
    return path.startswith("/v1/mesh/") or bool(_ADMIN_MIGRATE_PATH.fullmatch(path))


def _client_token_only(
    supplied: str | None, expected_token: str | None, client_token: str | None
) -> bool:
    client_tokens = _token_set(client_token)
    if not client_tokens:
        return False
    return _token_matches_any(supplied, client_tokens) and not _token_matches_any(
        supplied, _token_set(expected_token)
    )


def _request_connect_token(request: Request) -> str | None:
    token = request.query_params.get("token") or request.query_params.get("access_token")
    if token:
        return token
    header = request.headers.get("authorization")
    if header:
        scheme, _, value = header.partition(" ")
        if scheme.lower() == "bearer" and value:
            return value.strip()
    return None


def _ws_connect_token(websocket: WebSocket) -> str | None:
    token = websocket.query_params.get("token") or websocket.query_params.get("access_token")
    if token:
        return token
    return _ws_bearer_token(websocket)

def _require_bearer(
    request: Request, expected_token: str | None, client_token: str | None = None
) -> None:
    if not _bearer_token_authorized(_request_bearer_token(request), expected_token, client_token):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail={"code": "unauthorized", "message": "unauthorized"},
            headers={"WWW-Authenticate": "Bearer"},
        )


def make_require_auth(ctx: ServerRuntime):
    bearer_dependency = Depends(ctx.bearer)
    async def require_auth(
        credentials: HTTPAuthorizationCredentials | None = bearer_dependency,
    ) -> None:
        supplied = (
            credentials.credentials
            if credentials is not None and credentials.scheme.lower() == "bearer"
            else None
        )
        if not _bearer_token_authorized(supplied, ctx.expected_token, ctx.client_token):
            ctx.supervisor.count("auth_failed")
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                detail={"code": "unauthorized", "message": "unauthorized"},
                headers={"WWW-Authenticate": "Bearer"},
            )

    return require_auth


def require_connect_token(ctx: ServerRuntime, record: VMRecord, supplied: str | None) -> None:
    expected = ctx.supervisor.connect_token(record, create=False)
    if expected is None or not _tokens_match(supplied, expected):
        ctx.supervisor.count("auth_failed")
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail={"code": "unauthorized", "message": "invalid connect token"},
            headers={"WWW-Authenticate": "Bearer"},
        )
