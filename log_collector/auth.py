"""Single-admin authentication for collector human-facing routes."""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import time
from collections import defaultdict, deque
from typing import Optional

from aiohttp import web

from .metadata_store import MetadataStore

COOKIE_NAME = "oati_session"


def hash_password(password: str, *, salt: Optional[bytes] = None) -> str:
    if len(password) < 10:
        raise ValueError("Admin password must be at least 10 characters")
    salt = salt or secrets.token_bytes(16)
    key = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return "scrypt$16384$" + base64.urlsafe_b64encode(salt).decode() + "$" + base64.urlsafe_b64encode(key).decode()


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, n_raw, salt_raw, key_raw = encoded.split("$", 3)
        if algorithm != "scrypt":
            return False
        salt = base64.urlsafe_b64decode(salt_raw)
        expected = base64.urlsafe_b64decode(key_raw)
        actual = hashlib.scrypt(
            password.encode("utf-8"), salt=salt, n=int(n_raw), r=8, p=1, dklen=len(expected)
        )
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        return False


class AuthManager:
    def __init__(
        self,
        metadata: MetadataStore,
        *,
        username: str,
        password_hash: str,
        session_hours: int = 12,
        secure_cookie: bool = False,
    ) -> None:
        self.metadata = metadata
        self.username = username
        self.password_hash = password_hash
        self.session_seconds = max(900, int(session_hours) * 3600)
        self.secure_cookie = secure_cookie
        self._failures: dict[str, deque[float]] = defaultdict(deque)

    @property
    def enabled(self) -> bool:
        return bool(self.username and self.password_hash)

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("ascii", errors="ignore")).hexdigest()

    def authenticate(self, username: str, password: str, remote: str) -> Optional[tuple[str, str]]:
        now = time.monotonic()
        attempts = self._failures[remote]
        while attempts and attempts[0] < now - 300:
            attempts.popleft()
        if len(attempts) >= 8:
            return None
        if not hmac.compare_digest(str(username), self.username) or not verify_password(
            str(password), self.password_hash
        ):
            attempts.append(now)
            return None
        attempts.clear()
        token = secrets.token_urlsafe(32)
        csrf = secrets.token_urlsafe(24)
        self.metadata.create_session(
            self._token_hash(token), csrf, int(time.time()) + self.session_seconds
        )
        return token, csrf

    def session(self, request: web.Request) -> Optional[dict]:
        token = request.cookies.get(COOKIE_NAME)
        if not token:
            return None
        return self.metadata.get_session(self._token_hash(token))

    def logout(self, request: web.Request) -> None:
        token = request.cookies.get(COOKIE_NAME)
        if token:
            self.metadata.delete_session(self._token_hash(token))

    def set_cookie(self, response: web.StreamResponse, token: str) -> None:
        response.set_cookie(
            COOKIE_NAME,
            token,
            max_age=self.session_seconds,
            httponly=True,
            secure=self.secure_cookie,
            samesite="Strict",
            path="/",
        )


def auth_middleware(manager: AuthManager):
    @web.middleware
    async def middleware(request: web.Request, handler):
        # Vehicle uploads and login assets must keep working without a browser session.
        if request.path in ("/ingest", "/login"):
            return await handler(request)
        if not manager.enabled:
            if request.method in ("POST", "PUT", "PATCH", "DELETE") and (
                request.path.startswith("/api/base-stations")
                or request.path.startswith("/api/quality-profiles")
            ):
                raise web.HTTPServiceUnavailable(
                    reason="Enable collector authentication before editing metadata"
                )
            return await handler(request)
        session = manager.session(request)
        if session is None:
            if request.path.startswith("/api/"):
                raise web.HTTPUnauthorized(reason="Authentication required")
            raise web.HTTPFound("/login")
        request["admin_session"] = session
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            supplied = request.headers.get("X-CSRF-Token", "")
            if not hmac.compare_digest(supplied, str(session["csrf_token"])):
                raise web.HTTPForbidden(reason="Invalid CSRF token")
        return await handler(request)

    return middleware
