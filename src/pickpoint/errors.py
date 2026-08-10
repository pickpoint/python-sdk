from __future__ import annotations


class PickpointError(Exception):
    """Base SDK error."""


class AuthError(PickpointError):
    """Auth failed (401 / 402 / 403 / refresh failed)."""


class NotFoundError(PickpointError):
    """Resource not found (404)."""


class ConflictError(PickpointError):
    """Conflict (409)."""


class InvalidConfigError(PickpointError):
    """Invalid client configuration."""


class APIError(PickpointError):
    """Non-2xx public-api response (or transport failure after retries)."""

    def __init__(
        self,
        *,
        status: int = 0,
        code: str = "",
        message: str = "",
        body: bytes | bytearray | memoryview | str = b"",
    ) -> None:
        self.status = status
        self.code = code
        if isinstance(body, str):
            self.body = body.encode()
        else:
            self.body = bytes(body)
        self.message = message or f"request failed (status={status} code={code})"
        super().__init__(f"pickpoint: {self.message} (status={status} code={code})")

    def is_auth(self) -> bool:
        return self.code in ("API_AUTH", "REFRESH_FAILED")

    def is_not_found(self) -> bool:
        return self.code == "NOT_FOUND"

    def is_conflict(self) -> bool:
        return self.code == "CONFLICT"

    def is_invalid_config(self) -> bool:
        return self.code == "INVALID_CONFIG"
