from __future__ import annotations

from .types import ErrorCode, WireError


class Error(Exception):
    """Typed tracking protocol / SDK error."""

    def __init__(
        self,
        code: int,
        message: str = "",
        *,
        retry_after_ms: int = 0,
        track_uid: str = "",
    ) -> None:
        self.code = code
        self.message = message
        self.retry_after_ms = retry_after_ms
        self.track_uid = track_uid
        super().__init__(self._format())

    def _format(self) -> str:
        if self.message:
            return f"tracking: {self.message} ({self.code})"
        return f"tracking: {self.code}"


def new_error(code: int, message: str = "") -> Error:
    return Error(code, message)


def error_from_wire(err: WireError | None) -> Error:
    if err is None:
        return new_error(ErrorCode.INVALID, "unknown error")
    return Error(
        int(err.code),
        err.message,
        retry_after_ms=err.retry_after_ms,
        track_uid=err.track_uid,
    )


def is_fatal_resume_error(code: int) -> bool:
    """Fatal for Resume: AUTH and TRACK_NOT_FOUND only."""
    return code in (ErrorCode.AUTH, ErrorCode.TRACK_NOT_FOUND)


def is_retry_resume_error(code: int) -> bool:
    return code in (ErrorCode.FENCED, ErrorCode.TRY_AGAIN)


def is_auth_error(code: int) -> bool:
    return code in (ErrorCode.AUTH, ErrorCode.UNAUTHORIZED)
