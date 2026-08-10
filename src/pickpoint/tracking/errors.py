from __future__ import annotations

from .v2 import ErrorCode, messages


class Error(Exception):
    """Typed tracking protocol / SDK error."""

    def __init__(self, code: int, message: str = "") -> None:
        self.code = code
        self.message = message
        super().__init__(self._format())

    def _format(self) -> str:
        if self.message:
            return f"tracking: {self.message} ({self.code})"
        return f"tracking: {self.code}"


def new_error(code: int, message: str = "") -> Error:
    return Error(code, message)


def error_from_wire(err: messages.Error | None) -> Error:
    if err is None:
        return new_error(ErrorCode.ERROR_CODE_INVALID, "unknown error")
    return new_error(err.code, err.message)


def is_fatal_resume_error(code: int) -> bool:
    return code in (
        ErrorCode.ERROR_CODE_TRACK_NOT_FOUND,
        ErrorCode.ERROR_CODE_FENCED,
        ErrorCode.ERROR_CODE_AUTH,
        ErrorCode.ERROR_CODE_UNAUTHORIZED,
    )


def is_auth_error(code: int) -> bool:
    return code in (ErrorCode.ERROR_CODE_AUTH, ErrorCode.ERROR_CODE_UNAUTHORIZED)
