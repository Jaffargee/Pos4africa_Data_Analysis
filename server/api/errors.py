from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from server.domain.errors import (
    ConflictError,
    NotFoundError,
    PosOutcomeUnknownError,
    PosRejectedError,
    PosUnavailableError,
    ServerError,
    ValidationFailed,
)

_STATUS: list[tuple[type[ServerError], int]] = [
    (NotFoundError, 404),
    (ConflictError, 409),
    (ValidationFailed, 422),
    (PosRejectedError, 422),
    (PosUnavailableError, 503),         # nothing was applied: safe to retry with the SAME key
    (PosOutcomeUnknownError, 502),      # may have been applied: retry with the SAME key to learn the result
]


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ServerError)
    async def _handle(_: Request, exc: ServerError) -> JSONResponse:
        status = next((code for cls, code in _STATUS if isinstance(exc, cls)), 500)
        return JSONResponse(status_code=status, content={"error": exc.code, "detail": str(exc)})
