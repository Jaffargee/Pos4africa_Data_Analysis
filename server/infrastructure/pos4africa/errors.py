from __future__ import annotations

import httpx

from server.domain.errors import (
    PosOutcomeUnknownError,
    PosRejectedError,
    PosUnavailableError,
    ServerError,
)

# Failures that happen before a byte of the request is sent: nothing can have been applied.
_NOT_SENT = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)


def classify_http_error(exc: Exception) -> ServerError:
    """Turn a transport/HTTP failure on a *write* into the right recovery category."""
    if isinstance(exc, ServerError):
        return exc
    if isinstance(exc, _NOT_SENT):
        return PosUnavailableError(f"POS unreachable: {exc!r}")
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if 400 <= code < 500:
            return PosRejectedError(f"POS rejected the request (HTTP {code})")
        return PosOutcomeUnknownError(f"POS answered HTTP {code}; the write may or may not have applied")
    return PosOutcomeUnknownError(f"POS call failed after sending: {exc!r}")


def classify_pre_commit_error(exc: Exception) -> ServerError:
    """Same, for cart-building steps that precede the final 'complete': nothing is
    committed yet, so any failure there is safe to retry."""
    if isinstance(exc, ServerError):
        return exc
    if isinstance(exc, httpx.HTTPStatusError) and 400 <= exc.response.status_code < 500:
        return PosRejectedError(f"POS rejected a cart step (HTTP {exc.response.status_code})")
    return PosUnavailableError(f"cart step failed before completing the sale: {exc!r}")
