"""Errors the server speaks.  The API layer maps each one to an HTTP status.

The POS errors are split by one question that drives the whole write flow:
*could the write have been applied?*

  PosRejectedError         no  - POS looked at it and said no (retrying won't help)
  PosUnavailableError      no  - nothing was sent / session was logged out (safe to retry)
  PosOutcomeUnknownError   MAYBE - request sent, answer lost or unreadable (never blind-retry)
"""

from __future__ import annotations


class ServerError(Exception):
    code = "server_error"


class NotFoundError(ServerError):
    code = "not_found"


class ConflictError(ServerError):
    code = "conflict"


class ValidationFailed(ServerError):
    code = "validation_failed"


class PosRejectedError(ServerError):
    code = "pos_rejected"


class PosUnavailableError(ServerError):
    code = "pos_unavailable"


class PosSessionExpiredError(PosUnavailableError):
    code = "pos_session_expired"


class PosOutcomeUnknownError(ServerError):
    code = "pos_outcome_unknown"
