from __future__ import annotations

import hashlib
import json

from pydantic import BaseModel


def request_hash(model: BaseModel, **extra: object) -> str:
    """Canonical hash of a request body (plus any path parameters passed as extras)."""
    body = {"body": model.model_dump(mode="json"), **extra}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
