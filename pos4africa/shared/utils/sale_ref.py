"""Idempotency reference helpers shared by the ETL and the server.

The server writes a short reference into the POS ``comment`` field of every
sale it creates (``"<comment> ~R:AB12CD34EF"``).  That ref is how a sale that
was created in POS can be recognised later, even if the HTTP reply was lost.
The ETL strips it again so analytics never see it.

Pure functions only: no settings, no I/O, safe to import anywhere.
"""

from __future__ import annotations

import hashlib
import re

_REF_RE = re.compile(r"\s*~R:([0-9A-Fa-f]{10})(?![0-9A-Za-z])")


def ref_from_key(kind: str, key: str) -> str:
    """Deterministic 10-hex ref for (operation kind, client idempotency key)."""
    return hashlib.sha256(f"{kind}:{key}".encode()).hexdigest()[:10].upper()


def attach_ref(comment: str | None, ref: str) -> str:
    base = (comment or "").strip()
    return f"{base} ~R:{ref}".strip()


def extract_ref(comment: str | None) -> str | None:
    match = _REF_RE.search(comment or "")
    return match.group(1).upper() if match else None


def strip_ref(comment: str | None) -> str | None:
    if comment is None:
        return None
    cleaned = _REF_RE.sub("", comment).strip()
    return cleaned or None
