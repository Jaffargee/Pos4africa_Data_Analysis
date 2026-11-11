from __future__ import annotations

import re


def get_client():
    """The shared Supabase client from the ETL package (lazy: needs credentials)."""
    from pos4africa.infra.supabase_client import spb_client

    return spb_client


_UNSAFE = re.compile(r"[^\w\s.\-']", re.UNICODE)


def safe_term(q: str | None) -> str | None:
    """Make free text safe to splice into a PostgREST ``or=(...ilike...)`` filter
    (commas, parentheses, wildcards and backslashes have meaning there)."""
    if not q:
        return None
    cleaned = _UNSAFE.sub(" ", q)
    cleaned = " ".join(cleaned.split())
    return cleaned or None
