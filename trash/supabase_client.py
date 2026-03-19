"""Supabase client helpers.

This module provides a single Supabase client instance that can be reused
across the project, plus a small convenience helper for common lookups.

The client is created once at import time and reads configuration from the
environment variables defined in `.env` (SUPABASE_URL and SUPABASE_KEY).
"""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from supabase import Client, create_client


load_dotenv()  # load values from .env into os.environ


def _get_supabase_config() -> tuple[str, str]:
      url = os.getenv("SUPABASE_URL")
      key = os.getenv("SUPABASE_KEY")
      if not url or not key:
            raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY must be set in the environment. "
            "Set them in .env or export them before running."
            )
      return url, key


_SUPABASE_URL, _SUPABASE_KEY = _get_supabase_config()

# Reuse a single client instance across the project.
supabase: Client = create_client(_SUPABASE_URL, _SUPABASE_KEY)

