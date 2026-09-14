
import json
import time
from pathlib import Path
from typing import Any
from infra.supabase_client import spb_client

# Resolve path relative to script location
CACHE_FILE = Path(__file__).parent / "supabase_cache.json"
CACHE_TTL_SECONDS = 3600  # 1 hour cache validity


def _fetch_and_cache_revenue() -> list[dict | Any]:
      """Fetch fresh daily revenue data from Supabase and persist to local cache."""
      response = spb_client.table("v_revenue_daily").select("*").execute()
      data = response.data or []

      try:
            with open(CACHE_FILE, "w") as cache_file:
                  json.dump(data, cache_file, indent=4)
      except IOError as e:
            print(f"Warning: Failed to write cache: {e}")

      return data


def _read_cache() -> list[dict] | None:
      """Read data from cache if the file exists and hasn't expired."""
      if not CACHE_FILE.exists():
            return None

      # Cache expiration check
      file_age = time.time() - CACHE_FILE.stat().st_mtime
      if file_age > CACHE_TTL_SECONDS:
            return None

      try:
            with open(CACHE_FILE, "r") as cache_file:
                  return json.load(cache_file)
      except (json.JSONDecodeError, IOError):
            return None


def get_daily_revenue() -> list[dict]:
      """Get daily revenue data with cache-first fallback."""
      cached_data = _read_cache()
      if cached_data is not None:
            return cached_data

      return _fetch_and_cache_revenue()
