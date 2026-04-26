"""
connector.py — PosConnector

Responsibilities:
  1. Maintain an authenticated httpx session (login + cookie refresh)
  2. Paginate through the sales listing for a given date range
  3. Write each raw HTML page into short-term memory
  4. Respect the per-node rate limiter
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from uuid import uuid4

import httpx

from pos4africa.config.settings import settings
from pos4africa.shared.utils.retry import with_retry_async
from pos4africa.worker.components.base import BaseComponent
from pos4africa.worker.components.rate_limiter import RateLimiter
from pos4africa.manager.memory.store import MemoryStore


class PosConnector(BaseComponent):
      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id, memory)
            self._session: httpx.AsyncClient | None = None
            self._rate_limiter = RateLimiter(
                  rps=settings.rate_limit_rps,
                  burst=settings.rate_limit_burst,
            )

      # ── Lifecycle ─────────────────────────────────────────────────────────────

      async def __aenter__(self) -> "PosConnector":
            headers = {
                  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                  "Accept-Language": "en-US,en;q=0.5",
                  "Accept-Encoding": "gzip, deflate, br",
                  "Connection": "keep-alive",
                  "Upgrade-Insecure-Requests": "1",
            }
            self._session = httpx.AsyncClient(
                  base_url=settings.pos_base_url,
                  timeout=settings.pos_request_timeout,
                  follow_redirects=True,
                  http2=True,
                  headers=headers,
            )
            await self._login()
            return self

      async def __aexit__(self, *_: object) -> None:
            if self._session:
                  await self._session.aclose()

      # ── Auth ──────────────────────────────────────────────────────────────────

      @with_retry_async
      async def _login(self) -> None:
            assert self._session is not None
            headers = {
                  "Content-Type": "application/x-www-form-urlencoded",
                  "Origin": settings.pos_base_url,
                  "Referer": f"{settings.pos_base_url}{settings.pos_login_path}",
            }
            resp = await self._session.post(
                  f"{settings.pos_login_path}",
                  data={
                        "username": settings.pos_username,
                        "password": settings.pos_password.get_secret_value(),
                  },
                  headers=headers
            )
            resp.raise_for_status()
            self.log.info("connector.logged_in", status=resp.status_code)

      @property
      def session(self) -> httpx.AsyncClient:
            return self._session
      
      # ── Main interface ────────────────────────────────────────────────────────

      async def run(self, pos_sale_id: int) -> str:
            await self._rate_limiter.acquire()
            return await self._fetch_page(pos_sale_id)


      @with_retry_async
      async def _fetch_page(self, pos_sale_id: int) -> str:
            assert self._session is not None
            
            url = f"{settings.pos_sales_path}/{pos_sale_id}"
            
            # ✅ Let httpx handle decompression automatically
            resp = await self._session.get(url)
                 
            # Re-login on session expiry
            if resp.status_code in (401, 403):
                  self.log.warning("connector.session_expired", status=resp.status_code)
                  await self._login()
                  resp = await self._session.get(url)
            
            resp.raise_for_status()
            
            # ✅ Use resp.text - httpx automatically decompresses gzip
            html = resp.text
            
            # ✅ Debug: Check if we got valid HTML
            if not html or len(html) < 100:
                  self.log.error("connector.empty_or_short_response", length=len(html))
                  # Try manual decompression as fallback
                  import gzip
                  try:
                        decompressed = gzip.decompress(resp.content)
                        html = decompressed.decode('utf-8')
                        self.log.info("connector.manual_decompress_success", length=len(html))
                  except Exception as e:
                        self.log.error("connector.decompress_failed", error=str(e))
                        return ""
            
            # ✅ Check if it's actually HTML
            if not html.strip().startswith('<!DOCTYPE') and '<html' not in html.lower():
                  self.log.warning("connector.not_html", preview=html[:200])
            
            # ✅ Save first few for debugging
            if pos_sale_id % 1000 == 0:  # Save every 1000th
                  with open(f"debug_sale_{pos_sale_id}.html", "w", encoding="utf-8") as f:
                        f.write(html)
            
            self.log.info("connector.page_fetched", pos_sale_id=pos_sale_id, length=len(html))
            return html


      @staticmethod
      def _is_empty_page(html: str) -> bool:
            """Heuristic: if no table rows, we've gone past the last page."""
            return "<tr" not in html or "no records" in html.lower()