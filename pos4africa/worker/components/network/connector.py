"""
connector.py — PosConnector

Responsibilities:
  1. Maintain an authenticated httpx session (login + cookie refresh)
"""

from __future__ import annotations

import httpx

from pos4africa.config.settings import settings
from pos4africa.shared.utils.retry import with_retry_async
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.utils.rate_limiter import RateLimiter


class PosConnector(BaseComponent):
      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id, memory=memory)
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
