"""
connector.py — PosConnector

Responsibilities:
  1. Maintain an authenticated httpx session (login + cookie refresh)
"""

from __future__ import annotations
import asyncio

import httpx

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from pos4africa.config.settings import settings
from pos4africa.shared.utils.retry import with_retry_async
from pos4africa.worker.components.base import BaseComponent
from pos4africa.shared.utils.rate_limiter import RateLimiter

class PosConnector(BaseComponent):
      def __init__(self) -> None:
            super().__init__()
            self._session: httpx.AsyncClient | None = None
            self._rate_limiter = RateLimiter(
                  rps=settings.rate_limit_rps,
                  burst=settings.rate_limit_burst,
            )
            self._user_agent = (
                  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
            )

      @property
      def headers(self) -> dict:
            return {
                  "User-Agent": self._user_agent,
                  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                  "Accept-Language": "en-US,en;q=0.5",
                  "Accept-Encoding": "gzip, deflate, br",
                  "Connection": "keep-alive",
                  "Upgrade-Insecure-Requests": "1",
            }

      # ── Lifecycle ─────────────────────────────────────────────────────────────
      async def __aenter__(self) -> "PosConnector":
            return await self.init()

      async def __aexit__(self, *_: object) -> None:
            await self.close()

      async def init(self) -> PosConnector:
            self._session = httpx.AsyncClient(
                  base_url=settings.pos_base_url,
                  timeout=settings.pos_request_timeout,
                  follow_redirects=True,
                  http2=True,
                  headers=self.headers,
            )
            await self._login()
            return self

      async def close(self) -> None:
            if self._session:
                  await self._session.aclose()

      # ── Auth ──────────────────────────────────────────────────────────────────
      def _selenium_login_sync(self) -> tuple[list[dict], str]:
            """Runs Chrome in a worker thread to solve the JS challenge and log in."""
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument(f"--user-agent={self._user_agent}")

            driver = webdriver.Chrome(options=options)
            try:
                  login_url = f"{settings.pos_base_url}{settings.pos_login_path}"
                  driver.get(login_url)

                  # Wait up to 20s for the 5-second JS verification screen to reload into the login form
                  wait = WebDriverWait(driver, 20)
                  username_input = wait.until(
                        EC.presence_of_element_located((By.NAME, "username"))
                  )
                  password_input = driver.find_element(By.NAME, "password")

                  username_input.clear()
                  username_input.send_keys(settings.pos_username)
                  password_input.clear()
                  password_input.send_keys(settings.pos_password.get_secret_value())

                  # Submit the login form
                  password_input.submit()

                  # Wait until redirected away from the login URL
                  wait.until(lambda d: settings.pos_login_path not in d.current_url)

                  # Capture exact cookies and User-Agent so httpx matches the browser fingerprint
                  cookies = driver.get_cookies()
                  actual_ua = driver.execute_script("return navigator.userAgent;")
                  return cookies, actual_ua
            finally:
                  driver.quit()

      @with_retry_async
      async def _login(self) -> None:
            assert self._session is not None
            cookies, actual_ua = await asyncio.to_thread(self._selenium_login_sync)

            # Sync User-Agent and transfer all clearance + session cookies into httpx
            self._session.headers["User-Agent"] = actual_ua
            self._session.cookies.clear()
            for cookie in cookies:
                  self._session.cookies.set(
                        name=cookie["name"],
                        value=cookie["value"],
                        domain=cookie.get("domain", ""),
                        path=cookie.get("path", "/"),
                  )

            self.log.info("connector.logged_in", cookie_count=len(cookies))

      @property
      def session(self) -> httpx.AsyncClient:
            return self._session
