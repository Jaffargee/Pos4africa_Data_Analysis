"""
circuit_breaker.py — async-safe circuit breaker.

States:
  CLOSED    → normal operation, failures are counted
  OPEN      → fast-fail, no calls allowed until recovery_timeout elapses
  HALF_OPEN → one probe call allowed; success → CLOSED, failure → OPEN

Usage:
    cb = CircuitBreaker(name="supabase")

    async with cb:
        await supabase.upsert(...)
"""

from __future__ import annotations

import asyncio
import time
from enum import StrEnum

from pos4africa.config.settings import settings
from pos4africa.shared.utils.logger import get_logger

log = get_logger(__name__)


class CBState(StrEnum):
      CLOSED = "closed"
      OPEN = "open"
      HALF_OPEN = "half_open"


class CircuitBreakerOpen(Exception):
      """Raised when a call is attempted while the circuit is OPEN."""


class CircuitBreaker:
      def __init__(self, name: str, failure_threshold: int | None = None, recovery_timeout: float | None = None) -> None:
            self.name = name
            self._threshold = failure_threshold or settings.cb_failure_threshold
            self._recovery_timeout = recovery_timeout or settings.cb_recovery_timeout

            self._state = CBState.CLOSED
            self._failure_count = 0
            self._opened_at: float | None = None
            self._lock = asyncio.Lock()

      @property
      def state(self) -> CBState:
            return self._state

      async def __aenter__(self) -> "CircuitBreaker":
            async with self._lock:
                  if self._state == CBState.OPEN:
                        elapsed = time.monotonic() - (self._opened_at or 0)
                        if elapsed >= self._recovery_timeout:
                              log.info("circuit_breaker.half_open", name=self.name)
                              self._state = CBState.HALF_OPEN
                  else:
                        raise CircuitBreakerOpen(
                              f"Circuit '{self.name}' is OPEN. "
                              f"Retry in {self._recovery_timeout - elapsed:.1f}s"
                        )
            return self

      async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: object,
      ) -> bool:
            async with self._lock:
                  if exc_type is not None and exc_type is not CircuitBreakerOpen:
                        await self._on_failure()
                  else:
                        await self._on_success()
            return False  # don't suppress exceptions

      async def _on_failure(self) -> None:
            self._failure_count += 1
            log.warning(
                  "circuit_breaker.failure",
                  name=self.name,
                  count=self._failure_count,
                  threshold=self._threshold,
            )
            if self._failure_count >= self._threshold:
                  self._state = CBState.OPEN
                  self._opened_at = time.monotonic()
                  log.error("circuit_breaker.opened", name=self.name)

      async def _on_success(self) -> None:
            if self._state in (CBState.HALF_OPEN, CBState.OPEN):
                  log.info("circuit_breaker.closed", name=self.name)
            self._state = CBState.CLOSED
            self._failure_count = 0
            self._opened_at = None