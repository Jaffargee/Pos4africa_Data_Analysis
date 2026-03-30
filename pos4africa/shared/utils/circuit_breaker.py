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
from typing import Optional

from pos4africa.config.settings import settings
from pos4africa.shared.utils.logger import get_logger

log = get_logger(__name__)


# class CBState(StrEnum):
#       CLOSED = "closed"
#       OPEN = "open"
#       HALF_OPEN = "half_open"


# class CircuitBreakerOpen(Exception):
#       """Raised when a call is attempted while the circuit is OPEN."""


# class CircuitBreaker:
#       def __init__(self, name: str, failure_threshold: int | None = None, recovery_timeout: float | None = None) -> None:
#             self.name = name
#             self._threshold = failure_threshold or settings.cb_failure_threshold
#             self._recovery_timeout = recovery_timeout or settings.cb_recovery_timeout

#             self._state = CBState.CLOSED
#             self._failure_count = 0
#             self._opened_at: float | None = None
#             self._lock = asyncio.Lock()

#       @property
#       def state(self) -> CBState:
#             return self._state

#       async def __aenter__(self) -> "CircuitBreaker":
#             async with self._lock:
#                   if self._state == CBState.OPEN:
#                         elapsed = time.monotonic() - (self._opened_at or 0)
#                         if elapsed >= self._recovery_timeout:
#                               log.info("circuit_breaker.half_open", name=self.name)
#                               self._state = CBState.HALF_OPEN
#                   else:0
#                         raise CircuitBreakerOpen(
#                               f"Circuit '{self.name}' is OPEN. "
#                               f"Retry in {self._recovery_timeout - elapsed:.1f}s"
#                         )
#             return self

#       async def __aexit__(
#             self,
#             exc_type: type[BaseException] | None,
#             exc_val: BaseException | None,
#             exc_tb: object,
#       ) -> bool:
#             async with self._lock:
#                   if exc_type is not None and exc_type is not CircuitBreakerOpen:
#                         await self._on_failure()
#                   else:
#                         await self._on_success()
#             return False  # don't suppress exceptions

#       async def _on_failure(self) -> None:
#             self._failure_count += 1
#             log.warning(
#                   "circuit_breaker.failure",
#                   name=self.name,
#                   count=self._failure_count,
#                   threshold=self._threshold,
#             )
#             if self._failure_count >= self._threshold:
#                   self._state = CBState.OPEN
#                   self._opened_at = time.monotonic()
#                   log.error("circuit_breaker.opened", name=self.name)

#       async def _on_success(self) -> None:
#             if self._state in (CBState.HALF_OPEN, CBState.OPEN):
#                   log.info("circuit_breaker.closed", name=self.name)
#             self._state = CBState.CLOSED
#             self._failure_count = 0
#             self._opened_at = None


class CBState(StrEnum):
      CLOSED = "closed"
      OPEN = "open"
      HALF_OPEN = "half_open"
      
class CircuitBreakerOpen(Exception):
      """Raised when a call is attempted while the circuit is OPEN."""
      
class CircuitBreakerConfig:
      def __init__(
            self,
            failure_threshold: int = settings.cb_failure_threshold,
            recovery_timeout: float = settings.cb_recovery_timeout,
            half_open_max_calls: int = 3,
            half_open_success_threshold: int = 2,
            rolling_window: int = 100,
            timeout_seconds: Optional[float] = 10.0,
            exclude_exceptions: Optional[tuple[type[BaseException], ...]] = None,
            include_exceptions: Optional[tuple[type[BaseException], ...]] = None,
            on_state_change: Optional[callable[[CBState], None]] = None,
            on_failure: Optional[callable[[BaseException], None]] = None,
            on_success: Optional[callable[[], None]] = None,
            health_check: Optional[Callable] = None,
            retry_enabled: bool = True,
            retry_max_attempts: int = 3,
            retry_backoff_factor: float = 1.0,
            retry_max_delay: float = 10.0
            
      ) -> None:
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self.half_open_max_calls = half_open_max_calls
            self.half_open_success_threshold = half_open_success_threshold
            self.rolling_window = rolling_window
            self.timeout_seconds = timeout_seconds
            self.exclude_exceptions = exclude_exceptions or ()
            self.include_exceptions = include_exceptions or ()
            self.on_state_change = on_state_change
            self.on_failure = on_failure
            self.on_success = on_success
            self.health_check = health_check
            self.retry_enabled = retry_enabled
            self.retry_max_attempts = retry_max_attempts
            self.retry_backoff_factor = retry_backoff_factor
            self.retry_max_delay = retry_max_delay
      
      def update(self, **kwargs) -> None:
            for key, value in kwargs.items():
                  if hasattr(self, key):
                        setattr(self, key, value)

class CircuitBreaker:
      def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
            self._config = config or CircuitBreakerConfig()
            
      def configure(self, config: CircuitBreakerConfig) -> None:
            self._config = config
            
      
            
class CircuitBreakerRegistry:
      def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
            self._config = config or CircuitBreakerConfig()
            self._instances: dict[str, CircuitBreaker] = {}
            
      def configure(self, config: CircuitBreakerConfig) -> None:
            self._config = config
            
      def get_circuit_breaker(self, name: str) -> CircuitBreaker:
            if name not in self._instances:
                  self._instances[name] = CircuitBreaker(config=self._config)
            return self._instances[name]
      
      def get(self, name: str) -> CircuitBreaker:
            return self.get_circuit_breaker(name)
      