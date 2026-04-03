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
from datetime import datetime
import threading
from dataclasses import dataclass, field
from typing import List, Dict, Callable, Optional
from pos4africa.config.settings import settings
from pos4africa.shared.utils.logger import get_logger

log = get_logger(__name__)
      
class CircuitBreakerError(Exception):
    """Base exception for circuit breaker"""
    pass

class CircuitBreakerOpenError(CircuitBreakerError):
    """Raised when circuit breaker is open"""
    pass

class CircuitBreakerTimeoutError(CircuitBreakerError):
    """Raised when operation times out"""
    pass

class CBState(StrEnum):
      CLOSED = "closed"
      OPEN = "open"
      HALF_OPEN = "half_open"
      
@dataclass
class CircuitBreakerMetrics:
      """Metrics for circuit breaker monitoring"""
      total_calls: int = 0
      successful_calls: int = 0
      failed_calls: int = 0
      timeout_calls: int = 0
      rejected_calls: int = 0
      state_changes: List[Dict] = field(default_factory=list)
      last_failure_time: Optional[datetime] = None
      last_success_time: Optional[datetime] = None
      current_failure_rate: float = 0.0
      
      def to_dict(self) -> Dict:
            return {
                  'total_calls': self.total_calls,
                  'successful_calls': self.successful_calls,
                  'failed_calls': self.failed_calls,
                  'timeout_calls': self.timeout_calls,
                  'rejected_calls': self.rejected_calls,
                  'failure_rate': self.current_failure_rate,
                  'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
                  'last_success': self.last_success_time.isoformat() if self.last_success_time else None
            }
      
class CircuitBreakerConfig:
      def __init__(
            self,
            failure_threshold: int = settings.cb_failure_threshold,
            recovery_timeout: float = settings.cb_recovery_timeout,
            half_open_max_calls: int = 3,
            half_open_success_threshold: int = 2,
            rolling_window: int = 10,
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
            
            # State management
            self._state = CircuitBreakerState.CLOSED
            self._last_state_change = datetime.now()
            self._lock = threading.RLock()
            
            # Failure tracking
            self._failures: List[datetime] = []
            self._successes: List[datetime] = []
            self._half_open_calls = 0
            self._half_open_successes = 0
            
            
            self.metrics = CircuitBreakerMetrics()
            
      def configure(self, config: CircuitBreakerConfig) -> None:
            self._config = config
            
      def state(self) -> CBState:
            with self._lock:
                  return self._state
            
      def _record_failure(self, error: Optional[Exception] = None) -> None:
            now = datetime.now()
            with self._lock:
                  self._failures.append(now)
                  self.metrics.last_failure_time = now
                  self.metrics.failed_calls += 1
                  
                  # Remove old failures outside rolling window
                  cuttoff = now - timedelta(seconds=self._config.rolling_window)
                  self._failures = [f for f in self._failures if f > cuttoff]
                  self._successes = [s for s in self._successes if s > cutoff]
                  
                  log.warning(
                        "circuit_breaker.failure_recorded",
                        count=len(self._failures),
                        threshold=self._config.failure_threshold,
                  )
            
                  total = len(self._failures) + len(self._successes)
                  if total > 0:
                        self.metrics.current_failure_rate = len(self._failures) / total
                        
                        
      def _record_success(self) -> None:
            now = datetime.now()
            with self._lock:
                  self._successes.append(now)
                  self.metrics.last_success_time = now
                  self.metrics.successful_calls += 1
                  
                  # Remove old successes outside rolling window
                  cutoff = now - timedelta(seconds=self._config.rolling_window)
                  self._failures = [f for f in self._failures if f > cutoff]
                  self._successes = [s for s in self._successes if s > cutoff]
                  
                  log.info(
                        "circuit_breaker.success_recorded",
                        count=len(self._successes),
                  )
            
                  total = len(self._failures) + len(self._successes)
                  if total > 0:
                        self.metrics.current_failure_rate = len(self._failures) / total
      
      def _should_open(self) -> bool:
            """Determine if circuit should open"""
            with self._lock:
                  if self._state == CircuitBreakerState.FORCED_OPEN: return True
                  if self._state == CircuitBreakerState.FORCED_CLOSED: return False
                  if self._state == CircuitBreakerState.DISABLED: return False
                  
                  # Check if we have enough failures
                  if len(self._failures) >= self.failure_threshold: return True
                  
                  # Check failure rate
                  total = len(self._failures) + len(self._successes)
                  if total >= self.rolling_window_size:
                        failure_rate = len(self._failures) / total
                        if failure_rate >= 0.5:  # 50% failure rate threshold
                              return True
                              
                  return False
            
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

