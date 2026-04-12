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
from datetime import datetime, timedelta
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

                  
class CircuitBreakerRegistry:
      def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
            self._config = config or CircuitBreakerConfig()
            self._instances: dict[str, CircuitBreaker] = {}
            
      def configure(self, config: CircuitBreakerConfig) -> None:
            self._config = config
            
      def get_circuit_breaker(self, name: str) -> CircuitBreaker:
            if name not in self._instances:
                  self._instances[name] = CircuitBreaker(name=name, config=self._config)
            return self._instances[name]
      
      def get(self, name: str) -> CircuitBreaker:
            return self.get_circuit_breaker(name)

class CircuitBreaker:
      def __init__(self, name: str, config: CircuitBreakerConfig | None = None) -> None:
            self.name = name
            self._config = config or CircuitBreakerConfig()
            self.log = get_logger(f"CircuitBreaker[{id(self)}]")
            
            # State management
            self._state = CBState.CLOSED
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
            
      async def _record_failure(self, error: Optional[Exception] = None) -> None:
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
                        
                  if self._config.on_failure:
                        self._trigger_callback(self._config.on_failure, error)
                        
      async def _record_success(self) -> None:
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
                        
                  if self._config.on_success:
                        self._trigger_callback(self._config.on_success)
      
      def _should_open(self) -> bool:
            """Determine if circuit should open"""
            with self._lock:
                  if self._state == CBState.FORCED_OPEN: return True
                  if self._state == CBState.FORCED_CLOSED: return False
                  if self._state == CBState.DISABLED: return False

                  # Check if we have enough failures
                  if len(self._failures) >= self._config.failure_threshold: return True
                  
                  # Check failure rate
                  total = len(self._failures) + len(self._successes)
                  if total >= self._config.rolling_window:
                        failure_rate = len(self._failures) / total
                        if failure_rate >= 0.5:  # 50% failure rate threshold
                              return True
                              
                  return False
                     
      def _should_attempt_recovery(self) -> bool:
            """Check if it's time to attempt recovery"""
            with self._lock:
                  if self._state != CBState.OPEN:
                        return False

                  time_open = (datetime.now() - self._last_state_change).total_seconds()
                  return time_open >= self._config.recovery_timeout

      def _should_close(self) -> bool:
            """Determine if circuit should close"""
            with self._lock:
                  if self._state != CBState.HALF_OPEN:
                        return False

                  return self._half_open_successes >= self._config.half_open_success_threshold
            
      def _change_state(self, new_state: CBState) -> None:
            with self._lock:
                  old_state = self._state
                  
                  if old_state == new_state:
                        return  # no change
                  
                  if self.new_state == CBState.HALF_OPEN:
                        self._half_open_calls = 0
                        self._half_open_successes = 0
                  
                  self._state = new_state
                  self._last_state_change = datetime.now()
                  
                  self.metrics.state_changes.append({
                        'from': old_state.value,
                        'to': new_state.value,
                        'timestamp': self._last_state_change.isoformat()
                  })
                  
                  self.log.warning(
                        "circuit_breaker.state_changed",
                        from_state=old_state.value,
                        to_state=new_state.value,
                        timestamp=self._last_state_change.isoformat()
                  )
                  
      def _trigger_callback(self, callback: Callable, *args, **kwargs) -> None:
            """Safely trigger callback"""
            try:
                  callback(*args, **kwargs)
            except Exception as e:
                  self.log.error(f"Error in callback: {e}")
                  
      def force_open(self) -> None:
            """Force circuit breaker to open state"""
            with self._lock:
                  self._change_state(CBState.FORCED_OPEN)
      
      def force_close(self) -> None:
            """Force circuit breaker to closed state"""
            with self._lock:
                  self._change_state(CBState.FORCED_CLOSED)
      
      def disable(self) -> None:
            """Disable circuit breaker"""
            with self._lock:
                  self._change_state(CBState.DISABLED)
      
      def reset(self) -> None:
            """Reset circuit breaker to closed state"""
            with self._lock:
                  self._failures.clear()
                  self._successes.clear()
                  self._half_open_calls = 0
                  self._half_open_successes = 0
                  self._change_state(CBState.CLOSED)
      
      def get_metrics(self) -> Dict:
            """Get circuit breaker metrics"""
            with self._lock:
                  return {
                        'name': self.name,
                        'state': self._state.value,
                        'metrics': self.metrics.to_dict(),
                        'failures_in_window': len(self._failures),
                        'successes_in_window': len(self._successes),
                        'last_state_change': self._last_state_change.isoformat(),
                        'half_open_calls': self._half_open_calls,
                        'half_open_successes': self._half_open_successes
                  }
                  
      async def __aenter__(self) -> "CircuitBreaker":
            with self._lock:
                  if self._state == CBState.OPEN:
                        if self._should_attempt_recovery():
                              self._change_state(CBState.HALF_OPEN)
                        else:
                              self.metrics.rejected_calls += 1
                              raise CircuitBreakerOpenError(
                                    f"Circuit is OPEN. Retry after {self._config.recovery_timeout}s"
                              )
            return self
      
      async def __aexit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[object]
      ) -> bool:
            with self._lock:
                  if exc_type is None:
                        await self._record_success()
                        if self._state == CBState.HALF_OPEN and self._should_close():
                              self._change_state(CBState.CLOSED)
                              if self._config.on_state_change: self._trigger_callback(self._config.on_state_change, CBState.CLOSED)
                        else:
                              if self._config.on_success: self._trigger_callback(self._config.on_success)
                  else:
                        if exc_type in self._config.exclude_exceptions:
                              await self._record_success()
                              return False  # don't suppress
                        if self._config.include_exceptions and exc_type not in self._config.include_exceptions:
                              await self._record_success()
                              return False  # don't suppress
                              
                        await self._record_failure(exc_val)
                        if self._should_open():
                              self._change_state(CBState.OPEN)
                              if self._config.on_state_change: self._trigger_callback(self._config.on_state_change, CBState.OPEN)
                        else:
                              if self._config.on_failure: self._trigger_callback(self._config.on_failure, exc_val)
                  
