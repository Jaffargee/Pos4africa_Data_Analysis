
from __future__ import annotations

import asyncio
import time

class RateLimiter:
      
      def __init__(self, rps: float, burst: int) -> None:
            self._rps = rps
            self._burst = float(burst)
            self._tokens = float(burst)
            self._last_refill = time.monotonic()
            self._lock = asyncio.Lock()
            
      async def acquire(self, tokens: float = 1.0) -> None:
            async with self._lock:
                  while True:
                        self._refill()
                        if self._tokens >=  tokens:
                              self._tokens -= tokens
                              return
                        
                        deficit = tokens - self._tokens
                        sleep_time = deficit / self._rps
                        await asyncio.sleep(sleep_time)
                        
      def _refill(self) -> None:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._burst, self._tokens + elapsed * self._rps)
            self._last_refill = now