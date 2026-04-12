"""
retry.py — pre-configured retry decorators wrapping tenacity.

Usage:
    from pos4africa.shared.utils.retry import with_retry, with_retry_async

    @with_retry_async
    async def fetch_page(url: str) -> str:
        ...
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, TypeVar

from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pos4africa.config.settings import settings
from pos4africa.shared.utils.logger import get_logger

log = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

# Exceptions we always retry on
_RETRYABLE = (
      ConnectionError,
      TimeoutError,
      OSError,
)


def with_retry_async(func: F) -> F:
      """
      Decorator: retry an async function up to settings.retry_max_attempts times
      with exponential backoff. Logs each attempt.
      """

      @functools.wraps(func)
      async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async for attempt in AsyncRetrying(
                  retry=retry_if_exception_type(_RETRYABLE),
                  stop=stop_after_attempt(settings.retry_max_attempts),
                  wait=wait_exponential(
                  min=settings.retry_min_wait,
                  max=settings.retry_max_wait,
                  ),
                  reraise=True,
            ):
                  with attempt:
                        try:
                              return await func(*args, **kwargs)
                        except _RETRYABLE as exc:
                              log.warning(
                                    "retry.attempt",
                                    func=func.__qualname__,
                                    attempt=attempt.retry_state.attempt_number,
                                    error=str(exc),
                              )
                              raise

      return wrapper  # type: ignore[return-value]
