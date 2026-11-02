
from __future__ import annotations

from abc import abstractmethod
from typing import Any

from pos4africa.shared.utils.logger import get_logger

class BaseComponent:
      
      def __init__(self) -> None:
            self.log = get_logger(self.__class__.__name__)
            self._error_count = 0
            self._processed_count = 0
            
      @abstractmethod
      async def run(self, **kwargs: Any) -> Any:
            """Execute this component's stage. Kwargs vary per component."""
            pass
            
            
      def _on_success(self) -> None:
            self._processed_count += 1
            
      def _on_error(self, exc: Exception) -> None:
            self._error_count += 1
            self.log.error(
                  "component.error",
                  component=self.__class__.__name__,
                  error=str(exc),
                  error_type=type(exc).__name__,
            )
            
      @property
      def stats(self) -> None:
            return {
                  "component": self.__class__.__name__,
                  "node_id": self.node_id,
                  "processed": self._processed_count,
                  "errors": self._error_count,
            }