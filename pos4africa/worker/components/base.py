
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any
import structlog

from pos4africa.shared.utils.logger import get_logger
from pos4africa.manager.memory.store import MemoryStore

class BaseComponent:
      
      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            self.node_id = node_id
            self.memory = memory
            self.log = get_logger(self.__class__.__name__).bind(node_id=node_id)
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