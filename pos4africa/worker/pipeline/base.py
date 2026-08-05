
from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel, Field
from enum import Enum, auto
from typing import Any

class OnErrorPolicy(Enum):
      ABORT = auto()
      CONTINUE = auto()

class PipelineContext(BaseModel):
      """Shared state passed between components during execution."""
      node_id: str
      memory: Any | None = None

      # Dynamic storage for component outputs
      data: dict[str, Any] = Field(default_factory=dict)

      # Execution metrics and summary counters
      metrics: dict[str, int] = Field(
            default_factory=lambda: {
                  "loaded": 0,
                  "inserted": 0,
                  "inserted_customers": 0,
                  "inserted_catalog": 0,
                  "duplicates": 0,
                  "failed": 0,
            }
      )



class BaseComponent(ABC):
      def __init__(self, on_error: OnErrorPolicy = OnErrorPolicy.CONTINUE) -> None:
            self.on_error = on_error

      @abstractmethod
      def run(self, ctx: PipelineContext) -> None:
            pass