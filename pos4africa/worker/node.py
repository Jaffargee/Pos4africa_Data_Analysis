"""
node.py

Single-node Excel ingestion pipeline:
  ExcelScraper -> Parser -> Processor -> BatchWriter
"""

from __future__ import annotations

from pos4africa.config.settings import settings
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.pipeline import (
      BaseComponent, PipelineContext, OnErrorPolicy
)

from typing import Sequence

class WorkerNode:
      def __init__(self, node_id: str, components: Sequence[BaseComponent]) -> None:
            self._node_id = node_id
            self.log = get_logger(__name__).bind(node_id=node_id)
            self._running = False
            self._memory: MemoryStore | None = None
            self._components = list(components) if components else []

      @property
      def node_id(self) -> str:
            return self._node_id

      def add_component(self, component: BaseComponent) -> WorkerNode:
            """Builder pattern method to register pipeline steps."""
            self._components.append(component)
            return self

      async def start(self) -> None:
            self._memory = MemoryStore()
            await self._memory.initialise()
            self._running = True
            self.log.info("worker_node.started", excel_source_path=settings.excel_source_path)

      async def stop(self) -> None:
            self._running = False
            if self._memory:
                  await self._memory.close()
            self.log.info("worker_node.stopped")

      async def run_once(self) -> dict[str, int]:
            if not self._running or self._memory is None:
                  raise RuntimeError("WorkerNode must be started before run_once().")

            ctx = PipelineContext(node_id=self._node_id, memory=self._memory)

            for component in self._components:
                  component_name = component.__class__.__name__
                  try:
                        self.log.info("pipeline.step.start", component=component_name)
                        await component.run(ctx)
                        self.log.info("pipeline.step.completed", component=component_name)
                  except Exception as exc:
                        self.log.error(
                              "pipeline.step.failed", 
                              component=component_name, 
                              error=str(exc)
                        )
                        if component.on_error == OnErrorPolicy.ABORT:
                              self.log.error("pipeline.aborted", component=component_name)
                              raise exc



            summary = ctx.metrics
            self.log.info("worker_node.completed", **summary)
            return summary