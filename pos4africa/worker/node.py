"""
node.py

Single-node Excel ingestion pipeline:
  ExcelScraper -> Parser -> Processor -> BatchWriter
"""

from __future__ import annotations

from pos4africa.config.settings import settings
from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.pipeline import (
      BaseComponent, PipelineContext, OnErrorPolicy
)
from pos4africa.manager.memory.local_storage import LocalStorage

from typing import Sequence

class WorkerNode:
      def __init__(self, components: Sequence[BaseComponent]) -> None:
            self.log = get_logger(__name__)
            self._running = False
            self._components = list(components) if components else []
            self.__local_storage = LocalStorage()

      @property
      def node_id(self) -> str:
            return self._node_id

      def add_component(self, component: BaseComponent) -> WorkerNode:
            """Builder pattern method to register pipeline steps."""
            self._components.append(component)
            return self

      async def start(self) -> None:
            self._running = True
            await (
                  self.__local_storage
                  .add(settings.supabase_table_accounts, settings.payment_channels_cache_key, "id, bank_name")
                  .add(settings.supabase_table_items, settings.items_cache_key, "*")
            ).init_localstorage()
            
            self.log.info("worker_node.started", excel_source_path=settings.excel_source_path)

      async def stop(self) -> None:
            self._running = False
            self.log.info("worker_node.stopped")

      async def run_once(self) -> dict[str, int]:
            if not self._running:
                  raise RuntimeError("WorkerNode must be started before run_once().")

            ctx = PipelineContext()

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