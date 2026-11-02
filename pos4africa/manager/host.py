"""
host.py

Excel-based host manager for a single local worker node.
"""

from __future__ import annotations

from hashlib import sha256
from uuid import NAMESPACE_DNS, uuid5

from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.node import WorkerNode
from pos4africa.worker.pipeline import (
      SalesSyncer, CustomerSyncer, CatalogSyncer
)

log = get_logger(__name__)


class HostManager:
      def __init__(self) -> None:
            self._node = (
                  WorkerNode(components=[])
                  .add_component(CustomerSyncer())
                  .add_component(CatalogSyncer())
                  .add_component(SalesSyncer())
            )

      async def run(self) -> dict[str, int]:
            log.info("host_manager.starting", mode="excel_single_node",)

            await self._node.start()
            try:
                  summary = await self._node.run_once()
                  log.info("host_manager.completed", **summary)
                  return summary
            finally:
                  await self._node.stop()
