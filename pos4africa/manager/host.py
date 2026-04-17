"""
host.py

Excel-based host manager for a single local worker node.
"""

from __future__ import annotations

from hashlib import sha256
from uuid import NAMESPACE_DNS, uuid5

from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.node import WorkerNode

log = get_logger(__name__)


class HostManager:
      def __init__(self) -> None:
            node_seed = str(uuid5(NAMESPACE_DNS, "excel_node_01"))
            self._node_id = sha256(node_seed.encode()).hexdigest()
            self._node = WorkerNode(node_id=self._node_id)

      async def run(self) -> dict[str, int]:
            log.info("host_manager.starting", mode="excel_single_node", node_id=self._node_id)

            await self._node.start()
            try:
                  summary = await self._node.run_once()
                  log.info("host_manager.completed", **summary)
                  return summary
            finally:
                  await self._node.stop()
