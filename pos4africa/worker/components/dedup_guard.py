
from __future__ import annotations

from pos4africa.shared.utils.hasher import sale_fingerprint, html_fingerprint
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore

class DedupGuard(BaseComponent):
      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id, memory)
            self._skipped = 0
            
      async def run(self, pos_sale_id: int) -> bool:
            fp = sale_fingerprint(pos_sale_id=str(pos_sale_id))
            is_dup = await self.memory.ltm.is_duplicate(fp)
            
            if is_dup:
                  self._skipped += 1
                  self.log.debug(
                        "dedup.skipped",
                        pos_sale_id=pos_sale_id
                  )
                  return True
            
            await self.memory.ltm.mark_seen(fp)
            return False
      
      @property
      def skipped_count(self) -> int:
            return self._skipped