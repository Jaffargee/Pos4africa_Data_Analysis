
from __future__ import annotations

from pos4africa.shared.utils.hasher import sale_fingerprint
from pos4africa.worker.components.base import BaseComponent

class DedupGuard(BaseComponent):
      def __init__(self) -> None:
            self._skipped = 0
            self.__seen_sales: set[str] = set()
            
      def run(self, pos_sale_id: int) -> bool:
            fp = sale_fingerprint(pos_sale_id=str(pos_sale_id))
            is_dup = self.__is_duplicate(fp)
            
            if is_dup:
                  self._skipped += 1
                  self.log.debug("dedup.skipped", pos_sale_id=pos_sale_id, skipped=self.skipped_count)
                  return True
            
            self.__mark_seen(fp)
            return False
      
      @property
      def skipped_count(self) -> int:
            return self._skipped

      def __is_duplicate(self, fingerprint: str) -> bool:
            return fingerprint in self.__seen_sales

      def __mark_seen(self, fingerprint: str) -> None:
            self.__seen_sales.add(fingerprint)
