"""
schema_detector.py — SchemaDetector

Compares the structural fingerprint of incoming HTML pages against a
known-good baseline stored in Redis.

If the structure changes (pos4africa.ng updates their frontend), the
scraper would silently extract wrong data. This component detects that
and raises an alert before bad data enters the pipeline.

Strategy:
  - On first page seen, record the baseline fingerprint.
  - On subsequent pages, compare. If different, log an error and
    optionally pause the node (configurable).
  - Fingerprint is based on the table column headers, not content.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from pos4africa.infra.redis_client import RedisClient
from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.components.base import BaseComponent
from pos4africa.worker.memory.store import MemoryStore

_SCHEMA_KEY = "schema:sales_table_headers"


class SchemaDetector(BaseComponent):
      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id, memory)
            self._drift_detected = False

      async def check(self, html: str, scrape_id: str) -> None:
            """
            Extract column headers from the HTML and compare against baseline.
            Logs an error if drift is detected but does NOT raise — the scraper
            should still attempt extraction and let errors surface naturally.
            """
            headers = self._extract_headers(html)
            if not headers:
                  return

            fingerprint = "|".join(headers)
            baseline = await self.memory.short._redis.get(_SCHEMA_KEY)

            if baseline is None:
                  # First run — set baseline
                  await self.memory.short._redis.set(_SCHEMA_KEY, fingerprint.encode())
                  self.log.info("schema_detector.baseline_set", headers=headers)
                  return

            if baseline.decode() != fingerprint:
                  self._drift_detected = True
                  self.log.error(
                        "schema_detector.DRIFT_DETECTED",
                        scrape_id=scrape_id,
                        baseline=baseline.decode(),
                        current=fingerprint,
                        note="HTML structure may have changed — verify scraper selectors",
                  )
                  # TODO: emit a Prometheus alert / send notification
            else:
                  self.log.debug("schema_detector.ok", scrape_id=scrape_id)

      @staticmethod
      def _extract_headers(html: str) -> list[str]:
            soup = BeautifulSoup(html, "lxml")
            ths = soup.select("table.sales-table thead th")
            return [th.get_text(strip=True).lower() for th in ths]

      @property
      def drift_detected(self) -> bool:
            return self._drift_detected