"""
long_term.py

Single-process reference data store for the Excel ingestion pipeline.
This replaces the old Redis-backed shared memory because the pipeline now
processes one local Excel source on a single worker node.
"""

from __future__ import annotations

import asyncio

from pos4africa.manager.memory.search_nomalisation import search_
from pos4africa.shared.utils.logger import get_logger


def _normalise_name(name: str) -> str:
      return name.lower().strip()


class LongTermMemory:
      def __init__(self) -> None:
            self.log = get_logger(__name__)
            self._customers: dict[str, int] = {}
            self._accounts: dict[str, str] = {}
            self._seen_sales: set[str] = set()
            self._refresh_task: asyncio.Task | None = None

      async def warm_up(self) -> None:
            await self._sync_reference_data()

      async def shutdown(self) -> None:
            if self._refresh_task:
                  self._refresh_task.cancel()
                  try:
                        await self._refresh_task
                  except asyncio.CancelledError:
                        pass

      async def _sync_reference_data(self) -> None:
            from pos4africa.worker.components import Sync

            customers = await Sync.fetch_customers() or []
            accounts = await Sync.fetch_accounts() or []

            self._customers = {
                  _normalise_name(row.first_name): row.pos_customer_id
                  for row in customers
                  if row.first_name
            }
            self._accounts = {
                  _normalise_name(row.bank_name): str(row.id)
                  for row in accounts
                  if row.bank_name and row.id
            }

            self.log.info(
                  "long_term_memory.warmed",
                  customers=len(self._customers),
                  accounts=len(self._accounts),
            )

      async def get_customer_id_by_name(self, name: str) -> int | None:
            return await self.get_id_by_name(name, self._customers)

      async def get_accounts_id_by_name(self, name: str) -> str | None:
            return await self.get_id_by_name(name, self._accounts)

      async def get_id_by_name(self, name: str, mapping: dict[str, int | str]) -> int | str | None:
            normalised_name = _normalise_name(name)

            direct_match = mapping.get(normalised_name)
            if direct_match is not None:
                  return direct_match

            best_match = search_(normalised_name, list(mapping.keys()))
            if not best_match:
                  return None

            return mapping.get(best_match)

      async def get_customers(self) -> dict[str, int]:
            return dict(self._customers)

      async def get_accounts(self) -> dict[str, str]:
            return dict(self._accounts)

      async def is_duplicate(self, fingerprint: str) -> bool:
            return fingerprint in self._seen_sales

      async def mark_seen(self, fingerprint: str) -> None:
            self._seen_sales.add(fingerprint)
