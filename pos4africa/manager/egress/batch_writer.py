"""
batch_writer.py — BatchWriter

Takes a list of ProcessedSale dicts and bulk-upserts them into Supabase.
Uses upsert with on_conflict=pos_sale_id to be idempotent —
re-running a scrape won't duplicate records.
"""

from __future__ import annotations

from typing import List, Dict, Any

from pos4africa.config.settings import settings
from pos4africa.infra.supabase_client import spb_client
from pos4africa.shared.utils.logger import get_logger
from pos4africa.shared.utils.retry import with_retry_async

log = get_logger(__name__)


class BatchWriter:
      def __init__(self) -> None:
            self._table = settings.supabase_table_sales
            self._batch_size = settings.supabase_batch_size

      async def write(self, records: List[Dict[str, Any]]) -> int:
            """
            Upsert records into Supabase in sub-batches.
            Returns total number of records upserted.
            """
            if not records:
                  return 0

            total = 0

            for i in range(0, len(records), self._batch_size):
                  chunk = records[i : i + self._batch_size]

                  try:
                        await self._upsert_chunk(chunk)
                        total += len(chunk)

                        log.debug(
                              "batch_writer.chunk_written",
                              count=len(chunk),
                        )

                  except Exception as exc:
                        log.error(
                              "batch_writer.chunk_failed",
                              error=str(exc),
                              chunk_size=len(chunk),
                        )
                        # Fail fast — let consumer decide retry/DLQ
                        raise

            log.info("batch_writer.done", total=total)
            return total

      @with_retry_async
      async def _upsert_chunk(self, chunk: List[Dict[str, Any]]) -> None:
            """
            Performs the actual upsert operation with retry logic.
            """
            # Log first record as sample
            if chunk:
                  log.debug("batch_writer.sample_record", record=chunk[0].get('pos_sale_id'))
                  
            try:
                  # result = spb_client.rpc("insert_sale", {"payload": chunk}).execute()
                  for record in chunk:
                        result = spb_client.rpc("insert_sale", {"payload": record}).execute()
                  
                        # Check each result
                        if hasattr(result, "error") and result.error:
                              log.error(
                                    "batch_writer.supabase_error",
                                    error=str(result.error),
                                    record=record.get('pos_sale_id'),
                              )
                              raise RuntimeError(f"Supabase upsert error: {result.error}")
                  
            except Exception as exc:
                  # Network / transport error
                  log.error(
                        "batch_writer.transport_error",
                        error=str(exc),
                        chunk_size=len(chunk),
                  )
                  raise

            # Supabase-specific error handling
            if hasattr(result, "error") and result.error:
                  log.error(
                        "batch_writer.supabase_error",
                        error=str(result.error),
                        chunk_size=len(chunk),
                  )
                  raise RuntimeError(f"Supabase upsert error: {result.error}")

            # Optional: sanity check response
            if not hasattr(result, "data") or result.data is None:
                  log.warning(
                        "batch_writer.empty_response",
                        chunk_size=len(chunk),
                  )