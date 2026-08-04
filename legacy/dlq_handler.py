"""
dlq_handler.py — DLQHandler

Consumes from the Dead Letter Queue (DLQ) for messages that failed
in the main consumer after all retries.

Options per message:
  1. Retry once more (with a longer backoff)
  2. Log to a `failed_sales` table in Supabase for manual review
  3. Discard (after logging)
"""

from __future__ import annotations

import asyncio

import orjson
from aio_pika import connect_robust

from pos4africa.config.settings import settings
from pos4africa.infra.supabase_client import get_supabase
from pos4africa.shared.utils.logger import get_logger

log = get_logger(__name__)


class DLQHandler:
      async def run(self) -> None:
            connection = await connect_robust(settings.rabbitmq_url)
            async with connection:
                  channel = await connection.channel()
                  dlq = await channel.declare_queue(settings.rabbitmq_queue_dlq,durable=True,)
                  log.info("dlq_handler.listening", queue=settings.rabbitmq_queue_dlq)

                  async with dlq.iterator() as q:
                        async for message in q:
                              async with message.process():
                                    await self._handle(message.body)

      async def _handle(self, body: bytes) -> None:
            try:
                  payload = orjson.loads(body)
                  await self._log_to_supabase(payload)
                  log.warning(
                        "dlq_handler.recorded_failure",
                        pos_sale_id=payload.get("pos_sale_id"),
                        node_id=payload.get("node_id"),
                  )
            except Exception as exc:
                  log.error("dlq_handler.error", error=str(exc))

      async def _log_to_supabase(self, payload: dict) -> None:
            sb = get_supabase()
            sb.table("failed_sales").insert({
                  "payload": payload,
                  "pos_sale_id": payload.get("pos_sale_id"),
                  "node_id": payload.get("node_id"),
            }).execute()