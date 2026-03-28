"""
consumer.py — ManagerEgressConsumer

Consumes batches of ProcessedSale payloads from RabbitMQ and
delegates to the BatchWriter for bulk upsert into Supabase.

Flow:
  RabbitMQ queue (sales_ingest)
    → consume messages
    → accumulate into batch
    → flush via BatchWriter when batch_size or timeout reached
    → ack messages ONLY after successful flush
    → nack → DLQ on failure
"""

from __future__ import annotations

import asyncio
import orjson

from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage

from pos4africa.config.settings import settings
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.shared.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpen,
)
from pos4africa.shared.utils.logger import get_logger

log = get_logger(__name__)

_FLUSH_TIMEOUT = 5.0  # seconds


class ManagerEgressConsumer:
      def __init__(self) -> None:
            self._writer = BatchWriter()
            self._cb = CircuitBreaker(name="supabase_egress")

            self._batch: list[dict] = []
            self._pending_msgs: list[AbstractIncomingMessage] = []

            self._flush_task: asyncio.Task | None = None
            self._running = True

      async def run(self) -> None:
            connection = await connect_robust(settings.rabbitmq_url)

            async with connection:
                  channel = await connection.channel()
                  await channel.set_qos(prefetch_count=settings.rabbitmq_prefetch_count)

                  queue = await channel.declare_queue(settings.rabbitmq_queue_sales, durable=True)

                  log.info("egress_consumer.listening", queue=settings.rabbitmq_queue_sales)

                  # Start background flush loop
                  self._flush_task = asyncio.create_task(self._periodic_flush())

                  try:
                        async with queue.iterator() as q:
                              async for message in q:
                                    await self._handle_message(message)
                  finally:
                        # Graceful shutdown
                        self._running = False

                        if self._flush_task:
                              self._flush_task.cancel()
                              try:
                                    await self._flush_task
                              except asyncio.CancelledError:
                                    pass

                        # Final flush before exit
                        await self._flush()

      async def _handle_message(self, message: AbstractIncomingMessage) -> None:
            try:
                  payload = orjson.loads(message.body)

                  self._batch.append(payload)
                  self._pending_msgs.append(message)

                  if len(self._batch) >= settings.supabase_batch_size:
                        await self._flush()

            except Exception as exc:
                  log.error("egress_consumer.message_error", error=str(exc))

                  # Bad message → send to DLQ
                  await message.nack(requeue=False)

      async def _flush(self) -> None:
            if not self._batch:
                  return

            batch = self._batch[:]
            msgs = self._pending_msgs[:]

            self._batch.clear()
            self._pending_msgs.clear()

            try:
                  async with self._cb:
                        inserted = await self._writer.write(batch)
                        log.info("egress_consumer.flushed", records=inserted)

                  # ✅ ACK only after success
                  for msg in msgs:
                        await msg.ack()

            except CircuitBreakerOpen as exc:
                  log.error("egress_consumer.circuit_open", error=str(exc))

                  # Requeue in memory (retry later)
                  self._batch.extend(batch)
                  self._pending_msgs.extend(msgs)

            except Exception as exc:
                  log.error(
                        "egress_consumer.flush_failed",
                        error=str(exc),
                        records=len(batch),
                  )

                  # ❌ Send to DLQ
                  for msg in msgs:
                        await msg.nack(requeue=False)

      async def _periodic_flush(self) -> None:
            while self._running:
                  await asyncio.sleep(_FLUSH_TIMEOUT)

                  try:
                        await self._flush()
                  except Exception as exc:
                        log.error(
                              "egress_consumer.periodic_flush_error",
                              error=str(exc),
                        )
                        
                        
                        
                        
                        
                        
"""
Think of RabbitMQ like a post office. Here is how those terms work in plain English:
1. Ack and Nack (The Receipt)
When a worker takes a message, RabbitMQ needs to know if the "job" was finished.
Ack (Acknowledge): Your code tells RabbitMQ: "I'm done, you can delete this message now."
Nack (Negative Ack): Your code tells RabbitMQ: "I failed. Please put this message back in the queue (requeue) so someone else can try."
2. Connect Robust (The Self-Healing Pipe)
Standard connections break if the network hiccups or the Docker container restarts.
connect_robust: This is a "smart" connection from aio-pika. If the connection drops, it automatically tries to reconnect and restore all your open channels and queues without your code crashing.
3. Channel (The Conversation)
A Connection is the heavy physical cable between your app and Docker. A Channel is a "virtual" light-weight connection inside that cable.
Why use it? Opening a real connection is slow/expensive. Opening a channel is instant. You can have hundreds of channels (different tasks) sharing one single connection.
4. Set QoS & Prefetch (The Flow Control)
Without these, RabbitMQ will dump every message it has into your Python script at once, potentially crashing your RAM.
Prefetch Count: This tells RabbitMQ: "Only give me X messages at a time."
Set QoS (Quality of Service): This is the command used to apply the prefetch limit.
Example: If prefetch_count=10, your worker gets 10 messages. It won't get the 11th until it Acks one of the first 10.
Summary for your code:
In your get_channel function, you set prefetch_count=10. This means your worker is being "polite"—it only takes 10 sales records at a time to process them safely.
Do you want to see how to manually "Ack" a message in your worker code?

"""