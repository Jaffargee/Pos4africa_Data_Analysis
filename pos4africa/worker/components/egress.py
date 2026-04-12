

from __future__ import annotations

from pos4africa.infra.rabbitmq import get_connection, get_channel
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.config.settings import settings

from aio_pika import DeliveryMode, Message
from collections import deque
import orjson, json

class WorkerEgress(BaseComponent):
      
      _MAX_LOCAL_BUFFER = 500
      
      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id, memory)
            self._retry_buffer: deque[dict] = deque(maxlen=self._MAX_LOCAL_BUFFER)
            
      async def run(self, processed_sale: dict) -> bool:
            
            if not processed_sale:
                  self.log.warning("egress.processed_missing", node_id=self.node_id)
                  return False
            
            try:
                  await self._publish(processed_sale)
                  self._on_success()
                  
                  return True
            except Exception as exc:
                  self._on_error(exc)
                  self._retry_buffer.append(processed_sale)
                  self.log.warning(
                        "egress.buffered_locally",
                        node_id=self.node_id,
                        buffer_size=len(self._retry_buffer)
                  )
                  return False
            
      async def flush_retry_buffer(self) -> int:
            flushed = 0
            while self._retry_buffer:
                  payload = self._retry_buffer[0]
                  try:
                        await self._publish(payload=payload)
                        self._retry_buffer.popleft()
                        flushed += 1
                  except Exception:
                        self.log.debug("Remainder for the next flush attempt.")
                  
            if flushed:
                  self.log.info("egress.buffer_flushed", count=flushed)
                  
            return flushed
            
      async def _publish(self, payload: dict) -> None:
            
            if not payload:
                  self.log.warning(f"Rabbitmq payload is not provided.", node_id=self.node_id)
                  return None
                        
            channel = await get_channel()
            body = orjson.dumps({
                  **payload,
                  "_meta": {
                        "node_id": self.node_id,
                        "routing_key": f"sales.{self.node_id}",
                  }
            })
            
            message = Message(
                  body=body,
                  delivery_mode=DeliveryMode.PERSISTENT,
                  content_type="application/json",
            )
            
            await channel.declare_queue(settings.rabbitmq_queue_sales, durable=True)
            await channel.default_exchange.publish(message, routing_key=settings.rabbitmq_queue_sales)
            