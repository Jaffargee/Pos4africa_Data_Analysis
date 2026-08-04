
from __future__ import annotations

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from pos4africa.config.settings import settings
import asyncio


_connection: AbstractRobustConnection | None = None
_channel: AbstractChannel | None = None

_connection_lock = asyncio.Lock()
_channel_lock = asyncio.Lock()

async def get_connection() -> AbstractRobustConnection:
      global _connection
      async with _connection_lock:
            if _connection is None or _connection.is_closed:
                  _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
      return _connection

async def get_channel() -> AbstractChannel:
      global _channel
      
      async with _channel_lock:
            if _channel is None or _channel.is_closed:
                  conn = await get_connection()
                  _channel = await conn.channel()
                  await _channel.set_qos(prefetch_count=settings.rabbitmq_prefetch_count)
                  
      return _channel

async def close_rabbitmq() -> None:
      global _connection
      if _connection and not _connection.is_closed:
            await _connection.close()
            _connection = None