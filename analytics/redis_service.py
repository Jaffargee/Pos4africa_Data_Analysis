from infra.redis_client import RedisClient
from typing import Any
from redis.exceptions import RedisError
import json, logging

logger = logging.getLogger(__name__)

class RedisService:
      def __init__(self) -> None:
            self.__r_client: RedisClient = RedisClient()


      def store(self, key: str, data: Any, ttl_seconds: int = 3600):
            try:
                  self.__r_client.set_value(key, json.dumps(data), ttl_seconds=ttl_seconds)
            except RedisError as exc:
                  logger.warning("Redis SET failed for %s: %s", key, exc)

      def get(self, key: str):
            try:
                  raw = self.__r_client.get_value(key)
            except RedisError as exc:
                  logger.warning("Redis GET failed for %s: %s", key, exc)
                  return None
            
            if raw is None:
                  return None
            
            cache = str(raw).strip()

            if not cache.startswith('{') and not cache.startswith('['):
                  return cache
            try:
                  return json.loads(cache)
            except json.JSONDecodeError:
                  return cache

      def delete(self, key: str):
            try:
                  self.__r_client.delete_value(key)
            except RedisError as exc:
                  logger.warning("Redis DELETE failed for %s: %s", key, exc)
            
      def flush_db(self):
            try:
                  return self.__r_client.flush_db()
            except RedisError as exc:
                  logger.warning("Redis FLUSHDB failed: %s", exc)
                  return None