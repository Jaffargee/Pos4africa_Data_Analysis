import redis
from redis.exceptions import AuthenticationError, ConnectionError
from config import Config

class RedisClient:
      def __init__(self, host=Config.host, port=Config.port, db=0, password=Config.password):
            self.__redis_client = redis.Redis(
                  host=host,
                  port=port,
                  db=db,
                  password=password,
                  decode_responses=True
            )
      
      def test_connection(self):
            try:
                  if self.__redis_client.ping(): print("Successfully connected to Redis!")
                  return
            except AuthenticationError: 
                  print("Authentication failed. Please check your password.")
                  return False
            except ConnectionError: 
                  print("Could not connect to Redis. Ensure the Docker container is running.")
                  return False

      def set_value(self, name: str, value: str, ttl_seconds: int = 3600):
            return self.__redis_client.setex(name=name, value=value, time=ttl_seconds)

      def get_value(self, name: str):
            return self.__redis_client.get(name)
      
      def delete_value(self, name: str):
        return self.__redis_client.delete(name)

      def flush_db(self):
            """Flush the entire Redis database."""
            return self.__redis_client.flushdb()

      def close_connection(self):
            """Close the Redis connection."""
            self.__redis_client.close()

      def __repr__(self):
            return f"<RedisClient(host={self.__redis_client.connection_pool.connection_kwargs['host']}, port={self.__redis_client.connection_pool.connection_kwargs['port']}, db={self.__redis_client.connection_pool.connection_kwargs['db']})>"

