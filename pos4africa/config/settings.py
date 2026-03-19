from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from dotenv import load_dotenv

load_dotenv()

POS_BASE = os.getenv('POS_BASE')
POS_USERNAME = os.getenv('POS_USERNAME')
POS_PASSWORD = os.getenv('POS_USERNAME')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

class Settings(BaseSettings):
      
      model_config = SettingsConfigDict(
            env_file=".env",
            env_file_encoding="utf-8",
            case_sensitive=False,
      )

      # ── POS target ────────────────────────────────────────────────────────────
      pos_base_url: str = POS_BASE
      pos_username: str = POS_USERNAME
      pos_password: str = POS_PASSWORD
      pos_password: SecretStr
      pos_login_path: str = "/login"
      pos_sales_path: str = "/sales"
      pos_request_timeout: float = 30.0          # seconds per HTTP request
      pos_page_size: int = 50                    # records per page

      # ── Worker pool ───────────────────────────────────────────────────────────
      worker_count: int = Field(default=8, ge=1, le=32)
      worker_batch_size: int = 1000              # sales records per worker batch

      # ── Rate limiting (per worker) ────────────────────────────────────────────
      rate_limit_rps: float = 3.0                # max requests/sec per node
      rate_limit_burst: int = 5                  # burst allowance

      # ── Redis ─────────────────────────────────────────────────────────────────
      redis_url: str = "redis://localhost:6379/0"
      redis_short_term_ttl: int = 300            # 5 minutes (seconds)
      redis_long_term_refresh_interval: int = 1800  # 30 minutes

      # ── RabbitMQ ──────────────────────────────────────────────────────────────
      rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
      rabbitmq_exchange: str = "pos_data"
      rabbitmq_queue_sales: str = "sales_ingest"
      rabbitmq_queue_dlq: str = "sales_ingest.dlq"
      rabbitmq_prefetch_count: int = 100

      # ── Supabase ──────────────────────────────────────────────────────────────
      supabase_url: str = SUPABASE_URL
      supabase_service_key: SecretStr = SUPABASE_KEY
      supabase_batch_size: int = 200             # records per upsert call
      supabase_table_sales: str = "sales"
      supabase_table_customers: str = "customers"
      supabase_table_terminals: str = "terminals"

      # ── Circuit breaker ───────────────────────────────────────────────────────
      cb_failure_threshold: int = 5             # failures before OPEN
      cb_recovery_timeout: float = 60.0         # seconds before HALF-OPEN

      # ── Retry ─────────────────────────────────────────────────────────────────
      retry_max_attempts: int = 3
      retry_min_wait: float = 1.0
      retry_max_wait: float = 10.0

      # ── Observability ─────────────────────────────────────────────────────────
      log_level: str = "INFO"
      metrics_port: int = 9090


# Single shared instance — import this everywhere
settings = Settings()  # type: ignore[call-arg]