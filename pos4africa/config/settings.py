from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
      
      model_config = SettingsConfigDict(
            env_file=".env",
            env_file_encoding="utf-8",
            case_sensitive=False,
      )
      
      pos_base_url: str

      # Excel source of truth
      excel_source_path: str = "./Excels/DSR.xlsx"
      excel_sheet_name: str = "Sheet1"

      # ── Worker pool ───────────────────────────────────────────────────────────
      worker_count: int = Field(default=1, ge=1, le=32)
      worker_batch_size: int = 500              # sales records per worker batch
      max_queue_size: int = 5000
      
      
      # ── Rate limiting (per worker) ────────────────────────────────────────────
      rate_limit_rps: float = 3.0                # max requests/sec per node
      rate_limit_burst: int = 5                  # burst allowance

      # ── Supabase ──────────────────────────────────────────────────────────────
      supabase_url: str
      supabase_key: SecretStr
      supabase_batch_size: int = 200             # records per upsert call
      supabase_table_sales: str = "sales"
      supabase_table_customers: str = "customers"
      supabase_table_accounts: str = "accounts"

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
