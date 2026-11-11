from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
      
      model_config = SettingsConfigDict(
            env_file=".env",
            env_file_encoding="utf-8",
            case_sensitive=False,
            extra="ignore",   # the server shares this .env and adds its own keys
      )

      pos_base_url: str
      pos_username: str
      pos_password: SecretStr

      pos_login_path: str = "/index.php/login"
      pos_sales_path: str = "/index.php/sales/receipt"
      pos_customer_form: str = "/index.php/customers/view/-1/"
      pos_customer_form_action: str = "/index.php/customers/save/"
      pos_customer_edit_form_action: str = "/index.php/customers/save/{person_id}"
      pos_customer_delete: str = "/index.php/customers/delete"
      
      pos_request_timeout: float = 30.0          # seconds per HTTP request
      pos_page_size: int = 50                    # records per page
      
      # Excel source of truth
      excel_source_path: str = "./Excels/DSR.xlsx"
      customer_excel_path: str = "./Excels/Customers.xlsx"
      catalog_excel_path: str = "./Excels/Items.xlsx"
      excel_sheet_name: str = "Sheet1"
      customer_sheet_name: str = "Sheet1"

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
      supabase_batch_size: int = 260             # records per upsert call
      supabase_table_sales: str = "sales"
      supabase_table_customers: str = "customers"
      supabase_table_accounts: str = "accounts"
      supabase_table_items: str  = "items"

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


      default_ttl_seconds: int = 60 * 60
      payment_channels_cache_key: str = "payment_channel.cache"
      items_cache_key: str = "items.cache"



# Single shared instance — import this everywhere
settings = Settings()  # type: ignore[call-arg]
