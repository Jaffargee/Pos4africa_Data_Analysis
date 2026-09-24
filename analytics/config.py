
class Config:
      host = 'localhost'
      port = 6379
      password = 'tahir@2024'
      db = 0

      DEFAULT_TTL_SECONDS = 3600
      CACHABLES_TABLES_OR_VIEWS = {
            "accounts": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "items": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "customers": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "customers_addresses": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "sales": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "sale_items": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "payments": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "v_revenue_daily": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "v_revenue_monthly": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "v_revenue_weekly": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "v_revenue_by_dow": {"ttl_seconds": DEFAULT_TTL_SECONDS},
            "v_item_revenue_intelligence": {"ttl_seconds": DEFAULT_TTL_SECONDS},
      }

      PRICE_TIER = ["ENTRY", "CORE", "MID", "PREMIUM", "LUXURY", "ULTRA_LUXURY"]

      @staticmethod
      def cache_policy_for(table_name: str) -> dict | None:
            return Config.CACHABLES_TABLES_OR_VIEWS.get(table_name, None)