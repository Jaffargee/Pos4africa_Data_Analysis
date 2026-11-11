from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerSettings(BaseSettings):
    """Server-only settings.  POS and Supabase credentials stay in
    ``pos4africa.config.settings`` and are read from the same ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    server_api_key: SecretStr = Field(min_length=16)  # required: this API can write to your POS
    cors_origins: list[str] = []

    # "dry_run": rehearse in POS (cart built then discarded), log nothing, change nothing.
    # "commit": really write.  Flip only after a dry run behaves on your real POS.
    pos_write_mode: Literal["dry_run", "commit"] = "dry_run"

    # Logged-in POS sessions.  The POS keeps the cart in the session, so each session
    # runs one write at a time.  >1 only if POS does not log out older sessions of one user.
    pos_session_pool_size: int = Field(default=1, ge=1, le=8)
    pos_session_acquire_timeout: float = 30.0

    walk_in_customer_id: int = 23
    salesperson: str = "POS Server"  # provisional; the ETL overwrites with POS's real value

    # Reconciliation of writes with an unknown outcome
    uncertain_grace_seconds: int = 300
    reconcile_interval_seconds: int = 0  # 0 = off (use POST /recon/reconcile)

    # Register endpoints that are NOT used anywhere in the existing code yet.
    # UNVERIFIED defaults (standard PHPPOS paths): confirm in the browser network tab.
    pos_sales_add_item_path: str = "/index.php/sales/add"
    pos_sales_edit_item_path: str = "/index.php/sales/edit_item/{line}"
    pos_sales_add_payment_path: str = "/index.php/sales/add_payment"
    pos_customers_export_path: str = "/index.php/customers/excel_export"
    pos_sales_report_path: str = (
        "/index.php/reports/generate/detailed_sales?report_type=simple"
        "&report_date_range_simple={range}&sale_type=all&with_time=1&excel_export=0&export_excel=1"
    )
