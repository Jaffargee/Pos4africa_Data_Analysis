import json
import time
from pathlib import Path
from typing import Any
from infra.supabase_client import spb_client

CACHE_FILE = Path(__file__).parent / "supabase_cache_v2.json"
CACHE_TTL_SECONDS = 3600  # 1 hour cache validity

# Columns exposed by public.v_revenue_daily_v2 (see analytics/sql/v_revenue_daily_v2.sql).
# Extends the original 3-feature view (num_sales, revenue, items_sold) with:
#   - price-tier revenue composition (pct_rev_premium/midtier/standard/lowticket)
#   - price_dispersion (std dev of unit_price within the day - catches mixed-extreme
#     baskets that a plain average would hide)
#   - avg_margin_pct (sale_items.gross_profit is already computed in Postgres - unused
#     until now)
#   - distinct_categories, pct_rev_slow_dead (dead/slow stock exposure)
#   - pct_pay_cash (payments.account mix)
#   - return_rate, pct_anonymous, day_of_week/is_weekend/is_month_end
# NOTE: pct_revenue_wholesale_cust / pct_revenue_vip_cust were dropped after checking
# the data - every customer row is currently tagged 'STANDARD', so those columns were
# zero-variance (all customers.category values are the same), which is worse than
# just unhelpful for KMeans: StandardScaler divides by std, and a constant column has
# std=0, producing NaN/inf that silently corrupts the whole fit. Revisit once the
# business actually starts tagging WHSL1/WHSL2/VIP/SEASONAL customers in the POS.


def _fetch_and_cache_v2() -> list[dict | Any]:
      """Fetch fresh expanded daily features from Supabase and persist to local cache."""
      response = spb_client.table("v_revenue_daily_v2").select("*").execute()
      data = response.data or []

      try:
            with open(CACHE_FILE, "w") as cache_file:
                  json.dump(data, cache_file, indent=4)
      except IOError as e:
            print(f"Warning: Failed to write cache: {e}")

      return data


def _read_cache() -> list[dict] | None:
      if not CACHE_FILE.exists():
            return None

      file_age = time.time() - CACHE_FILE.stat().st_mtime
      if file_age > CACHE_TTL_SECONDS:
            return None

      try:
            with open(CACHE_FILE, "r") as cache_file:
                  return json.load(cache_file)
      except (json.JSONDecodeError, IOError):
            return None


def get_daily_features_v2() -> list[dict]:
      """Get the expanded daily feature set, cache-first."""
      cached_data = _read_cache()
      if cached_data is not None:
            return cached_data

      return _fetch_and_cache_v2()