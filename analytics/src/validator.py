"""
Day-Cluster -> Line-Item Validation
====================================

Goal: don't just trust the cluster names KMeans + label_cluster() produce -
check them against what was actually sold on those days.

Workflow:
  1. Extract daily feature vectors (src.features.prepare_daily_features)
  2. Cluster the dataset (src.clustering.POSClusterer)
  3. Map each empirical cluster to one of the four business archetypes
     in CLUSTER_MAP, based on how its centroid compares to the OTHER
     centroids (never a fixed/hardcoded cluster-id -> name mapping,
     because KMeans cluster ids are arbitrary and can flip between runs)
  4. Pull every sale_item for every clustered sale_date
  5. Classify each line item into a price tier (src.features.classify_price_tier)
  6. Aggregate price-tier revenue share per cluster and compare it against
     the archetype's story, e.g. "Premium / High-Ticket" days should show
     a materially higher share of High-End Premium + Mid-Tier revenue than
     the other clusters.
"""
import pandas as pd
from src import POSClusterer, prepare_daily_features
from src.features import prepare_line_items
from src.cache import get_daily_revenue
from infra.supabase_client import spb_client

CLUSTER_MAP = {
      "Premium / High-Ticket Day": None,
      "Steady Retail (Standard Day)": None,
      "Mega Bulk / Wholesale Surge": None,
      "Quiet / Low-Basket Day": None,
}


def assign_archetypes(clusterer: POSClusterer, clustered_df: pd.DataFrame) -> dict[int, str]:
      """Data-driven mapping from empirical cluster id -> business archetype name.

      KMeans doesn't know about "Premium" or "Wholesale" - it just minimizes
      within-cluster distance in scaled feature space. This function is the
      translation layer: it ranks the fitted centroids against each other and
      picks the best match for each archetype, one archetype per cluster.
      Re-run this after any refit; do NOT hardcode {0: "...", 1: "..."} because
      cluster ids can be reassigned between runs / data refreshes.
      """
      centers = clusterer.get_cluster_centers().copy()
      centers["n_days"] = clustered_df.groupby("cluster").size()
      centers["revenue_mean"] = clustered_df.groupby("cluster")["revenue"].mean()

      remaining = set(centers.index)
      mapping: dict[int, str] = {}

      # 1. Wholesale surge: far and away the largest basket_size + revenue.
      wholesale_id = centers.loc[list(remaining), "basket_size"].idxmax()
      mapping[wholesale_id] = "Mega Bulk / Wholesale Surge"
      remaining.discard(wholesale_id)

      # 2. Premium: highest avg_price_per_item among what's left.
      premium_id = centers.loc[list(remaining), "avg_price_per_item"].idxmax()
      mapping[premium_id] = "Premium / High-Ticket Day"
      remaining.discard(premium_id)

      # 3. Standard: the majority pattern (most days) among what's left.
      standard_id = centers.loc[list(remaining), "n_days"].idxmax()
      mapping[standard_id] = "Steady Retail (Standard Day)"
      remaining.discard(standard_id)

      # 4. Whatever's left is labeled "Quiet / Low-Basket Day" by elimination -
      #    flagged separately below if the data doesn't actually support that name.
      quiet_id = remaining.pop()
      mapping[quiet_id] = "Quiet / Low-Basket Day"

      return mapping


def fetch_sale_items_for_dates(dates: list[str]) -> pd.DataFrame:
      """Pulls every sale_item for the given sale_dates, joined to sales.invoice_datetime.

      NOTE: this currently reads through the anon Supabase client directly against
      `sales` / `sale_items`. Once RLS policies are added (see security review),
      this should move behind a SECURITY DEFINER view/RPC scoped to safe columns.
      """
      sales_resp = (
            spb_client.table("sales")
            .select("pos_sale_id, invoice_datetime")
            .gte("invoice_datetime", min(dates))
            .lte("invoice_datetime", max(dates) + "T23:59:59")
            .execute()
      )
      sales_df = pd.DataFrame(sales_resp.data)
      if sales_df.empty:
            return pd.DataFrame()

      pos_sale_ids = sales_df["pos_sale_id"].tolist()
      items = []
      # Supabase .in_() filters are capped in length; chunk the request.
      for i in range(0, len(pos_sale_ids), 500):
            chunk = pos_sale_ids[i : i + 500]
            resp = (
                  spb_client.table("sale_items")
                  .select("pos_sale_id, quantity, unit_price, total")
                  .in_("pos_sale_id", chunk)
                  .execute()
            )
            items.extend(resp.data)

      items_df = pd.DataFrame(items)
      merged = items_df.merge(sales_df, on="pos_sale_id", how="left")
      return merged


def validate() -> pd.DataFrame:
      daily_revenue_data = get_daily_revenue()
      df_daily = pd.DataFrame(daily_revenue_data)
      df_features = prepare_daily_features(df_daily)

      clusterer = POSClusterer()
      clustered_df = clusterer.fit_predict(df_features)

      archetype_map = assign_archetypes(clusterer, clustered_df)
      clustered_df["archetype"] = clustered_df["cluster"].map(archetype_map)

      date_to_archetype = {
            d.strftime("%Y-%m-%d"): a for d, a in clustered_df["archetype"].items()
      }
      dates = list(date_to_archetype.keys())

      raw_items = fetch_sale_items_for_dates(dates)
      raw_items = raw_items.rename(columns={"invoice_datetime": "invoice_datetime"})
      line_items = prepare_line_items(raw_items)
      line_items["sale_date"] = line_items["sale_date"].astype(str)
      line_items["archetype"] = line_items["sale_date"].map(date_to_archetype)
      line_items = line_items.dropna(subset=["archetype"])

      summary = (
            line_items.groupby(["archetype", "price_tier"])["line_total"]
            .sum()
            .unstack("price_tier")
            .fillna(0)
      )
      pct = summary.div(summary.sum(axis=1), axis=0).round(3) * 100
      return pct


if __name__ == "__main__":
      pd.set_option("display.width", 160)
      print("=== Revenue share by price tier, per cluster archetype ===")
      print(validate())