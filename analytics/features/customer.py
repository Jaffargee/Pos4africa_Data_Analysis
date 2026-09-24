import pandas as pd

"""
Customer Intelligence
      Variable Segments:
            Recency
            Frequency
            Monetary Value
      Buying Preferences:
            Category
            Price range
            Product Tier
            Dominant Category Share
            Largests sales/transactions & their contribution to revenue
            Items with highest quantity of purchases.
            Timing.
      Location & Origin:
            Kano local vs Out-of-State
            Cross border
            Regionality

Tables & Query = (Anything that has to be related with customers)
customers
sales
sale_items
items

customer & sales & sale_items & items:
      Total Revenue
      Num of items bought
      Gross profit
      Num of sales days
      Active Days Ratio = (Total Num of sales - Num of sales days)
      Top Products
      Top Category
      Top Product Tier
      Which Tier Generates more profit
            quantity
            revenue
            
"""
import pandas as pd
import numpy as np


class CustomerFeature:
      def __init__(self, reference_date: pd.Timestamp | None = None) -> None:
            # RFM recency needs a fixed "as of" date — default to now, but pass one
            # explicitly when building historical/training snapshots so recency
            # doesn't keep drifting every time you rerun the pipeline.
            self.reference_date = reference_date or pd.Timestamp.now().normalize()

      # ---------- Recency, Frequency, Monetary ----------
      def rfm(self, sales_df: pd.DataFrame) -> pd.DataFrame:
            if sales_df is None:
                  raise ValueError("sales_df must be provided.")

            s = sales_df.copy()
            s["sale_date"] = pd.to_datetime(s["invoice_datetime"]).dt.tz_localize(None).dt.normalize()

            rfm = s.groupby("pos_customer_id").agg(
                  last_purchase_date=("sale_date", "max"),
                  first_purchase_date=("sale_date", "min"),
                  frequency=("pos_sale_id", "count"),
                  monetary_value=("invoice_total", "sum"),
            ).reset_index()

            rfm["recency_days"] = (self.reference_date - rfm["last_purchase_date"]).dt.days
            rfm["tenure_days"] = (self.reference_date - rfm["first_purchase_date"]).dt.days
            return rfm

      # ---------- Purchase consistency ----------
      def purchase_consistency(self, sales_df: pd.DataFrame) -> pd.DataFrame:
            if sales_df is None:
                  raise ValueError("sales_df must be provided.")

            s = sales_df.copy()
            s["sale_date"] = pd.to_datetime(s["invoice_datetime"]).dt.tz_localize(None).dt.normalize()

            consistency = s.groupby("pos_customer_id").agg(
                  num_sale_days=("sale_date", "nunique"),
                  first_purchase_date=("sale_date", "min"),
            ).reset_index()

            # "Active Days Ratio" from your draft (num_sales - num_sale_days) doesn't
            # give a ratio at all — it's just "extra sales beyond one-per-active-day,"
            # which conflates buying frequency with consistency. What you actually
            # want is: of the days this customer *could* have bought (their tenure),
            # what share did they actually show up on.
            consistency["tenure_days"] = (self.reference_date - consistency["first_purchase_date"]).dt.days.clip(lower=1)
            consistency["active_days_ratio"] = consistency["num_sale_days"] / consistency["tenure_days"]
            return consistency[["pos_customer_id", "num_sale_days", "active_days_ratio"]]

      # ---------- Buying preferences: category, tier ----------
      def buying_preferences(self, sale_items_df: pd.DataFrame, sales_df: pd.DataFrame, items_df: pd.DataFrame) -> pd.DataFrame:
            if sale_items_df is None or sales_df is None or items_df is None:
                  raise ValueError("sale_items_df, sales_df, and items_df must all be provided.")

            li = sale_items_df.merge(
                  items_df[["pos_item_id", "category", "price_tier"]], on="pos_item_id", how="left"
            ).merge(
                  sales_df[["pos_sale_id", "pos_customer_id"]], on="pos_sale_id", how="left"
            )
            # sales with no attached customer (anonymous) can't feed customer-level preferences
            li = li.dropna(subset=["pos_customer_id"])

            cat_rev = li.groupby(["pos_customer_id", "category"])["total"].sum().reset_index()
            cat_share = cat_rev.groupby("pos_customer_id").apply(
                  lambda g: pd.Series({
                        "dominant_category": g.loc[g["total"].idxmax(), "category"],
                        "dominant_category_share": g["total"].max() / g["total"].sum(),
                  })
            ).reset_index()

            tier_rev = li.groupby(["pos_customer_id", "price_tier"], observed=True)["total"].sum().reset_index()
            tier_share = tier_rev.groupby("pos_customer_id").apply(
                  lambda g: pd.Series({
                        "dominant_tier": g.loc[g["total"].idxmax(), "price_tier"],
                        "dominant_tier_share": g["total"].max() / g["total"].sum(),
                  })
            ).reset_index()

            return cat_share.merge(tier_share, on="pos_customer_id", how="outer")

      # ---------- Profit contribution by tier ----------
      def profit_by_tier(self, sale_items_df: pd.DataFrame, sales_df: pd.DataFrame, items_df: pd.DataFrame) -> pd.DataFrame:
            if sale_items_df is None or sales_df is None or items_df is None:
                  raise ValueError("sale_items_df, sales_df, and items_df must all be provided.")

            li = sale_items_df.merge(
                  items_df[["pos_item_id", "price_tier"]], on="pos_item_id", how="left"
            ).merge(
                  sales_df[["pos_sale_id", "pos_customer_id"]], on="pos_sale_id", how="left"
            ).dropna(subset=["pos_customer_id"])

            profit = (
                  li.groupby(["pos_customer_id", "price_tier"], observed=True)["gross_profit"]
                  .sum()
                  .unstack(fill_value=0)
            )
            profit.columns = [f"profit_{c}" for c in profit.columns]
            return profit.reset_index()

      # ---------- Transaction concentration (whale-sale share, per customer) ----------
      def transaction_concentration(self, sales_df: pd.DataFrame) -> pd.DataFrame:
            if sales_df is None:
                  raise ValueError("sales_df must be provided.")

            def summarize(g: pd.DataFrame) -> pd.Series:
                  return pd.Series({
                        "largest_sale_share": g["invoice_total"].max() / g["invoice_total"].sum(),
                        "largest_sale_value": g["invoice_total"].max(),
                  })

            return sales_df.groupby("pos_customer_id").apply(summarize).reset_index()

      # ---------- Timing preference ----------
      def timing(self, sales_df: pd.DataFrame) -> pd.DataFrame:
            if sales_df is None:
                  raise ValueError("sales_df must be provided.")

            s = sales_df.copy()
            s["sale_date"] = pd.to_datetime(s["invoice_datetime"]).dt.tz_localize(None)
            s["is_weekend"] = s["sale_date"].dt.dayofweek >= 5

            timing = s.groupby("pos_customer_id")["is_weekend"].mean().reset_index()
            timing = timing.rename(columns={"is_weekend": "weekend_purchase_share"})
            return timing

      # ---------- Location ----------
      def location(self, customers_df: pd.DataFrame, addresses_df: pd.DataFrame) -> pd.DataFrame:
            if customers_df is None or addresses_df is None:
                  raise ValueError("customers_df and addresses_df must both be provided.")

            primary = addresses_df[addresses_df["is_primary"] == True]
            loc = customers_df[["pos_customer_id"]].merge(
                  primary[["pos_customer_id", "state", "country"]],
                  left_on="pos_customer_id", right_on="pos_customer_id", how="left"
            )
            # ~41% of customers have no address on file at all — make that explicit
            # rather than letting it silently become NaN downstream.
            loc["state"] = loc["state"].fillna("unknown")
            loc["is_kano_local"] = loc["state"].str.lower() == "kano"
            # every address on file is Nigeria right now — this column will be
            # constant (always False) until you actually have cross-border customers.
            # Keep the plumbing so it activates automatically later, but don't feed
            # it into any model as-is; a zero-variance feature is dead weight.
            loc["is_cross_border"] = loc["country"].notna() & (loc["country"].str.lower() != "nigeria")

            return loc[["pos_customer_id", "state", "is_kano_local", "is_cross_border"]]