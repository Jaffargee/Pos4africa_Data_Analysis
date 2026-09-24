import pandas as pd
from config import Config

class ItemsFeature:
      def __init__(self) -> None:
            pass

      def price_tier(self, items_df: pd.DataFrame) -> pd.DataFrame:
            if items_df is None:
                  raise ValueError("items_df must be provided.")

            df = items_df.copy()
            df["price_tier"] = pd.NA

            df["price_tier"] = pd.qcut(
                  df["selling_price"],
                  q=[0, 0.15, 0.30, 0.45, 0.60, 0.85, 1.0],
                  labels=Config.PRICE_TIER,
            )

            return df

      def contribution_of_price_tier_to_revenue(self, items_df: pd.DataFrame, si_df: pd.DataFrame) -> pd.DataFrame:

            df = items_df.copy()

            df = si_df.merge(items_df[["pos_item_id", "price_tier"]], on="pos_item_id", how="left")

            missing = df["price_tier"].isna().sum()
            
            if missing:
                  print(f"Warning: {missing} sale_items rows have no price_tier (item inactive or unmatched) and will be excluded.")
            df = df.dropna(subset=["price_tier"])

            tier_summary = df.groupby("price_tier", observed=True).agg(
                  revenue=("total", "sum"),
                  quantity=("quantity", "sum"),
                  gross_profit=("gross_profit", "sum"),
            ).reset_index()

            tier_summary["revenue_share"] = tier_summary["revenue"] / tier_summary["revenue"].sum()
            tier_summary["quantity_share"] = tier_summary["quantity"] / tier_summary["quantity"].sum()
            tier_summary["profit_share"] = tier_summary["gross_profit"] / tier_summary["gross_profit"].sum()
            tier_summary["margin_pct"] = tier_summary["gross_profit"] / tier_summary["revenue"]

            return tier_summary.sort_values("revenue", ascending=False).reset_index(drop=True)

