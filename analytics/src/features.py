# src/features.py
import numpy as np
import pandas as pd
from config import Config

def prepare_daily_features(df_daily: pd.DataFrame) -> pd.DataFrame:
      """Prepares daily aggregated data for clustering."""
      df = df_daily.copy()

      # Avoid division by zero
      num_sales = df["num_sales"].replace(0, np.nan)
      items_sold = df["items_sold"].replace(0, np.nan)

      # Core Behavioral Ratios
      df["avg_price_per_item"] = df["revenue"] / items_sold
      df["basket_size"] = items_sold / num_sales
      df["avg_order_value"] = df["revenue"] / num_sales
      

      return df.fillna(0)

def classify_price_tier(price: float) -> str:
      """Assigns a price tier label to individual line-item unit prices."""
      for tier_label, threshold in Config.PRICE_TIERS.items():
            if price >= threshold:
                  return tier_label
      return "4. Low-Ticket"

def prepare_line_items(df_items: pd.DataFrame) -> pd.DataFrame:
      """Cleans raw transaction line items and adds price tier labels."""
      df = df_items.copy()
      # Filter out voided / return items if calculating gross tier distribution
      df = df[df["quantity"] > 0].copy()
      df["unit_price"] = df["unit_price"].astype(float)
      df["line_total"] = df["quantity"] * df["unit_price"]
      df["price_tier"] = df["unit_price"].apply(classify_price_tier)
      df["sale_date"] = pd.to_datetime(df["invoice_datetime"]).dt.date
      return df