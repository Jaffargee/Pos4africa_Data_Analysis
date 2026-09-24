import pandas as pd

class DailyFeatures:
      def __init__(self) -> None:
            pass

      """
            Daily datasets contains, num_of_sales, revenue, items_sold, sale_date
            the avg() method average of those columns to create another field all part of the feature engineering.
            1. Average price per item - API
            2. Average order per transaction - AOP
            3. Average Items per transaction - AIP
      """
      def avg(self, df: pd.DataFrame) -> pd.DataFrame:
            if df is None:
                  raise ValueError("Pandas DataFrame is not provided.")

            n_sales = df["num_sales"]
            ni_sold = df["items_sold"]
            revenue = df["revenue"]

            df["avg_price_per_item"] = revenue / ni_sold
            df["avg_order_value"] = revenue / n_sales
            df["avg_basket_size"] = ni_sold / n_sales

            return df

      def threshold(self, df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
            if df is None:
                  raise ValueError("Pandas DataFrame is not provided.")
            if sales_df is None or sales_df.empty:
                  df["revenue_share_from_large_sales"] = 0.0
                  return df

            THRESHOLD = 500_000

            # 1. Normalize sales dates
            sales_df = sales_df.copy()
            sales_df["sale_date_clean"] = sales_df["invoice_datetime"].dt.normalize()

            # 2. Compute total revenue per day
            daily_total = sales_df.groupby("sale_date_clean")["invoice_total"].sum()

            # 3. Compute large sales revenue per day
            large_sales = sales_df[sales_df["invoice_total"] >= THRESHOLD]
            daily_large = large_sales.groupby("sale_date_clean")["invoice_total"].sum()

            # 4. Calculate ratio per day (fill 0 for dates with no large sales)
            daily_ratio = (daily_large / daily_total).fillna(0.0)

            # 5. Map results back to df based on normalized sale_date
            df["sale_date_clean"] = df["sale_date"].dt.normalize()
            df["revenue_share_from_large_sales"] = df["sale_date_clean"].map(daily_ratio).fillna(0.0)

            # Clean up temporary column
            df.drop(columns=["sale_date_clean"], inplace=True)

            return df

      def is_weekend(self, df: pd.DataFrame) -> pd.DataFrame:
            df['is_weekend'] = df['sale_date'].dt.dayofweek >= 5
            df['day_name'] = df['sale_date'].dt.day_name()
            df['day_num'] = df['sale_date'].dt.dayofweek
            return df

      def is_end_of_month(self, df: pd.DataFrame) -> pd.DataFrame:
            df['is_end_of_month'] = df['sale_date'].dt.day >= 25
            return df

      def is_start_of_month(self, df: pd.DataFrame) -> pd.DataFrame:
            df['is_start_of_month'] = df['sale_date'].dt.day <= 5
            return df
      
      def category_concentration(self, df: pd.DataFrame, sales_df: pd.DataFrame, sale_items_df: pd.DataFrame, items_df: pd.DataFrame) -> pd.DataFrame:
            if df is None or sales_df is None or sale_items_df is None or items_df is None:
                  raise ValueError("df, sales_df, sale_items_df, and items_df must all be provided.")

            sales = pd.merge(sale_items_df, items_df[["pos_item_id", "category"]], on="pos_item_id", how="left")
            sales = pd.merge(sales, sales_df[["pos_sale_id", "invoice_datetime"]], on="pos_sale_id", how="left")
            sales["sale_date"] = pd.to_datetime(sales["invoice_datetime"]).dt.tz_localize(None).dt.normalize()

            uncategorized = sales["category"].isna().sum()
            if uncategorized:
                  print(f"Warning: {uncategorized} sale_items rows have no matching category and will be dropped.")
            sales = sales.dropna(subset=["category"])

            daily_cat_revenue = (
                  sales.groupby(["sale_date", "category"])["total"]
                  .sum()
                  .reset_index()
            )
            daily_cat_revenue["day_total"] = daily_cat_revenue.groupby("sale_date")["total"].transform("sum")
            daily_cat_revenue["category_share"] = daily_cat_revenue["total"] / daily_cat_revenue["day_total"]

            def summarize(group: pd.DataFrame) -> pd.Series:
                  top = group.loc[group["category_share"].idxmax()]
                  hhi = (group["category_share"] ** 2).sum()
                  return pd.Series({
                        "dominant_category": top["category"],
                        "dominant_category_share": top["category_share"],
                        "category_concentration_hhi": hhi,
                  })

            daily_summary = (
                  daily_cat_revenue.groupby("sale_date")
                  .apply(summarize)
                  .reset_index()
            )

            # fold per-day results into the passed-in df — one merge, not a scalar smear per group
            df = df.merge(daily_summary, on="sale_date", how="left")

            return df
      

      def customer_structure(self, df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
            if df is None or sales_df is None:
                  raise ValueError("df and sales_df must both be provided.")

            sales = sales_df.copy()
            sales["sale_date"] = pd.to_datetime(sales["invoice_datetime"]).dt.tz_localize(None).dt.normalize()

            # anonymous sales have no reliable customer id — can't be classified new/returning, exclude from this ratio
            identified = sales[sales["is_anonymous_customer"] == False].copy()

            # first-ever purchase date per customer, across the whole history (not just this df's date range)
            first_purchase = (
                  identified.groupby("pos_customer_id")["sale_date"]
                  .min()
                  .rename("first_purchase_date")
            )
            identified = identified.merge(first_purchase, on="pos_customer_id", how="left")
            identified["is_new"] = identified["sale_date"] == identified["first_purchase_date"]

            daily_counts = (
                  identified.groupby("sale_date")["is_new"]
                  .agg(new_sales="sum", total_sales="count")
                  .reset_index()
            )
            daily_counts["new_customer_share"] = daily_counts["new_sales"] / daily_counts["total_sales"]
            daily_counts["returning_customer_share"] = 1 - daily_counts["new_customer_share"]

            daily_summary = daily_counts[["sale_date", "new_customer_share", "returning_customer_share"]]

            df = df.merge(daily_summary, on="sale_date", how="left")
            return df