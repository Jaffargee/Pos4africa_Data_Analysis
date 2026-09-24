import pandas as pd
from src import POSClusterer, prepare_daily_features
from src.cache import get_daily_revenue


if __name__ == "__main__":
      daily_revenue_data = get_daily_revenue()
      df_daily = pd.DataFrame(daily_revenue_data)

      df_features = prepare_daily_features(df_daily)
      clusterer = POSClusterer()
      clustered_df = clusterer.fit_predict(df_features)

      # Attach human-readable cluster labels to every daily record
      clustered_df["cluster_label"] = clustered_df["cluster"].apply(
            clusterer.label_cluster
      )

      date_index = pd.DatetimeIndex(clustered_df.index)
      clustered_df["day_name"] = date_index.day_name()
      clustered_df["is_weekend"] = date_index.dayofweek >= 5

      print(clustered_df.groupby("cluster_label").agg(
            num_days=("cluster", "count"),
            avg_revenue=("revenue", "mean"),
            avg_num_sales=("num_sales", "mean"),
      ).sort_values("avg_revenue", ascending=False))

      # day_cluster_matrix = pd.crosstab(clustered_df["day_name"], clustered_df["cluster_label"])
      # print("=== Cluster Archetype Frequency by Day of Week ===")
      # print(day_cluster_matrix)

      # View labeled historical days
      # print(clustered_df[["day_name", "is_weekend", "revenue", "num_sales", "cluster", "cluster_label"]].head(30))