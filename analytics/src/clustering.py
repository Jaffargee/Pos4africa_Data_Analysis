# src/clustering.py
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from src.config import Config

class POSClusterer:
      def __init__(self, n_clusters: int = Config.N_CLUSTERS, features: list[str] = Config.CLUSTERING_FEATURES):
            self.n_clusters = n_clusters
            self.features = features
            self.scaler = StandardScaler()
            self.model = KMeans(n_clusters=n_clusters, random_state=Config.RANDOM_STATE, n_init=10)
            self.is_fitted = False

      def fit_predict(self, df_daily: pd.DataFrame) -> pd.DataFrame:
            """Fits scaler and KMeans model on historical daily features and returns clustered dataframe."""
            df = df_daily.copy()
            df["sale_date"] = pd.to_datetime(df["sale_date"])   # 1. convert to datetime dtype
            df = df.set_index("sale_date")                       # 2. promote to index
            df = df.sort_index()                                 # 3. sort (good practice for time series)
            X = df[self.features]
            X_scaled = self.scaler.fit_transform(X)
            df["cluster"] = self.model.fit_predict(X_scaled)
            self.is_fitted = True
            return df

      def predict_single_day(self, single_day_features: pd.DataFrame) -> int:
            """Predicts the cluster archetype for a new incoming day."""
            if not self.is_fitted:
                  raise ValueError("Model must be fitted before running prediction.")
            X = single_day_features[self.features]
            X_scaled = self.scaler.transform(X)
            return int(self.model.predict(X_scaled)[0])

      def get_cluster_centers(self) -> pd.DataFrame:
            """Returns the cluster centers in the original feature space."""
            if not self.is_fitted:
                  raise ValueError("Model must be fitted before accessing cluster centers.")
            centers_scaled = self.model.cluster_centers_
            centers_original = self.scaler.inverse_transform(centers_scaled)
            return pd.DataFrame(centers_original, columns=self.features)

      def cluster_labels(self) -> list[int]:
            """Returns the cluster labels for each data point in the fitted model."""
            if not self.is_fitted:
                  raise ValueError("Model must be fitted before accessing cluster labels.")
            return self.model.labels_.tolist()

      def label_cluster(self, cluster_id: int) -> str:
            """Returns a descriptive, data-driven label for a fitted cluster.

            Each cluster is described relative to the other cluster centers rather
            than by its arbitrary KMeans ID.  The lower and upper quartiles make
            the labels useful even when the number of clusters changes.
            """
            if not self.is_fitted:
                  raise ValueError("Model must be fitted before labeling clusters.")
            if not isinstance(cluster_id, int) or not 0 <= cluster_id < self.n_clusters:
                  raise ValueError(
                        f"cluster_id must be an integer between 0 and {self.n_clusters - 1}."
                  )

            centers = self.get_cluster_centers()
            center = centers.iloc[cluster_id]
            descriptors: list[str] = []

            def describe(feature: str, high: str, low: str, middle: str) -> None:
                  if feature not in centers.columns:
                        return
                  values = centers[feature]
                  lower = values.quantile(0.25)
                  upper = values.quantile(0.75)
                  value = center[feature]
                  if value >= upper and upper > lower:
                        descriptors.append(high)
                  elif value <= lower and upper > lower:
                        descriptors.append(low)
                  else:
                        descriptors.append(middle)

            describe(
                  "avg_price_per_item",
                  "Premium",
                  "Budget",
                  "Mid-Price",
            )
            describe(
                  "basket_size",
                  "Big-Basket",
                  "Quick-Trip",
                  "Balanced-Basket",
            )
            describe(
                  "avg_order_value",
                  "High-Value",
                  "Low-Value",
                  "Moderate-Value",
            )

            return " / ".join(descriptors) or f"Cluster {cluster_id}"