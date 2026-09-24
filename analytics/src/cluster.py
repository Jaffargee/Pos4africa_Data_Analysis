"""
Day-Type Clustering v2 - expanded feature set
===============================================

Extends the original 3-feature model (avg_price_per_item, basket_size,
avg_order_value, all from v_revenue_daily) with features sourced from
v_revenue_daily_v2 (see analytics/sql/v_revenue_daily_v2.sql).

Feature selection reasoning (not "throw everything in"):
  - avg_price_per_item is DROPPED here in favor of the price-tier composition
    (pct_rev_premium/midtier/lowticket). The tier composition is a richer
    version of the same signal - a day with a 20k average could be all-20k
    items, or half 5k + half 35k. The mean can't tell the difference; the
    composition can. Keeping both would double-count the same information.
  - pct_rev_standard is DROPPED (kept implicit) because
    premium + midtier + standard + lowticket = 1 exactly. Including all four
    creates a perfect linear dependency across features, which silently
    inflates that group's weight in the Euclidean distance KMeans uses.
    Any 3 of the 4 fully determine the 4th; we keep 3.
  - basket_size and avg_order_value are KEPT even though avg_order_value
    correlates with basket_size (r=0.64) - not high enough to be redundant,
    and together they distinguish "many small sales" from "few big sales",
    which the tier composition alone can't.
  - day_of_week / is_weekend / is_month_end are EXCLUDED from the clustering
    inputs themselves (one-hot day-of-week would add 6-7 dimensions to a
    162-row dataset for one categorical fact) but are kept on the dataframe
    to check afterward whether any cluster skews toward a particular day type.
  - customer-category based features (wholesale/VIP revenue share) were
    tried and dropped - see cache_v2.py for why (zero-variance in the
    current data, not just weak).

Run `python trend_pattern_v2.py` for the same CLI-style report the original
trend_pattern.py gives, but grounded in this larger feature set.
"""
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from cache2 import get_daily_features_v2

FEATURES = [
    "basket_size",
    "avg_order_value",
    "pct_rev_premium",
    "pct_rev_midtier",
    "pct_rev_lowticket",
    "avg_margin_pct",
    "price_dispersion",
    "distinct_categories",
    "pct_pay_cash",
    "return_rate",
]

N_CLUSTERS = 4
RANDOM_STATE = 42


def prepare_features_v2(df_daily: pd.DataFrame) -> pd.DataFrame:
    df = df_daily.copy()
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df = df.sort_values("sale_date").set_index("sale_date")
    df["basket_size"] = df["items_sold"] / df["num_sales"]
    df["avg_order_value"] = df["revenue"] / df["num_sales"]
    return df


def choose_k(Xs, k_range=range(2, 8)) -> pd.Series:
    """Silhouette score per k - run this whenever the feature set changes.
    A higher score means better-separated clusters; it is NOT a guarantee
    that the clusters mean anything business-wise, only that KMeans found
    a mathematically cleaner split at that k."""
    scores = {}
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10).fit(Xs)
        scores[k] = silhouette_score(Xs, km.labels_)
    return pd.Series(scores)


class DayClustererV2:
    def __init__(self, n_clusters: int = N_CLUSTERS):
        self.n_clusters = n_clusters
        self.scaler = StandardScaler()
        self.model = KMeans(n_clusters=n_clusters, random_state=RANDOM_STATE, n_init=10)

    def fit_predict(self, df_features: pd.DataFrame) -> pd.DataFrame:
        X = df_features[FEATURES].values
        Xs = self.scaler.fit_transform(X)
        df_features = df_features.copy()
        df_features["cluster"] = self.model.fit_predict(Xs)
        return df_features

    def get_cluster_centers(self) -> pd.DataFrame:
        centers = self.scaler.inverse_transform(self.model.cluster_centers_)
        return pd.DataFrame(centers, columns=FEATURES)


def assign_archetypes(clusterer: DayClustererV2, clustered_df: pd.DataFrame) -> dict[int, str]:
    """Rank fitted centroids against each other to name each cluster.
    Never hardcode {0: "...", 1: "..."} - cluster ids are arbitrary and can
    be reassigned on refit."""
    centers = clusterer.get_cluster_centers().copy()
    centers["n_days"] = clustered_df.groupby("cluster").size()
    centers["revenue_mean"] = clustered_df.groupby("cluster")["revenue"].mean()

    remaining = set(centers.index)
    mapping: dict[int, str] = {}

    wholesale_id = centers.loc[list(remaining), "basket_size"].idxmax()
    mapping[wholesale_id] = "Mega Bulk / Wholesale Surge"
    remaining.discard(wholesale_id)

    premium_id = centers.loc[list(remaining), "pct_rev_premium"].idxmax()
    mapping[premium_id] = "Premium / High-Ticket Day"
    remaining.discard(premium_id)

    standard_id = centers.loc[list(remaining), "n_days"].idxmax()
    mapping[standard_id] = "Steady Retail (Standard Day)"
    remaining.discard(standard_id)

    # Whatever's left: name it from what actually distinguishes it, rather
    # than forcing it into a pre-conceived bucket. In this dataset it comes
    # out as a midtier-dominant, high-category-diversity day - NOT "quiet" -
    # so don't call it "Quiet/Low-Basket" just because it's the leftover.
    leftover_id = remaining.pop()
    mapping[leftover_id] = "Mid-Tier / Broad-Basket Day"

    return mapping


if __name__ == "__main__":
    pd.set_option("display.width", 160)
    raw = get_daily_features_v2()
    df_daily = pd.DataFrame(raw)
    df_features = prepare_features_v2(df_daily)

    Xs = StandardScaler().fit_transform(df_features[FEATURES].values)
    print("=== Silhouette score by k ===")
    print(choose_k(Xs).round(4))
    print()

    clusterer = DayClustererV2()
    clustered = clusterer.fit_predict(df_features)
    archetypes = assign_archetypes(clusterer, clustered)
    clustered["archetype"] = clustered["cluster"].map(archetypes)

    print("=== Cluster centers ===")
    centers = clusterer.get_cluster_centers()
    centers["n_days"] = clustered.groupby("cluster").size().values
    centers["archetype"] = [archetypes[c] for c in centers.index]
    print(centers.round(3))
    print()
    print("=== Days per archetype ===")
    print(clustered["archetype"].value_counts())