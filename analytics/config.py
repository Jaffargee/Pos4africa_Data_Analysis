
class Config:

      N_CLUSTERS = 4
      FEATURES = ["avg_price_per_item", "basket_size", "avg_order_value"]
      RANDOM_STATE = 42

      CLUSTERING_FEATURES = ["avg_price_per_item", "basket_size", "avg_order_value"]

      PRICE_TIERS = {
            "1. High-End Premium": 100000,
            "2. Mid-Tier": 30000,
            "3. Standard": 15000,
            "4. Low-Ticket": 0,
      }