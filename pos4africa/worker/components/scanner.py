from __future__ import annotations


import pandas as pd
from collections.abc import Sequence, Mapping

from pos4africa.worker.components.base import BaseComponent

REQUIRED_COLUMNS = {"pos_customer_id", "pos_sale_id", "comment", "invoice_total"}

class InvalidPayloadError(ValueError):
    """The extractor handed us data that breaks the contract."""

class Scanner(BaseComponent):
      def __init__(self):
            super().__init__()

      def init(self, data: Sequence[Mapping[str, object]]):
            df = self._load_data(data)
            df = self._cast_types(df)
            return df

      def get_comment_frequencies(self, df: pd.DataFrame, min_count: int = 3) -> list[dict]:
            return self._get_comments(df, min_count)

      def _load_data(self, data: Sequence[Mapping[str, object]]) -> pd.DataFrame:
            if data is None:
                  raise InvalidPayloadError(f"expected a list of records, got {type(data).__name__}")
            
            if isinstance(data, (str, bytes)) or not isinstance(data, Sequence):
                  raise InvalidPayloadError(f"expected a list of records, got {type(data).__name__}")

            if len(data) == 0:
                  self.log.info("load_data: 0 rows")
                  return df.DataFrame(columns=REQUIRED_COLUMNS)

            bad = [i for i, row in enumerate(data) if not isinstance(row, Mapping)]
            if bad:
                  raise InvalidPayloadError(f"{len(bad)} non-dict records, first at index {bad[0]}")

            df = pd.DataFrame.from_records(data)
            missing = set(REQUIRED_COLUMNS) - set(df.columns)
            if missing:
                  raise InvalidPayloadError(f"missing columns: {sorted(missing)}")

            self.log.info("load_data: %d rows", len(df))
            return df.loc[:, list(REQUIRED_COLUMNS)]

      def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()

            if "pos_customer_id" in df.columns:
                  df["pos_customer_id"] = pd.to_numeric(df["pos_customer_id"], errors="coerce")

            if "pos_sale_id" in df.columns:
                  df["pos_sale_id"] = pd.to_numeric(df["pos_sale_id"], errors="coerce")

            if "comment" in df.columns:
                  df["comment"] = df["comment"].astype(str).str.strip().str.upper()

            if "customer_name" in df.columns:
                  df["customer_name"] = df["customer_name"].astype(str).str.strip().str.upper()

            return df

      def _get_comments(self, df: pd.DataFrame, min_count: int = 3) -> list[dict]:
            
            if "comment" not in df.columns:
                  raise KeyError("The 'comment' column is missing from the fetched sales data.")
            
            comment_counts = (
                  df.groupby("comment", dropna=False)
                  .size()
                  .reset_index(name="count")
            )
            comment_counts = comment_counts[comment_counts["count"] > min_count]
            comment_counts = comment_counts.sort_values(by="count", ascending=False).reset_index(drop=True)

            return comment_counts.to_dict(orient="records")

      