from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.models.customer import Customer
from pos4africa.worker.components.base import BaseComponent
from pos4africa.config.settings import settings
import os

class CustomerScraper(BaseComponent):
      
      def __init__(self, node_id: str, memory: MemoryStore):
            super().__init__(node_id=node_id, memory=memory)

      async def run(self, excel_path: str | Path, sheet_name: str | int = 0) -> None:
            customers = self._scrape(excel_path=excel_path, sheet_name=sheet_name)
            return customers
      
      def _scrape(self, excel_path: str | Path, sheet_name: str | int = 0) -> list[Customer]:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            df = self._normalise_dataframe(df)

            if df.empty:
                  return []

            customers: list[Customer] = []

            for _, row in df.iterrows():
                  customer = self._build_customer(row)
                  if customer is not None:
                        customers.append(customer)

            return customers

      def _build_customer(self, row: pd.Series) -> Customer | None:
            pos_customer_id = row.get("pos_customer_id")
            if pd.isna(pos_customer_id):
                  return None

            first_name = row.get("first_name")
            last_name = row.get("last_name")

            customer = Customer(
                  pos_customer_id=int(pos_customer_id),
                  first_name=str(first_name) if not pd.isna(first_name) else None,
                  last_name=str(last_name) if not pd.isna(last_name) else None,
            )
            return customer

      def _normalise_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            df.columns = [self._normalise_column_name(col) for col in df.columns]
            df = df.rename(columns=self._column_mapping())
            return df

      def _normalise_column_name(self, column: Any) -> str:
            return re.sub(r"[^a-z0-9]+", "_", str(column).strip().lower()).strip("_")

      def _column_mapping(self) -> dict[str, str]:
            return {
                  "customer_id": "pos_customer_id",
                  "first_name": "first_name",
                  "last_name": "last_name",
            }