from __future__ import annotations

from pathlib import Path

import pandas as pd

from pos4africa.shared.models.item import Item
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.worker.components.extractors.extractor import Extractor

class CatalogExtractor(BaseComponent, Extractor):

      def __init__(self, node_id: str, memory: MemoryStore):
            super().__init__(node_id, memory)


      async def run(self, excel_path, sheet_name = 0):
            return self._extract(excel_path=excel_path, sheet_name=sheet_name)

      def _extract(self, excel_path, sheet_name = 0) -> list[Item]:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            df = self._normalise_dataframe(df)

            if df.empty:
                  return []

            items: list[Item] = []
            for _, row in df.iterrows():
                  item = self._build_catalog(row)
                  if item is not None:
                        items.append(item)
            return items

      def _build_catalog(self, row: pd.Series) -> Item | None:
            item_id = row.get("item_id")
            if pd.isna(item_id):
                  return None

            item_name = row.get("item_name")
            category = row.get("category")
            cost_price = row.get("cost_price")
            selling_price = row.get("selling_price")
            quantity = row.get("quantity")
            is_barcoded = row.get("is_barcoded")

            if pd.isna(is_barcoded):
                  is_barcoded = False
            else:
                  is_barcoded = bool(is_barcoded)

            return Item(
                  item_id=item_id,
                  item_name=item_name,
                  category=category,
                  cost_price=cost_price,
                  selling_price=selling_price,
                  quantity=quantity,
                  is_barcoded=is_barcoded,
            )

      def _column_map(self):
            return {
                  "item_id": "pos_item_id",
                  "item_name": "item_name",
                  "category": "category",
                  "cost_price": "cost_price",
                  "selling_price": "selling_price",
                  "quantity": "quantity",
                  "is_barcoded": "is_barcoded"
            }