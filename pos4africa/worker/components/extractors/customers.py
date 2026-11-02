from __future__ import annotations

from pathlib import Path

import pandas as pd

from pos4africa.shared.models.customer import Customer
from pos4africa.worker.components.base import BaseComponent
from pos4africa.worker.components.extractors.extractor import Extractor
from pydantic import RootModel

class CustomerExtractor(BaseComponent, Extractor):
      
      async def run(self, excel_path: str | Path, sheet_name: str | int = 0) -> None:
            customers = self._extract(excel_path=excel_path, sheet_name=sheet_name)
            return customers
      
      def _extract(self, excel_path: str | Path, sheet_name: str | int = 0) -> list[Customer]:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            df = self._normalise_dataframe(df)

            if df.empty:
                  return []

            df = df.dropna(subset=["pos_customer_id"])
            df["pos_customer_id"] = df["pos_customer_id"].astype(int)

            if "state_province" in df.columns:
                  df = df.rename(columns={"state_province": "state"})

            if "phone_number" in df.columns:
                  # Convert column values to string, handling floats cleanly
                  df["phone_number"] = (df["phone_number"].astype(str).str.replace(r"\.0$", "", regex=True))

            df = df.replace({pd.NA: None, float('nan'): None})
            records = df.to_dict(orient="records")

            return RootModel[list[Customer]](records).root


      def _build_customer(self, row: pd.Series) -> Customer | None:
            pos_customer_id = row.get("pos_customer_id")
            if pd.isna(pos_customer_id):
                  return None

            first_name = row.get("first_name")
            last_name = row.get("last_name")
            phone_no = row.get("phone_number")
            state_province = row.get("state_province")

            customer = Customer(
                  pos_customer_id=int(pos_customer_id),
                  first_name=str(first_name) if not pd.isna(first_name) else None,
                  last_name=str(last_name) if not pd.isna(last_name) else None,
                  phone_number=str(phone_no) if not pd.isna(phone_no) else None,
                  state=str(state_province) if not pd.isna(state_province) else None,
            )
            return customer

      def _column_map(self) -> dict[str, str]:
            return {
                  "customer_id": "pos_customer_id",
                  "first_name": "first_name",
                  "last_name": "last_name",
                  "state_province": "state",
            }