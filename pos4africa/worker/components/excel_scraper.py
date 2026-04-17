from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.models.sale import RawPayment, RawSale, RawSaleItem
from pos4africa.worker.components.base import BaseComponent


class ExcelScraper(BaseComponent):
      """
      Excel-backed alternative to the HTML Scraper.

      Reads the DSR-style workbook, groups item rows by sale ID, and emits the
      same RawSale family of models expected by the parser and processor.
      """

      _ANONYMOUS_ACCOUNTS = ("indoor", "online", "online customers")
      _PAYMENT_PATTERN = re.compile(
            r"([A-Za-z ]+):\s*([+-]?\s*[-]?\s*(?:N|₦)?[\d,]+(?:\.\d{1,2})?)",
            flags=re.IGNORECASE,
      )

      def __init__(self, node_id: str, memory: MemoryStore):
            super().__init__(node_id=node_id, memory=memory)

      async def run(
            self,
            excel_path: str | Path,
            sale_id: int | None = None,
            sheet_name: str | int = 0,
      ) -> list[RawSale] | RawSale | None:
            sales = self._scrape(excel_path=excel_path, sheet_name=sheet_name)

            if sale_id is None:
                  return sales

            for sale in sales:
                  if sale.pos_sale_id and int(sale.pos_sale_id) == int(sale_id):
                        return sale

            return None

      def _scrape(self, excel_path: str | Path, sheet_name: str | int = 0) -> list[RawSale]:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            df = self._normalise_dataframe(df)

            if df.empty:
                  return []

            grouped_sales: list[RawSale] = []
            for _, group in df.groupby("sale_id", sort=True):
                  sale = self._build_sale(group.reset_index(drop=True))
                  if sale is not None:
                        grouped_sales.append(sale)

            return grouped_sales

      def _normalise_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            df.columns = [self._normalise_column_name(col) for col in df.columns]
            df = df.rename(columns=self._column_map())

            if "sale_id" not in df.columns:
                  raise ValueError("Expected a 'Sale Id' column in the Excel source.")

            df["sale_id"] = df["sale_id"].apply(self._parse_sale_id)
            df = df.dropna(subset=["sale_id"]).copy()
            df["sale_id"] = df["sale_id"].astype(int)

            return df

      def _build_sale(self, group: pd.DataFrame) -> RawSale | None:
            if group.empty:
                  return None

            header = group.iloc[0]
            customer_name = self._clean_string(header.get("customer_name"))
            payments = self._parse_payments(header.get("payment_type"))

            return RawSale(
                  pos_sale_id=str(int(header["sale_id"])),
                  invoice_datetime=self._stringify(header.get("invoice_datetime")),
                  salesperson=self._clean_string(header.get("salesperson")),
                  customer_name=customer_name,
                  is_anonymous_customer=self._is_anonymous_customer(customer_name),
                  invoice_total=self._stringify_number(header.get("invoice_total")),
                  items_sold=self._stringify_number(header.get("items_sold")),
                  items_returned="0",
                  change_due=self._derive_change_due(payments),
                  comment=self._clean_string(header.get("comment")),
                  items=[self._build_item(row) for _, row in group.iterrows()],
                  payments=payments,
            )

      def _build_item(self, row: pd.Series) -> RawSaleItem:
            return RawSaleItem(
                  pos_sale_id=self._stringify_number(row.get("sale_id")),
                  pos_item_id=self._stringify_number(row.get("pos_item_id")),
                  name=self._clean_string(row.get("item_name")),
                  quantity=self._stringify_number(row.get("quantity")),
                  unit_price=self._stringify_number(row.get("unit_price")),
                  total=self._stringify_number(row.get("line_total")),
            )

      def _parse_payments(self, value: Any) -> list[RawPayment]:
            text = self._clean_string(value)
            if not text:
                  return []

            payments: list[RawPayment] = []
            for channel, amount in self._PAYMENT_PATTERN.findall(text):
                  channel_name = re.sub(r"\s+", " ", channel).strip().upper()
                  clean_amount = self._normalise_payment_amount(amount)

                  if channel_name:
                        payments.append(
                              RawPayment(channel=channel_name, amount=clean_amount)
                        )

            return payments

      def _derive_change_due(self, payments: list[RawPayment]) -> str:
            for payment in payments:
                  if payment.amount and payment.amount.startswith("-"):
                        return payment.amount.lstrip("-")
            return "0"

      def _is_anonymous_customer(self, customer_name: str | None) -> bool:
            if not customer_name:
                  return False
            lowered = customer_name.lower()
            return any(keyword in lowered for keyword in self._ANONYMOUS_ACCOUNTS)

      def _parse_sale_id(self, value: Any) -> int | None:
            if pd.isna(value):
                  return None

            match = re.search(r"\d+", str(value).strip())
            return int(match.group()) if match else None

      def _stringify(self, value: Any) -> str | None:
            if pd.isna(value):
                  return None
            text = str(value).strip()
            return text or None

      def _stringify_number(self, value: Any) -> str | None:
            if pd.isna(value):
                  return None

            if isinstance(value, str):
                  text = value.strip()
                  return text or None

            if isinstance(value, float) and value.is_integer():
                  return str(int(value))

            return str(value)

      def _clean_string(self, value: Any) -> str | None:
            if pd.isna(value):
                  return None
            text = re.sub(r"\s+", " ", str(value)).strip()
            return text or None

      def _normalise_payment_amount(self, amount: str) -> str:
            compact = amount.replace(" ", "")
            compact = compact.replace("N", "").replace("₦", "")
            if compact.startswith("--"):
                  compact = compact[1:]
            return compact

      def _normalise_column_name(self, column: Any) -> str:
            return re.sub(r"[^a-z0-9]+", "_", str(column).strip().lower()).strip("_")

      def _column_map(self) -> dict[str, str]:
            return {
                  "sale_id": "sale_id",
                  "date": "invoice_datetime",
                  "sold_by": "salesperson",
                  "sold_to": "customer_name",
                  "items_purchased": "items_sold",
                  "total_1": "invoice_total",
                  "payment_type": "payment_type",
                  "comments": "comment",
                  "item_id": "pos_item_id",
                  "name": "item_name",
                  "quantity_sold": "quantity",
                  "selling_price": "unit_price",
                  "total": "line_total",
                  "person_id": "pos_customer_id",
            }
