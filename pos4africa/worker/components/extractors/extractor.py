
from __future__ import annotations

from abc import abstractmethod, ABC
from typing import Any
import re
from pathlib import Path
import pandas as pd

class Extractor(ABC):

      def __init__(self):
            pass


      @abstractmethod
      def _extract(self, excel_path: str | Path, sheet_name: str | int = 0) -> Any:
            """Execute this component's stage. Kwargs vary per component."""
            pass

      def _normalise_column_name(self, column: Any) -> str:
            return re.sub(r"[^a-z0-9]+", "_", str(column).strip().lower()).strip("_")

      @abstractmethod
      def _column_map(self) -> dict[str, str]:
            pass

      def _stringify_number(self, value: Any) -> str | None:
            if pd.isna(value):
                  return None

            if isinstance(value, str):
                  text = value.strip()
                  return text or None

            if isinstance(value, float) and value.is_integer():
                  return str(int(value))

            return str(value)

      def _stringify(self, value: Any) -> str | None:
            if pd.isna(value):
                  return None
            text = str(value).strip()
            return text or None

      
      def _clean_string(self, value: Any) -> str | None:
            if pd.isna(value):
                  return None
            text = re.sub(r"\s+", " ", str(value)).strip()
            return text or None

      
      def _normalise_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            df.columns = [self._normalise_column_name(col) for col in df.columns]
            df = df.rename(columns=self._column_map())

            return df