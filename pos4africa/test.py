"""Compare the POS customer export (source of truth) with the Supabase customers table.

Matching is done on pos_customer_id. Names are only used for the "same id, different name" check.
"""
import asyncio

import pandas as pd

from pos4africa.worker.pipeline.base import PipelineContext
from pos4africa.worker.pipeline.s2c_mapper import S2CMapper

from pos4africa.infra.repository import Repository
from pos4africa.worker.components.syncer import Syncer
from pos4africa.config.settings import settings

EXPORT_CSV = False  # True writes missing_in_db.csv and only_in_db.csv next to the script


def clean_name(s: pd.Series) -> pd.Series:
      """Upper-case, straighten curly apostrophes, collapse repeated spaces, strip the ends."""
      return (
            s.fillna("").astype(str)
            .str.replace("[‘’]", "'", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.upper()
      )


def load_pos_customers(path: str) -> pd.DataFrame:
      df = pd.read_excel(path)[["Customer Id", "First Name", "Last Name"]]
      df = df.rename(columns={
            "Customer Id": "pos_customer_id",
            "First Name": "name",
            "Last Name": "lastname",
      })

      df["pos_customer_id"] = pd.to_numeric(df["pos_customer_id"], errors="coerce")
      df = df.dropna(subset=["pos_customer_id"]).astype({"pos_customer_id": int})

      # strip() after joining so a missing last name leaves no trailing space
      df["customer_name"] = (clean_name(df["name"]) + " " + clean_name(df["lastname"])).str.strip()
      return df[["pos_customer_id", "customer_name"]].reset_index(drop=True)


def load_db_customers(customers: list[dict]) -> pd.DataFrame:
      db = pd.DataFrame(customers)
      for col in ("pos_customer_id", "customer_name"):
            if col not in db.columns:
                  raise KeyError(f"repo.get_customers() rows must include '{col}'")

      db = db[["pos_customer_id", "customer_name"]].copy()
      db["pos_customer_id"] = pd.to_numeric(db["pos_customer_id"], errors="coerce")
      db["customer_name"] = clean_name(db["customer_name"])
      return db


def compare(pos: pd.DataFrame, db: pd.DataFrame) -> dict[str, pd.DataFrame]:
      db_no_id = db[db["pos_customer_id"].isna()]
      db_id = db.dropna(subset=["pos_customer_id"]).astype({"pos_customer_id": int})

      missing_in_db = pos[~pos["pos_customer_id"].isin(db_id["pos_customer_id"])]
      only_in_db = db_id[~db_id["pos_customer_id"].isin(pos["pos_customer_id"])]

      both = pos.merge(db_id.drop_duplicates("pos_customer_id"), on="pos_customer_id", suffixes=("_pos", "_db"))
      renamed = both[both["customer_name_pos"] != both["customer_name_db"]]

      duplicate_ids = db_id[db_id.duplicated("pos_customer_id", keep=False)].sort_values("pos_customer_id")

      return {
            "missing_in_db": missing_in_db.sort_values("pos_customer_id"),
            "only_in_db": only_in_db.sort_values("pos_customer_id"),
            "db_without_pos_id": db_no_id,
            "renamed": renamed.sort_values("pos_customer_id"),
            "duplicate_ids": duplicate_ids,
      }


def show(title: str, frame: pd.DataFrame, limit: int = 200) -> None:
      print(f"\n{title}: {len(frame)}")
      if frame.empty:
            return
      print(frame.head(limit).to_string(index=False))
      if len(frame) > limit:
            print(f"... {len(frame) - limit} more")


async def main() -> None:

      repo = Repository()
      syncer = Syncer()

      customers = repo.get_customers()

      pos = load_pos_customers(settings.customer_excel_path)
      db = load_db_customers(customers)
      result = compare(pos, db)

      print(f"POS customers: {len(pos)} | DB customers: {len(db)}")

      show("In POS but missing from Supabase", result["missing_in_db"])
      show("In Supabase but not in POS", result["only_in_db"])
      show("Supabase rows with no pos_customer_id", result["db_without_pos_id"])
      show("Same pos_customer_id, different name", result["renamed"])
      show("pos_customer_id used by more than one Supabase row", result["duplicate_ids"])

      if EXPORT_CSV:
            result["missing_in_db"].to_csv("missing_in_db.csv", index=False)
            result["only_in_db"].to_csv("only_in_db.csv", index=False)

      # ctx = PipelineContext()
      # s2c_mapper = S2CMapper()
      # await s2c_mapper.run(ctx)


if __name__ == "__main__":
      asyncio.run(main())