import asyncio
import sys

from pos4africa.manager.memory.store import MemoryStore
from pos4africa.infra.pos_connector import PosConnector
from pos4africa.worker.components.scrapers.sales.edit_sale import EditSaleCustomer, parse_sale_page

def offline_parse(path: str) -> None:
      """Step 0: parse a saved copy of the change_sale page, no network, no risk."""
      with open(path, encoding="utf-8") as f:
            print(parse_sale_page(f.read()))


async def dry_run(sale_id: int, new_person_id: int) -> None:
      memstore = MemoryStore()
      await memstore.initialise()
      async with PosConnector() as connector:
            editor = EditSaleCustomer(connector)
            # commit=False -> swaps in the edit cart, verifies, then discards. Nothing is saved.
            print(await editor.reassign(sale_id, new_person_id, commit=False))


if __name__ == "__main__":
      if sys.argv[1] == "parse":
            offline_parse(sys.argv[2])                       # python -m ...test_edit_sale parse sale.html
      else:
            asyncio.run(dry_run(int(sys.argv[1]), int(sys.argv[2])))   # ... 7040 <new_person_id>