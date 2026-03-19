from dataclasses import dataclass

@dataclass
class SaleItem:
      pos_sales_id: int
      pos_prd_id: int
      name: str
      quantity: float
      unit_price: int
      total: int
