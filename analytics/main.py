from db_object import DBObject
from features.customer import CustomerFeature
import pandas as pd

db = DBObject()

sales, customers, customer_addrs = (db.get("sales", limit=7000), db.get("customers"), db.get("customer_addresses"))

sales_df = pd.DataFrame(sales)
customers_df = pd.DataFrame(customers)
customer_addrs_df = pd.DataFrame(customers_addrs)

sales_df["invoice_datetime"] = pd.to_datetime(sales_df["invoice_datetime"]).dt.tz_localize(None)

