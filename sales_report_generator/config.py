"""
Central configuration for the sales report generator.

Everything that's specific to *your* Supabase schema lives here, so if a
table or column name doesn't match your database you only need to change
it in one place.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# --- Supabase connection ---------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # service_role or anon key with read access

# --- Schema mapping ----------------------------------------------------
# Adjust these constants to match your actual table/column names.
# Nothing else in the codebase needs to change if you rename things here.

CUSTOMERS_TABLE = "customers"
CUSTOMER_ID_COL = "pos_customer_id"
CUSTOMER_NAME_COL = "customer_name"
CUSTOMER_PHONE_COL = "phone"
CUSTOMER_EMAIL_COL = "email"

SALES_TABLE = "sales"
SALE_ID_COL = "pos_sale_id"
SALE_CUSTOMER_FK = "pos_customer_id"
SALE_DATE_COL = "invoice_datetime"
SALE_TOTAL_COL = "invoice_total"
SALE_PAYMENT_COL = "payment_method"

SALE_ITEMS_TABLE = "sale_items"
SALE_ITEM_SALE_FK = "pos_sale_id"
SALE_ITEM_PRODUCT_FK = "pos_item_id"
SALE_ITEM_QTY_COL = "quantity"
SALE_ITEM_PRICE_COL = "unit_price"

PRODUCTS_TABLE = "items"
PRODUCT_ID_COL = "pos_item_id"
PRODUCT_NAME_COL = "item_name"

# --- Report look & feel ------------------------------------------------
COMPANY_NAME = "Tahir General"
CURRENCY_SYMBOL = "\u20a6"  # NGN
TOP_N_PRODUCTS = 15
DEFAULT_OUTPUT_DIR = "reports"
