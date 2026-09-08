"""
Thin data-access layer over Supabase.

Deliberately avoids relying on PostgREST's nested-embed syntax
(`select("*, sale_items(*)")`) because that requires foreign keys to be
registered in a specific way in Supabase and breaks silently otherwise.
Instead we do plain, explicit queries and join in Python — a few extra
round trips, but it works against any schema without extra setup.
"""
from functools import lru_cache
from supabase import create_client, Client

import config


@lru_cache(maxsize=1)
def get_client() -> Client:
    if not config.SUPABASE_URL or not config.SUPABASE_KEY:
        raise RuntimeError(
            "SUPABASE_URL / SUPABASE_KEY not set. Copy .env.example to .env "
            "and fill in your project credentials."
        )
    return create_client(config.SUPABASE_URL, config.SUPABASE_KEY)


def fetch_all_customers():
    """Return every customer row (id + name + contact info)."""
    client = get_client()
    resp = (
        client.table(config.CUSTOMERS_TABLE)
        .select("*")
        .order(config.CUSTOMER_NAME_COL)
        .execute()
    )
    return resp.data or []


def find_customers(identifier: str):
    """
    Resolve a CLI identifier to one or more customer rows.
    Tries an exact id match first, then a case-insensitive name match
    (which can return multiple rows if names aren't unique).
    """
    client = get_client()

    # Try as an id (numeric or uuid) first.
    resp = (
        client.table(config.CUSTOMERS_TABLE)
        .select("*")
        .eq(config.CUSTOMER_ID_COL, identifier)
        .execute()
    )
    if resp.data:
        return resp.data

    # Fall back to name search.
    resp = (
        client.table(config.CUSTOMERS_TABLE)
        .select("*")
        .ilike(config.CUSTOMER_NAME_COL, f"%{identifier}%")
        .execute()
    )
    return resp.data or []


def fetch_sales_for_customer(customer_id, date_from=None, date_to=None):
    """All sales rows for a customer, optionally bounded by date."""
    client = get_client()
    query = (
        client.table(config.SALES_TABLE)
        .select("*")
        .eq(config.SALE_CUSTOMER_FK, customer_id)
    )
    if date_from:
        query = query.gte(config.SALE_DATE_COL, date_from)
    if date_to:
        query = query.lte(config.SALE_DATE_COL, date_to)
    resp = query.order(config.SALE_DATE_COL).execute()
    return resp.data or []


def fetch_items_for_sales(sale_ids):
    """All sale_items rows belonging to the given list of sale ids."""
    if not sale_ids:
        return []
    client = get_client()
    resp = (
        client.table(config.SALE_ITEMS_TABLE)
        .select("*")
        .in_(config.SALE_ITEM_SALE_FK, sale_ids)
        .execute()
    )
    return resp.data or []


def fetch_products(product_ids):
    """Map of product_id -> product name for the given ids."""
    if not product_ids:
        return {}
    client = get_client()
    resp = (
        client.table(config.PRODUCTS_TABLE)
        .select("*")
        .in_(config.PRODUCT_ID_COL, list(product_ids))
        .execute()
    )
    return {
        row[config.PRODUCT_ID_COL]: row.get(config.PRODUCT_NAME_COL, "Unknown")
        for row in (resp.data or [])
    }


def get_customer_sales_bundle(customer_id, date_from=None, date_to=None):
    """
    Convenience call that returns everything needed for one customer's
    report: their sales, each sale's line items (with product names
    resolved), ready for analytics.py to crunch.
    """
    sales = fetch_sales_for_customer(customer_id, date_from, date_to)
    sale_ids = [s[config.SALE_ID_COL] for s in sales]
    items = fetch_items_for_sales(sale_ids)

    product_ids = {i[config.SALE_ITEM_PRODUCT_FK] for i in items if i.get(config.SALE_ITEM_PRODUCT_FK)}
    product_names = fetch_products(product_ids)

    items_by_sale = {}
    for item in items:
        pid = item.get(config.SALE_ITEM_PRODUCT_FK)
        item["_product_name"] = product_names.get(pid, "Unknown product")
        items_by_sale.setdefault(item[config.SALE_ITEM_SALE_FK], []).append(item)

    for sale in sales:
        sale["_items"] = items_by_sale.get(sale[config.SALE_ID_COL], [])

    return sales
