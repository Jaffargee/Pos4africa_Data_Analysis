"""
Turns a list of "sale" dicts (each carrying an `_items` list, as produced
by db.get_customer_sales_bundle) into the summary numbers the PDF template
displays.
"""
from datetime import datetime
import pandas as pd

import config


def _parse_date(value):
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def build_analytics(sales: list) -> dict:
    if not sales:
        return {
            "order_count": 0,
            "total_revenue": 0,
            "avg_order_value": 0,
            "first_order_date": None,
            "last_order_date": None,
            "top_products": [],
            "monthly_breakdown": [],
            "payment_breakdown": [],
            "orders": [],
        }

    sales_df = pd.DataFrame(sales)
    sales_df["_date"] = sales_df[config.SALE_DATE_COL].apply(_parse_date)
    sales_df[config.SALE_TOTAL_COL] = pd.to_numeric(
        sales_df[config.SALE_TOTAL_COL], errors="coerce"
    ).fillna(0)

    total_revenue = float(sales_df[config.SALE_TOTAL_COL].sum())
    order_count = len(sales_df)
    avg_order_value = total_revenue / order_count if order_count else 0

    # Monthly trend
    monthly = (
        sales_df.dropna(subset=["_date"])
        .assign(month=lambda d: d["_date"].dt.strftime("%Y-%m"))
        .groupby("month")[config.SALE_TOTAL_COL]
        .sum()
        .sort_index()
    )
    monthly_breakdown = [
        {"month": m, "total": round(v, 2)} for m, v in monthly.items()
    ]

    # Payment method split
    payment_breakdown = []
    if config.SALE_PAYMENT_COL in sales_df.columns:
        pay = sales_df.groupby(config.SALE_PAYMENT_COL)[config.SALE_TOTAL_COL].sum()
        payment_breakdown = [
            {"method": m or "Unknown", "total": round(v, 2)} for m, v in pay.items()
        ]

    # Top products by revenue (qty * unit_price), across all sales' items
    all_items = []
    for sale in sales:
        for item in sale.get("_items", []):
            qty = float(item.get(config.SALE_ITEM_QTY_COL, 0) or 0)
            price = float(item.get(config.SALE_ITEM_PRICE_COL, 0) or 0)
            all_items.append(
                {
                    "product": item.get("_product_name", "Unknown"),
                    "qty": qty,
                    "revenue": qty * price,
                }
            )

    top_products = []
    if all_items:
        items_df = pd.DataFrame(all_items)
        grouped = (
            items_df.groupby("product")
            .agg(qty=("qty", "sum"), revenue=("revenue", "sum"))
            .sort_values("revenue", ascending=False)
            .head(config.TOP_N_PRODUCTS)
        )
        top_products = [
            {"product": p, "qty": row.qty, "revenue": round(row.revenue, 2)}
            for p, row in grouped.iterrows()
        ]

    orders = [
        {
            "date": rec["_date"].strftime("%Y-%m-%d") if rec["_date"] else "Unknown",
            "sale_id": rec[config.SALE_ID_COL],
            "total": round(rec[config.SALE_TOTAL_COL], 2),
            "payment": rec.get(config.SALE_PAYMENT_COL, ""),
            "items_sold": rec["items_sold"],
            "items_returned": rec["items_returned"],
        }
        for rec in sales_df.to_dict("records")
    ]

    return {
        "order_count": order_count,
        "total_revenue": round(total_revenue, 2),
        "avg_order_value": round(avg_order_value, 2),
        "first_order_date": sales_df["_date"].min(),
        "last_order_date": sales_df["_date"].max(),
        "top_products": top_products,
        "monthly_breakdown": monthly_breakdown,
        "payment_breakdown": payment_breakdown,
        "orders": orders,
    }
