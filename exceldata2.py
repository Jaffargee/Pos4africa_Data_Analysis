"""
sales_analysis.py — Detailed Sales Report Analyser
====================================================
Analyses the uploaded sales Excel file and produces:
  1. Summary KPIs
  2. Top products by revenue & volume
  3. Category breakdown
  4. Daily revenue trend
  5. Payment method breakdown
  6. Salesperson performance
  7. Hourly sales pattern
  8. 7-day revenue forecast (Linear Regression)
  9. Saves all charts to /mnt/user-data/outputs/

Usage:
    python3 sales_analysis.py
"""

from __future__ import annotations

import os
import warnings
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from sklearn.linear_model import LinearRegression

warnings.filterwarnings("ignore")

# ── Config ─────────────────────────────────────────────────────────────────────
FILE_PATH   = "/mnt/user-data/uploads/Detailed_Sales_Report__1_.xlsx"
OUTPUT_DIR  = "/mnt/user-data/outputs"
CURRENCY    = "N"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Palette ────────────────────────────────────────────────────────────────────
COLORS = ["#2563EB", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6",
          "#EC4899", "#06B6D4", "#84CC16", "#F97316", "#6366F1"]

plt.rcParams.update({
    "figure.facecolor":  "#0F172A",
    "axes.facecolor":    "#1E293B",
    "axes.edgecolor":    "#334155",
    "axes.labelcolor":   "#CBD5E1",
    "axes.titlecolor":   "#F1F5F9",
    "xtick.color":       "#94A3B8",
    "ytick.color":       "#94A3B8",
    "text.color":        "#F1F5F9",
    "grid.color":        "#334155",
    "grid.linestyle":    "--",
    "grid.alpha":        0.5,
    "font.family":       "DejaVu Sans",
    "figure.dpi":        130,
})

def fmt_currency(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"N{value/1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"N{value/1_000:.1f}K"
    return f"N{value:,.0f}"


# ══════════════════════════════════════════════════════════════════════════════
# 1. LOAD & CLEAN
# ══════════════════════════════════════════════════════════════════════════════
print("Loading data ...")
df = pd.read_excel(FILE_PATH)

# Parse datetime
df["datetime"] = pd.to_datetime(df["Date"], format="%m/%d/%Y-%I:%M %p", errors="coerce")
df["date"]     = df["datetime"].dt.date
df["hour"]     = df["datetime"].dt.hour
df["day_name"] = df["datetime"].dt.day_name()

# Clean payment type
def extract_payment_method(val: str) -> str:
    if pd.isna(val):
        return "Unknown"
    val = str(val)
    if "MONIEPOINT" in val.upper():   return "MONIEPOINT"
    if "ACCESS BANK" in val.upper():  return "ACCESS BANK"
    if "CASH" in val.upper():         return "CASH"
    if "STORE" in val.upper():        return "STORE ACCOUNT"
    return "OTHER"

df["payment_method"] = df["Payment Type"].apply(extract_payment_method)

# Remove returns/negatives for revenue KPIs
df_sales = df[df["Total"] > 0].copy()

print(f"Loaded {len(df):,} rows | {len(df_sales):,} positive transactions\n")


# ══════════════════════════════════════════════════════════════════════════════
# 2. KPI SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
total_revenue   = df_sales["Total"].sum()
total_txns      = df_sales["Sale Id"].nunique()
total_items     = df_sales["Quantity Sold"].sum()
avg_order_value = df_sales.groupby("Sale Id")["Total"].sum().mean()
date_range      = f"{df['date'].min()} to {df['date'].max()}"
num_days        = df_sales["date"].nunique()
daily_avg       = total_revenue / num_days if num_days else 0
top_product     = df_sales.groupby("Name")["Total"].sum().idxmax()
top_category    = df_sales.groupby("Category")["Total"].sum().idxmax()

print("=" * 55)
print("  SALES ANALYSIS REPORT")
print("=" * 55)
print(f"  Period             : {date_range}  ({num_days} days)")
print(f"  Total Revenue      : {fmt_currency(total_revenue)}")
print(f"  Total Transactions : {total_txns:,}")
print(f"  Items Sold         : {total_items:,}")
print(f"  Avg Order Value    : {fmt_currency(avg_order_value)}")
print(f"  Daily Avg Revenue  : {fmt_currency(daily_avg)}")
print(f"  Top Product        : {top_product}")
print(f"  Top Category       : {top_category}")
print("=" * 55)
print()


# ══════════════════════════════════════════════════════════════════════════════
# 3. CHART HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def save(fig: plt.Figure, name: str) -> None:
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Saved -> {path}")


# ══════════════════════════════════════════════════════════════════════════════
# 4. TOP 10 PRODUCTS BY REVENUE
# ══════════════════════════════════════════════════════════════════════════════
print("[1/8] Top products by revenue ...")
top_products_rev = (
    df_sales.groupby("Name")["Total"].sum()
    .sort_values(ascending=True)
    .tail(10)
)
fig, ax = plt.subplots(figsize=(11, 5))
bars = ax.barh(top_products_rev.index, top_products_rev.values, color=COLORS[:10][::-1])
for bar, val in zip(bars, top_products_rev.values):
    ax.text(bar.get_width() + max(top_products_rev.values)*0.01,
            bar.get_y() + bar.get_height()/2,
            fmt_currency(val), va="center", fontsize=8, color="#CBD5E1")
ax.set_title("Top 10 Products by Revenue", fontsize=13, fontweight="bold", pad=12)
ax.set_xlabel("Revenue")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_currency(x)))
ax.grid(axis="x")
fig.tight_layout()
save(fig, "01_top_products_revenue.png")


# ══════════════════════════════════════════════════════════════════════════════
# 5. TOP 10 PRODUCTS BY QUANTITY SOLD
# ══════════════════════════════════════════════════════════════════════════════
print("[2/8] Top products by volume ...")
top_products_qty = (
    df_sales.groupby("Name")["Quantity Sold"].sum()
    .sort_values(ascending=True)
    .tail(10)
)
fig, ax = plt.subplots(figsize=(11, 5))
bars = ax.barh(top_products_qty.index, top_products_qty.values, color=COLORS[:10][::-1])
for bar, val in zip(bars, top_products_qty.values):
    ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
            f"{val:,} units", va="center", fontsize=8, color="#CBD5E1")
ax.set_title("Top 10 Products by Units Sold", fontsize=13, fontweight="bold", pad=12)
ax.set_xlabel("Units Sold")
ax.grid(axis="x")
fig.tight_layout()
save(fig, "02_top_products_quantity.png")


# ══════════════════════════════════════════════════════════════════════════════
# 6. CATEGORY BREAKDOWN
# ══════════════════════════════════════════════════════════════════════════════
print("[3/8] Category breakdown ...")
cat_rev = df_sales.groupby("Category")["Total"].sum().sort_values(ascending=False)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
wedges, texts, autotexts = ax1.pie(
    cat_rev.values, labels=cat_rev.index,
    autopct="%1.1f%%", colors=COLORS[:len(cat_rev)],
    startangle=140, pctdistance=0.8,
    wedgeprops={"edgecolor": "#0F172A", "linewidth": 1.5}
)
for t in autotexts:
    t.set_fontsize(8)
ax1.set_title("Revenue Share by Category", fontsize=12, fontweight="bold")

bars = ax2.bar(cat_rev.index, cat_rev.values, color=COLORS[:len(cat_rev)])
for bar, val in zip(bars, cat_rev.values):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.01,
             fmt_currency(val), ha="center", fontsize=8, color="#CBD5E1")
ax2.set_title("Revenue by Category", fontsize=12, fontweight="bold")
ax2.set_xlabel("Category")
ax2.set_ylabel("Revenue")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_currency(x)))
ax2.grid(axis="y")
plt.xticks(rotation=20, ha="right")
fig.suptitle("Category Analysis", fontsize=14, fontweight="bold", y=1.02)
fig.tight_layout()
save(fig, "03_category_breakdown.png")


# ══════════════════════════════════════════════════════════════════════════════
# 7. DAILY REVENUE TREND
# ══════════════════════════════════════════════════════════════════════════════
print("[4/8] Daily revenue trend ...")
daily_rev = df_sales.groupby("date")["Total"].sum().reset_index()
daily_rev["date"] = pd.to_datetime(daily_rev["date"])
daily_rev = daily_rev.sort_values("date")

fig, ax = plt.subplots(figsize=(12, 5))
ax.fill_between(daily_rev["date"], daily_rev["Total"], alpha=0.25, color=COLORS[0])
ax.plot(daily_rev["date"], daily_rev["Total"],
        color=COLORS[0], linewidth=2.5, marker="o", markersize=6)
for _, row in daily_rev.iterrows():
    ax.annotate(fmt_currency(row["Total"]),
                (row["date"], row["Total"]),
                textcoords="offset points", xytext=(0, 8),
                ha="center", fontsize=7.5, color="#CBD5E1")
ax.set_title("Daily Revenue Trend", fontsize=13, fontweight="bold", pad=12)
ax.set_xlabel("Date")
ax.set_ylabel("Revenue")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_currency(x)))
ax.grid(axis="y")
fig.tight_layout()
save(fig, "04_daily_revenue_trend.png")


# ══════════════════════════════════════════════════════════════════════════════
# 8. PAYMENT METHOD BREAKDOWN
# ══════════════════════════════════════════════════════════════════════════════
print("[5/8] Payment methods ...")
pay_rev = df_sales.groupby("payment_method")["Total"].sum().sort_values(ascending=False)

fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.bar(pay_rev.index, pay_rev.values, color=COLORS[:len(pay_rev)], width=0.5)
for bar, val in zip(bars, pay_rev.values):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.01,
            fmt_currency(val), ha="center", fontsize=9, color="#CBD5E1")
ax.set_title("Revenue by Payment Method", fontsize=13, fontweight="bold", pad=12)
ax.set_xlabel("Payment Method")
ax.set_ylabel("Revenue")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_currency(x)))
ax.grid(axis="y")
fig.tight_layout()
save(fig, "05_payment_methods.png")


# ══════════════════════════════════════════════════════════════════════════════
# 9. HOURLY SALES PATTERN
# ══════════════════════════════════════════════════════════════════════════════
print("[6/8] Hourly pattern ...")
hourly = df_sales.groupby("hour").agg(
    revenue=("Total", "sum"),
    transactions=("Sale Id", "nunique")
).reindex(range(24), fill_value=0)

fig, ax1 = plt.subplots(figsize=(12, 5))
ax2 = ax1.twinx()
ax1.bar(hourly.index, hourly["revenue"], color=COLORS[0], alpha=0.7, label="Revenue")
ax2.plot(hourly.index, hourly["transactions"], color=COLORS[1],
         linewidth=2, marker="o", markersize=5, label="Transactions")
ax1.set_title("Hourly Sales Pattern", fontsize=13, fontweight="bold", pad=12)
ax1.set_xlabel("Hour of Day")
ax1.set_ylabel("Revenue", color=COLORS[0])
ax2.set_ylabel("Transactions", color=COLORS[1])
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_currency(x)))
ax1.set_xticks(range(24))
ax1.set_xticklabels([f"{h:02d}:00" for h in range(24)], rotation=45, ha="right", fontsize=7)
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
ax1.grid(axis="y")
fig.tight_layout()
save(fig, "06_hourly_pattern.png")


# ══════════════════════════════════════════════════════════════════════════════
# 10. SALESPERSON PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════
print("[7/8] Salesperson performance ...")
sp = df_sales.groupby("Sold By").agg(
    revenue=("Total", "sum"),
    transactions=("Sale Id", "nunique"),
    items=("Quantity Sold", "sum")
).sort_values("revenue", ascending=False)

fig, axes = plt.subplots(1, 3, figsize=(13, 5))
metrics = [
    ("revenue",      "Revenue",      lambda x: fmt_currency(x)),
    ("transactions", "Transactions", lambda x: f"{x:,}"),
    ("items",        "Items Sold",   lambda x: f"{x:,}"),
]
for ax, (col, label, fmt) in zip(axes, metrics):
    bars = ax.bar(sp.index, sp[col], color=COLORS[:len(sp)])
    for bar, val in zip(bars, sp[col]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.01,
                fmt(val), ha="center", fontsize=9, color="#CBD5E1")
    ax.set_title(f"By {label}", fontsize=11, fontweight="bold")
    ax.set_xlabel("Salesperson")
    ax.set_ylabel(label)
    ax.grid(axis="y")
fig.suptitle("Salesperson Performance", fontsize=14, fontweight="bold")
fig.tight_layout()
save(fig, "07_salesperson_performance.png")


# ══════════════════════════════════════════════════════════════════════════════
# 11. REVENUE FORECAST (Linear Regression - next 7 days)
# ══════════════════════════════════════════════════════════════════════════════
print("[8/8] Revenue forecast ...")
daily_rev_sorted = daily_rev.sort_values("date").copy()
daily_rev_sorted["day_num"] = range(len(daily_rev_sorted))

X = daily_rev_sorted[["day_num"]].values
y = daily_rev_sorted["Total"].values

model = LinearRegression()
model.fit(X, y)

n_existing   = len(daily_rev_sorted)
future_dates = pd.date_range(
    start=daily_rev_sorted["date"].max() + pd.Timedelta(days=1),
    periods=7
)
future_X   = np.array([[n_existing + i] for i in range(7)])
future_rev = model.predict(future_X)

residuals = y - model.predict(X)
std = residuals.std()

fig, ax = plt.subplots(figsize=(13, 5))
ax.fill_between(daily_rev_sorted["date"], daily_rev_sorted["Total"],
                alpha=0.15, color=COLORS[0])
ax.plot(daily_rev_sorted["date"], daily_rev_sorted["Total"],
        color=COLORS[0], linewidth=2.5, marker="o", markersize=6, label="Actual")
trend_y = model.predict(X)
ax.plot(daily_rev_sorted["date"], trend_y,
        color=COLORS[2], linewidth=1.5, linestyle="--", alpha=0.7, label="Trend line")
ax.plot(future_dates, future_rev,
        color=COLORS[1], linewidth=2.5, marker="s", markersize=7,
        linestyle="--", label="Forecast (7 days)")
ax.fill_between(future_dates, future_rev - std, future_rev + std,
                alpha=0.2, color=COLORS[1], label="+/- 1 Std Dev")
for d, v in zip(future_dates, future_rev):
    ax.annotate(fmt_currency(max(v, 0)), (d, max(v, 0)),
                textcoords="offset points", xytext=(0, 8),
                ha="center", fontsize=7.5, color=COLORS[1])
ax.set_title("Revenue Forecast - Next 7 Days (Linear Regression)",
             fontsize=13, fontweight="bold", pad=12)
ax.set_xlabel("Date")
ax.set_ylabel("Revenue")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_currency(x)))
ax.legend()
ax.grid(axis="y")
fig.tight_layout()
save(fig, "08_revenue_forecast.png")


# ══════════════════════════════════════════════════════════════════════════════
# 12. FORECAST TABLE + PRODUCT BREAKDOWN
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 45)
print("  7-DAY REVENUE FORECAST")
print("=" * 45)
for d, v in zip(future_dates, future_rev):
    bar_len = int(max(v, 0) / max(future_rev.max(), 1) * 20)
    bar_str = "#" * bar_len
    print(f"  {d.strftime('%a %d %b')}  {fmt_currency(max(v,0)):>12}  {bar_str}")
print("=" * 45)
print(f"\n  Model R2 score : {model.score(X, y):.3f}")
print(f"  Based on {n_existing} days of data. More data improves accuracy.")

print()
print("=" * 65)
print("  TOP 10 PRODUCTS - FULL BREAKDOWN")
print("=" * 65)
top10 = (
    df_sales.groupby("Name")
    .agg(
        revenue=("Total", "sum"),
        qty=("Quantity Sold", "sum"),
        avg_price=("Selling Price", "mean"),
        txns=("Sale Id", "nunique"),
    )
    .sort_values("revenue", ascending=False)
    .head(10)
)
print(f"  {'Product':<32} {'Revenue':>12} {'Qty':>7} {'Avg Price':>10} {'Invoices':>9}")
print("  " + "-" * 72)
for name, row in top10.iterrows():
    short = name[:31] if len(name) > 31 else name
    print(f"  {short:<32} {fmt_currency(row['revenue']):>12} {int(row['qty']):>7,} "
          f"{fmt_currency(row['avg_price']):>10} {int(row['txns']):>9,}")
print("=" * 65)
print()
print("Done. All charts saved to:", OUTPUT_DIR)