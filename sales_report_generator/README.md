# Sales Report Generator

Lightweight command-line tool that pulls a customer's sales history from
Supabase, computes some quick analytics, and renders a PDF report using a
Jinja2 HTML template. Works for a single customer, an explicit list of
customers, or your entire customer base — one PDF per customer.

No system-level dependencies: PDF rendering is done with `xhtml2pdf`
(pure Python), not WeasyPrint, so `pip install` is all you need.

## 1. Install

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Configure

```bash
cp .env.example .env
```

Fill in `.env`:

```
SUPABASE_URL=https://plfilcqufjpjzpmveihh.supabase.co
SUPABASE_KEY=your-key-here
```

Use a key with read access to `customers`, `sales`, `sale_items`, and
`products` (a service_role key is simplest for a local/internal tool like
this; an anon key works too if your RLS policies allow it).

## 3. Check the schema assumptions in `config.py`

This tool doesn't know your exact table/column names, so it ships with
reasonable defaults. **Open `config.py` and confirm these match your
database** before running it:

| Constant | Default | What it should point to |
|---|---|---|
| `CUSTOMERS_TABLE` | `customers` | your customers table |
| `SALES_TABLE` | `sales` | your sales/orders table |
| `SALE_CUSTOMER_FK` | `customer_id` | FK column on `sales` pointing to the customer |
| `SALE_DATE_COL` | `created_at` | timestamp column on `sales` |
| `SALE_TOTAL_COL` | `total_amount` | order total column on `sales` |
| `SALE_ITEMS_TABLE` | `sale_items` | line-items table |
| `SALE_ITEM_SALE_FK` | `sale_id` | FK on `sale_items` pointing to the sale |
| `SALE_ITEM_PRODUCT_FK` | `product_id` | FK on `sale_items` pointing to the product |
| `PRODUCTS_TABLE` | `products` | products table (for names) |

If a column is named differently in your `create_pos_sale()` schema, just
edit the value on the right — nothing else in the code needs to change.

## 4. Run it

```bash
# One customer, by name (partial, case-insensitive match) or by id
python generate_reports.py --customer "Musa Ibrahim"
python generate_reports.py --customer 42

# Several specific customers -> one PDF each
python generate_reports.py --customer "Musa Ibrahim" --customer "Amaka Obi"

# A list of customers from a text file (one name or id per line)
python generate_reports.py --customers-file customers.txt

# Every customer in the database
python generate_reports.py --all

# Restrict to a date range
python generate_reports.py --all --from 2026-01-01 --to 2026-06-30

# Not sure of the exact name? Search first, generate nothing.
python generate_reports.py --search "musa"

# Change where PDFs are saved (default: ./reports)
python generate_reports.py --all --output-dir /path/to/folder
```

PDFs are named `sales_report_<customer-name>_<date>.pdf` and saved to the
output directory (created automatically).

## What's in each report

- Header stats: total revenue, order count, average order value, distinct
  products purchased
- Top products by revenue
- Monthly revenue trend
- Payment method breakdown (Cash / Access Bank / Moniepoint / Stanbic IBTC, etc.)
- Full order history table

## Project layout

```
config.py            # table/column names, currency, company name
db.py                 # Supabase queries (no PostgREST embeds - plain joins in Python)
analytics.py          # pandas-based number crunching
pdf_generator.py       # Jinja2 render -> xhtml2pdf
generate_reports.py    # CLI entrypoint
templates/report.html  # the report layout - edit freely
```

## Customizing the look

Edit `templates/report.html` directly — it's plain HTML/CSS (xhtml2pdf
supports a useful subset of CSS2.1/basic CSS3: floats, tables, borders,
background colors, `@page` margins). Keep layout table-based rather than
flexbox/grid, since xhtml2pdf doesn't support those.
