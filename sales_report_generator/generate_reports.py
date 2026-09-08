#!/usr/bin/env python3
"""
Command-line sales report generator.

Examples
--------
# One customer, by name or id
python generate_reports.py --customer "Musa Ibrahim"
python generate_reports.py --customer 42

# A specific list of customers -> one PDF each
python generate_reports.py --customer "Musa Ibrahim" --customer "Amaka Obi" --customer 17

# Everyone in a text file (one name/id per line) -> one PDF each
python generate_reports.py --customers-file customers.txt

# Every customer in the database
python generate_reports.py --all

# Restrict to a date range
python generate_reports.py --all --from 2026-01-01 --to 2026-06-30

# Just see who a search term would match, without generating anything
python generate_reports.py --search "musa"
"""
import argparse
import sys

import config
import db
from analytics import build_analytics
from pdf_generator import render_customer_pdf


def collect_target_customers(args) -> list:
    """Resolve CLI args into a de-duplicated list of customer rows."""
    identifiers = list(args.customer or [])

    if args.customers_file:
        with open(args.customers_file, "r", encoding="utf-8") as f:
            identifiers += [line.strip() for line in f if line.strip()]

    customers = {}

    if args.all:
        for c in db.fetch_all_customers():
            customers[c[config.CUSTOMER_ID_COL]] = c

    for ident in identifiers:
        matches = db.find_customers(ident)
        if not matches:
            print(f"  [!] No customer found matching '{ident}' - skipping.", file=sys.stderr)
            continue
        if len(matches) > 1 and not args.all:
            names = ", ".join(m[config.CUSTOMER_NAME_COL] for m in matches)
            print(f"  [!] '{ident}' matched multiple customers ({names}) - including all.", file=sys.stderr)
        for m in matches:
            customers[m[config.CUSTOMER_ID_COL]] = m

    return list(customers.values())


def period_label(date_from, date_to):
    if date_from and date_to:
        return f"{date_from} to {date_to}"
    if date_from:
        return f"Since {date_from}"
    if date_to:
        return f"Up to {date_to}"
    return "All time"


def main():
    parser = argparse.ArgumentParser(description="Generate PDF sales reports for customers from Supabase.")
    parser.add_argument("--customer", action="append", help="Customer name or id. Repeatable.")
    parser.add_argument("--customers-file", help="Text file with one customer name/id per line.")
    parser.add_argument("--all", action="store_true", help="Generate a report for every customer.")
    parser.add_argument("--search", help="Just print customers matching this name, then exit.")
    parser.add_argument("--from", dest="date_from", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--to", dest="date_to", help="End date (YYYY-MM-DD).")
    parser.add_argument("--output-dir", default=config.DEFAULT_OUTPUT_DIR, help="Where to save PDFs.")
    args = parser.parse_args()

    if args.search:
        matches = db.find_customers(args.search)
        if not matches:
            print("No matches.")
        for m in matches:
            print(f"  {m[config.CUSTOMER_ID_COL]}  -  {m[config.CUSTOMER_NAME_COL]}")
        return

    if not (args.customer or args.customers_file or args.all):
        parser.error("Provide --customer, --customers-file, or --all (see --help).")

    targets = collect_target_customers(args)
    if not targets:
        print("No customers matched. Nothing to do.", file=sys.stderr)
        sys.exit(1)

    print(f"Generating reports for {len(targets)} customer(s)...")
    label = period_label(args.date_from, args.date_to)
    ok, failed = 0, 0

    for customer in targets:
        name = customer.get(config.CUSTOMER_NAME_COL, "Unknown")
        try:
            sales = db.get_customer_sales_bundle(
                customer[config.CUSTOMER_ID_COL], args.date_from, args.date_to
            )
            analytics = build_analytics(sales)
            path = render_customer_pdf(customer, analytics, label, args.output_dir)
            print(f"  [OK] {name} -> {path}")
            ok += 1
        except Exception as exc:
            print(f"  [FAIL] {name}: {exc}", file=sys.stderr)
            failed += 1

    print(f"\nDone. {ok} generated, {failed} failed.")


if __name__ == "__main__":
    main()
