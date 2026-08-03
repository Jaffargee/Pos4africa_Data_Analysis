# Pos4africa_Data_Analysis

An ETL pipeline that pulls sales and customer data out of a [pos4africa.com](https://pos4africa.com)
POS instance and syncs it into Supabase, plus a couple of standalone Excel reporting scripts.

Built for a retail/textile business's POS reporting: nightly (or manual) Excel exports from the
POS system get parsed, deduplicated, hashed, and upserted into Supabase, where they can be
queried/dashboarded from.

## Requirements

- Python 3.11+ (the codebase uses `enum.StrEnum`, added in 3.11)
- A Supabase project with `sales`, `customers`, and `accounts` tables
- Access to a pos4africa POS instance that can export "Detailed Sales Report" and "Customers"
  Excel reports

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in the values below
```

`.env`:

```
POS_BASE_URL=https://yourstore.pos4africa.com
POS_USERNAME=
POS_PASSWORD=
SUPABASE_URL=
SUPABASE_KEY=
```

See `pos4africa/config/settings.py` for the full list of configurable settings (batch sizes,
rate limits, retry/circuit-breaker tuning, table names, etc.) — everything has a sane default
except the five values above.

## Running it

```bash
python -m pos4africa.main
```

This runs a **single pass** of the pipeline:

```
Excels/DSR.xlsx        ──▶ ExcelScraper ──▶ Parser ──▶ Processor ──▶ Supabase (sales)
Excels/Customers.xlsx  ──▶ CustomerScraper ──▶ Syncer ──▶ Supabase (customers)
```

It expects two files to already exist:

| File | Purpose | Configured via |
|---|---|---|
| `Excels/DSR.xlsx` | "Detailed Sales Report" export from the POS system | `excel_source_path` |
| `Excels/Customers.xlsx` | Customer list export from the POS system | `customer_excel_path` |

### What happens on each run

1. **Sales**: `ExcelScraper` reads `DSR.xlsx`, groups line items by `Sale Id`, and parses payment
   strings (e.g. `"Cash: ₦5,000, POS: ₦2,000"`) into structured records. Each sale is deduplicated,
   parsed, and processed into a DB-ready shape with a content hash.
2. **Reconciliation**: before writing, the pipeline fetches existing `(pos_sale_id, hash)` pairs
   from Supabase and only upserts sales whose hash actually changed — unchanged historical sales
   aren't rewritten.
3. **Customers**: `CustomerScraper` reads `Customers.xlsx`, hashes each customer's identity
   fields, diffs against a local `customers.sync.json` cache, and only pushes customers whose
   data changed.
4. A run summary (`loaded`, `inserted`, `inserted_customers`, `duplicates`, `failed`) is logged
   at the end.

### Keeping the Excel files fresh

`auto_sync.py` is a separate, optional entrypoint that downloads the latest report exports
directly from the POS system's URLs and re-runs the pipeline on a loop:

```bash
python -m pos4africa.auto_sync        # defaults to today's report
python -m pos4africa.auto_sync all    # pulls the all-time report instead
```

> **Note:** the sync interval in `auto_sync.py` is currently set to 60 seconds ("for testing" per
> the inline comment) — change this to a real interval (e.g. once daily) or drive it from cron /
> a task scheduler before running it unattended.

## Project structure

```
pos4africa/
├── main.py                    # entrypoint — runs one pipeline pass
├── auto_sync.py                # optional entrypoint — downloads reports + loops the pipeline
├── config/settings.py           # all configuration (env-backed via pydantic-settings)
├── manager/
│   ├── host.py                  # HostManager — wraps the single worker node
│   ├── egress/
│   │   ├── batch_writer.py       # writes processed sales/customers to Supabase
│   │   └── syncer.py             # local JSON cache + diffing for customer records
│   └── memory/                  # long-term memory store used across a run
├── worker/
│   ├── node.py                  # WorkerNode — the actual pipeline used by main.py
│   └── components/
│       ├── excel_scraper.py      # reads DSR.xlsx → RawSale
│       ├── customer_scraper.py   # reads Customers.xlsx → Customer
│       ├── parser.py             # RawSale → typed Sale
│       ├── processor.py          # Sale → ProcessedSale (DB-ready, hashed)
│       └── dedup_guard.py        # skips sales already seen this run
├── shared/
│   ├── models/                  # pydantic models: sale, customer, account, job
│   └── utils/                   # logging, hashing, retry, batch processing
└── infra/
    └── supabase_client.py        # shared Supabase client instance

dsr.py             # standalone script — builds a styled Excel analytics dashboard
tahir/exldt.py     # standalone Excel-related script (see script for details)
Excels/            # input Excel files (DSR.xlsx, Customers.xlsx)
```

## Known limitations

- `worker/worker_node.py`, `manager/scheduler.py`, `infra/redis_client.py`,
  `infra/rabbitmq.py`, and related files implement a second, **unused** distributed
  architecture (Redis-backed job queue, HTML scraping via `PosConnector`, RabbitMQ egress,
  circuit breaker) that isn't wired into `main.py`. The pipeline that actually runs is the
  single-node, Excel-based one described above. See `CLEANUP.md` if present, or treat those
  files as candidates for removal if you're not planning to build out the distributed version.
- `dsr.py` has a hardcoded local file path and isn't parameterized via `config/settings.py` yet.
- `customers.sync.json` contains real customer data and should be excluded from version control.

## License

Internal/private project — no license specified.