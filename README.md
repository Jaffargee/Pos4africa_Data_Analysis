# Pos4africa_Data_Analysis

An ETL pipeline that pulls sales and customer Excel exports from a pos4africa.com
POS instance, normalizes and deduplicates them, and upserts the results into
Supabase for reporting and analytics. The project also contains a couple of
standalone Excel reporting scripts.

This repository is targeted at small/medium retail businesses using the
Pos4Africa POS who need nightly or on-demand synchronization of sales and
customer records into a SQL-backed analytics store.

## Stack
- Language(s): Python 3.11+
- Runtime / tooling: plain Python modules, pip / requirements.txt
- Notable libraries: pydantic (settings & models), requests (HTTP), openpyxl / pandas
  (Excel parsing), supabase client (Supabase writes)

## Quickstart — run one pipeline pass
1. Install dependencies and create a local env file:

```bash
pip install -r requirements.txt
cp .env.example.env .env
# Edit .env with your POS and Supabase credentials
```

2. Place Excel exports from your POS into the Excels/ directory:
- `Excels/DSR.xlsx` — Detailed Sales Report (DSR)
- `Excels/Customers.xlsx` — Customers export

3. Run the pipeline (single pass):

```bash
python -m pos4africa.main
```

This reads the two Excel files, parses and deduplicates sales and customer
records, and upserts changed records into the configured Supabase tables.

Optional: download and run the auto-sync loop which fetches reports from the
POS and re-runs the pipeline on a schedule:

```bash
python -m pos4africa.auto_sync        # defaults to today's report
python -m pos4africa.auto_sync all    # pulls the all-time report
```

Note: the default sync interval in `auto_sync.py` is short (use a real
interval or run from cron for production use).

## Configuration
Primary configuration is provided via environment variables read by
`pos4africa/config/settings.py`. Minimal required variables (set these in
`.env`):

```
POS_BASE_URL=https://yourstore.pos4africa.com
POS_USERNAME=
POS_PASSWORD=
SUPABASE_URL=
SUPABASE_KEY=
```

See `pos4africa/config/settings.py` for the full list of tuning knobs:
- Excel input paths & sheet names
- POS request timeouts, paging and rate limits
- Worker pool / batch sizing
- Supabase table names (sales, customers, accounts)
- Circuit-breaker and retry settings
- Logging / metrics configuration

## Project layout
```
pos4africa/                      # main package
├── main.py                       # CLI entrypoint — runs one pipeline pass
├── auto_sync.py                  # optional entrypoint — download reports + loop
├── config/                       # configuration (pydantic settings)
│   └── settings.py               # all env-backed settings and defaults
├── infra/                        # infra helpers (Supabase client, etc.)
├── manager/                      # process management, batching, host wrapper
├── shared/                       # pydantic models and shared utilities
└── worker/                       # pipeline node + components
    └── components/               # excel_scraper, parser, processor, dedup_guard

Excels/                           # input Excel files (DSR.xlsx, Customers.xlsx)
requirements.txt                   # Python dependencies
.env.example.env                   # example env file (copy to .env)
README.md                          # this file
image.png                          # project image used in README
```

How it fits together: `pos4africa.main` instantiates a single WorkerNode that
runs a pipeline of components: ExcelScraper → Parser → Processor → Egress
(batch writer / syncer) which pushes to Supabase. `auto_sync.py` wraps this
flow with a downloader and a loop for automatic operation.

## Useful scripts
- `dsr.py` — standalone script that builds a styled Excel analytics dashboard
- `tahir/exldt.py` — additional Excel utilities (see the script for details)
- `pos4africa/tester.py` — small helper used during development

## Known limitations & notes
- There are unused/discarded files and a second, partly-implemented
  distributed architecture (Redis, RabbitMQ, PosConnector) in the tree — the
  single-node Excel pipeline is the one exercised by `main.py`.
- `dsr.py` contains hardcoded paths and is not wired to `settings.py`.
- `customers.sync.json` (if present locally) contains real customer data and
  should not be committed.
- The default interval and retry settings in `auto_sync.py` are set for
  fast iteration — tune for production use.

## Contributing
This repository is currently maintained as an internal project. If you plan
to extend it:
- Use Python 3.11+ (StrEnum is used in the codebase)
- Add tests for parsing and deduplication logic before changing processing
  behavior

## License
Internal / private — no license specified.
