# Pos4africa Data Analysis

This repository contains the data ingestion and analytics tooling for a Pos4Africa retail environment. It is split into two main parts:

- `pos4africa/`: a Python ETL pipeline that reads Excel exports from the POS, normalizes and deduplicates them, and upserts the results into Supabase.
- `sales_report_generator/`: a separate tool that reads customer and sales data from Supabase and generates PDF sales reports per customer.

The project is designed around an operational workflow where daily or on-demand POS exports are synchronized into a structured analytics schema for downstream reporting.

## Repository at a glance

- Python-first ETL and analytics tooling
- Excel import pipeline for sales, customers, and catalog items
- Supabase-backed storage and reporting
- Optional scheduled auto-sync via `pos4africa.auto_sync`
- Separate PDF report generator for customer sales summaries

## Main project components

### 1) `pos4africa` ETL pipeline

The core package is built around a single-node worker architecture:

- `pos4africa/main.py` — entrypoint that runs the host manager and complete pipeline once
- `pos4africa/auto_sync.py` — optional report downloader and loop helper
- `pos4africa/config/settings.py` — app configuration via environment variables
- `pos4africa/worker/` — pipeline execution and component stages
- `pos4africa/manager/` — orchestration and output writing
- `pos4africa/shared/` — shared models, utilities, and errors

The live pipeline stages are conceptually:

- ExcelScraper / extractor
- Parser
- Processor / normalization
- Deduplication guard
- Syncer / Supabase writer

This flow is initiated by `HostManager`, which builds and runs a `WorkerNode` with the configured component chain.

### 2) `sales_report_generator`

The report generator is a separate CLI utility that reads data from Supabase and produces customer PDF reports using Jinja2 and xhtml2pdf. It is not part of the ETL ingestion path; it is used after the data has landed in the database.

## Stack

- Python 3.11+
- pandas, openpyxl
- pydantic + pydantic-settings
- supabase Python client
- Jinja2 + xhtml2pdf for PDF report generation
- Optional: httpx for auto-sync downloads

## Prerequisites

- Python 3.11+
- A Pos4Africa POS instance with access to the needed Excel exports
- A Supabase project with the schema in `schema.sql`
- Environment variables configured in a local `.env` file

## Quickstart: run one ETL pass

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create a local environment file from the example:

```bash
cp .env.example.env .env
```

3. Fill in the values in `.env`:

```env
POS_BASE_URL=https://yourstore.pos4africa.com
POS_USERNAME=your_username
POS_PASSWORD=your_password

SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_key
```

4. Place the required Excel files in `./Excels/`:

- `DSR.xlsx` — sales report export
- `Customers.xlsx` — customer export
- `Items.xlsx` — catalog / item export (if used by the pipeline)

5. Run the pipeline once:

```bash
python -m pos4africa.main
```

This executes the configured ETL worker, reads the Excel files, cleans and normalizes records, and upserts the results to the configured Supabase tables.

## Optional: auto-sync loop

The repository includes an optional download-and-sync helper that fetches POS reports and reruns the pipeline automatically.

```bash
python -m pos4africa.auto_sync today
python -m pos4africa.auto_sync all
```

This is primarily intended for scheduled refreshes, but the default loop settings should be reviewed before using it in production.

## Project layout

```text
.
├── .env.example.env                # example env file for the ETL pipeline
├── README.md                       # project overview
├── requirements.txt                # Python dependencies for the ETL pipeline
├── schema.sql                      # canonical Supabase table/schema foundation
├── image.png                      # repository image
├── pos4africa/                    # core ETL package
│   ├── auto_sync.py               # optional sync loop
│   ├── main.py                    # single pipeline run entrypoint
│   ├── config/
│   │   └── settings.py            # runtime settings
│   ├── manager/
│   │   ├── egress/
│   │   ├── host.py                # host manager that runs the worker
│   │   └── memory/
│   ├── shared/
│   │   ├── errors/
│   │   ├── models/
│   │   └── utils/
│   └── worker/
│       ├── components/
│       ├── node.py               # WorkerNode execution loop
│       └── pipeline/
├── sales_report_generator/        # PDF sales reporting utility
│   ├── README.md
│   ├── requirements.txt
│   ├── generate_reports.py
│   ├── analytics.py
│   ├── db.py
│   ├── pdf_generator.py
│   ├── config.py
│   ├── templates/
│   └── reports/
└── Excels/                        # local Excel input directory (not committed)
```

## Database / schema notes

The repository includes `schema.sql`, which defines the base analytics schema used by the ETL pipeline. This schema includes core tables such as:

- `customers`
- `items`
- `sales`
- `sale_items`
- `payments`
- `accounts`
- `deliveries`
- relevant supporting tables and indexes

The ETL pipeline is designed to land normalized sales and customer data into this structure for reporting and analytics.

## Sales report generator

The `sales_report_generator` tool is a standalone reporting utility. It can generate one PDF per customer, or run across a list of customers or the full customer base.

Typical usage:

```bash
cd sales_report_generator
pip install -r requirements.txt
cp .env.example .env
python generate_reports.py --all
```

For a detailed usage guide, see `sales_report_generator/README.md`.

## Notes

- This repo contains both ingestion and reporting workflows, so the deployment story depends on whether you are using the ETL pipeline or the report generator.
- The ETL pipeline is centered around local Excel-based sync and Supabase upserts.
- The report generator is a later-stage consumer of the data once it is in the database.
- `auto_sync.py` is useful for scheduled refreshes, but it should be tuned for the production environment before long-running use.

## License

No explicit repository license file is provided in the current project, so the repository is effectively unlicensed unless otherwise specified by the project owner.
