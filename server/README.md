# Pos4Africa Server

FastAPI backend between the analytics dashboard and POS4Africa + Supabase.
It lets you **run your own system in parallel with POS4Africa**: reads come from
Supabase, writes go to POS4Africa (the source of truth) through a write-ahead log.

## Run

```bash
pip install -r server/requirements.txt
cp server/.env.example .env        # merge into your existing .env
python -m pytest                    # 60 tests, no credentials needed
uvicorn server.main:create_app --factory --port 8000   # from the repo root, ONE worker
```

`POS_WRITE_MODE` defaults to `dry_run`: sales build and verify the cart in POS and then
discard it; customers are validated only; nothing is logged or changed.
Switch to `commit` only after a dry run behaves on your real POS.

## Layers: who is responsible for what

```
server/
  main.py            app factory, lifespan, optional background reconcile loop
  container.py       composition root: the ONLY place that knows every concrete class
  config.py          server-only settings (POS/Supabase creds stay in pos4africa.config)

  api/               HTTP only. Auth, header + body validation, call ONE use case, map errors
                     to status codes. No business rules, no SQL, no POS calls.
  application/       Use cases and orchestration.
    pos_write.py       PosWriteSaga: the write-ahead / idempotency / reconcile engine
    customers/         create, update
    sales/             create (resolves prices, checks payments, builds the fingerprint)
    recon/             operator-facing reconcile
  domain/            Pure Python. Models, rules, errors, and PORTS (Protocols). No I/O.
  infrastructure/    Adapters that implement the ports.
    pos4africa/        session pool, customer gateway, sales gateway, HTML parsing, error classification
    supabase/          repositories (+ the write log)
  migrations/        SQL to apply to Supabase (not applied automatically)
  tests/             fakes for every port; failure injection
```

Dependencies point inward: `api -> application -> domain <- infrastructure`.
Tests build a `Container` from fakes, so the whole write flow is tested without POS or Supabase.

## The write flow (why it is safe)

```
client --Idempotency-Key--> API --> use case --> PosWriteSaga
   1. validate + resolve (prices, accounts, customer)       -> reject early, POS untouched
   2. log intent (PENDING, ref from key, full payload)       -> write-ahead
   3. claim (compare-and-set to SENDING)                     -> concurrent duplicates send once
   4. send to POS (sale: comment carries "~R:<ref>")
   5. read it back from POS by ref, compare fingerprint      -> proves it is the sale we meant
   6. POSTED (POS id stored)  ->  commit to Supabase (one transaction)  ->  SYNCED
```

| Outcome of step 4 | Meaning | State | Client |
|---|---|---|---|
| POS said no | not applied, retry is pointless | FAILED (final) | 422 |
| never reached POS / session expired | not applied | FAILED (retryable) | 503, retry same key |
| sent, answer lost / unreadable | **may** be applied | UNCERTAIN | 502, then 409 until reconciled |
| POS ok, Supabase write failed | applied; POS id is in the log | POSTED | 202, reconcile finishes it |

An UNCERTAIN write is never re-sent. `POST /recon/reconcile` (or the optional loop) looks
the sale/customer up in POS: found -> adopts the real id; provably absent -> FAILED+retryable.

## Endpoints

All need `X-API-Key`. Writes also need `Idempotency-Key` (one UUID per user action; reuse it on retry).

```
GET  /customers?q=&limit=&offset=     GET  /customers/{pos_customer_id}
POST /customers                       PATCH /customers/{pos_customer_id}
GET  /sales?pos_customer_id=          GET  /sales/{pos_sale_id}      POST /sales
GET  /items?q=
GET  /recon?status=&kind=             GET  /recon/{ref}
POST /recon/reconcile                 POST /recon/{ref}/reconcile
GET  /health  (no auth)
```

## Before you flip to `commit`

1. **Apply `migrations/0001_pos_write_log.sql`** (creates `pos_write_log` and `commit_pos_sale`).
2. **Verify the register calls.** `sales/add`, `sales/edit_item/{line}` and `sales/add_payment` are
   standard PHPPOS paths I could not test against your POS; they are settings
   (`POS_SALES_*_PATH`). Record one real sale in the browser network tab and compare.
   The `POS_SALES_REPORT_PATH` report must include the sale `comment` column.
3. **Verify the customer view page** (`/customers/view/{id}`) exposes the form fields; update refuses
   to write when it cannot read the current form, so a wrong guess fails safe.
4. Run with `POS_WRITE_MODE=dry_run` and watch the cart verification pass.

## Known limits

- One worker process; POS keeps the cart in the session. `POS_SESSION_POOL_SIZE>1` only if POS
  does not log older sessions out.
- Customer outcome resolution matches by name (+phone). Two POS customers with the same name and
  no phone make it ambiguous: it stays UNCERTAIN for a human instead of guessing.
- The local sale row is provisional (time = commit time). The ETL overwrites it with POS's values.
  The server deliberately does not touch stock, balances or customer totals; the ETL owns those.
- Auth is a static API key. Swap in Supabase JWT verification when the dashboard has user logins.
