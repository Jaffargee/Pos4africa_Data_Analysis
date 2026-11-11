-- 0001: write-ahead log for every change the server makes to POS4Africa,
-- and an atomic, idempotent way to mirror a POS sale into Supabase.
--
-- NOT applied automatically. Review, then apply with `supabase db push`
-- or the Supabase MCP `apply_migration`.

begin;

-- 1. The log -----------------------------------------------------------------
create table if not exists public.pos_write_log (
    id           uuid primary key default gen_random_uuid(),
    kind         text not null check (kind in ('CUSTOMER_CREATE','CUSTOMER_UPDATE','SALE_CREATE')),
    ref          text not null unique,          -- from the client's Idempotency-Key; also in the POS comment
    request_hash text not null,
    status       text not null default 'PENDING'
                 check (status in ('PENDING','SENDING','POSTED','SYNCED','FAILED','UNCERTAIN')),
    payload      jsonb not null,
    pos_id       bigint,
    error        text,
    retryable    boolean not null default false,
    attempts     integer not null default 0,
    created_at   timestamptz not null default now(),
    updated_at   timestamptz not null default now()
);

create index if not exists idx_pos_write_log_status_updated
    on public.pos_write_log (status, updated_at);

create or replace function public.touch_pos_write_log() returns trigger
language plpgsql as $$
begin
    new.updated_at := now();
    return new;
end $$;

drop trigger if exists trg_touch_pos_write_log on public.pos_write_log;
create trigger trg_touch_pos_write_log before update on public.pos_write_log
    for each row execute function public.touch_pos_write_log();

-- Service-role only: RLS on, no policies.
alter table public.pos_write_log enable row level security;

-- 2. Mirror a POS sale into Supabase, atomically and idempotently ---------------
-- The id is the REAL POS sale code (found by reading POS back); never a local sequence.
-- Deliberately does NOT touch stock, account balances or customer totals: the ETL owns
-- those derived numbers, and touching them here would double-count once the ETL syncs.
create or replace function public.commit_pos_sale(
    p_pos_sale_id      bigint,
    p_pos_customer_id  integer,
    p_customer_name    text,
    p_salesperson      text,
    p_comment          text,
    p_invoice_datetime timestamptz,
    p_lines            jsonb,
    p_payments         jsonb
) returns jsonb
language plpgsql
set search_path = public
as $$
declare
    v_sale_id uuid;
    v_total   numeric;
    v_qty     numeric;
begin
    if p_lines is null or jsonb_typeof(p_lines) <> 'array' or jsonb_array_length(p_lines) = 0 then
        raise exception 'commit_pos_sale: at least one line is required';
    end if;
    if p_payments is null or jsonb_typeof(p_payments) <> 'array' or jsonb_array_length(p_payments) = 0 then
        raise exception 'commit_pos_sale: at least one payment is required';
    end if;

    select coalesce(sum((l.total)::numeric), 0), coalesce(sum((l.quantity)::numeric), 0)
      into v_total, v_qty
      from jsonb_to_recordset(p_lines) as l(total text, quantity text);

    insert into public.sales (
        pos_sale_id, pos_customer_id, salesperson, customer_name, comment,
        invoice_total, items_net, items_sold, items_returned, invoice_datetime
    ) values (
        p_pos_sale_id, p_pos_customer_id, p_salesperson, p_customer_name, p_comment,
        v_total, v_qty, v_qty, 0, p_invoice_datetime
    )
    on conflict (pos_sale_id) do nothing
    returning id into v_sale_id;

    if v_sale_id is null then  -- already there (a retry, or the ETL got there first)
        return jsonb_build_object('pos_sale_id', p_pos_sale_id, 'inserted', false);
    end if;

    insert into public.sale_items (pos_sale_id, pos_item_id, name, quantity, unit_price, total, cost_price)
    select p_pos_sale_id, l.pos_item_id, l.name, l.quantity, l.unit_price, l.total, i.cost_price
      from jsonb_to_recordset(p_lines) as l(
               pos_item_id integer, name text, quantity numeric, unit_price numeric, total numeric)
      left join public.items i on i.pos_item_id = l.pos_item_id;

    insert into public.payments (pos_sale_id, account_id, account, amount)
    select p_pos_sale_id, p.account_id, p.account, p.amount
      from jsonb_to_recordset(p_payments) as p(account_id uuid, account text, amount numeric);

    return jsonb_build_object('pos_sale_id', p_pos_sale_id, 'inserted', true);
end $$;

commit;
