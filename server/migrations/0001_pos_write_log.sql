-- 0001: write-ahead log for every change the server makes to POS4Africa.
--
-- The server's own bookkeeping only.  Business data (sales, customers, ...) reaches
-- Supabase exclusively through the ETL, so there is no server-side mirroring here.
--
-- NOT applied automatically. Review, then apply with `supabase db push`
-- or the Supabase MCP `apply_migration`.

begin;

create table if not exists public.pos_write_log (
    id           uuid primary key default gen_random_uuid(),
    kind         text not null check (kind in ('CUSTOMER_CREATE','CUSTOMER_UPDATE','SALE_CREATE')),
    ref          text not null unique,          -- from the client's Idempotency-Key; also in the POS comment
    request_hash text not null,
    status       text not null default 'PENDING'
                 check (status in ('PENDING','SENDING','POSTED','FAILED','UNCERTAIN')),
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

commit;
