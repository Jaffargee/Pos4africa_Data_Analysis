-- Tahir General POS / Analytics - portable public schema
-- Generated from the current Supabase project.
-- NOTE: This is schema-only: NO application data is included.
-- Supabase-managed schemas (auth, storage, realtime, etc.) are intentionally
-- not recreated because a new Supabase project supplies those.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- ENUMS
-- ============================================================

CREATE TYPE public.address_label AS ENUM ('Home','Work','Business','Delivery','Shipping');
CREATE TYPE public.contact_method_type AS ENUM ('email','phone');
CREATE TYPE public.customer_category AS ENUM ('WHSL1','WHSL2','RGL','RIWC','SEASONAL','VIP','STANDARD');
CREATE TYPE public.customer_status_level AS ENUM ('SILVER','GOLD','PLATINUM','DIAMOND');
CREATE TYPE public.delivery_method AS ENUM ('PICKUP','LOCAL_DELIVERY','COURIER','BUS_TRANSPORT','AGENT');
CREATE TYPE public.delivery_priority AS ENUM ('STANDARD','EXPRESS','URGENT');
CREATE TYPE public.delivery_status AS ENUM ('PENDING','DISPATCHED_TO_PARK','IN_TRANSIT','DELIVERED','FAILED');
CREATE TYPE public.item_lifecycle_status AS ENUM ('ACTIVE','SLOW','DEAD');
CREATE TYPE public.payment_type_enum AS ENUM ('Single Payment','Multiple Payments');
CREATE TYPE public.transaction_medium_enum AS ENUM ('Cash Payment','Debit Card','Bank Transfer');
CREATE TYPE public.transit_type AS ENUM ('MARKET_RUN','PARK_TRANSIT');
CREATE TYPE public.trip_status AS ENUM ('LOADING','DEPARTED','COMPLETED');

-- ============================================================
-- SEQUENCES
-- ============================================================

CREATE SEQUENCE public.app_pos_sale_id_seq;
CREATE SEQUENCE public.delivery_seq;

-- ============================================================
-- TABLES
-- ============================================================

CREATE TABLE public.customers (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    pos_customer_id integer NOT NULL,
    first_name text NOT NULL,
    last_name text,
    company_phone text,
    company_name text,
    internal_notes text,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    email text,
    company_email text,
    company_website text,
    category public.customer_category DEFAULT 'STANDARD'::public.customer_category NOT NULL,
    status_level public.customer_status_level DEFAULT 'SILVER'::public.customer_status_level NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    total_spent numeric(15,2) DEFAULT 0 NOT NULL,
    total_orders integer DEFAULT 0 NOT NULL,
    total_quantity_purchased integer DEFAULT 0 NOT NULL,
    lifetime_value numeric(15,2) DEFAULT 0 NOT NULL,
    category_updated_at timestamptz,
    status_updated_at timestamptz,
    last_order_at timestamptz,
    auto_email_receipt boolean DEFAULT false,
    always_sms_receipt boolean DEFAULT false,
    message_to_show_when_adding_customer_to_sale text,
    comment text,
    balance numeric(15,2) DEFAULT 0 NOT NULL,
    credit_limit numeric(15,2) DEFAULT 0 NOT NULL,
    taxable boolean DEFAULT true NOT NULL,
    non_tax_certificate_number text,
    default_invoice_terms text,
    disable_loyalty boolean DEFAULT false NOT NULL,
    profit_contribution numeric DEFAULT 0 NOT NULL,
    customer_name text GENERATED ALWAYS AS
      ((first_name || COALESCE((' '::text || last_name), ''::text))) STORED,
    CONSTRAINT customers_pkey PRIMARY KEY (id),
    CONSTRAINT customers_email_key UNIQUE (email),
    CONSTRAINT customers_pos_customer_id_key UNIQUE (pos_customer_id)
);

CREATE TABLE public.suppliers (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    pos_supplier_id integer NOT NULL,
    company_name text NOT NULL,
    first_name text,
    last_name text,
    email text,
    phone_number text,
    address_1 text,
    address_2 text,
    city text,
    state text,
    zip text,
    country text,
    comments text,
    account_no text,
    internal_notes text,
    balance numeric(15,2) DEFAULT 0.00,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    CONSTRAINT suppliers_pkey PRIMARY KEY (id),
    CONSTRAINT suppliers_pos_supplier_id_key UNIQUE (pos_supplier_id)
);

CREATE TABLE public.items (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    pos_item_id integer NOT NULL,
    item_number text,
    product_id text,
    item_name text NOT NULL,
    barcode_display_name text,
    variation text,
    quantity_unit_quantity numeric(10,4),
    category text,
    supplier_id integer,
    allow_price_override_regardless_of_permissions boolean DEFAULT false,
    disable_from_price_rules boolean DEFAULT false,
    only_allow_items_to_be_sold_in_whole_numbers boolean DEFAULT false,
    sold_in_a_series boolean DEFAULT false,
    series_quantity integer,
    number_of_days_series_must_be_used_within integer,
    is_barcoded boolean DEFAULT false,
    inactive boolean DEFAULT false,
    default_quantity_when_selling_or_receiving numeric(10,4) DEFAULT 1,
    cost_price numeric(15,2) DEFAULT 0.00,
    supply_price numeric(15,2) DEFAULT 0.00,
    selling_price numeric(15,2) DEFAULT 0.00,
    promo_price numeric(15,2),
    promo_start_date date,
    promo_end_date date,
    price_includes_tax boolean DEFAULT false,
    is_service boolean DEFAULT false,
    is_favorite boolean DEFAULT false,
    quantity numeric(10,4) DEFAULT 0,
    reorder_level numeric(10,4),
    replenish_level numeric(10,4),
    description text,
    long_description text,
    information_popup_when_adding_to_sale text,
    weight numeric(10,4),
    weight_unit text,
    length numeric(10,4),
    width numeric(10,4),
    height numeric(10,4),
    allow_alt_description boolean DEFAULT false,
    item_has_serial_number boolean DEFAULT false,
    commission numeric(10,4),
    commission_percent_based_on_profit boolean DEFAULT false,
    tax_group text,
    tags text,
    days_to_expiration integer,
    change_cost_price_during_sale boolean DEFAULT false,
    manufacturer text,
    location_at_store text,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    stock_lifecycle_status public.item_lifecycle_status DEFAULT 'ACTIVE'::public.item_lifecycle_status NOT NULL,
    lifecycle_marked_dead_at timestamptz,
    lifecycle_sales_since_dead integer DEFAULT 0 NOT NULL,
    lifecycle_last_evaluated_at timestamptz,
    excluded_from_analytics boolean GENERATED ALWAYS AS
      ((stock_lifecycle_status = 'DEAD'::public.item_lifecycle_status)) STORED,
    CONSTRAINT items_pkey PRIMARY KEY (id),
    CONSTRAINT items_pos_item_id_key UNIQUE (pos_item_id)
);

CREATE TABLE public.sales (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    pos_sale_id bigint NOT NULL,
    pos_customer_id integer NOT NULL,
    salesperson text NOT NULL,
    customer_name text DEFAULT ''::text NOT NULL,
    comment text,
    is_anonymous_customer boolean,
    invoice_total numeric NOT NULL,
    items_net bigint NOT NULL,
    items_sold bigint NOT NULL,
    items_returned bigint NOT NULL,
    invoice_datetime timestamptz NOT NULL,
    scraped_at timestamptz DEFAULT (now() AT TIME ZONE 'utc'::text) NOT NULL,
    hash text,
    CONSTRAINT sale_pkey PRIMARY KEY (id),
    CONSTRAINT sale_pos_sale_id_key UNIQUE (pos_sale_id)
);

CREATE TABLE public.sale_items (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    pos_sale_id bigint,
    pos_item_id integer,
    name text NOT NULL,
    quantity bigint NOT NULL,
    unit_price numeric NOT NULL,
    total numeric NOT NULL,
    cost_price numeric,
    total_cost numeric GENERATED ALWAYS AS
      (((quantity)::numeric * cost_price)) STORED,
    gross_profit numeric GENERATED ALWAYS AS
      ((total - ((quantity)::numeric * cost_price))) STORED,
    CONSTRAINT sales_items_pkey PRIMARY KEY (id)
);

CREATE TABLE public.accounts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    bank_name text NOT NULL,
    name text NOT NULL,
    account_no text NOT NULL,
    balance numeric NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT accounts_pkey PRIMARY KEY (id)
);

CREATE TABLE public.payments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    pos_sale_id bigint NOT NULL,
    account_id uuid NOT NULL,
    account text NOT NULL,
    amount numeric NOT NULL,
    CONSTRAINT payments_pkey PRIMARY KEY (id)
);

CREATE TABLE public.report_periods (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    period_from date NOT NULL,
    period_to date NOT NULL,
    notes text,
    created_at timestamptz DEFAULT now(),
    CONSTRAINT report_periods_pkey PRIMARY KEY (id)
);

CREATE TABLE public.customer_contact_methods (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    customer_id uuid NOT NULL,
    type public.contact_method_type NOT NULL,
    value text NOT NULL,
    is_primary boolean DEFAULT false NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT customer_contact_methods_pkey PRIMARY KEY (id)
);

CREATE TABLE public.customer_addresses (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    customer_id uuid NOT NULL,
    label public.address_label NOT NULL,
    is_primary boolean DEFAULT false NOT NULL,
    line_1 text NOT NULL,
    line_2 text,
    city text NOT NULL,
    state text NOT NULL,
    postal_code text,
    country text DEFAULT 'Nigeria'::text NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT customer_addresses_pkey PRIMARY KEY (id)
);

CREATE TABLE public.customer_accounts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    customer_id uuid NOT NULL,
    account_no text NOT NULL,
    account_name text NOT NULL,
    bank_name text NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT customer_accounts_pkey PRIMARY KEY (id)
);

CREATE TABLE public.customer_metrics (
    customer_id uuid NOT NULL,
    total_orders integer DEFAULT 0 NOT NULL,
    total_successful_transactions integer DEFAULT 0 NOT NULL,
    total_spent numeric(15,2) DEFAULT 0 NOT NULL,
    total_quantity_purchased integer DEFAULT 0 NOT NULL,
    average_order_value numeric(15,2) DEFAULT 0 NOT NULL,
    largest_single_order numeric(15,2) DEFAULT 0 NOT NULL,
    purchase_frequency_score numeric(5,2) DEFAULT 0 NOT NULL,
    loyalty_score numeric(5,2) DEFAULT 0 NOT NULL,
    returned_orders integer DEFAULT 0 NOT NULL,
    cancelled_orders integer DEFAULT 0 NOT NULL,
    last_purchase_at timestamptz,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT customer_metrics_pkey PRIMARY KEY (customer_id)
);

CREATE TABLE public.customer_category_rules (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    category public.customer_category NOT NULL,
    min_total_spent numeric(15,2) DEFAULT 0 NOT NULL,
    min_total_quantity integer DEFAULT 0 NOT NULL,
    min_orders integer DEFAULT 0 NOT NULL,
    benefits jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT customer_category_rules_pkey PRIMARY KEY (id),
    CONSTRAINT customer_category_rules_category_key UNIQUE (category)
);

CREATE TABLE public.status_level_rules (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    level public.customer_status_level NOT NULL,
    min_total_spent numeric(15,2) DEFAULT 0 NOT NULL,
    min_orders integer DEFAULT 0 NOT NULL,
    min_loyalty_score numeric(5,2) DEFAULT 0 NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT status_level_rules_pkey PRIMARY KEY (id),
    CONSTRAINT status_level_rules_level_key UNIQUE (level)
);

CREATE TABLE public."whatsApp_tracking" (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    items_id uuid NOT NULL,
    item_name text NOT NULL,
    media_type text DEFAULT 'image'::text NOT NULL,
    "time" timestamptz,
    posted_at timestamptz NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT whatsApp_tracking_pkey PRIMARY KEY (id)
);

CREATE TABLE public.market_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    event_name text NOT NULL,
    event_type text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    notes text,
    CONSTRAINT market_events_pkey PRIMARY KEY (id)
);

CREATE TABLE public.delivery_trips (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    destination_state text NOT NULL,
    driver_name text NOT NULL,
    driver_phone text NOT NULL,
    vehicle_plate text,
    park_name text,
    loaded_by text,
    loaded_at timestamptz,
    status public.trip_status DEFAULT 'LOADING'::public.trip_status NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT delivery_trips_pkey PRIMARY KEY (id)
);

CREATE TABLE public.deliveries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    customer_id uuid,
    customer_name text NOT NULL,
    customer_phone text,
    pos_sale_id bigint,
    transit_mode public.transit_type DEFAULT 'MARKET_RUN'::public.transit_type NOT NULL,
    status public.delivery_status DEFAULT 'PENDING'::public.delivery_status NOT NULL,
    destination_label text,
    destination_state text NOT NULL,
    destination_lga text,
    destination_country text DEFAULT 'Nigeria'::text NOT NULL,
    shop_handover_by text,
    local_runner_name text,
    left_shop_at timestamptz,
    trip_id uuid,
    driver_name text,
    driver_phone text,
    vehicle_plate text,
    park_loaded_at timestamptz,
    package_description text,
    package_weight numeric(8,2),
    package_count integer DEFAULT 1,
    delivery_fee numeric(15,2) DEFAULT 0,
    cod_amount numeric(15,2) DEFAULT 0,
    is_paid boolean DEFAULT false,
    delivered_at timestamptz,
    notes text,
    failure_reason text,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT deliveries_pkey PRIMARY KEY (id)
);

CREATE TABLE public.product_sales_frequency_metrics (
    pos_item_id integer NOT NULL,
    item_name text NOT NULL,
    category text,
    stock_lifecycle_status public.item_lifecycle_status NOT NULL,
    first_sale_date date,
    last_sale_date date,
    selling_days integer DEFAULT 0 NOT NULL,
    average_gap_days numeric(12,2),
    units_sold numeric DEFAULT 0 NOT NULL,
    average_units_per_selling_day numeric(12,2),
    average_weekly_demand numeric(12,2),
    revenue numeric(18,2) DEFAULT 0 NOT NULL,
    revenue_contribution_pct numeric(12,4),
    previous_4_week_units numeric DEFAULT 0 NOT NULL,
    recent_4_week_units numeric DEFAULT 0 NOT NULL,
    trend_growth_pct numeric(12,2),
    trend text NOT NULL,
    frequency_band text NOT NULL,
    refreshed_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT product_sales_frequency_metrics_pkey PRIMARY KEY (pos_item_id)
);

-- ============================================================
-- FOREIGN KEYS
-- ============================================================

ALTER TABLE public.customer_accounts
  ADD CONSTRAINT customer_accounts_customer_id_fkey
  FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE;

ALTER TABLE public.customer_addresses
  ADD CONSTRAINT customer_addresses_customer_id_fkey
  FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE;

ALTER TABLE public.customer_contact_methods
  ADD CONSTRAINT customer_contact_methods_customer_id_fkey
  FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE;

ALTER TABLE public.customer_metrics
  ADD CONSTRAINT customer_metrics_customer_id_fkey
  FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE;

ALTER TABLE public.items
  ADD CONSTRAINT items_supplier_id_fkey
  FOREIGN KEY (supplier_id) REFERENCES public.suppliers(pos_supplier_id)
  ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE public.sales
  ADD CONSTRAINT sale_pos_customer_id_fkey
  FOREIGN KEY (pos_customer_id) REFERENCES public.customers(pos_customer_id)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE public.sale_items
  ADD CONSTRAINT sales_items_pos_sale_id_fkey
  FOREIGN KEY (pos_sale_id) REFERENCES public.sales(pos_sale_id)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE public.sale_items
  ADD CONSTRAINT sales_items_pos_item_id_fkey
  FOREIGN KEY (pos_item_id) REFERENCES public.items(pos_item_id)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE public.payments
  ADD CONSTRAINT payments_pos_sale_id_fkey
  FOREIGN KEY (pos_sale_id) REFERENCES public.sales(pos_sale_id)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE public.payments
  ADD CONSTRAINT payments_account_id_fkey
  FOREIGN KEY (account_id) REFERENCES public.accounts(id)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE public."whatsApp_tracking"
  ADD CONSTRAINT whatsApp_tracking_items_id_fkey
  FOREIGN KEY (items_id) REFERENCES public.items(id)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE public.delivery_trips
  ADD CONSTRAINT delivery_trips_pkey CHECK (true) NOT VALID;

ALTER TABLE public.deliveries
  ADD CONSTRAINT deliveries_trip_id_fkey
  FOREIGN KEY (trip_id) REFERENCES public.delivery_trips(id)
  ON DELETE SET NULL;

ALTER TABLE public.product_sales_frequency_metrics
  ADD CONSTRAINT product_sales_frequency_metrics_pos_item_id_fkey
  FOREIGN KEY (pos_item_id) REFERENCES public.items(pos_item_id)
  ON DELETE CASCADE;

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_customer_accounts_customer
  ON public.customer_accounts USING btree (customer_id);

CREATE INDEX idx_customer_address_customer
  ON public.customer_addresses USING btree (customer_id);

CREATE INDEX idx_customer_contact_customer
  ON public.customer_contact_methods USING btree (customer_id);

CREATE INDEX idx_customer_metrics_loyalty
  ON public.customer_metrics USING btree (loyalty_score DESC);

CREATE INDEX idx_customer_metrics_spent
  ON public.customer_metrics USING btree (total_spent DESC);

CREATE INDEX idx_customers_active
  ON public.customers USING btree (is_active);

CREATE INDEX idx_customers_balance
  ON public.customers USING btree (balance DESC);

CREATE INDEX idx_customers_category
  ON public.customers USING btree (category);

CREATE INDEX idx_customers_last_order
  ON public.customers USING btree (last_order_at DESC);

CREATE INDEX idx_customers_status_level
  ON public.customers USING btree (status_level);

CREATE INDEX idx_customers_total_spent
  ON public.customers USING btree (total_spent DESC);

CREATE INDEX idx_deliveries_created
  ON public.deliveries USING btree (created_at DESC);

CREATE INDEX idx_deliveries_state_lga
  ON public.deliveries USING btree (destination_state, destination_lga);

CREATE INDEX idx_deliveries_status
  ON public.deliveries USING btree (status);

CREATE INDEX idx_deliveries_transit_mode
  ON public.deliveries USING btree (transit_mode);

CREATE INDEX idx_deliveries_trip
  ON public.deliveries USING btree (trip_id);

-- ============================================================
-- ROW LEVEL SECURITY
-- ============================================================

ALTER TABLE public.customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.suppliers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.items ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sales ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sale_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payments ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.report_periods ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.customer_contact_methods ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.customer_addresses ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.customer_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.customer_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.customer_category_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.status_level_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE public."whatsApp_tracking" ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.market_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.delivery_trips ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.product_sales_frequency_metrics ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated staff can read deliveries"
ON public.deliveries FOR SELECT TO public
USING (auth.role() = 'authenticated');

CREATE POLICY "Authenticated staff can update deliveries"
ON public.deliveries FOR UPDATE TO public
USING (auth.role() = 'authenticated');

CREATE POLICY "Authenticated staff can write deliveries"
ON public.deliveries FOR INSERT TO public
WITH CHECK (auth.role() = 'authenticated');

CREATE POLICY "Authenticated staff can read trips"
ON public.delivery_trips FOR SELECT TO public
USING (auth.role() = 'authenticated');

CREATE POLICY "Authenticated staff can write trips"
ON public.delivery_trips FOR INSERT TO public
WITH CHECK (auth.role() = 'authenticated');

CREATE POLICY product_sales_frequency_metrics_authenticated_read
ON public.product_sales_frequency_metrics FOR SELECT TO authenticated
USING (true);

-- ============================================================
-- IMPORTANT
-- ============================================================
-- The current database also contains legacy functions that reference
-- objects no longer present in the current public schema (notably the
-- old `transactions`/`summary` model), plus analytics views/functions.
-- Those objects should NOT be blindly replayed into a new database:
-- some are internally inconsistent with the current tables.
--
-- The current migration history contains 19 migrations through
-- 2026-08-30. The canonical way to reproduce EVERY historical object,
-- including all functions/views/triggers/scheduled jobs, is to export
-- the project's migration history or use:
--
--   supabase db dump --db-url "$SOURCE_DATABASE_URL" -f schema.sql
--
-- This file deliberately gives the clean current table/type/constraint/
-- index/RLS foundation rather than pretending the legacy objects are
-- executable when they currently reference missing tables/columns.

COMMIT;
