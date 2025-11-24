"""
Set up Dough Zone analytics schemas and tables in PostgreSQL.

Usage examples:
    export DATABASE_URL="postgresql://user:pass@localhost:5432/doughzone"
    python setup_database.py

    python setup_database.py --host localhost --port 5432 --dbname doughzone --user postgres
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable


try:
    import psycopg  # type: ignore[attr-defined]

    IS_PSYCOPG3 = True
except ImportError:  # pragma: no cover - fallback
    try:
        import psycopg2 as psycopg  # type: ignore

        IS_PSYCOPG3 = False
    except ImportError as exc:  # pragma: no cover - guidance for user
        raise SystemExit(
            "Install the PostgreSQL driver first: pip install psycopg[binary] or psycopg2-binary"
        ) from exc


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS core.dim_location (
    location_id TEXT PRIMARY KEY,
    location_name TEXT NOT NULL,
    timezone TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dim_employee (
    employee_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dim_revenue_center (
    center_id TEXT PRIMARY KEY,
    center_name TEXT NOT NULL,
    dining_area TEXT,
    service_period TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dim_menu_item (
    menu_item_id TEXT PRIMARY KEY,
    sku TEXT,
    plu TEXT,
    menu_item_name TEXT NOT NULL,
    menu_group TEXT,
    menu_subgroup TEXT,
    menu_name TEXT,
    sales_category TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    effective_from DATE,
    effective_to DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dim_modifier (
    modifier_id TEXT PRIMARY KEY,
    option_group_id TEXT,
    option_group_name TEXT,
    modifier_name TEXT NOT NULL,
    sales_category TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.fact_order (
    order_id BIGINT PRIMARY KEY,
    location_id TEXT REFERENCES core.dim_location(location_id),
    revenue_center_id TEXT REFERENCES core.dim_revenue_center(center_id),
    opened_at TIMESTAMPTZ,
    paid_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    guest_count INTEGER,
    check_count INTEGER,
    dining_area TEXT,
    dining_option TEXT,
    order_source TEXT,
    service_period TEXT,
    tab_name TEXT,
    discount_amount NUMERIC(12, 2),
    amount NUMERIC(12, 2),
    tax NUMERIC(12, 2),
    tip NUMERIC(12, 2),
    gratuity NUMERIC(12, 2),
    total NUMERIC(12, 2),
    voided BOOLEAN,
    duration_seconds INTEGER,
    business_date DATE,
    last_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.fact_check (
    check_id BIGINT PRIMARY KEY,
    order_id BIGINT REFERENCES core.fact_order(order_id),
    check_number INTEGER,
    tab_name TEXT,
    status TEXT,
    dining_option TEXT,
    server_id TEXT REFERENCES core.dim_employee(employee_id),
    paid_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    amount NUMERIC(12, 2),
    tax NUMERIC(12, 2),
    tip NUMERIC(12, 2),
    gratuity NUMERIC(12, 2),
    guest_count INTEGER,
    business_date DATE
);

CREATE TABLE IF NOT EXISTS core.fact_payment (
    payment_id BIGINT PRIMARY KEY,
    check_id BIGINT REFERENCES core.fact_check(check_id),
    payment_type TEXT,
    card_type TEXT,
    house_account_number TEXT,
    amount NUMERIC(12, 2),
    tip NUMERIC(12, 2),
    gratuity NUMERIC(12, 2),
    total NUMERIC(12, 2),
    swiped_amount NUMERIC(12, 2),
    keyed_amount NUMERIC(12, 2),
    refunded BOOLEAN DEFAULT FALSE,
    refunded_at TIMESTAMPTZ,
    refund_amount NUMERIC(12, 2),
    refund_tip_amount NUMERIC(12, 2),
    void_user TEXT,
    void_approver TEXT,
    status TEXT,
    source TEXT,
    email TEXT,
    phone TEXT,
    card_last4 TEXT,
    gift_card_last4 TEXT,
    gift_card_first5 TEXT,
    business_date DATE
);

CREATE TABLE IF NOT EXISTS core.fact_order_item (
    item_selection_id BIGINT PRIMARY KEY,
    order_id BIGINT REFERENCES core.fact_order(order_id),
    check_id BIGINT REFERENCES core.fact_check(check_id),
    menu_item_id TEXT REFERENCES core.dim_menu_item(menu_item_id),
    menu_item_name TEXT,
    sales_category TEXT,
    gross_price NUMERIC(12, 2),
    discount NUMERIC(12, 2),
    net_price NUMERIC(12, 2),
    qty NUMERIC(10, 2),
    tax NUMERIC(12, 2),
    voided BOOLEAN DEFAULT FALSE,
    deferred BOOLEAN,
    tax_exempt BOOLEAN,
    tax_mode TEXT,
    dining_option_tax TEXT,
    business_date DATE
);

CREATE TABLE IF NOT EXISTS core.fact_order_item_modifier (
    modifier_selection_id BIGINT PRIMARY KEY,
    item_selection_id BIGINT REFERENCES core.fact_order_item(item_selection_id),
    modifier_id TEXT REFERENCES core.dim_modifier(modifier_id),
    modifier_name TEXT,
    option_group_name TEXT,
    price_delta NUMERIC(12, 2),
    qty NUMERIC(10, 2),
    voided BOOLEAN DEFAULT FALSE,
    business_date DATE
);

CREATE TABLE IF NOT EXISTS core.fact_kitchen_timing (
    kitchen_event_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES core.fact_order(order_id),
    item_selection_id BIGINT REFERENCES core.fact_order_item(item_selection_id),
    station_name TEXT,
    fired_at TIMESTAMPTZ,
    bumped_at TIMESTAMPTZ,
    duration_seconds INTEGER,
    business_date DATE
);

CREATE TABLE IF NOT EXISTS core.fact_house_account_txn (
    txn_id BIGSERIAL PRIMARY KEY,
    account_number TEXT NOT NULL,
    payment_id BIGINT REFERENCES core.fact_payment(payment_id),
    amount NUMERIC(12, 2),
    balance_after NUMERIC(12, 2),
    business_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.fact_cash_drawer (
    cash_entry_id BIGINT PRIMARY KEY,
    location_id TEXT REFERENCES core.dim_location(location_id),
    drawer_name TEXT,
    entry_type TEXT,
    entry_ts TIMESTAMPTZ,
    amount NUMERIC(12, 2),
    note TEXT,
    business_date DATE
);

CREATE TABLE IF NOT EXISTS core.fact_time_entry (
    time_entry_id BIGINT PRIMARY KEY,
    employee_id TEXT REFERENCES core.dim_employee(employee_id),
    payroll_category TEXT,
    clock_in TIMESTAMPTZ,
    clock_out TIMESTAMPTZ,
    hours_worked NUMERIC(10, 2),
    business_date DATE,
    location_id TEXT REFERENCES core.dim_location(location_id)
);

CREATE TABLE IF NOT EXISTS meta.etl_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    business_date DATE NOT NULL,
    source_folder TEXT NOT NULL,
    status TEXT NOT NULL,
    row_counts JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fact_order_business_date
    ON core.fact_order (business_date);
CREATE INDEX IF NOT EXISTS idx_fact_order_source
    ON core.fact_order (order_source);
CREATE INDEX IF NOT EXISTS idx_fact_payment_business_date
    ON core.fact_payment (business_date);
CREATE INDEX IF NOT EXISTS idx_fact_payment_type
    ON core.fact_payment (payment_type);
CREATE INDEX IF NOT EXISTS idx_fact_order_item_menu
    ON core.fact_order_item (menu_item_id, business_date);
CREATE INDEX IF NOT EXISTS idx_fact_order_item_modifier_item
    ON core.fact_order_item_modifier (item_selection_id);
CREATE INDEX IF NOT EXISTS idx_fact_time_entry_employee
    ON core.fact_time_entry (employee_id, business_date);
CREATE INDEX IF NOT EXISTS idx_fact_cash_drawer_location
    ON core.fact_cash_drawer (location_id, business_date);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision PostgreSQL schemas for Dough Zone data.")
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", "postgres"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument(
        "--dsn",
        default=os.getenv("DATABASE_URL"),
        help="Full PostgreSQL DSN (overrides individual connection pieces).",
    )
    return parser.parse_args()


def get_connection(args: argparse.Namespace):
    if args.dsn:
        conn = psycopg.connect(args.dsn)
    else:
        conn = psycopg.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
        )

    if not IS_PSYCOPG3:
        conn.autocommit = True
    else:
        conn.autocommit = True

    return conn


def execute_statements(conn, sql_blob: str) -> None:
    statements = [stmt.strip() for stmt in sql_blob.strip().split(";\n") if stmt.strip()]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement + ";")


def main() -> None:
    args = parse_args()
    conn = get_connection(args)
    try:
        execute_statements(conn, SCHEMA_SQL)
        print("Schemas provisioned successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
