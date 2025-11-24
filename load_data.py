"""
Load Dough Zone daily exports into PostgreSQL.

Usage:
    export DATABASE_URL="postgresql://postgres:password@127.0.0.1:5432/doughzone"
    python load_data.py --root "/path/to/Analytics Dashboard Data" --location 90984

Optional filters:
    python load_data.py --location 90984 --date 20250209
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:
    import psycopg  # type: ignore[attr-defined]

    IS_PSYCOPG3 = True
except ImportError:  # pragma: no cover - fallback
    import psycopg2 as psycopg  # type: ignore

    IS_PSYCOPG3 = False


ROOT_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CSV exports into PostgreSQL.")
    parser.add_argument(
        "--root",
        default=str(ROOT_DIR),
        help="Root folder containing location subdirectories (defaults to script directory).",
    )
    parser.add_argument("--location", help="Location folder (e.g., 90984). Defaults to all locations.")
    parser.add_argument(
        "--date",
        action="append",
        help="Business date in YYYYMMDD. Use multiple --date flags to load several specific days.",
    )
    parser.add_argument("--host", default=os.getenv("PGHOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", "postgres"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL"))
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

    if IS_PSYCOPG3:
        conn.autocommit = False
    else:
        conn.autocommit = False

    return conn


def iter_location_dirs(root: Path, selection: Optional[str]) -> Iterable[Path]:
    for path in sorted(root.iterdir()):
        if not path.is_dir():
            continue
        if not path.name.isdigit():
            continue
        if selection and path.name != selection:
            continue
        yield path


def iter_business_dates(location_dir: Path, specific_dates: Optional[Sequence[str]]) -> Iterable[Tuple[str, Path]]:
    if specific_dates:
        for day in specific_dates:
            target = location_dir / day
            if target.is_dir():
                yield day, target
        return

    for path in sorted(location_dir.iterdir()):
        if path.is_dir() and path.name.isdigit() and len(path.name) == 8:
            yield path.name, path


def parse_bool(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False})
    )


def parse_datetime_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def parse_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def read_csv_safe(path: Path) -> pd.DataFrame:
    """Read CSV with UTF-8 first, then fall back to latin-1 and skip bad lines."""
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1", on_bad_lines="skip")


def duration_to_seconds(duration_text: pd.Series) -> pd.Series:
    def _parse(value: Optional[str]) -> Optional[int]:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        if not value or value in {"", "NONE", "nan"}:
            return None
        parts = str(value).split(":")
        if len(parts) != 3:
            return None
        try:
            hours, minutes, seconds = [int(part) for part in parts]
        except ValueError:
            return None
        return hours * 3600 + minutes * 60 + seconds

    return duration_text.apply(_parse)


def to_date(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def cleaned_rows(df: pd.DataFrame, columns: Sequence[str]) -> List[Tuple]:
    if df.empty:
        return []
    out_rows: List[Tuple] = []
    for _, row in df.iterrows():
        converted: List = []
        for col in columns:
            val = row[col]
            if isinstance(val, pd.Timestamp):
                converted.append(val.to_pydatetime())
                continue
            if pd.isna(val):
                converted.append(None)
            else:
                converted.append(val)
        out_rows.append(tuple(converted))
    return out_rows


def upsert_rows(
    cur,
    table: str,
    columns: Sequence[str],
    rows: List[Tuple],
    conflict_cols: Sequence[str],
) -> int:
    if not rows:
        return 0

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    conflict_clause = ", ".join(conflict_cols)
    update_cols = [col for col in columns if col not in conflict_cols]
    if update_cols:
        update_clause = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
        query = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"
    else:
        query = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({conflict_clause}) DO NOTHING"
    cur.executemany(query, rows)
    return len(rows)


class DataLoader:
    def __init__(self, root: Path, conn) -> None:
        self.root = root
        self.conn = conn

    def run(self, location_filter: Optional[str], specific_dates: Optional[Sequence[str]]) -> None:
        for location_dir in iter_location_dirs(self.root, location_filter):
            location_id = location_dir.name
            for business_date, day_dir in iter_business_dates(location_dir, specific_dates):
                print(f"Loading {location_id} for {business_date}")
                self.load_business_day(location_id, business_date, day_dir)

    def load_business_day(self, location_id: str, business_date_text: str, day_dir: Path) -> None:
        business_dt = to_date(business_date_text)
        orders_path = day_dir / "OrderDetails.csv"
        if not orders_path.exists():
            print(f"  Skipping {business_date_text}: OrderDetails.csv missing")
            return

        orders = read_csv_safe(orders_path)
        if orders.empty:
            print(f"  Skipping {business_date_text}: OrderDetails.csv empty")
            return

        orders = orders.rename(
            columns={
                "Order Id": "order_id",
                "Location": "location_name",
                "Opened": "opened_at",
                "Paid": "paid_at",
                "Closed": "closed_at",
                "Checks": "check_count",
                "# of Guests": "guest_count",
                "Tab Names": "tab_name",
                "Revenue Center": "revenue_center_name",
                "Dining Area": "dining_area",
                "Service": "service_period",
                "Dining Options": "dining_option",
                "Order Source": "order_source",
                "Discount Amount": "discount_amount",
                "Amount": "amount",
                "Tax": "tax",
                "Tip": "tip",
                "Gratuity": "gratuity",
                "Total": "total",
                "Voided": "voided",
                "Duration (Opened to Paid)": "duration_text",
            }
        )

        location_name = orders["location_name"].dropna().iloc[0]
        revenue_centers = self.prepare_revenue_centers(location_id, orders)
        orders_clean = self.prepare_orders(location_id, business_dt, orders)

        payments = self.prepare_payments(day_dir, business_dt)
        items = self.prepare_items(day_dir, business_dt)
        checks = self.prepare_checks(payments, orders_clean, items)
        valid_item_ids = set(items["item_selection_id"].dropna().tolist()) if not items.empty else set()
        modifiers = self.prepare_modifiers(day_dir, business_dt, valid_item_ids)
        cash_entries = self.prepare_cash_entries(day_dir, business_dt, location_id)
        time_entries = self.prepare_time_entries(day_dir, business_dt, location_id)

        menu_dim = self.prepare_menu_dim(items)
        modifier_dim = self.prepare_modifier_dim(modifiers)
        employees_dim = self.prepare_employees(time_entries)

        row_counts: Dict[str, int] = {}

        with self.conn.cursor() as cur:
            row_counts["dim_location"] = self.upsert_location(cur, location_id, location_name)
            row_counts["dim_revenue_center"] = self.upsert_revenue_centers(cur, revenue_centers)
            row_counts["dim_menu_item"] = self.upsert_menu_items(cur, menu_dim)
            row_counts["dim_modifier"] = self.upsert_modifiers(cur, modifier_dim)
            row_counts["dim_employee"] = self.upsert_employees(cur, employees_dim)
            row_counts["fact_order"] = self.insert_orders(cur, orders_clean)
            row_counts["fact_check"] = self.insert_checks(cur, checks)
            row_counts["fact_payment"] = self.insert_payments(cur, payments)
            row_counts["fact_order_item"] = self.insert_order_items(cur, items)
            row_counts["fact_order_item_modifier"] = self.insert_item_modifiers(cur, modifiers)
            row_counts["fact_cash_drawer"] = self.insert_cash_entries(cur, cash_entries)
            row_counts["fact_time_entry"] = self.insert_time_entries(cur, time_entries)
            self.log_etl_run(cur, business_dt, str(day_dir), row_counts)

        self.conn.commit()
        print("  Loaded:", json.dumps(row_counts, default=int))

    def upsert_location(self, cur, location_id: str, location_name: str) -> int:
        rows = [(location_id, location_name, None)]
        return upsert_rows(
            cur,
            "core.dim_location",
            ["location_id", "location_name", "timezone"],
            rows,
            ["location_id"],
        )

    def prepare_revenue_centers(self, location_id: str, orders: pd.DataFrame) -> pd.DataFrame:
        df = (
            orders[["revenue_center_name", "dining_area", "service_period"]]
            .drop_duplicates()
            .copy()
        )
        df["center_name"] = df["revenue_center_name"].fillna("Unspecified")
        df["center_id"] = df["center_name"].apply(lambda val: f"{location_id}:{val}")
        return df[["center_id", "center_name", "dining_area", "service_period"]]

    def upsert_revenue_centers(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(df, ["center_id", "center_name", "dining_area", "service_period"])
        return upsert_rows(
            cur,
            "core.dim_revenue_center",
            ["center_id", "center_name", "dining_area", "service_period"],
            rows,
            ["center_id"],
        )

    def prepare_orders(self, location_id: str, business_dt: date, orders: pd.DataFrame) -> pd.DataFrame:
        orders = orders.copy()
        orders["order_id"] = parse_numeric(orders["order_id"]).astype("Int64")
        orders["opened_at"] = parse_datetime_series(orders["opened_at"])
        orders["paid_at"] = parse_datetime_series(orders["paid_at"])
        orders["closed_at"] = parse_datetime_series(orders["closed_at"])
        orders["check_count"] = parse_numeric(orders["check_count"]).astype("Int64")
        orders["guest_count"] = parse_numeric(orders["guest_count"]).astype("Int64")
        numeric_fields = ["discount_amount", "amount", "tax", "tip", "gratuity", "total"]
        for field in numeric_fields:
            orders[field] = parse_numeric(orders[field])
        orders["voided"] = parse_bool(orders["voided"])
        orders["duration_seconds"] = duration_to_seconds(orders["duration_text"])
        orders["business_date"] = business_dt
        orders["location_id"] = location_id
        orders["revenue_center_name"] = orders["revenue_center_name"].fillna("Unspecified")
        orders["revenue_center_id"] = orders["revenue_center_name"].apply(
            lambda value: f"{location_id}:{value}"
        )
        return orders[
            [
                "order_id",
                "location_id",
                "revenue_center_id",
                "opened_at",
                "paid_at",
                "closed_at",
                "guest_count",
                "check_count",
                "dining_area",
                "dining_option",
                "order_source",
                "service_period",
                "tab_name",
                "discount_amount",
                "amount",
                "tax",
                "tip",
                "gratuity",
                "total",
                "voided",
                "duration_seconds",
                "business_date",
            ]
        ]

    def insert_orders(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "order_id",
                "location_id",
                "revenue_center_id",
                "opened_at",
                "paid_at",
                "closed_at",
                "guest_count",
                "check_count",
                "dining_area",
                "dining_option",
                "order_source",
                "service_period",
                "tab_name",
                "discount_amount",
                "amount",
                "tax",
                "tip",
                "gratuity",
                "total",
                "voided",
                "duration_seconds",
                "business_date",
            ],
        )
        return upsert_rows(
            cur,
            "core.fact_order",
            [
                "order_id",
                "location_id",
                "revenue_center_id",
                "opened_at",
                "paid_at",
                "closed_at",
                "guest_count",
                "check_count",
                "dining_area",
                "dining_option",
                "order_source",
                "service_period",
                "tab_name",
                "discount_amount",
                "amount",
                "tax",
                "tip",
                "gratuity",
                "total",
                "voided",
                "duration_seconds",
                "business_date",
            ],
            rows,
            ["order_id"],
        )

    def prepare_payments(self, day_dir: Path, business_dt: date) -> pd.DataFrame:
        path = day_dir / "PaymentDetails.csv"
        if not path.exists():
            return pd.DataFrame()
            payments = read_csv_safe(path).rename(
            columns={
                "Payment Id": "payment_id",
                "Order Id": "order_id",
                "Check Id": "check_id",
                "Check #": "check_number",
                "Tab Name": "tab_name",
                "Dining Option": "dining_option",
                "Server": "server_name",
                "Paid Date": "paid_at",
                "Refund Date": "refund_date",
                "Amount": "amount",
                "Tip": "tip",
                "Gratuity": "gratuity",
                "Total": "total",
                "Swiped Card Amount": "swiped_amount",
                "Keyed Card Amount": "keyed_amount",
                "House Acct #": "house_account",
                "Refund Amount": "refund_amount",
                "Refund Tip Amount": "refund_tip_amount",
                "Status": "status",
                "Type": "payment_type",
                "Card Type": "card_type",
                "Email": "email",
                "Phone": "phone",
                "Last 4 Card Digits": "card_last4",
                "Last 4 Gift Card Digits": "gift_card_last4",
                "First 5 Gift Card Digits": "gift_card_first5",
                "Source": "source",
            }
        )
        numeric_cols = [
            "payment_id",
            "order_id",
            "check_id",
            "check_number",
            "amount",
            "tip",
            "gratuity",
            "total",
            "swiped_amount",
            "keyed_amount",
            "refund_amount",
            "refund_tip_amount",
        ]
        for field in numeric_cols:
            payments[field] = parse_numeric(payments[field])
        for field in ["payment_id", "order_id", "check_id", "check_number"]:
            payments[field] = payments[field].astype("Int64")
        payments["paid_at"] = parse_datetime_series(payments["paid_at"])
        payments["refund_date"] = parse_datetime_series(payments["refund_date"])
        payments["refunded"] = payments["refund_amount"].fillna(0) > 0
        payments["business_date"] = business_dt
        return payments

    def prepare_checks(self, payments: pd.DataFrame, orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
        if payments.empty:
            payment_agg = pd.DataFrame()
        else:
            payment_agg = (
            payments.groupby("check_id")
            .agg(
                order_id=("order_id", "max"),
                check_number=("check_number", "max"),
                tab_name=("tab_name", "max"),
                dining_option=("dining_option", "max"),
                paid_at=("paid_at", "max"),
                amount=("amount", "sum"),
                tip=("tip", "sum"),
                gratuity=("gratuity", "sum"),
                total=("total", "sum"),
                status=("status", "max"),
            )
            .reset_index()
        )
        if not payment_agg.empty:
            payment_agg["closed_at"] = payment_agg["paid_at"]
            payment_agg["business_date"] = orders["business_date"].iloc[0]
            payment_agg["server_id"] = None
            payment_agg["guest_count"] = None
            payment_agg["tax"] = None
        base_checks = pd.DataFrame()
        if not items.empty:
            base_checks = (
                items[
                    [
                        "check_id",
                        "order_id",
                        "order_number",
                        "tab_name",
                        "dining_option",
                        "business_date",
                    ]
                ]
                .drop_duplicates(subset=["check_id"])
                .rename(columns={"order_number": "check_number"})
            )
            base_checks["status"] = None
            base_checks["server_id"] = None
            base_checks["paid_at"] = None
            base_checks["closed_at"] = None
            base_checks["amount"] = None
            base_checks["tax"] = None
            base_checks["tip"] = None
            base_checks["gratuity"] = None
            base_checks["guest_count"] = None

        if payment_agg.empty and base_checks.empty:
            return pd.DataFrame()
        if payment_agg.empty:
            checks = base_checks
        elif base_checks.empty:
            checks = payment_agg
        else:
            checks = payment_agg.merge(
                base_checks,
                on="check_id",
                how="outer",
                suffixes=("", "_base"),
            )
            for column in ["order_id", "check_number", "tab_name", "dining_option", "business_date"]:
                base_col = f"{column}_base"
                if base_col in checks:
                    checks[column] = checks[column].combine_first(checks[base_col])
                    checks.drop(columns=[base_col], inplace=True)

        order_ids = set(orders["order_id"].dropna().tolist())
        if order_ids and not checks.empty:
            checks.loc[~checks["order_id"].isin(order_ids), "order_id"] = None

        return checks[
            [
                "check_id",
                "order_id",
                "check_number",
                "tab_name",
                "status",
                "dining_option",
                "server_id",
                "paid_at",
                "closed_at",
                "amount",
                "tax",
                "tip",
                "gratuity",
                "guest_count",
                "business_date",
            ]
        ]

    def insert_checks(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "check_id",
                "order_id",
                "check_number",
                "tab_name",
                "status",
                "dining_option",
                "server_id",
                "paid_at",
                "closed_at",
                "amount",
                "tax",
                "tip",
                "gratuity",
                "guest_count",
                "business_date",
            ],
        )
        return upsert_rows(
            cur,
            "core.fact_check",
            [
                "check_id",
                "order_id",
                "check_number",
                "tab_name",
                "status",
                "dining_option",
                "server_id",
                "paid_at",
                "closed_at",
                "amount",
                "tax",
                "tip",
                "gratuity",
                "guest_count",
                "business_date",
            ],
            rows,
            ["check_id"],
        )

    def insert_payments(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "payment_id",
                "check_id",
                "payment_type",
                "card_type",
                "house_account",
                "amount",
                "tip",
                "gratuity",
                "total",
                "swiped_amount",
                "keyed_amount",
                "refunded",
                "refund_date",
                "refund_amount",
                "refund_tip_amount",
                "status",
                "source",
                "email",
                "phone",
                "card_last4",
                "gift_card_last4",
                "gift_card_first5",
                "business_date",
            ],
        )
        return upsert_rows(
            cur,
            "core.fact_payment",
            [
                "payment_id",
                "check_id",
                "payment_type",
                "card_type",
                "house_account_number",
                "amount",
                "tip",
                "gratuity",
                "total",
                "swiped_amount",
                "keyed_amount",
                "refunded",
                "refunded_at",
                "refund_amount",
                "refund_tip_amount",
                "status",
                "source",
                "email",
                "phone",
                "card_last4",
                "gift_card_last4",
                "gift_card_first5",
                "business_date",
            ],
            rows,
            ["payment_id"],
        )

    def prepare_items(self, day_dir: Path, business_dt: date) -> pd.DataFrame:
        path = day_dir / "ItemSelectionDetails.csv"
        if not path.exists():
            return pd.DataFrame()
        items = read_csv_safe(path).rename(
            columns={
                "Item Selection Id": "item_selection_id",
                "Order Id": "order_id",
                "Order #": "order_number",
                "Check Id": "check_id",
                "Item Id": "menu_item_id",
                "Menu Item": "menu_item_name",
                "Menu Subgroup(s)": "menu_subgroup",
                "Menu Group": "menu_group",
                "Menu": "menu_name",
                "Sales Category": "sales_category",
                "Dining Option": "dining_option",
                "Tab Name": "tab_name",
                "Gross Price": "gross_price",
                "Discount": "discount",
                "Net Price": "net_price",
                "Qty": "qty",
                "Tax": "tax",
                "Void?": "voided",
                "Deferred": "deferred",
                "Tax Exempt": "tax_exempt",
                "Tax Inclusion Option": "tax_mode",
                "Dining Option Tax": "dining_option_tax",
            }
        )
        numeric_cols = [
            "item_selection_id",
            "order_id",
            "check_id",
            "gross_price",
            "discount",
            "net_price",
            "qty",
            "tax",
        ]
        for field in numeric_cols:
            items[field] = parse_numeric(items[field])
        for field in ["item_selection_id", "order_id", "check_id"]:
            items[field] = items[field].astype("Int64")
        items["voided"] = parse_bool(items["voided"])
        items["deferred"] = parse_bool(items["deferred"])
        items["tax_exempt"] = parse_bool(items["tax_exempt"])
        items["business_date"] = business_dt
        items["order_number"] = parse_numeric(items["order_number"]).astype("Int64")
        return items[
            [
                "item_selection_id",
                "order_id",
                "check_id",
                "menu_item_id",
                "menu_item_name",
                "sales_category",
                "gross_price",
                "discount",
                "net_price",
                "qty",
                "tax",
                "voided",
                "deferred",
                "tax_exempt",
                "tax_mode",
                "dining_option_tax",
                "business_date",
                "menu_group",
                "menu_subgroup",
                "menu_name",
                "order_number",
                "dining_option",
                "tab_name",
            ]
        ]

    def insert_order_items(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "item_selection_id",
                "order_id",
                "check_id",
                "menu_item_id",
                "menu_item_name",
                "sales_category",
                "gross_price",
                "discount",
                "net_price",
                "qty",
                "tax",
                "voided",
                "deferred",
                "tax_exempt",
                "tax_mode",
                "dining_option_tax",
                "business_date",
            ],
        )
        return upsert_rows(
            cur,
            "core.fact_order_item",
            [
                "item_selection_id",
                "order_id",
                "check_id",
                "menu_item_id",
                "menu_item_name",
                "sales_category",
                "gross_price",
                "discount",
                "net_price",
                "qty",
                "tax",
                "voided",
                "deferred",
                "tax_exempt",
                "tax_mode",
                "dining_option_tax",
                "business_date",
            ],
            rows,
            ["item_selection_id"],
        )

    def prepare_menu_dim(self, items: pd.DataFrame) -> pd.DataFrame:
        if items.empty:
            return pd.DataFrame()
        dim = (
            items[
                [
                    "menu_item_id",
                    "menu_item_name",
                    "menu_group",
                    "menu_subgroup",
                    "menu_name",
                    "sales_category",
                ]
            ]
            .drop_duplicates(subset=["menu_item_id"])
            .copy()
        )
        dim.rename(columns={"menu_item_name": "menu_item_name"}, inplace=True)
        return dim

    def upsert_menu_items(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "menu_item_id",
                "menu_item_name",
                "menu_group",
                "menu_subgroup",
                "menu_name",
                "sales_category",
            ],
        )
        return upsert_rows(
            cur,
            "core.dim_menu_item",
            [
                "menu_item_id",
                "menu_item_name",
                "menu_group",
                "menu_subgroup",
                "menu_name",
                "sales_category",
            ],
            rows,
            ["menu_item_id"],
        )

    def prepare_modifiers(
        self, day_dir: Path, business_dt: date, valid_item_ids: Optional[set]
    ) -> pd.DataFrame:
        path = day_dir / "ModifiersSelectionDetails.csv"
        if not path.exists():
            return pd.DataFrame()
        modifiers = read_csv_safe(path).rename(
            columns={
                "Item Selection Id": "item_selection_id",
                "Modifier Id": "modifier_selection_id",
                "Master Id": "modifier_id",
                "Modifier": "modifier_name",
                "Option Group ID": "option_group_id",
                "Option Group Name": "option_group_name",
                "Sales Category": "sales_category",
                "Gross Price": "gross_price",
                "Discount": "discount",
                "Net Price": "net_price",
                "Qty": "qty",
                "Void?": "voided",
            }
        )
        numeric_cols = [
            "modifier_selection_id",
            "modifier_id",
            "item_selection_id",
            "gross_price",
            "discount",
            "net_price",
            "qty",
        ]
        for field in numeric_cols:
            modifiers[field] = parse_numeric(modifiers[field])
        modifiers["voided"] = parse_bool(modifiers["voided"])
        modifiers["business_date"] = business_dt
        modifiers["price_delta"] = modifiers["net_price"]
        modifiers = modifiers.dropna(subset=["modifier_selection_id"])
        if valid_item_ids:
            modifiers = modifiers[modifiers["item_selection_id"].isin(valid_item_ids)]
        return modifiers[
            [
                "modifier_selection_id",
                "item_selection_id",
                "modifier_id",
                "modifier_name",
                "option_group_id",
                "option_group_name",
                "price_delta",
                "qty",
                "voided",
                "business_date",
                "sales_category",
            ]
        ]

    def insert_item_modifiers(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "modifier_selection_id",
                "item_selection_id",
                "modifier_id",
                "modifier_name",
                "option_group_name",
                "price_delta",
                "qty",
                "voided",
                "business_date",
            ],
        )
        return upsert_rows(
            cur,
            "core.fact_order_item_modifier",
            [
                "modifier_selection_id",
                "item_selection_id",
                "modifier_id",
                "modifier_name",
                "option_group_name",
                "price_delta",
                "qty",
                "voided",
                "business_date",
            ],
            rows,
            ["modifier_selection_id"],
        )

    def prepare_modifier_dim(self, modifiers: pd.DataFrame) -> pd.DataFrame:
        if modifiers.empty:
            return pd.DataFrame()
        dim = (
            modifiers[
                [
                    "modifier_id",
                    "modifier_name",
                    "option_group_id",
                    "option_group_name",
                    "sales_category",
                ]
            ]
            .drop_duplicates(subset=["modifier_id"])
            .dropna(subset=["modifier_id"])
        )
        return dim

    def upsert_modifiers(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "modifier_id",
                "option_group_id",
                "option_group_name",
                "modifier_name",
                "sales_category",
            ],
        )
        return upsert_rows(
            cur,
            "core.dim_modifier",
            [
                "modifier_id",
                "option_group_id",
                "option_group_name",
                "modifier_name",
                "sales_category",
            ],
            rows,
            ["modifier_id"],
        )

    def prepare_cash_entries(self, day_dir: Path, business_dt: date, location_id: str) -> pd.DataFrame:
        path = day_dir / "CashEntries.csv"
        if not path.exists():
            return pd.DataFrame()
        cash = read_csv_safe(path).rename(
            columns={
                "Entry Id": "entry_id",
                "Created Date": "entry_ts",
                "Action": "entry_type",
                "Amount": "amount",
                "Cash Drawer": "cash_drawer",
                "Comment": "comment",
            }
        )
        cash["entry_id"] = parse_numeric(cash["entry_id"]).astype("Int64")
        cash["entry_ts"] = parse_datetime_series(cash["entry_ts"])
        cash["amount"] = parse_numeric(cash["amount"])
        cash["location_id"] = location_id
        cash["business_date"] = business_dt
        return cash[["entry_id", "location_id", "cash_drawer", "entry_type", "entry_ts", "amount", "comment", "business_date"]]

    def insert_cash_entries(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "entry_id",
                "location_id",
                "cash_drawer",
                "entry_type",
                "entry_ts",
                "amount",
                "comment",
                "business_date",
            ],
        )
        return upsert_rows(
            cur,
            "core.fact_cash_drawer",
            [
                "cash_entry_id",
                "location_id",
                "drawer_name",
                "entry_type",
                "entry_ts",
                "amount",
                "note",
                "business_date",
            ],
            rows,
            ["cash_entry_id"],
        )

    def prepare_time_entries(self, day_dir: Path, business_dt: date, location_id: str) -> pd.DataFrame:
        path = day_dir / "TimeEntries.csv"
        if not path.exists():
            return pd.DataFrame()
        time_entries = read_csv_safe(path).rename(
            columns={
                "Id": "time_entry_id",
                "Employee Id": "employee_id",
                "Employee": "employee_name",
                "Job Title": "job_title",
                "In Date": "clock_in",
                "Out Date": "clock_out",
                "Payable Hours": "payable_hours",
                "Total Hours": "total_hours",
            }
        )
        time_entries["clock_in"] = parse_datetime_series(time_entries["clock_in"])
        time_entries["clock_out"] = parse_datetime_series(time_entries["clock_out"])
        time_entries["payable_hours"] = parse_numeric(time_entries["payable_hours"])
        time_entries["time_entry_id"] = parse_numeric(time_entries["time_entry_id"]).astype("Int64")
        time_entries["employee_id"] = parse_numeric(time_entries["employee_id"]).astype("Int64")
        time_entries["location_id"] = location_id
        time_entries["business_date"] = business_dt
        return time_entries[
            [
                "time_entry_id",
                "employee_id",
                "employee_name",
                "job_title",
                "clock_in",
                "clock_out",
                "payable_hours",
                "location_id",
                "business_date",
            ]
        ]

    def insert_time_entries(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(
            df,
            [
                "time_entry_id",
                "employee_id",
                "job_title",
                "clock_in",
                "clock_out",
                "payable_hours",
                "business_date",
                "location_id",
            ],
        )
        return upsert_rows(
            cur,
            "core.fact_time_entry",
            [
                "time_entry_id",
                "employee_id",
                "payroll_category",
                "clock_in",
                "clock_out",
                "hours_worked",
                "business_date",
                "location_id",
            ],
            rows,
            ["time_entry_id"],
        )

    def prepare_employees(self, time_entries: pd.DataFrame) -> pd.DataFrame:
        if time_entries.empty:
            return pd.DataFrame()
        dim = (
            time_entries[["employee_id", "employee_name", "job_title"]]
            .dropna(subset=["employee_id"])
            .drop_duplicates(subset=["employee_id"])
        )
        return dim

    def upsert_employees(self, cur, df: pd.DataFrame) -> int:
        rows = cleaned_rows(df, ["employee_id", "employee_name", "job_title"])
        return upsert_rows(
            cur,
            "core.dim_employee",
            ["employee_id", "display_name", "role"],
            rows,
            ["employee_id"],
        )

    def log_etl_run(self, cur, business_date: date, folder: str, counts: Dict[str, int]) -> None:
        cur.execute(
            """
            INSERT INTO meta.etl_run_log (business_date, source_folder, status, row_counts)
            VALUES (%s, %s, %s, %s)
            """,
            (business_date, folder, "loaded", json.dumps(counts)),
        )


def main() -> None:
    args = parse_args()
    conn = get_connection(args)
    try:
        loader = DataLoader(Path(args.root), conn)
        loader.run(args.location, args.date)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
