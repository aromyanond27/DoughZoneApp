"""
Streamlit dashboard for Dough Zone daily exports.

Usage:
    streamlit run dashboard.py
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    import psycopg  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - Streamlit UI will handle missing dependency
    psycopg = None


DATA_ROOT = Path(__file__).resolve().parent
RAW_DATA_FOLDER = DATA_ROOT / "90984"  # default location folder name


def iter_location_dirs(root: Path) -> Iterable[Path]:
    """Yield top-level directories that look like location IDs."""
    for path in root.iterdir():
        if path.is_dir() and path.name.isdigit():
            yield path


def iter_business_days(location_dir: Path) -> List[str]:
    """Return available business dates (YYYYMMDD) sorted descending."""
    dates: List[str] = []
    for path in location_dir.iterdir():
        if path.is_dir() and path.name.isdigit() and len(path.name) == 8:
            dates.append(path.name)
    return sorted(dates, reverse=True)


@st.cache_data(show_spinner=False)
def load_csv(path: Path, parse_dates: Optional[List[str]] = None) -> pd.DataFrame:
    """Load a CSV if it exists, else return an empty DataFrame."""
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, parse_dates=parse_dates)
    except UnicodeDecodeError:
        # Some vendor exports include Windows-1252/Latin-1 characters; fall back rather than failing.
        return pd.read_csv(path, parse_dates=parse_dates, encoding="latin-1", on_bad_lines="skip")


def ensure_numeric(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    """Convert columns to numeric in-place."""
    for column in columns:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")


def format_currency(value: float) -> str:
    if pd.isna(value):
        value = 0.0
    return f"${value:,.0f}"


def summarize_orders(orders: pd.DataFrame) -> Dict[str, float]:
    ensure_numeric(orders, ["Amount", "Tax", "Discount Amount", "# of Guests"])
    orders["Opened"] = pd.to_datetime(orders.get("Opened"), errors="coerce")
    gross_sales = orders["Amount"].sum()
    net_sales = (orders["Amount"] - orders["Discount Amount"].fillna(0)).sum()
    guest_count = orders["# of Guests"].sum()
    order_count = len(orders)
    avg_check = net_sales / order_count if order_count else 0.0
    avg_per_guest = net_sales / guest_count if guest_count else 0.0
    return {
        "gross_sales": gross_sales,
        "net_sales": net_sales,
        "guest_count": guest_count,
        "order_count": order_count,
        "avg_check": avg_check,
        "avg_per_guest": avg_per_guest,
    }


def service_mix_chart(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame()
    ensure_numeric(orders, ["Amount"])
    grouped = (
        orders.groupby("Dining Options")["Amount"]
        .sum()
        .reset_index()
        .sort_values("Amount", ascending=False)
    )
    grouped.rename(columns={"Amount": "Sales"}, inplace=True)
    return grouped


def order_source_chart(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame()
    ensure_numeric(orders, ["Amount"])
    grouped = (
        orders.groupby("Order Source")["Amount"]
        .sum()
        .reset_index()
        .sort_values("Amount", ascending=False)
    )
    grouped.rename(columns={"Amount": "Sales"}, inplace=True)
    return grouped


def top_menu_items(items: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if items.empty:
        return pd.DataFrame()
    ensure_numeric(items, ["Net Price", "Qty"])
    grouped = (
        items.groupby("Menu Item")[["Net Price", "Qty"]]
        .sum()
        .reset_index()
        .sort_values("Net Price", ascending=False)
        .head(limit)
    )
    grouped.rename(columns={"Net Price": "Sales", "Qty": "Qty Sold"}, inplace=True)
    return grouped


def payment_mix(payments: pd.DataFrame) -> pd.DataFrame:
    if payments.empty:
        return pd.DataFrame()
    ensure_numeric(payments, ["Total", "Tip"])
    grouped = (
        payments.groupby("Type")[["Total", "Tip"]]
        .sum()
        .reset_index()
        .sort_values("Total", ascending=False)
    )
    grouped.rename(columns={"Total": "Payment"}, inplace=True)
    return grouped


def build_connection_params(
    dsn: str,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> Dict[str, str]:
    if dsn:
        return {"dsn": dsn}
    params = {
        "host": host,
        "port": str(port),
        "dbname": dbname,
        "user": user,
    }
    if password:
        params["password"] = password
    return params


def fetch_db_top_items(
    conn_params: Dict[str, str], location_id: str, lookback_days: int, limit: int = 5
) -> pd.DataFrame:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Run `pip install psycopg[binary]`.")

    query = """
        SELECT
            COALESCE(mi.menu_item_name, 'Unknown Item') AS menu_item_name,
            SUM(foi.qty) AS qty_sold,
            SUM(foi.net_price) AS sales
        FROM core.fact_order_item foi
        JOIN core.fact_order fo ON fo.order_id = foi.order_id
        LEFT JOIN core.dim_menu_item mi ON mi.menu_item_id = foi.menu_item_id
        WHERE fo.location_id = %(location_id)s
          AND foi.business_date >= CURRENT_DATE - (%(lookback)s * INTERVAL '1 day')
        GROUP BY COALESCE(mi.menu_item_name, 'Unknown Item')
        ORDER BY qty_sold DESC NULLS LAST, sales DESC NULLS LAST
        LIMIT %(limit)s;
    """
    connect_kwargs = {}
    if "dsn" in conn_params and conn_params["dsn"]:
        connect_kwargs = {"conninfo": conn_params["dsn"]}
    else:
        connect_kwargs = {
            "host": conn_params.get("host"),
            "port": conn_params.get("port"),
            "dbname": conn_params.get("dbname"),
            "user": conn_params.get("user"),
            "password": conn_params.get("password"),
        }

    with psycopg.connect(**connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                {
                    "location_id": location_id,
                    "lookback": max(lookback_days, 1),
                    "limit": limit,
                },
            )
            rows = cur.fetchall()

    return pd.DataFrame(rows, columns=["Menu Item", "Qty Sold", "Sales"])


def resolve_data_paths(
    location_dir: Path, business_date: str
) -> Dict[str, Path]:
    day_dir = location_dir / business_date
    return {
        "orders": day_dir / "OrderDetails.csv",
        "payments": day_dir / "PaymentDetails.csv",
        "items": day_dir / "ItemSelectionDetails.csv",
        "modifiers": day_dir / "ModifiersSelectionDetails.csv",
        "kitchen": day_dir / "KitchenTimings.csv",
        "checks": day_dir / "CheckDetails.csv",
    }


def main() -> None:
    st.set_page_config(page_title="Dough Zone Dashboard", layout="wide")
    st.title("Dough Zone Analytics Dashboard")
    st.caption("Visualize Revel exports by location and business date.")

    location_dirs = list(iter_location_dirs(DATA_ROOT))
    if not location_dirs:
        st.error("No location directories found. Add folders like '90984/20250209/'.")
        return

    location_options = {path.name: path for path in location_dirs}
    option_names = sorted(location_options.keys())
    default_location_name = (
        RAW_DATA_FOLDER.name if RAW_DATA_FOLDER.name in location_options else option_names[0]
    )
    default_index = option_names.index(default_location_name)
    selected_location_name = st.sidebar.selectbox(
        "Location", options=option_names, index=default_index
    )
    location_dir = location_options[selected_location_name]

    db_sidebar = st.sidebar.expander("PostgreSQL Insights", expanded=False)
    enable_db = False
    lookback_days = 7
    conn_params: Optional[Dict[str, str]] = None
    with db_sidebar:
        enable_db = st.checkbox("Enable database queries", value=False)
        if enable_db:
            dsn = st.text_input("DATABASE_URL", value=os.getenv("DATABASE_URL", ""))
            host = st.text_input("Host", value=os.getenv("PGHOST", "127.0.0.1"))
            port = st.number_input(
                "Port", value=int(os.getenv("PGPORT", "5432")), min_value=1, max_value=65535
            )
            dbname = st.text_input("Database", value=os.getenv("PGDATABASE", "postgres"))
            user = st.text_input("User", value=os.getenv("PGUSER", "postgres"))
            password = st.text_input("Password", value=os.getenv("PGPASSWORD", ""), type="password")
            lookback_days = st.number_input("Lookback days", min_value=1, max_value=365, value=7)
            conn_params = build_connection_params(dsn, host, port, dbname, user, password)
            if not dsn and (not host or not dbname or not user):
                conn_params = None

    business_days = iter_business_days(location_dir)
    if not business_days:
        st.error(f"No business-day folders found inside {location_dir}.")
        return

    selected_day = st.sidebar.selectbox(
        "Business Date (YYYYMMDD)", options=business_days, index=0
    )
    st.sidebar.markdown(
        f"Data folder: `{location_dir.name}/{selected_day}`"
    )

    st.info(
        f"Store {selected_location_name} • Business Date {selected_day}",
        icon="🏷️",
    )

    data_paths = resolve_data_paths(location_dir, selected_day)
    orders = load_csv(data_paths["orders"])
    payments = load_csv(data_paths["payments"])
    items = load_csv(data_paths["items"])

    if orders.empty:
        st.warning("No OrderDetails.csv found for the selected date.")
        return

    metrics = summarize_orders(orders)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Gross Sales", format_currency(metrics["gross_sales"]))
    col2.metric("Net Sales", format_currency(metrics["net_sales"]))
    col3.metric("Orders", int(metrics["order_count"]))
    col4.metric("Guests", int(metrics["guest_count"]))

    col5, col6 = st.columns(2)
    col5.metric("Average Check", format_currency(metrics["avg_check"]))
    col6.metric("Sales per Guest", format_currency(metrics["avg_per_guest"]))

    st.divider()

    mix_col1, mix_col2 = st.columns(2)
    service_data = service_mix_chart(orders)
    if not service_data.empty:
        mix_col1.subheader("Dining Option Mix")
        mix_col1.bar_chart(
            data=service_data.set_index("Dining Options")["Sales"],
            height=360,
        )

    source_data = order_source_chart(orders)
    if not source_data.empty:
        mix_col2.subheader("Order Source Mix")
        mix_col2.bar_chart(
            data=source_data.set_index("Order Source")["Sales"],
            height=360,
        )

    st.divider()
    st.subheader("Top Menu Items")
    top_items_df = top_menu_items(items)
    if top_items_df.empty:
        st.info("No ItemSelectionDetails.csv found or file is empty.")
    else:
        st.dataframe(
            top_items_df,
            hide_index=True,
            use_container_width=True,
        )

    st.divider()
    st.subheader("Payment Mix")
    payment_df = payment_mix(payments)
    if payment_df.empty:
        st.info("No PaymentDetails.csv found or file is empty.")
    else:
        st.dataframe(payment_df, hide_index=True, use_container_width=True)

    with st.expander("Raw Data Preview"):
        st.write("OrderDetails sample", orders.head())
        if not payments.empty:
            st.write("PaymentDetails sample", payments.head())
        if not items.empty:
            st.write("ItemSelectionDetails sample", items.head())

    if enable_db:
        st.divider()
        st.subheader(f"Last {lookback_days}-Day Menu Leader (PostgreSQL)")
        if psycopg is None:
            st.error("Install psycopg (`pip install psycopg[binary]`) to enable database queries.")
        elif conn_params is None:
            st.info("Provide connection settings in the sidebar to query PostgreSQL.")
        else:
            try:
                recent_top = fetch_db_top_items(conn_params, selected_location_name, lookback_days)
            except Exception as exc:  # pragma: no cover - runtime feedback
                st.error(f"Database query failed: {exc}")
            else:
                if recent_top.empty:
                    st.info("No menu items found for the selected lookback window.")
                else:
                    leader = recent_top.iloc[0]
                    st.metric(
                        label="Top Item",
                        value=leader["Menu Item"],
                        help=f"{int(leader['Qty Sold'])} sold in the last {lookback_days} days",
                    )
                    st.dataframe(
                        recent_top.assign(
                            **{
                                "Sales": recent_top["Sales"].map(lambda x: f"${x:,.0f}" if pd.notna(x) else "$0"),
                            }
                        ),
                        hide_index=True,
                        use_container_width=True,
                    )


if __name__ == "__main__":
    main()
