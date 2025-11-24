from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional

import altair as alt
import pandas as pd
import streamlit as st

# Paths
DATA_ROOT = Path(__file__).resolve().parent
DEFAULT_LOCATION = DATA_ROOT / "90984"


# --------- Data helpers ---------

def iter_location_dirs(root: Path) -> Iterable[Path]:
    for path in root.iterdir():
        if path.is_dir() and path.name.isdigit():
            yield path


def iter_business_days(location_dir: Path) -> List[str]:
    dates: List[str] = []
    for path in location_dir.iterdir():
        if path.is_dir() and path.name.isdigit() and len(path.name) == 8:
            dates.append(path.name)
    return sorted(dates, reverse=True)


def filter_dates_by_preset(days: List[str], preset: str) -> List[str]:
    """Return a subset of dates based on preset selection."""
    if not days:
        return []
    newest = pd.to_datetime(days[0], format="%Y%m%d", errors="coerce")
    if pd.isna(newest):
        return days[:1]
    if preset == "Past day":
        cutoff = newest - pd.Timedelta(days=1)
    elif preset == "Past week":
        cutoff = newest - pd.Timedelta(days=7)
    elif preset == "Past quarter":
        cutoff = newest - pd.Timedelta(days=90)
    elif preset == "Past year":
        cutoff = newest - pd.Timedelta(days=365)
    else:
        return days
    return [d for d in days if pd.to_datetime(d, format="%Y%m%d", errors="coerce") >= cutoff]


def load_csv_safe(path: Path, parse_dates: Optional[List[str]] = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, parse_dates=parse_dates)
    except UnicodeDecodeError:
        return pd.read_csv(path, parse_dates=parse_dates, encoding="latin-1", on_bad_lines="skip")


def load_day_data(location_dir: Path, dates: List[str]) -> Dict[str, pd.DataFrame]:
    orders_list: List[pd.DataFrame] = []
    payments_list: List[pd.DataFrame] = []
    items_list: List[pd.DataFrame] = []

    for day in dates:
        day_dir = location_dir / day
        orders_list.append(load_csv_safe(day_dir / "OrderDetails.csv"))
        payments_list.append(load_csv_safe(day_dir / "PaymentDetails.csv"))
        items_list.append(load_csv_safe(day_dir / "ItemSelectionDetails.csv"))

    return {
        "orders": pd.concat(orders_list, ignore_index=True) if orders_list else pd.DataFrame(),
        "payments": pd.concat(payments_list, ignore_index=True) if payments_list else pd.DataFrame(),
        "items": pd.concat(items_list, ignore_index=True) if items_list else pd.DataFrame(),
    }


def ensure_numeric(df: pd.DataFrame, cols: List[str]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def summarize_orders(orders: pd.DataFrame) -> Dict[str, float]:
    if orders.empty:
        return {k: 0.0 for k in ["gross", "net", "guests", "orders", "avg_check", "avg_guest"]}

    ensure_numeric(orders, ["Amount", "Tax", "Discount Amount", "# of Guests"])
    gross = orders["Amount"].sum()
    net = (orders["Amount"] - orders.get("Discount Amount", 0)).sum()
    guests = orders.get("# of Guests", pd.Series()).sum()
    count = len(orders)
    avg_check = net / count if count else 0.0
    avg_guest = net / guests if guests else 0.0
    return {
        "gross": gross,
        "net": net,
        "guests": guests,
        "orders": float(count),
        "avg_check": avg_check,
        "avg_guest": avg_guest,
    }


def service_mix(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty or "Dining Options" not in orders:
        return pd.DataFrame()
    ensure_numeric(orders, ["Amount"])
    return (
        orders.groupby("Dining Options")["Amount"].sum().reset_index().rename(columns={"Amount": "Sales"})
    )


def order_source_mix(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty or "Order Source" not in orders:
        return pd.DataFrame()
    ensure_numeric(orders, ["Amount"])
    return (
        orders.groupby("Order Source")["Amount"].sum().reset_index().rename(columns={"Amount": "Sales"})
    )


def top_items(items: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if items.empty or "Menu Item" not in items:
        return pd.DataFrame()
    ensure_numeric(items, ["Net Price", "Qty"])
    grouped = (
        items.groupby("Menu Item")["Net Price"].sum().reset_index().sort_values("Net Price", ascending=False)
    )
    grouped["Rank"] = range(1, len(grouped) + 1)
    return grouped.head(limit).rename(columns={"Net Price": "Sales"})


def payment_mix(payments: pd.DataFrame) -> pd.DataFrame:
    if payments.empty:
        return pd.DataFrame()
    col_total = "Total" if "Total" in payments else ("PaymentAmount" if "PaymentAmount" in payments else None)
    if col_total is None:
        return pd.DataFrame()
    ensure_numeric(payments, [col_total, "Tip"] if "Tip" in payments else [col_total])
    agg = payments.groupby("Type")[[col_total] + (["Tip"] if "Tip" in payments else [])].sum().reset_index()
    agg = agg.rename(columns={col_total: "Payment"})
    return agg


def revenue_trend(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame()
    if "Opened" in orders:
        orders = orders.copy()
        orders["Opened"] = pd.to_datetime(orders["Opened"], errors="coerce")
        orders["biz_date"] = orders["Opened"].dt.date
    else:
        return pd.DataFrame()
    ensure_numeric(orders, ["Amount"])
    return orders.groupby("biz_date")["Amount"].sum().reset_index().rename(columns={"Amount": "Revenue"})


def revenue_by_month(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty or "Opened" not in orders:
        return pd.DataFrame()
    orders = orders.copy()
    orders["Opened"] = pd.to_datetime(orders["Opened"], errors="coerce")
    ensure_numeric(orders, ["Amount"])
    grp = (
        orders.groupby([orders["Opened"].dt.year.rename("Year"), orders["Opened"].dt.month.rename("Month")])["Amount"]
        .sum()
        .reset_index()
        .rename(columns={"Amount": "Revenue"})
    )
    grp["Year-Month"] = pd.to_datetime(dict(year=grp["Year"], month=grp["Month"], day=1))
    return grp


# --------- AI helper (local, no API) ---------

def synthesize_insight(question: str, metrics: Dict[str, float], top_item: Optional[str], top_channel: Optional[str]) -> str:
    lines = [
        f"Revenue ${metrics['gross']:,.0f} across {int(metrics['orders'])} orders; net ${metrics['net']:,.0f}.",
        f"Avg check ${metrics['avg_check']:,.2f}; per guest ${metrics['avg_guest']:,.2f}.",
    ]
    if top_item:
        lines.append(f"Top item: {top_item}.")
    if top_channel:
        lines.append(f"Leading channel: {top_channel}.")
    lines.append(f"Question: {question}")
    lines.append("Actions: lean into best channels, feature top items, and coach low performers based on mix.")
    return "\n".join(lines)


# --------- UI renderers ---------

def render_performance(location_dir: Path, selected_dates: List[str]) -> None:
    data = load_day_data(location_dir, selected_dates)
    orders, payments, items = data["orders"], data["payments"], data["items"]
    metrics = summarize_orders(orders)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Gross Sales", f"${metrics['gross']:,.0f}")
    col2.metric("Net Sales", f"${metrics['net']:,.0f}")
    col3.metric("Orders", int(metrics["orders"]))
    col4.metric("Guests", int(metrics["guests"]))

    c1, c2 = st.columns(2)
    c1.metric("Average Check", f"${metrics['avg_check']:,.2f}")
    c2.metric("Sales per Guest", f"${metrics['avg_guest']:,.2f}")

    st.divider()

    rev_df = revenue_trend(orders)
    if not rev_df.empty:
        chart = (
            alt.Chart(rev_df)
            .mark_line(point=True)
            .encode(x=alt.X("biz_date:T", title="Date"), y=alt.Y("Revenue:Q", title="Revenue ($)"))
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No revenue trend available for the selected dates.")

    # Year-over-year monthly view
    yoy = revenue_by_month(orders)
    if not yoy.empty:
        st.subheader("Year-over-Year by Month")
        yoy_chart = (
            alt.Chart(yoy)
            .mark_line(point=True)
            .encode(
                x=alt.X("Month:O", title="Month"),
                y=alt.Y("Revenue:Q", title="Revenue ($)"),
                color=alt.Color("Year:O", title="Year"),
                tooltip=["Year", "Month", "Revenue"],
            )
            .properties(height=320)
        )
        st.altair_chart(yoy_chart, use_container_width=True)
    else:
        st.info("No YOY data available for the selected dates.")

    mix_col1, mix_col2 = st.columns(2)
    svc = service_mix(orders)
    if not svc.empty:
        mix_col1.subheader("Dining Option Mix")
        mix_col1.bar_chart(svc.set_index("Dining Options"), height=320)
    src = order_source_mix(orders)
    if not src.empty:
        mix_col2.subheader("Order Source Mix")
        mix_col2.bar_chart(src.set_index("Order Source"), height=320)

    st.divider()
    st.subheader("Top Menu Items")
    top_df = top_items(items)
    if top_df.empty:
        st.info("No ItemSelectionDetails.csv data for these dates.")
    else:
        display = top_df[["Rank", "Menu Item", "Sales"]] if "Menu Item" in top_df else top_df
        display["Sales"] = display["Sales"].map(lambda x: f"${x:,.0f}")
        st.dataframe(display, hide_index=True, use_container_width=True)

    st.divider()
    st.subheader("Payment Mix")
    pay_df = payment_mix(payments)
    if pay_df.empty:
        st.info("No PaymentDetails.csv data for these dates.")
    else:
        st.dataframe(pay_df, hide_index=True, use_container_width=True)


def render_guests(location_dir: Path, selected_dates: List[str]) -> None:
    data = load_day_data(location_dir, selected_dates)
    orders = data["orders"]
    if orders.empty:
        st.info("No orders found for the selected dates.")
        return

    orders = orders.copy()
    ensure_numeric(orders, ["Amount", "# of Guests"])
    if "Opened" in orders:
        orders["Opened"] = pd.to_datetime(orders["Opened"], errors="coerce")
    orders["Dining Options"] = orders.get("Dining Options", "")

    search = st.text_input("Filter by Server / Dining Option", "")
    filtered = orders
    if search:
        mask = pd.Series(False, index=orders.index)
        for col in ["Server", "Dining Options", "Order Source"]:
            if col in orders:
                mask |= orders[col].astype(str).str.contains(search, case=False, na=False)
        filtered = orders[mask]

    cols = [c for c in ["Check Id", "Server", "Dining Options", "Order Source", "Amount", "# of Guests", "Opened"] if c in filtered]
    if not cols:
        st.info("No recognizable guest columns found in orders.")
        return

    display = filtered[cols].copy()
    if "Amount" in display:
        display["Amount"] = display["Amount"].map(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    if "Opened" in display:
        display["Opened"] = pd.to_datetime(display["Opened"]).dt.strftime("%Y-%m-%d %H:%M")

    st.dataframe(display, hide_index=True, use_container_width=True)

    # Server performance charts
    if "Server" in filtered and "Amount" in filtered:
        st.subheader("Server Performance")
        perf_df = (
            filtered.groupby("Server")["Amount"]
            .agg(["sum", "count"])
            .reset_index()
            .rename(columns={"sum": "Revenue", "count": "Orders"})
            .sort_values("Revenue", ascending=False)
        )
        chart = (
            alt.Chart(perf_df)
            .mark_bar()
            .encode(x=alt.X("Server:N", sort="-y"), y=alt.Y("Revenue:Q"), tooltip=["Server", "Revenue", "Orders"])
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)
        perf_df["Revenue"] = perf_df["Revenue"].map(lambda x: f"${x:,.0f}")
        st.dataframe(perf_df, hide_index=True, use_container_width=True)


def render_ai(location_dir: Path, selected_dates: List[str]) -> None:
    data = load_day_data(location_dir, selected_dates)
    orders, payments, items = data["orders"], data["payments"], data["items"]
    metrics = summarize_orders(orders)

    st.write("Ask a strategic question. Responses are generated locally from your data (no external API).")
    question = st.text_input("Question", "What is performing best?")
    preset_col1, preset_col2, preset_col3 = st.columns(3)
    if preset_col1.button("Channel mix"):
        question = "Which channels drive revenue?"
    if preset_col2.button("Top items"):
        question = "What should we promote on the menu?"
    if preset_col3.button("Staffing"):
        question = "Which servers drive the most sales?"

    top_item = None
    ti = top_items(items)
    if not ti.empty:
        top_item = ti.iloc[0]["Menu Item"] if "Menu Item" in ti.columns else None
    top_channel = None
    src = order_source_mix(orders)
    if not src.empty:
        top_channel = src.iloc[0]["Order Source"]

    if st.button("Generate insight"):
        answer = synthesize_insight(question, metrics, top_item, top_channel)
        st.text_area("AI Insight", answer, height=180)


# --------- Main ---------

def main() -> None:
    st.set_page_config(page_title="Dough Zone Analytics", layout="wide")
    st.title("Dough Zone Analytics")
    st.caption("TSX-inspired UI using your local 90984 exports.")

    # Location selection
    locations = list(iter_location_dirs(DATA_ROOT))
    if not locations:
        st.error("No numeric location folders found next to the app.")
        return
    options = {loc.name: loc for loc in locations}
    default_loc = options.get(DEFAULT_LOCATION.name, locations[0])
    loc_name = st.sidebar.selectbox("Location", list(options.keys()), index=list(options.keys()).index(default_loc.name))
    location_dir = options[loc_name]

    # Date selection with presets
    days = iter_business_days(location_dir)
    if not days:
        st.error("No business-date folders found.")
        return
    preset = st.sidebar.selectbox("Date range", ["Most recent day", "Past day", "Past week", "Past quarter", "Past year", "All available"], index=0)
    if preset == "Most recent day":
        selected_dates = days[:1]
    elif preset == "All available":
        selected_dates = days
    else:
        selected_dates = filter_dates_by_preset(days, preset)
    st.sidebar.write(f"Selected {len(selected_dates)} day(s)")
    if not selected_dates:
        st.warning("No dates matched the selected range.")
        return

    tab_perf, tab_guests, tab_ai = st.tabs(["Performance", "Service", "AI Query"])

    with tab_perf:
        render_performance(location_dir, selected_dates)

    with tab_guests:
        render_guests(location_dir, selected_dates)

    with tab_ai:
        render_ai(location_dir, selected_dates)


if __name__ == "__main__":
    main()
