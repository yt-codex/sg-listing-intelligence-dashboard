from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

# Allow running via `streamlit run app/streamlit_app.py` before package install.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from sg_listing_intel.db import available_weeks, connect_readonly, latest_week, read_frame  # noqa: E402


def parse_db_arg() -> Path:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", type=Path, default=ROOT / "data/analytics/listing_intel.sqlite")
    args, _ = parser.parse_known_args()
    return args.db


@st.cache_resource(show_spinner=False)
def get_connection(db_path: str):
    return connect_readonly(Path(db_path))


@st.cache_data(ttl=300, show_spinner=False)
def load_project_metrics(db_path: str, week: str, min_listings: int) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            project_name,
            district_text,
            region_text,
            active_listings,
            new_listings,
            price_cut_listings,
            ROUND(100.0 * stale_60d_share, 1) AS stale_60d_pct,
            avg_price,
            avg_psf,
            distinct_agents,
            distinct_agencies,
            ROUND(100.0 * top_agent_share, 1) AS top_agent_pct
        FROM project_week_metrics
        WHERE snapshot_week_id = ? AND active_listings >= ?
        ORDER BY price_cut_listings DESC, active_listings DESC
        LIMIT 200
        """,
        (week, min_listings),
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_district_metrics(db_path: str, week: str) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            district_text,
            region_text,
            active_listings,
            new_listings,
            price_cut_listings,
            ROUND(100.0 * stale_60d_share, 1) AS stale_60d_pct,
            avg_price,
            avg_psf,
            distinct_projects,
            distinct_agents
        FROM district_week_metrics
        WHERE snapshot_week_id = ?
        ORDER BY active_listings DESC
        """,
        (week,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_price_cuts(db_path: str, week: str) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            project_name,
            district_text,
            bedrooms,
            floor_area_sqft,
            prior_price_value,
            price_value,
            price_change_abs,
            ROUND(100.0 * price_change_pct, 2) AS price_change_pct,
            price_per_area_value,
            age_days
        FROM price_cut_events
        WHERE snapshot_week_id = ?
        ORDER BY price_change_pct ASC
        LIMIT 100
        """,
        (week,),
    )


def metric_row(df: pd.DataFrame) -> None:
    total_active = int(df["active_listings"].sum()) if not df.empty else 0
    total_new = int(df["new_listings"].sum()) if not df.empty else 0
    total_cuts = int(df["price_cut_listings"].sum()) if not df.empty else 0
    avg_stale = float(df["stale_60d_pct"].mean()) if not df.empty else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active listings", f"{total_active:,}")
    c2.metric("New listings", f"{total_new:,}")
    c3.metric("Price cuts", f"{total_cuts:,}")
    c4.metric("Avg stale share", f"{avg_stale:.1f}%")


def main() -> None:
    st.set_page_config(page_title="SG Listing Intelligence", layout="wide")
    db_path = parse_db_arg()

    st.title("SG Listing Intelligence Dashboard")
    st.caption("MVP: compact analytics over weekly PropertyGuru listing snapshots")

    try:
        con = get_connection(str(db_path))
        weeks = available_weeks(con)
    except Exception as exc:  # pragma: no cover - visible app error
        st.error(f"Could not open analytics DB: {exc}")
        st.stop()

    default_week = latest_week(con)
    with st.sidebar:
        st.header("Filters")
        week = st.selectbox("Snapshot week", weeks, index=weeks.index(default_week))
        min_listings = st.slider("Minimum project listings", min_value=1, max_value=50, value=5)
        st.caption(f"Analytics DB: `{db_path}`")

    district = load_district_metrics(str(db_path), week)
    projects = load_project_metrics(str(db_path), week, min_listings)
    cuts = load_price_cuts(str(db_path), week)

    metric_row(district)

    st.subheader("District pressure")
    st.dataframe(district, use_container_width=True, hide_index=True)

    st.subheader("Project pressure ranking")
    st.caption("Sorted by price-cut count, then active listings. Limited to top 200 rows.")
    st.dataframe(projects, use_container_width=True, hide_index=True)

    st.subheader("Largest price-cut events")
    st.caption("Compact event view; raw listing payloads are intentionally excluded.")
    st.dataframe(cuts, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
