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
            project_uid,
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


@st.cache_data(ttl=300, show_spinner=False)
def load_project_options(db_path: str, min_listings: int) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        WITH latest_project AS (
            SELECT *
            FROM project_week_metrics
            WHERE snapshot_week_id = (SELECT MAX(snapshot_week_id) FROM project_week_metrics)
        )
        SELECT
            project_uid,
            project_name,
            district_text,
            active_listings,
            price_cut_listings
        FROM latest_project
        WHERE project_uid IS NOT NULL AND active_listings >= ?
        ORDER BY active_listings DESC, project_name
        LIMIT 500
        """,
        (min_listings,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_project_trend(db_path: str, project_uid: str) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            snapshot_week_id,
            snapshot_date,
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
        WHERE project_uid = ?
        ORDER BY snapshot_week_id
        """,
        (project_uid,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_project_cut_events(db_path: str, project_uid: str) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            snapshot_week_id,
            bedrooms,
            floor_area_sqft,
            prior_price_value,
            price_value,
            price_change_abs,
            ROUND(100.0 * price_change_pct, 2) AS price_change_pct,
            price_per_area_value,
            age_days
        FROM price_cut_events
        WHERE project_uid = ?
        ORDER BY snapshot_week_id DESC, price_change_pct ASC
        LIMIT 100
        """,
        (project_uid,),
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


def render_project_detail(db_path: str, min_listings: int) -> None:
    st.subheader("Project detail")
    options = load_project_options(db_path, min_listings)
    if options.empty:
        st.info("No projects match the current minimum-listing threshold.")
        return

    labels = {
        f"{row.project_name} — {row.district_text} "
        f"({int(row.active_listings):,} active, {int(row.price_cut_listings):,} cuts)": row.project_uid
        for row in options.itertuples(index=False)
    }
    selected_label = st.selectbox("Project", list(labels.keys()))
    project_uid = labels[selected_label]

    trend = load_project_trend(db_path, project_uid)
    cuts = load_project_cut_events(db_path, project_uid)
    if trend.empty:
        st.info("No trend rows for this project.")
        return

    latest = trend.iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest active", f"{int(latest.active_listings):,}")
    c2.metric("Latest price cuts", f"{int(latest.price_cut_listings):,}")
    c3.metric("Latest avg PSF", f"{latest.avg_psf:,.0f}" if pd.notna(latest.avg_psf) else "n/a")
    c4.metric("Latest stale share", f"{latest.stale_60d_pct:.1f}%")

    chart_df = trend.set_index("snapshot_week_id")
    left, right = st.columns(2)
    with left:
        st.caption("Listing pressure")
        st.line_chart(chart_df[["active_listings", "new_listings", "price_cut_listings"]])
    with right:
        st.caption("Pricing and stale share")
        st.line_chart(chart_df[["avg_psf", "stale_60d_pct", "top_agent_pct"]])

    with st.expander("Project weekly metrics", expanded=False):
        st.dataframe(trend, use_container_width=True, hide_index=True)

    st.caption("Recent project price-cut events")
    st.dataframe(cuts, use_container_width=True, hide_index=True)


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

    tab_overview, tab_project, tab_events = st.tabs(
        ["Overview", "Project detail", "Price-cut events"]
    )

    with tab_overview:
        st.subheader("District pressure")
        st.dataframe(district, use_container_width=True, hide_index=True)

        st.subheader("Project pressure ranking")
        st.caption("Sorted by price-cut count, then active listings. Limited to top 200 rows.")
        st.dataframe(projects, use_container_width=True, hide_index=True)

    with tab_project:
        render_project_detail(str(db_path), min_listings)

    with tab_events:
        st.subheader("Largest price-cut events")
        st.caption("Compact event view; raw listing payloads are intentionally excluded.")
        st.dataframe(cuts, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
