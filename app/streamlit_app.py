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
def load_market_trend(db_path: str) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            snapshot_week_id,
            snapshot_date,
            active_listings,
            new_listings,
            disappeared_listings,
            price_cut_listings,
            ROUND(100.0 * price_cut_rate, 1) AS price_cut_rate_pct,
            ROUND(100.0 * stale_60d_share, 1) AS stale_60d_pct,
            avg_price,
            avg_psf,
            distinct_projects,
            distinct_districts,
            distinct_agents,
            duplicate_cluster_count,
            duplicate_candidate_listings
        FROM market_week_metrics
        ORDER BY snapshot_week_id
        """,
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_etl_metadata(db_path: str) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(con, "SELECT * FROM etl_metadata")


@st.cache_data(ttl=300, show_spinner=False)
def load_project_metrics(db_path: str, week: str, min_listings: int) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            project_uid,
            project_name,
            project_group_type,
            postal_code,
            district_text,
            region_text,
            active_listings,
            pressure_score,
            new_listings,
            disappeared_listings,
            price_cut_listings,
            ROUND(100.0 * price_cut_rate, 1) AS price_cut_rate_pct,
            ROUND(100.0 * stale_60d_share, 1) AS stale_60d_pct,
            avg_price,
            avg_psf,
            distinct_agents,
            distinct_agencies,
            ROUND(100.0 * top_agent_share, 1) AS top_agent_pct,
            duplicate_cluster_count,
            duplicate_candidate_listings
        FROM project_week_metrics
        WHERE snapshot_week_id = ? AND active_listings >= ?
        ORDER BY active_listings DESC, pressure_score DESC, price_cut_listings DESC
        LIMIT 250
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
            pressure_score,
            active_listings,
            new_listings,
            disappeared_listings,
            price_cut_listings,
            ROUND(100.0 * price_cut_rate, 1) AS price_cut_rate_pct,
            ROUND(100.0 * stale_60d_share, 1) AS stale_60d_pct,
            avg_price,
            avg_psf,
            distinct_projects,
            distinct_agents,
            duplicate_candidate_listings
        FROM district_week_metrics
        WHERE snapshot_week_id = ?
        ORDER BY pressure_score DESC, active_listings DESC
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
            listing_type,
            project_name,
            district_text,
            bedrooms,
            floor_area_sqft,
            prior_price_value,
            price_value,
            price_change_abs,
            ROUND(100.0 * price_change_pct, 2) AS price_change_pct,
            price_per_area_value,
            prior_price_per_area_value,
            age_days,
            quality_flag
        FROM price_cut_events
        WHERE snapshot_week_id = ? AND quality_flag = 'ok'
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
            project_group_type,
            postal_code,
            district_text,
            active_listings,
            pressure_score,
            price_cut_listings
        FROM latest_project
        WHERE project_uid IS NOT NULL AND active_listings >= ?
        ORDER BY active_listings DESC, pressure_score DESC, project_name
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
            pressure_score,
            active_listings,
            new_listings,
            disappeared_listings,
            price_cut_listings,
            ROUND(100.0 * price_cut_rate, 1) AS price_cut_rate_pct,
            ROUND(100.0 * stale_60d_share, 1) AS stale_60d_pct,
            avg_price,
            avg_psf,
            distinct_agents,
            distinct_agencies,
            ROUND(100.0 * top_agent_share, 1) AS top_agent_pct,
            duplicate_candidate_listings
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
            listing_type,
            bedrooms,
            floor_area_sqft,
            prior_price_value,
            price_value,
            price_change_abs,
            ROUND(100.0 * price_change_pct, 2) AS price_change_pct,
            price_per_area_value,
            prior_price_per_area_value,
            age_days
        FROM price_cut_events
        WHERE project_uid = ? AND quality_flag = 'ok'
        ORDER BY snapshot_week_id DESC, price_change_pct ASC
        LIMIT 100
        """,
        (project_uid,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_duplicate_clusters(db_path: str, week: str) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            project_name,
            district_text,
            bedrooms,
            area_bucket_sqft,
            price_bucket,
            agent_id,
            agency_id,
            candidate_listing_count,
            min_price_value,
            max_price_value,
            avg_psf,
            listing_ids
        FROM duplicate_cluster_candidates
        WHERE snapshot_week_id = ?
        ORDER BY candidate_listing_count DESC, project_name
        LIMIT 150
        """,
        (week,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_agent_concentration(db_path: str, week: str, min_agent_listings: int) -> pd.DataFrame:
    con = get_connection(db_path)
    return read_frame(
        con,
        """
        SELECT
            project_name,
            district_text,
            agent_id,
            agent_license,
            agency_id,
            active_listings,
            price_cut_listings,
            ROUND(100.0 * stale_60d_share, 1) AS stale_60d_pct,
            avg_psf
        FROM agent_project_week_metrics
        WHERE snapshot_week_id = ? AND active_listings >= ?
        ORDER BY active_listings DESC, price_cut_listings DESC
        LIMIT 200
        """,
        (week, min_agent_listings),
    )


def metric_row(df: pd.DataFrame) -> None:
    total_active = int(df["active_listings"].sum()) if not df.empty else 0
    total_new = int(df["new_listings"].sum()) if not df.empty else 0
    total_disappeared = int(df["disappeared_listings"].sum()) if not df.empty else 0
    total_cuts = int(df["price_cut_listings"].sum()) if not df.empty else 0
    avg_score = float(df["pressure_score"].mean()) if not df.empty else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Active listings", f"{total_active:,}")
    c2.metric("New", f"{total_new:,}")
    c3.metric("Disappeared", f"{total_disappeared:,}")
    c4.metric("Price cuts", f"{total_cuts:,}")
    c5.metric("Avg pressure", f"{avg_score:.1f}")


def render_project_detail(db_path: str, min_listings: int) -> None:
    st.subheader("Project detail")
    options = load_project_options(db_path, min_listings)
    if options.empty:
        st.info("No projects match the current minimum-listing threshold.")
        return

    labels = {
        f"{row.project_name} — {row.district_text} "
        f"(score {row.pressure_score:.1f}, {int(row.active_listings):,} active)": row.project_uid
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
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Pressure score", f"{latest.pressure_score:.1f}")
    c2.metric("Latest active", f"{int(latest.active_listings):,}")
    c3.metric("Latest price cuts", f"{int(latest.price_cut_listings):,}")
    c4.metric("Latest avg PSF", f"{latest.avg_psf:,.0f}" if pd.notna(latest.avg_psf) else "n/a")
    c5.metric("Stale share", f"{latest.stale_60d_pct:.1f}%")

    chart_df = trend.set_index("snapshot_week_id")
    left, right = st.columns(2)
    with left:
        st.caption("Listing lifecycle")
        st.line_chart(
            chart_df[["active_listings", "new_listings", "disappeared_listings", "price_cut_listings"]]
        )
    with right:
        st.caption("Pressure, pricing, and concentration")
        st.line_chart(chart_df[["pressure_score", "avg_psf", "stale_60d_pct", "top_agent_pct"]])

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
        min_agent_listings = st.slider("Minimum agent-project listings", 2, 50, 5)
        st.caption(f"Analytics DB: `{db_path}`")

    market_trend = load_market_trend(str(db_path))
    metadata = load_etl_metadata(str(db_path))
    district = load_district_metrics(str(db_path), week)
    projects = load_project_metrics(str(db_path), week, min_listings)
    cuts = load_price_cuts(str(db_path), week)
    duplicates = load_duplicate_clusters(str(db_path), week)
    agents = load_agent_concentration(str(db_path), week, min_agent_listings)

    metric_row(district)

    tab_overview, tab_project, tab_events, tab_duplicates, tab_agents, tab_quality = st.tabs(
        [
            "Overview",
            "Project detail",
            "Price-cut events",
            "Duplicate candidates",
            "Agent concentration",
            "Data quality",
        ]
    )

    with tab_overview:
        st.subheader("Market trend")
        trend_chart = market_trend.set_index("snapshot_week_id")
        left, right = st.columns(2)
        with left:
            st.caption("Inventory lifecycle")
            st.line_chart(
                trend_chart[["active_listings", "new_listings", "disappeared_listings", "price_cut_listings"]]
            )
        with right:
            st.caption("Rates and pricing")
            st.line_chart(trend_chart[["price_cut_rate_pct", "stale_60d_pct", "avg_psf"]])

        with st.expander("Market weekly metrics", expanded=False):
            st.dataframe(market_trend, use_container_width=True, hide_index=True)

        st.subheader("District pressure")
        st.markdown(
            """
            Think of this as a **seller-stress / listing-oversupply signal** for the selected week, market, and property segment.
            A higher score means the district has more visible listing-side pressure than peer districts.

            - **Many active listings** → lots of competing units available.
            - **Many price cuts** → sellers/agents are adjusting downward.
            - **Many stale listings** → units are sitting unsold or unrented.
            - **Duplicate/shadow listings** → the same stock may be aggressively marketed.

            Formula: **45%** active-listing scale + **25%** price-cut rate + **20%** stale-60d share + **10%** duplicate candidates.
            """
        )
        st.dataframe(district, use_container_width=True, hide_index=True)

        st.subheader("Project / postal pressure ranking")
        st.markdown(
            """
            Same **seller-stress / listing-oversupply signal**, but at actual project level where available.
            If a row only has a marketing title rather than a true project, it falls back to postal-code grouping where possible.
            The table is sorted by active listings first, then pressure score.

            - **Many active listings** → many competing units in the project/postal area.
            - **Many price cuts** → sellers/agents are adjusting downward.
            - **Many stale listings** → units are sitting unsold or unrented.
            - **Duplicate/shadow listings** → similar stock may be repeatedly marketed.
            - **Top-agent concentration** → one agent may be pushing many similar units.

            Formula: **40%** active-listing scale + **25%** price-cut rate + **20%** stale-60d share + **10%** top-agent share + **5%** duplicate candidates.
            """
        )
        st.dataframe(projects, use_container_width=True, hide_index=True)

    with tab_project:
        render_project_detail(str(db_path), min_listings)

    with tab_events:
        st.subheader("Largest price-cut events")
        st.caption("Compact event view; raw listing payloads are intentionally excluded.")
        st.dataframe(cuts, use_container_width=True, hide_index=True)

    with tab_duplicates:
        st.subheader("Likely duplicate / shadow inventory clusters")
        st.caption(
            "Heuristic groups: same project, bedrooms, rounded area, rounded price, and same agent. "
            "Use as a triage signal, not a final duplicate label."
        )
        st.dataframe(duplicates, use_container_width=True, hide_index=True)

    with tab_agents:
        st.subheader("Agent concentration by project")
        st.caption("Agent IDs are retained; names are not retained in the lean snapshot schema.")
        st.dataframe(agents, use_container_width=True, hide_index=True)

    with tab_quality:
        st.subheader("Analytics build metadata")
        st.dataframe(metadata, use_container_width=True, hide_index=True)

        st.subheader("Snapshot coverage")
        st.caption("Use this to spot stale or unexpectedly thin snapshots after refreshes.")
        st.dataframe(market_trend, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
