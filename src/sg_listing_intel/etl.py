from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


PANEL_SQL = """
CREATE TABLE listing_week_panel AS
WITH base AS (
    SELECT
        s.snapshot_week_id,
        s.snapshot_date,
        s.listing_id,
        s.listing_type,
        li.project_uid,
        COALESCE(NULLIF(TRIM(li.title), ''), 'Unknown project') AS project_name,
        li.full_address,
        li.district_code,
        COALESCE(NULLIF(TRIM(li.district_text), ''), 'Unknown district') AS district_text,
        li.region_code,
        COALESCE(NULLIF(TRIM(li.region_text), ''), 'Unknown region') AS region_text,
        li.bedrooms,
        li.bathrooms,
        COALESCE(li.floor_area_sqft, NULL) AS floor_area_sqft,
        s.price_value,
        s.price_per_area_value,
        s.agent_id,
        s.agency_id,
        s.agent_license,
        li.first_seen_at,
        li.last_seen_at,
        CAST(
            JULIANDAY(COALESCE(s.snapshot_date, s.scraped_at))
            - JULIANDAY(COALESCE(li.first_seen_at, s.scraped_at))
            AS INTEGER
        ) AS age_days,
        LAG(s.price_value) OVER (
            PARTITION BY s.listing_id
            ORDER BY s.snapshot_week_id
        ) AS prior_price_value
    FROM source.search_snapshot s
    JOIN source.listing_identity li
        ON li.listing_id = s.listing_id
    WHERE s.price_value IS NOT NULL
), panel AS (
    SELECT
        *,
        price_value - prior_price_value AS price_change_abs,
        CASE
            WHEN prior_price_value > 0
            THEN (price_value - prior_price_value) * 1.0 / prior_price_value
        END AS price_change_pct,
        CASE
            WHEN prior_price_value IS NOT NULL AND price_value < prior_price_value THEN 1
            ELSE 0
        END AS is_price_cut,
        CASE
            WHEN snapshot_week_id = MIN(snapshot_week_id) OVER (PARTITION BY listing_id) THEN 1
            ELSE 0
        END AS is_new_this_week,
        CASE WHEN age_days >= 60 THEN 1 ELSE 0 END AS is_stale_60d,
        CASE WHEN age_days >= 90 THEN 1 ELSE 0 END AS is_stale_90d
    FROM base
)
SELECT * FROM panel;
"""

PROJECT_SQL = """
CREATE TABLE project_week_metrics AS
WITH agent_counts AS (
    SELECT
        snapshot_week_id,
        project_uid,
        agent_id,
        COUNT(*) AS listings_by_agent
    FROM listing_week_panel
    WHERE agent_id IS NOT NULL
    GROUP BY snapshot_week_id, project_uid, agent_id
), top_agent AS (
    SELECT snapshot_week_id, project_uid, MAX(listings_by_agent) AS top_agent_listings
    FROM agent_counts
    GROUP BY snapshot_week_id, project_uid
)
SELECT
    p.snapshot_week_id,
    p.snapshot_date,
    p.project_uid,
    p.project_name,
    p.district_code,
    p.district_text,
    p.region_text,
    COUNT(*) AS active_listings,
    SUM(p.is_new_this_week) AS new_listings,
    SUM(p.is_price_cut) AS price_cut_listings,
    ROUND(AVG(p.is_stale_60d), 4) AS stale_60d_share,
    ROUND(AVG(p.is_stale_90d), 4) AS stale_90d_share,
    ROUND(AVG(p.price_value), 2) AS avg_price,
    ROUND(AVG(p.price_per_area_value), 2) AS avg_psf,
    COUNT(DISTINCT p.agent_id) AS distinct_agents,
    COUNT(DISTINCT p.agency_id) AS distinct_agencies,
    ROUND(COALESCE(t.top_agent_listings, 0) * 1.0 / COUNT(*), 4) AS top_agent_share
FROM listing_week_panel p
LEFT JOIN top_agent t
    ON t.snapshot_week_id = p.snapshot_week_id
    AND (t.project_uid = p.project_uid OR (t.project_uid IS NULL AND p.project_uid IS NULL))
GROUP BY
    p.snapshot_week_id,
    p.snapshot_date,
    p.project_uid,
    p.project_name,
    p.district_code,
    p.district_text,
    p.region_text;
"""

DISTRICT_SQL = """
CREATE TABLE district_week_metrics AS
SELECT
    snapshot_week_id,
    snapshot_date,
    district_code,
    district_text,
    region_text,
    COUNT(*) AS active_listings,
    SUM(is_new_this_week) AS new_listings,
    SUM(is_price_cut) AS price_cut_listings,
    ROUND(AVG(is_stale_60d), 4) AS stale_60d_share,
    ROUND(AVG(is_stale_90d), 4) AS stale_90d_share,
    ROUND(AVG(price_value), 2) AS avg_price,
    ROUND(AVG(price_per_area_value), 2) AS avg_psf,
    COUNT(DISTINCT project_uid) AS distinct_projects,
    COUNT(DISTINCT agent_id) AS distinct_agents,
    COUNT(DISTINCT agency_id) AS distinct_agencies
FROM listing_week_panel
GROUP BY snapshot_week_id, snapshot_date, district_code, district_text, region_text;
"""

PRICE_CUT_SQL = """
CREATE TABLE price_cut_events AS
SELECT
    snapshot_week_id,
    snapshot_date,
    listing_id,
    project_uid,
    project_name,
    district_text,
    region_text,
    bedrooms,
    floor_area_sqft,
    prior_price_value,
    price_value,
    price_change_abs,
    price_change_pct,
    price_per_area_value,
    age_days,
    agent_id,
    agency_id
FROM listing_week_panel
WHERE is_price_cut = 1;
"""

INDEX_SQL = [
    "CREATE INDEX idx_panel_week_project ON listing_week_panel(snapshot_week_id, project_uid)",
    "CREATE INDEX idx_panel_listing_week ON listing_week_panel(listing_id, snapshot_week_id)",
    "CREATE INDEX idx_project_week ON project_week_metrics(snapshot_week_id, active_listings)",
    "CREATE INDEX idx_district_week ON district_week_metrics(snapshot_week_id, active_listings)",
    "CREATE INDEX idx_price_cut_week ON price_cut_events(snapshot_week_id, price_change_pct)",
]


def build_analytics_db(source: Path, output: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(f"Source database not found: {source}")

    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    with sqlite3.connect(output) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("ATTACH DATABASE ? AS source", (str(source),))

        con.executescript(PANEL_SQL)
        con.executescript(PROJECT_SQL)
        con.executescript(DISTRICT_SQL)
        con.executescript(PRICE_CUT_SQL)
        for statement in INDEX_SQL:
            con.execute(statement)

        con.execute(
            """
            CREATE TABLE etl_metadata AS
            SELECT
                DATETIME('now') AS built_at_utc,
                ? AS source_path,
                COUNT(DISTINCT snapshot_week_id) AS snapshot_weeks,
                COUNT(*) AS listing_week_rows
            FROM listing_week_panel
            """,
            (str(source),),
        )
        con.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build compact listing intelligence analytics DB")
    parser.add_argument("--source", type=Path, required=True, help="Path to PropertyGuru source SQLite")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/analytics/listing_intel.sqlite"),
        help="Output analytics SQLite path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_analytics_db(args.source, args.output)
    print(f"Built analytics DB: {args.output}")


if __name__ == "__main__":
    main()
