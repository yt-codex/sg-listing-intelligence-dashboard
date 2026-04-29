from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


PANEL_SQL = """
CREATE TABLE listing_week_panel AS
WITH search_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.snapshot_week_id, s.listing_id
                ORDER BY COALESCE(s.scraped_at, s.snapshot_date) DESC, s.run_id DESC
            ) AS rn
        FROM source.search_snapshot s
        WHERE s.price_value IS NOT NULL
    )
    WHERE rn = 1
), identity_dedup AS (
    SELECT *
    FROM (
        SELECT
            li.*,
            ROW_NUMBER() OVER (
                PARTITION BY li.listing_id
                ORDER BY COALESCE(li.last_seen_at, li.first_seen_at) DESC
            ) AS rn
        FROM source.listing_identity li
    )
    WHERE rn = 1
), base AS (
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
            PARTITION BY s.listing_id, s.listing_type
            ORDER BY s.snapshot_week_id
        ) AS prior_price_value,
        LAG(s.price_per_area_value) OVER (
            PARTITION BY s.listing_id, s.listing_type
            ORDER BY s.snapshot_week_id
        ) AS prior_price_per_area_value
    FROM search_dedup s
    JOIN identity_dedup li
        ON li.listing_id = s.listing_id
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
            WHEN snapshot_week_id = MIN(snapshot_week_id) OVER (PARTITION BY listing_id, listing_type) THEN 1
            ELSE 0
        END AS is_new_this_week,
        CASE WHEN age_days >= 60 THEN 1 ELSE 0 END AS is_stale_60d,
        CASE WHEN age_days >= 90 THEN 1 ELSE 0 END AS is_stale_90d
    FROM base
)
SELECT * FROM panel;
"""

WEEK_SQL = """
CREATE TABLE snapshot_week AS
SELECT
    snapshot_week_id,
    MIN(snapshot_date) AS snapshot_date,
    ROW_NUMBER() OVER (ORDER BY snapshot_week_id) AS week_index,
    LEAD(snapshot_week_id) OVER (ORDER BY snapshot_week_id) AS next_snapshot_week_id
FROM listing_week_panel
GROUP BY snapshot_week_id;
"""

DISAPPEARED_SQL = """
CREATE TABLE disappeared_listing_events AS
SELECT
    next_week.snapshot_week_id AS snapshot_week_id,
    next_week.snapshot_date AS snapshot_date,
    prev.listing_id,
    prev.listing_type,
    prev.project_uid,
    prev.project_name,
    prev.district_code,
    prev.district_text,
    prev.region_text,
    prev.bedrooms,
    prev.floor_area_sqft,
    prev.price_value AS last_price_value,
    prev.price_per_area_value AS last_price_per_area_value,
    prev.agent_id,
    prev.agency_id,
    prev.age_days AS last_observed_age_days
FROM listing_week_panel prev
JOIN snapshot_week this_week
    ON this_week.snapshot_week_id = prev.snapshot_week_id
JOIN snapshot_week next_week
    ON next_week.snapshot_week_id = this_week.next_snapshot_week_id
LEFT JOIN listing_week_panel current
    ON current.snapshot_week_id = next_week.snapshot_week_id
    AND current.listing_id = prev.listing_id
    AND current.listing_type = prev.listing_type
WHERE current.listing_id IS NULL;
"""

DUPLICATE_SQL = """
CREATE TABLE duplicate_cluster_candidates AS
WITH keyed AS (
    SELECT
        snapshot_week_id,
        snapshot_date,
        project_uid,
        project_name,
        district_text,
        region_text,
        bedrooms,
        CAST(ROUND(COALESCE(floor_area_sqft, 0) / 50.0) * 50 AS INTEGER) AS area_bucket_sqft,
        CAST(ROUND(COALESCE(price_value, 0) / 50000.0) * 50000 AS INTEGER) AS price_bucket,
        agent_id,
        agency_id,
        listing_id,
        price_value,
        price_per_area_value
    FROM listing_week_panel
    WHERE project_uid IS NOT NULL
        AND bedrooms IS NOT NULL
        AND floor_area_sqft IS NOT NULL
        AND price_value IS NOT NULL
)
SELECT
    snapshot_week_id,
    snapshot_date,
    project_uid,
    project_name,
    district_text,
    region_text,
    bedrooms,
    area_bucket_sqft,
    price_bucket,
    agent_id,
    agency_id,
    COUNT(*) AS candidate_listing_count,
    MIN(price_value) AS min_price_value,
    MAX(price_value) AS max_price_value,
    ROUND(AVG(price_per_area_value), 2) AS avg_psf,
    GROUP_CONCAT(listing_id) AS listing_ids
FROM keyed
GROUP BY
    snapshot_week_id,
    project_uid,
    bedrooms,
    area_bucket_sqft,
    price_bucket,
    agent_id
HAVING COUNT(*) >= 2;
"""

PROJECT_SQL = """
CREATE TABLE project_week_metrics AS
WITH agent_counts AS (
    SELECT
        snapshot_week_id,
        project_uid,
        project_name,
        district_code,
        district_text,
        region_text,
        agent_id,
        COUNT(*) AS listings_by_agent
    FROM listing_week_panel
    WHERE agent_id IS NOT NULL
    GROUP BY snapshot_week_id, project_uid, project_name, district_code, district_text, region_text, agent_id
), top_agent AS (
    SELECT
        snapshot_week_id,
        project_uid,
        project_name,
        district_code,
        district_text,
        region_text,
        MAX(listings_by_agent) AS top_agent_listings
    FROM agent_counts
    GROUP BY snapshot_week_id, project_uid, project_name, district_code, district_text, region_text
), disappeared AS (
    SELECT
        snapshot_week_id,
        project_uid,
        project_name,
        district_code,
        district_text,
        region_text,
        COUNT(*) AS disappeared_listings
    FROM disappeared_listing_events
    GROUP BY snapshot_week_id, project_uid, project_name, district_code, district_text, region_text
), duplicate AS (
    SELECT
        snapshot_week_id,
        project_uid,
        COUNT(*) AS duplicate_cluster_count,
        SUM(candidate_listing_count) AS duplicate_candidate_listings
    FROM duplicate_cluster_candidates
    GROUP BY snapshot_week_id, project_uid
), base AS (
    SELECT
        p.snapshot_week_id,
        MIN(p.snapshot_date) AS snapshot_date,
        p.project_uid,
        p.project_name,
        p.district_code,
        p.district_text,
        p.region_text,
        COUNT(*) AS active_listings,
        SUM(p.is_new_this_week) AS new_listings,
        COALESCE(d.disappeared_listings, 0) AS disappeared_listings,
        SUM(p.is_price_cut) AS price_cut_listings,
        ROUND(SUM(p.is_price_cut) * 1.0 / COUNT(*), 4) AS price_cut_rate,
        ROUND(AVG(p.is_stale_60d), 4) AS stale_60d_share,
        ROUND(AVG(p.is_stale_90d), 4) AS stale_90d_share,
        ROUND(AVG(p.price_value), 2) AS avg_price,
        ROUND(AVG(p.price_per_area_value), 2) AS avg_psf,
        ROUND(
            CASE
                WHEN COUNT(p.price_per_area_value) > 1
                THEN AVG(p.price_per_area_value * p.price_per_area_value)
                     - AVG(p.price_per_area_value) * AVG(p.price_per_area_value)
            END,
            2
        ) AS psf_variance,
        COUNT(DISTINCT p.agent_id) AS distinct_agents,
        COUNT(DISTINCT p.agency_id) AS distinct_agencies,
        ROUND(COALESCE(t.top_agent_listings, 0) * 1.0 / COUNT(*), 4) AS top_agent_share,
        COALESCE(du.duplicate_cluster_count, 0) AS duplicate_cluster_count,
        COALESCE(du.duplicate_candidate_listings, 0) AS duplicate_candidate_listings
    FROM listing_week_panel p
    LEFT JOIN top_agent t
        ON t.snapshot_week_id = p.snapshot_week_id
        AND (t.project_uid = p.project_uid OR (t.project_uid IS NULL AND p.project_uid IS NULL))
        AND t.project_name = p.project_name
        AND (t.district_code = p.district_code OR (t.district_code IS NULL AND p.district_code IS NULL))
        AND t.district_text = p.district_text
        AND t.region_text = p.region_text
    LEFT JOIN disappeared d
        ON d.snapshot_week_id = p.snapshot_week_id
        AND (d.project_uid = p.project_uid OR (d.project_uid IS NULL AND p.project_uid IS NULL))
        AND d.project_name = p.project_name
        AND (d.district_code = p.district_code OR (d.district_code IS NULL AND p.district_code IS NULL))
        AND d.district_text = p.district_text
        AND d.region_text = p.region_text
    LEFT JOIN duplicate du
        ON du.snapshot_week_id = p.snapshot_week_id
        AND du.project_uid = p.project_uid
    GROUP BY
        p.snapshot_week_id,
        p.project_uid,
        p.project_name,
        p.district_code,
        p.district_text,
        p.region_text
), week_max AS (
    SELECT
        snapshot_week_id,
        MAX(active_listings) AS max_active_listings,
        MAX(price_cut_rate) AS max_price_cut_rate,
        MAX(duplicate_candidate_listings) AS max_duplicate_candidate_listings
    FROM base
    GROUP BY snapshot_week_id
)
SELECT
    b.*,
    ROUND(
        40.0 * b.active_listings / NULLIF(w.max_active_listings, 0)
        + 25.0 * b.price_cut_rate / NULLIF(w.max_price_cut_rate, 0)
        + 20.0 * b.stale_60d_share
        + 10.0 * b.top_agent_share
        + 5.0 * b.duplicate_candidate_listings / NULLIF(w.max_duplicate_candidate_listings, 0),
        2
    ) AS pressure_score
FROM base b
JOIN week_max w ON w.snapshot_week_id = b.snapshot_week_id;
"""

DISTRICT_SQL = """
CREATE TABLE district_week_metrics AS
WITH disappeared AS (
    SELECT snapshot_week_id, district_code, COUNT(*) AS disappeared_listings
    FROM disappeared_listing_events
    GROUP BY snapshot_week_id, district_code
), duplicate AS (
    SELECT snapshot_week_id, district_text, SUM(candidate_listing_count) AS duplicate_candidate_listings
    FROM duplicate_cluster_candidates
    GROUP BY snapshot_week_id, district_text
), base AS (
    SELECT
        p.snapshot_week_id,
        MIN(p.snapshot_date) AS snapshot_date,
        p.district_code,
        p.district_text,
        p.region_text,
        COUNT(*) AS active_listings,
        SUM(p.is_new_this_week) AS new_listings,
        COALESCE(d.disappeared_listings, 0) AS disappeared_listings,
        SUM(p.is_price_cut) AS price_cut_listings,
        ROUND(SUM(p.is_price_cut) * 1.0 / COUNT(*), 4) AS price_cut_rate,
        ROUND(AVG(p.is_stale_60d), 4) AS stale_60d_share,
        ROUND(AVG(p.is_stale_90d), 4) AS stale_90d_share,
        ROUND(AVG(p.price_value), 2) AS avg_price,
        ROUND(AVG(p.price_per_area_value), 2) AS avg_psf,
        COUNT(DISTINCT p.project_uid) AS distinct_projects,
        COUNT(DISTINCT p.agent_id) AS distinct_agents,
        COUNT(DISTINCT p.agency_id) AS distinct_agencies,
        COALESCE(du.duplicate_candidate_listings, 0) AS duplicate_candidate_listings
    FROM listing_week_panel p
    LEFT JOIN disappeared d
        ON d.snapshot_week_id = p.snapshot_week_id
        AND (d.district_code = p.district_code OR (d.district_code IS NULL AND p.district_code IS NULL))
    LEFT JOIN duplicate du
        ON du.snapshot_week_id = p.snapshot_week_id
        AND du.district_text = p.district_text
    GROUP BY p.snapshot_week_id, p.district_code, p.district_text, p.region_text
), week_max AS (
    SELECT
        snapshot_week_id,
        MAX(active_listings) AS max_active_listings,
        MAX(price_cut_rate) AS max_price_cut_rate,
        MAX(duplicate_candidate_listings) AS max_duplicate_candidate_listings
    FROM base
    GROUP BY snapshot_week_id
)
SELECT
    b.*,
    ROUND(
        45.0 * b.active_listings / NULLIF(w.max_active_listings, 0)
        + 25.0 * b.price_cut_rate / NULLIF(w.max_price_cut_rate, 0)
        + 20.0 * b.stale_60d_share
        + 10.0 * b.duplicate_candidate_listings / NULLIF(w.max_duplicate_candidate_listings, 0),
        2
    ) AS pressure_score
FROM base b
JOIN week_max w ON w.snapshot_week_id = b.snapshot_week_id;
"""

PRICE_CUT_SQL = """
CREATE TABLE price_cut_events AS
SELECT
    snapshot_week_id,
    snapshot_date,
    listing_id,
    listing_type,
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
    prior_price_per_area_value,
    age_days,
    agent_id,
    agency_id,
    CASE
        WHEN price_change_pct <= -0.75
            AND prior_price_value > 0
            AND price_value > 0
            AND prior_price_value * 1.0 / price_value BETWEEN 8.5 AND 11.5
        THEN 'likely_extra_digit_or_parser_correction'
        WHEN price_change_pct <= -0.75
            AND prior_price_per_area_value > 0
            AND price_per_area_value > 0
            AND prior_price_per_area_value * 1.0 / price_per_area_value BETWEEN 8.5 AND 11.5
        THEN 'likely_extra_digit_or_parser_correction'
        WHEN listing_type = 'RENT'
            AND prior_price_value >= 30000
            AND price_value < 30000
        THEN 'likely_parser_outlier_correction'
        WHEN listing_type = 'RENT'
            AND COALESCE(bedrooms, 0) = 0
            AND COALESCE(floor_area_sqft, 0) <= 350
            AND price_change_pct <= -0.35
        THEN 'possible_room_rent_basis_change'
        ELSE 'ok'
    END AS quality_flag
FROM listing_week_panel
WHERE is_price_cut = 1;
"""

MARKET_WEEK_SQL = """
CREATE TABLE market_week_metrics AS
SELECT
    p.snapshot_week_id,
    MIN(p.snapshot_date) AS snapshot_date,
    COUNT(*) AS active_listings,
    SUM(p.is_new_this_week) AS new_listings,
    COALESCE(d.disappeared_listings, 0) AS disappeared_listings,
    SUM(p.is_price_cut) AS price_cut_listings,
    ROUND(SUM(p.is_price_cut) * 1.0 / COUNT(*), 4) AS price_cut_rate,
    ROUND(AVG(p.is_stale_60d), 4) AS stale_60d_share,
    ROUND(AVG(p.is_stale_90d), 4) AS stale_90d_share,
    ROUND(AVG(p.price_value), 2) AS avg_price,
    ROUND(AVG(p.price_per_area_value), 2) AS avg_psf,
    COUNT(DISTINCT p.project_uid) AS distinct_projects,
    COUNT(DISTINCT p.district_text) AS distinct_districts,
    COUNT(DISTINCT p.agent_id) AS distinct_agents,
    COUNT(DISTINCT p.agency_id) AS distinct_agencies,
    COALESCE(du.duplicate_cluster_count, 0) AS duplicate_cluster_count,
    COALESCE(du.duplicate_candidate_listings, 0) AS duplicate_candidate_listings
FROM listing_week_panel p
LEFT JOIN (
    SELECT snapshot_week_id, COUNT(*) AS disappeared_listings
    FROM disappeared_listing_events
    GROUP BY snapshot_week_id
) d ON d.snapshot_week_id = p.snapshot_week_id
LEFT JOIN (
    SELECT
        snapshot_week_id,
        COUNT(*) AS duplicate_cluster_count,
        SUM(candidate_listing_count) AS duplicate_candidate_listings
    FROM duplicate_cluster_candidates
    GROUP BY snapshot_week_id
) du ON du.snapshot_week_id = p.snapshot_week_id
GROUP BY p.snapshot_week_id;
"""

AGENT_PROJECT_SQL = """
CREATE TABLE agent_project_week_metrics AS
SELECT
    snapshot_week_id,
    MIN(snapshot_date) AS snapshot_date,
    project_uid,
    project_name,
    district_text,
    agent_id,
    agent_license,
    agency_id,
    COUNT(*) AS active_listings,
    SUM(is_price_cut) AS price_cut_listings,
    ROUND(AVG(price_per_area_value), 2) AS avg_psf,
    ROUND(AVG(is_stale_60d), 4) AS stale_60d_share
FROM listing_week_panel
WHERE agent_id IS NOT NULL
GROUP BY
    snapshot_week_id,
    project_uid,
    project_name,
    district_text,
    agent_id,
    agent_license,
    agency_id;
"""

INDEX_SQL = [
    "CREATE INDEX idx_panel_week_project ON listing_week_panel(snapshot_week_id, project_uid)",
    "CREATE INDEX idx_panel_listing_week ON listing_week_panel(listing_id, snapshot_week_id)",
    "CREATE INDEX idx_project_week ON project_week_metrics(snapshot_week_id, pressure_score)",
    "CREATE INDEX idx_district_week ON district_week_metrics(snapshot_week_id, pressure_score)",
    "CREATE INDEX idx_price_cut_week ON price_cut_events(snapshot_week_id, price_change_pct)",
    "CREATE INDEX idx_disappeared_week_project ON disappeared_listing_events(snapshot_week_id, project_uid)",
    "CREATE INDEX idx_duplicate_week_project ON duplicate_cluster_candidates(snapshot_week_id, project_uid)",
    "CREATE INDEX idx_agent_project_week ON agent_project_week_metrics(snapshot_week_id, project_uid)",
    "CREATE INDEX idx_market_week ON market_week_metrics(snapshot_week_id)",
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
        con.executescript(WEEK_SQL)
        con.executescript(DISAPPEARED_SQL)
        con.executescript(DUPLICATE_SQL)
        con.executescript(PROJECT_SQL)
        con.executescript(DISTRICT_SQL)
        con.executescript(PRICE_CUT_SQL)
        con.executescript(MARKET_WEEK_SQL)
        con.executescript(AGENT_PROJECT_SQL)
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
