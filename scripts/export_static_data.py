from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any


def rows(con: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    con.row_factory = sqlite3.Row
    return [dict(row) for row in con.execute(sql, params).fetchall()]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")


STATIC_EXPORT_COLUMNS = {
    "weeks": {"snapshot_week_id", "snapshot_date"},
    "market": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "active_listings",
        "new_listings",
        "disappeared_listings",
        "price_cut_listings",
        "price_cut_rate",
        "stale_60d_share",
        "avg_psf",
        "distinct_projects",
        "distinct_districts",
        "distinct_agents",
        "duplicate_candidate_listings",
    },
    "regions": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "region_text",
        "active_listings",
        "new_listings",
        "disappeared_listings",
        "price_cut_listings",
        "price_cut_rate",
        "stale_60d_share",
        "avg_psf",
        "distinct_districts",
        "distinct_projects",
        "distinct_agents",
        "duplicate_candidate_listings",
        "pressure_score",
    },
    "districts": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "district_code",
        "district_text",
        "region_text",
        "active_listings",
        "new_listings",
        "disappeared_listings",
        "price_cut_listings",
        "price_cut_rate",
        "stale_60d_share",
        "avg_psf",
        "distinct_projects",
        "distinct_agents",
        "duplicate_candidate_listings",
        "pressure_score",
    },
    "projects": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "project_uid",
        "project_name",
        "project_group_type",
        "district_code",
        "district_text",
        "region_text",
        "postal_code",
        "active_listings",
        "new_listings",
        "disappeared_listings",
        "price_cut_listings",
        "price_cut_rate",
        "stale_60d_share",
        "avg_psf",
        "distinct_agents",
        "top_agent_share",
        "duplicate_candidate_listings",
        "pressure_score",
    },
    "projectTrends": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "project_uid",
        "active_listings",
        "new_listings",
        "disappeared_listings",
        "price_cut_listings",
        "price_cut_rate",
        "stale_60d_share",
        "avg_psf",
        "distinct_agents",
        "top_agent_share",
        "duplicate_candidate_listings",
        "pressure_score",
    },
    "priceCuts": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "project_uid",
        "project_name",
        "project_group_type",
        "postal_code",
        "district_text",
        "region_text",
        "bedrooms",
        "floor_area_sqft",
        "prior_price_value",
        "price_value",
        "price_change_pct",
        "age_days",
        "agent_id",
        "agency_id",
        "quality_flag",
    },
    "duplicates": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "project_uid",
        "project_name",
        "project_group_type",
        "postal_code",
        "district_text",
        "region_text",
        "bedrooms",
        "area_bucket_sqft",
        "price_bucket",
        "agent_id",
        "agency_id",
        "candidate_listing_count",
        "avg_psf",
    },
    "agents": {
        "snapshot_week_id",
        "snapshot_date",
        "listing_type",
        "property_segment",
        "project_uid",
        "project_name",
        "project_group_type",
        "postal_code",
        "district_text",
        "agent_id",
        "agent_license",
        "agency_id",
        "active_listings",
        "price_cut_listings",
        "avg_psf",
        "stale_60d_share",
    },
    "sourceWeekQuality": {
        "snapshot_week_id",
        "listing_type",
        "listing_count",
        "trailing_reference_count",
        "passes_completeness_gate",
        "week_is_eligible",
    },
}


def trim_rows(section: str, data_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keep = STATIC_EXPORT_COLUMNS[section]
    return [{key: row[key] for key in row.keys() if key in keep} for row in data_rows]


def export_static_data(db_path: Path, out_dir: Path, top_projects: int = 300) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Analytics DB not found: {db_path}")

    with sqlite3.connect(db_path) as con:
        weeks = rows(con, "SELECT * FROM snapshot_week ORDER BY snapshot_week_id")
        latest_week = weeks[-1]["snapshot_week_id"] if weeks else None

        metadata = rows(con, "SELECT * FROM etl_metadata")
        market = rows(con, "SELECT * FROM market_week_metrics ORDER BY snapshot_week_id, listing_type, property_segment")
        regions = rows(
            con,
            """
            SELECT *
            FROM region_week_metrics
            ORDER BY snapshot_week_id, pressure_score DESC, active_listings DESC
            """,
        )
        districts = rows(
            con,
            """
            SELECT *
            FROM district_week_metrics
            ORDER BY snapshot_week_id, pressure_score DESC, active_listings DESC
            """,
        )

        projects = rows(
            con,
            """
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY snapshot_week_id, listing_type, property_segment
                        ORDER BY active_listings DESC, pressure_score DESC
                    ) AS rn
                FROM project_week_metrics
                WHERE project_uid IS NOT NULL
            )
            SELECT *
            FROM ranked
            WHERE rn <= ?
            ORDER BY snapshot_week_id, rn
            """,
            (top_projects,),
        )

        project_ids = sorted({row["project_uid"] for row in projects if row.get("project_uid")})
        if project_ids:
            placeholders = ",".join("?" for _ in project_ids)
            project_trends = rows(
                con,
                f"""
                SELECT *
                FROM project_week_metrics
                WHERE project_uid IN ({placeholders})
                ORDER BY project_uid, snapshot_week_id
                """,
                tuple(project_ids),
            )
        else:
            project_trends = []

        price_cuts = rows(
            con,
            """
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY snapshot_week_id, listing_type, property_segment
                        ORDER BY price_change_pct ASC
                    ) AS rn
                FROM price_cut_events
                WHERE quality_flag = 'ok'
            )
            SELECT *
            FROM ranked
            WHERE rn <= 150
            ORDER BY snapshot_week_id, rn
            """,
        )

        duplicates = rows(
            con,
            """
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY snapshot_week_id, listing_type, property_segment
                        ORDER BY candidate_listing_count DESC, project_name
                    ) AS rn
                FROM duplicate_cluster_candidates
            )
            SELECT *
            FROM ranked
            WHERE rn <= 150
            ORDER BY snapshot_week_id, rn
            """,
        )

        agents = rows(
            con,
            """
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY snapshot_week_id, listing_type, property_segment
                        ORDER BY active_listings DESC, price_cut_listings DESC
                    ) AS rn
                FROM agent_project_week_metrics
                WHERE active_listings >= 3
            )
            SELECT *
            FROM ranked
            WHERE rn <= 200
            ORDER BY snapshot_week_id, rn
            """,
        )
        source_week_quality = rows(
            con,
            """
            SELECT *
            FROM source_snapshot_week_quality
            ORDER BY snapshot_week_id, listing_type
            """,
        )

    payload = {
        "metadata": metadata[0] if metadata else {},
        "latestWeek": latest_week,
        "weeks": trim_rows("weeks", weeks),
        "market": trim_rows("market", market),
        "regions": trim_rows("regions", regions),
        "districts": trim_rows("districts", districts),
        "projects": trim_rows("projects", projects),
        "projectTrends": trim_rows("projectTrends", project_trends),
        "priceCuts": trim_rows("priceCuts", price_cuts),
        "duplicates": trim_rows("duplicates", duplicates),
        "agents": trim_rows("agents", agents),
        "sourceWeekQuality": trim_rows("sourceWeekQuality", source_week_quality),
    }
    write_json(out_dir / "dashboard-data.json", payload)
    write_json(
        out_dir / "manifest.json",
        {
            "latestWeek": latest_week,
            "rowCounts": {
                "market": len(market),
                "regions": len(regions),
                "districts": len(districts),
                "projects": len(projects),
                "projectTrends": len(project_trends),
                "priceCuts": len(price_cuts),
                "duplicates": len(duplicates),
                "agents": len(agents),
            },
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export aggregate dashboard JSON for GitHub Pages")
    parser.add_argument("--db", type=Path, default=Path("data/analytics/listing_intel.sqlite"))
    parser.add_argument("--out", type=Path, default=Path("docs/assets"))
    parser.add_argument("--top-projects", type=int, default=300)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    export_static_data(args.db, args.out, args.top_projects)
    print(f"Exported static dashboard data to {args.out}")


if __name__ == "__main__":
    main()
