from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.export_static_data import export_static_data
from sg_listing_intel.etl import build_analytics_db


def make_source_db(path: Path) -> None:
    with sqlite3.connect(path) as con:
        con.executescript(
            """
            CREATE TABLE search_snapshot (
                run_id TEXT,
                snapshot_week_id TEXT,
                snapshot_date TEXT,
                listing_id INTEGER,
                listing_type TEXT,
                status_code TEXT,
                price_value NUMERIC,
                price_text TEXT,
                currency TEXT,
                price_per_area_value NUMERIC,
                price_per_area_text TEXT,
                posted_at_text TEXT,
                posted_at_unix INTEGER,
                availability_info TEXT,
                mrt_text TEXT,
                agent_id INTEGER,
                agent_license TEXT,
                agency_id INTEGER,
                photo_count INTEGER,
                floorplan_count INTEGER,
                video_count INTEGER,
                badge_json TEXT,
                product_flags_json TEXT,
                is_verified INTEGER,
                is_official_listing INTEGER,
                scraped_at TEXT
            );
            CREATE TABLE listing_identity (
                listing_id INTEGER,
                first_seen_snapshot_week_id TEXT,
                first_seen_run_id TEXT,
                first_seen_at TEXT,
                last_seen_snapshot_week_id TEXT,
                last_seen_run_id TEXT,
                last_seen_at TEXT,
                latest_listing_type TEXT,
                latest_search_url TEXT,
                latest_property_type_code TEXT,
                latest_property_type_text TEXT,
                detail_eligible INTEGER,
                has_detail_snapshot INTEGER,
                last_detail_scraped_at TEXT,
                project_uid TEXT,
                title TEXT,
                full_address TEXT,
                district_code TEXT,
                district_text TEXT,
                region_code TEXT,
                region_text TEXT,
                bedrooms INTEGER,
                bathrooms INTEGER,
                floor_area_sqft NUMERIC,
                street_number TEXT,
                street_name TEXT,
                postal_code TEXT,
                latitude NUMERIC,
                longitude NUMERIC,
                masked_location INTEGER,
                land_area_sqft NUMERIC,
                furnishing_code TEXT,
                furnishing_text TEXT,
                floor_level_code TEXT,
                floor_level_text TEXT
            );
            CREATE TABLE detail_project (
                project_uid TEXT,
                project_uid_type TEXT,
                source_project_id INTEGER,
                project_name TEXT
            );
            """
        )
        con.executemany(
            """
            INSERT INTO detail_project (project_uid, project_uid_type, source_project_id, project_name)
            VALUES (?, ?, ?, ?)
            """,
            [
                ("p1", "project_id", 101, "Alpha Condo"),
                ("h3", "heuristic", None, "Marketing near school"),
                ("h4", "heuristic", None, "Cheap freehold buy"),
            ],
        )
        con.executemany(
            """
            INSERT INTO listing_identity (
                listing_id, first_seen_at, last_seen_at, project_uid, title,
                district_code, district_text, region_code, region_text,
                bedrooms, bathrooms, floor_area_sqft, postal_code
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, "2026-01-01", "2026-04-01", "p1", "Alpha Condo", "D09", "Orchard", "CCR", "CCR", 2, 2, 800, "111111"),
                (2, "2026-03-01", "2026-04-01", "p1", "Alpha Condo", "D09", "Orchard", "CCR", "CCR", 3, 2, 1000, "111112"),
                (3, "2026-03-01", "2026-04-01", "h3", "Marketing near school", "D15", "East Coast", "RCR", "RCR", 2, 1, 700, "456789"),
                (4, "2026-03-01", "2026-04-01", "h4", "Cheap freehold buy", "D15", "East Coast", "RCR", "RCR", 2, 1, 710, "456789"),
            ],
        )
        con.executemany(
            """
            INSERT INTO search_snapshot (
                snapshot_week_id, snapshot_date, listing_id, listing_type,
                price_value, price_per_area_value, agent_id, agent_license, agency_id, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("w1", "2026-04-01", 1, "sale", 1_000_000, 1250, 10, "L1", 100, "2026-04-01"),
                ("w2", "2026-04-08", 1, "sale", 950_000, 1187.5, 10, "L1", 100, "2026-04-08"),
                ("w2", "2026-04-08", 2, "sale", 1_500_000, 1500, 11, "L2", 101, "2026-04-08"),
                ("w2", "2026-04-08", 3, "sale", 900_000, 1285.7, 12, "L3", 102, "2026-04-08"),
                ("w2", "2026-04-08", 4, "sale", 910_000, 1281.7, 13, "L4", 103, "2026-04-08"),
            ],
        )


def test_build_analytics_db(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    output = tmp_path / "analytics.sqlite"
    make_source_db(source)

    build_analytics_db(source, output)

    with sqlite3.connect(output) as con:
        panel_count = con.execute("SELECT COUNT(*) FROM listing_week_panel").fetchone()[0]
        cut_count = con.execute("SELECT COUNT(*) FROM price_cut_events").fetchone()[0]
        region_count = con.execute("SELECT COUNT(*) FROM region_week_metrics").fetchone()[0]
        project_count = con.execute("SELECT COUNT(*) FROM project_week_metrics").fetchone()[0]
        postal_group = con.execute(
            """
            SELECT project_name, project_group_type, postal_code, active_listings
            FROM project_week_metrics
            WHERE project_uid = 'postal:456789'
            """
        ).fetchone()

    assert panel_count == 5
    assert cut_count == 1
    assert region_count == 3
    assert project_count == 3
    assert postal_group == ("Postal 456789", "postal_code", "456789", 2)


def test_static_export_respects_project_and_event_caps(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    output = tmp_path / "analytics.sqlite"
    assets = tmp_path / "assets"
    make_source_db(source)
    build_analytics_db(source, output)

    export_static_data(output, assets, top_projects=1, event_rows_per_group=1, agent_rows_per_group=1)

    import json

    payload = json.loads((assets / "dashboard-data.json").read_text())
    manifest = json.loads((assets / "manifest.json").read_text())

    assert manifest["rowCounts"]["projects"] == 2
    assert payload["projects"] == []  # loaded only when requested
    detail = json.loads((assets / "weeks/w2.json").read_text())
    events = json.loads((assets / "diagnostics/w2.json").read_text())
    assert len(detail["projects"]) == 2
    assert len(events["priceCuts"]) == 1
    assert len(events["duplicates"]) <= 2
    assert len(events["agents"]) <= 2
    assert payload["market"][-1]["matched_price_count"] == 1
    assert payload["market"][-1]["matched_price_cut_rate"] == 1
    assert payload["market"][-1]["median_psf"] > 0


def test_invalid_postal_is_not_exported_as_project(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    output = tmp_path / "analytics.sqlite"
    assets = tmp_path / "assets"
    make_source_db(source)
    with sqlite3.connect(source) as con:
        con.execute("UPDATE listing_identity SET postal_code='000000' WHERE listing_id IN (3,4)")
    build_analytics_db(source, output)
    with sqlite3.connect(output) as con:
        assert con.execute("SELECT COUNT(*) FROM project_week_metrics WHERE project_uid='postal:000000'").fetchone()[0] == 0
    export_static_data(output, assets)
    import json
    projects = json.loads((assets/'weeks/w2.json').read_text())['projects']
    assert all(r['project_group_type'] == 'actual_project' for r in projects)


def test_matched_rate_excludes_gap_returners(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    output = tmp_path / "analytics.sqlite"
    assets = tmp_path / "assets"
    make_source_db(source)
    with sqlite3.connect(source) as con:
        con.execute("UPDATE search_snapshot SET snapshot_date='2026-04-15', scraped_at='2026-04-15' WHERE snapshot_week_id='w2'")
    build_analytics_db(source, output)
    export_static_data(output, assets)
    import json
    markets = json.loads((assets/'dashboard-data.json').read_text())['market']
    assert not markets[-1]['comparison_eligible']
    assert 'matched_price_cut_rate' not in markets[-1]


def test_capture_warning_blocks_comparison_and_exports_only_safe_metadata(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    output = tmp_path / "analytics.sqlite"
    assets = tmp_path / "assets"
    make_source_db(source)
    with sqlite3.connect(source) as con:
        con.execute('CREATE TABLE crawl_run(snapshot_week_id, started_at, finished_at, status, qa_summary_json)')
        con.execute('INSERT INTO crawl_run VALUES (?, ?, ?, ?, ?)',
                    ('w2', '2026-04-08T00:00:00Z', '2026-04-08T01:00:00Z', 'completed',
                     '{"status":"completed_with_warnings","search":{"search_blocked_count":1},"internal":"not public"}'))
    build_analytics_db(source, output)
    export_static_data(output, assets)
    import json
    raw = (assets/'dashboard-data.json').read_text()
    payload = json.loads(raw)
    latest = payload['market'][-1]
    assert latest['capture_search_warning'] is True
    assert latest['comparison_eligible'] is False
    assert latest['latest_run_status'] == 'completed_with_warnings'
    assert 'not public' not in raw
    assert 'source_path' not in payload['metadata']


def test_conflicting_project_location_history_is_withheld(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    output = tmp_path / "analytics.sqlite"
    assets = tmp_path / "assets"
    make_source_db(source)
    with sqlite3.connect(source) as con:
        con.execute("UPDATE listing_identity SET district_code='D15', district_text='East Coast', region_code='RCR', region_text='RCR' WHERE listing_id=2")
    build_analytics_db(source, output)
    export_static_data(output, assets)
    import json
    payload = json.loads((assets/'dashboard-data.json').read_text())
    detail = json.loads((assets/'weeks/w2.json').read_text())
    assert payload['metadata']['ambiguous_project_history_keys_withheld'] == 1
    assert not any(r['project_uid']=='p1' and r['snapshot_week_id']=='w2' for r in detail['projects'])
    assert any(r['project_uid']=='p1' and r['snapshot_week_id']=='w1' for r in detail['projects'])
