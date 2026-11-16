"""Versioned executive reporting contract and bounded static payloads.

Scores are descriptive screening rules, not calibrated forecasts. Quality gates are
conservative comparability checks; portal population completeness remains unknown.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from collections import defaultdict
from datetime import date
from pathlib import Path

RECENT_WEEKS = 14
BUDGETS = {
    "initial": 2_500_000,
    "week": 1_500_000,
    "diagnostics": 1_000_000,
    "year": 10_000_000,
    "project": 500_000,
    "manifest": 100_000,
}
SUMMARY = ("market", "regions", "districts", "sourceWeekQuality")


def score_components(row):
    return {
        "price_cuts": round(70 * min(max(row.get("price_cut_rate") or 0, 0) / 0.10, 1), 2),
        "listing_persistence": round(30 * min(max(row.get("stale_60d_share") or 0, 0), 1), 2),
    }


def comparison_flags(current, previous, prior):
    days = (
        (
            date.fromisoformat(current["snapshot_date"][:10])
            - date.fromisoformat(previous["snapshot_date"][:10])
        ).days
        if previous
        else None
    )

    def clean(row):
        return bool(
            row
            and not row.get("imputed_active_listings")
            and row.get("passes_completeness_gate", True)
            and not row.get("capture_search_warning", False)
        )

    eligible = days == 7 and clean(current) and clean(previous)
    prior_days = (
        (
            date.fromisoformat(previous["snapshot_date"][:10])
            - date.fromisoformat(prior["snapshot_date"][:10])
        ).days
        if previous and prior
        else None
    )
    return {
        "comparison_days": days,
        "comparison_from": previous["snapshot_date"] if previous else None,
        "comparison_eligible": eligible,
        "rate_comparison_eligible": eligible and prior_days == 7 and clean(prior),
    }


def attach_capture_metadata(payload):
    source_path = Path(payload["metadata"].get("source_path") or "__unavailable__")
    if source_path.is_file():
        with sqlite3.connect(f"file:{source_path.resolve()}?mode=ro", uri=True) as source:
            source.row_factory = sqlite3.Row
            if source.execute("SELECT 1 FROM sqlite_master WHERE name='crawl_run'").fetchone():
                run_groups = defaultdict(list)
                for run in source.execute(
                    "SELECT snapshot_week_id, started_at, finished_at, status, qa_summary_json FROM crawl_run ORDER BY started_at"
                ):
                    run_groups[run["snapshot_week_id"]].append(run)
                for row in payload["market"]:
                    runs = run_groups.get(row["snapshot_week_id"], [])
                    if runs:
                        latest = runs[-1]
                        try:
                            qa = json.loads(latest["qa_summary_json"] or "{}")
                        except (ValueError, TypeError):
                            qa = {}
                        row.update(
                            capture_started_at=runs[0]["started_at"],
                            capture_finished_at=latest["finished_at"],
                            capture_run_count=len(runs),
                            latest_run_status=qa.get("status") or latest["status"],
                            capture_search_warning=bool(
                                (qa.get("search") or {}).get("search_error_count")
                                or (qa.get("search") or {}).get("search_blocked_count")
                                or latest["status"] != "completed"
                            ),
                        )


def enrich_payload(payload, db_path):
    """Attach measured quality/comparison fields and observed asking-price medians."""
    attach_capture_metadata(payload)
    quality = {(r["snapshot_week_id"], r["listing_type"]): r for r in payload["sourceWeekQuality"]}
    groups = defaultdict(list)
    for row in payload["market"]:
        row["passes_completeness_gate"] = bool(
            quality.get((row["snapshot_week_id"], row["listing_type"]), {}).get(
                "passes_completeness_gate", False
            )
        )
        groups[(row["listing_type"], row["property_segment"])].append(row)
    market_lookup = {}
    for group in groups.values():
        group.sort(key=lambda r: r["snapshot_date"])
        for i, row in enumerate(group):
            row.update(
                comparison_flags(row, group[i - 1] if i else None, group[i - 2] if i > 1 else None)
            )
            row["quality_status"] = (
                "Reconstructed"
                if row.get("imputed_active_listings")
                else "Observed; completeness unverified"
            )
            row["crawl_completeness"] = "unverified"
            market_lookup[
                (row["snapshot_week_id"], row["listing_type"], row["property_segment"])
            ] = row
    with sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True) as con:
        con.row_factory = sqlite3.Row
        # Median excludes reconstructed rows and invalid/nonpositive PSF. Observation
        # count is shown so a median is never passed off as a constant-quality index.
        stats = con.execute("""WITH ordered AS (
          SELECT snapshot_week_id, listing_type, property_segment, price_per_area_value,
            ROW_NUMBER() OVER (PARTITION BY snapshot_week_id, listing_type, property_segment ORDER BY price_per_area_value) AS rn,
            COUNT(*) OVER (PARTITION BY snapshot_week_id, listing_type, property_segment) AS n
          FROM listing_week_panel WHERE is_imputed=0 AND price_per_area_value > 0)
          SELECT snapshot_week_id, listing_type, property_segment, AVG(price_per_area_value) AS median_psf, MAX(n) AS price_observation_count
          FROM ordered WHERE rn IN ((n+1)/2, (n+2)/2)
          GROUP BY snapshot_week_id, listing_type, property_segment""")
        for stat in stats:
            row = market_lookup.get(
                tuple(stat[k] for k in ("snapshot_week_id", "listing_type", "property_segment"))
            )
            if row:
                row.update(
                    median_psf=round(stat["median_psf"], 2),
                    price_observation_count=stat["price_observation_count"],
                )
        # Exact seven-day pairs only; returning after a gap is not a weekly price cut.
        stats = con.execute("""SELECT c.snapshot_week_id, c.listing_type, c.property_segment,
            COUNT(*) AS matched_price_count,
            SUM(CASE WHEN c.price_value < p.price_value AND c.is_clean_price_cut=1 THEN 1 ELSE 0 END) AS matched_price_cuts
          FROM listing_week_panel c JOIN listing_week_panel p
          ON c.listing_id=p.listing_id AND c.listing_type=p.listing_type
          AND c.property_segment=p.property_segment AND p.snapshot_date=DATE(c.snapshot_date, '-7 days')
          WHERE c.is_imputed=0 AND p.is_imputed=0 AND c.price_value>0 AND p.price_value>0
          AND NOT (c.is_price_cut=1 AND c.is_clean_price_cut=0)
          GROUP BY c.snapshot_week_id, c.listing_type, c.property_segment""")
        for stat in stats:
            row = market_lookup.get(
                tuple(stat[k] for k in ("snapshot_week_id", "listing_type", "property_segment"))
            )
            if row:
                row.update(
                    matched_price_count=stat["matched_price_count"],
                    matched_price_cuts=stat["matched_price_cuts"],
                    matched_price_cut_rate=round(
                        stat["matched_price_cuts"] / stat["matched_price_count"], 5
                    ),
                )
    for section in ("regions", "districts", "projects", "projectTrends"):
        for row in payload[section]:
            source = market_lookup.get(
                (row["snapshot_week_id"], row["listing_type"], row["property_segment"]), {}
            )
            row.update(
                {
                    k: source.get(k)
                    for k in (
                        "comparison_eligible",
                        "rate_comparison_eligible",
                        "comparison_days",
                        "quality_status",
                    )
                }
            )
            row["score_components"] = score_components(row)
            row["pressure_score"] = round(sum(row["score_components"].values()), 2)
            row["score_version"] = "fixed-v1"
            if section in ("projects", "projectTrends"):
                row["trend_key"] = hashlib.sha256(row["project_uid"].encode()).hexdigest()[:16]
    payload["metadata"].pop("source_path", None)
    payload["metadata"].update(
        first_snapshot_date=payload["weeks"][0]["snapshot_date"] if payload["weeks"] else None,
        score_method="fixed-v1: 70 × min(price-cut rate / 10%, 1) + 30 × observed-age-60d share; unvalidated screening rule",
        age_definition="Days since first observed advert, not verified continuous time on market or unit sale duration",
        completeness="Source count gate is a heuristic; full portal coverage is not certified",
        official_context_status="Not integrated; consult source releases with their own reference dates",
        price_basis="Advert asking PSF; unadjusted for composition; matched rates use exact seven-day observed pairs",
    )


def encode(payload):
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()


def write_sharded_payload(out_dir: Path, payload):
    """Append-only time-partitioned archives; fixed window bootstrap, hard byte gates.

    All budget checks happen before any output mutation. Shards are deterministic;
    old generated files are retained, so concurrent readers never lose an archive.
    """
    weeks = payload["weeks"]
    recent = {r["snapshot_week_id"] for r in weeks[-RECENT_WEEKS:]}
    files = {}

    def stage(name, value, kind):
        raw = encode(value)
        if len(raw) > BUDGETS[kind]:
            raise ValueError(f"Payload budget exceeded: {name}: {len(raw)} > {BUDGETS[kind]} bytes")
        files[name] = (raw, kind)

    def subset(ids):
        return {key: [r for r in payload[key] if r["snapshot_week_id"] in ids] for key in SUMMARY}

    initial = {
        "schemaVersion": 2,
        "metadata": payload["metadata"],
        "latestWeek": payload["latestWeek"],
        "weeks": weeks[-RECENT_WEEKS:],
        **subset(recent),
        **{key: [] for key in ("projects", "projectTrends", "priceCuts", "duplicates", "agents")},
    }
    stage("dashboard-data.json", initial, "initial")
    by_year = defaultdict(list)
    for i, week in enumerate(weeks):
        wid = week["snapshot_week_id"]
        by_year[week["snapshot_date"][:4]].append(week)
        ids = {wid, weeks[i - 1]["snapshot_week_id"] if i else wid}
        stage(
            f"weeks/{wid}.json",
            {
                "weeks": weeks[max(0, i - 1) : i + 1],
                **subset({wid}),
                "projects": [r for r in payload["projects"] if r["snapshot_week_id"] in ids],
            },
            "week",
        )
        stage(
            f"diagnostics/{wid}.json",
            {
                key: [r for r in payload[key] if r["snapshot_week_id"] == wid]
                for key in ("priceCuts", "duplicates", "agents")
            },
            "diagnostics",
        )
    for year, year_weeks in by_year.items():
        stage(
            f"years/{year}.json",
            {"weeks": year_weeks, **subset({r["snapshot_week_id"] for r in year_weeks})},
            "year",
        )
    projects = defaultdict(list)
    for row in payload["projectTrends"]:
        projects[(row["trend_key"], row["snapshot_date"][:4])].append(row)
    for (key, year), trend in projects.items():
        columns = list(dict.fromkeys(k for row in trend for k in row))
        compact = {"columns": columns, "rows": [[row.get(k) for k in columns] for row in trend]}
        stage(f"projects/{key}/{year}.json", {"projectTrends": compact}, "project")
    largest_name = max(files, key=lambda name: len(files[name][0]))
    sizes = {
        "initialRawBytes": len(files["dashboard-data.json"][0]),
        "initialGzipBytes": len(gzip.compress(files["dashboard-data.json"][0], mtime=0)),
        "largestShardRawBytes": len(files[largest_name][0]),
        "largestShardGzipBytes": len(gzip.compress(files[largest_name][0], mtime=0)),
        "largestShardPath": largest_name,
        "totalRawBytes": sum(len(raw) for raw, _ in files.values()),
        "files": len(files),
        "budgetsRawBytes": BUDGETS,
    }
    manifest = {
        "schemaVersion": 2,
        "latestWeek": payload["latestWeek"],
        "years": sorted(by_year),
        "recentWeeks": RECENT_WEEKS,
        "rowCounts": {key: len(payload[key]) for key in payload if isinstance(payload[key], list)},
        "sizes": sizes,
    }
    stage("manifest.json", manifest, "manifest")
    for name, (raw, _) in files.items():
        target = out_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_bytes(raw)
        temporary.replace(target)
    return manifest
