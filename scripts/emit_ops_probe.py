#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

DASHBOARD_URL = "https://yt-codex.github.io/sg-listing-intelligence-dashboard/"
DATA_URL = f"{DASHBOARD_URL}assets/dashboard-data.json"
WARN_SNAPSHOT_AGE_DAYS = 10
FAIL_SNAPSHOT_AGE_DAYS = 21
MIN_ROWS = {
    "market": 1,
    "districts": 1,
    "projects": 1,
    "projectTrends": 1,
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            parsed = date.fromisoformat(text)
            return datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)
        # dashboard build metadata is currently written as UTC without an explicit offset
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(name: str, status: str, detail: str, metric: Any | None = None) -> dict[str, Any]:
    row = {"name": name, "status": status, "detail": detail}
    if metric is not None:
        row["metric"] = metric
    return row


def worst_status(statuses: list[str]) -> str:
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "OK"


def build_probe(root: Path, current: datetime) -> dict[str, Any]:
    data_path = root / "docs" / "assets" / "dashboard-data.json"
    manifest_path = root / "docs" / "assets" / "manifest.json"
    index_path = root / "docs" / "index.html"
    app_path = root / "docs" / "app.js"

    checks: list[dict[str, Any]] = []
    warnings: list[str] = []
    row_counts: dict[str, int | float] = {}
    latest_week: str | None = None
    latest_date_text: str | None = None

    try:
        data = read_json(data_path)
        manifest = read_json(manifest_path)
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "schema_version": "1.0",
            "status": "FAIL",
            "last_run_time": iso_utc(current),
            "duration_seconds": None,
            "freshness": {"max_date": None, "lag_seconds": None, "stale": None},
            "row_counts": {},
            "schema_hash": sha256_file(app_path),
            "key_checks": [check("dashboard data", "FAIL", f"could not read dashboard JSON: {exc}")],
            "warnings": ["dashboard data could not be read"],
            "artifact_links": [{"label": "dashboard", "url": DASHBOARD_URL}],
            "meta": {"dashboard_url": DASHBOARD_URL},
        }

    metadata = data.get("metadata", {}) if isinstance(data.get("metadata"), dict) else {}
    latest_week = str(data.get("latestWeek") or manifest.get("latestWeek") or "") or None
    weeks = data.get("weeks") if isinstance(data.get("weeks"), list) else []
    latest_week_row = next((row for row in weeks if isinstance(row, dict) and row.get("snapshot_week_id") == latest_week), None)
    latest_date_text = str((latest_week_row or {}).get("snapshot_date") or "") or None
    latest_date = parse_dt(latest_date_text)

    manifest_counts = manifest.get("rowCounts") if isinstance(manifest.get("rowCounts"), dict) else {}
    row_counts = {key: value for key, value in manifest_counts.items() if isinstance(value, (int, float))}
    if not row_counts:
        for key in ["market", "regions", "districts", "projects", "projectTrends", "priceCuts", "duplicates", "agents"]:
            value = data.get(key)
            if isinstance(value, list):
                row_counts[key] = len(value)

    if latest_week and data.get("latestWeek") == manifest.get("latestWeek"):
        checks.append(check("latest week manifest", "OK", f"latest week is {latest_week}"))
    else:
        checks.append(check("latest week manifest", "FAIL", "dashboard data and manifest latestWeek disagree"))
        warnings.append("dashboard latestWeek mismatch between data and manifest")

    if latest_date is None:
        checks.append(check("snapshot freshness", "FAIL", "latest snapshot date is missing or invalid"))
        warnings.append("latest snapshot date is missing or invalid")
        lag_seconds = None
    else:
        lag_seconds = max(0.0, (current - latest_date).total_seconds())
        age_days = lag_seconds / 86_400
        if age_days > FAIL_SNAPSHOT_AGE_DAYS:
            status = "FAIL"
            warnings.append(f"latest dashboard snapshot is {age_days:.1f} days old")
        elif age_days > WARN_SNAPSHOT_AGE_DAYS:
            status = "WARN"
            warnings.append(f"latest dashboard snapshot is {age_days:.1f} days old")
        else:
            status = "OK"
        checks.append(check("snapshot freshness", status, f"latest snapshot date is {latest_date_text}", round(age_days, 2)))

    for key, minimum in MIN_ROWS.items():
        count = int(row_counts.get(key, 0))
        status = "OK" if count >= minimum else "FAIL"
        checks.append(check(f"{key} rows", status, f"{count:,} rows exported", count))
        if status == "FAIL":
            warnings.append(f"{key} export has no rows")

    if index_path.exists() and app_path.exists():
        checks.append(check("static app assets", "OK", "index.html and app.js are present"))
    else:
        checks.append(check("static app assets", "FAIL", "index.html or app.js is missing"))
        warnings.append("static app assets are missing")

    status = worst_status([str(row["status"]) for row in checks])
    return {
        "schema_version": "1.0",
        "status": status,
        "last_run_time": iso_utc(current),
        "duration_seconds": None,
        "freshness": {
            "max_date": latest_date_text,
            "lag_seconds": lag_seconds,
            "stale": status in {"WARN", "FAIL"} if latest_date_text else None,
        },
        "row_counts": row_counts,
        "schema_hash": sha256_file(app_path),
        "key_checks": checks,
        "warnings": warnings,
        "artifact_links": [
            {"label": "dashboard", "url": DASHBOARD_URL},
            {"label": "dashboard_data", "url": DATA_URL},
        ],
        "meta": {
            "repo": "yt-codex/sg-listing-intelligence-dashboard",
            "dashboard_url": DASHBOARD_URL,
            "latest_week": latest_week,
            "built_at_utc": metadata.get("built_at_utc"),
            "source_path": metadata.get("source_path"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit ops/probe.json for the listing dashboard.")
    parser.add_argument("--output", default="ops/probe.json")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    payload = build_probe(root, now_utc())
    output_path = root / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0 if payload["status"] != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
