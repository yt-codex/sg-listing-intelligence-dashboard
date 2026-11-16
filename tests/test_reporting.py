import json
from datetime import date, timedelta

import pytest

from sg_listing_intel.reporting import (
    BUDGETS,
    comparison_flags,
    score_components,
    write_sharded_payload,
)


def row(day, imputed=0):
    return {
        "snapshot_date": day,
        "imputed_active_listings": imputed,
        "passes_completeness_gate": True,
    }


def test_gap_and_imputation_block_direction():
    assert comparison_flags(row("2026-11-14"), row("2026-11-07"), row("2026-10-10")) == {
        "comparison_days": 7,
        "comparison_from": "2026-11-07",
        "comparison_eligible": True,
        "rate_comparison_eligible": False,
    }
    assert not comparison_flags(row("2026-11-07"), row("2026-10-10"), row("2026-10-03"))[
        "comparison_eligible"
    ]
    assert not comparison_flags(row("2026-11-14"), row("2026-11-07", 10), row("2026-10-31"))[
        "comparison_eligible"
    ]
    assert comparison_flags(row("2026-11-14"), row("2026-11-07"), row("2026-10-31"))[
        "rate_comparison_eligible"
    ]
    assert not comparison_flags(
        {**row("2026-11-14"), "capture_search_warning": True}, row("2026-11-07"), row("2026-10-31")
    )["comparison_eligible"]


def test_score_is_absolute_and_ignores_advertising_proxies():
    original = {
        "price_cut_rate": 0.05,
        "stale_60d_share": 0.5,
        "active_listings": 20,
        "duplicate_candidate_listings": 0,
    }
    changed = {
        **original,
        "active_listings": 20000,
        "duplicate_candidate_listings": 10000,
        "top_agent_share": 1,
    }
    assert (
        score_components(original)
        == score_components(changed)
        == {"price_cuts": 35, "listing_persistence": 15}
    )
    assert sum(score_components({"price_cut_rate": 1, "stale_60d_share": 1}).values()) == 100


def fixture(week_count):
    result = {
        "metadata": {},
        **{
            k: []
            for k in (
                "weeks",
                "market",
                "regions",
                "districts",
                "projects",
                "projectTrends",
                "priceCuts",
                "duplicates",
                "agents",
                "sourceWeekQuality",
            )
        },
    }
    for i in range(week_count):
        day = str(date(2020, 1, 4) + timedelta(days=i * 7))
        wid = "sg-week-" + day
        result["weeks"].append({"snapshot_week_id": wid, "snapshot_date": day})
        for key in ("market", "regions", "districts", "sourceWeekQuality"):
            for group in range(
                {"market": 6, "regions": 60, "districts": 174, "sourceWeekQuality": 2}[key]
            ):
                result[key].append(
                    {
                        "snapshot_week_id": wid,
                        "snapshot_date": day,
                        "active_listings": 100,
                        "group": group,
                    }
                )
        result["projects"].append(
            {"snapshot_week_id": wid, "snapshot_date": day, "project_uid": "a", "trend_key": "abc"}
        )
        result["projectTrends"].append(
            {"snapshot_week_id": wid, "snapshot_date": day, "project_uid": "a", "trend_key": "abc"}
        )
    result["latestWeek"] = wid
    return result


def test_future_growth_keeps_bootstrap_and_each_project_shard_bounded(tmp_path):
    early = write_sharded_payload(tmp_path / "early", fixture(30))
    future = write_sharded_payload(tmp_path / "future", fixture(520))
    assert early["sizes"]["initialRawBytes"] == future["sizes"]["initialRawBytes"]
    boot = json.loads((tmp_path / "future/dashboard-data.json").read_text())
    assert len(boot["weeks"]) == 14 and boot["projects"] == [] and boot["projectTrends"] == []
    assert len(future["years"]) > len(early["years"])
    old = json.loads((tmp_path / "future/projects/abc/2020.json").read_text())
    assert len(old["projectTrends"]["rows"]) <= 53
    assert len(json.loads((tmp_path / "future/years/2020.json").read_text())["weeks"]) <= 53
    assert (tmp_path / "future/weeks/sg-week-2020-01-04.json").exists()


def test_budget_failure_does_not_replace_existing_bootstrap(tmp_path):
    payload = fixture(30)
    write_sharded_payload(tmp_path, payload)
    before = (tmp_path / "dashboard-data.json").read_bytes()
    payload["metadata"]["oversized"] = "x" * BUDGETS["initial"]
    with pytest.raises(ValueError, match="budget exceeded"):
        write_sharded_payload(tmp_path, payload)
    assert (tmp_path / "dashboard-data.json").read_bytes() == before


def test_probe_checks_shards_and_separates_comparison_warning(tmp_path):
    from datetime import datetime, timezone
    from scripts.emit_ops_probe import build_probe

    root = tmp_path
    assets = root / "docs/assets"
    payload = fixture(30)
    for r in payload["market"]:
        r.update(
            listing_type="SALE",
            property_segment="PRIVATE_NON_LANDED",
            rate_comparison_eligible=False,
        )
    write_sharded_payload(assets, payload)
    (root / "docs/index.html").write_text("ok")
    (root / "docs/app.js").write_text("ok")
    latest = date.fromisoformat(payload["weeks"][-1]["snapshot_date"])
    now = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
    probe = build_probe(root, now)
    assert probe["status"] == "WARN"
    assert probe["freshness"]["stale"] is False
    assert any(
        c["name"] == "market inference comparability" and c["status"] == "WARN"
        for c in probe["key_checks"]
    )
    (assets / "weeks" / f"{payload['latestWeek']}.json").unlink()
    assert build_probe(root, now)["status"] == "FAIL"
