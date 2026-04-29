# SG Listing Intelligence Dashboard

Lightweight MVP dashboard for turning weekly Singapore property portal snapshots into market intelligence signals: inventory pressure, price cuts, stale listings, and project/district-level ranking.

This repo starts from the local PropertyGuru weekly snapshot SQLite database and builds a compact analytics database for the dashboard. The frontend reads only aggregated or panel-ready analytics tables; it does **not** embed raw listing payloads or unnecessary source data.

## MVP scope

The first version focuses on a **Project-level Listing Pressure Dashboard**:

- weekly active listings
- new/disappeared listing counts
- asking price and PSF medians
- price-cut counts and cut sizes
- stale listing share using first-seen age proxy
- district/project ranking tables

## Execution checklist

- [x] Step 1 — Confirm source data shape and available snapshot weeks.
- [x] Step 2 — Create repo scaffold and README implementation plan.
- [x] Step 3 — Define lightweight analytics schema for snapshot updates.
- [x] Step 4 — Implement ETL from source SQLite into compact analytics SQLite.
- [x] Step 5 — Implement light dashboard frontend that queries analytics tables only.
- [x] Step 6 — Add smoke tests / coding hygiene checks.
- [x] Step 7 — Run ETL against the live local source DB and smoke-test the dashboard.
- [x] Step 8 — Create GitHub repo and push initial commit.
- [x] Step 9 — Add lifecycle metrics: disappeared listings, price-cut rates, and pressure scores.
- [x] Step 10 — Add duplicate-candidate and agent-concentration analytics/views.
- [x] Step 11 — Add market-wide trend and data-quality coverage views.

## Data architecture

```text
PropertyGuru source SQLite
        |
        |  read-only ETL
        v
Compact analytics SQLite
        |
        |  aggregate SQL queries only
        v
Streamlit dashboard
```

### Source database

Default local source path:

```text
../propertyguru-sg-weekly-snapshots-prod-20260403-125756/data/state/propertyguru.sqlite
```

Known useful source tables:

- `search_snapshot` — listing-week observations, price, PSF, agent/agency ids
- `listing_identity` — project, district, region, bedrooms, floor area, coordinates, identity fields
- `detail_snapshot` — richer but optional detail features
- `crawl_run` — run metadata and snapshot week ids

### Analytics tables

The ETL creates these compact tables in `data/analytics/listing_intel.sqlite`:

#### `listing_week_panel`

One row per `listing_id × snapshot_week_id` with only dashboard-relevant fields:

- `snapshot_week_id`, `snapshot_date`, `listing_id`
- project/district/region/bedroom/floor-area fields
- `price_value`, `price_per_area_value`
- `agent_id`, `agency_id`, `agent_license`
- `first_seen_at`, `last_seen_at`
- `age_days`
- `prior_price_value`, `price_change_abs`, `price_change_pct`
- `is_price_cut`, `is_new_this_week`, `is_stale_60d`, `is_stale_90d`

#### `project_week_metrics`

Aggregated project-week metrics:

- active listings
- new listings
- disappeared listings
- price cuts and price-cut rate
- average asking price / PSF
- stale share
- agent/agency counts
- concentration proxy using top-agent share
- duplicate-cluster counts
- pressure score

#### `district_week_metrics`

Aggregated district-week metrics for market maps/tables, including lifecycle counts and district pressure scores.

#### `price_cut_events`

Compact event table for drilldown into price cuts without carrying raw portal payloads.

#### `disappeared_listing_events`

Listings observed in one snapshot week but absent in the next observed snapshot week. This is a withdrawal/absorption proxy, not a confirmed transaction label.

#### `duplicate_cluster_candidates`

Heuristic groups of likely duplicate/shadow inventory using same project, bedrooms, rounded area, rounded price, and same agent. These are triage candidates, not final labels.

#### `market_week_metrics`

Market-wide weekly coverage table for trend charts and refresh sanity checks:

- active/new/disappeared listings
- price-cut counts and rates
- duplicate-candidate counts
- distinct projects/districts/agents/agencies
- average asking price and PSF

#### `agent_project_week_metrics`

Agent/project/week concentration view using retained agent IDs and licences. Agent names are intentionally absent because the lean snapshot schema does not retain them.

## Snapshot update design

The dashboard is designed for future weekly snapshot updates:

1. New crawler snapshot writes to the source SQLite.
2. Run the ETL:

```bash
make refresh
```

Equivalent explicit command:

```bash
python -m sg_listing_intel.etl \
  --source ../propertyguru-sg-weekly-snapshots-prod-20260403-125756/data/state/propertyguru.sqlite \
  --output data/analytics/listing_intel.sqlite
```

3. Dashboard reads the refreshed analytics DB:

```bash
make dashboard
```

Equivalent explicit command:

```bash
streamlit run app/streamlit_app.py -- --db data/analytics/listing_intel.sqlite
```

The current ETL rebuilds analytics tables from source for correctness and simplicity. A later version can add incremental rebuilds by `snapshot_week_id` once the dashboard stabilizes.

## Implementation notes

- Keep frontend light: use SQL aggregation and pagination; do not load raw snapshots wholesale into browser/session state.
- Treat source DB as read-only.
- Keep analytics DB reproducible and disposable.
- Prefer clear SQL and small Python modules over a heavy app framework.
- No raw payloads, thumbnails, descriptions, or unnecessary detail fields in the dashboard analytics layer.

## Future views to add

1. **Duplicate / shadow inventory detector**
   - likely duplicate clusters by project, price, floor area, bedrooms, agent/agency, and posted timing
   - duplicate-adjusted inventory

2. **Agent / agency concentration view**
   - top agents by project/district
   - agency concentration
   - relationship between concentration and asking-price dispersion

3. **Listing lifecycle view**
   - first seen, last seen, reappeared listings
   - estimated time-on-market distribution
   - disappeared-listing proxy for absorption or withdrawal

4. **Rental vs sale split**
   - separate sale/rent dashboards if listing type coverage is strong enough
   - rent PSF, rental inventory pressure, lease availability timing

5. **Project detail page**
   - trend chart for one project
   - comparable projects
   - price-cut history
   - stale listing list

6. **Geospatial view**
   - district/project map using aggregated metrics only
   - MRT/school proximity filters from detail snapshots

7. **Research export layer**
   - clean panel exports for econometric work
   - project-week variables: inventory pressure, price-cut intensity, stale share, concentration, asking-price dispersion

8. **Scheduled refresh + diff report**
   - weekly ETL job after crawler completion
   - markdown/email summary of biggest changes

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
make check
```

## Static GitHub Pages dashboard

The `docs/` directory contains a static dashboard for GitHub Pages. It uses exported aggregate JSON from the analytics database and does not ship raw listing snapshots.

Build/update the static data bundle:

```bash
make refresh
make export-static
```

Preview locally:

```bash
make serve-static
```

Then open `http://127.0.0.1:8513`.

## Implemented dashboard views

- **Overview** — market trend charts, district pressure table, and project pressure ranking for the selected snapshot week.
- **Project detail** — selectable project page with weekly trends for active listings, new listings, disappeared listings, price cuts, average PSF, stale share, pressure score, and top-agent share.
- **Price-cut events** — compact table of the largest observed price-cut events for the selected snapshot week.
- **Duplicate candidates** — likely duplicate/shadow-inventory clusters for triage.
- **Agent concentration** — project-agent concentration table using retained IDs/licences.
- **Data quality** — analytics build metadata and snapshot coverage table for refresh sanity checks.

## Current source-data observation

Initial inspection found the local PropertyGuru database has approximately:

- 383,968 rows in `search_snapshot`
- 113,947 rows in `listing_identity`
- 58,303 rows in `detail_snapshot`
- 4 distinct snapshot weeks from `sg-week-2026-04-04` to `sg-week-2026-04-25`
