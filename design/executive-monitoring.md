# Executive monitoring upgrade

## Approved outcome

Provide a management overview of private-housing listing conditions with clear limitations, comparable time windows and evidence-backed drilldown. Keep payload transfer bounded as weekly history grows. Preserve the analyst views and all retained source data.

## Design and acceptance

- Bootstrap: last 14 snapshots of market/region/district summaries only. No project trends or diagnostic events on initial load.
- Detail: current/prior project candidates per weekly file; diagnostics loaded only when requested; archive summaries and project histories partitioned by calendar year.
- Every export enforces raw byte budgets before writing files. Initial summary <=2.5 MB; weekly detail <=1.5 MB; diagnostics <=1 MB; archive year <=10 MB; project-year <=500 KB; manifest <=100 KB. A budget breach fails the refresh rather than publishing an unbounded payload. Tests simulate ten years of history.
- All generated files are staged by the scheduled publication script, preserving the existing automatic publication opt-in.
- Seven-day comparison eligibility requires observed current and preceding snapshots passing the source count gate. Rate-direction eligibility additionally requires the prior window also be seven days with eligible data. Intervals are not divided by elapsed weeks. Missing points are not equivalent to zero changes.
- Price-cut matched rate uses only positive-price, observed listings present exactly seven days earlier; parser-correction exclusions still apply. Display median asking PSF and sample size; do not label as a transaction or constant-quality index.
- Descriptive pressure score v1: `70 × min(cut rate / 10%, 1) + 30 × first-observed-age >=60d share`. The 10% threshold is a fixed screening convention, not a statistically calibrated stress boundary. Inventory, duplicate advertising and agent concentration remain diagnostic context, not score inputs. Scores from old versions must not be spliced into this version.
- Quality badges describe evidence adequacy. They do not certify market coverage, deduplicated units or statistical confidence.
- Unknown/invalid postal groups are excluded from executive candidates. Only actual project and usable postal groups are exported as project candidates; candidates remain capped at the largest 75 groups per market/segment/week, explicitly not an exhaustive project universe.
- Actual source remains read-only. No network collection, credential change or public publication is part of this implementation branch.

## Interpretation limits and follow-ups

- Portal-wide completeness is not certified by the analytics data. Passing a relative count gate or having zero imputation cannot certify complete crawling. The quality strip says so.
- First-seen age measures the age of an advert identity, not uninterrupted time on market of a unique dwelling. Reused developer advertisements can stay old. The many 100% persistent project rows are therefore not proof that every unit is unsold for 60 days. Validating/relisting/deduplicating unit identity requires additional source work.
- New-launch and resale classification, room-versus-whole-unit rental classification and project-unit denominators are not consistently available in this analytics schema. Do not infer them from project names or price thresholds. Until validated enrichment exists, they remain explicit coverage limitations.
- No stale third-party mirror of official data is presented as a current market statistic. Official market context links to authoritative source releases; automated ingestion remains a follow-up requiring reliable series mappings, reference-period and revision handling.
- Region labels in the source may be portal regions rather than URA CCR/RCR/OCR. Preserve source labels and label their basis; do not falsely translate district groups into official market segments.
- Imputed historical observations may be revised as later snapshots bridge a gap. Export build time and imputation shares must be retained in the weekly brief; this is not an immutable real-time-vintage backtest.

## Storage and retention

Browser requests are bounded per view/year; total retained static storage still grows approximately linearly with history. Files are written deterministically and atomically per-file after all byte budgets pass. Existing archive shards are retained so archived links are not broken. This does not provide an atomic multi-file transaction; deploy the complete build through GitHub Pages. A schema/version change must update the app and data together.

`dashboard-data.json` is now a compact bootstrap, not the former 72 MB all-history package. `manifest.json` lists available archive years, row counts and raw/gzip size measurements. `years/YYYY.json` contains that year's summaries; `weeks/<week>.json` contains that week's summary and capped current/prior project candidates; `diagnostics/<week>.json` contains capped events; `projects/<stable-hash>/YYYY.json` contains the selected project candidates' available history for that year. A project absent from a year may legitimately have no history shard.

Project-year histories use a columnar JSON encoding (`columns` plus `rows`) to avoid repeating field names in every observation; the loader expands only the requested shard in memory.

When available, read-only crawl QA supplies collection-run start/finish (including downstream processing), run count,
and latest run status. Explicit search errors, suspected blocking or unfinished runs
also disable comparisons. A clean recovery run remains insufficient to certify
whole-portal completeness. Operational freshness and inference warnings are separate
in the ops probe: a fresh but incomparable snapshot is not labelled stale.

Historical project keys with multiple location/group rows in the same market, segment and week are withheld from candidates and histories, rather than summed or plotted repeatedly. Counts of withheld keys and candidate rows are exposed in metadata. Missing history remains a gap, not a zero value.

The legacy descriptive cut share (used in the screening score) divides clean cuts since each advert’s last observation by all current adverts; an individual returning advert can have a longer interval even when adjacent aggregate snapshots are weekly. The executive matched rate is a separate exact-seven-day measure with its own denominator. These are labelled separately; matched rates are not yet computed at every geographic/project level.
