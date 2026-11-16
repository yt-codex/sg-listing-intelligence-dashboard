# Verification — 16 November 2026

Branch: `feat/executive-monitoring`. Local implementation only; no push or deployment.

- Rebuilt analytics against the actual source: 2,454,051 panel observations retained. Latest private non-landed sale inventory remains 31,745. Invalid `postal:000000` analytic groups removed.
- Current market price observations: 31,729; median asking PSF S$2,101.01. Exact-seven-day matched observations: 29,247; 738 clean cuts (2.523%). These are advertisements, not distinct homes or transactions.
- The 14 November rate-direction comparison is withheld because the preceding aggregate observation window crossed the 10 October–7 November gap.
- Python regression suite: 11 passed. JavaScript contract suite: 10 passed. Ruff, JavaScript syntax, shell syntax and whitespace checks passed.
- Actual-data browser smoke passed: initial load requests only bootstrap and manifest; six analyst tabs, project histories, archived-year and week selection, mobile width; no page errors. Plot data checked against actual latest inventory and median PSF.
- A4 weekly brief: one page on actual latest data. Screenshots and PDF in workspace `artifacts/listing-executive-review/`.
- Ops probe: WARN for market-inference comparability; snapshot is fresh, static assets/shards and byte budgets pass. Warning is intentional and is distinct from operational failure.

## Payload evidence

| Measure | Before | After |
|---|---:|---:|
| Initial data, raw bytes | 75,196,806 | 2,271,348 |
| Initial data, gzip bytes | 2,586,149 | 152,023 |
| Total generated JSON bytes, including manifest | 75,196,806 monolith alone | 33,912,092 |

Largest optional shard: `years/2026.json`, 4,869,056 raw bytes / 310,637 gzip bytes.
There are 406 generated JSON files including the manifest. Gzip sizes are local
compression measurements, not a guarantee about a hosting service's transfer encoding.
The regression suite simulates 520 weeks with representative market/region/district
group counts: the bootstrap remains fixed-window and each archive/project history
remains calendar-year bounded. All hard budget checks pass on the actual export.

## Remaining limitations

Official series are linked, not ingested. New-launch/resale and room/whole-unit
classification, verified project-unit denominators and unit-level deduplication are
not available consistently. Region labels retain the portal's definition rather
than claiming verified CCR/RCR/OCR geography. The screening score is a fixed,
unvalidated rule, not a forecast; its descriptive all-advert cut share differs from
the exact-seven-day matched market rate. Full portal completeness is unverified.
See the design note for all definitions and archive/retention behavior.
