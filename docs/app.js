const fmt = new Intl.NumberFormat("en-SG");
const money = new Intl.NumberFormat("en-SG", { maximumFractionDigits: 0 });
let data;
let charts = {};
let selectedWeek;
let selectedType = "SALE";
let selectedSegment = "PRIVATE_NON_LANDED";
let projectOptionRows = [];
let selectedProjectUid = null;

function n(value) {
  return value === null || value === undefined || Number.isNaN(Number(value)) ? 0 : Number(value);
}

function fmtNum(value, digits = 0) {
  if (value === null || value === undefined || value === "") return "—";
  const safeDigits = Number.isFinite(Number(digits)) ? Math.max(0, Math.min(20, Number(digits))) : 0;
  return Number(value).toLocaleString("en-SG", { maximumFractionDigits: safeDigits });
}

function fmtPct(value, digits = 1) {
  if (value === null || value === undefined || value === "") return "—";
  return `${fmtNum(n(value) * 100, digits)}%`;
}

function fmtPp(value, digits = 1) {
  if (value === null || value === undefined || value === "") return "—";
  const pp = n(value) * 100;
  return `${pp > 0 ? "+" : ""}${fmtNum(pp, digits)} pp`;
}

function fmtSigned(value, digits = 0) {
  if (value === null || value === undefined || value === "") return "—";
  const number = n(value);
  return `${number > 0 ? "+" : ""}${fmtNum(number, digits)}`;
}

function metric(label, value, note = "") {
  return `<article class="metric"><div class="label">${label}</div><div class="value">${value}</div>${note ? `<div class="note">${note}</div>` : ""}</article>`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>\"]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[ch]));
}

function matchesSegment(row) {
  return (!row.listing_type || row.listing_type === selectedType)
    && (!row.property_segment || row.property_segment === selectedSegment);
}

function selectedRows(rows) {
  return rows.filter(matchesSegment);
}

function byWeek(rows) {
  return rows.filter((row) => row.snapshot_week_id === selectedWeek && matchesSegment(row));
}

function priorWeekId() {
  const idx = data.weeks.findIndex((w) => w.snapshot_week_id === selectedWeek);
  return idx > 0 ? data.weeks[idx - 1].snapshot_week_id : null;
}

function destroyChart(id) {
  if (charts[id]) {
    charts[id].destroy();
    delete charts[id];
  }
}

function lineChart(id, labels, series, options = {}) {
  destroyChart(id);
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: series.map((s) => ({
        label: s.label,
        data: s.data,
        borderWidth: 2,
        tension: 0.25,
        pointRadius: 2,
        spanGaps: false,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "bottom" } },
      scales: { y: { beginAtZero: options.beginAtZero ?? false } },
    },
  });
}

function barChart(id, labels, series, options = {}) {
  destroyChart(id);
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets: series.map((s) => ({ label: s.label, data: s.data, borderWidth: 1 })) },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: options.horizontal ? "y" : "x",
      plugins: { legend: { position: "bottom" } },
      scales: { x: { beginAtZero: true }, y: { beginAtZero: true } },
    },
  });
}

function movementValue(rows, idx, key) {
  return idx === 0 ? null : n(rows[idx][key]);
}

function isFirstSelectedSnapshot(rows) {
  return rows.findIndex((r) => r.snapshot_week_id === selectedWeek) === 0;
}

function isFirstSnapshotRow(row) {
  return row.snapshot_week_id === data.weeks?.[0]?.snapshot_week_id;
}

function fmtMovement(value, row) {
  return isFirstSnapshotRow(row) ? "n/a" : fmtNum(value);
}

function table(containerId, rows, columns, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!rows.length) {
    el.innerHTML = `<p class="subtle">No rows for this view.</p>`;
    return;
  }
  const thead = columns.map((c) => `<th class="${c.num ? "num" : ""}">${c.label}</th>`).join("");
  const body = rows
    .map((row) => {
      const cells = columns
        .map((c) => {
          const raw = row[c.key];
          const value = c.format ? c.format(raw, row) : raw ?? "—";
          return `<td class="${c.num ? "num" : ""}">${value}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  const maxHeight = options.compact ? "420px" : "620px";
  el.innerHTML = `<div class="table-wrap" style="max-height:${maxHeight}"><table><thead><tr>${thead}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function setupTabs() {
  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
      document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
      button.classList.add("active");
      document.getElementById(button.dataset.tab).classList.add("active");
    });
  });
}

function rankWithin(rows, key, direction = "desc") {
  const sorted = [...rows].sort((a, b) => direction === "desc" ? n(b[key]) - n(a[key]) : n(a[key]) - n(b[key]));
  const ranks = new Map();
  sorted.forEach((row, idx) => ranks.set(row.__key, idx + 1));
  return ranks;
}

function percentileWithin(rows, key) {
  const sorted = [...rows].sort((a, b) => n(a[key]) - n(b[key]));
  const percentiles = new Map();
  const denom = Math.max(1, sorted.length - 1);
  sorted.forEach((row, idx) => percentiles.set(row.__key, idx / denom));
  return percentiles;
}

function enrichRows(rows, keyFn, level) {
  const all = selectedRows(rows).map((row) => ({ ...row, __key: keyFn(row) })).filter((row) => row.__key);
  const byKey = new Map();
  all.forEach((row) => {
    if (!byKey.has(row.__key)) byKey.set(row.__key, []);
    byKey.get(row.__key).push(row);
  });
  byKey.forEach((group) => group.sort((a, b) => data.weeks.findIndex((w) => w.snapshot_week_id === a.snapshot_week_id) - data.weeks.findIndex((w) => w.snapshot_week_id === b.snapshot_week_id)));

  const rankMaps = new Map();
  const pctMaps = new Map();
  data.weeks.forEach((week) => {
    const weekRows = all.filter((r) => r.snapshot_week_id === week.snapshot_week_id);
    rankMaps.set(week.snapshot_week_id, rankWithin(weekRows, "pressure_score", "desc"));
    pctMaps.set(week.snapshot_week_id, percentileWithin(weekRows, "pressure_score"));
  });

  return all.map((row) => {
    const group = byKey.get(row.__key) || [];
    const idx = group.findIndex((r) => r.snapshot_week_id === row.snapshot_week_id);
    const prev = idx > 0 ? group[idx - 1] : null;
    const ranks = rankMaps.get(row.snapshot_week_id) || new Map();
    const prevRanks = prev ? (rankMaps.get(prev.snapshot_week_id) || new Map()) : new Map();
    const percentiles = pctMaps.get(row.snapshot_week_id) || new Map();
    const pressureDelta = prev ? n(row.pressure_score) - n(prev.pressure_score) : null;
    const activeDelta = prev ? n(row.active_listings) - n(prev.active_listings) : null;
    const cutRateDelta = prev ? n(row.price_cut_rate) - n(prev.price_cut_rate) : null;
    const staleDelta = prev ? n(row.stale_60d_share) - n(prev.stale_60d_share) : null;
    const duplicateDelta = prev ? n(row.duplicate_candidate_listings) - n(prev.duplicate_candidate_listings) : null;
    const rank = ranks.get(row.__key) ?? null;
    const prevRank = prev ? (prevRanks.get(row.__key) ?? null) : null;
    const rankMove = rank && prevRank ? prevRank - rank : null;
    return {
      ...row,
      level,
      prev,
      pressure_delta: pressureDelta,
      active_delta: activeDelta,
      cut_rate_delta: cutRateDelta,
      stale_delta: staleDelta,
      duplicate_delta: duplicateDelta,
      pressure_rank: rank,
      prev_pressure_rank: prevRank,
      rank_move: rankMove,
      pressure_percentile: percentiles.get(row.__key) ?? null,
      confidence: confidence(row, level),
    };
  });
}

function confidence(row, level) {
  if (level === "region") {
    if (n(row.active_listings) >= 200) return "High";
    if (n(row.active_listings) >= 75) return "Medium";
    return "Low";
  }
  if (level === "district") {
    if (n(row.active_listings) >= 40) return "High";
    if (n(row.active_listings) >= 15) return "Medium";
    return "Low";
  }
  if (row.project_group_type === "actual_project" && n(row.active_listings) >= 12) return "High";
  if (row.project_group_type !== "listing_title" && n(row.active_listings) >= 6) return "Medium";
  return "Low";
}

function badge(text, tone = "neutral") {
  return `<span class="badge ${tone}">${escapeHtml(text)}</span>`;
}

function confidenceBadge(value) {
  const tone = value === "High" ? "good" : value === "Medium" ? "warn" : "low";
  return badge(value, tone);
}

function directionBadge(value, suffix = "") {
  if (value === null || value === undefined) return badge("new", "neutral");
  if (n(value) > 0) return badge(`▲ ${fmtNum(value, 1)}${suffix}`, "bad");
  if (n(value) < 0) return badge(`▼ ${fmtNum(Math.abs(value), 1)}${suffix}`, "good");
  return badge("flat", "neutral");
}

function signalReasons(row) {
  const reasons = [];
  if (row.pressure_delta !== null && n(row.pressure_delta) >= 8) reasons.push(`pressure +${fmtNum(row.pressure_delta, 1)}`);
  if (row.rank_move !== null && n(row.rank_move) >= 5) reasons.push(`rank up ${fmtNum(row.rank_move)}`);
  if (row.cut_rate_delta !== null && n(row.cut_rate_delta) >= 0.02) reasons.push(`cut rate ${fmtPp(row.cut_rate_delta)}`);
  if (row.stale_delta !== null && n(row.stale_delta) >= 0.03) reasons.push(`stale share ${fmtPp(row.stale_delta)}`);
  if (row.duplicate_delta !== null && n(row.duplicate_delta) >= 3) reasons.push(`duplicates +${fmtNum(row.duplicate_delta)}`);
  if (n(row.pressure_percentile) >= 0.9) reasons.push(`top ${fmtNum((1 - n(row.pressure_percentile)) * 100, 0)}% pressure`);
  if (!reasons.length && n(row.pressure_score) >= 60) reasons.push("high pressure level");
  if (!reasons.length && row.pressure_delta !== null && n(row.pressure_delta) < 0) reasons.push(`pressure easing ${fmtNum(row.pressure_delta, 1)}`);
  if (!reasons.length) reasons.push("monitor for next snapshot");
  return reasons.slice(0, 3).join("; ");
}

function projectVsDistrict(row) {
  const districtRows = byWeek(data.districts);
  const d = districtRows.find((r) => r.district_code === row.district_code || r.district_text === row.district_text);
  if (!d) return "—";
  const delta = n(row.pressure_score) - n(d.pressure_score);
  return `${delta > 0 ? "+" : ""}${fmtNum(delta, 1)} vs district`;
}

function currentDistricts() {
  return enrichRows(data.districts, (r) => r.district_code || r.district_text, "district")
    .filter((r) => r.snapshot_week_id === selectedWeek)
    .sort((a, b) => n(b.pressure_score) - n(a.pressure_score));
}

function currentRegions() {
  return enrichRows(data.regions || [], (r) => r.region_text, "region")
    .filter((r) => r.snapshot_week_id === selectedWeek)
    .sort((a, b) => n(b.pressure_score) - n(a.pressure_score));
}

function currentProjects() {
  return enrichRows(data.projects, (r) => r.project_uid, "project")
    .filter((r) => r.snapshot_week_id === selectedWeek)
    .sort((a, b) => n(b.pressure_score) - n(a.pressure_score));
}

function currentMarketRows() {
  return selectedRows(data.market).sort((a, b) => data.weeks.findIndex((w) => w.snapshot_week_id === a.snapshot_week_id) - data.weeks.findIndex((w) => w.snapshot_week_id === b.snapshot_week_id));
}

function regionTrendContext() {
  const rows = selectedRows(data.regions || []);
  const current = rows
    .filter((r) => r.snapshot_week_id === selectedWeek)
    .sort((a, b) => n(b.active_listings) - n(a.active_listings));
  const regionNames = current.map((r) => r.region_text).slice(0, 10);
  const rowMap = new Map(rows.map((r) => [`${r.snapshot_week_id}|${r.region_text}`, r]));
  return {
    labels: data.weeks.map((w) => w.snapshot_week_id),
    regionNames,
    rowMap,
  };
}

function regionTrendSeries(context, key, transform = (value) => n(value)) {
  return context.regionNames.map((region) => ({
    label: region,
    data: context.labels.map((week) => {
      const row = context.rowMap.get(`${week}|${region}`);
      return row ? transform(row[key], row) : null;
    }),
  }));
}

function marketCurrentAndPrev() {
  const rows = currentMarketRows();
  const idx = rows.findIndex((r) => r.snapshot_week_id === selectedWeek);
  return { current: rows[idx] || {}, prev: idx > 0 ? rows[idx - 1] : null, rows };
}

function renderPulse() {
  const { current, prev, rows: marketRows } = marketCurrentAndPrev();
  const districts = currentDistricts();
  const risingDistricts = districts.filter((r) => n(r.pressure_delta) >= 5).length;
  const highPressureDistricts = districts.filter((r) => n(r.pressure_percentile) >= 0.9).length;
  const activeDelta = prev ? n(current.active_listings) - n(prev.active_listings) : null;
  const cutRateDelta = prev ? n(current.price_cut_rate) - n(prev.price_cut_rate) : null;
  const staleDelta = prev ? n(current.stale_60d_share) - n(prev.stale_60d_share) : null;

  document.getElementById("metricCards").innerHTML = [
    metric("Active inventory", fmt.format(n(current.active_listings)), activeDelta === null ? "first observed week" : `${fmtSigned(activeDelta)} WoW`),
    metric("Price-cut rate", fmtPct(current.price_cut_rate), cutRateDelta === null ? "first observed week" : `${fmtPp(cutRateDelta)} WoW`),
    metric("Stale 60d share", fmtPct(current.stale_60d_share), staleDelta === null ? "first observed week" : `${fmtPp(staleDelta)} WoW`),
    metric("Rising districts", fmtNum(risingDistricts), "pressure +5 score or more"),
    metric("High-pressure districts", fmtNum(highPressureDistricts), "top-decile district pressure"),
  ].join("");

  const topDeterioration = [...districts].filter((r) => r.pressure_delta !== null).sort((a, b) => n(b.pressure_delta) - n(a.pressure_delta)).slice(0, 5);
  const topPressure = districts.slice(0, 10);
  const easing = [...districts].filter((r) => r.pressure_delta !== null).sort((a, b) => n(a.pressure_delta) - n(b.pressure_delta)).slice(0, 5);

  document.getElementById("pulseNarrative").innerHTML = `
    <div class="callout">
      <strong>Monitoring read:</strong>
      ${prev ? `For ${escapeHtml(selectedWeek)}, active inventory moved ${fmtSigned(activeDelta)}, price-cut rate moved ${fmtPp(cutRateDelta)}, and stale-share moved ${fmtPp(staleDelta)} versus the prior snapshot.` : "This is the first snapshot for the selected segment, so movement signals are not yet available."}
      This tab is district-level only: deterioration is rising pressure, current pressure is the highest composite stress score, and easing is falling pressure.
    </div>`;

  table("pulseDeteriorationTable", topDeterioration, monitorColumns("district"), { compact: true });
  table("pulsePressureTable", topPressure, monitorColumns("district"), { compact: true });
  table("pulseEasingTable", easing, monitorColumns("district"), { compact: true });

  const labels = marketRows.map((r) => r.snapshot_week_id);
  lineChart("pulseRatesChart", labels, [
    { label: "Price-cut rate %", data: marketRows.map((_, idx) => idx === 0 ? null : n(marketRows[idx].price_cut_rate) * 100) },
    { label: "Stale 60d %", data: marketRows.map((r) => n(r.stale_60d_share) * 100) },
  ], { beginAtZero: true });

  barChart("pulseDistrictChart", topDeterioration.map((r) => r.district_text), [
    { label: "Pressure-score WoW change", data: topDeterioration.map((r) => n(r.pressure_delta)) },
  ], { horizontal: true });
}

function monitorColumns(level) {
  const nameKey = level === "region" ? "region_text" : level === "district" ? "district_text" : "project_name";
  const cols = [
    { key: nameKey, label: level === "region" ? "Region" : level === "district" ? "District" : "Project / postal group" },
  ];
  if (level === "project") cols.push({ key: "district_text", label: "District" });
  cols.push(
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW", num: true, format: (v) => directionBadge(v) },
    { key: "pressure_rank", label: "Rank", num: true, format: (v, r) => r.rank_move ? `#${fmtNum(v)} (${fmtSigned(r.rank_move)})` : `#${fmtNum(v)}` },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "price_cut_rate", label: "Cut %", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Stale %", num: true, format: fmtPct },
    { key: "confidence", label: "Confidence", format: (v) => confidenceBadge(v) },
    { key: "reason", label: "Why flagged", format: (_, r) => escapeHtml(signalReasons(r)) },
  );
  return cols;
}

function renderWatchlist() {
  const projects = currentProjects();
  const candidateProjects = projects.filter((r) => n(r.active_listings) >= 3);

  const newStress = candidateProjects
    .filter((r) => n(r.pressure_percentile) >= 0.85 && (r.prev_pressure_rank === null || n(r.rank_move) >= 10 || n(r.pressure_delta) >= 8))
    .sort((a, b) => n(b.pressure_delta) - n(a.pressure_delta))
    .slice(0, 15);
  const persistent = candidateProjects
    .filter((r) => n(r.pressure_percentile) >= 0.9 && r.prev_pressure_rank !== null && n(r.prev_pressure_rank) <= Math.max(10, candidateProjects.length * 0.1))
    .sort((a, b) => n(b.pressure_score) - n(a.pressure_score))
    .slice(0, 15);
  const deterioration = [...candidateProjects]
    .filter((r) => r.pressure_delta !== null)
    .sort((a, b) => n(b.pressure_delta) - n(a.pressure_delta))
    .slice(0, 15);
  const easing = [...candidateProjects]
    .filter((r) => r.pressure_delta !== null)
    .sort((a, b) => n(a.pressure_delta) - n(b.pressure_delta))
    .slice(0, 15);
  const lowConfidence = candidateProjects
    .filter((r) => r.confidence === "Low" && (n(r.pressure_percentile) >= 0.9 || n(r.pressure_delta) >= 8))
    .sort((a, b) => n(b.pressure_score) - n(a.pressure_score))
    .slice(0, 15);

  table("newStressTable", newStress, monitorColumns("project"), { compact: true });
  table("persistentStressTable", persistent, monitorColumns("project"), { compact: true });
  table("deteriorationTable", deterioration, monitorColumns("project"), { compact: true });
  table("easingTable", easing, monitorColumns("project"), { compact: true });
  table("lowConfidenceTable", lowConfidence, monitorColumns("project"), { compact: true });
}

function renderMetrics() {
  renderPulse();
}

function renderOverview() {
  const marketRows = currentMarketRows();
  const regionRows = currentRegions();
  const labels = marketRows.map((r) => r.snapshot_week_id);
  const regionTrend = regionTrendContext();
  lineChart("activeInventoryChart", labels, [
    { label: "Active listings", data: marketRows.map((r) => n(r.active_listings)) },
  ]);
  lineChart("lifecycleChart", labels, [
    { label: "New", data: marketRows.map((_, idx) => movementValue(marketRows, idx, "new_listings")) },
    { label: "Disappeared", data: marketRows.map((_, idx) => movementValue(marketRows, idx, "disappeared_listings")) },
    { label: "Price cuts", data: marketRows.map((_, idx) => movementValue(marketRows, idx, "price_cut_listings")) },
  ], { beginAtZero: true });
  lineChart("ratesChart", labels, [
    { label: "Price-cut rate %", data: marketRows.map((_, idx) => idx === 0 ? null : n(marketRows[idx].price_cut_rate) * 100) },
    { label: "Stale 60d %", data: marketRows.map((r) => n(r.stale_60d_share) * 100) },
  ], { beginAtZero: true });
  lineChart("pricingChart", labels, [
    { label: "Avg PSF", data: marketRows.map((r) => n(r.avg_psf)) },
  ]);

  lineChart("regionActiveInventoryChart", regionTrend.labels, regionTrendSeries(regionTrend, "active_listings"));
  lineChart("regionPricingChart", regionTrend.labels, regionTrendSeries(regionTrend, "avg_psf"));
  lineChart("regionCutRateChart", regionTrend.labels.slice(1), regionTrendSeries(regionTrend, "price_cut_rate", (value) => n(value) * 100).map((series) => ({
    ...series,
    data: series.data.slice(1),
  })), { beginAtZero: true });
  lineChart("regionStaleRateChart", regionTrend.labels, regionTrendSeries(regionTrend, "stale_60d_share", (value) => n(value) * 100), { beginAtZero: true });

  table("regionTable", regionRows, [
    { key: "region_text", label: "Region" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW score", num: true, format: (v) => directionBadge(v) },
    { key: "pressure_rank", label: "Rank", num: true, format: (v, r) => r.rank_move ? `#${fmtNum(v)} (${fmtSigned(r.rank_move)})` : `#${fmtNum(v)}` },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "active_delta", label: "Active Δ", num: true, format: fmtSigned },
    { key: "price_cut_rate", label: "Cut %", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Stale 60d %", num: true, format: fmtPct },
    { key: "duplicate_candidate_listings", label: "Dupes", num: true, format: fmtNum },
    { key: "distinct_districts", label: "Districts", num: true, format: fmtNum },
    { key: "confidence", label: "Confidence", format: confidenceBadge },
    { key: "reason", label: "Read", format: (_, r) => escapeHtml(signalReasons(r)) },
  ]);

  table("districtTable", currentDistricts(), [
    { key: "district_text", label: "District" },
    { key: "region_text", label: "Region" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW score", num: true, format: (v) => directionBadge(v) },
    { key: "pressure_rank", label: "Rank", num: true, format: (v, r) => r.rank_move ? `#${fmtNum(v)} (${fmtSigned(r.rank_move)})` : `#${fmtNum(v)}` },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "active_delta", label: "Active Δ", num: true, format: fmtSigned },
    { key: "price_cut_rate", label: "Cut %", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Stale 60d %", num: true, format: fmtPct },
    { key: "duplicate_candidate_listings", label: "Dupes", num: true, format: fmtNum },
    { key: "confidence", label: "Confidence", format: confidenceBadge },
    { key: "reason", label: "Read", format: (_, r) => escapeHtml(signalReasons(r)) },
  ]);

  table("projectTable", currentProjects(), [
    { key: "project_name", label: "Project / postal group" },
    { key: "project_group_type", label: "Level" },
    { key: "postal_code", label: "Postal" },
    { key: "district_text", label: "District" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW score", num: true, format: (v) => directionBadge(v) },
    { key: "district_gap", label: "Benchmark", num: true, format: (_, r) => projectVsDistrict(r) },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "price_cut_rate", label: "Cut %", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Stale 60d %", num: true, format: fmtPct },
    { key: "top_agent_share", label: "Top agent %", num: true, format: fmtPct },
    { key: "confidence", label: "Confidence", format: confidenceBadge },
    { key: "reason", label: "Read", format: (_, r) => escapeHtml(signalReasons(r)) },
  ]);
}

function projectLabel(row) {
  const postal = row.project_group_type === "postal_code" && row.postal_code ? `, postal ${row.postal_code}` : "";
  return `${row.project_name}${postal} (${fmtNum(row.active_listings)} active, score ${fmtNum(row.pressure_score, 1)})`;
}

function projectSearchText(row) {
  return [row.project_name, row.postal_code, row.district_text, row.region_text]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function renderProjectOptions({ open = false } = {}) {
  const select = document.getElementById("projectSelect");
  const search = document.getElementById("projectSearch");
  const query = search.value.trim().toLowerCase();
  const tokens = query.split(/\s+/).filter(Boolean);
  const rows = tokens.length
    ? projectOptionRows.filter((row) => tokens.every((token) => projectSearchText(row).includes(token)))
    : projectOptionRows;
  select.innerHTML = rows.map((r) => `<option value="${escapeHtml(r.project_uid)}">${escapeHtml(projectLabel(r))}</option>`).join("");
  select.classList.toggle("open", open && rows.length > 0);
  if (!rows.length) {
    selectedProjectUid = null;
    document.getElementById("projectCards").innerHTML = `<p class="subtle">No matching projects.</p>`;
    return;
  }
  if (!rows.some((r) => r.project_uid === selectedProjectUid)) {
    selectedProjectUid = rows[0].project_uid;
  }
  select.value = selectedProjectUid;
  renderProjectDetail();
}

function hideProjectOptionsSoon() {
  window.setTimeout(() => document.getElementById("projectSelect").classList.remove("open"), 150);
}

function setupProjectSelect() {
  const search = document.getElementById("projectSearch");
  const select = document.getElementById("projectSelect");
  projectOptionRows = currentProjects().filter((r) => r.project_uid);
  if (!projectOptionRows.some((r) => r.project_uid === selectedProjectUid)) {
    selectedProjectUid = projectOptionRows[0]?.project_uid ?? null;
  }
  search.onfocus = () => renderProjectOptions({ open: true });
  search.oninput = () => renderProjectOptions({ open: true });
  search.onblur = hideProjectOptionsSoon;
  select.onblur = hideProjectOptionsSoon;
  select.onchange = () => {
    selectedProjectUid = select.value;
    search.value = "";
    select.classList.remove("open");
    renderProjectDetail();
  };
  renderProjectOptions();
}

function renderProjectDetail() {
  const projectUid = selectedProjectUid;
  const trend = enrichRows(data.projectTrends, (r) => r.project_uid, "project")
    .filter((r) => r.project_uid === projectUid && r.listing_type === selectedType && r.property_segment === selectedSegment);
  if (!trend.length) return;
  const latest = trend[trend.length - 1];
  document.getElementById("projectCards").innerHTML = [
    metric("Pressure", fmtNum(latest.pressure_score, 1), latest.pressure_delta === null ? "first observed" : `${fmtSigned(latest.pressure_delta, 1)} WoW`),
    metric("Rank", latest.pressure_rank ? `#${fmtNum(latest.pressure_rank)}` : "—", latest.rank_move ? `${fmtSigned(latest.rank_move)} places` : ""),
    metric("Confidence", confidenceBadge(latest.confidence), `${fmtNum(latest.active_listings)} active listings`),
    metric("Vs district", projectVsDistrict(latest), "pressure-score gap"),
    metric("Stale 60d", fmtPct(latest.stale_60d_share), latest.stale_delta === null ? "" : `${fmtPp(latest.stale_delta)} WoW`),
  ].join("");
  document.getElementById("projectRead").innerHTML = `<div class="callout"><strong>Read:</strong> ${escapeHtml(signalReasons(latest))}. Confidence is ${escapeHtml(latest.confidence.toLowerCase())}; use low-confidence rows as leads, not conclusions.</div>`;

  const labels = trend.map((r) => r.snapshot_week_id);
  lineChart("projectActiveChart", labels, [
    { label: "Active listings", data: trend.map((r) => n(r.active_listings)) },
  ]);
  lineChart("projectLifecycleChart", labels, [
    { label: "New", data: trend.map((_, idx) => movementValue(trend, idx, "new_listings")) },
    { label: "Disappeared", data: trend.map((_, idx) => movementValue(trend, idx, "disappeared_listings")) },
    { label: "Cuts", data: trend.map((_, idx) => movementValue(trend, idx, "price_cut_listings")) },
  ], { beginAtZero: true });
  lineChart("projectPressureChart", labels, [
    { label: "Pressure", data: trend.map((r) => n(r.pressure_score)) },
  ]);
  lineChart("projectRatesChart", labels, [
    { label: "Price-cut rate %", data: trend.map((r) => n(r.price_cut_rate) * 100) },
    { label: "Stale 60d %", data: trend.map((r) => n(r.stale_60d_share) * 100) },
  ], { beginAtZero: true });
  lineChart("projectPricingChart", labels, [
    { label: "Avg PSF", data: trend.map((r) => n(r.avg_psf)) },
  ]);

  table("projectTrendTable", trend, [
    { key: "snapshot_week_id", label: "Week" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW score", num: true, format: (v) => directionBadge(v) },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "new_listings", label: "New", num: true, format: fmtMovement },
    { key: "disappeared_listings", label: "Gone", num: true, format: fmtMovement },
    { key: "price_cut_rate", label: "Cut %", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Stale %", num: true, format: fmtPct },
    { key: "avg_psf", label: "Avg PSF", num: true, format: fmtNum },
    { key: "reason", label: "Read", format: (_, r) => escapeHtml(signalReasons(r)) },
  ]);
}

function renderEventTables() {
  table("cutsTable", byWeek(data.priceCuts), [
    { key: "project_name", label: "Project" },
    { key: "district_text", label: "District" },
    { key: "bedrooms", label: "Beds", num: true, format: fmtNum },
    { key: "floor_area_sqft", label: "Sqft", num: true, format: fmtNum },
    { key: "prior_price_value", label: "Prior price", num: true, format: fmtNum },
    { key: "price_value", label: "Price", num: true, format: fmtNum },
    { key: "price_change_pct", label: "Change %", num: true, format: (v) => fmtPct(v, 2) },
    { key: "quality_flag", label: "Flag" },
    { key: "age_days", label: "Age days", num: true, format: fmtNum },
  ]);

  table("duplicatesTable", byWeek(data.duplicates), [
    { key: "project_name", label: "Project" },
    { key: "district_text", label: "District" },
    { key: "bedrooms", label: "Beds", num: true, format: fmtNum },
    { key: "area_bucket_sqft", label: "Area bucket", num: true, format: fmtNum },
    { key: "price_bucket", label: "Price bucket", num: true, format: fmtNum },
    { key: "agent_id", label: "Agent" },
    { key: "candidate_listing_count", label: "Listings", num: true, format: fmtNum },
    { key: "avg_psf", label: "Avg PSF", num: true, format: fmtNum },
  ]);

  table("agentsTable", byWeek(data.agents), [
    { key: "project_name", label: "Project" },
    { key: "district_text", label: "District" },
    { key: "agent_id", label: "Agent" },
    { key: "agent_license", label: "Licence" },
    { key: "agency_id", label: "Agency" },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "price_cut_listings", label: "Cuts", num: true, format: fmtNum },
    { key: "avg_psf", label: "Avg PSF", num: true, format: fmtNum },
  ]);

  table("metadataTable", [data.metadata], Object.keys(data.metadata).map((key) => ({ key, label: key })));
  table("coverageTable", data.market, [
    { key: "listing_type", label: "Type" },
    { key: "property_segment", label: "Segment" },
    { key: "snapshot_week_id", label: "Week" },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "new_listings", label: "New", num: true, format: fmtMovement },
    { key: "disappeared_listings", label: "Gone", num: true, format: fmtMovement },
    { key: "price_cut_listings", label: "Cuts", num: true, format: fmtMovement },
    { key: "distinct_projects", label: "Projects", num: true, format: fmtNum },
    { key: "distinct_agents", label: "Agents", num: true, format: fmtNum },
  ]);
}

function renderAll() {
  renderMetrics();
  renderWatchlist();
  renderOverview();
  setupProjectSelect();
  renderEventTables();
}

async function init() {
  setupTabs();
  const res = await fetch("assets/dashboard-data.json?v=20260503-region-charts-fix", { cache: "no-store" });
  data = await res.json();
  selectedWeek = data.latestWeek;
  const weekSelect = document.getElementById("weekSelect");
  weekSelect.innerHTML = data.weeks.map((w) => `<option value="${w.snapshot_week_id}">${w.snapshot_week_id}</option>`).join("");
  weekSelect.value = selectedWeek;
  const typeSelect = document.getElementById("typeSelect");
  const segmentSelect = document.getElementById("segmentSelect");
  typeSelect.value = selectedType;
  segmentSelect.value = selectedSegment;
  weekSelect.addEventListener("change", () => {
    selectedWeek = weekSelect.value;
    renderAll();
  });
  typeSelect.addEventListener("change", () => {
    selectedType = typeSelect.value;
    renderAll();
  });
  segmentSelect.addEventListener("change", () => {
    selectedSegment = segmentSelect.value;
    renderAll();
  });
  renderAll();
}

init().catch((error) => {
  document.body.innerHTML = `<main><article class="box"><h1>Failed to load dashboard</h1><pre>${error.stack || error}</pre></article></main>`;
});
