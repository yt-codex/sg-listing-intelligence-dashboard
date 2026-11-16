const fmt = new Intl.NumberFormat("en-SG");
const money = new Intl.NumberFormat("en-SG", { maximumFractionDigits: 0 });
let data;
let charts = {};
let selectedWeek;
let selectedType = "SALE";
let selectedSegment = "PRIVATE_NON_LANDED";
let projectOptionRows = [];
let selectedProjectUid = null;
let districtPolygons;
let manifest;
let activeTab = "executive";
let renderGeneration = 0;
let projectGeneration = 0;
let weekDataLoaded = null;
let diagnosticsLoaded = null;
let initialData;
const resourceCache = new Map();

async function fetchJson(path) {
  // Small bounded cache; a long archive browsing session must not accumulate all history.
  if (resourceCache.has(path)) return resourceCache.get(path);
  const response = await fetch(path, { cache: "no-cache" });
  if (!response.ok) throw new Error(`Data request failed (${response.status}): ${path}`);
  const payload = expandCompactProjectHistory(await response.json());
  if (resourceCache.size >= 8) resourceCache.delete(resourceCache.keys().next().value);
  resourceCache.set(path, payload);
  return payload;
}
function expandCompactProjectHistory(payload) {
  const section = payload?.projectTrends;
  if (section && !Array.isArray(section)) {
    if (!Array.isArray(section.columns) || !Array.isArray(section.rows) ||
        section.columns.some((key) => typeof key !== "string" || ["__proto__", "constructor", "prototype"].includes(key)) ||
        new Set(section.columns).size !== section.columns.length ||
        section.rows.some((row) => !Array.isArray(row) || row.length !== section.columns.length)) {
      throw new Error("Invalid project-history data schema");
    }
    payload.projectTrends = section.rows.map((row) => Object.fromEntries(section.columns.map((key, index) => [key, row[index]])));
  }
  return payload;
}

function showStatus(message, isError = false) {
  const el = document.getElementById("loadStatus");
  el.textContent = message;
  el.classList.toggle("load-error", isError);
}
function marketFor(row) {
  return data.market.find((m) => m.snapshot_week_id === row.snapshot_week_id && m.listing_type === row.listing_type && m.property_segment === row.property_segment) || {};
}
function comparable(row, rate = false) {
  const key = rate ? "rate_comparison_eligible" : "comparison_eligible";
  return (row[key] ?? marketFor(row)[key]) === true || (row[key] ?? marketFor(row)[key]) === 1;
}
function validIdentity(row) {
  return row.project_group_type !== "listing_title" && !/^0+$/.test(String(row.postal_code || "")) && !/^postal\s+0+$/i.test(row.project_name || "");
}
function observedShare(row) {
  return n(row.active_listings) ? n(row.observed_active_listings) / n(row.active_listings) : null;
}
function snapshotDays(a, b) {
  const da = parseSnapshotDate(a?.snapshot_date || a?.snapshot_week_id?.replace("sg-week-", ""));
  const db = parseSnapshotDate(b?.snapshot_date || b?.snapshot_week_id?.replace("sg-week-", ""));
  return da && db ? Math.round((da - db) / 86400000) : null;
}

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

function weekLabel(weekId) {
  const row = data?.weeks?.find((w) => w.snapshot_week_id === weekId);
  const raw = row?.snapshot_date || String(weekId || "").replace("sg-week-", "");
  const date = new Date(`${raw}T00:00:00`);
  return Number.isNaN(date.getTime()) ? String(weekId || "") : date.toLocaleDateString("en-SG", { month: "short", day: "numeric" });
}

function chartLabels(rowsOrWeekIds) {
  return rowsOrWeekIds.map((row) => String(typeof row === "string" ? row : row.snapshot_date || row.snapshot_week_id).replace("sg-week-", "").slice(0, 10));
}

function parseSnapshotDate(value) {
  if (!value) return null;
  const date = new Date(`${String(value).slice(0, 10)}T00:00:00Z`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function earliestSnapshotDate() {
  if (data?.metadata?.first_snapshot_date) return parseSnapshotDate(data.metadata.first_snapshot_date);
  const dates = (data?.weeks || []).map((week) => parseSnapshotDate(week.snapshot_date)).filter(Boolean);
  if (!dates.length) return null;
  return new Date(Math.min(...dates.map((date) => date.getTime())));
}

function isStale60dMature(rowOrWeekId) {
  const firstDate = parseSnapshotDate(data?.metadata?.first_snapshot_date);
  if (!firstDate) return true;
  const week = typeof rowOrWeekId === "string"
    ? data?.weeks?.find((item) => item.snapshot_week_id === rowOrWeekId)
    : rowOrWeekId;
  const snapshotDate = parseSnapshotDate(week?.snapshot_date);
  if (!snapshotDate) return true;
  const daysSinceFirstSnapshot = (snapshotDate - firstDate) / (1000 * 60 * 60 * 24);
  return daysSinceFirstSnapshot >= 60;
}

function stale60dChartValue(row, multiplier = 1) {
  return isStale60dMature(row) ? n(row?.stale_60d_share) * multiplier : null;
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
  // Categorical charts otherwise join across absent weeks as if nothing was missed.
  const expandedLabels = [];
  const expandedSeries = series.map((item) => ({ ...item, data: [] }));
  labels.forEach((label, idx) => {
    const date = parseSnapshotDate(label);
    const prior = idx ? parseSnapshotDate(labels[idx - 1]) : null;
    if (date && prior && date - prior > 8 * 86400000) {
      expandedLabels.push("Missing snapshots");
      expandedSeries.forEach((item) => item.data.push(null));
    }
    expandedLabels.push(label);
    expandedSeries.forEach((item, j) => item.data.push(series[j].data[idx]));
  });
  labels = expandedLabels;
  series = expandedSeries;
  destroyChart(id);
  const ctx = document.getElementById(id);
  if (!ctx) return;
  if (typeof Chart === "undefined") { ctx.outerHTML = `<p class="subtle">Chart library unavailable; numerical tables remain available.</p>`; return; }
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
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "bottom" } },
      scales: {
        x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 7 } },
        y: { beginAtZero: options.beginAtZero ?? false },
      },
    },
  });
}

function barChart(id, labels, series, options = {}) {
  destroyChart(id);
  const ctx = document.getElementById(id);
  if (!ctx) return;
  if (typeof Chart === "undefined") { ctx.outerHTML = `<p class="subtle">Chart library unavailable; numerical tables remain available.</p>`; return; }
  charts[id] = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets: series.map((s) => ({ label: s.label, data: s.data, borderWidth: 1 })) },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: options.horizontal ? "y" : "x",
      plugins: { legend: { position: "bottom" } },
      scales: {
        x: { beginAtZero: true, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 7 } },
        y: { beginAtZero: true },
      },
    },
  });
}

function movementValue(rows, idx, key) {
  return idx === 0 || !comparable(rows[idx]) ? null : n(rows[idx][key]);
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
          const value = c.format ? c.format(raw, row) : escapeHtml(raw ?? "—");
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
      activeTab = button.dataset.tab;
      document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b === button));
      document.querySelectorAll(".panel").forEach((p) => p.classList.toggle("active", p.id === activeTab));
      renderAll();
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
  byKey.forEach((group) => group.sort((a, b) => a.snapshot_week_id.localeCompare(b.snapshot_week_id)));

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
    const pressureDelta = prev && comparable(row, true) && snapshotDays(row, prev) === 7 ? n(row.pressure_score) - n(prev.pressure_score) : null;
    const activeDelta = prev && comparable(row) && snapshotDays(row, prev) === 7 ? n(row.active_listings) - n(prev.active_listings) : null;
    const cutRateDelta = prev && comparable(row, true) && snapshotDays(row, prev) === 7 ? n(row.price_cut_rate) - n(prev.price_cut_rate) : null;
    const staleDelta = prev && comparable(row) && snapshotDays(row, prev) === 7 ? n(row.stale_60d_share) - n(prev.stale_60d_share) : null;
    const duplicateDelta = prev ? n(row.duplicate_candidate_listings) - n(prev.duplicate_candidate_listings) : null;
    const rank = ranks.get(row.__key) ?? null;
    const prevRank = prev ? (prevRanks.get(row.__key) ?? null) : null;
    const rankMove = pressureDelta !== null && rank && prevRank ? prevRank - rank : null;
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
  if (!comparable(row, true) || (level === "project" && !validIdentity(row))) return "Low";
  const market = marketFor(row);
  if (n(market.imputed_active_listings) > 0 || (level === "project" && n(row.stale_60d_share) >= 0.999)) return "Low";
  // Complete crawl coverage is not certified by a listing-count gate alone.
  if (market.crawl_complete !== true) {
    const min = level === "project" ? 12 : level === "district" ? 40 : 200;
    return n(row.active_listings) >= min ? "Medium" : "Low";
  }
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

function tagList(tags) {
  return `<span class="tag-list">${tags.map((tag) => badge(tag.text, tag.tone)).join("")}</span>`;
}

function directionBadge(value, suffix = "") {
  if (value === null || value === undefined) return badge("not comparable", "neutral");
  if (n(value) > 0) return badge(`▲ ${fmtNum(value, 1)}${suffix}`, "bad");
  if (n(value) < 0) return badge(`▼ ${fmtNum(Math.abs(value), 1)}${suffix}`, "good");
  return badge("flat", "neutral");
}

function directionGlyph(value) {
  if (value === null || value === undefined) return "•";
  if (n(value) > 0) return "▲";
  if (n(value) < 0) return "▼";
  return "•";
}

function signalReasons(row) {
  const reasons = [];
  if (row.pressure_delta !== null && n(row.pressure_delta) >= 8) reasons.push(`pressure +${fmtNum(row.pressure_delta, 1)}`);

  if (row.cut_rate_delta !== null && n(row.cut_rate_delta) >= 0.02) reasons.push(`cut rate ${fmtPp(row.cut_rate_delta)}`);
  if (row.stale_delta !== null && n(row.stale_delta) >= 0.03) reasons.push(`stale share ${fmtPp(row.stale_delta)}`);

  if (!comparable(row, true)) reasons.push("weekly comparison withheld");
  if (!reasons.length && n(row.pressure_score) >= 60) reasons.push("high pressure level");
  if (!reasons.length && row.pressure_delta !== null && n(row.pressure_delta) < 0) reasons.push(`pressure easing ${fmtNum(row.pressure_delta, 1)}`);
  if (!reasons.length) reasons.push("monitor for next snapshot");
  return reasons.slice(0, 3).join("; ");
}

function signalReasonTags(row) {
  const tags = [];
  if (row.pressure_delta !== null && n(row.pressure_delta) >= 8) tags.push({ text: "pressure jump", tone: "bad" });

  if (row.cut_rate_delta !== null && n(row.cut_rate_delta) >= 0.02) tags.push({ text: "cut-rate spike", tone: "warn" });
  if (row.stale_delta !== null && n(row.stale_delta) >= 0.03) tags.push({ text: "aging spike", tone: "warn" });

  if (n(row.pressure_score) >= 60) tags.push({ text: "score ≥60", tone: "warn" });
  if (!comparable(row, true)) tags.push({ text: "comparison withheld", tone: "low" });
  if (row.confidence === "Low") tags.push({ text: "low base", tone: "low" });
  if (!tags.length && row.pressure_delta !== null && n(row.pressure_delta) < 0) tags.push({ text: "easing", tone: "good" });
  if (!tags.length) tags.push({ text: "watch", tone: "neutral" });
  return tags.slice(0, 4);
}

function isUnknownDistrict(row) {
  const text = String(row.district_text || "").trim().toLowerCase();
  return !text || text === "unknown district" || text.includes("unknown");
}

function hasStaleSignal(rows) {
  return rows.some((row) => isStale60dMature(row) && (n(row.stale_60d_share) > 0 || n(row.stale_delta) !== 0));
}

function rateSeries(rows) {
  const series = [
    { label: "Price-cut rate %", data: rows.map((row) => comparable(row) ? n(row.price_cut_rate) * 100 : null) },
  ];
  if (hasStaleSignal(rows)) {
    series.push({ label: "Stale 60d %", data: rows.map((r) => stale60dChartValue(r, 100)) });
  }
  return series;
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
    .filter((r) => r.snapshot_week_id === selectedWeek && validIdentity(r))
    .sort((a, b) => n(b.pressure_score) - n(a.pressure_score));
}

function projectDescriptorMap() {
  const descriptors = new Map();
  const keyFor = (row) => [row.project_uid, row.listing_type, row.property_segment].join("|");
  selectedRows(data.projects || []).forEach((row) => {
    const key = keyFor(row);
    if (!descriptors.has(key) || row.snapshot_week_id === selectedWeek) {
      descriptors.set(key, {
        project_name: row.project_name,
        project_group_type: row.project_group_type,
        district_code: row.district_code,
        district_text: row.district_text,
        region_text: row.region_text,
        postal_code: row.postal_code,
      });
    }
  });
  return descriptors;
}

function hydratedProjectTrends() {
  const descriptors = projectDescriptorMap();
  return (data.projectTrends || []).map((row) => ({
    ...descriptors.get([row.project_uid, row.listing_type, row.property_segment].join("|")),
    ...row,
  }));
}

function currentMarketRows() {
  return selectedRows(data.market).sort((a, b) => a.snapshot_week_id.localeCompare(b.snapshot_week_id));
}

function regionTrendContext() {
  const rows = selectedRows(data.regions || []);
  const current = rows
    .filter((r) => r.snapshot_week_id === selectedWeek)
    .sort((a, b) => n(b.active_listings) - n(a.active_listings));
  const regionNames = current.map((r) => r.region_text).slice(0, 10);
  const rowMap = new Map(rows.map((r) => [`${r.snapshot_week_id}|${r.region_text}`, r]));
  return {
    labels: data.weeks.filter((w) => w.snapshot_week_id <= selectedWeek).map((w) => w.snapshot_week_id),
    regionNames,
    rowMap,
  };
}

function regionTrendSeries(context, key, transform = (value) => n(value)) {
  return context.regionNames.map((region) => ({
    label: region,
    data: context.labels.map((week) => {
      const row = context.rowMap.get(`${week}|${region}`);
      return row && row[key] != null ? transform(row[key], row) : null;
    }),
  }));
}

function marketCurrentAndPrev() {
  const rows = currentMarketRows();
  const idx = rows.findIndex((r) => r.snapshot_week_id === selectedWeek);
  return { current: rows[idx] || {}, prev: idx > 0 ? rows[idx - 1] : null, rows };
}

function renderPulse() {
  const { current, prev, rows } = marketCurrentAndPrev();
  const marketRows = rows.filter((r) => r.snapshot_week_id <= selectedWeek);
  const districts = currentDistricts();
  const signalDistricts = districts.filter((r) => !isUnknownDistrict(r));
  const unmappedListings = districts.filter(isUnknownDistrict).reduce((sum, row) => sum + n(row.active_listings), 0);
  const risingDistricts = signalDistricts.filter((r) => n(r.pressure_delta) >= 5).length;
  const highPressureDistricts = signalDistricts.filter((r) => alertState(r) === "Persistent").length;
  const activeDelta = prev && comparable(current) ? n(current.active_listings) - n(prev.active_listings) : null;
  const cutRateDelta = prev && comparable(current, true) ? n(current.price_cut_rate) - n(prev.price_cut_rate) : null;
  const staleDelta = prev && comparable(current) ? n(current.stale_60d_share) - n(prev.stale_60d_share) : null;
  const staleActive = hasStaleSignal(marketRows);

  document.getElementById("metricCards").innerHTML = [
    metric("Active inventory", fmt.format(n(current.active_listings)), activeDelta === null ? "comparison unavailable" : `${fmtSigned(activeDelta)} WoW`),
    metric("Price-cut rate", fmtPct(current.price_cut_rate), cutRateDelta === null ? "comparison unavailable" : `${fmtPp(cutRateDelta)} WoW`),
    staleActive
      ? metric("Stale 60d share", fmtPct(current.stale_60d_share), staleDelta === null ? "comparison unavailable" : `${fmtPp(staleDelta)} WoW`)
      : metric("Listing-age signal", "Not active", "stale-60d is all zero in current extracts"),
    metric("Rising districts", comparable(current, true) ? fmtNum(risingDistricts) : "—", "requires comparable weekly observations"),
    metric("Persistent district signals", comparable(current, true) ? fmtNum(highPressureDistricts) : "—", "fixed threshold; comparable observations"),
  ].join("");

  const topDeterioration = [...signalDistricts].filter((r) => r.pressure_delta !== null && n(r.pressure_delta) >= 5).sort((a, b) => n(b.pressure_delta) - n(a.pressure_delta)).slice(0, 5);
  const topPressure = signalDistricts.slice(0, 10);
  const easing = [...signalDistricts].filter((r) => r.pressure_delta !== null && n(r.pressure_delta) <= -5).sort((a, b) => n(a.pressure_delta) - n(b.pressure_delta)).slice(0, 5);

  const moverText = topDeterioration.length
    ? `Watch ${topDeterioration.slice(0, 2).map((r) => escapeHtml(r.district_text)).join(" and ")} for the sharpest district deterioration.`
    : comparable(current, true) ? "No mapped district meets the material-deterioration rule." : "District direction is withheld where comparisons are not eligible.";
  const staleText = staleActive ? "" : " The stale-60d extract is currently all zero, so aging should not drive interpretation this week.";
  const unmappedText = unmappedListings ? ` ${fmtNum(unmappedListings)} active listings are unmapped/unknown district and are excluded from ranked district signals.` : "";

  document.getElementById("pulseNarrative").innerHTML = `
    <div class="callout">
      <strong>Monitoring read:</strong>
      ${escapeHtml(marketRead(current, prev))}
      ${moverText}${staleText}${unmappedText}
    </div>`;

  table("pulseDeteriorationTable", topDeterioration, monitorColumns("district"), { compact: true });
  table("pulsePressureTable", topPressure, monitorColumns("district"), { compact: true });
  table("pulseEasingTable", easing, monitorColumns("district"), { compact: true });
  renderDistrictChoropleth(signalDistricts);

  const labels = chartLabels(marketRows);
  lineChart("pulseRatesChart", labels, rateSeries(marketRows), { beginAtZero: true });

  barChart("pulseDistrictChart", topDeterioration.map((r) => r.district_text), [
    { label: "Pressure-score WoW change", data: topDeterioration.map((r) => n(r.pressure_delta)) },
  ], { horizontal: true });
}

function districtHeatTone(row) {
  if (!row) return 0;
  return Math.max(0, Math.min(1, n(row.pressure_score) / 100));
}

function coordinatesForGeometry(geometry) {
  if (!geometry) return [];
  if (geometry.type === "Polygon") return geometry.coordinates;
  if (geometry.type === "MultiPolygon") return geometry.coordinates.flat();
  return [];
}

function geometryBounds(features) {
  const bounds = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
  features.forEach((feature) => {
    coordinatesForGeometry(feature.geometry).forEach((ring) => ring.forEach(([x, y]) => {
      bounds.minX = Math.min(bounds.minX, x);
      bounds.maxX = Math.max(bounds.maxX, x);
      bounds.minY = Math.min(bounds.minY, y);
      bounds.maxY = Math.max(bounds.maxY, y);
    }));
  });
  return bounds;
}

function geometryCentroid(geometry) {
  let sx = 0;
  let sy = 0;
  let count = 0;
  coordinatesForGeometry(geometry).forEach((ring) => ring.forEach(([x, y]) => {
    sx += x;
    sy += y;
    count += 1;
  }));
  return count ? [sx / count, sy / count] : [0, 0];
}

function projectPoint(x, y, bounds, width, height, padding = 18) {
  const scale = Math.min((width - padding * 2) / (bounds.maxX - bounds.minX), (height - padding * 2) / (bounds.maxY - bounds.minY));
  const usedW = (bounds.maxX - bounds.minX) * scale;
  const usedH = (bounds.maxY - bounds.minY) * scale;
  const offsetX = (width - usedW) / 2;
  const offsetY = (height - usedH) / 2;
  return [offsetX + (x - bounds.minX) * scale, height - offsetY - (y - bounds.minY) * scale];
}

function pathForGeometry(geometry, bounds, width, height) {
  const polygonPath = (rings) => rings.map((ring) => ring.map(([x, y], idx) => {
    const [px, py] = projectPoint(x, y, bounds, width, height);
    return `${idx === 0 ? "M" : "L"}${px.toFixed(1)},${py.toFixed(1)}`;
  }).join(" ") + " Z").join(" ");
  if (geometry.type === "Polygon") return polygonPath(geometry.coordinates);
  if (geometry.type === "MultiPolygon") return geometry.coordinates.map(polygonPath).join(" ");
  return "";
}

function heatColor(tone) {
  const stops = [
    [238, 242, 247],
    [133, 153, 176],
    [18, 24, 38],
  ];
  const scaled = Math.max(0, Math.min(1, tone)) * 2;
  const idx = Math.min(1, Math.floor(scaled));
  const local = scaled - idx;
  const rgb = stops[idx].map((start, i) => Math.round(start + (stops[idx + 1][i] - start) * local));
  return `rgb(${rgb.join(",")})`;
}

function renderDistrictChoropleth(rows) {
  const el = document.getElementById("districtChoropleth");
  if (!el) return;
  if (!districtPolygons?.features?.length) {
    el.innerHTML = `<p class="subtle">District geography is not available.</p>`;
    return;
  }
  const byCode = new Map(rows.map((row) => [row.district_code, row]));
  const features = districtPolygons.features;
  const bounds = geometryBounds(features);
  const width = 980;
  const height = 520;
  const paths = features.map((feature) => {
    const code = feature.properties.district_code;
    const row = byCode.get(code);
    const tone = districtHeatTone(row);
    const title = row ? `${code} ${row.district_text}: score ${fmtNum(row.pressure_score, 1)}, WoW ${fmtSigned(row.pressure_delta, 1)}, active ${fmtNum(row.active_listings)}` : `${code}: no current data`;
    const d = pathForGeometry(feature.geometry, bounds, width, height);
    return `<path d="${d}" fill="${row ? heatColor(tone) : "#f8fafc"}" class="district-shape" tabindex="0" aria-label="${escapeHtml(title)}"><title>${escapeHtml(title)}</title></path>`;
  }).join("");
  const labels = features.map((feature) => {
    const code = feature.properties.district_code;
    const row = byCode.get(code);
    if (!row) return "";
    const [cx, cy] = geometryCentroid(feature.geometry);
    const [x, y] = projectPoint(cx, cy, bounds, width, height);
    return `
      <g class="district-map-label" transform="translate(${x.toFixed(1)} ${y.toFixed(1)})">
        <text class="code">${escapeHtml(code)}</text>
      </g>`;
  }).join("");
  const caveat = escapeHtml(districtPolygons.metadata?.caveat || "Derived analytical geography, not official boundaries.");
  el.innerHTML = `
    <div class="district-map-wrap">
      <svg class="district-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Singapore district pressure choropleth">
        ${paths}
        ${labels}
      </svg>
    </div>
    <div class="heatmap-legend">
      <span>Lower pressure</span><span class="legend-ramp"></span><span>Higher pressure</span>
    </div>
    <p class="subtle map-caveat">${caveat}</p>`;
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
    { key: "pressure_rank", label: "Sample rank", num: true, format: (v, r) => r.rank_move ? `#${fmtNum(v)} (${fmtSigned(r.rank_move)})` : `#${fmtNum(v)}` },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "price_cut_rate", label: "Cut %", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Stale %", num: true, format: fmtPct },
    { key: "confidence", label: "Confidence", format: (v) => confidenceBadge(v) },
    { key: "reason", label: "Why flagged", format: (_, r) => tagList(signalReasonTags(r)) },
  );
  return cols;
}

function renderWatchlist() {
  const projects = currentProjects();
  const candidateProjects = projects.filter((r) => n(r.active_listings) >= 12 && validIdentity(r));

  const newStress = candidateProjects
    .filter((r) => alertState(r) === "New")
    .sort((a, b) => n(b.pressure_delta) - n(a.pressure_delta))
    .slice(0, 15);
  const persistent = candidateProjects
    .filter((r) => alertState(r) === "Persistent")
    .sort((a, b) => n(b.pressure_score) - n(a.pressure_score))
    .slice(0, 15);
  const deterioration = [...candidateProjects]
    .filter((r) => r.pressure_delta !== null && n(r.pressure_delta) >= 5 && r.confidence !== "Low")
    .sort((a, b) => n(b.pressure_delta) - n(a.pressure_delta))
    .slice(0, 15);
  const easing = [...candidateProjects]
    .filter((r) => r.pressure_delta !== null && n(r.pressure_delta) <= -5 && r.confidence !== "Low")
    .sort((a, b) => n(a.pressure_delta) - n(b.pressure_delta))
    .slice(0, 15);
  const lowConfidence = candidateProjects
    .filter((r) => r.confidence === "Low" && (n(r.pressure_score) >= 60 || n(r.pressure_delta) >= 8))
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
  const marketRows = currentMarketRows().filter((r) => r.snapshot_week_id <= selectedWeek);
  const regionRows = currentRegions();
  const labels = chartLabels(marketRows);
  const regionTrend = regionTrendContext();
  const latestDate = data.weeks.find((w) => w.snapshot_week_id === data.latestWeek)?.snapshot_date || "—";
  const builtAt = data.metadata?.built_at_utc ? `${data.metadata.built_at_utc} UTC` : "—";
  document.getElementById("overviewFreshness").innerHTML = `
    <h2>Data freshness</h2>
    <p class="subtle explainer"><strong>Latest dashboard snapshot:</strong> ${escapeHtml(data.latestWeek || "—")} (${escapeHtml(latestDate)}). <strong>Selected snapshot:</strong> ${escapeHtml(selectedWeek || "—")}. <strong>Analytics build:</strong> ${escapeHtml(builtAt)}.</p>
  `;
  lineChart("activeInventoryChart", labels, [
    { label: "Active listings", data: marketRows.map((r) => n(r.active_listings)) },
  ]);
  lineChart("lifecycleChart", labels, [
    { label: "New", data: marketRows.map((_, idx) => movementValue(marketRows, idx, "new_listings")) },
    { label: "Disappeared", data: marketRows.map((_, idx) => movementValue(marketRows, idx, "disappeared_listings")) },
    { label: "Price cuts", data: marketRows.map((_, idx) => movementValue(marketRows, idx, "price_cut_listings")) },
  ], { beginAtZero: true });
  lineChart("ratesChart", labels, rateSeries(marketRows), { beginAtZero: true });
  lineChart("pricingChart", labels, [
    { label: "Avg PSF", data: marketRows.map((r) => r.avg_psf ?? null) },
  ]);

  lineChart("regionActiveInventoryChart", chartLabels(regionTrend.labels), regionTrendSeries(regionTrend, "active_listings"));
  lineChart("regionPricingChart", chartLabels(regionTrend.labels), regionTrendSeries(regionTrend, "avg_psf"));
  lineChart("regionCutRateChart", chartLabels(regionTrend.labels.slice(1)), regionTrendSeries(regionTrend, "price_cut_rate", (value, row) => comparable(row) ? n(value) * 100 : null).map((series) => ({
    ...series,
    data: series.data.slice(1),
  })), { beginAtZero: true });
  lineChart("regionStaleRateChart", chartLabels(regionTrend.labels), regionTrendSeries(regionTrend, "stale_60d_share", (value, row) => stale60dChartValue(row, 100)), { beginAtZero: true });

  table("regionTable", regionRows, [
    { key: "region_text", label: "Region" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW score", num: true, format: (v) => directionBadge(v) },
    { key: "pressure_rank", label: "Sample rank", num: true, format: (v, r) => r.rank_move ? `#${fmtNum(v)} (${fmtSigned(r.rank_move)})` : `#${fmtNum(v)}` },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "active_delta", label: "Active Δ", num: true, format: fmtSigned },
    { key: "price_cut_rate", label: "Cut %", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Stale 60d %", num: true, format: fmtPct },
    { key: "duplicate_candidate_listings", label: "Dupes", num: true, format: fmtNum },
    { key: "distinct_districts", label: "Districts", num: true, format: fmtNum },
    { key: "confidence", label: "Confidence", format: confidenceBadge },
    { key: "reason", label: "Read", format: (_, r) => escapeHtml(signalReasons(r)) },
  ]);

  table("districtTable", currentDistricts().filter((r) => !isUnknownDistrict(r)), [
    { key: "district_text", label: "District" },
    { key: "region_text", label: "Region" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW score", num: true, format: (v) => directionBadge(v) },
    { key: "pressure_rank", label: "Sample rank", num: true, format: (v, r) => r.rank_move ? `#${fmtNum(v)} (${fmtSigned(r.rank_move)})` : `#${fmtNum(v)}` },
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

function projectDisplayName(row) {
  const parts = [row.project_name];
  if (row.postal_code) parts.push(`postal ${row.postal_code}`);
  if (row.district_text) parts.push(row.district_text);
  return parts.filter(Boolean).join(" · ");
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
  loadProjectDetail();
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
    loadProjectDetail();
  };
  renderProjectOptions();
}

function renderProjectDetail() {
  const projectUid = selectedProjectUid;
  const trend = enrichRows(hydratedProjectTrends(), (r) => r.project_uid, "project")
    .filter((r) => r.project_uid === projectUid && r.listing_type === selectedType && r.property_segment === selectedSegment && r.snapshot_week_id <= selectedWeek);
  if (!trend.length) { document.getElementById("projectCards").textContent = "No project observations for this selection."; return; }
  const latest = trend.find((r) => r.snapshot_week_id === selectedWeek) || trend[trend.length - 1];
  const staleActive = hasStaleSignal(trend);
  document.getElementById("selectedProjectTitle").innerHTML = `<span>Selected project</span>${escapeHtml(projectDisplayName(latest))}`;
  document.getElementById("projectCards").innerHTML = [
    metric("Selected project", escapeHtml(latest.project_name), [latest.district_text, latest.postal_code ? `postal ${latest.postal_code}` : null].filter(Boolean).join(" · ")),
    metric("Pressure", fmtNum(latest.pressure_score, 1), latest.pressure_delta === null ? (latest.prev ? "change not comparable" : "first observed") : `${fmtSigned(latest.pressure_delta, 1)} WoW`),
    metric("History window", selectedWeek.slice(8, 12), "Selected calendar year; archive selector loads others"),
    metric("Confidence", confidenceBadge(latest.confidence), `${fmtNum(latest.active_listings)} active listings`),
    staleActive
      ? metric("Stale 60d", fmtPct(latest.stale_60d_share), latest.stale_delta === null ? "" : `${fmtPp(latest.stale_delta)} WoW`)
      : metric("Listing-age signal", "Not active", "stale-60d all zero"),
  ].join("");
  document.getElementById("projectRead").innerHTML = `<div class="callout"><strong>Read:</strong> ${escapeHtml(signalReasons(latest))}. Confidence is ${escapeHtml(latest.confidence.toLowerCase())}; use low-confidence rows as leads, not conclusions.</div>`;

  const labels = chartLabels(trend);
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
  lineChart("projectRatesChart", labels, rateSeries(trend), { beginAtZero: true });
  lineChart("projectPricingChart", labels, [
    { label: "Avg PSF", data: trend.map((r) => r.avg_psf ?? null) },
  ]);

  table("projectTrendTable", trend, [
    { key: "snapshot_week_id", label: "Week" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "pressure_delta", label: "WoW score", num: true, format: (v) => directionBadge(v) },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "new_listings", label: "New", num: true, format: fmtMovement },
    { key: "disappeared_listings", label: "No longer observed", num: true, format: fmtMovement },
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
    { key: "disappeared_listings", label: "No longer observed", num: true, format: fmtMovement },
    { key: "price_cut_listings", label: "Cuts", num: true, format: fmtMovement },
    { key: "observed_active_listings", label: "Observed", num: true, format: fmtNum },
    { key: "imputed_active_listings", label: "Imputed", num: true, format: fmtNum },
    { key: "comparison_days", label: "Days since prior", num: true, format: fmtNum },
    { key: "rate_comparison_eligible", label: "Weekly rate comparison", format: (v) => v ? "Eligible" : "Withheld" },
    { key: "quality_status", label: "Quality", format: escapeHtml },
  ]);
}


function alertState(row) {
  const minimum = row.level === "project" ? 12 : row.level === "region" ? 200 : 40;
  if (!comparable(row, true) || row.confidence === "Low" || !row.prev || n(row.prev.active_listings) < minimum) return "Unverified";
  if (n(row.pressure_score) >= 60 && n(row.prev.pressure_score) >= 60) return "Persistent";
  if (n(row.pressure_score) >= 60 && n(row.prev.pressure_score) < 60 && n(row.pressure_delta) >= 5) return "New";
  if (n(row.pressure_score) < 55 && n(row.prev.pressure_score) >= 60) return "Resolved";
  return "Monitor";
}

function marketRead(current, prev) {
  if (!current.snapshot_week_id) return "No observations available for this selection.";
  const date = current.snapshot_date || current.snapshot_week_id.replace("sg-week-", "");
  if (!prev) return `For ${date}, weekly market direction is withheld because the preceding observation is outside the loaded history window.`;
  if (!comparable(current, true)) {
    return `For ${date}, weekly market direction is withheld: observation windows or source coverage are not comparable. The current ${fmtPct(current.price_cut_rate)} all-advert cut share is descriptive, and a lower value does not establish easing.`;
  }
  const delta = n(current.active_listings) - n(prev?.active_listings);
  const cutDelta = n(current.price_cut_rate) - n(prev?.price_cut_rate);
  return `For ${date}, advertisements changed ${fmtSigned(delta)} and the price-cut share changed ${fmtPp(cutDelta)} against ${prev?.snapshot_date || "the previous snapshot"}. These are listing-side observations; transaction outcomes and complete portal coverage remain unverified.`;
}

function horizonChange(row, weeks) {
  const history = data.market.filter((r) => r.listing_type === row.listing_type && r.property_segment === row.property_segment && r.snapshot_week_id <= row.snapshot_week_id).sort((a, b) => a.snapshot_week_id.localeCompare(b.snapshot_week_id));
  const start = history.findIndex((r) => snapshotDays(row, r) === weeks * 7);
  if (start < 0 || history.slice(start + 1).length !== weeks || history.slice(start + 1).some((r) => !comparable(r)) || history.slice(start).some((r) => n(r.imputed_active_listings) > 0)) return "not comparable";
  const base = n(history[start].active_listings);
  return base ? `${fmtSigned((n(row.active_listings) / base - 1) * 100, 1)}%` : "unavailable";
}

function captureTime(value) {
  if (!value) return "unavailable";
  const timestamp = new Date(value);
  return Number.isNaN(timestamp.getTime()) ? "unavailable" : timestamp.toLocaleString("en-SG", { timeZone: "Asia/Singapore", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", hour12: false });
}

function renderExecutive() {
  const { current, prev } = marketCurrentAndPrev();
  const currentDate = current.snapshot_date || selectedWeek.replace("sg-week-", "");
  const todaySG = new Intl.DateTimeFormat("en-CA", {timeZone: "Asia/Singapore"}).format(new Date());
  const age = Math.floor((parseSnapshotDate(todaySG) - parseSnapshotDate(currentDate)) / 86400000);
  const observed = observedShare(current);
  const comparisonFrom = current.comparison_from || prev?.snapshot_date || "unavailable";
  document.getElementById("qualityStrip").innerHTML = `<strong>Data reliability: ${badge(comparable(current, true) ? "provisional" : "comparison restricted", "warn")}</strong><span>Snapshot ${escapeHtml(currentDate)} · ${age} calendar days old</span><span>Observed ${fmtPct(observed)} · imputed ${fmtNum(current.imputed_active_listings)}</span><span>Comparison ${escapeHtml(comparisonFrom)} → ${escapeHtml(currentDate)} (${fmtNum(current.comparison_days)} days)</span><span>Crawl completeness: unverified</span><span>Run window: ${captureTime(current.capture_started_at)} → ${captureTime(current.capture_finished_at)} SGT · ${fmtNum(current.capture_run_count)} run(s), latest ${escapeHtml(current.latest_run_status || "unavailable")}</span>`;
  document.getElementById("briefDate").textContent = `Market assessment • ${currentDate}`;
  document.getElementById("executiveRead").innerHTML = `<p class="assessment">${escapeHtml(marketRead(current, prev))}</p><p class="subtle">Scope: ${escapeHtml(document.getElementById("segmentSelect").selectedOptions[0]?.text || selectedSegment)}. Portal advertisements may include several agents advertising the same home, launch marketing and relisted advertisements.</p>`;
  document.getElementById("marketPair").innerHTML = ["SALE", "RENT"].map((type) => {
    const row = data.market.find((r) => r.snapshot_week_id === selectedWeek && r.property_segment === selectedSegment && r.listing_type === type);
    if (!row) return `<article class="box"><h2>${type === "SALE" ? "Sales" : "Rentals"}</h2><p>No observations.</p></article>`;
    return `<article class="box market-summary"><h2>${type === "SALE" ? "Sale advertisements" : "Rental advertisements"}</h2><div class="summary-number">${fmtNum(row.active_listings)}</div><p class="subtle">${fmtNum(row.observed_active_listings)} observed · ${fmtNum(row.imputed_active_listings)} imputed</p><dl><div><dt>4-week inventory change</dt><dd>${horizonChange(row, 4)}</dd></div><div><dt>13-week inventory change</dt><dd>${horizonChange(row, 13)}</dd></div><div><dt>Median asking PSF${type === "RENT" ? " / month" : ""}</dt><dd>S$${fmtNum(row.median_psf, type === "RENT" ? 2 : 0)}</dd></div><div><dt>PSF observations</dt><dd>${fmtNum(row.price_observation_count)}</dd></div><div><dt>7-day matched-listing cuts</dt><dd>${comparable(row) ? fmtPct(row.matched_price_cut_rate) : "withheld"}</dd></div><div><dt>Matched price observations</dt><dd>${fmtNum(row.matched_price_count)}</dd></div><div><dt>Observed age ≥60 days</dt><dd>${fmtPct(row.stale_60d_share)}</dd></div></dl><p class="subtle">${comparable(row, true) ? "Weekly rates are comparable under the data gate." : "Weekly rate change withheld: unequal windows or inadequate coverage."} First-observed age is not verified time on market.</p></article>`;
  }).join("");
  const history = currentMarketRows().filter((r) => r.snapshot_week_id <= selectedWeek);
  lineChart("executiveInventoryChart", chartLabels(history), [
    { label: "Observed advertisements", data: history.map((r) => r.observed_active_listings ?? null) },
    { label: "Imputed advertisements", data: history.map((r) => r.imputed_active_listings ?? null) },
  ], { beginAtZero: true });
  lineChart("executivePriceChart", chartLabels(history), [{ label: "Median asking PSF (S$)", data: history.map((r) => r.median_psf ?? null) }]);
  const districts = currentDistricts().filter((r) => !isUnknownDistrict(r));
  const alerts = districts.filter((r) => ["New", "Persistent", "Resolved"].includes(alertState(r))).sort((a, b) => Math.abs(n(b.pressure_delta)) - Math.abs(n(a.pressure_delta))).slice(0, 3);
  document.getElementById("executiveSignals").innerHTML = alerts.length ? alerts.map((row) => {
    const share = n(current.active_listings) ? n(row.active_listings) / n(current.active_listings) : null;
    const contributions = Object.entries(row.score_components || {}).map(([key, value]) => `${key.replaceAll("_", " ")}: ${fmtNum(value, 1)} points`).join("; ");
    return `<div class="signal-item"><h3>${badge(alertState(row), alertState(row) === "Resolved" ? "neutral" : "warn")} ${escapeHtml(row.district_text)}</h3><p>Score ${fmtNum(row.pressure_score, 1)} (${fmtSigned(row.pressure_delta, 1)}); ${fmtNum(row.active_listings)} advertisements (${fmtPct(share)} of selected-market inventory). ${escapeHtml(contributions)}.</p><p class="subtle">Follow-up: inspect comparable project advertisements and corroborate with transaction evidence. Reliability: ${escapeHtml(row.confidence)}; thresholds are exploratory.</p></div>`;
  }).join("") : `<p>No eligible material alerts for this selection. ${comparable(current, true) ? "No district met the fixed threshold and persistence rules." : "Comparability restrictions prevent escalation; this is not an all-clear market signal."}</p>`;
  table("executiveRegions", currentRegions(), [
    { key: "region_text", label: "Source region", format: escapeHtml },
    { key: "active_listings", label: "Advertisements", num: true, format: fmtNum },
    { key: "price_cut_rate", label: "Cut share", num: true, format: fmtPct },
    { key: "stale_60d_share", label: "Age ≥60d", num: true, format: fmtPct },
    { key: "pressure_score", label: "Fixed score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "confidence", label: "Reliability", format: confidenceBadge },
  ]);
  document.getElementById("briefProvenance").textContent = `Snapshot ${currentDate}; built ${data.metadata?.built_at_utc || "unknown"} UTC. Results may be revised after recovery or data repair. Source: PropertyGuru observations; no confirmed sale/lease outcomes. Official series not integrated.`;
  document.getElementById("methodology").innerHTML = `<p><strong>Fixed-v1 score:</strong> 70 × min(price-cut share / 10%, 1) + 30 × observed-age-60d share. It uses a fixed scale, not the week's largest value. The weights and thresholds are exploratory and have not been validated against transactions.</p><p><strong>Comparison gate:</strong> directional rate/score changes require consecutive seven-day observation windows and acceptable source quality. Missing or imputed periods are disclosed; unavailable changes are not zero. Unknown crawl completeness caps reliability at Medium. Run timestamps include downstream processing, not just page capture.</p><p><strong>Alerts:</strong> minimum sample 40 district / 12 project advertisements; score ≥60 in two comparable observations is persistent; crossing 60 with a ≥5-point increase is new; falling below 55 after ≥60 is resolved. Low-reliability groups are excluded. Project ranking is within the exported high-volume sample, not all developments. Ambiguous project/location groups are withheld (${fmtNum(data.metadata?.ambiguous_project_history_keys_withheld)} historical keys).</p><p><strong>Asking prices:</strong> medians are descriptive, not mix-adjusted. The matched-listing rate uses observed positive-price pairs exactly seven days apart; the descriptive all-advert cut share uses current advertisements as its denominator and each advert’s last observed price (which can predate the preceding snapshot). The screening score uses that descriptive share, not the matched rate. Rental advertisements are not yet reliably separated into room and whole-unit markets; launch and resale advertisements are not yet reliably separated. Project supply is not normalized by verified unit counts.</p><p><strong>Age:</strong> elapsed time since first observation, not verified time on market; repeated 100% aged project groups need investigation and receive Low reliability. An advertisement leaving observation is not evidence of a sale or lease.</p><p><strong>Confidence:</strong> a rule-based reliability label, not a statistical confidence interval. It considers comparison eligibility, imputation, identity, age anomalies and sample size. Duplicates and agent concentration are diagnostic heuristics only.</p>`;
}

function applySummary(payload) {
  for (const key of ["weeks", "market", "regions", "districts", "sourceWeekQuality"]) {
    if (payload[key]) data[key] = payload[key];
  }
  data.projects = []; data.projectTrends = []; data.priceCuts = []; data.duplicates = []; data.agents = [];
  weekDataLoaded = null; diagnosticsLoaded = null;
}

async function ensureTabData(generation) {
  const week = selectedWeek;
  if (["watchlist", "overview", "project"].includes(activeTab) && weekDataLoaded !== week) {
    const payload = await fetchJson(`assets/weeks/${week}.json`);
    if (generation !== renderGeneration) return false;
    data.projects = payload.projects || [];
    data.projectTrends = [];
    weekDataLoaded = week;
  }
  if (activeTab === "diagnostics" && diagnosticsLoaded !== week) {
    const payload = await fetchJson(`assets/diagnostics/${week}.json`);
    if (generation !== renderGeneration) return false;
    for (const key of ["priceCuts", "duplicates", "agents"]) data[key] = payload[key] || [];
    diagnosticsLoaded = week;
  }
  if (activeTab === "pulse" && !districtPolygons) {
    const payload = await fetchJson("assets/postal-districts-derived.geojson");
    if (generation !== renderGeneration) return false;
    districtPolygons = payload;
  }
  return generation === renderGeneration;
}

async function loadProjectDetail() {
  const generation = ++projectGeneration;
  const row = projectOptionRows.find((r) => r.project_uid === selectedProjectUid);
  if (!row || activeTab !== "project") return;
  document.getElementById("projectCards").innerHTML = '<p class="subtle">Loading selected project history…</p>';
  document.getElementById("selectedProjectTitle").textContent = projectDisplayName(row);
  document.getElementById("projectRead").textContent = "";
  document.getElementById("projectTrendTable").textContent = "";
  for (const id of ["projectActiveChart", "projectLifecycleChart", "projectPressureChart", "projectRatesChart", "projectPricingChart"]) destroyChart(id);
  try {
    const year = selectedWeek.replace("sg-week-", "").slice(0, 4);
    const path = row.trend_key ? `projects/${row.trend_key}/${year}.json` : row.trend_file;
    if (!path) throw new Error("No history file available for selected project.");
    const payload = await fetchJson(`assets/${path.replace(/^assets\//, "")}`);
    if (generation !== projectGeneration || activeTab !== "project") return;
    data.projectTrends = Array.isArray(payload) ? payload : payload.projectTrends || [];
    renderProjectDetail();
    showStatus("");
  } catch (error) {
    if (generation !== projectGeneration) return;
    document.getElementById("projectCards").textContent = "Project history could not be loaded. Select the project again to retry.";
    showStatus(error.message, true);
  }
}

async function renderAll() {
  const generation = ++renderGeneration;
  ++projectGeneration;
  try {
    renderExecutive();
    showStatus("Loading selected view…");
    if (!await ensureTabData(generation)) return;
    if (activeTab === "pulse") renderPulse();
    if (activeTab === "watchlist") renderWatchlist();
    if (activeTab === "overview") renderOverview();
    if (activeTab === "project") setupProjectSelect();
    if (["diagnostics", "quality"].includes(activeTab)) renderEventTables();
    showStatus("");
  } catch (error) {
    if (generation === renderGeneration) showStatus(`${error.message}. Change the view to retry.`, true);
  }
}

function fillWeekSelect(preferred = selectedWeek) {
  data.weeks.sort((a, b) => a.snapshot_week_id.localeCompare(b.snapshot_week_id));
  const select = document.getElementById("weekSelect");
  select.innerHTML = data.weeks.map((w) => `<option value="${escapeHtml(w.snapshot_week_id)}">${escapeHtml(w.snapshot_date || w.snapshot_week_id.replace("sg-week-", ""))}</option>`).join("");
  selectedWeek = data.weeks.some((w) => w.snapshot_week_id === preferred) ? preferred : data.weeks.at(-1)?.snapshot_week_id;
  select.value = selectedWeek;
}

async function init() {
  setupTabs();
  [initialData, manifest] = await Promise.all([fetchJson("assets/dashboard-data.json"), fetchJson("assets/manifest.json")]);
  data = { ...initialData };
  for (const key of ["projects", "projectTrends", "priceCuts", "duplicates", "agents"]) data[key] ||= [];
  selectedWeek = data.latestWeek;
  fillWeekSelect();
  const yearSelect = document.getElementById("yearSelect");
  const years = manifest.years || [...new Set(data.weeks.map((w) => w.snapshot_week_id.replace("sg-week-", "").slice(0, 4)))];
  yearSelect.innerHTML = '<option value="recent">Recent snapshots</option>' + years.map((y) => `<option value="${escapeHtml(y)}">${escapeHtml(y)}</option>`).join("");
  yearSelect.addEventListener("change", async () => {
    const generation = ++renderGeneration;
    const value = yearSelect.value;
    showStatus("Loading archive summary…");
    try {
      const payload = value === "recent" ? initialData : await fetchJson(`assets/years/${value}.json`);
      if (generation !== renderGeneration) return;
      applySummary(payload);
      fillWeekSelect(value === "recent" ? initialData.latestWeek : selectedWeek);
      renderAll();
    } catch (error) { if (generation === renderGeneration) showStatus(error.message, true); }
  });
  document.getElementById("weekSelect").addEventListener("change", (event) => {
    selectedWeek = event.target.value; selectedProjectUid = null;
    data.projects = []; data.projectTrends = []; weekDataLoaded = null;
    document.getElementById("projectSearch").value = "";
    renderAll();
  });
  document.getElementById("typeSelect").addEventListener("change", (event) => { selectedType = event.target.value; document.getElementById("projectSearch").value = ""; renderAll(); });
  document.getElementById("segmentSelect").addEventListener("change", (event) => { selectedSegment = event.target.value; document.getElementById("projectSearch").value = ""; renderAll(); });
  document.getElementById("printBrief").addEventListener("click", () => window.print());
  await renderAll();
}

init().catch((error) => {
  showStatus(`Failed to load dashboard: ${error.message}. Reload to retry.`, true);
});
