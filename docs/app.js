const fmt = new Intl.NumberFormat("en-SG");
const money = new Intl.NumberFormat("en-SG", { maximumFractionDigits: 0 });
let data;
let charts = {};
let selectedWeek;
let selectedType = "SALE";
let selectedSegment = "PRIVATE_NON_LANDED";

function n(value) {
  return value === null || value === undefined || Number.isNaN(Number(value)) ? 0 : Number(value);
}

function fmtNum(value, digits = 0) {
  if (value === null || value === undefined || value === "") return "—";
  const safeDigits = Number.isFinite(Number(digits)) ? Math.max(0, Math.min(20, Number(digits))) : 0;
  return Number(value).toLocaleString("en-SG", { maximumFractionDigits: safeDigits });
}

function metric(label, value) {
  return `<article class="metric"><div class="label">${label}</div><div class="value">${value}</div></article>`;
}

function matchesSegment(row) {
  return (!row.listing_type || row.listing_type === selectedType)
    && (!row.property_segment || row.property_segment === selectedSegment);
}

function byWeek(rows) {
  return rows.filter((row) => row.snapshot_week_id === selectedWeek && matchesSegment(row));
}

function destroyChart(id) {
  if (charts[id]) {
    charts[id].destroy();
    delete charts[id];
  }
}

function lineChart(id, labels, series) {
  destroyChart(id);
  const ctx = document.getElementById(id);
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
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "bottom" } },
      scales: { y: { beginAtZero: false } },
    },
  });
}

function table(containerId, rows, columns) {
  const el = document.getElementById(containerId);
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
  el.innerHTML = `<div class="table-wrap"><table><thead><tr>${thead}</tr></thead><tbody>${body}</tbody></table></div>`;
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

function renderMetrics() {
  const row = data.market.find((r) => r.snapshot_week_id === selectedWeek && r.listing_type === selectedType && r.property_segment === selectedSegment) || {};
  document.getElementById("metricCards").innerHTML = [
    metric("Active listings", fmt.format(n(row.active_listings))),
    metric("New", fmt.format(n(row.new_listings))),
    metric("Disappeared", fmt.format(n(row.disappeared_listings))),
    metric("Price cuts", fmt.format(n(row.price_cut_listings))),
    metric("Avg PSF", money.format(n(row.avg_psf))),
  ].join("");
}

function renderOverview() {
  const marketRows = data.market.filter((r) => r.listing_type === selectedType && r.property_segment === selectedSegment);
  const labels = marketRows.map((r) => r.snapshot_week_id);
  lineChart("lifecycleChart", labels, [
    { label: "Active", data: marketRows.map((r) => n(r.active_listings)) },
    { label: "New", data: marketRows.map((r) => n(r.new_listings)) },
    { label: "Disappeared", data: marketRows.map((r) => n(r.disappeared_listings)) },
    { label: "Price cuts", data: marketRows.map((r) => n(r.price_cut_listings)) },
  ]);
  lineChart("pricingChart", labels, [
    { label: "Price-cut rate %", data: marketRows.map((r) => n(r.price_cut_rate) * 100) },
    { label: "Stale 60d %", data: marketRows.map((r) => n(r.stale_60d_share) * 100) },
    { label: "Avg PSF", data: marketRows.map((r) => n(r.avg_psf)) },
  ]);

  table("districtTable", byWeek(data.districts), [
    { key: "district_text", label: "District" },
    { key: "region_text", label: "Region" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "price_cut_rate", label: "Cut %", num: true, format: (v) => fmtNum(n(v) * 100, 1) },
    { key: "stale_60d_share", label: "Stale 60d %", num: true, format: (v) => fmtNum(n(v) * 100, 1) },
    { key: "duplicate_candidate_listings", label: "Duplicate candidates", num: true, format: fmtNum },
    { key: "avg_psf", label: "Avg PSF", num: true, format: fmtNum },
  ]);

  table("projectTable", byWeek(data.projects), [
    { key: "project_name", label: "Project / postal group" },
    { key: "project_group_type", label: "Level" },
    { key: "postal_code", label: "Postal" },
    { key: "district_text", label: "District" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "price_cut_rate", label: "Cut %", num: true, format: (v) => fmtNum(n(v) * 100, 1) },
    { key: "stale_60d_share", label: "Stale 60d %", num: true, format: (v) => fmtNum(n(v) * 100, 1) },
    { key: "top_agent_share", label: "Top agent %", num: true, format: (v) => fmtNum(n(v) * 100, 1) },
    { key: "duplicate_candidate_listings", label: "Duplicate candidates", num: true, format: fmtNum },
    { key: "avg_psf", label: "Avg PSF", num: true, format: fmtNum },
  ]);
}

function projectLabel(row) {
  const level = row.project_group_type === "postal_code" ? `postal ${row.postal_code}` : row.project_group_type || "project";
  return `${row.project_name} — ${level} — ${row.district_text} (${fmtNum(row.active_listings)} active, score ${fmtNum(row.pressure_score, 1)})`;
}

function setupProjectSelect() {
  const select = document.getElementById("projectSelect");
  const latestRows = byWeek(data.projects).filter((r) => r.project_uid);
  select.innerHTML = latestRows.map((r) => `<option value="${r.project_uid}">${projectLabel(r)}</option>`).join("");
  select.onchange = renderProjectDetail;
  renderProjectDetail();
}

function renderProjectDetail() {
  const projectUid = document.getElementById("projectSelect").value;
  const trend = data.projectTrends.filter((r) => r.project_uid === projectUid && r.listing_type === selectedType && r.property_segment === selectedSegment);
  if (!trend.length) return;
  const latest = trend[trend.length - 1];
  document.getElementById("projectCards").innerHTML = [
    metric("Pressure", fmtNum(latest.pressure_score, 1)),
    metric("Active", fmtNum(latest.active_listings)),
    metric("Price cuts", fmtNum(latest.price_cut_listings)),
    metric("Avg PSF", fmtNum(latest.avg_psf)),
    metric("Stale 60d", `${fmtNum(n(latest.stale_60d_share) * 100, 1)}%`),
  ].join("");

  const labels = trend.map((r) => r.snapshot_week_id);
  lineChart("projectLifecycleChart", labels, [
    { label: "Active", data: trend.map((r) => n(r.active_listings)) },
    { label: "New", data: trend.map((r) => n(r.new_listings)) },
    { label: "Disappeared", data: trend.map((r) => n(r.disappeared_listings)) },
    { label: "Cuts", data: trend.map((r) => n(r.price_cut_listings)) },
  ]);
  lineChart("projectPressureChart", labels, [
    { label: "Pressure", data: trend.map((r) => n(r.pressure_score)) },
    { label: "Avg PSF", data: trend.map((r) => n(r.avg_psf)) },
    { label: "Stale 60d %", data: trend.map((r) => n(r.stale_60d_share) * 100) },
  ]);

  table("projectTrendTable", trend, [
    { key: "snapshot_week_id", label: "Week" },
    { key: "pressure_score", label: "Score", num: true, format: (v) => fmtNum(v, 1) },
    { key: "active_listings", label: "Active", num: true, format: fmtNum },
    { key: "new_listings", label: "New", num: true, format: fmtNum },
    { key: "disappeared_listings", label: "Gone", num: true, format: fmtNum },
    { key: "price_cut_listings", label: "Cuts", num: true, format: fmtNum },
    { key: "avg_psf", label: "Avg PSF", num: true, format: fmtNum },
  ]);
}

function renderEventTables() {
  table("cutsTable", byWeek(data.priceCuts), [
    { key: "listing_type", label: "Type" },
    { key: "project_name", label: "Project" },
    { key: "district_text", label: "District" },
    { key: "bedrooms", label: "Beds", num: true, format: fmtNum },
    { key: "floor_area_sqft", label: "Sqft", num: true, format: fmtNum },
    { key: "prior_price_value", label: "Prior price", num: true, format: fmtNum },
    { key: "price_value", label: "Price", num: true, format: fmtNum },
    { key: "price_change_pct", label: "Change %", num: true, format: (v) => fmtNum(n(v) * 100, 2) },
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
    { key: "new_listings", label: "New", num: true, format: fmtNum },
    { key: "disappeared_listings", label: "Gone", num: true, format: fmtNum },
    { key: "price_cut_listings", label: "Cuts", num: true, format: fmtNum },
    { key: "distinct_projects", label: "Projects", num: true, format: fmtNum },
    { key: "distinct_agents", label: "Agents", num: true, format: fmtNum },
  ]);
}

function renderAll() {
  renderMetrics();
  renderOverview();
  setupProjectSelect();
  renderEventTables();
}

async function init() {
  setupTabs();
  const res = await fetch("assets/dashboard-data.json?v=20260501-0010", { cache: "no-store" });
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
