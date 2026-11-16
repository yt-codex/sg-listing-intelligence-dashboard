const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('docs/app.js', 'utf8').replace(/init\(\)\.catch\([\s\S]*$/, '');
function context(rows = []) {
  const ctx = vm.createContext({ Intl, Date, Map, Set, console });
  vm.runInContext(source, ctx);
  ctx.rows = rows;
  vm.runInContext('data = {market: rows, weeks: [], metadata: {}};', ctx);
  return ctx;
}
const row = (date, extra = {}) => ({ snapshot_week_id: `sg-week-${date}`, snapshot_date: date, listing_type: 'SALE', property_segment: 'PRIVATE_NON_LANDED', active_listings: 100, observed_active_listings: 100, imputed_active_listings: 0, comparison_eligible: true, rate_comparison_eligible: true, ...extra });
test('unequal rate windows suppress direction and alert escalation', () => {
  const r = row('2026-11-14', {rate_comparison_eligible: false, pressure_score: 90, confidence: 'Medium', prev: {pressure_score: 70}});
  const ctx = context([r]); ctx.r = r;
  assert.equal(vm.runInContext('alertState(r)', ctx), 'Unverified');
  assert.match(vm.runInContext('marketRead(r, null)', ctx), /direction is withheld/);
});
test('fixed materiality and persistence thresholds control alert states', () => {
  const ctx = context();
  ctx.r = row('2026-11-14', {pressure_score: 65, pressure_delta: 6, confidence:'Medium', prev:{pressure_score:59,active_listings:100}});
  assert.equal(vm.runInContext('alertState(r)', ctx), 'New');
  ctx.r.prev.pressure_score = 61;
  assert.equal(vm.runInContext('alertState(r)', ctx), 'Persistent');
  ctx.r.pressure_score = 52;
  assert.equal(vm.runInContext('alertState(r)', ctx), 'Resolved');
});
test('four-week inventory change needs all four valid intervening weeks', () => {
  const rows = ['2026-10-17','2026-10-24','2026-10-31','2026-11-07','2026-11-14'].map(d=>row(d));
  rows[4].active_listings = 110;
  const ctx = context(rows);ctx.r = rows[4];
  assert.equal(vm.runInContext('horizonChange(r, 4)', ctx), '+10%');
  rows.splice(2,1);
  assert.equal(vm.runInContext('horizonChange(r, 4)', ctx), 'not comparable');
});
test('unknown crawl completeness caps confidence and invalid identities fail', () => {
  const r = row('2026-11-14'); const ctx=context([r]);ctx.r=r;
  assert.equal(vm.runInContext('confidence(r, "district")',ctx), 'Medium');
  ctx.r = {...r, project_group_type:'postal_code', postal_code:'000000'};
  assert.equal(vm.runInContext('confidence(r, "project")',ctx), 'Low');
});
test('fully aged project signals do not get high reliability', () => {
  const r=row('2026-11-14', {stale_60d_share:1,project_group_type:'actual_project'});
  const ctx=context([r]);ctx.r=r;
  assert.equal(vm.runInContext('confidence(r,"project")',ctx),'Low');
});
test('compact project-year history expands fields without changing other sections', () => {
  const ctx=context();
  ctx.payload={projectTrends:{columns:['snapshot_week_id','active_listings','price_cut_rate'],rows:[['sg-week-2026-11-14',32,null]]},market:[{active_listings:100}]};
  vm.runInContext('expandCompactProjectHistory(payload)',ctx);
  assert.equal(ctx.payload.projectTrends[0].active_listings,32);
  assert.equal(ctx.payload.projectTrends[0].price_cut_rate,null);
  assert.equal(ctx.payload.market[0].active_listings,100);
  vm.runInContext('expandCompactProjectHistory(payload)',ctx);
  assert.equal(ctx.payload.projectTrends.length,1);
});
test('malformed compact histories fail visibly instead of shifting column meanings', () => {
  const ctx=context();ctx.payload={projectTrends:{columns:['snapshot_week_id','active_listings'],rows:[['sg-week-2026-11-14']]}};
  assert.throws(()=>vm.runInContext('expandCompactProjectHistory(payload)',ctx), /Invalid project-history/);
});
test('calendar gaps break plotted lines rather than joining missing weeks', () => {
  const ctx=context();ctx.rows=[row('2026-10-10'),row('2026-11-07')];
  ctx.document={getElementById:()=>({})};ctx.Chart=function(_canvas, config){ctx.config=config;};
  vm.runInContext('lineChart("test",chartLabels(rows),[{label:"Observed",data:[100,110]}])',ctx);
  assert.equal(ctx.config.data.labels[1],'Missing snapshots');
  assert.equal(ctx.config.data.datasets[0].data[1],null);
  assert.equal(ctx.config.data.datasets[0].spanGaps,false);
});

test('persistent alerts need adequate prior-period sample size', () => {
  const ctx=context();ctx.r=row('2026-11-14',{pressure_score:70,pressure_delta:5,confidence:'Medium',prev:{pressure_score:65,active_listings:2}});
  assert.equal(vm.runInContext('alertState(r)',ctx),'Unverified');
});

test('first loaded archive observation never compares against an implicit zero', () => {
  const ctx=context();ctx.r=row('2027-01-02');
  assert.match(vm.runInContext('marketRead(r,null)',ctx), /preceding observation is outside/);
});
