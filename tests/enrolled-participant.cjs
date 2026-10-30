// Run with: node tests/enrolled-participant.cjs
// Exercise the dashboard's real data loader using captured aggregate API rows,
// including the empty P.13 response that caused the production regression.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const fixture = require('./fixtures/enrolled-participant.json');
const html = fs.readFileSync(path.join(__dirname, '..', 'dashboard_v7.html'), 'utf8');
const script = html.slice(html.indexOf('<script>') + 8, html.lastIndexOf('</script>'));
const code = script.slice(0, script.lastIndexOf('preloadDatasetMap().then(boot)'));
const elements = new Map();
function element(id) {
  if (!elements.has(id)) elements.set(id, {
    checked: true, value: '', options: [], textContent: '',
    set innerHTML(value) { this.options = Array.from(value.matchAll(/<option value="([^"]+)"/g), m => ({ value: m[1] })); }
  });
  return elements.get(id);
}
const requests = [];
const plots = new Map();
const downloads = [];
const context = vm.createContext({
  window: { location: { hostname: 'example.test' } },
  document: { getElementById: element },
  Plotly: { newPlot: (id, traces, layout) => plots.set(id, { traces, layout }) },
  fetch: async (url, options = {}) => {
    const body = options.body ? JSON.parse(options.body) : null;
    requests.push({ url, body });
    let data;
    if (url === '/api/filters') data = { datasets: fixture.datasets };
    else if (url === '/api/data/timeseries') data = { data: body.dataset_id === 9 ?
      (body.geo_type === 'Australia' ? fixture.enrolledAustralia : fixture.enrolledState) : [] };
    else if (url === '/api/dwellings/sa3') data = { bt: [], dc: [], maxres: [] };
    else if (url === '/api/participants/dc') data = { by_sa4: fixture.participantState, by_sa3: [] };
    else if (url === '/api/providers') data = { timeseries: [], by_state: [] };
    else data = {};
    return { ok: true, json: async () => data };
  }
});
vm.runInContext(code, context);
context.captureCSV = (filename, content) => downloads.push({ filename, content });
vm.runInContext('_downloadCSV = captureCSV', context);
const run = code => vm.runInContext(code, context);

(async () => {
  run(`state.quarters = ${JSON.stringify(fixture.quarters)}; state.latestQ = '2026-Q2';`);
  await run('preloadDatasetMap()');
  await run('loadAllData()');
  assert.equal(run('cache.dwSa3Raw.bt.length'), 0);
  assert.equal(requests.filter(r => r.body?.dataset_id === 9).length, 2, 'Must load P.4 at national and state levels');
  run('renderSDATypeChart(); csvSDAType(); renderEnrolledStateGrowth();');
  assert.deepEqual(Array.from(plots.get('chart-sda-type').traces, t => t.y.at(-1)), [10242, 172, 3670, 151]);
  assert.match(downloads.at(-1).content, /2025-26 Q4,10242,172,3670,151/);
  assert.equal(element('enrolled-growth-start').value, '2024-Q2');
  assert.equal(element('enrolled-growth-end').value, '2026-Q2');
  assert.deepEqual(Array.from(plots.get('chart-enrolled-state-growth').traces[0].y), [608,1554,966,915,510,95,63,37]);

  run("state.selectedState = 'NSW'; renderSDATypeChart(); csvSDAType();");
  assert.match(downloads.at(-1).content, /2025-26 Q4,2128,38,1179,50/);
  element('st-newbuild').checked = false;
  run('renderSDATypeChart()');
  assert.equal(plots.get('chart-sda-type').traces[0].name, 'New Build Refurbishment');
  assert.equal(plots.get('chart-sda-type').traces[0].y.at(-1), 38);

  // An incomplete category set cannot become a false total when Total is absent.
  const complete = run('cache.enrolledTypeState');
  context.incomplete = complete.filter(r => !(r.state === 'NSW' && ['Total','Legacy'].includes(r.measure_name)));
  run('cache.enrolledTypeState = incomplete');
  assert.equal(run("enrolledGrowthTotals()['NSW|2026-Q2']"), undefined);
  context.complete = complete;
  run('cache.enrolledTypeState = complete');
  element('enrolled-growth-start').value = '2026-Q2';
  run('renderEnrolledStateGrowth()');
  assert.equal(plots.get('chart-enrolled-state-growth').traces.length, 0);

  run("state.selectedQuarter = null; renderParticipantStateDC(); csvParticipantStateDC();");
  let dc = plots.get('chart-pdc-state');
  assert.equal(dc.traces.length, 5);
  assert.equal(dc.traces[0].x.length, 8);
  for (let i = 0; i < 8; i++) assert.ok(Math.abs(dc.traces.reduce((sum, t) => sum + t.y[i], 0) - 100) < 1e-8);
  assert.equal(dc.traces[0].customdata[1][0], 2381);
  assert.match(downloads.at(-1).content, /June 2026 \(2025-26 Q4\),NSW,Improved Liveability,2381,/);
  run("state.selectedQuarter = '2025-Q2'; renderParticipantStateDC();");
  assert.equal(plots.get('chart-pdc-state').traces[0].customdata[1][0], 2210);
  run("state.selectedQuarter = '2024-Q2'; renderParticipantStateDC();");
  dc = plots.get('chart-pdc-state');
  assert.equal(dc.traces.length, 0);
  assert.match(dc.layout.annotations[0].text, /No participant design-category data/);
  console.log('Passed: P.4 loader with empty P.13, actual national/state CSV figures, independent refurbishment, eight state growth totals, incomplete data, invalid dates, 100% participant stacks, global quarter changes and missing quarters.');
})().catch(error => { console.error(error); process.exitCode = 1; });
