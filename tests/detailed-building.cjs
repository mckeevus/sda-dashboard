// Run with: node tests/detailed-building.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const fixture = require('./fixtures/detailed-building.json');
const html = fs.readFileSync(path.join(__dirname,'..','dashboard_v7.html'),'utf8');
const inline = html.slice(html.indexOf('<script>')+8,html.lastIndexOf('</script>'));
const code = inline.slice(0,inline.lastIndexOf('preloadDatasetMap().then(boot)'));
const elements = new Map();
function element(id) {
  if (!elements.has(id)) elements.set(id,{checked:true,value:''});
  return elements.get(id);
}
const downloads=[];
const context=vm.createContext({window:{location:{hostname:'example.test'}},document:{getElementById:element},fixture,
  captureCSV:(name,text)=>downloads.push({name,text})});
vm.runInContext(code,context);
const run=code=>vm.runInContext(code,context);
run(`state.quarters=['2022-Q4','2026-Q2']; state.latestQ='2026-Q2';
  cache.nb11Raw=attachDims(fixture['11_Australia']);cache.ex12Raw=attachDims(fixture['12_Australia']);
  cache.nb11State=attachDims(fixture['11_State']);cache.ex12State=attachDims(fixture['12_State']);
  _downloadCSV=captureCSV;`);
element('building-config-start').value='2022-Q4';element('building-config-end').value='2026-Q2';element('building-config-stock').value='all';
let data=run('detailedBuildingData()');
const expect={
  'Apartment, 1 bedroom, 1 resident':[761,1380],
  'Apartment, 2 bedrooms, 1 resident':[555,1512],
  'Apartment, 2 bedrooms, 2 residents':[120,239],
  'House, 2 residents':[439,2933],
  'Legacy Stock, 6+ residents':[217,151]
};
assert.equal(data.rows.length,5);
data.rows.forEach(r=>assert.deepEqual([r.from,r.to],expect[r.configuration]));
assert.equal(data.rows.find(r=>r.configuration==='House, 2 residents').growth,2494);
run('csvDetailedBuildingComparison();csvDetailedBuildingHistory();');
assert.match(downloads[0].text,/"Apartment, 2 bedrooms, 1 resident"/);
assert.match(downloads[1].text,/"761","1380"/);

element('building-config-stock').value='newbuild';
const newRows=run('detailedBuildingData().rows');
element('building-config-stock').value='existing';
const oldRows=run('detailedBuildingData().rows');
for(const r of newRows) {
  const old=oldRows.find(o=>o.configuration===r.configuration);
  assert.equal(r.from+old.from,expect[r.configuration][0]);
  assert.equal(r.to+old.to,expect[r.configuration][1]);
}
element('building-config-stock').value='all';
run("state.selectedState='NSW'");
data=run('detailedBuildingData()');
assert.equal(data.rows.find(r=>r.configuration==='House, 2 residents').to,149);
run("state.selectedState='All';cache.ex12Raw=cache.ex12Raw.filter(r=>r.quarter_str!=='2022-Q4')");
data=run('detailedBuildingData()');
assert.ok(data.rows.every(r=>r.from===null && r.growth===null),'Missing component data cannot become a partial total');
assert.ok(data.rows.every(r=>Number.isFinite(r.to)));
console.log('Passed: exact bedroom/resident configurations, real reported counts, CSV quoting, separate stock coverage, state filtering and missing-source protection.');
