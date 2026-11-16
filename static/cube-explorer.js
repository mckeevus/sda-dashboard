/* Native SDA cube explorer: valid dimensions, complete aggregates and source-aware exports. */
(() => {
  const root=document.getElementById('cube-explorer');
  if(!root)return;
  const $=id=>document.getElementById('cube-'+id);
  const fmt=n=>n==null?'Unavailable':Number(n).toLocaleString('en-AU');
  const colors=['#522E75','#00897B','#1E88E5','#E65100','#8B5FC0','#6B7280','#B45309','#1565C0'];
  const stateOrder=['ACT','NSW','NT','QLD','SA','TAS','VIC','WA','Missing'];
  const ageOrder=['Under 18','19 to 34','35 to 64','65+'];
  const presets={
    demand_usage:[
      {id:'need-trend',label:'Need and usage over time',type:'line',x:'report_date',y:'sda_status'},
      {id:'need-design',label:'Design category by state · heatmap',type:'heatmap',x:'design_category',y:'state'},
      {id:'need-share',label:'Design mix by state · 100% stacked bars',type:'stack',x:'state',y:'design_category'},
      {id:'need-building',label:'Detailed building eligibility over time',type:'line',x:'report_date',y:'building_type'}],
    characteristics:[{id:'characteristics-age',label:'Age and disability · heatmap',type:'heatmap',x:'age_band',y:'disability_group'}],
    demographics:[
      {id:'demographics-first',label:'First Nations profile by SDA status',type:'bar',x:'first_nations',y:'sda_status'},
      {id:'demographics-remote',label:'Remoteness and First Nations · heatmap',type:'heatmap',x:'first_nations',y:'remoteness'}],
    enrolled:[
      {id:'enrolled-growth',label:'Net stock growth and last-quarter flags',type:'growth',x:'report_date',y:'new_last_quarter'},
      {id:'enrolled-sda',label:'SDA stock type over time',type:'line',x:'report_date',y:'sda_type'},
      {id:'enrolled-building',label:'Detailed building configurations over time',type:'line',x:'report_date',y:'building_type'},
      {id:'enrolled-design',label:'State and design category · heatmap',type:'heatmap',x:'design_category',y:'state'}],
    pipeline:[
      {id:'pipeline-cohorts',label:'Pipeline entry cohorts and new records',type:'bar',x:'entry_cohort',y:'new_last_quarter'},
      {id:'pipeline-design',label:'State and design category · heatmap',type:'heatmap',x:'design_category',y:'state'},
      {id:'pipeline-building',label:'Building configuration by state · heatmap',type:'heatmap',x:'state',y:'building_type'}]
  };
  let catalogue=null, filterValues={},activeCube='demand_usage',activePreset=null,activeRelease=null;
  let choices={}, axes={}, lastResult=null,lastRequest=null,displayRows=[],displayHeaders=[];
  let loading=null, requestToken=0, lastScope='', scaleCache=new Map();
  const validFilters={};
  function el(tag,text,cls){const e=document.createElement(tag);if(text!=null)e.textContent=text;if(cls)e.className=cls;return e;}
  function options(select,items,selected) {
    select.replaceChildren(...items.map(item=>{const o=el('option',item.label??item);o.value=item.value??item;return o;}));
    if([...select.options].some(o=>o.value===selected))select.value=selected;
  }
  function metadata(){return catalogue?.cubes.find(c=>c.id===activeCube);}
  function selectedView(){return presets[activeCube].find(v=>v.id===$('view').value)||presets[activeCube][0];}
  function order(values,dim) {
    const list=dim==='state'?stateOrder:dim==='age_band'?ageOrder:filterValues[dim]||[];
    return [...new Set(values)].sort((a,b)=>list.length?(list.includes(a)?list.indexOf(a):999)-(list.includes(b)?list.indexOf(b):999)||String(a).localeCompare(String(b)):String(a).localeCompare(String(b)));
  }
  async function api(path,body) {
    const response=await fetch(path,{method:body?'POST':'GET',headers:body?{'Content-Type':'application/json'}:{},body:body?JSON.stringify(body):undefined});
    const result=await response.json();if(!response.ok)throw new Error(result.error||'The cube request failed.');return result;
  }
  function setBusy(busy){$('csv').disabled=busy||!lastResult?.data.length;$('png').disabled=busy||!lastResult?.data.length;root.setAttribute('aria-busy',String(busy));}
  async function loadCatalogue(release) {
    catalogue=await api('/api/cubes'+(release?'?release_id='+encodeURIComponent(release):''));
    if(!catalogue.cubes.length)throw new Error('No cube release has been loaded.');
    activeRelease=catalogue.release_id;
    options($('dataset'),catalogue.cubes.map(c=>({value:c.id,label:c.name})),activeCube);
    options($('release'),catalogue.releases.map(r=>({value:r.release_id,label:r.label})),activeRelease);
    $('dataset').disabled=$('view').disabled=$('release').disabled=false;
    // The shared quarter picker must also offer dates present only in cubes.
    if(state.quarters?.length)state.quarters=[...new Set([...state.quarters,...catalogue.cubes.flatMap(c=>c.quarters)])].sort();
    coverage();
  }
  async function loadCube() {
    activeCube=$('dataset').value;
    options($('view'),presets[activeCube].map(v=>({value:v.id,label:v.label})),choices[activeCube]?.view);
    const loadingCube=activeCube,loadingRelease=activeRelease;
    const result=await api('/api/cubes/'+loadingCube+'/filters?release_id='+encodeURIComponent(loadingRelease));
    if(loadingCube!==activeCube || loadingRelease!==activeRelease)return false;
    filterValues=result.filters;
    validFilters[activeCube]=filterValues;
    renderFilters();setupAxes();return true;
  }
  function renderFilters() {
    const filters=choices[activeCube]?.filters||{};
    const dims=metadata().dimensions;
    $('filters').replaceChildren();
    for(const [field,values] of Object.entries(filterValues)) {
      if(field==='state')continue;
      const label=el('label',dims[field]),select=el('select');select.dataset.field=field;select.id='cube-filter-'+field;
      const selection=values.includes(filters[field])?filters[field]:'';
      options(select,[{value:'',label:'All published categories'},...values],selection);
      select.addEventListener('change',()=>{remember();render();});label.appendChild(select);$('filters').appendChild(label);
    }
    // Useful readable profiles initially; retain the user's choices when switching cubes.
    if(!choices[activeCube]){
      if(activeCube==='characteristics'){
        const status=$('filter-sda_status');if(status)status.value=filterValues.sda_status.find(v=>v.startsWith('2.'))||'';
      }
      if(activeCube==='demographics'){
        const remote=$('filter-remoteness');if(remote)remote.value='Major cities';
      }
    }
  }
  function remember() {
    const filters={};$('filters').querySelectorAll('select').forEach(s=>{if(s.value)filters[s.dataset.field]=s.value;});
    choices[activeCube]={filters,view:$('view').value};
    $('filter-count').textContent=Object.keys(filters).length?'('+Object.keys(filters).length+' active)':'';
  }
  function setupAxes() {
    const view=selectedView(), heat=view.type==='heatmap',dims=metadata().dimensions;
    $('axes').hidden=!heat;
    const fields=Object.keys(dims).filter(d=>d!=='report_date');
    const saved=axes[view.id];
    options($('x'),fields.map(d=>({value:d,label:dims[d]})),saved?.x||view.x);
    options($('y'),fields.map(d=>({value:d,label:dims[d]})),saved?.y||view.y);
    $('measure').value=saved?.measure||'count';
    activePreset=view.id;
    const flags=$('filter-new_last_quarter');
    if(flags){flags.disabled=view.type==='growth';if(flags.disabled)flags.value='';}
    remember();
  }
  function parameters() {
    remember();const view=selectedView();
    const x=view.type==='heatmap'?$('x').value:view.x,y=view.type==='heatmap'?$('y').value:view.y;
    if(x===y)throw new Error('Choose different row and column dimensions.');
    const filters={...choices[activeCube].filters};
    if(state.selectedState!=='All')filters.state=state.selectedState;
    return {release_id:activeRelease,quarter:effectiveQ(),group_by:[x,y],filters};
  }
  async function refresh(force=false) {
    if(state.datasetFocus!=='cubes')return;
    document.body.classList.add('cube-focus');
    if(!state.latestQ)return;
    try {
      if(!catalogue){
        if(!loading)loading=(async()=>{await loadCatalogue();await loadCube();})().finally(()=>loading=null);
        await loading;
      }
      const scope=[effectiveQ(),state.selectedState,activeCube,activeRelease].join('|');
      if(!force && scope===lastScope)return;
      lastScope=scope;await render();
    }catch(error){showError(error);}
  }
  function showError(error) {
    lastScope='';
    $('summary').textContent='Could not display this selection.';
    $('unavailable').hidden=false;$('unavailable').textContent=error.message;
    lastResult=null;setBusy(false);
    if(document.getElementById('chart-cube').data)Plotly.purge('chart-cube');
  }
  async function render() {
    const token=++requestToken;
    try {
      const params=parameters(),view=selectedView();setBusy(true);
      $('summary').textContent='Loading selected cube…';
      if(view.type==='heatmap')axes[view.id]={x:$('x').value,y:$('y').value,measure:$('measure').value};
      const result=await api('/api/cubes/'+activeCube+'/query',params);
      if(token!==requestToken)return;
      lastRequest=params;lastResult=result;
      $('unavailable').hidden=result.quarter_available && result.data.length>0;
      if(!result.quarter_available)$('unavailable').textContent='No cube snapshot is available for '+growthQuarterLabel(result.quarter)+'. Available dates: '+metadata().quarters.map(growthQuarterLabel).join(', ')+'. Select an available quarter using the global selector.';
      else if(!result.data.length)$('unavailable').textContent='No published records match these filters. An absent combination is not an observed zero.';
      const group=params.group_by;
      const currentRows=group.includes('report_date')?result.data.filter(r=>r.report_date.startsWith(result.quarter.slice(0,4))&&Math.ceil(Number(r.report_date.slice(5,7))/3)===Number(result.quarter.at(-1))):result.data;
      const complete=currentRows.length && currentRows.every(r=>r.value_status==='exact');
      const total=complete?currentRows.reduce((s,r)=>s+r.value,0):null;
      const missing=currentRows.filter(r=>r.value_status!=='exact').length;
      $('summary').textContent=growthQuarterLabel(result.quarter)+' · '+(state.selectedState==='All'?'Australia':state.selectedState)+' · '+(total!=null?fmt(total)+' '+result.unit.toLowerCase():missing+' unavailable aggregates')+(group.includes('report_date')?' · history through selection':'');
      $('chart-title').textContent=view.label;
      $('source').textContent='Source: '+result.source.filename+' · '+result.release_id+' · '+result.source.dates[0]+' to '+result.source.dates.at(-1);
      $('note').textContent=result.note+' '+result.missing_rule;
      document.getElementById('data-note-text').textContent='Data cubes · '+growthQuarterLabel(result.quarter)+' · '+result.release_id;
      await draw(result,view);
      valuesTable();setBusy(false);
    }catch(error){if(token===requestToken)showError(error);}
  }
  function short(text,max=24) {
    const words=String(text).split(' ');let lines=[''];
    for(const word of words){if((lines.at(-1)+' '+word).length>max&&lines.at(-1))lines.push(word);else lines[lines.length-1]+=(lines.at(-1)?' ':'')+word;}
    return lines.map(line=>line.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')).join('<br>');
  }
  async function draw(result,view) {
    const [xd,yd]=result.group_by,dims=metadata().dimensions;
    const x=order(result.data.map(r=>r[xd]),xd),y=order(result.data.map(r=>r[yd]),yd);
    const lookup=new Map(result.data.map(r=>[JSON.stringify([r[xd],r[yd]]),r]));
    const get=(a,b)=>lookup.get(JSON.stringify([a,b]));
    const chart=document.getElementById('chart-cube'),width=chart.getBoundingClientRect().width,narrow=width<650;
    let traces=[],height=460;
    let layout={margin:{l:narrow?58:75,r:25,t:25,b:110},paper_bgcolor:'white',plot_bgcolor:'white',
      font:{family:"'DM Sans',Arial,sans-serif",size:12,color:'#374151'},legend:{orientation:'h',y:-.3,x:0,xanchor:'left'},
      xaxis:{title:dims[xd],automargin:true},yaxis:{title:result.unit,rangemode:'tozero',automargin:true},uirevision:activeCube+'|'+view.id+'|'+xd+'|'+yd};
    displayHeaders=[dims[xd],dims[yd],result.unit,'Value status'];
    displayRows=result.data.map(r=>[r[xd],r[yd],r.value,r.value_status]);
    if(view.type==='heatmap' && (x.length>24 || y.length>45)) {
      $('unavailable').hidden=false;$('unavailable').textContent='This selection has '+y.length+' rows and '+x.length+' columns. Use the cube filters or choose broader axes for a readable heatmap. The selected data remains available in View chart data and CSV.';
      chart.style.height='300px';
      await Plotly.react(chart,[],{...layout,annotations:[{text:'Narrow the selection to display the heatmap',xref:'paper',yref:'paper',x:.5,y:.5,showarrow:false}]},{displayModeBar:false});return;
    }
    if(view.type==='heatmap') {
      const shares=$('measure').value==='share';
      const rowComplete=y.map(b=>x.every(a=>get(a,b)?.value_status==='exact'));
      const totals=y.map((b,j)=>rowComplete[j]?x.reduce((s,a)=>s+get(a,b).value,0):null);
      const z=y.map((b,j)=>x.map(a=>{const r=get(a,b);return !r||r.value==null||shares&&(!rowComplete[j]||!totals[j])?null:shares?r.value/totals[j]*100:r.value;}));
      const text=z.map(row=>row.map(v=>v==null?'—':shares?v.toFixed(1)+'%':fmt(v)));
      const details=y.map((b,j)=>x.map(a=>{const r=get(a,b);return r?.value_status==='exact'&&(!shares||rowComplete[j])?fmt(r.value)+' '+result.unit.toLowerCase():r?.value_status==='suppressed'?'Contains suppressed values':shares&&!rowComplete[j]?'Row percentage unavailable: incomplete denominator':'Not published';}));
      const scaleKey=[activeRelease,activeCube,xd,yd,JSON.stringify(result.filters)].join('|');
      const observed=Math.max(1,...z.flat().filter(Number.isFinite));
      scaleCache.set(scaleKey,Math.max(scaleCache.get(scaleKey)||1,result.scale_max||observed));
      const max=shares?100:scaleCache.get(scaleKey);
      const showValues=x.length<=8;
      traces=[{type:'heatmap',x,y,z,customdata:details,text,texttemplate:showValues?'%{text}':'',textfont:{color:'#1A1A2E',size:12},
        colorscale:[[0,'#F8F5FC'],[.5,'#DFCEEC'],[1,'#B99DCF']],zmin:0,zmax:max,zsmooth:false,hoverongaps:true,
        hovertemplate:'%{y}<br>%{x}<br>%{customdata}<extra></extra>',colorbar:{title:shares?'%':result.unit,orientation:'h',y:-.45,len:.7,thickness:12}}];
      height=Math.max(400,Math.min(2400,y.length*(narrow?46:34)+210));
      layout={...layout,margin:{l:narrow?120:200,r:25,t:30,b:150},
        xaxis:{title:dims[xd],type:'category',tickmode:'array',tickvals:x,ticktext:x.map(v=>short(narrow&&xd==='design_category'?({'High Physical Support':'HPS','Fully Accessible':'FA','Improved Liveability':'IL','Multi-Design Category':'Multi'}[v]||v):v,narrow?11:20)),tickangle:narrow&&x.length>4?-45:0,automargin:true},
        yaxis:{title:'',type:'category',tickmode:'array',tickvals:y,ticktext:y.map(v=>short(v,narrow?16:27)),autorange:'reversed',automargin:true},plot_bgcolor:'#F3F4F6'};
      if(narrow&&xd==='design_category')$('note').textContent+=' HPS: High Physical Support; FA: Fully Accessible; IL: Improved Liveability; Multi: Multi-Design Category.';
      $('note').textContent+=' — marks unavailable cells. '+(shares?'Percentages require every displayed contributor in the row to be published. ':'Colour scale remains fixed across reporting quarters for the same selection. ');
      displayHeaders.push(shares?'Share within row (%)':'');
      displayRows=[];y.forEach((b,j)=>x.forEach((a,i)=>{const r=get(a,b);displayRows.push([a,b,r?.value??null,r?.value_status||'not_published',shares?z[j][i]:null]);}));
    } else if(view.type==='growth') {
      const stock=x.map(a=>{const cells=result.data.filter(r=>r[xd]===a);return cells.length&&cells.every(r=>r.value_status==='exact')?cells.reduce((s,r)=>s+r.value,0):null;});
      const flagged=x.map((a,i)=>get(a,'1')?.value??(stock[i]!=null?0:null));
      const quarterIndex=date=>Number(date.slice(0,4))*4+Math.ceil(Number(date.slice(5,7))/3);
      const net=stock.map((v,i)=>i&&v!=null&&stock[i-1]!=null&&quarterIndex(x[i])-quarterIndex(x[i-1])===1?v-stock[i-1]:null);
      $('note').textContent+=' Net growth requires consecutive quarterly snapshots; the first observation has no growth value.';
      traces=[{type:'bar',x,y:flagged,name:'Flagged enrolled last quarter',marker:{color:colors[0]}},{type:'bar',x,y:net,name:'Net change in enrolled stock',marker:{color:colors[1]}}];
      layout.barmode='group';layout.xaxis.type='date';layout.xaxis.title='Quarter end';
      displayHeaders=['Quarter end','Enrolled stock','Flagged enrolled last quarter','Net stock change'];
      displayRows=x.map((a,i)=>[a,stock[i],flagged[i],net[i]]);
    } else {
      const shares=view.type==='stack';
      const complete=x.map(a=>y.every(b=>get(a,b)?.value_status==='exact'));
      const totals=x.map((a,i)=>complete[i]?y.reduce((s,b)=>s+get(a,b).value,0):null);
      traces=y.map((b,k)=>({type:view.type==='line'?'scatter':'bar',mode:view.type==='line'?'lines+markers':undefined,
        x,y:x.map((a,i)=>{const v=get(a,b)?.value;return v==null?null:shares?complete[i]&&totals[i]?v/totals[i]*100:null:v;}),
        name:yd==='new_last_quarter'?(b==='1'?'Flagged new last quarter':'Other reported pipeline'):b,
        marker:{color:colors[k%colors.length],size:5},line:{color:colors[k%colors.length],width:2,dash:k>=colors.length?'dot':'solid'},connectgaps:false,
        hovertemplate:'%{x}<br>%{fullData.name}: %{y:,.1f}'+(shares?'%':' '+result.unit.toLowerCase())+'<extra></extra>'}));
      if(view.type==='line')layout.xaxis={...layout.xaxis,type:'date',tickformat:'%b %Y',nticks:narrow?4:8};
      else layout.xaxis={...layout.xaxis,tickmode:'array',tickvals:x,ticktext:x.map(v=>short(v,narrow?12:25)),tickangle:0};
      layout.barmode=shares||view.id==='pipeline-cohorts'?'stack':'group';
      if(shares){
        displayHeaders.push('Share within state (%)');
        displayRows=result.data.map(r=>{const i=x.indexOf(r[xd]);return [r[xd],r[yd],r.value,r.value_status,complete[i]&&totals[i]&&r.value!=null?r.value/totals[i]*100:null];});
        layout.yaxis={...layout.yaxis,title:'Share of participants (%)',range:[0,100],ticksuffix:'%'};$('note').textContent+=' Percentages use the full published row denominator; incomplete rows are unavailable.';}
    }
    if(!result.data.length){traces=[];layout.annotations=[{text:'No published data for this selection',xref:'paper',yref:'paper',x:.5,y:.5,showarrow:false}];}
    chart.style.height=height+'px';
    chart.setAttribute('aria-label',view.label+' · '+growthQuarterLabel(result.quarter)+' · '+(state.selectedState==='All'?'Australia':state.selectedState)+'. Exact values and availability are in View chart data.');
    await Plotly.react(chart,traces,layout,{displayModeBar:false,responsive:true});
  }
  function valuesTable() {
    const table=$('data-table');
    table.tHead.replaceChildren();const tr=el('tr');displayHeaders.filter(Boolean).forEach(h=>tr.appendChild(el('th',h)));table.tHead.appendChild(tr);
    const body=table.tBodies[0];body.replaceChildren();
    displayRows.forEach(row=>{const tr=el('tr');row.slice(0,displayHeaders.filter(Boolean).length).forEach(v=>tr.appendChild(el('td',typeof v==='number'?fmt(v):v??'Unavailable',typeof v==='number'?'cube-value':'')));body.appendChild(tr);});
  }
  function coverage() {
    const body=$('coverage-table').tBodies[0];body.replaceChildren();
    catalogue.cubes.forEach(c=>{const tr=el('tr');[c.name,c.dates[0]+' to '+c.dates.at(-1),fmt(c.row_count),fmt(c.suppressed_cells)].forEach(v=>tr.appendChild(el('td',v)));body.appendChild(tr);});
    $('checks').replaceChildren();
    const reviewed=activeRelease==='sda-cubes-2026-09';
    const notes=[
      'All five files are validated against their full published dimension keys; SHA256 checksums identify each immutable source file.',
      ...(reviewed?['June 2026 pipeline: the supplied cube reports 6,544 dwellings, compared with 6,549 in Supplement P. The sources remain separate.',
      'March 2025 participant status split differs from Supplement P by −191 in use / +191 eligible, with the same overall total. This is a source discrepancy, not evidence of identified participant transitions.',
      'Pipeline entry years needing review: 14 dwellings in June 2026 (1899 or missing); 3 in September 2026 (future years). Reviewed entry-cohort views retain these as Unverified entry year.',
      ]:[]),
      'Suppressed cells retain their disclosure status. An aggregate containing an unavailable input has no numeric value; omitted combinations are not filled with zero.'
    ];
    if(!catalogue.revisions?.length)notes.push('Baseline release. Later uploads will retain this version and show new reporting dates and changes to overlapping source cells.');
    for(const change of catalogue.revisions||[]) {
      const name=catalogue.cubes.find(c=>c.id===change.cube).name;
      notes.push(name+' versus '+change.compared_with+': '+change.changed_existing_cells+' changed existing cells, '+change.added_historical_cells+' added and '+change.removed_historical_cells+' removed cells at overlapping dates. New reporting dates: '+(change.new_dates.join(', ')||'none')+'.');
      if(change.dates_not_in_current_file.length)notes.push(name+': earlier reporting dates omitted from this file remain accessible in the prior source release: '+change.dates_not_in_current_file.join(', ')+'.');
    }
    notes.forEach(n=>$('checks').appendChild(el('li',n)));
  }
  async function exportCSV() {
    if(!lastResult||!lastRequest)return;
    const meta=['Unit','Selected calendar quarter','Source release','Source file','SHA256','Filters'];
    const suffix=[lastResult.unit,lastResult.quarter,lastResult.release_id,lastResult.source.filename,lastResult.source.sha256,JSON.stringify(lastResult.filters)];
    const width=displayHeaders.filter(Boolean).length;
    const rows=[[...displayHeaders.filter(Boolean),...meta],...displayRows.map(row=>[...row.slice(0,width),...suffix])];
    const cell=value=>{
      let str=value==null?'':String(value);
      if(typeof value==='string'&&/^[=+@-]/.test(str))str="'"+str;
      return '"'+str.replace(/"/g,'""')+'"';
    };
    const blob=new Blob(['\uFEFF'+rows.map(row=>row.map(cell).join(',')).join('\r\n')],{type:'text/csv;charset=utf-8'});
    const url=URL.createObjectURL(blob),a=el('a');a.href=url;a.download='SDA_'+lastResult.cube+'_'+lastResult.quarter+'.csv';document.body.appendChild(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
  }
  $('dataset').addEventListener('change',async()=>{++requestToken;setBusy(true);try{if(await loadCube())await render();}catch(e){showError(e);}});
  $('view').addEventListener('change',()=>{setupAxes();render();});
  $('release').addEventListener('change',async()=>{++requestToken;try{await loadCatalogue($('release').value);await loadCube();await render();}catch(e){showError(e);}});
  for(const field of ['x','y','measure'])$(field).addEventListener('change',()=>render());
  $('reset').addEventListener('click',()=>{$('filters').querySelectorAll('select').forEach(s=>s.value='');remember();render();});
  $('csv').addEventListener('click',exportCSV);
  $('png').addEventListener('click',()=>Plotly.downloadImage('chart-cube',{format:'png',width:1400,height:900,filename:'SDA_'+activeCube+'_'+effectiveQ()}));
  window.sdaCubeExplorer={refresh,exportCSV,getResult:()=>lastResult,resetFilters:()=>{lastScope='';choices={};$('filters').querySelectorAll('select').forEach(s=>s.value='');}};
})();
