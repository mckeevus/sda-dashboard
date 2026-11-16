/* Plotly's responsive flag observes window resizes, not card/focus changes. */
(() => {
  const pending=new Set();
  let frame=0;
  function schedule(el) {
    pending.add(el);
    if(frame)return;
    frame=requestAnimationFrame(()=>{
      frame=0;
      for(const chart of pending) {
        if(chart.isConnected && chart.getBoundingClientRect().width>0 && chart._fullLayout) {
          const box=chart.getBoundingClientRect();
          if(Math.abs(chart._fullLayout.width-box.width)>1 || Math.abs(chart._fullLayout.height-box.height)>1)
            Plotly.relayout(chart,{width:Math.floor(box.width),height:Math.floor(box.height),autosize:true}).catch(()=>{});
        }
      }
      pending.clear();
    });
  }
  const observer=new ResizeObserver(entries=>entries.forEach(e=>schedule(e.target)));
  const watched=new WeakSet();
  for(const method of ['newPlot','react']) {
    const original=Plotly[method];
    Plotly[method]=function(target,data,layout={},config={}) {
      const el=typeof target==='string'?document.getElementById(target):target;
      const next={...layout,autosize:true};
      delete next.width;
      if(el) {
        const box=el.getBoundingClientRect();
        if(box.width>0)next.width=Math.floor(box.width);
        if(box.height>0 && !layout.height)next.height=Math.floor(box.height);
        if(!watched.has(el)){observer.observe(el);watched.add(el);}
      }
      const result=original.call(Plotly,target,data,next,{...config,responsive:true});
      Promise.resolve(result).then(()=>el && schedule(el)).catch(()=>{});
      return result;
    };
  }
  window.sdaResizeCharts=()=>document.querySelectorAll('.js-plotly-plot').forEach(schedule);
})();
