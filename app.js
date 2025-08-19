// Fallback: Toronto bounds（约略包住 GTA）
const CITY_BOUNDS = [[-79.90, 43.40], [-78.90, 43.90]];
const CITY_CENTER = [-79.3832, 43.6532];
const CITY_ZOOM   = 10;

// ---- data urls (two datasets: all, autotheft) ----
// ---- data urls (two datasets: all, autotheft) ----
const DATASETS = {
  all: {
    heat: "geojson/heat_90d.geojson",
    emerg: "geojson/emerging.geojson",
    near: "geojson/near_repeat.geojson",
  },
  autotheft: {
    heat: "geojson/heat_90d_autotheft.geojson",
    emerg: "geojson/emerging_autotheft.geojson",
    near: "geojson/near_repeat_autotheft.geojson",
  },
};

let current = "all";

(function main() {
  if (!window.MAPBOX_TOKEN || !String(window.MAPBOX_TOKEN).startsWith("pk.")) {
    alert("Missing/invalid Mapbox token. Set window.MAPBOX_TOKEN in web/config.js");
    return;
  }
  mapboxgl.accessToken = window.MAPBOX_TOKEN;

  const map = new mapboxgl.Map({
    container: "map",
    style: "mapbox://styles/mapbox/light-v11",
    center: CITY_CENTER,
    zoom: CITY_ZOOM
  });

  const ui = {
    note: document.getElementById("note"),
    badge: document.getElementById("badge"),
    toggleHeat: document.getElementById("toggleHeat"),
    toggleNear: document.getElementById("toggle"),
    toggleEmerging: document.getElementById("toggleEmerging"),
    toggleEmergingNI: document.getElementById("toggleEmergingNI"),
    covSlider: document.getElementById("cov"),
    covLabel: document.getElementById("covLabel"),
    btnFit: document.getElementById("btnFit"),
    offenceSel: document.getElementById("offenceSel"),
  };
  ui.badge && (ui.badge.textContent = "loading…");

  map.on("error", (e) => {
    console.error("Mapbox error:", e?.error);
    ui.badge && (ui.badge.textContent = "map error");
    ui.note && (ui.note.textContent = "Map failed to load style. Check token & Allowed URLs.");
  });

  map.on("load", async () => {
    // ---------- safe ops（避免“layer not exist”报错） ----------
    const safe = {
      filter: (id, f) => { if (map.getLayer(id)) map.setFilter(id, f); },
      paint:  (id, prop, val) => { if (map.getLayer(id)) map.setPaintProperty(id, prop, val); },
      move:   (id, before) => { if (map.getLayer(id) && map.getLayer(before)) { try{ map.moveLayer(id, before); } catch(_){} } }
    };

    // ---------- helpers ----------
    async function loadJSON(url){ const r=await fetch(url); if(!r.ok) throw new Error(url); return r.json(); }
    function isValidBounds(b){
        if (!b || !Array.isArray(b) || !Array.isArray(b[0]) || !Array.isArray(b[1])) return false;
        const [minX,minY] = b[0], [maxX,maxY] = b[1];
        const nums = [minX,minY,maxX,maxY];
        if (nums.some(v => !Number.isFinite(v))) return false;
        // lon/lat 合法范围 & 左下 < 右上
        if (minX < -180 || maxX > 180 || minY < -85 || maxY > 85) return false;
        if (!(maxX > minX) || !(maxY > minY)) return false;
        // 过大（说明算坏了）或过小（点太集中）都回退城市
        const spanX = Math.abs(maxX - minX);
        const spanY = Math.abs(maxY - minY);
        if (spanX > 20 || spanY > 20) return false;   // 太大
        return true;
        }
    function forceToronto(){
        map.fitBounds(CITY_BOUNDS, { padding: 40, duration: 600 });
    }


    // 递归提取 [lon,lat]，稳住 Polygon/MultiPolygon 等结构
    function walkLngLat(node, cb) {
      if (!node) return;
      if (Array.isArray(node) && typeof node[0] === "number" && typeof node[1] === "number") {
        const lon = node[0], lat = node[1];
        if (Number.isFinite(lon) && Number.isFinite(lat)) cb(lon, lat);
        return;
      }
      if (Array.isArray(node)) for (const child of node) walkLngLat(child, cb);
    }
    function bboxFromGeoJSON(gj) {
      let minX =  180, minY =  90, maxX = -180, maxY = -90;
      const extend = (lon, lat) => {
        lat = Math.max(-85, Math.min(85, lat)); // clamp lat
        if (lon < minX) minX = lon;
        if (lat < minY) minY = lat;
        if (lon > maxX) maxX = lon;
        if (lat > maxY) maxY = lat;
      };
      (gj?.features || []).forEach(f => walkLngLat(f?.geometry?.coordinates, extend));
      if (minX === 180) return null;
      return [[minX, minY], [maxX, maxY]];
    }

    // 当前缓存（用于 Reset / Fit）
    let currentData = { heat:null, emerg:null, near:null };

    // ---------- 预取初始数据，然后用对象喂给 source ----------
    const [heat0, emerg0, near0] = await Promise.all([
      loadJSON(DATASETS[current].heat),
      loadJSON(DATASETS[current].emerg),
      loadJSON(DATASETS[current].near),
    ]);
    currentData = { heat: heat0, emerg: emerg0, near: near0 };
    const EMPTY = { type:"FeatureCollection", features:[] };

    // ---------- sources ----------
    map.addSource("heat90",   { type: "geojson", data: heat0  || EMPTY });
    map.addSource("emerging", { type: "geojson", data: emerg0 || EMPTY });
    map.addSource("nearrep",  { type: "geojson", data: near0  || EMPTY });

    // ---------- layers ----------
    map.addLayer({
      id:"heat90-fill", type:"fill", source:"heat90",
      paint:{
        "fill-color":[ "interpolate",["linear"],["get","count_90d"],
          0,"#f1f1f1",5,"#fdd0a2",10,"#fd8d3c",20,"#f03b20",40,"#bd0026" ],
        "fill-opacity":0.30
      }
    });
    map.addLayer({
      id:"heat90-outline", type:"line", source:"heat90",
      paint:{ "line-color":"#f03b20","line-width":0.8,"line-opacity":0.55 }
    });

    map.addLayer({
      id:"emerging-fill", type:"fill", source:"emerging",
      paint:{
        "fill-color":[ "match",["get","label"],
          "New","#2ca25f","Intensifying","#756bb1","Persistent","#f16913","#cccccc"],
        "fill-opacity":0.55
      }
    }, "heat90-fill");
    map.addLayer({
      id:"emerging-outline", type:"line", source:"emerging",
      paint:{ "line-color":"#555","line-width":0.8 }
    }, "heat90-outline");

    map.addLayer({
      id:"nearrep-fill", type:"fill", source:"nearrep",
      paint:{
        "fill-color":[ "interpolate",["linear"],["get","coverage"],
          1,"#9ecae1",2,"#6baed6",3,"#4292c6",5,"#2171b5",8,"#084594"],
        "fill-opacity":[ "interpolate",["linear"],["get","coverage"], 1,0.10, 3,0.18, 6,0.26 ]
      }
    });
    map.addLayer({
      id:"nearrep-outline", type:"line", source:"nearrep",
      paint:{ "line-color":"#084594","line-width":0.6 }
    });

    // 层级：near-repeat 在 emerging 下面
    safe.move("nearrep-fill", "emerging-fill");
    safe.move("nearrep-outline","emerging-outline");

    // ---------- Reset / Fit ----------
    function fitToCurrent(opts = {}){
        const arr = [currentData.heat, currentData.emerg, currentData.near].filter(Boolean);
        if (!arr.length) { forceToronto(); return; }

        let union = null;
        for (const gj of arr){
            const b = bboxFromGeoJSON(gj);
            if (!b) continue;
            if (!union) union = [[...b[0]], [...b[1]]];
            else {
            union[0][0] = Math.min(union[0][0], b[0][0]);
            union[0][1] = Math.min(union[0][1], b[0][1]);
            union[1][0] = Math.max(union[1][0], b[1][0]);
            union[1][1] = Math.max(union[1][1], b[1][1]);
            }
        }

        // 兜底：算不出 or 异常 -> 回到城市
        if (!isValidBounds(union) || opts.force === true) { forceToronto(); return; }

        map.fitBounds(union, { padding: 40, duration: 600 });
    }

    // ui.btnFit?.addEventListener("click", () => fitToCurrent({force:true}));
    ui.btnFit?.addEventListener("click", () => forceToronto());

    // ---------- Heat 分位数配色 ----------
    function setHeatQuantilesFrom(heatGJ){
        const vals = (heatGJ?.features || [])
            .map(f => Number(f.properties?.count_90d ?? 0))
            .filter(v => Number.isFinite(v))
            .sort((a,b)=>a-b);

        // 没数据 or 全 0 → 简单两段式，保证不报错
        const vmax = vals.length ? vals[vals.length-1] : 0;
        if (!vals.length || vmax <= 0){
            map.setPaintProperty("heat90-fill","fill-color",[
                "interpolate", ["linear"], ["get","count_90d"],
                0, "#f1f1f1",
                1, "#fd8d3c"
            ]);
            return;
        }

        const q = p => vals[Math.floor((vals.length-1)*p)];
        // 可能出现重复 → 去重并强制递增
        let stops = [0, q(0.50), q(0.75), q(0.90), q(0.97)];

        // 保证严格递增（相等时加一个极小的 epsilon）
        const eps = Math.max(vmax * 1e-6, 1e-6);
        for (let i=1; i<stops.length; i++){
            if (!(stops[i] > stops[i-1])) stops[i] = stops[i-1] + eps;
        }

        map.setPaintProperty("heat90-fill","fill-color",[
            "interpolate", ["linear"], ["get","count_90d"],
            stops[0], "#f1f1f1",
            stops[1], "#fdd0a2",
            stops[2], "#fd8d3c",
            stops[3], "#f03b20",
            stops[4], "#bd0026"
        ]);
    }


    // ---------- 侧栏文案/slider ----------
    function refreshMetaFrom(nearGJ, emergGJ){
      const feats = nearGJ?.features||[];
      if (feats.length){
        const p = Number(feats[0].properties?.p_value ?? 0);
        ui.badge && (ui.badge.textContent = `p≈${p.toFixed(3)}`);
        const anchor = emergGJ?.features?.[0]?.properties?.anchor_date || null;
        const note = `Knox near-repeat (d≤250 m, t≤14 d).${anchor?` Anchor=${anchor}.`:''} Tip: filter to coverage ≥2.`;
        ui.note && (ui.note.textContent = note);
        const covMax = feats.reduce((m,f)=>Math.max(m, Number(f.properties?.coverage||1)),1);
        if (ui.covSlider){
          ui.covSlider.max = String(Math.max(2, covMax));
          ui.covSlider.value = covMax >= 3 ? 2 : 1;
          ui.covLabel && (ui.covLabel.textContent = ui.covSlider.value);
          const f = [">=", ["get","coverage"], Number(ui.covSlider.value)];
          safe.filter("nearrep-fill", f);
          safe.filter("nearrep-outline", f);
        }
      } else {
        ui.badge && (ui.badge.textContent = "empty");
        ui.note  && (ui.note.textContent = "No near-repeat features in recent window.");
      }
    }

    // 初始视图：分位数 + 文案 + fit
    setHeatQuantilesFrom(heat0);
    refreshMetaFrom(near0, emerg0);
    fitToCurrent();

    // ---------- 交互：显隐 ----------
    const setVis = (id,on)=> map.setLayoutProperty(id,"visibility",on?"visible":"none");
    ui.toggleHeat?.addEventListener("change", ()=>{ setVis("heat90-fill",ui.toggleHeat.checked); setVis("heat90-outline",ui.toggleHeat.checked); });
    ui.toggleNear?.addEventListener("change", ()=>{ setVis("nearrep-fill",ui.toggleNear.checked); setVis("nearrep-outline",ui.toggleNear.checked); });
    ui.toggleEmerging?.addEventListener("change", ()=>{ setVis("emerging-fill",ui.toggleEmerging.checked); setVis("emerging-outline",ui.toggleEmerging.checked); });

    // Emerging 只看 New + Intensifying
    function applyEmergingFilter(onlyNI){
      const f = onlyNI ? ["match",["get","label"],["New","Intensifying"], true, false] : null;
      safe.filter("emerging-fill", f);
      safe.filter("emerging-outline", f);
    }
    if (ui.toggleEmergingNI){
      applyEmergingFilter(ui.toggleEmergingNI.checked);
      ui.toggleEmergingNI.addEventListener("change", ()=> applyEmergingFilter(ui.toggleEmergingNI.checked));
    }

    // coverage 滑块
    function applyCov(v){
      ui.covLabel && (ui.covLabel.textContent = String(v));
      const f = [">=", ["get","coverage"], Number(v)];
      safe.filter("nearrep-fill", f);
      safe.filter("nearrep-outline", f);
      saveURL();
    }
    if (ui.covSlider){
      ui.covSlider.addEventListener("input", e => applyCov(e.target.value));
      applyCov(ui.covSlider.value || 1);
    }

    // Hover 高亮
    map.on("mouseenter","nearrep-fill",()=> map.getCanvas().style.cursor="pointer");
    map.on("mouseleave","nearrep-fill",()=> map.getCanvas().style.cursor="");
    map.on("mousemove","nearrep-fill",()=> safe.paint("nearrep-outline","line-width",1.0));
    map.on("mouseleave","nearrep-fill",()=> safe.paint("nearrep-outline","line-width",0.6));

    map.on("mouseenter","emerging-fill",()=> map.getCanvas().style.cursor="pointer");
    map.on("mouseleave","emerging-fill",()=> map.getCanvas().style.cursor="");
    map.on("mousemove","emerging-fill",()=> safe.paint("emerging-outline","line-width",1.2));
    map.on("mouseleave","emerging-fill",()=> safe.paint("emerging-outline","line-width",0.8));

    // Popups
    map.on("click","nearrep-fill",(e)=>{
      const p = e.features[0].properties;
      new mapboxgl.Popup().setLngLat(e.lngLat)
        .setHTML(`<b>Coverage</b>: ${p.coverage}<br/><b>Global p</b>: ${Number(p.p_value||0).toFixed(3)}<br/><span class="muted">k=${p.k||1}, window=${p.window_days||14}d</span>`).addTo(map);
    });
    map.on("click","emerging-fill",(e)=>{
      const p = e.features[0].properties;
      new mapboxgl.Popup().setLngLat(e.lngLat)
        .setHTML(`<b>${p.label}</b><br/>recent=${Number(p.recent_mean).toFixed(2)}/w · baseline=${Number(p.baseline_mean).toFixed(2)}/w<br/>Δ=${Number(p.delta).toFixed(2)}, z≈${Number(p.z).toFixed(2)}<br/><span class="muted">anchor=${p.anchor_date}, windows=${p.recent_weeks}/${p.baseline_weeks}w</span>`).addTo(map);
    });

    // ---------- Offence 下拉：切换数据集 ----------
    async function switchDataset(kind){
      const ds = DATASETS[kind] || DATASETS.all;
      const [heatGJ, emergGJ, nearGJ] = await Promise.all([
        loadJSON(ds.heat), loadJSON(ds.emerg), loadJSON(ds.near)
      ]);
      map.getSource("heat90").setData(heatGJ || EMPTY);
      map.getSource("emerging").setData(emergGJ || EMPTY);
      map.getSource("nearrep").setData(nearGJ || EMPTY);
      currentData = { heat: heatGJ, emerg: emergGJ, near: nearGJ };
        setHeatQuantilesFrom(heatGJ || EMPTY);
        refreshMetaFrom(nearGJ || EMPTY, emergGJ || EMPTY);

        // 等 source 真正生效再 fit（更稳），同时有兜底
        map.once("idle", () => fitToCurrent());

        current = kind;
        saveURL();
    }
    ui.offenceSel?.addEventListener("change", (e)=> switchDataset(e.target.value));

    // ---------- Permalink（保存/恢复状态） ----------
    function saveURL(){
      const c = map.getCenter(), z = map.getZoom().toFixed(2);
      const params = new URLSearchParams({
        z, lng:c.lng.toFixed(5), lat:c.lat.toFixed(5),
        cov: ui.covSlider?.value || 1,
        near: ui.toggleNear?.checked ? 1 : 0,
        emer: ui.toggleEmerging?.checked ? 1 : 0,
        onlyNI: ui.toggleEmergingNI?.checked ? 1 : 0,
        heat: ui.toggleHeat?.checked ? 1 : 0,
        ds: current
      });
      history.replaceState(null,"",`?${params.toString()}`);
    }
    map.on("moveend", saveURL);
    [ui.covSlider, ui.toggleNear, ui.toggleEmerging, ui.toggleEmergingNI, ui.toggleHeat, ui.offenceSel]
      .forEach(el=> el && el.addEventListener("change", saveURL));

    (function loadURL(){
      const p = new URLSearchParams(location.search);
      if (p.has("z")){
        map.setZoom(Number(p.get("z")));
        map.setCenter([Number(p.get("lng")), Number(p.get("lat"))]);
      }
      const setChk=(el,key)=> el && (el.checked = p.get(key)==="1");
      setChk(ui.toggleNear,"near"); setChk(ui.toggleEmerging,"emer");
      setChk(ui.toggleEmergingNI,"onlyNI"); setChk(ui.toggleHeat,"heat");
      if (ui.covSlider && p.has("cov")) ui.covSlider.value = p.get("cov");
      const ds = p.get("ds");
      if (ds && ds!==current) switchDataset(ds);
    })();
  });
})();
