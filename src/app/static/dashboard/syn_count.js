let globalHist = []; // [{syn_count, count}, ...]
let byNeuropil = {}; // { neuropil: { total_pairs, histogram:[{syn_count,count},...] } }
let byNeurotransmitter = {}; // { nt: { total_pairs, histogram:[{syn_count,count},...] } }
let neuropils = []; // sorted
let neuropilIndex = {}; // neuropil -> index
let precomputed = {}; // neuropil -> {syn:[], suffix:[]}
let precomputedByNt = {}; // nt -> neuropil -> {syn:[], suffix:[]}
let selectedNt = null; // null means "all"
let selectedNeuropil = null; // null means "no neuropil filter"
let _pairsFetchTimer = null;
let _neuropilClickHandlerAttached = false;
let _neuropilHoverHandlerAttached = false;
let _lastNeuropilBaseColors = null;
let _lastNeuropilBaseLineWidths = null;
let _lastNeuropilBaseLineColors = null;
let _lastNeuropilHoverIndex = null;
let _summaryNts = null;

function _cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name);
  const s = String(v ?? "").trim();
  return s || fallback;
}

function _themeBg() {
  return _cssVar("--dash-bg", "#ECE8E4");
}

function _themeTextColor() {
  return _cssVar("--dash-text", "#505050");
}

function _themeFontFamily() {
  return _cssVar(
    "--dash-font",
    'system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif'
  );
}

function _initThemeToggle() {
  const key = "synCountTheme";
  const saved = (localStorage.getItem(key) || "").toLowerCase();
  const theme = saved === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = theme;

  const sw = document.getElementById("themeSwitch");
  if (sw) {
    sw.checked = theme === "dark";
    sw.addEventListener("change", () => {
      const nextTheme = sw.checked ? "dark" : "light";
      document.documentElement.dataset.theme = nextTheme;
      localStorage.setItem(key, nextTheme);

      // Re-render UI that uses theme colors.
      try {
        renderNeurotransmitters(_summaryNts);
      } catch (_) {
        // ignore
      }
      const slider = document.getElementById("thr");
      if (slider && Array.isArray(globalHist) && globalHist.length) {
        const thr = parseInt(slider.value, 10);
        _setSliderPct(slider);
        updateChart(Number.isFinite(thr) ? thr : 1);
      }
    });
  }
}

function _setSliderPct(sliderEl) {
  if (!sliderEl) return;
  const min = parseInt(sliderEl.min || "0", 10);
  const max = parseInt(sliderEl.max || "100", 10);
  const val = parseInt(sliderEl.value || String(min), 10);
  const denom = Math.max(1, max - min);
  const pct = ((val - min) / denom) * 100;
  sliderEl.style.setProperty("--slider-pct", `${Math.max(0, Math.min(100, pct))}%`);
}

function _scrollToId(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.scrollIntoView({ behavior: "smooth", block: "start" });
}

function _setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function _safeInt(x, fallback) {
  const n = parseInt(String(x ?? ""), 10);
  return Number.isFinite(n) ? n : fallback;
}

function _isNeurotransmitterColumn(colName) {
  const k = String(colName || "").toLowerCase();
  return k === "neurotransmitter" || k === "dominant_nt" || k.endsWith("_nt") || k === "nt";
}

function _formatNeurotransmitterHtml(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";

  // If multiple tied neurotransmitters are present (comma-separated), color each token.
  const parts = raw.split(",").map((s) => s.trim()).filter(Boolean);
  const nts = parts.length ? parts : [raw];

  return nts
    .map((nt) => {
      const color = (typeof NT_COLORS === "object" && NT_COLORS && NT_COLORS[nt]) ? NT_COLORS[nt] : null;
      const safe = _escapeHtml(nt);
      return color ? `<span style=\"color:${color}\">${safe}</span>` : safe;
    })
    .join(", ");
}

function _renderTable(elId, rows, opts) {
  const el = document.getElementById(elId);
  if (!el) return;

  const maxRows = opts?.maxRows ?? 200;
  if (!Array.isArray(rows) || rows.length === 0) {
    el.innerHTML = `<div style="opacity:0.75">(no results)</div>`;
    return;
  }

  const view = rows.slice(0, maxRows);
  const columns = opts?.columns ?? Array.from(
    view.reduce((acc, r) => {
      Object.keys(r || {}).forEach((k) => acc.add(k));
      return acc;
    }, new Set())
  );

  const headerStyle = "text-align:left;padding:6px 10px;white-space:nowrap";
  const cellStyle = "padding:6px 10px;white-space:nowrap";

  const html = [
    `<div style="opacity:0.75;margin-bottom:6px">` +
      (rows.length > maxRows ? `Showing first ${maxRows} of ${rows.length}.` : `Rows: ${rows.length}.`) +
    `</div>`,
    `<div style="overflow:auto">`,
    `<table style="border-collapse:collapse;width:100%">`,
    `<thead><tr>`,
    ...columns.map((c) => `<th style="${headerStyle}">${_escapeHtml(c)}</th>`),
    `</tr></thead>`,
    `<tbody>`,
  ];

  for (const r of view) {
    html.push("<tr>");
    for (const c of columns) {
      const v = (r || {})[c];
      const cellHtml = _isNeurotransmitterColumn(c)
        ? _formatNeurotransmitterHtml(v)
        : _escapeHtml(v ?? "");
      html.push(`<td style="${cellStyle}">${cellHtml}</td>`);
    }
    html.push("</tr>");
  }

  html.push(`</tbody></table></div>`);
  el.innerHTML = html.join("");
}

async function _fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }
  return await res.json();
}

function _buildNeuronSearchFilters() {
  // neurotransmitter select
  const ntSel = document.getElementById("nsNt");
  if (ntSel) {
    // keep the first option (all)
    const keepFirst = ntSel.querySelector("option[value='']");
    ntSel.innerHTML = "";
    if (keepFirst) ntSel.appendChild(keepFirst);
    for (const nt of ["GABA", "Acetylcholine", "Glutamate", "Octopamine", "Serotonin", "Dopamine"]) {
      const opt = document.createElement("option");
      opt.value = nt;
      opt.textContent = nt;
      ntSel.appendChild(opt);
    }
  }

  // neuropil select from summary keys
  const npSel = document.getElementById("nsNeuropil");
  if (npSel) {
    const keepFirst = npSel.querySelector("option[value='']");
    npSel.innerHTML = "";
    if (keepFirst) npSel.appendChild(keepFirst);
    for (const np of neuropils) {
      const opt = document.createElement("option");
      opt.value = np;
      opt.textContent = np;
      npSel.appendChild(opt);
    }
  }

  // default threshold to match overview slider (if present)
  const thrSlider = document.getElementById("thr");
  const thrInput = document.getElementById("nsThreshold");
  if (thrSlider && thrInput) {
    thrInput.value = String(_safeInt(thrSlider.value, 0));
  }
}

async function runNeuronSearch() {
  const statusEl = document.getElementById("nsStatus");
  const rootId = (document.getElementById("nsRootId")?.value ?? "").trim();
  const threshold = _safeInt(document.getElementById("nsThreshold")?.value, 0);
  const k = Math.min(5, Math.max(1, _safeInt(document.getElementById("nsK")?.value, 3)));
  const nt = (document.getElementById("nsNt")?.value ?? "").trim();
  const np = (document.getElementById("nsNeuropil")?.value ?? "").trim();

  if (!rootId) {
    if (statusEl) statusEl.textContent = "Please enter a neuron root id.";
    return;
  }
  if (!/^\d+$/.test(rootId)) {
    if (statusEl) statusEl.textContent = "root id must be an integer.";
    return;
  }

  const base = `/neuron/${encodeURIComponent(rootId)}`;
  const params = new URLSearchParams();
  params.set("threshold", String(threshold));
  if (nt) params.set("neurotransmitter", nt);
  if (np) params.set("neuropil", np);

  const urls = {
    presyn: `${base}/presynaptic?${params.toString()}`,
    postsyn: `${base}/postsynaptic?${params.toString()}`,
    twoUp: `${base}/two_hop_upstream?${params.toString()}`,
    twoDown: `${base}/two_hop_downstream?${params.toString()}`,
    kUp: `${base}/k_hop_upstream?${params.toString()}&k=${k}`,
    kDown: `${base}/k_hop_downstream?${params.toString()}&k=${k}`,
    circuit: `${base}/circuit?${params.toString()}&k=${k}`,
  };

  const label = `root_id=${rootId}, threshold≥${threshold}` + (nt ? `, nt=${nt}` : "") + (np ? `, neuropil=${np}` : "") + `, k=${k}`;
  if (statusEl) statusEl.textContent = `Running queries (${label})...`;

  // Clear counts quickly
  _setText("nsPresynCount", "");
  _setText("nsPostsynCount", "");
  _setText("ns2UpCount", "");
  _setText("ns2DownCount", "");
  _setText("nsKUpCount", "");
  _setText("nsKDownCount", "");
  _setText("nsCircuitCount", "");

  const tasks = Object.entries(urls).map(async ([key, url]) => {
    try {
      const data = await _fetchJson(url);
      return { key, ok: true, data };
    } catch (e) {
      return { key, ok: false, error: String(e?.message ?? e) };
    }
  });

  const results = await Promise.all(tasks);

  // presyn/postsyn/two-hop are lists
  const presyn = results.find((r) => r.key === "presyn");
  if (presyn?.ok) {
    _setText("nsPresynCount", `(${presyn.data.length})`);
    _renderTable("nsPresyn", presyn.data, { maxRows: 200 });
  } else {
    _setText("nsPresynCount", "(error)");
    document.getElementById("nsPresyn").innerHTML = `<div style="color:#b91c1c">${_escapeHtml(presyn?.error ?? "error")}</div>`;
  }

  const postsyn = results.find((r) => r.key === "postsyn");
  if (postsyn?.ok) {
    _setText("nsPostsynCount", `(${postsyn.data.length})`);
    _renderTable("nsPostsyn", postsyn.data, { maxRows: 200 });
  } else {
    _setText("nsPostsynCount", "(error)");
    document.getElementById("nsPostsyn").innerHTML = `<div style="color:#b91c1c">${_escapeHtml(postsyn?.error ?? "error")}</div>`;
  }

  const twoUp = results.find((r) => r.key === "twoUp");
  if (twoUp?.ok) {
    _setText("ns2UpCount", `(${twoUp.data.length})`);
    _renderTable("ns2Up", twoUp.data, { maxRows: 200 });
  } else {
    _setText("ns2UpCount", "(error)");
    document.getElementById("ns2Up").innerHTML = `<div style="color:#b91c1c">${_escapeHtml(twoUp?.error ?? "error")}</div>`;
  }

  const twoDown = results.find((r) => r.key === "twoDown");
  if (twoDown?.ok) {
    _setText("ns2DownCount", `(${twoDown.data.length})`);
    _renderTable("ns2Down", twoDown.data, { maxRows: 200 });
  } else {
    _setText("ns2DownCount", "(error)");
    document.getElementById("ns2Down").innerHTML = `<div style="color:#b91c1c">${_escapeHtml(twoDown?.error ?? "error")}</div>`;
  }

  // k-hop paths are list of {nodes, edges}; show small preview table with nodes count and first/last id
  function pathPreview(paths) {
    const view = (paths || []).slice(0, 50).map((p) => {
      const nodes = p?.nodes ?? [];
      return {
        hops: Math.max(0, nodes.length - 1),
        start: nodes[0],
        end: nodes[nodes.length - 1],
        nodes: nodes.length,
        edges: (p?.edges ?? []).length,
      };
    });
    return view;
  }

  const kUp = results.find((r) => r.key === "kUp");
  if (kUp?.ok) {
    _setText("nsKUpCount", `(${kUp.data.length})`);
    _renderTable("nsKUp", pathPreview(kUp.data), { maxRows: 50, columns: ["hops", "start", "end", "nodes", "edges"] });
  } else {
    _setText("nsKUpCount", "(error)");
    document.getElementById("nsKUp").innerHTML = `<div style="color:#b91c1c">${_escapeHtml(kUp?.error ?? "error")}</div>`;
  }

  const kDown = results.find((r) => r.key === "kDown");
  if (kDown?.ok) {
    _setText("nsKDownCount", `(${kDown.data.length})`);
    _renderTable("nsKDown", pathPreview(kDown.data), { maxRows: 50, columns: ["hops", "start", "end", "nodes", "edges"] });
  } else {
    _setText("nsKDownCount", "(error)");
    document.getElementById("nsKDown").innerHTML = `<div style="color:#b91c1c">${_escapeHtml(kDown?.error ?? "error")}</div>`;
  }

  // circuit response: {circuits:[{connections:[...]}]}
  const circuit = results.find((r) => r.key === "circuit");
  if (circuit?.ok) {
    const connections = circuit.data?.circuits?.[0]?.connections ?? [];
    _setText("nsCircuitCount", `(${connections.length})`);
    _renderTable("nsCircuit", connections, { maxRows: 200 });
  } else {
    _setText("nsCircuitCount", "(error)");
    document.getElementById("nsCircuit").innerHTML = `<div style="color:#b91c1c">${_escapeHtml(circuit?.error ?? "error")}</div>`;
  }

  if (statusEl) statusEl.textContent = `Done (${label}).`;
}

const NT_COLORS = {
  GABA: "#79AEDA",
  Acetylcholine: "#F2A65A",
  Glutamate: "#79BFA3",
  Octopamine: "#E58B8B",
  Serotonin: "#A08CCF",
  Dopamine: "#B4977A",
};

const NT_COLORS_HOVER = {
  GABA: "#5FA6E6",
  Acetylcholine: "#E97C1F",
  Glutamate: "#5FBF9A",
  Octopamine: "#E56F6F",
  Serotonin: "#9076D8",
  Dopamine: "#B6865A",
};

function _schedulePairsFetch(threshold) {
  if (_pairsFetchTimer) {
    clearTimeout(_pairsFetchTimer);
    _pairsFetchTimer = null;
  }
  _pairsFetchTimer = setTimeout(() => {
    _pairsFetchTimer = null;
    fetchAndRenderPairs(threshold);
  }, 250);
}

function _ntToColor(nt) {
  if (!nt) return "#6b7280";
  // If multiple tied neurotransmitters are present (comma-separated), keep it neutral.
  if (String(nt).includes(",")) return "#6b7280";
  return NT_COLORS[nt] ?? "#6b7280";
}

async function fetchAndRenderPairs(threshold) {
  const tableEl = document.getElementById("pairsTable");
  const hintEl = document.getElementById("pairsHint");
  if (!tableEl || !hintEl) return;

  if (!selectedNeuropil) {
    hintEl.textContent = "Click a neuropil circle to filter the histogram and list pairs.";
    tableEl.innerHTML = "";
    return;
  }

  const params = new URLSearchParams();
  params.set("threshold", String(threshold));
  params.set("neuropil", selectedNeuropil);
  if (selectedNt) params.set("neurotransmitter", selectedNt);
  const limit = 200;
  params.set("limit", String(limit));

  hintEl.textContent = "Loading pairs…";
  tableEl.innerHTML = "";

  let rows;
  try {
    const res = await fetch(`/dataset/pairs?${params.toString()}`);
    if (!res.ok) {
      const text = await res.text();
      hintEl.textContent = `Failed to load pairs: ${text}`;
      return;
    }
    rows = await res.json();
  } catch (e) {
    hintEl.textContent = `Failed to load pairs: ${String(e)}`;
    return;
  }

  const filterText = `neuropil=${selectedNeuropil}, syn_count≥${threshold}` + (selectedNt ? `, dominant=${selectedNt}` : "");
  if (rows.length >= limit) {
    hintEl.textContent = `Showing top ${limit} pairs (${filterText}).`;
  } else {
    hintEl.textContent = `Found ${rows.length} pairs (${filterText}).`;
  }

  const headerStyle = "text-align:left;padding:6px 10px;white-space:nowrap";
  const cellStyle = "padding:6px 10px;white-space:nowrap";

  const html = [
    `<table style="border-collapse:collapse;width:100%">`,
    `<thead><tr>`,
    `<th style="${headerStyle}">presynaptic neuron id</th>`,
    `<th style="${headerStyle}">postsynaptic neuron id</th>`,
    `<th style="${headerStyle}">synapse count</th>`,
    `<th style="${headerStyle}">dominant neurotransmitter</th>`,
    `</tr></thead>`,
    `<tbody>`,
  ];

  for (const r of rows) {
    const nt = r?.dominant_nt ?? "";
    const color = _ntToColor(nt);
    html.push(
      `<tr>` +
        `<td style="${cellStyle}">${_escapeHtml(r?.pre_id ?? "")}</td>` +
        `<td style="${cellStyle}">${_escapeHtml(r?.post_id ?? "")}</td>` +
        `<td style="${cellStyle}">${_escapeHtml(r?.syn_count ?? "")}</td>` +
        `<td style="${cellStyle};color:${color};font-weight:600">${_escapeHtml(nt)}</td>` +
      `</tr>`
    );
  }

  html.push(`</tbody></table>`);
  tableEl.innerHTML = html.join("");
}

function _escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderNeurotransmitters(list) {
  const el = document.getElementById("neurotransmitters");
  if (!el) return;
  const nts = Array.isArray(list) && list.length ? list : Object.keys(NT_COLORS);

  const pills = [
    {
      key: "__all__",
      label: "all",
      color: "#808080",
      title: "Show all neurotransmitters",
      isSelected: selectedNt === null,
    },
    ...nts.map((nt) => ({
      key: nt,
      label: nt,
      color: NT_COLORS[nt] ?? "#6b7280",
      title: `Click to filter charts by ${nt}`,
      isSelected: selectedNt === nt,
    })),
  ];

  el.innerHTML = pills
    .map((p) => {
      const border = "none";
      const opacity = selectedNt !== null && !p.isSelected ? "0.35" : "1";
      return `
        <button
          class="ntPill"
          data-nt="${_escapeHtml(p.key)}"
          title="${_escapeHtml(p.title)}"
          style="cursor:pointer;display:inline-block;padding:2px 10px;border-radius:999px;border:${border};background:${p.color};color:white;font-size:12px;opacity:${opacity}"
        >${_escapeHtml(p.label)}</button>
      `.trim();
    })
    .join("");

  el.querySelectorAll(".ntPill").forEach((btn) => {
    btn.addEventListener("click", () => {
      const nt = btn.getAttribute("data-nt");
      if (!nt) return;
      if (nt === "__all__") {
        selectedNt = null;
      } else {
        selectedNt = selectedNt === nt ? null : nt;
      }
      renderNeurotransmitters(nts);
      const slider = document.getElementById("thr");
      const thr = slider ? parseInt(slider.value, 10) : 1;
      updateChart(thr);
    });
  });
}

function _binarySearchFirstGte(arr, x) {
  // arr is sorted ascending
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] < x) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function _pairsAtOrAbove(neuropil, threshold) {
  const p = precomputed[neuropil];
  if (!p) return 0;
  const idx = _binarySearchFirstGte(p.syn, threshold);
  if (idx >= p.syn.length) return 0;
  return p.suffix[idx];
}

function _dominantNtColor(nt) {
  return NT_COLORS[nt] ?? "#6b7280";
}

function _dominantNtHoverColor(nt) {
  return NT_COLORS_HOVER[nt] ?? _dominantNtColor(nt);
}

function _dominantNtForNeuropil(neuropil, threshold) {
  const entry = byNeuropil?.[neuropil];
  const byNt = entry?.by_neurotransmitter;
  if (!byNt) return null;

  let bestNt = null;
  let bestCount = -1;
  // deterministic order for tie-breaking
  const order = ["GABA", "Acetylcholine", "Glutamate", "Octopamine", "Serotonin", "Dopamine"];
  for (const nt of order) {
    const hist = byNt?.[nt]?.histogram ?? [];
    if (!hist.length) continue;
    // compute tail count >= threshold
    // build syn sorted & suffix on the fly (hist is small), avoids bigger precompute complexity
    const syn = hist.map((x) => x.syn_count).sort((a, b) => a - b);
    const cMap = new Map();
    for (const item of hist) cMap.set(item.syn_count, item.count);
    const counts = syn.map((s) => cMap.get(s) ?? 0);
    let running = 0;
    const suffix = new Array(counts.length);
    for (let i = counts.length - 1; i >= 0; i--) {
      running += counts[i];
      suffix[i] = running;
    }
    const idx = _binarySearchFirstGte(syn, threshold);
    const tail = idx >= syn.length ? 0 : suffix[idx];
    if (tail > bestCount) {
      bestCount = tail;
      bestNt = nt;
    }
  }
  return bestNt;
}

function updateChart(threshold) {
  document.getElementById("thrLabel").textContent = String(threshold);

  const DASH_BG = _themeBg();
  const DASH_TEXT = _themeTextColor();
  const DASH_FONT = _themeFontFamily();

  const activeNt = selectedNt;
  const activeNeuropil = selectedNeuropil;

  let histForBar = globalHist;
  if (activeNeuropil) {
    const npEntry = byNeuropil?.[activeNeuropil] ?? {};
    if (activeNt) {
      histForBar = npEntry?.by_neurotransmitter?.[activeNt]?.histogram ?? [];
    } else {
      histForBar = npEntry?.histogram ?? [];
    }
  } else if (activeNt) {
    histForBar = byNeurotransmitter?.[activeNt]?.histogram ?? [];
  }

  // ----- Bar chart: global syn_count distribution filtered to >= threshold -----
  const xs = [];
  const ys = [];
  let totalTail = 0;
  for (const item of histForBar) {
    if (item.syn_count >= threshold) {
      xs.push(item.syn_count);
      ys.push(item.count);
      totalTail += item.count;
    }
  }
  document.getElementById("pairsLabel").textContent = String(totalTail);

  Plotly.react(
    "histChart",
    [
      {
        type: "bar",
        x: xs,
        y: ys,
        marker: { color: activeNt ? _dominantNtColor(activeNt) : "#808080" },
      },
    ],
    {
      margin: { t: 20, r: 20, b: 60, l: 60 },
      paper_bgcolor: DASH_BG,
      plot_bgcolor: DASH_BG,
      font: { color: DASH_TEXT, family: DASH_FONT },
      hoverlabel: { font: { color: DASH_TEXT, family: DASH_FONT }, bordercolor: "rgba(0,0,0,0)", borderwidth: 0 },
      xaxis: {
        title: activeNt
          ? `synapse count (dominant: ${activeNt}; filtered to ≥ threshold)`
          : "synapse count (filtered to ≥ threshold)",
      },
      yaxis: { title: "pair count (auto-scaled)" },
      height: 520,
    },
    { displayModeBar: false }
  );

  // ----- Circle chart: per-neuropil tail counts -----
  const counts = [];
  for (const np of neuropils) {
    if (!activeNt) {
      counts.push(_pairsAtOrAbove(np, threshold));
      continue;
    }
    const p = precomputedByNt?.[activeNt]?.[np];
    if (!p) {
      counts.push(0);
      continue;
    }
    const idx = _binarySearchFirstGte(p.syn, threshold);
    counts.push(idx >= p.syn.length ? 0 : p.suffix[idx]);
  }

  const maxCount = Math.max(0, ...counts);
  const minSize = 14;
  const maxSize = 72;

  const sizes = counts.map((c) => {
    if (maxCount <= 0) return minSize;
    // Diameter ∝ sqrt(pairs)  =>  area ∝ pairs
    const s = minSize + (maxSize - minSize) * Math.sqrt(c / maxCount);
    return Math.max(minSize, s);
  });

  const opacities = counts.map((c) => (c > 0 ? 0.95 : 0.2));
  const colors = activeNt
    ? neuropils.map((np) => {
        // Keep selected circle in the same (brighter) hover color.
        if (activeNeuropil && np === activeNeuropil) return _dominantNtHoverColor(activeNt);
        return _dominantNtColor(activeNt);
      })
    : neuropils.map((np) => {
        const nt = _dominantNtForNeuropil(np, threshold);
        if (activeNeuropil && np === activeNeuropil) return nt ? _dominantNtHoverColor(nt) : "#6b7280";
        return nt ? _dominantNtColor(nt) : "#6b7280";
      });

  // Save base colors so hover can temporarily brighten a single circle.
  _lastNeuropilBaseColors = colors;
  _lastNeuropilBaseLineWidths = null;
  _lastNeuropilBaseLineColors = null;
  _lastNeuropilHoverIndex = null;

  const lineWidths = neuropils.map((np) => (activeNeuropil && np === activeNeuropil ? 2 : 0));
  const lineColors = neuropils.map((np) => {
    if (!(activeNeuropil && np === activeNeuropil)) return "rgba(0,0,0,0)";
    const ntForBorder = activeNt ? activeNt : _dominantNtForNeuropil(np, threshold);
    return ntForBorder ? _dominantNtHoverColor(ntForBorder) : _themeTextColor();
  });

  // Save base lines so hover can temporarily suppress borders.
  _lastNeuropilBaseLineWidths = lineWidths;
  _lastNeuropilBaseLineColors = lineColors;

  const hover = neuropils.map((np, i) => {
    if (activeNt) {
      return `${np}<br>dominant: ${activeNt}<br>pairs ≥ ${threshold}: ${counts[i]}`;
    }
    const nt = _dominantNtForNeuropil(np, threshold);
    return `${np}<br>pairs ≥ ${threshold}: ${counts[i]}${nt ? `<br>dominant: ${nt}` : ""}`;
  });

  // Layout circles on a simple grid.
  const cols = 10;
  const gridX = neuropils.map((_, i) => i % cols);
  const gridY = neuropils.map((_, i) => Math.floor(i / cols));
  const rows = Math.max(1, Math.ceil(neuropils.length / cols));

  Plotly.react(
    "neuropilChart",
    [
      {
        type: "scatter",
        mode: "markers",
        x: gridX,
        y: gridY,
        text: hover,
        hoverinfo: "text",
        marker: {
          symbol: "circle",
          size: sizes,
          color: colors,
          opacity: opacities,
          line: { width: lineWidths, color: lineColors },
        },
      },
    ],
    {
      margin: { t: 10, r: 10, b: 10, l: 10 },
      paper_bgcolor: DASH_BG,
      plot_bgcolor: DASH_BG,
      font: { color: DASH_TEXT, family: DASH_FONT },
      hoverlabel: { font: { color: DASH_TEXT, family: DASH_FONT }, bordercolor: "rgba(0,0,0,0)", borderwidth: 0 },
      // Fix axis ranges so changes in marker size do NOT trigger autoscaling
      // (autoscaling causes apparent "jitter" of marker positions).
      xaxis: { visible: false, fixedrange: true, autorange: false, range: [-0.5, cols - 0.5] },
      yaxis: { visible: false, fixedrange: true, autorange: false, range: [rows - 0.5, -0.5] },
      showlegend: false,
      height: 560,
      uirevision: "neuropil-static-grid",
    },
    { displayModeBar: false }
  );

  // Click-to-filter on neuropil circles.
  const plotEl = document.getElementById("neuropilChart");
  if (plotEl && !_neuropilClickHandlerAttached) {
    _neuropilClickHandlerAttached = true;
    plotEl.on("plotly_click", (eventData) => {
      const pt = eventData?.points?.[0];
      const idx = pt?.pointIndex;
      if (typeof idx !== "number") return;
      const np = neuropils[idx];
      if (!np) return;
      selectedNeuropil = selectedNeuropil === np ? null : np;
      const slider = document.getElementById("thr");
      const thr = slider ? parseInt(slider.value, 10) : threshold;
      updateChart(thr);
    });
  }

  // Hover-to-brighten the pointed circle only.
  if (plotEl && !_neuropilHoverHandlerAttached) {
    _neuropilHoverHandlerAttached = true;

    plotEl.on("plotly_hover", (eventData) => {
      const pt = eventData?.points?.[0];
      const idx = pt?.pointIndex;
      if (typeof idx !== "number") return;
      if (!_lastNeuropilBaseColors || idx < 0 || idx >= _lastNeuropilBaseColors.length) return;
      if (_lastNeuropilHoverIndex === idx) return;

      if (
        !_lastNeuropilBaseLineWidths ||
        !_lastNeuropilBaseLineColors ||
        idx >= _lastNeuropilBaseLineWidths.length ||
        idx >= _lastNeuropilBaseLineColors.length
      ) {
        return;
      }

      const slider = document.getElementById("thr");
      const thr = slider ? parseInt(slider.value, 10) : threshold;
      const np = neuropils[idx];
      const nt = selectedNt ? selectedNt : _dominantNtForNeuropil(np, thr);
      const hoverColor = nt ? _dominantNtHoverColor(nt) : "#6b7280";

      const newColors = _lastNeuropilBaseColors.slice();
      newColors[idx] = hoverColor;

      // Ensure hovered circle has no border (even if it is the selected neuropil).
      const newLineWidths = _lastNeuropilBaseLineWidths.slice();
      const newLineColors = _lastNeuropilBaseLineColors.slice();
      newLineWidths[idx] = 0;
      newLineColors[idx] = "rgba(0,0,0,0)";

      _lastNeuropilHoverIndex = idx;
      Plotly.restyle("neuropilChart", {
        "marker.color": [newColors],
        "marker.line.width": [newLineWidths],
        "marker.line.color": [newLineColors],
      });
    });

    plotEl.on("plotly_unhover", () => {
      if (!_lastNeuropilBaseColors) return;
      if (!_lastNeuropilBaseLineWidths || !_lastNeuropilBaseLineColors) return;
      if (_lastNeuropilHoverIndex === null) return;
      _lastNeuropilHoverIndex = null;
      Plotly.restyle("neuropilChart", {
        "marker.color": [_lastNeuropilBaseColors],
        "marker.line.width": [_lastNeuropilBaseLineWidths],
        "marker.line.color": [_lastNeuropilBaseLineColors],
      });
    });
  }

  _schedulePairsFetch(threshold);
}

async function init() {
  _initThemeToggle();

  const res = await fetch("/dataset/summary");
  if (!res.ok) {
    const text = await res.text();
    document.getElementById("histChart").textContent =
      "Failed to load /dataset/summary: " + text;
    return;
  }

  const data = await res.json();
  _summaryNts = data?.neurotransmitters ?? null;
  globalHist = data?.syn_count?.histogram ?? [];
  byNeuropil = data?.by_neuropil ?? {};
  byNeurotransmitter = data?.by_neurotransmitter ?? {};

  // Top summary
  const uniq = data?.unique ?? {};
  document.getElementById("totalNeurons").textContent = String(uniq?.neuron_ids_union ?? "-");
  document.getElementById("preNeurons").textContent = String(uniq?.pre_pt_root_id ?? "-");
  document.getElementById("postNeurons").textContent = String(uniq?.post_pt_root_id ?? "-");
  document.getElementById("totalPairs").textContent = String(data?.total_rows ?? "-");
  document.getElementById("neuropilCount").textContent = String(uniq?.neuropil ?? "-");
  renderNeurotransmitters(_summaryNts);

  if (!globalHist || globalHist.length === 0) {
    document.getElementById("histChart").textContent =
      "Missing 'syn_count.histogram' in /dataset/summary. Re-generate summary JSON with summarize_data.py (use --out-json).";
    return;
  }

  if (!byNeuropil || Object.keys(byNeuropil).length === 0) {
    document.getElementById("neuropilChart").textContent =
      "Missing 'by_neuropil' in /dataset/summary. Re-generate summary JSON with the latest summarize_data.py (use --out-json).";
    return;
  }

  // If neurotransmitter breakdown is missing, we can still render charts (all circles gray)
  // but dynamic coloring by dominant neurotransmitter will be unavailable.
  const anyNeuropil = Object.keys(byNeuropil)[0];
  const hasNtBreakdown = !!byNeuropil?.[anyNeuropil]?.by_neurotransmitter;
  if (!hasNtBreakdown) {
    // Keep going; user may be using an older summary JSON.
    const note =
      "Note: summary JSON missing 'by_neurotransmitter' (regenerate with updated summarize_data.py for dynamic colors).";
    // put the note above the neuropil chart without breaking layout
    const el = document.getElementById("neuropilChart");
    if (el) el.setAttribute("data-note", note);
  }

  neuropils = Object.keys(byNeuropil).sort();
  neuropilIndex = {};
  neuropils.forEach((np, i) => (neuropilIndex[np] = i));

  // Nav buttons + neuron search panel wiring
  const btnOverview = document.getElementById("btnOverview");
  if (btnOverview) {
    btnOverview.addEventListener("click", () => {
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  }
  const btnNeuronSearch = document.getElementById("btnNeuronSearch");
  if (btnNeuronSearch) btnNeuronSearch.addEventListener("click", () => _scrollToId("neuronSearchPanel"));

  _buildNeuronSearchFilters();
  const runBtn = document.getElementById("nsRun");
  if (runBtn) runBtn.addEventListener("click", () => runNeuronSearch());
  const rootInput = document.getElementById("nsRootId");
  if (rootInput) {
    rootInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") runNeuronSearch();
    });
  }

  // Precompute suffix sums for each neuropil histogram.
  precomputed = {};
  for (const np of neuropils) {
    const hist = byNeuropil[np]?.histogram ?? [];
    const syn = hist.map((x) => x.syn_count).sort((a, b) => a - b);
    // Build map to counts to avoid relying on histogram order.
    const cMap = new Map();
    for (const item of hist) cMap.set(item.syn_count, item.count);
    const counts = syn.map((s) => cMap.get(s) ?? 0);

    const suffix = new Array(counts.length);
    let running = 0;
    for (let i = counts.length - 1; i >= 0; i--) {
      running += counts[i];
      suffix[i] = running;
    }
    precomputed[np] = { syn, suffix };
  }

  // Precompute suffix sums per neurotransmitter per neuropil for fast filtering.
  precomputedByNt = {};
  const nts = data?.neurotransmitters ?? Object.keys(NT_COLORS);
  for (const nt of nts) {
    precomputedByNt[nt] = {};
  }
  for (const np of neuropils) {
    const byNt = byNeuropil?.[np]?.by_neurotransmitter ?? {};
    for (const nt of Object.keys(precomputedByNt)) {
      const hist = byNt?.[nt]?.histogram ?? [];
      if (!hist.length) {
        precomputedByNt[nt][np] = { syn: [], suffix: [] };
        continue;
      }
      const syn = hist.map((x) => x.syn_count).sort((a, b) => a - b);
      const cMap = new Map();
      for (const item of hist) cMap.set(item.syn_count, item.count);
      const counts = syn.map((s) => cMap.get(s) ?? 0);
      const suffix = new Array(counts.length);
      let running = 0;
      for (let i = counts.length - 1; i >= 0; i--) {
        running += counts[i];
        suffix[i] = running;
      }
      precomputedByNt[nt][np] = { syn, suffix };
    }
  }

  const maxSyn = data?.syn_count?.max ?? 1;
  const slider = document.getElementById("thr");
  slider.max = String(maxSyn);
  slider.value = "1";

  _setSliderPct(slider);

  slider.addEventListener("input", (e) => {
    const thr = parseInt(e.target.value, 10);
    _setSliderPct(e.target);
    updateChart(thr);
  });

  updateChart(1);
}

init();
