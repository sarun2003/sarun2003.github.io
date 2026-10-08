/* ==========================================================================
   Project dashboards: KPI tiles, charts (Apache ECharts), tables.
   Reads ./data.json next to the page. Plain JavaScript, no other dependencies.
   ========================================================================== */
(function () {
  'use strict';

  initCodeBlocks();

  var root = document.querySelector('[data-dashboard]');
  if (!root) return;

  var reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /* ---- Design tokens (dark dashboard surface; categorical order validated) ---- */
  var C = {
    surface: '#071a1e', surface2: '#0b2328',
    ink: '#faf9f6', ink2: '#c3c9c8', muted: '#8a9594',
    grid: '#13272b', axis: '#2a3d41', pointer: '#4a5d61',
    series: ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767'],
    seq: ['#0d366b', '#104281', '#184f95', '#1c5cab', '#256abf', '#2a78d6', '#3987e5', '#5598e7', '#6da7ec', '#86b6ef', '#9ec5f4', '#b7d3f6'],
    ordinal: ['#9ec5f4', '#6da7ec', '#3987e5', '#256abf', '#184f95'],
    band: '#8a9594',
    context: '#56696d',
    status: { good: '#0ca30c', warning: '#fab219', serious: '#ec835a', critical: '#d03b3b', neutral: '#5b6b6e' },
    sans: '"Plus Jakarta Sans", system-ui, -apple-system, "Segoe UI", sans-serif',
    mono: '"JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace'
  };
  var STATUS_LABEL = { good: 'Healthy', warning: 'Warning', serious: 'Degraded', critical: 'Failed', neutral: 'Skipped' };

  var ICON = {
    good: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><circle cx="8" cy="8" r="6.6"/><path d="M5.2 8.2l1.9 1.9 3.7-3.9" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    warning: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M8 1.8l6.6 11.6H1.4z" stroke-linejoin="round"/><path d="M8 6.2v3.3" stroke-linecap="round"/><circle cx="8" cy="11.4" r=".5" fill="currentColor"/></svg>',
    serious: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M5.3 1.8h5.4l3.5 3.5v5.4l-3.5 3.5H5.3l-3.5-3.5V5.3z" stroke-linejoin="round"/><path d="M8 4.9v3.6" stroke-linecap="round"/><circle cx="8" cy="10.9" r=".5" fill="currentColor"/></svg>',
    critical: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><circle cx="8" cy="8" r="6.6"/><path d="M5.7 5.7l4.6 4.6M10.3 5.7l-4.6 4.6" stroke-linecap="round"/></svg>',
    neutral: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><circle cx="8" cy="8" r="6.6"/><path d="M5.2 8h5.6" stroke-linecap="round"/></svg>',
    up: '<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M6 10V2.5M2.8 5.5L6 2.3l3.2 3.2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    down: '<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path d="M6 2v7.5M2.8 6.5L6 9.7l3.2-3.2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    table: '<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.3" aria-hidden="true"><rect x="1.5" y="2" width="9" height="8" rx="1.2"/><path d="M1.5 5h9M1.5 7.6h9M5 5v5"/></svg>',
    chart: '<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.3" aria-hidden="true"><path d="M1.5 10.2h9" stroke-linecap="round"/><path d="M3 8.2V5.6M6 8.2V2.6M9 8.2V4.4" stroke-linecap="round"/></svg>'
  };

  /* ---- Formatting ---- */
  var nf0 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
  var nf1 = new Intl.NumberFormat('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  var nf2 = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  function trimZeros(s) { return s.replace(/\.0+([A-Za-z]*)$/, '$1').replace(/(\.\d*[1-9])0+([A-Za-z]*)$/, '$1$2'); }
  function compact(v) {
    var a = Math.abs(v);
    if (a >= 1e9) return trimZeros((v / 1e9).toFixed(2)) + 'B';
    if (a >= 1e8) return trimZeros((v / 1e6).toFixed(1)) + 'M';
    if (a >= 1e6) return trimZeros((v / 1e6).toFixed(2)) + 'M';
    if (a >= 1e4) return trimZeros((v / 1e3).toFixed(1)) + 'K';
    return nf0.format(v);
  }
  function duration(sec) {
    sec = Math.round(sec);
    if (sec < 60) return sec + 's';
    var h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = sec % 60;
    if (h) return h + 'h ' + m + 'm';
    if (!s) return m + 'm';
    return m + 'm ' + (s < 10 ? '0' : '') + s + 's';
  }
  function fmt(v, f) {
    if (v === null || v === undefined || v === '' || (typeof v === 'number' && isNaN(v))) return '–';
    if (typeof v !== 'number') return String(v);
    switch (f) {
      case 'int': return nf0.format(v);
      case 'compact': return compact(v);
      case 'usd': return (v < 0 ? '−$' : '$') + (Math.abs(v) >= 1e4 ? compact(Math.abs(v)) : nf0.format(Math.abs(v)));
      case 'usd2': return (v < 0 ? '−$' : '$') + nf2.format(Math.abs(v));
      case 'pct': return nf1.format(v * 100) + '%';
      case 'pct2': return nf2.format(v * 100) + '%';
      case 'pct0': return nf0.format(v * 100) + '%';
      case 'sec': return nf1.format(v) + ' s';
      case 'ms': return nf0.format(v) + ' ms';
      case 'min': return nf0.format(v) + ' min';
      case 'dur': return duration(v);
      case 'gb': return nf1.format(v) + ' GB';
      case 'mb': return nf0.format(v) + ' MB';
      case 'tb': return nf2.format(v) + ' TB';
      case 'ratio': return nf2.format(v);
      case 'dec1': return nf1.format(v);
      case 'x': return nf1.format(v) + '×';
      case 'persec': return nf0.format(v) + '/s';
      default: return nf0.format(v);
    }
  }
  function axisFmt(f) {
    return function (v) {
      if (f === 'pct2') return trimZeros(nf2.format(v * 100)) + '%';
      if (f === 'pct' || f === 'pct0') return (Math.abs(v) < 0.1 && Math.round(v * 1000) % 10 ? nf1 : nf0).format(v * 100) + '%';
      if (f === 'usd' || f === 'usd2') return '$' + compact(v);
      if (f === 'sec') return nf0.format(v) + ' s';
      if (f === 'ms') return nf0.format(v) + ' ms';
      if (f === 'min') return nf0.format(v) + 'm';
      if (f === 'ratio') return nf2.format(v);
      if (f === 'dur') return duration(v);
      return compact(v);
    };
  }
  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text !== undefined) n.textContent = text;
    return n;
  }
  function statusEl(s, label) {
    var span = el('span', 'status is-' + s);
    span.innerHTML = ICON[s] || ICON.neutral; // static, trusted markup
    span.appendChild(document.createTextNode(label || STATUS_LABEL[s] || s));
    return span;
  }

  /* ---- KPI tiles ---- */
  /* A sparkline that stretches to the tile width: the line scales, the end dot stays round. */
  function sparkline(values) {
    var h = 28, pad = 3;
    var min = Math.min.apply(null, values), max = Math.max.apply(null, values);
    var span = max - min || 1;
    var pts = values.map(function (v, i) {
      return [(i / (values.length - 1)) * 97, h - pad - ((v - min) / span) * (h - pad * 2)];
    });
    var d = pts.map(function (p, i) { return (i ? 'L' : 'M') + p[0].toFixed(2) + ' ' + p[1].toFixed(2); }).join(' ');
    var ns = 'http://www.w3.org/2000/svg';
    var wrap = el('div', 'kpi-spark');
    wrap.setAttribute('aria-hidden', 'true');
    var svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('viewBox', '0 0 100 ' + h);
    svg.setAttribute('preserveAspectRatio', 'none');
    var path = document.createElementNS(ns, 'path');
    path.setAttribute('d', d);
    path.setAttribute('fill', 'none');
    path.setAttribute('stroke', '#55696c');
    path.setAttribute('stroke-width', '1.5');
    path.setAttribute('stroke-linejoin', 'round');
    path.setAttribute('stroke-linecap', 'round');
    path.setAttribute('vector-effect', 'non-scaling-stroke');
    svg.appendChild(path);
    wrap.appendChild(svg);
    var last = pts[pts.length - 1];
    var dot = el('span', 'kpi-spark-dot');
    dot.style.left = last[0] + '%';
    dot.style.top = (last[1] / h) * 100 + '%';
    wrap.appendChild(dot);
    return wrap;
  }

  function renderKpis(host, kpis) {
    kpis.forEach(function (k) {
      var tile = el('div', 'kpi');
      tile.appendChild(el('p', 'kpi-label', k.label));
      tile.appendChild(el('p', 'kpi-value', fmt(k.value, k.fmt)));
      if (k.spark && k.spark.length > 2) tile.appendChild(sparkline(k.spark));
      var foot = el('div', 'kpi-foot');
      if (k.status) {
        foot.appendChild(statusEl(k.status, k.statusLabel));
      } else if (typeof k.delta === 'number') {
        var up = k.delta >= 0;
        var good = (up && k.upIsGood !== false) || (!up && k.upIsGood === false);
        var d = el('p', 'kpi-delta ' + (k.delta === 0 ? '' : (good ? 'is-good' : 'is-bad')));
        d.innerHTML = up ? ICON.up : ICON.down;
        var b = el('b', null, (up ? '+' : '−') + nf1.format(Math.abs(k.delta) * 100) + '%');
        d.appendChild(b);
        d.appendChild(document.createTextNode(' ' + (k.deltaLabel || 'vs prior period')));
        foot.appendChild(d);
      } else if (k.note) {
        foot.appendChild(el('p', 'kpi-delta', k.note));
      }
      tile.appendChild(foot);
      host.appendChild(tile);
    });
  }

  /* ---- Tables ---- */
  var NUMERIC = { int: 1, compact: 1, usd: 1, usd2: 1, pct: 1, pct2: 1, pct0: 1, sec: 1, ms: 1, min: 1, dur: 1, gb: 1, mb: 1, tb: 1, ratio: 1, dec1: 1, x: 1, persec: 1 };

  function renderTable(host, spec) {
    var table = el('table', 'dash-table');
    if (spec.caption) {
      var cap = el('caption', 'sr-only', spec.caption);
      table.appendChild(cap);
    }
    var thead = el('thead');
    var tr = el('tr');
    spec.columns.forEach(function (c) {
      var th = el('th', NUMERIC[c.fmt] ? 'num' : '', c.label);
      th.setAttribute('scope', 'col');
      tr.appendChild(th);
    });
    thead.appendChild(tr);
    table.appendChild(thead);
    var tbody = el('tbody');
    spec.rows.forEach(function (row) {
      var r = el('tr');
      spec.columns.forEach(function (c) {
        var v = Array.isArray(row) ? row[spec.columns.indexOf(c)] : row[c.key];
        var td = el('td');
        if (c.fmt === 'status' && v && typeof v === 'object') {
          td.appendChild(statusEl(v.s, v.t));
        } else if (c.fmt === 'code') {
          td.appendChild(el('code', null, v));
        } else if (NUMERIC[c.fmt]) {
          td.className = 'num';
          td.textContent = fmt(v, c.fmt);
        } else {
          td.textContent = v === null || v === undefined ? '–' : String(v);
          if (c.wrap) td.className = 'wrap';
        }
        if (c.strong) td.className = (td.className ? td.className + ' ' : '') + 'strong';
        r.appendChild(td);
      });
      tbody.appendChild(r);
    });
    table.appendChild(tbody);
    host.appendChild(table);
  }

  /* The table twin of a chart, so every value is reachable without hovering. */
  function chartToTable(s) {
    var cols, rows;
    if (s.kind === 'line' || s.kind === 'bar') {
      var series = s.series.filter(function (x) { return x.name.charAt(0) !== '_'; });
      cols = [{ key: 'x', label: s.xLabel || 'Category' }].concat(series.map(function (x, i) { return { key: 's' + i, label: x.name, fmt: s.fmt }; }));
      if (s.band) cols.push({ key: 'lo', label: 'Expected low', fmt: s.fmt }, { key: 'hi', label: 'Expected high', fmt: s.fmt });
      rows = s.x.map(function (x, j) {
        var row = { x: x };
        series.forEach(function (ser, i) { row['s' + i] = ser.data[j]; });
        if (s.band) { row.lo = s.band.lower[j]; row.hi = s.band.upper[j]; }
        return row;
      });
    } else if (s.kind === 'heatmap') {
      cols = [{ key: 'y', label: s.yLabel || '' }].concat(s.x.map(function (x, i) { return { key: 'c' + i, label: x, fmt: s.fmt }; }));
      rows = s.y.map(function (y) { return { y: y }; });
      s.values.forEach(function (v) { rows[v[1]]['c' + v[0]] = v[2]; });
    } else if (s.kind === 'statusgrid') {
      cols = [{ key: 'y', label: s.yLabel || 'Task' }].concat(s.x.map(function (x, i) { return { key: 'c' + i, label: x }; }));
      rows = s.y.map(function (y) { return { y: y }; });
      s.cells.forEach(function (v) { rows[v[1]]['c' + v[0]] = s.states[v[2]].label; });
    } else if (s.kind === 'sankey') {
      cols = [{ key: 'a', label: 'From' }, { key: 'b', label: 'To' }, { key: 'v', label: s.unitLabel || 'Rows', fmt: s.fmt || 'int' }];
      rows = s.links.map(function (l) { return { a: l.source, b: l.target, v: l.value }; });
    } else if (s.kind === 'graph') {
      cols = [{ key: 'n', label: 'Model' }, { key: 'g', label: 'Layer' }, { key: 'st', label: 'Status', fmt: 'status' }, { key: 'up', label: 'Depends on' }];
      rows = s.nodes.map(function (n) {
        var ups = s.edges.filter(function (e) { return e[1] === n.name; }).map(function (e) { return e[0]; });
        return { n: n.name, g: s.columns[n.col], st: { s: n.status || 'good', t: n.statusLabel || STATUS_LABEL[n.status || 'good'] }, up: ups.join(', ') || '–' };
      });
    } else if (s.kind === 'funnel') {
      var first = s.steps[0].value;
      cols = [{ key: 'n', label: 'Step' }, { key: 'v', label: s.unitLabel || 'Count', fmt: s.fmt || 'int' }, { key: 'p', label: 'Of first step', fmt: 'pct' }];
      rows = s.steps.map(function (st) { return { n: st.name, v: st.value, p: st.value / first }; });
    } else if (s.kind === 'bullet') {
      cols = [{ key: 'n', label: s.yLabel || 'Item' }, { key: 'v', label: s.valueLabel || 'Value', fmt: s.fmt }, { key: 't', label: s.targetLabel || 'Target', fmt: s.fmt }, { key: 'st', label: 'Status', fmt: 'status' }];
      rows = s.categories.map(function (c, i) { return { n: c, v: s.values[i], t: s.targets[i], st: { s: s.status[i], t: s.statusText ? s.statusText[i] : STATUS_LABEL[s.status[i]] } }; });
    } else {
      return null;
    }
    return { columns: cols, rows: rows, caption: s.title };
  }

  /* ---- Chart options ---- */
  function seriesColor(ser, i) {
    if (ser.status) return C.status[ser.status];
    if (ser.context) return C.context;
    return C.series[ser.slot !== undefined ? ser.slot : i];
  }

  /* Legends are drawn in HTML above the chart (see htmlLegend); ECharts keeps an invisible legend for toggling. */
  function base(s, legendItems) {
    var hasLegend = legendItems && legendItems.length > 1;
    var opt = {
      backgroundColor: 'transparent',
      animation: !reduceMotion,
      animationDuration: 600,
      textStyle: { fontFamily: C.sans, color: C.ink2 },
      aria: { enabled: true, label: { description: s.desc || s.title || '' } },
      grid: { left: 4, right: s.endLabels ? 64 : 14, top: 12, bottom: s.bottomPad || 4, containLabel: true }
    };
    if (hasLegend) {
      opt.legend = { show: false, data: legendItems.map(function (it) { return it.name; }), selectedMode: true };
      opt._legend = legendItems;
    }
    return opt;
  }
  function tipBase(trigger) {
    return {
      trigger: trigger, confine: true, appendToBody: false,
      backgroundColor: '#0b2127', borderColor: 'rgba(255,255,255,.14)', borderWidth: 1, padding: [9, 11],
      extraCssText: 'border-radius:10px;box-shadow:0 12px 30px rgba(0,0,0,.45);',
      textStyle: { color: C.ink, fontFamily: C.sans, fontSize: 12 },
      axisPointer: { type: 'line', lineStyle: { color: C.pointer, width: 1 }, shadowStyle: { color: 'rgba(255,255,255,.04)' } }
    };
  }
  function tipRow(color, value, name, box) {
    return '<div class="dash-tip-row"><span class="dash-tip-key' + (box ? ' is-box' : '') + '" style="background:' + color + '"></span><b>' + esc(value) + '</b><span>' + esc(name) + '</span></div>';
  }
  function catAxis(data, s, horizontal) {
    var edge = s.kind === 'line' && !horizontal;
    return {
      type: 'category', data: data, boundaryGap: s.kind === 'bar', inverse: !!horizontal,
      axisLine: { show: !horizontal, lineStyle: { color: C.axis } }, axisTick: { show: false },
      axisLabel: {
        color: C.muted, fontFamily: horizontal ? C.sans : C.mono, fontSize: horizontal ? 11.5 : 10.5,
        interval: s.labelEvery !== undefined ? s.labelEvery : 'auto', hideOverlap: !!horizontal, margin: 10,
        width: horizontal ? 150 : undefined, overflow: horizontal ? 'truncate' : undefined,
        alignMinLabel: edge ? 'left' : undefined, alignMaxLabel: edge ? 'right' : undefined,
        // 'day' ticks show only the date part of labels like "Sep 30 06:00"
        formatter: s.tickLabel === 'day' ? function (v) { return String(v).split(' ').slice(0, 2).join(' '); } : undefined
      },
      splitLine: { show: false }
    };
  }
  function valAxis(s) {
    if (s.log) {
      return {
        type: 'log', logBase: 10, min: s.yMin || 1, max: s.yMax,
        axisLine: { show: false }, axisTick: { show: false },
        axisLabel: { color: C.muted, fontFamily: C.mono, fontSize: 10.5, formatter: axisFmt(s.fmt) },
        splitLine: { lineStyle: { color: C.grid, width: 1, type: 'solid' } }
      };
    }
    return {
      type: 'value', min: s.yMin, max: s.yMax, scale: !!s.scale, interval: s.yStep,
      axisLine: { show: false }, axisTick: { show: false },
      axisLabel: { color: C.muted, fontFamily: C.mono, fontSize: 10.5, formatter: axisFmt(s.fmt) },
      splitLine: { lineStyle: { color: C.grid, width: 1, type: 'solid' } },
      splitNumber: 4
    };
  }
  function thresholdLines(s) {
    if (!s.thresholds) return undefined;
    return {
      silent: true, symbol: 'none', precision: 6,
      data: s.thresholds.map(function (t) {
        var d = {}; d[s.horizontal ? 'xAxis' : 'yAxis'] = t.value;
        d.lineStyle = { color: C.status[t.status || 'critical'], type: 'dashed', width: 1 };
        d.label = { formatter: t.label, color: C.ink2, fontSize: 10.5, fontFamily: C.mono, position: 'insideEndTop' };
        return d;
      })
    };
  }

  function lineOption(s) {
    var visible = s.series.filter(function (x) { return x.name.charAt(0) !== '_'; });
    var legend = visible.map(function (x) { return { name: x.name, color: seriesColor(x, s.series.indexOf(x)), shape: 'line' }; });
    if (s.band) legend.push({ name: s.band.name || 'Expected range', color: C.band, shape: 'band' });
    if (s.points) legend.push({ name: s.points.name || 'Anomaly', color: C.status.critical, shape: 'dot' });
    var o = base(s, legend);
    o.tooltip = tipBase('axis');
    o.tooltip.formatter = function (params) {
      var p = Array.isArray(params) ? params : [params];
      var idx = p[0].dataIndex;
      var html = '<div class="dash-tip"><div class="dash-tip-head">' + esc(p[0].axisValueLabel || p[0].name) + '</div>';
      p.forEach(function (x) {
        if (x.seriesName.charAt(0) === '_' || x.seriesType === 'scatter') return;
        if (x.seriesName === (s.band && s.band.name)) return;
        html += tipRow(x.color, fmt(x.value, s.fmt), x.seriesName);
      });
      if (s.band) html += tipRow(C.band, fmt(s.band.lower[idx], s.fmt) + '–' + fmt(s.band.upper[idx], s.fmt), s.band.name || 'Expected range', true);
      if (s.points) {
        s.points.items.forEach(function (pt) { if (pt[0] === idx) html += tipRow(C.status.critical, pt[2] || 'Anomaly', 'flagged', true); });
      }
      return html + '</div>';
    };
    o.xAxis = catAxis(s.x, s);
    o.yAxis = valAxis(s);
    o.series = [];
    if (s.band) {
      o.series.push({
        type: 'line', name: '_lower', data: s.band.lower, stack: 'band', symbol: 'none', silent: true,
        lineStyle: { opacity: 0 }, areaStyle: { opacity: 0 }, tooltip: { show: false }, z: 1
      });
      o.series.push({
        type: 'line', name: s.band.name || 'Expected range', data: s.band.upper.map(function (u, i) { return u - s.band.lower[i]; }),
        stack: 'band', symbol: 'none', silent: true, lineStyle: { opacity: 0 }, itemStyle: { color: C.band },
        areaStyle: { color: C.band, opacity: 0.16 }, z: 1
      });

    }
    s.series.forEach(function (ser, i) {
      if (ser.name.charAt(0) === '_') return;
      var col = seriesColor(ser, i);
      var area = !ser.context && (ser.area || (visible.length === 1 && s.area));
      o.series.push({
        type: 'line', name: ser.name, data: ser.data, showSymbol: false, symbol: 'circle', symbolSize: 8, smooth: false,
        lineStyle: { width: 2, color: col, cap: 'round', join: 'round' },
        itemStyle: { color: col, borderColor: C.surface, borderWidth: 2 },
        areaStyle: area ? { color: col, opacity: 0.1 } : undefined,
        emphasis: { focus: 'none', scale: 1.2 },
        endLabel: s.endLabels ? { show: true, color: C.ink2, fontFamily: C.mono, fontSize: 10.5, distance: 6, formatter: function (p) { return fmt(p.value, s.fmt); } } : undefined,
        markLine: i === 0 ? thresholdLines(s) : undefined,
        z: ser.context ? 2 : 3
      });
    });
    if (s.points) {
      o.series.push({
        type: 'scatter', name: s.points.name || 'Anomaly', symbolSize: 10, z: 5,
        data: s.points.items.map(function (pt) { return [pt[0], pt[1]]; }),
        itemStyle: { color: C.status.critical, borderColor: C.surface, borderWidth: 2 },
        tooltip: { show: false }
      });
    }
    return o;
  }

  function barOption(s) {
    var horizontal = !!s.horizontal;
    var multi = s.series.length > 1;
    var o = base(s, s.series.map(function (x, i) { return { name: x.name, color: seriesColor(x, i), shape: 'box' }; }));
    o.tooltip = tipBase(horizontal && !multi ? 'item' : 'axis');
    o.tooltip.axisPointer = { type: 'shadow', shadowStyle: { color: 'rgba(255,255,255,.035)' } };
    o.tooltip.formatter = function (params) {
      var p = Array.isArray(params) ? params : [params];
      var html = '<div class="dash-tip"><div class="dash-tip-head">' + esc(p[0].axisValueLabel || p[0].name) + '</div>';
      var total = 0;
      p.forEach(function (x) { total += (typeof x.value === 'number' ? x.value : 0); html += tipRow(x.color, fmt(x.value, s.fmt), x.seriesName, true); });
      if (s.stack && p.length > 1) html += '<div class="dash-tip-row"><span></span><b>' + esc(fmt(total, s.fmt)) + '</b><span>Total</span></div>';
      return html + '</div>';
    };
    var cat = catAxis(s.x, s, horizontal), val = valAxis(s);
    if (horizontal) { o.yAxis = cat; o.xAxis = val; } else { o.xAxis = cat; o.yAxis = val; }
    var lastIdx = s.series.length - 1;
    o.series = s.series.map(function (ser, i) {
      var col = seriesColor(ser, i);
      var rounded = !s.stack || i === lastIdx;
      var radius = rounded ? (horizontal ? [0, 4, 4, 0] : [4, 4, 0, 0]) : 0;
      var data = ser.data;
      if (ser.itemStatus) {
        data = ser.data.map(function (v, j) { return { value: v, itemStyle: { color: ser.itemStatus[j] ? C.status[ser.itemStatus[j]] : col } }; });
      }
      return {
        type: 'bar', name: ser.name, data: data, stack: s.stack ? 'total' : undefined,
        barMaxWidth: s.barMaxWidth || 24, barGap: s.overlap ? '-100%' : '12%', barCategoryGap: s.barCategoryGap || '32%',
        itemStyle: { color: col, borderRadius: radius, borderColor: s.stack ? C.surface : undefined, borderWidth: s.stack ? 1 : 0 },
        emphasis: { focus: 'none', itemStyle: { opacity: 0.85 } },
        label: s.valueLabels && (!s.stack || i === lastIdx) ? {
          show: true, position: horizontal ? 'right' : 'top', color: C.ink2, fontFamily: C.mono, fontSize: 10.5, distance: 6,
          formatter: function (p) {
            if (!s.stack) return fmt(p.value && p.value.value !== undefined ? p.value.value : p.value, s.fmt);
            var tot = 0; s.series.forEach(function (x) { tot += x.data[p.dataIndex] || 0; }); return fmt(tot, s.fmt);
          }
        } : undefined,
        markLine: i === 0 ? (s.avgLine ? {
          silent: true, symbol: 'none', precision: 6,
          data: [{ type: 'average', name: 'Average' }],
          lineStyle: { color: C.ink2, type: 'dashed', width: 1, opacity: 0.6 },
          label: { formatter: function (p) { return 'avg ' + fmt(p.value, s.fmt); }, color: C.ink2, fontSize: 10.5, fontFamily: C.mono, position: 'insideEndTop' }
        } : thresholdLines(s)) : undefined
      };
    });
    if (s.valueLabels && horizontal) o.grid.right = 54;
    return o;
  }

  function heatmapOption(s) {
    var o = base(s, null);
    o.grid.bottom = 44;
    o.tooltip = tipBase('item');
    o.tooltip.formatter = function (p) {
      return '<div class="dash-tip"><div class="dash-tip-head">' + esc(s.y[p.value[1]] + ' · ' + s.x[p.value[0]]) + '</div>' +
        tipRow(C.seq[8], fmt(p.value[2], s.fmt), s.unitLabel || '', true) + '</div>';
    };
    o.xAxis = catAxis(s.x, { kind: 'bar', labelEvery: s.labelEvery });
    o.yAxis = catAxis(s.y, { kind: 'bar' }, true);
    o.yAxis.axisLabel.fontFamily = C.mono; o.yAxis.axisLabel.fontSize = 10.5;
    var vals = s.values.map(function (v) { return v[2]; });
    o.visualMap = {
      min: s.min !== undefined ? s.min : Math.min.apply(null, vals), max: s.max !== undefined ? s.max : Math.max.apply(null, vals),
      calculable: false, orient: 'horizontal', left: 'center', bottom: 0, itemWidth: 10, itemHeight: 180,
      text: [s.highLabel || 'More', s.lowLabel || 'Less'], textGap: 8,
      textStyle: { color: C.muted, fontSize: 10.5, fontFamily: C.sans },
      inRange: { color: C.seq.slice(1) }
    };
    o.series = [{
      type: 'heatmap', data: s.values, progressive: 0,
      itemStyle: { borderColor: C.surface, borderWidth: 2, borderRadius: 2 },
      emphasis: { itemStyle: { borderColor: C.ink, borderWidth: 1 } }
    }];
    return o;
  }

  function statusGridOption(s) {
    var o = base(s, null);
    o.tooltip = tipBase('item');
    o.tooltip.formatter = function (p) {
      var st = s.states[p.value[2]];
      return '<div class="dash-tip"><div class="dash-tip-head">' + esc(s.x[p.value[0]] + ' · ' + s.y[p.value[1]]) + '</div>' +
        tipRow(C.status[st.status], st.label, p.value[3] || '', true) + '</div>';
    };
    o.xAxis = catAxis(s.x, { kind: 'bar', labelEvery: s.labelEvery });
    o.yAxis = catAxis(s.y, { kind: 'bar' }, true);
    o.yAxis.axisLabel.fontFamily = C.mono; o.yAxis.axisLabel.fontSize = 10.5;
    // Colours come from a hidden piecewise map; the key is an HTML legend (it wraps on small screens).
    o.visualMap = {
      type: 'piecewise', dimension: 2, show: false,
      pieces: s.states.map(function (st, i) { return { value: i, label: st.label, color: C.status[st.status] }; })
    };
    o._legend = s.states.map(function (st) { return { name: st.label, color: C.status[st.status], shape: 'box' }; });
    o._legendStatic = true;
    o.series = [{
      type: 'heatmap', data: s.cells,
      itemStyle: { borderColor: C.surface, borderWidth: 2, borderRadius: 3 },
      emphasis: { itemStyle: { borderColor: C.ink, borderWidth: 1 } }
    }];
    return o;
  }

  function sankeyOption(s) {
    var o = base(s, null);
    o.tooltip = tipBase('item');
    o.tooltip.formatter = function (p) {
      var unit = s.unitLabel || 'rows';
      if (p.dataType === 'edge') {
        return '<div class="dash-tip"><div class="dash-tip-head">' + esc(p.data.source + ' → ' + p.data.target) + '</div>' + tipRow(p.color, fmt(p.data.value, s.fmt || 'int'), unit, true) + '</div>';
      }
      return '<div class="dash-tip"><div class="dash-tip-head">' + esc(p.name) + '</div>' + tipRow(p.color, fmt(p.value, s.fmt || 'int'), unit, true) + '</div>';
    };
    o.series = [{
      type: 'sankey', left: 4, right: 150, top: 8, bottom: 8, nodeWidth: 10, nodeGap: 12, layoutIterations: 64, draggable: false,
      nodeAlign: 'left',
      emphasis: { focus: 'adjacency' },
      data: s.nodes.map(function (n) {
        var col = n.status ? C.status[n.status] : C.ordinal[Math.min(n.stage || 0, C.ordinal.length - 1)];
        return { name: n.name, itemStyle: { color: col, borderColor: C.surface, borderWidth: 1 }, label: { color: C.ink2 } };
      }),
      links: s.links.map(function (l) { return { source: l.source, target: l.target, value: l.value }; }),
      lineStyle: { color: 'source', opacity: 0.22, curveness: 0.5 },
      label: { color: C.ink2, fontSize: 11, fontFamily: C.mono, formatter: function (p) { return p.name; } }
    }];
    return o;
  }

  function graphOption(s) {
    var o = base(s, null);
    o.tooltip = tipBase('item');
    o.tooltip.formatter = function (p) {
      if (p.dataType === 'edge') return '<div class="dash-tip">' + esc(p.data.source + ' → ' + p.data.target) + '</div>';
      var n = s.nodes[p.dataIndex];
      var st = n.status || 'good';
      return '<div class="dash-tip"><div class="dash-tip-head">' + esc(s.columns[n.col]) + '</div>' + tipRow(C.status[st], n.name, n.statusLabel || STATUS_LABEL[st], true) + (n.note ? '<div class="dash-tip-row"><span></span><span></span><span>' + esc(n.note) + '</span></div>' : '') + '</div>';
    };
    o.series = [{
      type: 'graph', layout: 'none', roam: false, top: 26, bottom: 26, left: '9%', right: '9%',
      symbol: 'roundRect', edgeSymbol: ['none', 'arrow'], edgeSymbolSize: [0, 6],
      data: graphNodes(s, 1000, 360)
    }];
    o.series[0].links = s.edges.map(function (e) { return { source: e[0], target: e[1] }; });
    o.series[0].lineStyle = { color: '#3d5054', width: 1, curveness: 0.06, opacity: 1 };
    o.series[0].emphasis = { focus: 'adjacency', lineStyle: { color: C.series[0], width: 1.5 } };
    return o;
  }

  /* Node positions in pixels of the plot area, so the view's x and y scales are both 1 and the boxes are not squashed. */
  function graphNodes(s, width, height) {
    var cols = 1, rows = 0;
    s.nodes.forEach(function (n) { cols = Math.max(cols, n.col + 1); rows = Math.max(rows, n.row); });
    var plotW = width * 0.82, plotH = Math.max(40, height - 52);
    return s.nodes.map(function (n) {
        var w = Math.max(70, n.name.length * 6.6 + 30);
        return {
          name: n.name, x: (n.col / Math.max(1, cols - 1)) * plotW, y: (n.row / Math.max(1, rows)) * plotH, symbolSize: [w, 24],
          itemStyle: { color: C.surface2, borderColor: n.status && n.status !== 'good' ? C.status[n.status] : 'rgba(255,255,255,.18)', borderWidth: 1 },
          label: {
            show: true, color: C.ink, fontFamily: C.mono, fontSize: 10, formatter: '{d|●} ' + n.name,
            rich: { d: { color: C.status[n.status || 'good'], fontSize: 9 } }
          }
        };
    });
  }

  function funnelOption(s) {
    var o = base(s, null);
    var first = s.steps[0].value;
    o.grid.right = 120;
    o.tooltip = tipBase('item');
    o.tooltip.formatter = function (p) {
      return '<div class="dash-tip"><div class="dash-tip-head">' + esc(p.name) + '</div>' +
        tipRow(p.color, fmt(p.value, s.fmt || 'int'), s.unitLabel || '', true) +
        tipRow(p.color, fmt(p.value / first, 'pct'), 'of first step', true) + '</div>';
    };
    o.yAxis = catAxis(s.steps.map(function (x) { return x.name; }), { kind: 'bar' }, true);
    o.xAxis = valAxis({ fmt: s.fmt || 'int' });
    o.xAxis.show = false;
    o.series = [{
      type: 'bar', barMaxWidth: 24,
      data: s.steps.map(function (st, i) { return { value: st.value, itemStyle: { color: C.ordinal[Math.min(i + 1, C.ordinal.length - 1)], borderRadius: [0, 4, 4, 0] } }; }),
      label: { show: true, position: 'right', color: C.ink2, fontFamily: C.mono, fontSize: 10.5, formatter: function (p) { return fmt(p.value, s.fmt || 'compact') + ' · ' + fmt(p.value / first, 'pct'); } }
    }];
    return o;
  }

  function bulletOption(s) {
    var o = base(s, [{ name: s.valueLabel || 'Value', color: C.series[0], shape: 'box' }, { name: s.targetLabel || 'Target', color: C.ink, shape: 'tick' }]);
    o.tooltip = tipBase('axis');
    o.tooltip.axisPointer = { type: 'shadow', shadowStyle: { color: 'rgba(255,255,255,.035)' } };
    o.tooltip.formatter = function (params) {
      var i = params[0].dataIndex;
      return '<div class="dash-tip"><div class="dash-tip-head">' + esc(s.categories[i]) + '</div>' +
        tipRow(C.status[s.status[i]], fmt(s.values[i], s.fmt), (s.valueLabel || 'Value') + ' · ' + (s.statusText ? s.statusText[i] : STATUS_LABEL[s.status[i]]), true) +
        tipRow(C.ink, fmt(s.targets[i], s.fmt), s.targetLabel || 'Target') + '</div>';
    };
    o.yAxis = catAxis(s.categories, { kind: 'bar' }, true);
    o.yAxis.axisLabel.fontFamily = C.mono; o.yAxis.axisLabel.fontSize = 10.5;
    o.xAxis = valAxis({ fmt: s.fmt });
    o.series = [{
      type: 'bar', name: s.valueLabel || 'Value', barMaxWidth: 14,
      data: s.values.map(function (v, i) { return { value: v, itemStyle: { color: C.status[s.status[i]], borderRadius: [0, 4, 4, 0] } }; })
    }, {
      type: 'scatter', name: s.targetLabel || 'Target', symbol: 'rect', symbolSize: [2, 20], z: 5,
      data: s.targets.map(function (t, i) { return [t, i]; }),
      itemStyle: { color: C.ink }
    }];
    return o;
  }

  function optionFor(s) {
    switch (s.kind) {
      case 'line': return lineOption(s);
      case 'bar': return barOption(s);
      case 'heatmap': return heatmapOption(s);
      case 'statusgrid': return statusGridOption(s);
      case 'sankey': return sankeyOption(s);
      case 'graph': return graphOption(s);
      case 'funnel': return funnelOption(s);
      case 'bullet': return bulletOption(s);
      default: return null;
    }
  }

  /* ---- HTML-only visuals ---- */
  function renderMeters(host, s) {
    var wrap = el('div', 'dash-meters');
    s.items.forEach(function (it) {
      var ratio = it.fill !== undefined ? it.fill : it.value;
      var row = el('div', 'meter-row');
      // Table names may break after a dot (zero-width space) rather than mid-word.
      row.appendChild(el('span', 'meter-label' + (s.codeLabels ? ' is-code' : ''), s.codeLabels ? it.label.replace(/\./g, '.\u200b') : it.label));
      row.appendChild(el('span', 'meter-value', it.text || fmt(it.value, s.fmt || 'pct')));
      var track = el('div', 'meter-track');
      var tone = it.status === 'warning' ? ' is-warning' : (it.status === 'critical' || it.status === 'serious') ? ' is-critical' : '';
      var fill = el('div', 'meter-fill' + tone);
      fill.style.width = Math.max(0, Math.min(1, ratio)) * 100 + '%';
      if (s.marker !== undefined) {
        var mk = el('span', 'meter-marker');
        mk.style.left = Math.max(0, Math.min(1, s.marker)) * 100 + '%';
        track.appendChild(mk);
      }
      track.setAttribute('role', 'meter');
      track.setAttribute('aria-label', it.label);
      track.setAttribute('aria-valuemin', '0');
      track.setAttribute('aria-valuemax', '100');
      track.setAttribute('aria-valuenow', String(Math.round(ratio * 100)));
      if (it.text) track.setAttribute('aria-valuetext', it.text);
      track.appendChild(fill);
      row.appendChild(track);
      if (it.status || it.note) {
        var foot = el('div', 'meter-foot');
        if (it.status && s.statusChips) foot.appendChild(statusEl(it.status, it.statusLabel));
        if (it.note) foot.appendChild(el('span', 'meter-note', it.note));
        row.appendChild(foot);
      }
      wrap.appendChild(row);
    });
    host.appendChild(wrap);
  }

  function renderMatrix(host, s) {
    var all = [];
    s.values.forEach(function (r) { r.forEach(function (v) { all.push(v); }); });
    var max = Math.max.apply(null, all);
    var t = el('table', 'dash-matrix');
    t.appendChild(el('caption', 'sr-only', s.title || 'Confusion matrix'));
    var head = el('tr');
    head.appendChild(el('th'));
    s.cols.forEach(function (c) { var th = el('th', null, c); th.setAttribute('scope', 'col'); head.appendChild(th); });
    var thead = el('thead'); thead.appendChild(head); t.appendChild(thead);
    var tb = el('tbody');
    s.rows.forEach(function (rname, i) {
      var tr = el('tr');
      var th = el('th', null, rname); th.setAttribute('scope', 'row'); tr.appendChild(th);
      s.values[i].forEach(function (v, j) {
        var td = el('td');
        var level = Math.round(Math.sqrt(v / max) * 9);
        td.style.background = C.seq[Math.max(1, Math.min(10, level + 1))];
        td.style.color = level >= 7 ? C.surface : C.ink;
        var b = el('b', null, fmt(v, s.fmt || 'int')); if (level >= 7) b.style.color = C.surface;
        var sp = el('span', null, s.labels[i][j]); sp.style.color = level >= 7 ? '#0b2328' : C.ink2;
        td.appendChild(b); td.appendChild(sp);
        tr.appendChild(td);
      });
      tb.appendChild(tr);
    });
    t.appendChild(tb);
    host.appendChild(t);
  }

  /* ---- Wiring ---- */
  var charts = [];

  function htmlLegend(node, chart, items, isStatic) {
    var wrap = el('div', 'dash-legend');
    items.forEach(function (it) {
      if (isStatic) {
        var item = el('span', 'dash-legend-item is-static');
        var k = el('span', 'dash-legend-key is-' + it.shape);
        k.style.background = it.color;
        k.setAttribute('aria-hidden', 'true');
        item.appendChild(k);
        item.appendChild(document.createTextNode(it.name));
        wrap.appendChild(item);
        return;
      }
      var b = el('button', 'dash-legend-item');
      b.type = 'button';
      b.setAttribute('aria-pressed', 'true');
      b.title = 'Show or hide ' + it.name;
      var key = el('span', 'dash-legend-key is-' + it.shape);
      key.style.background = it.color;
      key.setAttribute('aria-hidden', 'true');
      b.appendChild(key);
      b.appendChild(document.createTextNode(it.name));
      b.addEventListener('click', function () {
        var on = b.getAttribute('aria-pressed') === 'true';
        b.setAttribute('aria-pressed', on ? 'false' : 'true');
        chart.dispatchAction({ type: 'legendToggleSelect', name: it.name });
      });
      wrap.appendChild(b);
    });
    node.parentNode.insertBefore(wrap, node);
  }

  /* Thin out category labels to what fits the chart's current width (keeps the same rhythm, e.g. every 3 h → 6 h). */
  function fitLabels(chart, s, node) {
    var vertical = s.kind === 'line' || s.kind === 'heatmap' || s.kind === 'statusgrid' || (s.kind === 'bar' && !s.horizontal);
    if (!vertical || !s.x || !s.x.length) return;
    var longest = 0;
    s.x.forEach(function (l) { longest = Math.max(longest, (s.tickLabel === 'day' ? String(l).split(' ').slice(0, 2).join(' ') : String(l)).length); });
    var plot = Math.max(120, node.clientWidth - (s.kind === 'heatmap' || s.kind === 'statusgrid' ? 110 : 70));
    var fits = Math.max(2, Math.floor(plot / (longest * 6.6 + 16)));
    var step = s.labelEvery !== undefined ? s.labelEvery + 1 : 1;
    while (Math.ceil(s.x.length / step) > fits) step = step === 1 ? 2 : step * 2;
    chart.setOption({ xAxis: { axisLabel: { interval: step - 1 } } });
  }

  function mountChart(node, spec) {
    var panel = node.closest('.dash-panel');
    if (spec.kind === 'meters') { renderMeters(node, spec); return; }
    if (spec.kind === 'matrix') { renderMatrix(node, spec); return; }
    var option = optionFor(spec);
    if (!option || !window.echarts) return;
    if (spec.kind === 'graph') {
      // The DAG keeps a readable width and scrolls sideways on small screens.
      var scroller = el('div', 'dash-scroll');
      var inner = el('div', 'dash-scroll-inner');
      node.parentNode.insertBefore(scroller, node);
      scroller.appendChild(inner);
      if (spec.columns) {
        // Layer names, centred over each column of nodes (the graph spans 9%–91% of its width).
        var head = el('div', 'graph-cols');
        head.setAttribute('aria-hidden', 'true');
        spec.columns.forEach(function (c, i) {
          var span = el('span', null, c);
          span.style.left = (9 + (i / Math.max(1, spec.columns.length - 1)) * 82) + '%';
          head.appendChild(span);
        });
        inner.appendChild(head);
      }
      inner.appendChild(node);
    }
    node.setAttribute('role', 'img');
    node.setAttribute('aria-label', (spec.title || '') + (spec.desc ? '. ' + spec.desc : ''));
    if (spec.kind === 'graph') option.series[0].data = graphNodes(spec, node.clientWidth, node.clientHeight);
    var legendItems = option._legend, legendStatic = option._legendStatic;
    delete option._legend;
    delete option._legendStatic;
    var chart = window.echarts.init(node, null, { renderer: 'svg' });
    chart.setOption(option);
    if (legendItems) htmlLegend(spec.kind === 'graph' ? node.parentNode.parentNode : node, chart, legendItems, legendStatic);
    fitLabels(chart, spec, node);
    charts.push(chart);
    if ('ResizeObserver' in window) {
      var lastW = node.clientWidth;
      new ResizeObserver(function () {
        if (!node.clientWidth) return;
        if (node.clientWidth !== lastW) {
          lastW = node.clientWidth;
          fitLabels(chart, spec, node);
          if (spec.kind === 'graph') chart.setOption({ series: [{ data: graphNodes(spec, node.clientWidth, node.clientHeight) }] });
        }
        chart.resize();
      }).observe(node);
    }

    var twin = chartToTable(spec);
    var toggle = panel && panel.querySelector('[data-toggle]');
    if (twin && toggle) {
      var holder = el('div', 'dash-chart-table');
      renderTable(holder, twin);
      var after = spec.kind === 'graph' ? node.parentNode.parentNode : node;
      after.parentNode.insertBefore(holder, after.nextSibling);
      toggle.hidden = false;
      toggle.addEventListener('click', function () {
        var on = !panel.classList.contains('is-table');
        panel.classList.toggle('is-table', on);
        toggle.setAttribute('aria-pressed', on ? 'true' : 'false');
        if (!on) chart.resize();
      });
    } else if (toggle) {
      toggle.hidden = true;
    }
  }

  function render(data) {
    var kpiHost = root.querySelector('[data-kpis]');
    if (kpiHost && data.kpis) renderKpis(kpiHost, data.kpis);
    root.querySelectorAll('[data-chart]').forEach(function (node) {
      var spec = data.charts && data.charts[node.getAttribute('data-chart')];
      if (spec) mountChart(node, spec);
    });
    root.querySelectorAll('[data-table]').forEach(function (node) {
      var spec = data.tables && data.tables[node.getAttribute('data-table')];
      if (!spec) return;
      var title = node.closest('.dash-panel') && node.closest('.dash-panel').querySelector('.dash-panel-title');
      if (!spec.caption && title) spec.caption = title.textContent;
      renderTable(node, spec);
    });
    root.setAttribute('data-ready', 'true');
  }

  /* ---- Key code: file tabs + copy ---- */
  function initCodeBlocks() {
    Array.prototype.forEach.call(document.querySelectorAll('[data-code-block]'), function (block) {
      var tabs = Array.prototype.slice.call(block.querySelectorAll('[role="tab"]'));
      var panels = tabs.map(function (t) { return document.getElementById(t.getAttribute('aria-controls')); });
      function select(i, focus) {
        tabs.forEach(function (t, j) {
          var on = i === j;
          t.setAttribute('aria-selected', on ? 'true' : 'false');
          t.tabIndex = on ? 0 : -1;
          if (panels[j]) panels[j].hidden = !on;
        });
        if (focus) tabs[i].focus();
      }
      tabs.forEach(function (t, i) {
        t.addEventListener('click', function () { select(i); });
        t.addEventListener('keydown', function (e) {
          var n = tabs.length, j = null;
          if (e.key === 'ArrowRight') j = (i + 1) % n;
          else if (e.key === 'ArrowLeft') j = (i - 1 + n) % n;
          else if (e.key === 'Home') j = 0;
          else if (e.key === 'End') j = n - 1;
          if (j !== null) { e.preventDefault(); select(j, true); }
        });
      });
      var copy = block.querySelector('[data-copy]');
      if (copy && navigator.clipboard && window.isSecureContext) {
        var label = copy.querySelector('span');
        copy.hidden = false;
        copy.addEventListener('click', function () {
          var open = panels.filter(function (p) { return p && !p.hidden; })[0];
          var code = open && open.querySelector('code');
          if (!code) return;
          navigator.clipboard.writeText(code.textContent).then(function () {
            label.textContent = 'Copied';
            setTimeout(function () { label.textContent = 'Copy'; }, 1600);
          }, function () {
            label.textContent = 'Copy failed';
            setTimeout(function () { label.textContent = 'Copy'; }, 1600);
          });
        });
      }
    });
  }

  var src = root.getAttribute('data-src') || 'data.json';
  fetch(src, { cache: 'no-cache' })
    .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(render)
    .catch(function () {
      var msg = el('p', 'dash-noscript', 'The dashboard data could not be loaded. Refresh the page to try again.');
      root.insertBefore(msg, root.firstChild);
    });
})();
