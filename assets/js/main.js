/* ==========================================================================
   Sarun Shrestha — site interactions. Plain JavaScript, no dependencies.
   ========================================================================== */
(function () {
  'use strict';

  var root = document.documentElement;
  // If the head failsafe already switched the page to its static version, do nothing.
  if (!root.classList.contains('js')) return;
  root.classList.add('app-ready');

  var $ = function (sel, ctx) { return (ctx || document).querySelector(sel); };
  var $$ = function (sel, ctx) { return Array.prototype.slice.call((ctx || document).querySelectorAll(sel)); };
  var reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var finePointer = window.matchMedia('(hover: hover) and (pointer: fine)').matches;

  /* ------------------------------------------------------------------------
     Intro greeting
     ------------------------------------------------------------------------ */
  var GREETINGS = ['Hello', 'नमस्ते', 'Bonjour', 'Hola', 'Ciao', 'Olá', 'Hallo'];

  function showHero() {
    // Two frames, so the hidden starting state is painted before it transitions.
    requestAnimationFrame(function () {
      requestAnimationFrame(function () { root.classList.add('hero-ready'); });
    });
  }

  function runIntro() {
    var screen = $('.loading-screen');
    if (!screen || root.classList.contains('skip-loader')) {
      if (screen) screen.parentNode.removeChild(screen);
      root.classList.remove('is-loading');
      showHero();
      return;
    }
    try { sessionStorage.setItem('ss:intro-seen', '1'); } catch (e) { /* storage unavailable */ }

    var word = $('.loading-word-text', screen);
    var fill = $('.loading-bar-fill', screen);
    var index = 0;
    var done = false;

    function leave() {
      if (done) return;
      done = true;
      screen.classList.add('is-leaving');
      root.classList.remove('is-loading');
      setTimeout(function () { root.classList.add('hero-ready'); }, 140);
      setTimeout(function () { if (screen.parentNode) screen.parentNode.removeChild(screen); }, 1200);
    }

    function next() {
      index += 1;
      if (index >= GREETINGS.length) { setTimeout(leave, 420); return; }
      var text = GREETINGS[index];
      word.textContent = text;
      word.classList.toggle('is-deva', /[ऀ-ॿ]/.test(text));
      word.classList.remove('is-entering');
      void word.offsetWidth; // restart the CSS animation
      word.classList.add('is-entering');
      fill.style.width = ((index + 1) / GREETINGS.length) * 100 + '%';
      setTimeout(next, 230);
    }

    // "Hello" is already on screen from the markup.
    fill.style.width = (1 / GREETINGS.length) * 100 + '%';
    setTimeout(next, 700);
    setTimeout(leave, 5000); // safety net
  }

  /* ------------------------------------------------------------------------
     Hero: animated data pipeline (sources → ingest → bronze → silver → gold → serve)
     ------------------------------------------------------------------------ */
  function initPipeline() {
    var hero = $('#hero');
    var canvas = hero && $('.hero-canvas', hero);
    if (!canvas || !canvas.getContext) return;

    var ctx = canvas.getContext('2d');
    var SKY = '56,189,248';
    var CREAM = '250,249,246';
    var TAU = Math.PI * 2;
    var W = 0, H = 0;
    var nodes = [], edges = [], cols = [], packets = [];
    var bandTop = 0, bandBottom = 0;
    var spawnClock = 0, batchClock = 0;
    var running = false, raf = 0, last = 0, inView = true;

    function seeded(seed) {
      return function () {
        seed |= 0; seed = (seed + 0x6D2B79F5) | 0;
        var t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
      };
    }

    function stages(width) {
      if (width <= 480) return { labels: ['sources', 'lakehouse', 'serve'], counts: [3, 4, 3] };
      if (width <= 900) return { labels: ['sources', 'ingest', 'lakehouse', 'serve'], counts: [4, 3, 4, 3] };
      return { labels: ['sources', 'ingest', 'bronze', 'silver', 'gold', 'serve'], counts: [5, 3, 4, 4, 3, 4] };
    }

    function link(a, b) {
      var e = { a: a, b: b, len: Math.max(40, Math.hypot(b.x - a.x, b.y - a.y) * 1.1) };
      edges.push(e);
      a.out.push(e);
    }

    function build() {
      W = hero.clientWidth;
      H = hero.clientHeight;
      var dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(W * dpr);
      canvas.height = Math.round(H * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

      var cfg = stages(W);
      var rand = seeded(11);
      var loc = $('.hero-location', hero);
      var role = $('.hero-role', hero);
      var meta = $('.hero-meta', hero);
      var nav = $('.ds-nav');
      var navH = nav ? nav.offsetHeight : 64;
      var left, right, top, bottom;

      // offset* values ignore transforms, so the entrance animation doesn't skew the layout.
      var locShown = loc.offsetParent !== null;
      var stacked = window.getComputedStyle(role).position !== 'absolute';
      if (stacked) {
        // Phones and tablets: pill on top, role card below, pipeline in between.
        left = W * 0.1;
        right = W * 0.9;
        top = Math.max(navH + 56, locShown ? loc.offsetTop + loc.offsetHeight + 58 : 0);
        bottom = role.offsetTop - 38;
      } else if (locShown) {
        // Desktop: centre the pipeline between the location pill and the role card.
        var minLeft = loc.offsetLeft + loc.offsetWidth + 90;
        var maxRight = role.offsetLeft - 90;
        var halfSpan = Math.min(W / 2 - minLeft, maxRight - W / 2);
        left = W / 2 - halfSpan;
        right = W / 2 + halfSpan;
        if (right - left < 420) { left = minLeft - 30; right = maxRight + 30; } // not enough room to centre
        top = navH + (H < 700 ? 64 : 96);
        bottom = meta.offsetTop - 44;
      } else {
        // Short landscape screens: pipeline to the left of the role card.
        left = W * 0.08;
        right = role.offsetLeft - 60;
        top = navH + 56;
        bottom = meta.offsetTop - 30;
      }
      // Keep the graph compact: at least 150px tall, at most ~55% of the width.
      var band = Math.max(150, Math.min(bottom - top, Math.max(320, W * 0.55)));
      var mid = (top + bottom) / 2;
      top = mid - band / 2;
      bottom = mid + band / 2;
      bandTop = top;
      bandBottom = bottom;

      nodes = []; edges = []; cols = []; packets = [];
      var n = cfg.counts.length;
      var span = bottom - top;
      cfg.counts.forEach(function (count, c) {
        var x = left + (right - left) * (c / (n - 1));
        var col = { x: x, label: cfg.labels[c], nodes: [] };
        for (var i = 0; i < count; i++) {
          var y = top + span * ((i + 0.5) / count) + (rand() - 0.5) * (span / count) * 0.4;
          var node = { x: x, y: y, out: [], glow: 0, r: c === n - 1 ? 4.2 : 3.2 };
          col.nodes.push(node);
          nodes.push(node);
        }
        cols.push(col);
      });

      for (var c = 0; c < n - 1; c++) {
        var from = cols[c].nodes;
        var to = cols[c + 1].nodes;
        var fed = {};
        for (var i = 0; i < from.length; i++) {
          var j = Math.min(to.length - 1, Math.floor(((i + 0.5) / from.length) * to.length));
          var targets = {};
          targets[j] = true;
          if (rand() < 0.75) targets[Math.max(0, Math.min(to.length - 1, j + (rand() < 0.5 ? -1 : 1)))] = true;
          if (rand() < 0.3) targets[Math.floor(rand() * to.length)] = true;
          for (var t in targets) { link(from[i], to[t]); fed[t] = true; }
        }
        for (var k = 0; k < to.length; k++) {
          if (!fed[k]) link(from[Math.min(from.length - 1, Math.floor(((k + 0.5) / to.length) * from.length))], to[k]);
        }
      }
    }

    function pointOn(e, t) {
      var dx = (e.b.x - e.a.x) * 0.5;
      var x1 = e.a.x + dx, x2 = e.b.x - dx;
      var u = 1 - t;
      return [
        u * u * u * e.a.x + 3 * u * u * t * x1 + 3 * u * t * t * x2 + t * t * t * e.b.x,
        u * u * u * e.a.y + 3 * u * u * t * e.a.y + 3 * u * t * t * e.b.y + t * t * t * e.b.y
      ];
    }

    function emit(node) {
      if (!node.out.length || packets.length > 90) return;
      var e = node.out[Math.floor(Math.random() * node.out.length)];
      packets.push({ e: e, t: 0, speed: 130 + Math.random() * 90 });
    }

    function update(dt) {
      if (!cols.length) return;
      spawnClock += dt;
      batchClock += dt;
      var sources = cols[0].nodes;
      if (spawnClock > 0.42) {
        spawnClock = 0;
        emit(sources[Math.floor(Math.random() * sources.length)]);
      }
      if (batchClock > 3.8) {
        batchClock = 0;
        sources.forEach(function (s) { s.glow = 1; emit(s); });
      }
      for (var i = packets.length - 1; i >= 0; i--) {
        var p = packets[i];
        p.t += (p.speed * dt) / p.e.len;
        if (p.t >= 1) {
          packets.splice(i, 1);
          p.e.b.glow = 1;
          emit(p.e.b); // carry on to the next stage
        }
      }
      for (var k = 0; k < nodes.length; k++) {
        if (nodes[k].glow > 0) nodes[k].glow = Math.max(0, nodes[k].glow - dt * 1.4);
      }
    }

    function render() {
      ctx.clearRect(0, 0, W, H);

      // Stage labels and guides
      ctx.font = '500 11px "JetBrains Mono", ui-monospace, monospace';
      ctx.textAlign = 'center';
      ctx.lineWidth = 1;
      cols.forEach(function (c) {
        ctx.fillStyle = 'rgba(' + CREAM + ',.36)';
        ctx.fillText(c.label, c.x, bandTop - 30);
        ctx.strokeStyle = 'rgba(' + CREAM + ',.05)';
        ctx.setLineDash([2, 6]);
        ctx.beginPath();
        ctx.moveTo(c.x, bandTop - 16);
        ctx.lineTo(c.x, bandBottom + 16);
        ctx.stroke();
      });
      ctx.setLineDash([]);

      // Edges
      ctx.strokeStyle = 'rgba(' + CREAM + ',.1)';
      ctx.beginPath();
      edges.forEach(function (e) {
        var dx = (e.b.x - e.a.x) * 0.5;
        ctx.moveTo(e.a.x, e.a.y);
        ctx.bezierCurveTo(e.a.x + dx, e.a.y, e.b.x - dx, e.b.y, e.b.x, e.b.y);
      });
      ctx.stroke();

      // Packets: a bright head with a tapering trail
      ctx.lineCap = 'round';
      packets.forEach(function (p) {
        var trail = Math.min(p.t, 40 / p.e.len);
        var head = pointOn(p.e, p.t);
        var prev = head;
        for (var k = 1; k <= 8; k++) {
          var pt = pointOn(p.e, p.t - trail * (k / 8));
          ctx.strokeStyle = 'rgba(' + SKY + ',' + (0.8 * (1 - k / 8)).toFixed(3) + ')';
          ctx.lineWidth = 2.4 * (1 - k / 10);
          ctx.beginPath();
          ctx.moveTo(prev[0], prev[1]);
          ctx.lineTo(pt[0], pt[1]);
          ctx.stroke();
          prev = pt;
        }
        ctx.fillStyle = 'rgb(' + SKY + ')';
        ctx.beginPath();
        ctx.arc(head[0], head[1], 2.1, 0, TAU);
        ctx.fill();
      });
      ctx.lineCap = 'butt';

      // Nodes
      nodes.forEach(function (node) {
        if (node.glow > 0.01) {
          var g = ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, 20);
          g.addColorStop(0, 'rgba(' + SKY + ',' + (0.4 * node.glow).toFixed(3) + ')');
          g.addColorStop(1, 'rgba(' + SKY + ',0)');
          ctx.fillStyle = g;
          ctx.beginPath();
          ctx.arc(node.x, node.y, 20, 0, TAU);
          ctx.fill();
        }
        ctx.beginPath();
        ctx.arc(node.x, node.y, node.r, 0, TAU);
        ctx.fillStyle = '#011013';
        ctx.fill();
        ctx.lineWidth = 1.2;
        ctx.strokeStyle = node.glow > 0.02
          ? 'rgba(' + SKY + ',' + (0.35 + 0.65 * node.glow).toFixed(3) + ')'
          : 'rgba(' + CREAM + ',.38)';
        ctx.stroke();
      });
    }

    function warmUp() {
      // Simulate a few seconds so the pipeline is already busy when it appears.
      for (var i = 0; i < 90; i++) update(1 / 30);
    }

    function frame(now) {
      var dt = Math.min(0.05, (now - last) / 1000 || 0);
      last = now;
      update(dt);
      render();
      raf = requestAnimationFrame(frame);
    }

    function start() {
      if (running || reduceMotion || !inView || document.hidden) return;
      running = true;
      last = performance.now();
      raf = requestAnimationFrame(frame);
    }

    function stop() {
      running = false;
      cancelAnimationFrame(raf);
    }

    build();
    if (reduceMotion) {
      render();
      if (document.fonts && document.fonts.ready) document.fonts.ready.then(render);
    } else {
      warmUp();
      render();
      start();
    }

    var lastW = W, lastH = H, resizeTimer = 0;
    window.addEventListener('resize', function () {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(function () {
        if (hero.clientWidth === lastW && Math.abs(hero.clientHeight - lastH) < 2) return;
        lastW = hero.clientWidth;
        lastH = hero.clientHeight;
        build();
        if (!reduceMotion) warmUp();
        render();
      }, 160);
    });

    if ('IntersectionObserver' in window) {
      new IntersectionObserver(function (entries) {
        inView = entries[0].isIntersecting;
        if (inView) start(); else stop();
      }).observe(hero);
    }
    document.addEventListener('visibilitychange', function () {
      if (document.hidden) stop(); else start();
    });
  }

  /* ------------------------------------------------------------------------
     Big name marquee — reverses with scroll direction
     ------------------------------------------------------------------------ */
  function fillTrack(track) {
    // Keep the track as two identical halves, each wider than the viewport.
    var guard = 0;
    while (track.scrollWidth < (window.innerWidth + 200) * 2 && guard < 3) {
      track.innerHTML += track.innerHTML;
      guard += 1;
    }
  }

  function initMarquee() {
    var hero = $('#hero');
    var track = $('.marquee-track');
    if (!track || reduceMotion) return;

    var half = 0, x = 0, dir = -1, v = 0, lastY = window.scrollY;
    var running = false, raf = 0, last = 0, inView = true;
    var SECONDS_PER_LOOP = 22;

    function measure() {
      fillTrack(track);
      half = track.scrollWidth / 2;
    }

    track.style.animation = 'none';
    measure();
    v = -half / SECONDS_PER_LOOP;
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(measure);
    window.addEventListener('resize', measure);
    window.addEventListener('scroll', function () {
      var y = window.scrollY;
      if (y > lastY + 1) dir = -1;
      else if (y < lastY - 1) dir = 1;
      lastY = y;
    }, { passive: true });

    function frame(now) {
      var dt = Math.min(0.05, (now - last) / 1000 || 0);
      last = now;
      var target = dir * (half / SECONDS_PER_LOOP);
      v += (target - v) * Math.min(1, dt * 2.5);
      x += v * dt;
      if (half > 0) {
        if (x <= -half) x += half;
        else if (x > 0) x -= half;
      }
      track.style.transform = 'translate3d(' + x.toFixed(2) + 'px,0,0)';
      raf = requestAnimationFrame(frame);
    }
    function start() {
      if (running || !inView || document.hidden) return;
      running = true;
      last = performance.now();
      raf = requestAnimationFrame(frame);
    }
    function stop() { running = false; cancelAnimationFrame(raf); }

    start();
    if ('IntersectionObserver' in window && hero) {
      new IntersectionObserver(function (entries) {
        inView = entries[0].isIntersecting;
        if (inView) start(); else stop();
      }).observe(hero);
    }
    document.addEventListener('visibilitychange', function () {
      if (document.hidden) stop(); else start();
    });
  }

  /* ------------------------------------------------------------------------
     Focus strip — duplicate the set for a seamless CSS loop
     ------------------------------------------------------------------------ */
  function initStrip() {
    var track = $('.rolling-strip-track');
    var set = track && $('.rolling-strip-set', track);
    if (!set) return;
    var copy = set.cloneNode(true);
    copy.setAttribute('aria-hidden', 'true');
    track.appendChild(copy);
    fillTrack(track);
    root.classList.add('strip-ready');
  }

  /* ------------------------------------------------------------------------
     Split a heading into its rendered lines, so each line can slide up
     ------------------------------------------------------------------------ */
  function escapeHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function splitLines(el) {
    var text = el.getAttribute('data-text');
    if (!text) {
      text = el.textContent.replace(/\s+/g, ' ').trim();
      el.setAttribute('data-text', text);
    }
    var words = text.split(' ');
    el.innerHTML = words.map(function (w) { return '<span class="split-word">' + escapeHtml(w) + '</span>'; }).join(' ');
    var lines = [], current = [], lastTop = null;
    $$('.split-word', el).forEach(function (span) {
      var top = span.offsetTop;
      if (lastTop !== null && Math.abs(top - lastTop) > 2) { lines.push(current); current = []; }
      current.push(span.textContent);
      lastTop = top;
    });
    if (current.length) lines.push(current);
    el.innerHTML = lines.map(function (ws, i) {
      return '<span class="line"><span style="--i:' + i + '">' + ws.map(escapeHtml).join(' ') + '</span></span>';
    }).join('');
  }

  function initSplitLines() {
    var els = $$('[data-split-lines]');
    if (!els.length) return;
    var width = window.innerWidth, timer = 0;
    els.forEach(splitLines);
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(function () { els.forEach(splitLines); });
    window.addEventListener('resize', function () {
      clearTimeout(timer);
      timer = setTimeout(function () {
        if (window.innerWidth === width) return;
        width = window.innerWidth;
        els.forEach(splitLines);
      }, 150);
    });
  }

  /* ------------------------------------------------------------------------
     Scroll reveals
     ------------------------------------------------------------------------ */
  function initReveals() {
    $$('[data-reveal="tags"]').forEach(function (group, g) {
      $$('.skill-tag', group).forEach(function (tag, i) {
        tag.style.setProperty('--d', (Math.min(i, 12) * 0.035 + (g % 3) * 0.06).toFixed(3) + 's');
      });
    });

    var items = $$('[data-reveal]');
    if (reduceMotion || !('IntersectionObserver' in window)) {
      items.forEach(function (el) { el.classList.add('is-in'); });
      return;
    }
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          entry.target.classList.add('is-in');
          io.unobserve(entry.target);
        }
      });
    }, { rootMargin: '0px 0px -8% 0px', threshold: 0.12 });
    items.forEach(function (el) { io.observe(el); });
  }

  /* ------------------------------------------------------------------------
     Experience accordion (one role open at a time)
     ------------------------------------------------------------------------ */
  function initAccordion() {
    var cards = $$('.exp-card');
    function setOpen(card, open) {
      card.classList.toggle('is-open', open);
      $('.exp-row-btn', card).setAttribute('aria-expanded', open ? 'true' : 'false');
    }
    cards.forEach(function (card) {
      $('.exp-row-btn', card).addEventListener('click', function () {
        var open = !card.classList.contains('is-open');
        cards.forEach(function (other) { if (other !== card) setOpen(other, false); });
        setOpen(card, open);
        // If a role above collapsed and pushed this one out of view, bring it back.
        setTimeout(function () {
          var top = card.getBoundingClientRect().top;
          if (top < 72) window.scrollBy({ top: top - 96, behavior: reduceMotion ? 'auto' : 'smooth' });
        }, 520);
      });
    });
  }

  /* ------------------------------------------------------------------------
     Side menu
     ------------------------------------------------------------------------ */
  function initMenu() {
    var toggle = $('.menu-toggle');
    var menu = $('#side-menu');
    if (!toggle || !menu) return;
    var lastFocus = null;

    function focusables() { return $$('a[href], button:not([disabled])', menu); }

    function onKey(e) {
      if (e.key === 'Escape') { e.preventDefault(); close(); return; }
      if (e.key !== 'Tab') return;
      var f = focusables();
      if (!f.length) return;
      var first = f[0], lastEl = f[f.length - 1];
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); lastEl.focus(); }
      else if (!e.shiftKey && document.activeElement === lastEl) { e.preventDefault(); first.focus(); }
    }

    function open() {
      lastFocus = document.activeElement;
      root.classList.add('menu-open');
      toggle.setAttribute('aria-expanded', 'true');
      requestAnimationFrame(function () {
        var c = $('.side-menu-close', menu);
        if (c) c.focus({ preventScroll: true });
      });
      document.addEventListener('keydown', onKey);
    }

    function close(restoreFocus) {
      root.classList.remove('menu-open');
      toggle.setAttribute('aria-expanded', 'false');
      document.removeEventListener('keydown', onKey);
      if (restoreFocus !== false && lastFocus && lastFocus.focus) lastFocus.focus();
    }

    toggle.addEventListener('click', function () {
      if (root.classList.contains('menu-open')) close(); else open();
    });
    $$('[data-menu-close]').forEach(function (el) { el.addEventListener('click', function () { close(); }); });
    $$('a', menu).forEach(function (a) { a.addEventListener('click', function () { close(false); }); });
  }

  /* ------------------------------------------------------------------------
     Active section in the navigation
     ------------------------------------------------------------------------ */
  function initScrollSpy() {
    if (!('IntersectionObserver' in window)) return;
    var links = $$('[data-nav]');
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) return;
        var id = entry.target.id;
        links.forEach(function (a) { a.classList.toggle('is-active', a.getAttribute('data-nav') === id); });
      });
    }, { rootMargin: '-45% 0px -50% 0px' });
    ['hero', 'about', 'work', 'experience', 'skills', 'contact'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) io.observe(el);
    });
  }

  /* ------------------------------------------------------------------------
     Custom cursor + magnetic buttons (mouse users only)
     ------------------------------------------------------------------------ */
  function initCursor() {
    if (!finePointer || reduceMotion) return;
    var dot = document.createElement('div');
    var ring = document.createElement('div');
    dot.className = 'cursor-dot';
    ring.className = 'cursor-ring';
    dot.setAttribute('aria-hidden', 'true');
    ring.setAttribute('aria-hidden', 'true');
    document.body.appendChild(dot);
    document.body.appendChild(ring);
    root.classList.add('has-cursor');

    var mx = -100, my = -100, rx = -100, ry = -100, shown = false;
    document.addEventListener('mousemove', function (e) {
      mx = e.clientX;
      my = e.clientY;
      dot.style.transform = 'translate3d(' + mx + 'px,' + my + 'px,0)';
      if (!shown) {
        shown = true;
        rx = mx; ry = my;
        root.classList.add('cursor-visible');
      }
    }, { passive: true });
    document.addEventListener('mouseout', function (e) {
      if (!e.relatedTarget) { shown = false; root.classList.remove('cursor-visible'); }
    });
    document.addEventListener('mouseover', function (e) {
      var hit = e.target.closest && e.target.closest('a, button, .skill-tag, [data-cursor]');
      ring.classList.toggle('hovered', !!hit);
    });
    (function loop() {
      rx += (mx - rx) * 0.2;
      ry += (my - ry) * 0.2;
      ring.style.transform = 'translate3d(' + rx.toFixed(1) + 'px,' + ry.toFixed(1) + 'px,0)';
      requestAnimationFrame(loop);
    })();
  }

  function initMagnetic() {
    if (!finePointer || reduceMotion) return;
    $$('[data-magnetic]').forEach(function (el) {
      var inner = $('[data-magnetic-inner]', el);
      var strength = 0.32;
      var follow = 'translate .25s cubic-bezier(.16,1,.3,1)';
      var settle = 'translate .9s cubic-bezier(.34,1.56,.64,1)';
      el.addEventListener('mousemove', function (e) {
        var r = el.getBoundingClientRect();
        var dx = e.clientX - (r.left + r.width / 2);
        var dy = e.clientY - (r.top + r.height / 2);
        el.style.transition = follow;
        el.style.translate = (dx * strength).toFixed(1) + 'px ' + (dy * strength).toFixed(1) + 'px';
        if (inner) {
          inner.style.transition = follow;
          inner.style.translate = (dx * strength * 0.45).toFixed(1) + 'px ' + (dy * strength * 0.45).toFixed(1) + 'px';
        }
      });
      el.addEventListener('mouseleave', function () {
        el.style.transition = settle;
        el.style.translate = '0px 0px';
        if (inner) {
          inner.style.transition = settle;
          inner.style.translate = '0px 0px';
        }
      });
    });
  }

  /* ------------------------------------------------------------------------
     Footer: live local time + year
     ------------------------------------------------------------------------ */
  function initClock() {
    var el = $('[data-clock]');
    if (!el) return;
    var fmt;
    try {
      fmt = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/Chicago',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: true, timeZoneName: 'short'
      });
    } catch (e) { return; }
    function tick() { el.textContent = fmt.format(new Date()); }
    tick();
    setInterval(tick, 1000);
  }

  function setYear() {
    var year = String(new Date().getFullYear());
    $$('[data-year]').forEach(function (el) { el.textContent = year; });
  }

  /* ------------------------------------------------------------------------ */
  initStrip();
  initMarquee();
  initPipeline();
  initSplitLines();
  initReveals();
  initAccordion();
  initMenu();
  initScrollSpy();
  initClock();
  setYear();
  initCursor();
  initMagnetic();
  runIntro();
})();
