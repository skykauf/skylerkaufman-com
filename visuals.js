(() => {
  const canvas = document.getElementById("bg");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  const reduced =
    window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;

  let w = 0;
  let h = 0;
  let dpr = 1;
  let mx = 0;
  let my = 0;
  let targetMx = 0;
  let targetMy = 0;
  let raf = 0;
  let running = false;
  const particles = [];
  const planets = [];
  const meteors = [];

  const ACCENT = { r: 196, g: 165, b: 116 };
  const LINE_DIST = 118;
  const LINE_DIST_SQ = LINE_DIST * LINE_DIST;

  function resize() {
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    w = window.innerWidth;
    h = window.innerHeight;
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(h * dpr);
    canvas.style.width = `${w}px`;
    canvas.style.height = `${h}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    initParticles();
    initPlanets();
    meteors.length = 0;
  }

  function initParticles() {
    particles.length = 0;
    const area = w * h;
    const n = reduced
      ? 0
      : Math.min(95, Math.max(36, Math.floor(area / 18000)));
    for (let i = 0; i < n; i++) {
      particles.push({
        x: Math.random() * w,
        y: Math.random() * h,
        vx: (Math.random() - 0.5) * 0.35,
        vy: (Math.random() - 0.5) * 0.35,
        r: Math.random() * 1.2 + 0.6,
      });
    }
  }

  function drawAurora(t) {
    const g1 = ctx.createRadialGradient(
      w * 0.2 + Math.sin(t * 0.00022) * w * 0.15,
      h * 0.35 + Math.cos(t * 0.00018) * h * 0.12,
      0,
      w * 0.35,
      h * 0.4,
      Math.max(w, h) * 0.55
    );
    g1.addColorStop(0, "rgba(55, 42, 78, 0.45)");
    g1.addColorStop(0.45, "rgba(18, 22, 38, 0.2)");
    g1.addColorStop(1, "rgba(12, 12, 14, 0)");
    ctx.fillStyle = g1;
    ctx.fillRect(0, 0, w, h);

    const g2 = ctx.createRadialGradient(
      w * 0.85 + Math.cos(t * 0.00025) * w * 0.1,
      h * 0.65 + Math.sin(t * 0.0002) * h * 0.1,
      0,
      w * 0.75,
      h * 0.55,
      Math.max(w, h) * 0.5
    );
    g2.addColorStop(0, "rgba(45, 62, 58, 0.35)");
    g2.addColorStop(0.5, "rgba(20, 28, 32, 0.12)");
    g2.addColorStop(1, "rgba(12, 12, 14, 0)");
    ctx.fillStyle = g2;
    ctx.fillRect(0, 0, w, h);

    const g3 = ctx.createRadialGradient(
      w * 0.5 + Math.sin(t * 0.00015 + 1) * w * 0.25,
      h * 0.15 + Math.cos(t * 0.00012) * h * 0.08,
      0,
      w * 0.5,
      h * 0.2,
      w * 0.4
    );
    g3.addColorStop(0, `rgba(${ACCENT.r}, ${ACCENT.g}, ${ACCENT.b}, 0.08)`);
    g3.addColorStop(1, "rgba(12, 12, 14, 0)");
    ctx.fillStyle = g3;
    ctx.fillRect(0, 0, w, h);
  }

  function drawStaticAurora() {
    drawAurora(0);
  }

  function initPlanets() {
    planets.length = 0;
    const s = Math.min(w, h);
    const small = w < 520;

    planets.push({
      relX: 0.1,
      relY: 0.2,
      r: s * (small ? 0.06 : 0.072),
      drift: s * 0.012,
      phase: 1.7,
      ring: false,
      core: [120, 90, 140],
      edge: [55, 38, 72],
      rim: [180, 160, 210],
    });
    planets.push({
      relX: 0.88,
      relY: 0.28,
      r: s * (small ? 0.045 : 0.055),
      drift: s * 0.009,
      phase: 4.1,
      ring: true,
      tilt: 0.42,
      core: [70, 95, 118],
      edge: [28, 42, 52],
      rim: [130, 165, 188],
    });
    planets.push({
      relX: 0.72,
      relY: 0.78,
      r: s * (small ? 0.035 : 0.042),
      drift: s * 0.015,
      phase: 2.4,
      ring: false,
      core: [140, 100, 85],
      edge: [72, 48, 38],
      rim: [210, 170, 140],
    });
    if (!small) {
      planets.push({
        relX: 0.32,
        relY: 0.72,
        r: s * 0.028,
        drift: s * 0.008,
        phase: 5.2,
        ring: false,
        core: [90, 110, 130],
        edge: [40, 52, 68],
        rim: [150, 175, 195],
      });
    }
  }

  function drawPlanets(t) {
    const time = reduced ? 0 : t * 0.00012;

    for (const p of planets) {
      const ox = Math.sin(time + p.phase) * p.drift;
      const oy = Math.cos(time * 0.85 + p.phase * 0.7) * p.drift * 0.6;
      const cx = p.relX * w + ox;
      const cy = p.relY * h + oy;

      const body = ctx.createRadialGradient(
        cx - p.r * 0.35,
        cy - p.r * 0.35,
        0,
        cx,
        cy,
        p.r
      );
      body.addColorStop(
        0,
        `rgba(${p.core[0]}, ${p.core[1]}, ${p.core[2]}, 0.95)`
      );
      body.addColorStop(
        0.55,
        `rgba(${p.edge[0]}, ${p.edge[1]}, ${p.edge[2]}, 0.88)`
      );
      body.addColorStop(
        1,
        `rgba(${p.edge[0] * 0.5}, ${p.edge[1] * 0.5}, ${p.edge[2] * 0.5}, 0)`
      );
      ctx.fillStyle = body;
      ctx.beginPath();
      ctx.arc(cx, cy, p.r, 0, Math.PI * 2);
      ctx.fill();

      const limb = ctx.createRadialGradient(cx, cy, p.r * 0.65, cx, cy, p.r * 1.35);
      limb.addColorStop(0, "rgba(255,255,255,0)");
      limb.addColorStop(
        0.88,
        `rgba(${p.rim[0]}, ${p.rim[1]}, ${p.rim[2]}, 0.22)`
      );
      limb.addColorStop(1, "rgba(0,0,0,0)");
      ctx.fillStyle = limb;
      ctx.beginPath();
      ctx.arc(cx, cy, p.r * 1.2, 0, Math.PI * 2);
      ctx.fill();

      if (p.ring) {
        ctx.save();
        ctx.translate(cx, cy);
        ctx.rotate(p.tilt);
        ctx.scale(1, 0.22);
        ctx.strokeStyle = `rgba(${p.rim[0]}, ${p.rim[1]}, ${p.rim[2]}, 0.35)`;
        ctx.lineWidth = p.r * 0.09;
        ctx.beginPath();
        ctx.arc(0, 0, p.r * 1.55, 0, Math.PI * 2);
        ctx.stroke();
        ctx.strokeStyle = `rgba(255,255,255,0.06)`;
        ctx.lineWidth = p.r * 0.04;
        ctx.beginPath();
        ctx.arc(0, 0, p.r * 1.42, 0.2, Math.PI - 0.2);
        ctx.stroke();
        ctx.restore();
      }

      ctx.fillStyle = `rgba(${ACCENT.r}, ${ACCENT.g}, ${ACCENT.b}, 0.12)`;
      ctx.beginPath();
      ctx.arc(cx - p.r * 0.2, cy - p.r * 0.25, p.r * 0.12, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  function spawnMeteor() {
    const fromTop = Math.random() > 0.28;
    let x;
    let y;
    let vx;
    let vy;
    const speed = 11 + Math.random() * 14;

    if (fromTop) {
      x = Math.random() * (w + 160) - 80;
      y = -30 - Math.random() * 120;
      const spread = 0.35 + Math.random() * 0.55;
      vx = Math.cos(spread) * speed;
      vy = Math.sin(spread) * speed;
    } else {
      x = -40 - Math.random() * 100;
      y = Math.random() * h * 0.55;
      vx = speed * (0.85 + Math.random() * 0.2);
      vy = 2 + Math.random() * 5;
    }

    meteors.push({
      x,
      y,
      vx,
      vy,
      life: 1,
      len: 70 + Math.random() * 100,
    });
  }

  function stepMeteors() {
    if (Math.random() < 0.007) spawnMeteor();

    for (let i = meteors.length - 1; i >= 0; i--) {
      const m = meteors[i];
      m.x += m.vx;
      m.y += m.vy;
      m.life -= 0.014;
      if (m.life <= 0 || m.x > w + 120 || m.y > h + 120) {
        meteors.splice(i, 1);
      }
    }
  }

  function drawMeteors() {
    for (const m of meteors) {
      const spd = Math.hypot(m.vx, m.vy) || 1;
      const nx = -m.vx / spd;
      const ny = -m.vy / spd;
      const tail = m.len * Math.max(0.15, m.life);
      const x1 = m.x;
      const y1 = m.y;
      const x0 = x1 + nx * tail;
      const y0 = y1 + ny * tail;

      const g = ctx.createLinearGradient(x0, y0, x1, y1);
      g.addColorStop(0, "rgba(255,255,255,0)");
      g.addColorStop(0.55, `rgba(${ACCENT.r}, ${ACCENT.g}, ${ACCENT.b}, ${0.15 * m.life})`);
      g.addColorStop(0.92, `rgba(230,235,255,${0.55 * m.life})`);
      g.addColorStop(1, `rgba(255,255,255,${0.95 * m.life})`);

      ctx.strokeStyle = g;
      ctx.lineWidth = 1.1 + m.life * 0.8;
      ctx.lineCap = "round";
      ctx.beginPath();
      ctx.moveTo(x0, y0);
      ctx.lineTo(x1, y1);
      ctx.stroke();

      ctx.fillStyle = `rgba(255,255,255,${0.55 * m.life})`;
      ctx.beginPath();
      ctx.arc(x1, y1, 1.1 + m.life, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  function stepParticles() {
    const influence = 140;
    const influenceSq = influence * influence;

    for (const p of particles) {
      const dx = p.x - mx;
      const dy = p.y - my;
      const dsq = dx * dx + dy * dy;
      if (dsq < influenceSq && dsq > 1) {
        const d = Math.sqrt(dsq);
        const f = ((influence - d) / influence) * 0.08;
        p.vx += (dx / d) * f;
        p.vy += (dy / d) * f;
      }

      p.x += p.vx;
      p.y += p.vy;
      p.vx *= 0.988;
      p.vy *= 0.988;

      const margin = 40;
      if (p.x < -margin) p.x = w + margin;
      if (p.x > w + margin) p.x = -margin;
      if (p.y < -margin) p.y = h + margin;
      if (p.y > h + margin) p.y = -margin;
    }
  }

  function drawNetwork() {
    const len = particles.length;
    for (let i = 0; i < len; i++) {
      for (let j = i + 1; j < len; j++) {
        const a = particles[i];
        const b = particles[j];
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const dsq = dx * dx + dy * dy;
        if (dsq > LINE_DIST_SQ) continue;
        const alpha = (1 - dsq / LINE_DIST_SQ) * 0.22;
        ctx.strokeStyle = `rgba(${ACCENT.r}, ${ACCENT.g}, ${ACCENT.b}, ${alpha})`;
        ctx.lineWidth = 0.6;
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
    }

    for (const p of particles) {
      const dx = p.x - mx;
      const dy = p.y - my;
      const glow = Math.min(1, 120 / (Math.sqrt(dx * dx + dy * dy) + 40));
      ctx.fillStyle = `rgba(${ACCENT.r}, ${ACCENT.g}, ${ACCENT.b}, ${0.35 + glow * 0.45})`;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  function frame(t) {
    if (document.hidden) {
      running = false;
      raf = 0;
      return;
    }

    mx += (targetMx - mx) * 0.06;
    my += (targetMy - my) * 0.06;

    ctx.fillStyle = "#0a0a0c";
    ctx.fillRect(0, 0, w, h);
    drawAurora(t);
    drawPlanets(t);
    stepParticles();
    drawNetwork();
    stepMeteors();
    drawMeteors();

    raf = requestAnimationFrame(frame);
  }

  function startLoop() {
    if (reduced || running) return;
    running = true;
    raf = requestAnimationFrame(frame);
  }

  function stopLoop() {
    running = false;
    if (raf) cancelAnimationFrame(raf);
    raf = 0;
  }

  function onMove(e) {
    targetMx = e.clientX;
    targetMy = e.clientY;
  }

  function onLeave() {
    targetMx = w * 0.5;
    targetMy = h * 0.5;
  }

  window.addEventListener("resize", resize, { passive: true });
  window.addEventListener("pointermove", onMove, { passive: true });
  window.addEventListener("pointerleave", onLeave);

  resize();
  targetMx = mx = w * 0.5;
  targetMy = my = h * 0.5;

  if (reduced) {
    ctx.fillStyle = "#0a0a0c";
    ctx.fillRect(0, 0, w, h);
    drawStaticAurora();
    drawPlanets(0);
  } else {
    startLoop();
  }

  document.addEventListener("visibilitychange", () => {
    if (reduced) return;
    if (document.hidden) stopLoop();
    else startLoop();
  });
})();
