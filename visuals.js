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
    stepParticles();
    drawNetwork();

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
  } else {
    startLoop();
  }

  document.addEventListener("visibilitychange", () => {
    if (reduced) return;
    if (document.hidden) stopLoop();
    else startLoop();
  });
})();
