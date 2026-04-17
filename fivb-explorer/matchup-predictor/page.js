(function () {
  const statusEl = document.getElementById("status");
  const predictFormEl = document.getElementById("predictForm");
  const predictionResultsEl = document.getElementById("predictionResults");
  const playerOptionsEl = document.getElementById("playerOptions");

  const fields = {
    a1: document.getElementById("playerA1"),
    a2: document.getElementById("playerA2"),
    b1: document.getElementById("playerB1"),
    b2: document.getElementById("playerB2"),
    genderPool: document.getElementById("genderPool"),
  };

  const playerCache = new Map();
  const suggestionCache = new Map();
  const directoryCache = new Map();
  let calibrationPromise = null;
  let suggestionTimer = 0;
  const DEFAULT_MATCHUP = {
    a1: "Paul Pascariuc #156224",
    a2: "Alexander Horst #103677",
    b1: "Dexter Campbell #222241",
    b2: "Skyler Kaufman #219467",
    genderPool: "0",
  };

  function esc(v) {
    return String(v ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function toNum(v, fallback = 0) {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  function pct(v) {
    return `${(toNum(v) * 100).toFixed(1)}%`;
  }

  function fmt(v, digits = 1) {
    return toNum(v).toLocaleString("en-US", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }

  function parseSets(scoreSets) {
    if (!scoreSets || typeof scoreSets !== "string") return null;
    const m = scoreSets.match(/(\d+)\s*-\s*(\d+)/);
    if (!m) return null;
    const left = Number(m[1]);
    const right = Number(m[2]);
    if (!Number.isFinite(left) || !Number.isFinite(right)) return null;
    return { left, right };
  }

  function computeRecentForm(playerId, timeline, recentWindow) {
    const pid = String(playerId);
    const rows = Array.isArray(timeline) ? timeline : [];
    const eligible = rows
      .filter((r) => r && r.score_sets)
      .slice(0, recentWindow);

    let wins = 0;
    let total = 0;
    let setsWon = 0;
    let setsLost = 0;

    for (const row of eligible) {
      const sets = parseSets(row.score_sets);
      if (!sets) continue;
      const onTeamA = [row.no_player_a1, row.no_player_a2].some((id) => String(id || "") === pid);
      const onTeamB = [row.no_player_b1, row.no_player_b2].some((id) => String(id || "") === pid);
      if (!onTeamA && !onTeamB) continue;

      const meSets = onTeamA ? sets.left : sets.right;
      const oppSets = onTeamA ? sets.right : sets.left;
      wins += meSets > oppSets ? 1 : 0;
      setsWon += meSets;
      setsLost += oppSets;
      total += 1;
    }

    const eloValues = rows
      .map((r) => toNum(r.elo_rating, NaN))
      .filter((n) => Number.isFinite(n));
    const latestElo = eloValues.length ? eloValues[0] : 1500;
    const earlierElo = eloValues.length > 1 ? eloValues[Math.min(eloValues.length - 1, recentWindow - 1)] : latestElo;
    const eloTrend = latestElo - earlierElo;

    return {
      recentMatches: total,
      winRate: total > 0 ? wins / total : 0.5,
      avgSetDiff: total > 0 ? (setsWon - setsLost) / total : 0,
      eloTrend,
      latestElo,
    };
  }

  function formToPoints(form) {
    const winTerm = (form.winRate - 0.5) * 110;
    const setTerm = form.avgSetDiff * 38;
    const trendTerm = form.eloTrend * 0.2;
    return winTerm + setTerm + trendTerm;
  }

  function logisticElo(teamA, teamB) {
    return 1 / (1 + 10 ** ((teamB - teamA) / 400));
  }

  function normalizeDistribution(rows) {
    const total = rows.reduce((sum, row) => sum + toNum(row.prob), 0);
    if (total <= 0) return rows;
    return rows.map((row) => ({ ...row, prob: row.prob / total }));
  }

  function fallbackCalibration() {
    return {
      set21: {
        regular: [
          { loser_points: 15, pct: 0.13 },
          { loser_points: 16, pct: 0.15 },
          { loser_points: 17, pct: 0.18 },
          { loser_points: 18, pct: 0.2 },
          { loser_points: 19, pct: 0.19 },
          { loser_points: 20, pct: 0.15 },
        ],
        overtime: [
          { loser_points: 20, pct: 0.38 },
          { loser_points: 21, pct: 0.34 },
          { loser_points: 22, pct: 0.18 },
          { loser_points: 23, pct: 0.1 },
        ],
      },
      set15: {
        regular: [
          { loser_points: 9, pct: 0.12 },
          { loser_points: 10, pct: 0.16 },
          { loser_points: 11, pct: 0.2 },
          { loser_points: 12, pct: 0.21 },
          { loser_points: 13, pct: 0.19 },
          { loser_points: 14, pct: 0.12 },
        ],
        overtime: [
          { loser_points: 14, pct: 0.46 },
          { loser_points: 15, pct: 0.32 },
          { loser_points: 16, pct: 0.14 },
          { loser_points: 17, pct: 0.08 },
        ],
      },
    };
  }

  async function getCalibration() {
    if (calibrationPromise) return calibrationPromise;
    calibrationPromise = (async () => {
      try {
        const out = await callExplorer("matchup_calibration", {});
        const cal = out && out.calibration ? out.calibration : null;
        if (!cal) return fallbackCalibration();
        return cal;
      } catch (_) {
        return fallbackCalibration();
      }
    })();
    return calibrationPromise;
  }

  function pickLoserPoints(distribution, closenessBias) {
    const rows = Array.isArray(distribution) ? distribution : [];
    if (!rows.length) return null;
    const sorted = [...rows].sort((a, b) => Number(a.loser_points) - Number(b.loser_points));
    const total = sorted.reduce((sum, row) => sum + toNum(row.pct, 0), 0) || 1;
    const targetQ = clamp(0.18 + closenessBias * 0.64, 0.04, 0.96);
    let run = 0;
    for (const row of sorted) {
      run += toNum(row.pct, 0) / total;
      if (run >= targetQ) return Number(row.loser_points);
    }
    return Number(sorted[sorted.length - 1].loser_points);
  }

  function makeSetScore(targetPoints, loserPoints, overtime) {
    const loser = clamp(Math.round(loserPoints), 0, overtime ? 200 : targetPoints - 1);
    const winner = overtime ? loser + 2 : targetPoints;
    return `${winner}-${loser}`;
  }

  function flipScore(score) {
    const m = String(score).match(/^(\d+)-(\d+)$/);
    if (!m) return score;
    return `${m[2]}-${m[1]}`;
  }

  function solveSetWinProbFromMatchProb(matchProb) {
    const target = clamp(toNum(matchProb, 0.5), 0.001, 0.999);
    let lo = 0.001;
    let hi = 0.999;
    for (let i = 0; i < 44; i += 1) {
      const mid = (lo + hi) * 0.5;
      const bestOf3 = mid * mid * (3 - 2 * mid);
      if (bestOf3 < target) lo = mid;
      else hi = mid;
    }
    return (lo + hi) * 0.5;
  }

  function normalizeProbMap(probMap) {
    const total = Object.values(probMap).reduce((sum, v) => sum + toNum(v, 0), 0);
    if (total <= 0) return probMap;
    const out = {};
    Object.keys(probMap).forEach((k) => {
      out[k] = probMap[k] / total;
    });
    return out;
  }

  function buildLoserPointDist(targetPoints, calBlock, winnerStrength) {
    const regularRows = Array.isArray(calBlock?.regular) ? calBlock.regular : [];
    const overtimeRows = Array.isArray(calBlock?.overtime) ? calBlock.overtime : [];
    const regularMass = regularRows.reduce((sum, row) => sum + toNum(row.pct, 0), 0);
    const overtimeMass = overtimeRows.reduce((sum, row) => sum + toNum(row.pct, 0), 0);
    const rawTotal = regularMass + overtimeMass;

    const observedOvertimeShare = rawTotal > 0 ? overtimeMass / rawTotal : targetPoints === 15 ? 0.12 : 0.1;
    const overtimeShare = clamp(
      observedOvertimeShare * (1 + (1 - Math.abs(winnerStrength)) * 0.3),
      0.02,
      targetPoints === 15 ? 0.35 : 0.28
    );
    const regularShare = 1 - overtimeShare;

    const tilt = -winnerStrength * 1.2;
    const scoreWeights = {};

    const regTotal = regularRows.reduce((sum, row) => sum + toNum(row.pct, 0), 0) || 1;
    regularRows.forEach((row) => {
      const lp = Math.floor(toNum(row.loser_points, targetPoints - 2));
      if (lp < 0 || lp > targetPoints - 2) return;
      const base = toNum(row.pct, 0) / regTotal;
      const centered = lp - (targetPoints - 2) / 2;
      scoreWeights[lp] = (scoreWeights[lp] || 0) + base * Math.exp((tilt * centered) / (targetPoints / 2));
    });

    const regNorm = normalizeProbMap(scoreWeights);
    const out = {};
    Object.keys(regNorm).forEach((k) => {
      out[k] = regNorm[k] * regularShare;
    });

    const overtimeWeights = {};
    const otTotal = overtimeRows.reduce((sum, row) => sum + toNum(row.pct, 0), 0) || 1;
    let maxObserved = targetPoints + 3;
    overtimeRows.forEach((row) => {
      const lp = Math.max(targetPoints - 1, Math.floor(toNum(row.loser_points, targetPoints - 1)));
      maxObserved = Math.max(maxObserved, lp);
      const base = toNum(row.pct, 0) / otTotal;
      overtimeWeights[lp] = (overtimeWeights[lp] || 0) + base;
    });

    const otObservedNorm = normalizeProbMap(overtimeWeights);
    let observedMass = 0;
    Object.keys(otObservedNorm).forEach((k) => {
      const prob = otObservedNorm[k] * overtimeShare * 0.85;
      out[k] = (out[k] || 0) + prob;
      observedMass += prob;
    });

    const tailMass = Math.max(0, overtimeShare - observedMass);
    if (tailMass > 0) {
      const tailRatio = 0.57;
      let remaining = tailMass;
      let lp = maxObserved + 1;
      const tailCap = targetPoints === 15 ? 40 : 55;
      while (remaining > 1e-5 && lp < tailCap) {
        const part = remaining * (1 - tailRatio);
        out[lp] = (out[lp] || 0) + part;
        remaining *= tailRatio;
        lp += 1;
      }
      if (remaining > 0) {
        const k = Math.min(tailCap, lp);
        out[k] = (out[k] || 0) + remaining;
      }
    }

    return normalizeProbMap(out);
  }

  function buildSetOutcomes(targetPoints, calBlock, winnerIsTeamA, winnerStrength) {
    const loserDist = buildLoserPointDist(targetPoints, calBlock, winnerStrength);
    const rows = [];
    Object.keys(loserDist).forEach((k) => {
      const loserPoints = Number(k);
      const overtime = loserPoints >= targetPoints - 1;
      const winnerPoints = overtime ? loserPoints + 2 : targetPoints;
      const aPoints = winnerIsTeamA ? winnerPoints : loserPoints;
      const bPoints = winnerIsTeamA ? loserPoints : winnerPoints;
      rows.push({
        score: `${aPoints}-${bPoints}`,
        prob: toNum(loserDist[k], 0),
      });
    });
    return rows;
  }

  function combineMatchRows(setRows, bucket) {
    const map = new Map();
    setRows.forEach((row) => {
      const key = row.score;
      const prev = map.get(key) || 0;
      map.set(key, prev + toNum(row.prob, 0));
    });
    return Array.from(map.entries()).map(([score, prob]) => ({ score, prob, bucket }));
  }

  function scoreDistribution(pWin, teamASetDiff, teamBSetDiff, calibration) {
    const cal = calibration || fallbackCalibration();
    const strength = clamp((pWin - 0.5) * 2 + (teamASetDiff - teamBSetDiff) * 0.12, -1, 1);
    const pSetA = solveSetWinProbFromMatchProb(pWin);
    const pSet3A = clamp(0.5 + (pSetA - 0.5) * 0.9, 0.04, 0.96);

    const set21A = buildSetOutcomes(21, cal.set21, true, strength);
    const set21B = buildSetOutcomes(21, cal.set21, false, -strength);
    const set15A = buildSetOutcomes(15, cal.set15, true, strength);
    const set15B = buildSetOutcomes(15, cal.set15, false, -strength);

    const win2Rows = [];
    set21A.forEach((s1) => {
      set21A.forEach((s2) => {
        win2Rows.push({
          score: `${s1.score}, ${s2.score}`,
          prob: pSetA * pSetA * s1.prob * s2.prob,
        });
      });
    });

    const win3Rows = [];
    set21A.forEach((s1) => {
      set21B.forEach((s2) => {
        set15A.forEach((s3) => {
          win3Rows.push({
            score: `${s1.score}, ${s2.score}, ${s3.score}`,
            prob: pSetA * (1 - pSetA) * pSet3A * s1.prob * s2.prob * s3.prob,
          });
          win3Rows.push({
            score: `${s2.score}, ${s1.score}, ${s3.score}`,
            prob: (1 - pSetA) * pSetA * pSet3A * s2.prob * s1.prob * s3.prob,
          });
        });
      });
    });

    const lose3Rows = [];
    set21A.forEach((s1) => {
      set21B.forEach((s2) => {
        set15B.forEach((s3) => {
          lose3Rows.push({
            score: `${s1.score}, ${s2.score}, ${s3.score}`,
            prob: pSetA * (1 - pSetA) * (1 - pSet3A) * s1.prob * s2.prob * s3.prob,
          });
          lose3Rows.push({
            score: `${s2.score}, ${s1.score}, ${s3.score}`,
            prob: (1 - pSetA) * pSetA * (1 - pSet3A) * s2.prob * s1.prob * s3.prob,
          });
        });
      });
    });

    const lose2Rows = [];
    set21B.forEach((s1) => {
      set21B.forEach((s2) => {
        lose2Rows.push({
          score: `${s1.score}, ${s2.score}`,
          prob: (1 - pSetA) * (1 - pSetA) * s1.prob * s2.prob,
        });
      });
    });

    const rows = [
      ...combineMatchRows(win2Rows, "win_2"),
      ...combineMatchRows(win3Rows, "win_3"),
      ...combineMatchRows(lose3Rows, "lose_3"),
      ...combineMatchRows(lose2Rows, "lose_2"),
    ];

    return normalizeDistribution(rows);
  }

  async function callExplorer(action, filters) {
    const res = await fetch("/api/fivb-table-explorer", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, filters }),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error(data.error || data.reason || "Explorer request failed.");
    }
    return data;
  }

  function normalizePlayerInput(text) {
    const value = String(text || "").trim();
    if (!value) return null;
    if (/^\d+$/.test(value)) return { id: value };
    const m = value.match(/#(\d+)\s*$/);
    if (m) return { id: m[1] };
    return { name: value };
  }

  async function resolvePlayer(rawInput, genderPool) {
    const parsed = normalizePlayerInput(rawInput);
    if (!parsed) throw new Error("Each player field must be filled.");

    if (parsed.id && playerCache.has(`id:${parsed.id}`)) {
      return playerCache.get(`id:${parsed.id}`);
    }
    if (parsed.name && playerCache.has(`name:${parsed.name.toLowerCase()}`)) {
      return playerCache.get(`name:${parsed.name.toLowerCase()}`);
    }

    let resolved;
    if (parsed.id) {
      const out = await callExplorer("player_history", { player_id: parsed.id, history_limit: 25 });
      if (!out.profile) throw new Error(`Could not find player with ID ${parsed.id}.`);
      resolved = out;
    } else {
      const search = await callExplorer("search_players", {
        name: parsed.name,
        gender: genderPool,
        limit: 1,
      });
      const top = search.rows && search.rows[0];
      if (!top || !top.player_id) throw new Error(`No player found for "${parsed.name}".`);
      resolved = await callExplorer("player_history", { player_id: top.player_id, history_limit: 25 });
    }

    const profile = resolved.profile || {};
    const keyId = `id:${profile.player_id}`;
    const keyName = `name:${String(profile.full_name || "").toLowerCase()}`;
    playerCache.set(keyId, resolved);
    playerCache.set(keyName, resolved);
    return resolved;
  }

  async function refreshSuggestions(query) {
    const q = String(query || "").trim();
    if (q.length < 2) return;
    const genderPool = fields.genderPool?.value || "0";
    const key = `${genderPool}:${q}`;
    if (suggestionCache.has(key)) {
      renderSuggestions(suggestionCache.get(key));
      return;
    }
    try {
      const out = await callExplorer("search_players", { name: q, gender: genderPool, limit: 20 });
      suggestionCache.set(key, out.rows || []);
      renderSuggestions(out.rows || []);
    } catch (_) {
      // Keep silent to avoid status flicker while typing.
    }
  }

  async function loadPlayerDirectory(genderPool) {
    const pool = String(genderPool || "0");
    if (directoryCache.has(pool)) {
      renderSuggestions(directoryCache.get(pool));
      return;
    }
    statusEl.textContent = "Loading player dropdown...";
    try {
      const out = await callExplorer("player_directory", { gender: pool, limit: 50000 });
      const rows = Array.isArray(out.rows) ? out.rows : [];
      directoryCache.set(pool, rows);
      renderSuggestions(rows);
      statusEl.textContent = `Loaded ${rows.length.toLocaleString("en-US")} players for dropdown.`;
    } catch (err) {
      statusEl.textContent = err.message || "Failed to load player dropdown.";
    }
  }

  function renderSuggestions(rows) {
    const options = (rows || [])
      .map((r) => {
        const name = r.full_name || "Unknown";
        const cc = r.country_code || "UNK";
        const id = r.player_id || "";
        return `<option value="${esc(name)} #${esc(id)}">${esc(cc)} | ID ${esc(id)}</option>`;
      })
      .join("");
    playerOptionsEl.innerHTML = options;
  }

  function parseMatchMargin(score) {
    const sets = String(score || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    let margin = 0;
    for (const setScore of sets) {
      const m = setScore.match(/(\d+)\s*-\s*(\d+)/);
      if (!m) continue;
      margin += Number(m[1]) - Number(m[2]);
    }
    return margin;
  }

  function aggregateMargins(distribution) {
    const map = new Map();
    for (const row of distribution || []) {
      const m = parseMatchMargin(row.score);
      const p = toNum(row.prob, 0);
      map.set(m, (map.get(m) || 0) + p);
    }
    return map;
  }

  function aggregateSetMargins(distribution, setIndex) {
    const map = new Map();
    let totalProb = 0;
    for (const row of distribution || []) {
      const sets = String(row.score || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (setIndex >= sets.length) continue;
      const m = sets[setIndex].match(/(\d+)\s*-\s*(\d+)/);
      if (!m) continue;
      const margin = Number(m[1]) - Number(m[2]);
      const p = toNum(row.prob, 0);
      map.set(margin, (map.get(margin) || 0) + p);
      totalProb += p;
    }
    return { marginMap: map, totalProb };
  }

  function bucketMargins(marginMap, bucketSize = 2) {
    const map = new Map();
    marginMap.forEach((prob, margin) => {
      const center = Math.round(margin / bucketSize) * bucketSize;
      map.set(center, (map.get(center) || 0) + prob);
    });
    return Array.from(map.entries())
      .map(([center, prob]) => ({ center: Number(center), prob: toNum(prob, 0) }))
      .sort((a, b) => a.center - b.center);
  }

  function buildMarginChartSvg(bins, expectedMargin) {
    if (!bins || bins.length === 0) return "";
    const width = 820;
    const height = 260;
    const pad = { left: 40, right: 18, top: 18, bottom: 42 };
    const plotW = width - pad.left - pad.right;
    const plotH = height - pad.top - pad.bottom;

    const minX = bins[0].center;
    const maxX = bins[bins.length - 1].center;
    const span = Math.max(1, maxX - minX);
    const maxP = Math.max(...bins.map((b) => b.prob), 0.000001);
    const step = bins.length > 1 ? (plotW / span) * (bins[1].center - bins[0].center) : 28;
    const barW = clamp(step * 0.78, 4, 24);

    const x = (v) => pad.left + ((v - minX) / span) * plotW;
    const y = (p) => pad.top + plotH - (p / maxP) * plotH;
    const zeroX = x(clamp(0, minX, maxX));
    const expX = x(clamp(expectedMargin, minX, maxX));
    const closeLo = x(clamp(-4, minX, maxX));
    const closeHi = x(clamp(4, minX, maxX));

    const bars = bins
      .map((b) => {
        const bx = x(b.center) - barW / 2;
        const by = y(b.prob);
        const bh = pad.top + plotH - by;
        const color = b.center < 0 ? "#a96a6a" : b.center > 0 ? "#6f8ddd" : "#9a97b8";
        return `<rect x="${bx.toFixed(2)}" y="${by.toFixed(2)}" width="${barW.toFixed(
          2
        )}" height="${bh.toFixed(2)}" rx="2" fill="${color}" />`;
      })
      .join("");

    const tickVals = Array.from(
      new Set([minX, -4, 0, 4, maxX].filter((v) => v >= minX && v <= maxX).map((v) => Math.round(v)))
    ).sort((a, b) => a - b);
    const ticks = tickVals
      .map((v) => {
        const tx = x(v);
        return `
          <line x1="${tx.toFixed(2)}" y1="${pad.top + plotH}" x2="${tx.toFixed(2)}" y2="${
            pad.top + plotH + 5
          }" stroke="#6f6d7f" stroke-width="1" />
          <text x="${tx.toFixed(2)}" y="${pad.top + plotH + 18}" text-anchor="middle" fill="#9a978f" font-size="11">${v}</text>
        `;
      })
      .join("");

    return `
      <svg viewBox="0 0 ${width} ${height}" class="margin-svg" aria-label="Match margin distribution">
        <rect x="0" y="0" width="${width}" height="${height}" fill="#131420" />
        <rect x="${closeLo.toFixed(2)}" y="${pad.top}" width="${Math.max(0, closeHi - closeLo).toFixed(
          2
        )}" height="${plotH}" fill="rgba(176, 169, 226, 0.12)" />
        <line x1="${pad.left}" y1="${pad.top + plotH}" x2="${pad.left + plotW}" y2="${pad.top + plotH}" stroke="#3a3a49" />
        ${bars}
        <line x1="${zeroX.toFixed(2)}" y1="${pad.top}" x2="${zeroX.toFixed(
          2
        )}" y2="${pad.top + plotH}" stroke="#b6b4c6" stroke-width="1.3" stroke-dasharray="4 3" />
        <line x1="${expX.toFixed(2)}" y1="${pad.top}" x2="${expX.toFixed(
          2
        )}" y2="${pad.top + plotH}" stroke="#7fc0ac" stroke-width="1.6" />
        ${ticks}
        <text x="${pad.left + 4}" y="${pad.top + 12}" fill="#9a978f" font-size="11">Probability density</text>
      </svg>
    `;
  }

  function renderResult(model) {
    const bucketMap = {
      win_2: "Team A win in 2 sets",
      win_3: "Team A win in 3 sets",
      lose_3: "Team B win in 3 sets",
      lose_2: "Team B win in 2 sets",
    };
    const bucketHtml = Object.entries(bucketMap)
      .map(([k, label]) => {
        const prob = (model.distribution || [])
          .filter((row) => row.bucket === k)
          .reduce((sum, row) => sum + toNum(row.prob, 0), 0);
        return `
          <div class="bucket-item">
            <div class="bucket-label">${esc(label)}</div>
            <div class="bucket-value">${pct(prob)}</div>
          </div>
        `;
      })
      .join("");

    const marginMap = aggregateMargins(model.distribution || []);
    const marginBins = bucketMargins(marginMap, 2);
    const expectedMargin = (model.distribution || []).reduce(
      (sum, row) => sum + parseMatchMargin(row.score) * toNum(row.prob, 0),
      0
    );
    const teamAWinFromArea = (model.distribution || [])
      .filter((row) => parseMatchMargin(row.score) > 0)
      .reduce((sum, row) => sum + toNum(row.prob, 0), 0);
    const closeZoneProb = (model.distribution || [])
      .filter((row) => Math.abs(parseMatchMargin(row.score)) <= 4)
      .reduce((sum, row) => sum + toNum(row.prob, 0), 0);
    const marginSvg = buildMarginChartSvg(marginBins, expectedMargin);

    const set1 = aggregateSetMargins(model.distribution || [], 0);
    const set2 = aggregateSetMargins(model.distribution || [], 1);
    const set3 = aggregateSetMargins(model.distribution || [], 2);

    function renderSetPanel(title, setAgg) {
      const bins = bucketMargins(setAgg.marginMap, 2);
      const total = Math.max(setAgg.totalProb, 0);
      if (!bins.length || total <= 0) {
        return `
          <div class="set-panel">
            <h4>${esc(title)}</h4>
            <p class="muted">No outcomes in current distribution.</p>
          </div>
        `;
      }
      const condExpected =
        Array.from(setAgg.marginMap.entries()).reduce((sum, [m, p]) => sum + Number(m) * toNum(p, 0), 0) / total;
      const teamASetWin =
        Array.from(setAgg.marginMap.entries())
          .filter(([m]) => Number(m) > 0)
          .reduce((sum, [, p]) => sum + toNum(p, 0), 0) / total;
      const closeZone =
        Array.from(setAgg.marginMap.entries())
          .filter(([m]) => Math.abs(Number(m)) <= 2)
          .reduce((sum, [, p]) => sum + toNum(p, 0), 0) / total;
      const svg = buildMarginChartSvg(bins, condExpected);
      return `
        <div class="set-panel">
          <h4>${esc(title)}</h4>
          <div class="set-panel-meta">
            <span>Played in: <strong>${pct(total)}</strong></span>
            <span>Team A set win: <strong>${pct(teamASetWin)}</strong></span>
            <span>E[set margin]: <strong>${condExpected.toFixed(1)}</strong></span>
            <span>Close zone (-2 to +2): <strong>${pct(closeZone)}</strong></span>
          </div>
          <div class="margin-chart-wrap">${svg}</div>
        </div>
      `;
    }

    predictionResultsEl.innerHTML = `
      <div class="kpis">
        <div class="kpi">
          <div class="kpi-label">Team A win probability</div>
          <div class="kpi-value">${pct(model.teamAWinProb)}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Team B win probability</div>
          <div class="kpi-value">${pct(1 - model.teamAWinProb)}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Team A effective rating</div>
          <div class="kpi-value">${fmt(model.teamARating)}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Team B effective rating</div>
          <div class="kpi-value">${fmt(model.teamBRating)}</div>
        </div>
      </div>
      <p class="muted">
        Effective rating = average career Elo + recent-form adjustment (win rate, set differential,
        and Elo trend over the selected recent-match window).
      </p>
      <div class="bucket-grid">${bucketHtml}</div>
      <h3 class="outcome-title">Match point-margin distribution</h3>
      <div class="margin-metrics">
        <span>Area right of 0 (Team A win): <strong>${pct(teamAWinFromArea)}</strong></span>
        <span>Expected margin: <strong>${expectedMargin.toFixed(1)}</strong></span>
        <span>Close-match zone (-4 to +4): <strong>${pct(closeZoneProb)}</strong></span>
      </div>
      <div class="margin-chart-wrap">${marginSvg}</div>
      <p class="muted">
        Left of zero indicates Team B-leaning outcomes. Right of zero indicates Team A-leaning outcomes.
      </p>
      <h3 class="outcome-title">Set-level margin distributions</h3>
      <div class="set-grid">
        ${renderSetPanel("Set 1", set1)}
        ${renderSetPanel("Set 2", set2)}
        ${renderSetPanel("Set 3 (if played)", set3)}
      </div>
    `;
  }

  async function runPrediction(event) {
    event.preventDefault();
    statusEl.textContent = "Loading players and computing prediction...";
    predictionResultsEl.innerHTML = '<p class="muted">Running model...</p>';

    try {
      const calibration = await getCalibration();
      const recentWindow = 12;
      const genderPool = fields.genderPool?.value || "0";
      const pA1 = await resolvePlayer(fields.a1.value, genderPool);
      const pA2 = await resolvePlayer(fields.a2.value, genderPool);
      const pB1 = await resolvePlayer(fields.b1.value, genderPool);
      const pB2 = await resolvePlayer(fields.b2.value, genderPool);

      const ids = [
        pA1.profile.player_id,
        pA2.profile.player_id,
        pB1.profile.player_id,
        pB2.profile.player_id,
      ].map((v) => String(v));
      const unique = new Set(ids);
      if (unique.size !== 4) {
        throw new Error("Please select four distinct players.");
      }

      const fA1 = computeRecentForm(pA1.profile.player_id, pA1.timeline, recentWindow);
      const fA2 = computeRecentForm(pA2.profile.player_id, pA2.timeline, recentWindow);
      const fB1 = computeRecentForm(pB1.profile.player_id, pB1.timeline, recentWindow);
      const fB2 = computeRecentForm(pB2.profile.player_id, pB2.timeline, recentWindow);

      const teamACareerElo = (fA1.latestElo + fA2.latestElo) / 2;
      const teamBCareerElo = (fB1.latestElo + fB2.latestElo) / 2;

      const teamAFormPoints = (formToPoints(fA1) + formToPoints(fA2)) / 2;
      const teamBFormPoints = (formToPoints(fB1) + formToPoints(fB2)) / 2;

      const teamARating = teamACareerElo + teamAFormPoints;
      const teamBRating = teamBCareerElo + teamBFormPoints;
      const teamAWinProb = logisticElo(teamARating, teamBRating);

      const teamASetDiff = (fA1.avgSetDiff + fA2.avgSetDiff) / 2;
      const teamBSetDiff = (fB1.avgSetDiff + fB2.avgSetDiff) / 2;
      const distribution = scoreDistribution(teamAWinProb, teamASetDiff, teamBSetDiff, calibration);

      renderResult({
        teamAWinProb,
        teamARating,
        teamBRating,
        distribution,
      });
      statusEl.textContent = "Prediction ready.";
    } catch (err) {
      predictionResultsEl.innerHTML = `<p class="muted">${esc(err.message || String(err))}</p>`;
      statusEl.textContent = "Prediction failed.";
    }
  }

  function bindSuggestionInputs() {
    const inputs = [fields.a1, fields.a2, fields.b1, fields.b2];
    inputs.forEach((el) => {
      el?.addEventListener("input", () => {
        window.clearTimeout(suggestionTimer);
        suggestionTimer = window.setTimeout(() => {
          refreshSuggestions(el.value);
        }, 180);
      });
    });
  }

  predictFormEl?.addEventListener("submit", runPrediction);
  bindSuggestionInputs();
  if (fields.a1) fields.a1.value = DEFAULT_MATCHUP.a1;
  if (fields.a2) fields.a2.value = DEFAULT_MATCHUP.a2;
  if (fields.b1) fields.b1.value = DEFAULT_MATCHUP.b1;
  if (fields.b2) fields.b2.value = DEFAULT_MATCHUP.b2;
  if (fields.genderPool) fields.genderPool.value = DEFAULT_MATCHUP.genderPool;
  fields.genderPool?.addEventListener("change", () => {
    loadPlayerDirectory(fields.genderPool.value);
  });
  loadPlayerDirectory(fields.genderPool?.value || "0").finally(() => {
    if (predictFormEl) {
      runPrediction({ preventDefault() {} });
    }
  });
})();
