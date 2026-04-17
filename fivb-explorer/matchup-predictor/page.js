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
    recentWindow: document.getElementById("recentWindow"),
  };

  const playerCache = new Map();
  const suggestionCache = new Map();
  let calibrationPromise = null;
  let suggestionTimer = 0;

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

  function scoreDistribution(pWin, teamASetDiff, teamBSetDiff, calibration) {
    const dominance = Math.abs(pWin - 0.5) * 2;
    const setEdge = clamp((teamASetDiff - teamBSetDiff) * 0.08, -0.18, 0.18);
    const straightAIfWin = clamp(0.34 + dominance * 0.26 + setEdge, 0.18, 0.78);
    const straightBIfWin = clamp(0.34 + dominance * 0.26 - setEdge, 0.18, 0.78);

    const a20 = pWin * straightAIfWin;
    const a21 = pWin * (1 - straightAIfWin);
    const b20 = (1 - pWin) * straightBIfWin;
    const b21 = (1 - pWin) * (1 - straightBIfWin);

    const overtimeShare = clamp(0.08 + (1 - dominance) * 0.2, 0.08, 0.24);
    const regularShare = 1 - overtimeShare;
    const cal = calibration || fallbackCalibration();

    const strongBias = clamp(0.2 - (pWin - 0.5) * 0.7, 0.05, 0.55);
    const mediumBias = clamp(0.36 - (pWin - 0.5) * 0.45, 0.1, 0.7);
    const closeBias = clamp(0.7 - (pWin - 0.5) * 0.25, 0.3, 0.94);

    const revStrongBias = clamp(0.2 - ((1 - pWin) - 0.5) * 0.7, 0.05, 0.55);
    const revMediumBias = clamp(0.36 - ((1 - pWin) - 0.5) * 0.45, 0.1, 0.7);
    const revCloseBias = clamp(0.7 - ((1 - pWin) - 0.5) * 0.25, 0.3, 0.94);

    const aOppStrong = pickLoserPoints(cal.set21.regular, strongBias);
    const aOppMedium = pickLoserPoints(cal.set21.regular, mediumBias);
    const aOppClose = pickLoserPoints(cal.set21.regular, closeBias);
    const bOppStrong = pickLoserPoints(cal.set21.regular, revStrongBias);
    const bOppMedium = pickLoserPoints(cal.set21.regular, revMediumBias);
    const bOppClose = pickLoserPoints(cal.set21.regular, revCloseBias);

    const aOverLose = pickLoserPoints(cal.set21.overtime, closeBias);
    const bOverLose = pickLoserPoints(cal.set21.overtime, revCloseBias);
    const deciderA = pickLoserPoints(cal.set15.regular, closeBias);
    const deciderB = pickLoserPoints(cal.set15.regular, revCloseBias);
    const deciderAOver = pickLoserPoints(cal.set15.overtime, closeBias);
    const deciderBOver = pickLoserPoints(cal.set15.overtime, revCloseBias);

    const rows = [
      {
        score: `${makeSetScore(21, aOppStrong, false)}, ${makeSetScore(21, aOppMedium, false)}`,
        prob: a20 * regularShare * 0.6,
      },
      {
        score: `${makeSetScore(21, aOppClose, false)}, ${makeSetScore(21, aOppMedium, false)}`,
        prob: a20 * regularShare * 0.4,
      },
      {
        score: `${makeSetScore(21, aOverLose, true)}, ${makeSetScore(21, aOverLose, true)} (overtime win Team A)`,
        prob: a20 * overtimeShare,
      },

      {
        score: `${makeSetScore(21, aOppClose, false)}, ${aOppClose}-21, ${makeSetScore(15, deciderA, false)}`,
        prob: a21 * regularShare * 0.55,
      },
      {
        score: `${aOppClose}-21, ${makeSetScore(21, aOppClose, false)}, ${makeSetScore(15, deciderA, false)}`,
        prob: a21 * regularShare * 0.45,
      },
      {
        score: `${makeSetScore(21, aOverLose, true)}, ${aOppClose}-21, ${makeSetScore(15, deciderAOver, true)} (overtime win Team A)`,
        prob: a21 * overtimeShare,
      },

      {
        score: `${flipScore(makeSetScore(21, bOppClose, false))}, ${makeSetScore(21, bOppClose, false)}, ${flipScore(
          makeSetScore(15, deciderB, false)
        )}`,
        prob: b21 * regularShare * 0.45,
      },
      {
        score: `${makeSetScore(21, bOppClose, false)}, ${flipScore(makeSetScore(21, bOppClose, false))}, ${flipScore(
          makeSetScore(15, deciderB, false)
        )}`,
        prob: b21 * regularShare * 0.55,
      },
      {
        score: `${flipScore(makeSetScore(21, bOverLose, true))}, ${makeSetScore(
          21,
          bOppClose,
          false
        )}, ${flipScore(makeSetScore(15, deciderBOver, true))} (overtime win Team B)`,
        prob: b21 * overtimeShare,
      },

      {
        score: `${bOppMedium}-21, ${bOppStrong}-21`,
        prob: b20 * regularShare * 0.6,
      },
      {
        score: `${bOppMedium}-21, ${bOppClose}-21`,
        prob: b20 * regularShare * 0.4,
      },
      {
        score: `${flipScore(makeSetScore(21, bOverLose, true))}, ${flipScore(
          makeSetScore(21, bOverLose, true)
        )} (overtime win Team B)`,
        prob: b20 * overtimeShare,
      },
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

  async function resolvePlayer(rawInput) {
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
      const search = await callExplorer("search_players", { name: parsed.name, limit: 1 });
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
    if (suggestionCache.has(q)) {
      renderSuggestions(suggestionCache.get(q));
      return;
    }
    try {
      const out = await callExplorer("search_players", { name: q, limit: 8 });
      suggestionCache.set(q, out.rows || []);
      renderSuggestions(out.rows || []);
    } catch (_) {
      // Keep silent to avoid status flicker while typing.
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

  function renderResult(model) {
    const distRows = model.distribution
      .map((row) => `<tr><td>${esc(row.score)}</td><td>${pct(row.prob)}</td></tr>`)
      .join("");

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
      <table>
        <thead>
          <tr><th>Likely final score</th><th>Probability</th></tr>
        </thead>
        <tbody>${distRows}</tbody>
      </table>
    `;
  }

  async function runPrediction(event) {
    event.preventDefault();
    statusEl.textContent = "Loading players and computing prediction...";
    predictionResultsEl.innerHTML = '<p class="muted">Running model...</p>';

    try {
      const calibration = await getCalibration();
      const recentWindow = clamp(toNum(fields.recentWindow.value, 12), 3, 40);
      const pA1 = await resolvePlayer(fields.a1.value);
      const pA2 = await resolvePlayer(fields.a2.value);
      const pB1 = await resolvePlayer(fields.b1.value);
      const pB2 = await resolvePlayer(fields.b2.value);

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
})();
