(function () {
  const statusEl = document.getElementById("status");
  const playerSearchForm = document.getElementById("playerSearchForm");
  const historyForm = document.getElementById("historyForm");
  const playersEl = document.getElementById("players");
  const playerProfileEl = document.getElementById("playerProfile");
  const matchesEl = document.getElementById("matches");
  const eloEl = document.getElementById("elo");

  const nameEl = document.getElementById("name");
  const countryEl = document.getElementById("country");
  const genderEl = document.getElementById("gender");
  const limitEl = document.getElementById("limit");

  const historyPlayerIdEl = document.getElementById("historyPlayerId");
  const historyPlayerNameEl = document.getElementById("historyPlayerName");
  const historyLimitEl = document.getElementById("historyLimit");

  function esc(v) {
    return String(v ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function fmtNum(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return esc(v);
    return n.toLocaleString("en-US");
  }

  function fmtGender(v) {
    if (String(v) === "0") return "Male";
    if (String(v) === "1") return "Female";
    return esc(v);
  }

  function toMaybeNumber(v) {
    if (v === null || v === undefined || String(v).trim() === "") return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  }

  function renderTable(container, columns, rows) {
    if (!rows || rows.length === 0) {
      container.innerHTML = '<p class="muted">No rows.</p>';
      return;
    }
    const head = columns.map((c) => `<th>${esc(c.label)}</th>`).join("");
    const body = rows
      .map((row) => {
        const tds = columns
          .map((c) => {
            const value = c.format ? c.format(row[c.key], row) : esc(row[c.key]);
            return `<td>${value}</td>`;
          })
          .join("");
        return `<tr>${tds}</tr>`;
      })
      .join("");
    container.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
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

  async function searchPlayers(e) {
    e?.preventDefault();
    statusEl.textContent = "Searching players...";
    try {
      const out = await callExplorer("search_players", {
        name: nameEl.value,
        country_code: countryEl.value,
        gender: genderEl.value,
        limit: toMaybeNumber(limitEl.value) || 50,
      });

      renderTable(
        playersEl,
        [
          { key: "player_id", label: "Player ID", format: (v) => fmtNum(v) },
          { key: "full_name", label: "Name" },
          { key: "country_code", label: "Country" },
          { key: "gender", label: "Gender", format: (v) => fmtGender(v) },
          { key: "height_cm", label: "Height (cm)", format: (v) => fmtNum(v) },
          { key: "weight_kg_est", label: "Weight (kg est)", format: (v) => fmtNum(v) },
          {
            key: "player_id",
            label: "Action",
            format: (v) =>
              `<button type="button" class="load-history" data-player-id="${esc(v)}">Load History</button>`,
          },
        ],
        out.rows
      );

      playersEl.querySelectorAll(".load-history").forEach((btn) => {
        btn.addEventListener("click", () => {
          historyPlayerIdEl.value = btn.getAttribute("data-player-id") || "";
          historyPlayerNameEl.value = "";
          loadHistory();
        });
      });

      statusEl.textContent = `Found ${out.rows.length} players.`;
    } catch (err) {
      statusEl.textContent = err.message || String(err);
    }
  }

  async function loadHistory(e) {
    e?.preventDefault();
    statusEl.textContent = "Loading player history...";
    try {
      const out = await callExplorer("player_history", {
        player_id: toMaybeNumber(historyPlayerIdEl.value),
        player_name: historyPlayerNameEl.value,
        history_limit: toMaybeNumber(historyLimitEl.value) || 250,
      });

      const p = out.profile;
      if (!p) {
        playerProfileEl.innerHTML = '<p class="muted">Player not found.</p>';
      } else {
        playerProfileEl.innerHTML = `
          <p>
            <strong>${esc(p.full_name)}</strong>
            (ID ${fmtNum(p.player_id)}) - ${esc(p.country_code || "UNK")} - gender ${fmtGender(p.gender)} -
            height ${fmtNum(p.height_cm)} cm - weight ${fmtNum(p.weight_kg_est)} kg
          </p>
        `;
      }

      renderTable(
        matchesEl,
        [
          { key: "match_id", label: "Match ID", format: (v) => fmtNum(v) },
          { key: "local_date", label: "Local Date" },
          { key: "tournament_name", label: "Tournament" },
          { key: "round_name", label: "Round" },
          { key: "team_a_name", label: "Team A" },
          { key: "team_b_name", label: "Team B" },
          { key: "score_sets", label: "Score" },
        ],
        out.matches
      );

      renderTable(
        eloEl,
        [
          { key: "as_of_date", label: "As Of" },
          { key: "match_id", label: "Match ID", format: (v) => fmtNum(v) },
          { key: "elo_rating", label: "ELO" },
          { key: "gender", label: "Gender", format: (v) => fmtGender(v) },
        ],
        out.elo_history
      );

      statusEl.textContent = `Loaded ${out.matches.length} matches and ${out.elo_history.length} ELO rows.`;
    } catch (err) {
      statusEl.textContent = err.message || String(err);
    }
  }

  playerSearchForm?.addEventListener("submit", searchPlayers);
  historyForm?.addEventListener("submit", loadHistory);
  searchPlayers();
})();
