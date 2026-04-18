(function () {
  function displaySnake(str) {
    const s = String(str ?? "");
    if (!s) return s;
    if (!/[a-z][A-Z]/.test(s)) return s;
    return s
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .replace(/([A-Z])([A-Z][a-z])/g, "$1_$2")
      .toLowerCase();
  }

  const statusEl = document.getElementById("status");
  const freshnessEl = document.getElementById("freshness");
  const nullRatesEl = document.getElementById("nullRates");
  const countriesEl = document.getElementById("countries");
  const eloEl = document.getElementById("elo");
  const yearsEl = document.getElementById("years");
  const refreshBtn = document.getElementById("refresh");

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

  function renderNullRates(rows) {
    if (!rows || rows.length === 0) {
      nullRatesEl.innerHTML = '<p class="muted">No rows.</p>';
      return;
    }

    const html = rows
      .map((row) => {
        const rates = row.null_pct || {};
        const inner = Object.keys(rates)
          .map((k) => `<tr><td>${esc(displaySnake(k))}</td><td>${esc(rates[k])}%</td></tr>`)
          .join("");
        return `
          <div class="card" style="margin-top:0.8rem">
            <h3 style="margin:0 0 0.55rem;font-size:0.95rem;">${esc(row.table_name)} (${fmtNum(
          row.total_rows
        )} rows)</h3>
            <table><tbody>${inner}</tbody></table>
          </div>
        `;
      })
      .join("");
    nullRatesEl.innerHTML = html;
  }

  async function loadProfile() {
    statusEl.textContent = "Loading profile...";
    try {
      const res = await fetch("/api/fivb-profile", {
        method: "GET",
        credentials: "same-origin",
      });
      const data = await res.json();

      if (!res.ok || !data.ok) {
        const msg = data.reason || data.error || "Profile request failed.";
        statusEl.textContent = msg;
        return;
      }

      statusEl.textContent = `Last updated: ${new Date(data.generated_at).toLocaleString()}`;

      renderTable(
        freshnessEl,
        [
          { key: "table_name", label: "Table" },
          { key: "rows", label: "Rows", format: (v) => fmtNum(v) },
          {
            key: "latest_ingested_at",
            label: "Latest Ingested",
            format: (v) => esc(v ? new Date(v).toLocaleString() : "null"),
          },
        ],
        data.freshness
      );

      renderNullRates(data.null_rates);

      renderTable(
        countriesEl,
        [
          { key: "country_code", label: "Country" },
          { key: "players", label: "Players", format: (v) => fmtNum(v) },
        ],
        data.top_countries
      );

      renderTable(
        eloEl,
        [
          { key: "bucket", label: "Bucket" },
          { key: "min_elo", label: "Min ELO" },
          { key: "max_elo", label: "Max ELO" },
          { key: "rows", label: "Rows", format: (v) => fmtNum(v) },
        ],
        data.elo_buckets
      );

      renderTable(
        yearsEl,
        [
          {
            key: "year",
            label: "Year",
            format: (v) => (Number(v) === -1 ? "Unknown" : esc(v)),
          },
          { key: "matches", label: "Matches", format: (v) => fmtNum(v) },
        ],
        data.matches_by_year
      );
    } catch (err) {
      statusEl.textContent = err.message || String(err);
    }
  }

  refreshBtn?.addEventListener("click", loadProfile);
  loadProfile();
})();
