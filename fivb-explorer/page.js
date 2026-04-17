(function () {
  const statusEl = document.getElementById("status");
  const freshnessEl = document.getElementById("freshness");
  const nullRatesEl = document.getElementById("nullRates");
  const countriesEl = document.getElementById("countries");
  const eloEl = document.getElementById("elo");
  const yearsEl = document.getElementById("years");
  const refreshBtn = document.getElementById("refresh");
  const filterTableSearchEl = document.getElementById("filterTableSearch");
  const filterCountryLimitEl = document.getElementById("filterCountryLimit");
  const filterMinYearEl = document.getElementById("filterMinYear");
  const filterMaxYearEl = document.getElementById("filterMaxYear");
  const filterIncludeUnknownYearEl = document.getElementById("filterIncludeUnknownYear");
  const filterNullThresholdEl = document.getElementById("filterNullThreshold");

  let profileData = null;
  let yearBounds = { min: null, max: null };

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

  function toFiniteNumber(v, fallback) {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  }

  function getFilters() {
    return {
      tableSearch: String(filterTableSearchEl?.value || "")
        .trim()
        .toLowerCase(),
      countryLimit: Math.max(1, toFiniteNumber(filterCountryLimitEl?.value, 10)),
      minYear: toFiniteNumber(filterMinYearEl?.value, yearBounds.min),
      maxYear: toFiniteNumber(filterMaxYearEl?.value, yearBounds.max),
      includeUnknownYear: Boolean(filterIncludeUnknownYearEl?.checked),
      nullThreshold: Math.min(100, Math.max(0, toFiniteNumber(filterNullThresholdEl?.value, 10))),
    };
  }

  function getValidYearBounds(rows) {
    const years = (rows || [])
      .map((r) => Number(r.year))
      .filter((y) => Number.isFinite(y) && y > 0);
    if (years.length === 0) return { min: 2000, max: 2030 };
    return { min: Math.min(...years), max: Math.max(...years) };
  }

  function initYearInputs(matchesByYear) {
    yearBounds = getValidYearBounds(matchesByYear);
    if (filterMinYearEl && filterMaxYearEl) {
      filterMinYearEl.min = String(yearBounds.min);
      filterMinYearEl.max = String(yearBounds.max);
      filterMaxYearEl.min = String(yearBounds.min);
      filterMaxYearEl.max = String(yearBounds.max);
      if (!filterMinYearEl.value) filterMinYearEl.value = String(yearBounds.min);
      if (!filterMaxYearEl.value) filterMaxYearEl.value = String(yearBounds.max);
    }
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

  function renderNullRates(rows, nullThreshold) {
    if (!rows || rows.length === 0) {
      nullRatesEl.innerHTML = '<p class="muted">No rows.</p>';
      return;
    }

    const html = rows
      .map((row) => {
        const rates = row.null_pct || {};
        const inner = Object.keys(rates)
          .map((k) => {
            const pct = Number(rates[k]);
            const warning = Number.isFinite(pct) && pct >= nullThreshold;
            return `<tr><td>${esc(k)}</td><td class="${warning ? "warning" : ""}">${esc(
              rates[k]
            )}%</td></tr>`;
          })
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

  function renderFromState() {
    if (!profileData) return;
    const filters = getFilters();

    const freshnessRows = (profileData.freshness || []).filter((r) =>
      String(r.table_name || "")
        .toLowerCase()
        .includes(filters.tableSearch)
    );
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
      freshnessRows
    );

    const nullRows = (profileData.null_rates || []).filter((r) =>
      String(r.table_name || "")
        .toLowerCase()
        .includes(filters.tableSearch)
    );
    renderNullRates(nullRows, filters.nullThreshold);

    const countryRows = (profileData.top_countries || []).slice(0, filters.countryLimit);
    renderTable(
      countriesEl,
      [
        { key: "country_code", label: "Country" },
        { key: "players", label: "Players", format: (v) => fmtNum(v) },
      ],
      countryRows
    );

    renderTable(
      eloEl,
      [
        { key: "bucket", label: "Bucket" },
        { key: "min_elo", label: "Min ELO" },
        { key: "max_elo", label: "Max ELO" },
        { key: "rows", label: "Rows", format: (v) => fmtNum(v) },
      ],
      profileData.elo_buckets
    );

    const minYear = Math.min(filters.minYear, filters.maxYear);
    const maxYear = Math.max(filters.minYear, filters.maxYear);
    const yearRows = (profileData.matches_by_year || []).filter((r) => {
      const y = Number(r.year);
      if (y === -1) return filters.includeUnknownYear;
      if (!Number.isFinite(y)) return false;
      return y >= minYear && y <= maxYear;
    });

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
      yearRows
    );
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

      profileData = data;
      initYearInputs(data.matches_by_year);
      statusEl.textContent = `Last updated: ${new Date(data.generated_at).toLocaleString()}`;
      renderFromState();
    } catch (err) {
      statusEl.textContent = err.message || String(err);
    }
  }

  refreshBtn?.addEventListener("click", loadProfile);
  [
    filterTableSearchEl,
    filterCountryLimitEl,
    filterMinYearEl,
    filterMaxYearEl,
    filterIncludeUnknownYearEl,
    filterNullThresholdEl,
  ].forEach((el) => el?.addEventListener("input", renderFromState));
  loadProfile();
})();
