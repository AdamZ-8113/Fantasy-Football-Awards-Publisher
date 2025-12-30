const SECTION_MAP = {
  league_champion_dna: "Performance & Results",
  paper_tiger: "Performance & Results",
  unluckiest_manager: "Performance & Results",
  juggernaut: "Performance & Results",
  consistent_king: "Performance & Results",
  boom_or_bust: "Performance & Results",
  soul_crushing_loss: "Pain, Chaos & Heartbreak",
  highest_score_loss: "Pain, Chaos & Heartbreak",
  blowout_victim: "Pain, Chaos & Heartbreak",
  schedule_screwed_me: "Pain, Chaos & Heartbreak",
  always_the_bridesmaid: "Pain, Chaos & Heartbreak",
  ride_or_die: "Manager Tendencies",
  fantasy_sicko: "Manager Tendencies",
  waiver_wire_addict: "Manager Tendencies",
  trade_machine: "Manager Tendencies",
  draft_loyalist: "Manager Tendencies",
  commitment_issues: "Manager Tendencies",
  draft_steal: "Draft & Value",
  draft_bust: "Draft & Value",
  reached_and_regretted: "Draft & Value",
  late_round_wizardry: "Draft & Value",
  bench_war_crime: "Start/Sit Decisions",
  set_and_forget: "Start/Sit Decisions",
  overthinker: "Start/Sit Decisions",
  favorite_player: "Player Stats",
  emotional_support: "Player Stats",
  why_dont_he_want_me: "Player Stats",
  peak_week: "Weekly & Seasonal Storylines",
  rock_bottom: "Weekly & Seasonal Storylines",
  mid_season_glow_up: "Weekly & Seasonal Storylines",
  late_season_collapse: "Weekly & Seasonal Storylines",
  looked_better_on_paper: "Fun Awards",
  trust_the_process: "Fun Awards",
  well_get_em_next_year: "Fun Awards",
};

const SECTION_ORDER = [
  "Performance & Results",
  "Pain, Chaos & Heartbreak",
  "Manager Tendencies",
  "Draft & Value",
  "Start/Sit Decisions",
  "Player Stats",
  "Weekly & Seasonal Storylines",
  "Fun Awards",
  "Other",
];

const AWARD_DESCRIPTIONS = {
  league_champion_dna: "Most weeks finishing among the top three regular-season scores.",
  paper_tiger: "Best win-loss record paired with the lowest average points scored.",
  unluckiest_manager: "Most total points scored against their team over the season.",
  juggernaut: "Highest average points per week.",
  consistent_king: "Lowest week-to-week scoring variance.",
  boom_or_bust: "Highest week-to-week scoring variance.",
  soul_crushing_loss: "Closest loss of the season.",
  highest_score_loss: "Highest single-week score that still resulted in a loss.",
  blowout_victim: "Largest margin of defeat.",
  schedule_screwed_me: "Worst swing between actual wins and league-average wins.",
  always_the_bridesmaid: "Lowest average margin of loss (min 3 losses).",
  ride_or_die: "Fewest roster changes across the season.",
  fantasy_sicko: "Highest roster churn across the season.",
  waiver_wire_addict: "Most waiver adds and drops.",
  trade_machine: "Most trades completed.",
  draft_loyalist: "Most drafted players still on the final roster.",
  commitment_issues: "Fewest drafted players still on the final roster.",
  draft_steal: "Biggest positive gap between draft rank and season finish.",
  draft_bust: "Highest-drafted player with minimal season contribution.",
  reached_and_regretted: "Largest draft reach that underperformed.",
  late_round_wizardry: "Best performer drafted in the late rounds.",
  bench_war_crime: "Highest-scoring bench player left out.",
  set_and_forget: "Player started the most weeks by a manager.",
  overthinker: "Games lost due to suboptimal start/sit decisions.",
  favorite_player: "Player rostered by the most unique teams.",
  emotional_support: "Player started most often by a single manager.",
  why_dont_he_want_me: "Most total points scored while on the bench.",
  peak_week: "Highest single-week score league-wide.",
  rock_bottom: "Lowest single-week score league-wide.",
  mid_season_glow_up: "Biggest improvement from first to second half.",
  late_season_collapse: "Largest drop-off after midseason.",
  looked_better_on_paper: "Largest weekly gap between projected and actual score.",
  trust_the_process: "Started slow but still made the playoffs.",
  well_get_em_next_year: "Most points scored while missing the playoffs.",
};

const HIDDEN_INSIGHTS = new Set([
  "league_summary",
  "draft_position_champion",
  "average_playoff_cutoff",
]);

const state = {
  seasons: [],
  leagueBySeason: {},
  leagueKeyBySeason: {},
  seasonByLeagueKey: {},
  summaryBySeason: {},
  overviewBySeason: {},
  finalPlacementsBySeason: {},
  currentSeason: null,
  currentSeasonData: null,
  currentTeamKey: null,
  teamsBySeason: {},
  teamInsightsBySeason: {},
  teamInsightsStatus: {},
  themes: [
    { id: "ember", name: "Ember", swatch: ["#f97316", "#14b8a6"] },
    { id: "glacier", name: "Glacier", swatch: ["#38bdf8", "#a855f7"] },
    { id: "light", name: "Light", swatch: ["#2563eb", "#14b8a6"] },
  ],
};

const els = {
  leaguePill: document.getElementById("league-pill"),
  seasonPill: document.getElementById("season-pill"),
  seasonList: document.getElementById("season-list"),
  teamList: document.getElementById("team-list"),
  teamNote: document.getElementById("team-note"),
  teamViewBadge: document.getElementById("team-view-badge"),
  summarySection: document.getElementById("summary-section"),
  missingItems: document.getElementById("missing-items"),
  insightSections: document.getElementById("insight-sections"),
  summaryBody: document.getElementById("summary-body"),
  overviewGrid: document.getElementById("overview-grid"),
  overviewBrackets: document.getElementById("overview-brackets"),
  themeButtons: document.getElementById("theme-buttons"),
  generatedFootnote: document.getElementById("data-generated"),
};

function prettyLabel(value) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatNumber(value) {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value.toFixed(2);
}

function formatPercent(value, digits = 1) {
  if (value === null || value === undefined) {
    return "—";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function formatTeam(team) {
  if (!team) {
    return "—";
  }
  const name = team.team_name || "Unknown";
  const manager = team.manager_names ? ` (${team.manager_names})` : "";
  return `${name}${manager}`;
}

function formatPlayer(player) {
  if (!player) {
    return "—";
  }
  const name = player.player_name || "Unknown";
  const pos = player.player_position ? ` • ${player.player_position}` : "";
  return `${name}${pos}`;
}

function formatValue(value) {
  if (value === null || value === undefined) {
    return "—";
  }
  if (typeof value === "number") {
    return formatNumber(value);
  }
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(formatValue).join(", ");
  }
  if (typeof value === "object") {
    if (value.team_name || value.manager_names) {
      return formatTeam(value);
    }
    if (value.player_name || value.player_position) {
      return formatPlayer(value);
    }
    return Object.entries(value)
      .map(([key, nested]) => `${prettyLabel(key)}: ${formatValue(nested)}`)
      .join(" • ");
  }
  return String(value);
}

function metricEntries(metric) {
  if (!metric || typeof metric !== "object") {
    return [];
  }
  return Object.entries(metric).map(([key, value]) => {
    const isObject = value && typeof value === "object" && !Array.isArray(value);
    const isTeam = isObject && (value.team_name || value.manager_names);
    return {
      label: prettyLabel(key),
      value: formatValue(value),
      isTeam,
    };
  });
}

function buildBadge(text, variant = "neutral") {
  const badge = document.createElement("span");
  badge.className = `badge badge--${variant}`;
  badge.textContent = text;
  return badge;
}

function renderMissing(missing) {
  if (!missing.length) {
    els.missingItems.textContent = "All awards available.";
    return;
  }
  els.missingItems.textContent = "";
  missing.forEach((item) => {
    const row = document.createElement("div");
    row.textContent = `${prettyLabel(item.id)}: ${item.reason}`;
    els.missingItems.appendChild(row);
  });
}

function renderSummaryTable(rows, season) {
  if (!rows || !rows.length) {
    els.summaryBody.textContent = "";
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 8;
    cell.textContent = "No summary data available.";
    row.appendChild(cell);
    els.summaryBody.appendChild(row);
    return;
  }

  els.summaryBody.textContent = "";
  const placements = state.finalPlacementsBySeason[season] || {};
  rows
    .sort((a, b) => (a.rank || 999) - (b.rank || 999))
    .forEach((team) => {
      const row = document.createElement("tr");

      const rank = document.createElement("td");
      rank.textContent = team.rank ?? "—";

      const teamCell = document.createElement("td");
      const teamWrap = document.createElement("div");
      teamWrap.className = "summary__team";
      const name = document.createElement("div");
      name.className = "summary__team-name";
      name.textContent = team.team_name || "Unknown";
      const manager = document.createElement("div");
      manager.className = "summary__team-manager";
      manager.textContent = team.manager_names || "—";
      teamWrap.appendChild(name);
      teamWrap.appendChild(manager);
      teamCell.appendChild(teamWrap);

      const record = document.createElement("td");
      record.textContent = `${team.wins ?? 0}-${team.losses ?? 0}-${team.ties ?? 0}`;

      const pf = document.createElement("td");
      pf.textContent =
        team.points_for === null || team.points_for === undefined
          ? "—"
          : Number(team.points_for).toFixed(2);

      const pa = document.createElement("td");
      pa.textContent =
        team.points_against === null || team.points_against === undefined
          ? "—"
          : Number(team.points_against).toFixed(2);

      const waiver = document.createElement("td");
      waiver.textContent = team.waiver_moves ?? 0;

      const moves = document.createElement("td");
      moves.textContent = team.total_moves ?? 0;

      const finalPlace = document.createElement("td");
      const placementEntry = placements[team.team_key];
      const finalLabel = placementEntry?.final_label || "—";
      finalPlace.textContent = finalLabel;
      const finalPlaceNumber = placementEntry?.final_place;
      if (finalPlaceNumber === 1) {
        finalPlace.classList.add("final-rank--gold");
      } else if (finalPlaceNumber === 2) {
        finalPlace.classList.add("final-rank--silver");
      } else if (finalPlaceNumber === 3) {
        finalPlace.classList.add("final-rank--bronze");
      }

      row.appendChild(rank);
      row.appendChild(teamCell);
      row.appendChild(record);
      row.appendChild(pf);
      row.appendChild(pa);
      row.appendChild(waiver);
      row.appendChild(moves);
      row.appendChild(finalPlace);
      els.summaryBody.appendChild(row);
    });
}

function createOverviewCard(title, rows, listItems = [], description = "") {
  const card = document.createElement("div");
  card.className = "overview-card";

  const heading = document.createElement("div");
  heading.className = "overview-card__title";
  heading.textContent = title;
  card.appendChild(heading);

  if (description) {
    const desc = document.createElement("div");
    desc.className = "overview-card__description";
    desc.textContent = description;
    card.appendChild(desc);
  }

  const rowsWrap = document.createElement("div");
  rowsWrap.className = "overview-card__rows";

  rows.forEach(({ label, value }) => {
    const row = document.createElement("div");
    row.className = "overview-row";

    const labelEl = document.createElement("div");
    labelEl.className = "overview-row__label";
    labelEl.textContent = label;

    const valueEl = document.createElement("div");
    valueEl.className = "overview-row__value";
    valueEl.textContent = value === null || value === undefined || value === "" ? "—" : value;

    row.appendChild(labelEl);
    row.appendChild(valueEl);
    rowsWrap.appendChild(row);
  });

  card.appendChild(rowsWrap);

  if (listItems.length) {
    const list = document.createElement("div");
    list.className = "overview-list";
    listItems.forEach((item) => {
      const row = document.createElement("div");
      row.className = "overview-list__item";

      const label = document.createElement("strong");
      label.textContent = item.label;

      const value = document.createElement("span");
      value.textContent = item.value;

      row.appendChild(label);
      row.appendChild(value);
      list.appendChild(row);
    });
    card.appendChild(list);
  }

  return card;
}

function createSparkline(values, axisConfig = {}) {
  if (!values.length) {
    return { svg: null, min: null, max: null };
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const pad = Math.max(5, range * 0.15);
  const minBoundRaw = min - pad;
  const maxBoundRaw = max + pad;
  let minBound = Math.floor(minBoundRaw / 10) * 10;
  let maxBound = Math.ceil(maxBoundRaw / 10) * 10;
  if (maxBound <= minBound) {
    maxBound = minBound + 40;
  }
  let step = Math.ceil(((maxBound - minBound) / 4) / 10) * 10;
  if (step < 10) {
    step = 10;
  }
  maxBound = minBound + step * 4;
  while (maxBound < maxBoundRaw) {
    step += 10;
    maxBound = minBound + step * 4;
  }
  const boundedRange = maxBound - minBound || 1;
  const width = 840;
  const height = 200;
  const padding = { left: 56, right: 18, top: 12, bottom: 46 };
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const coords = values.map((value, index) => {
    const x =
      values.length === 1
        ? padding.left + innerWidth / 2
        : padding.left + (index / (values.length - 1)) * innerWidth;
    const y = padding.top + (1 - (value - minBound) / boundedRange) * innerHeight;
    return [x, y];
  });

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.classList.add("overview-sparkline");

  const axisLine = document.createElementNS("http://www.w3.org/2000/svg", "path");
  const axisPathData = [
    `M ${padding.left} ${padding.top}`,
    `L ${padding.left} ${height - padding.bottom}`,
    `L ${width - padding.right} ${height - padding.bottom}`,
  ].join(" ");
  axisLine.setAttribute("d", axisPathData);
  axisLine.classList.add("sparkline-axis");

  const yTickValues = Array.from({ length: 5 }, (_, idx) => maxBound - step * idx);
  yTickValues.forEach((value) => {
    const y = padding.top + (1 - (value - minBound) / boundedRange) * innerHeight;
    const grid = document.createElementNS("http://www.w3.org/2000/svg", "line");
    grid.setAttribute("x1", padding.left);
    grid.setAttribute("x2", width - padding.right);
    grid.setAttribute("y1", y);
    grid.setAttribute("y2", y);
    grid.classList.add("sparkline-grid");
    svg.appendChild(grid);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.textContent = `${value}`;
    label.setAttribute("x", padding.left - 6);
    label.setAttribute("y", y + 4);
    label.setAttribute("text-anchor", "end");
    label.classList.add("sparkline-tick");
    svg.appendChild(label);
  });

  const area = document.createElementNS("http://www.w3.org/2000/svg", "path");
  const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  const pointString = coords.map(([x, y]) => `${x},${y}`).join(" ");

  let areaPath = `M ${coords[0][0]} ${coords[0][1]}`;
  for (let i = 1; i < coords.length; i += 1) {
    areaPath += ` L ${coords[i][0]} ${coords[i][1]}`;
  }
  areaPath += ` L ${coords[coords.length - 1][0]} ${height - padding.bottom}`;
  areaPath += ` L ${coords[0][0]} ${height - padding.bottom} Z`;

  area.setAttribute("d", areaPath);
  area.classList.add("sparkline-area");

  line.setAttribute("points", pointString);

  svg.appendChild(axisLine);
  svg.appendChild(area);
  svg.appendChild(line);

  const yLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
  yLabel.textContent = "Avg Points";
  yLabel.setAttribute("x", 6);
  yLabel.setAttribute("y", height / 2);
  yLabel.setAttribute("text-anchor", "middle");
  yLabel.setAttribute("transform", `rotate(-90 6 ${height / 2})`);
  yLabel.classList.add("sparkline-label");
  svg.appendChild(yLabel);

  const xLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
  xLabel.textContent = "Week";
  xLabel.setAttribute("x", width / 2);
  xLabel.setAttribute("y", height - 6);
  xLabel.setAttribute("text-anchor", "middle");
  xLabel.classList.add("sparkline-label");
  svg.appendChild(xLabel);

  const axisWeeks = axisConfig.weeks || [];
  if (axisWeeks.length === coords.length) {
    axisWeeks.forEach((week, index) => {
      const tick = document.createElementNS("http://www.w3.org/2000/svg", "text");
      tick.textContent = `W${week}`;
      tick.setAttribute("x", coords[index][0]);
      tick.setAttribute("y", height - padding.bottom + 14);
      tick.setAttribute("text-anchor", "middle");
      tick.classList.add("sparkline-tick");
      svg.appendChild(tick);
    });
  } else if (
    axisConfig.startWeek !== undefined &&
    axisConfig.endWeek !== undefined
  ) {
    const start = document.createElementNS("http://www.w3.org/2000/svg", "text");
    start.textContent = `W${axisConfig.startWeek}`;
    start.setAttribute("x", padding.left);
    start.setAttribute("y", height - padding.bottom + 14);
    start.setAttribute("text-anchor", "start");
    start.classList.add("sparkline-tick");
    svg.appendChild(start);

    const end = document.createElementNS("http://www.w3.org/2000/svg", "text");
    end.textContent = `W${axisConfig.endWeek}`;
    end.setAttribute("x", width - padding.right);
    end.setAttribute("y", height - padding.bottom + 14);
    end.setAttribute("text-anchor", "end");
    end.classList.add("sparkline-tick");
    svg.appendChild(end);
  }

  const tooltip = axisConfig.tooltip;
  const tooltipEl = tooltip?.element;
  const tooltipContainer = tooltip?.container;

  coords.forEach((point, index) => {
    const dot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    dot.setAttribute("cx", point[0]);
    dot.setAttribute("cy", point[1]);
    dot.setAttribute("r", 3);
    dot.classList.add("sparkline-point");

    const weekLabel =
      axisWeeks[index] !== undefined ? `Week ${axisWeeks[index]}` : `Week ${index + 1}`;

    if (tooltipEl && tooltipContainer) {
      const valueLabel = formatNumber(values[index]);
      dot.addEventListener("mouseenter", (event) => {
        tooltipEl.textContent = `${weekLabel}: ${valueLabel}`;
        tooltipEl.style.opacity = "1";
        const rect = tooltipContainer.getBoundingClientRect();
        tooltipEl.style.transform = `translate(${event.clientX - rect.left + 12}px, ${event.clientY - rect.top - 12}px)`;
      });
      dot.addEventListener("mousemove", (event) => {
        const rect = tooltipContainer.getBoundingClientRect();
        tooltipEl.style.transform = `translate(${event.clientX - rect.left + 12}px, ${event.clientY - rect.top - 12}px)`;
      });
      dot.addEventListener("mouseleave", () => {
        tooltipEl.style.opacity = "0";
      });
    }

    svg.appendChild(dot);
  });

  return { svg, min, max };
}

function renderOverview(overview, season) {
  els.overviewGrid.textContent = "";
  els.overviewBrackets.textContent = "";
  if (!overview) {
    const empty = createOverviewCard("Season Snapshot", [
      { label: "Total points", value: "—" },
      { label: "Avg weekly points", value: "—" },
      { label: "Avg margin", value: "—" },
    ]);
    els.overviewGrid.appendChild(empty);
    return;
  }
  const placements = state.finalPlacementsBySeason[season] || {};

  const snapshot = overview.snapshot || {};
  els.overviewGrid.appendChild(
    createOverviewCard("Season Snapshot", [
      {
        label: "Total points",
        value: snapshot.total_points === null ? "—" : formatNumber(snapshot.total_points),
      },
      {
        label: "Avg weekly points",
        value:
          snapshot.avg_weekly_points === null
            ? "—"
            : formatNumber(snapshot.avg_weekly_points),
      },
      {
        label: "Avg margin",
        value: snapshot.avg_margin === null ? "—" : formatNumber(snapshot.avg_margin),
      },
      {
        label: "Closest game",
        value:
          snapshot.closest_margin === null ? "—" : formatNumber(snapshot.closest_margin),
      },
      {
        label: "Biggest blowout",
        value:
          snapshot.blowout_margin === null ? "—" : formatNumber(snapshot.blowout_margin),
      },
    ])
  );

  const competitive = overview.competitive_balance || {};
  els.overviewGrid.appendChild(
    createOverviewCard("Competitive Balance", [
      {
        label: "Median margin",
        value:
          competitive.median_margin === null || competitive.median_margin === undefined
            ? "—"
            : formatNumber(competitive.median_margin),
      },
      {
        label: `Close games (<= ${competitive.close_threshold ?? 10})`,
        value:
          competitive.close_games !== null && competitive.close_games !== undefined
            ? `${competitive.close_games} (${formatPercent(competitive.close_game_rate, 0)})`
            : "—",
      },
    ])
  );

  const median = overview.median_record || {};
  const medianLeader = median.leader ? formatTeam(median.leader) : "—";
  const medianGapTeam = median.biggest_gap_team ? formatTeam(median.biggest_gap_team) : "—";
  const gapValue =
    median.biggest_gap === null || median.biggest_gap === undefined
      ? "—"
      : `${median.biggest_gap >= 0 ? "+" : ""}${median.biggest_gap}`;
  const medianScoreLabel =
    median.median_score === null || median.median_score === undefined
      ? "Median score: —"
      : `Median score: ${formatNumber(median.median_score)}`;
  els.overviewGrid.appendChild(
    createOverviewCard(
      "Median Wins (vs League Median)",
      [
        {
          label: "Weeks scored above Median",
          value:
            median.leader_median_wins !== null &&
            median.leader_median_wins !== undefined
              ? `${medianLeader} (${median.leader_median_wins})`
              : "—",
        },
        {
          label: "Games lost while scoring above median",
          value:
            median.biggest_gap !== null && median.biggest_gap !== undefined
              ? `${medianGapTeam} (${gapValue})`
              : "—",
        },
      ],
      [],
      medianScoreLabel
    )
  );

  const upset = overview.upset_rate || {};
  els.overviewGrid.appendChild(
    createOverviewCard("Upset Rate", [
      {
        label: "Upsets",
        value:
          upset.upsets !== null && upset.games
            ? `${upset.upsets}/${upset.games}`
            : "—",
      },
      {
        label: "Rate",
        value: formatPercent(upset.rate),
      },
    ])
  );

  const trendCard = document.createElement("div");
  trendCard.className = "overview-card overview-card--wide";
  const trendTitle = document.createElement("div");
  trendTitle.className = "overview-card__title";
  trendTitle.textContent = "Scoring Trendline";
  trendCard.appendChild(trendTitle);

  const tooltip = document.createElement("div");
  tooltip.className = "sparkline-tooltip";
  tooltip.setAttribute("aria-hidden", "true");
  trendCard.appendChild(tooltip);

  const trendEntries = overview.scoring_trend || [];
  const trendFiltered = trendEntries.filter(
    (entry) => entry.avg_points !== null && entry.avg_points !== undefined
  );
  const trendValues = trendFiltered.map((entry) => entry.avg_points);
  const trendWeeks = trendFiltered.map((entry) => entry.week);
  const startWeek = trendEntries[0]?.week;
  const endWeek = trendEntries[trendEntries.length - 1]?.week;
  const sparkline = createSparkline(trendValues, {
    startWeek,
    endWeek,
    weeks: trendWeeks,
    tooltip: { element: tooltip, container: trendCard },
  });
  if (sparkline.svg) {
    trendCard.appendChild(sparkline.svg);
    const labels = document.createElement("div");
    labels.className = "overview-sparkline__labels";
    const low = document.createElement("span");
    low.textContent = `Low: ${formatNumber(sparkline.min)}`;
    const high = document.createElement("span");
    high.textContent = `High: ${formatNumber(sparkline.max)}`;
    labels.appendChild(low);
    labels.appendChild(high);
    trendCard.appendChild(labels);
  } else {
    const empty = document.createElement("div");
    empty.className = "overview-row__label";
    empty.textContent = "No scoring data available.";
    trendCard.appendChild(empty);
  }
  els.overviewGrid.appendChild(trendCard);

  renderBracketSection(
    overview.playoff_bracket,
    "Playoff Bracket",
    placements
  );
  renderBracketSection(
    overview.consolation_bracket,
    "Consolation Bracket",
    placements
  );
}

function renderBracketSection(bracket, title, placements) {
  const rounds = bracket?.rounds || [];
  if (!rounds.length) {
    return;
  }

  const lastWeekByTeam = {};
  rounds.forEach((round) => {
    const week = Number(round.week);
    round.matchups.forEach((matchup) => {
      matchup.teams.forEach((team) => {
        if (!team.team_key || Number.isNaN(week)) {
          return;
        }
        const previous = lastWeekByTeam[team.team_key];
        if (previous === undefined || week > previous) {
          lastWeekByTeam[team.team_key] = week;
        }
      });
    });
  });

  const section = document.createElement("div");
  section.className = "bracket";

  const heading = document.createElement("div");
  heading.className = "bracket__title";
  heading.textContent = title;
  section.appendChild(heading);

  const roundsWrap = document.createElement("div");
  roundsWrap.className = "bracket__rounds";

  rounds.forEach((round) => {
    const roundWeek = Number(round.week);
    const roundCol = document.createElement("div");
    const roundTitle = document.createElement("div");
    roundTitle.className = "bracket__round-title";
    roundTitle.textContent = `Week ${round.week}`;
    roundCol.appendChild(roundTitle);

    round.matchups.forEach((matchup) => {
      const card = document.createElement("div");
      card.className = "bracket__matchup";

      matchup.teams.forEach((team) => {
        const row = document.createElement("div");
        row.className = "bracket__team";
        if (team.is_winner) {
          row.classList.add("is-winner");
        }

        const info = document.createElement("div");
        const name = document.createElement("div");
        name.className = "bracket__team-name";
        name.textContent = team.team?.team_name || "Unknown";
        const placement = placements?.[team.team_key];
        if (
          placement?.final_label &&
          lastWeekByTeam[team.team_key] === roundWeek
        ) {
          const badge = document.createElement("span");
          badge.className = "bracket__placement";
          badge.textContent = placement.final_label;
          name.appendChild(badge);
        }
        const meta = document.createElement("div");
        meta.className = "bracket__team-meta";
        meta.textContent = team.team?.manager_names || "—";
        info.appendChild(name);
        info.appendChild(meta);

        const score = document.createElement("div");
        score.className = "bracket__team-score";
        score.textContent =
          team.points === null || team.points === undefined
            ? "—"
            : Number(team.points).toFixed(2);

        row.appendChild(info);
        row.appendChild(score);
        card.appendChild(row);
      });

      roundCol.appendChild(card);
    });

    roundsWrap.appendChild(roundCol);
  });

  section.appendChild(roundsWrap);
  els.overviewBrackets.appendChild(section);
}

function renderInsights(insights) {
  els.insightSections.textContent = "";
  const grouped = {};
  insights.forEach((insight) => {
    if (HIDDEN_INSIGHTS.has(insight.id)) {
      return;
    }
    const section = SECTION_MAP[insight.id] || "Other";
    if (!grouped[section]) {
      grouped[section] = [];
    }
    grouped[section].push(insight);
  });

  SECTION_ORDER.forEach((sectionName) => {
    const items = grouped[sectionName];
    if (!items || !items.length) {
      return;
    }
    const section = document.createElement("div");
    section.className = "section";

    const heading = document.createElement("div");
    heading.className = "section__title";
    heading.textContent = sectionName;

    const cards = document.createElement("div");
    cards.className = "cards";

    items.forEach((insight, idx) => {
      const card = document.createElement("div");
      card.className = "card";
      if (insight.id === "league_summary") {
        card.classList.add("card--wide");
      }
      card.style.transitionDelay = `${Math.min(idx * 40, 240)}ms`;

      const title = document.createElement("div");
      title.className = "card__title";

      const titleText = document.createElement("span");
      titleText.textContent = insight.title;
      title.appendChild(titleText);

      const description = document.createElement("div");
      description.className = "card__description";
      description.textContent =
        AWARD_DESCRIPTIONS[insight.id] || "Award description unavailable.";

      const badges = document.createElement("div");
      badges.className = "card__badges";
      if (insight.team) {
        badges.appendChild(buildBadge(formatTeam(insight.team), "warm"));
      }
      if (insight.player) {
        badges.appendChild(buildBadge(formatPlayer(insight.player), "neutral"));
      }
      if (insight.players && insight.players.length) {
        const stackNames = insight.players.map(formatPlayer).join(" + ");
        badges.appendChild(buildBadge(stackNames, "neutral"));
      }
      if (insight.teams && insight.teams.length) {
        insight.teams.forEach((team) => {
          const label = team.team_name
            ? `${team.team_name}${team.manager_names ? ` (${team.manager_names})` : ""}`
            : "Unknown";
          badges.appendChild(buildBadge(label, "warm"));
        });
      }

      const metric = document.createElement("div");
      metric.className = "metric";
      metricEntries(insight.metric).forEach((entry) => {
        const row = document.createElement("div");
        row.className = "metric__row";

        const label = document.createElement("div");
        label.className = "metric__label";
        label.textContent = entry.label;

      const value = document.createElement("div");
      value.className = "metric__value";
      if (entry.isTeam) {
        value.classList.add("metric__value--team");
      }
      value.textContent = entry.value;

        row.appendChild(label);
        row.appendChild(value);
        metric.appendChild(row);
      });

      card.appendChild(title);
      card.appendChild(description);
      if (badges.childNodes.length) {
        card.appendChild(badges);
      }
      card.appendChild(metric);
      cards.appendChild(card);
    });

    section.appendChild(heading);
    section.appendChild(cards);
    els.insightSections.appendChild(section);
  });

  revealCards();
}

function revealCards() {
  const cards = document.querySelectorAll(".card");
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("in-view");
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.2 }
  );
  cards.forEach((card) => observer.observe(card));
}

function renderSeasonButtons() {
  els.seasonList.textContent = "";
  state.seasons.forEach((season) => {
    const button = document.createElement("button");
    button.className = "season-button";
    button.textContent = season;
    if (season === state.currentSeason) {
      button.classList.add("is-active");
    }
    button.addEventListener("click", () => {
      if (season === state.currentSeason) {
        return;
      }
      loadSeason(season);
    });
    els.seasonList.appendChild(button);
  });
}

function teamLabel(team) {
  if (!team) {
    return "Unknown";
  }
  const name = team.team_name || team.name || "Unknown";
  const manager = team.manager_names ? ` (${team.manager_names})` : "";
  return `${name}${manager}`;
}

function getTeamsForSeason(season) {
  const teamInsights = state.teamInsightsBySeason[season];
  if (teamInsights?.teams?.length) {
    return teamInsights.teams;
  }
  return state.teamsBySeason[season] || [];
}

function updateTeamNote(season, teams, insightsAvailable) {
  if (!els.teamNote) {
    return;
  }
  if (!teams.length) {
    els.teamNote.textContent = "No team list available for this season.";
    return;
  }
  if (!insightsAvailable) {
    els.teamNote.textContent = "Team insights are available for 2024 only right now.";
    return;
  }
  if (state.currentTeamKey) {
    const teamEntry =
      state.teamInsightsBySeason[season]?.teamsByKey?.[state.currentTeamKey] ||
      teams.find((team) => team.team_key === state.currentTeamKey);
    const label = teamEntry ? teamLabel(teamEntry) : "Selected team";
    els.teamNote.textContent = `Showing ${label} insights. Click All Teams to return to league-wide awards.`;
    return;
  }
  els.teamNote.textContent =
    "Pick a team to view manager-specific insights, or stay on All Teams for league-wide awards.";
}

function renderTeamButtons(season) {
  if (!els.teamList) {
    return;
  }
  const teams = getTeamsForSeason(season).slice();
  teams.sort((a, b) => (a.team_name || a.name || "").localeCompare(b.team_name || b.name || ""));
  const insightsAvailable = Boolean(state.teamInsightsBySeason[season]);

  els.teamList.textContent = "";
  const allButton = document.createElement("button");
  allButton.className = "team-button";
  allButton.textContent = "All Teams";
  if (!state.currentTeamKey) {
    allButton.classList.add("is-active");
  }
  allButton.addEventListener("click", () => {
    setTeamSelection(null);
  });
  els.teamList.appendChild(allButton);

  teams.forEach((team) => {
    const button = document.createElement("button");
    button.className = "team-button";
    button.textContent = teamLabel(team);
    if (state.currentTeamKey === team.team_key) {
      button.classList.add("is-active");
    }
    if (!insightsAvailable) {
      button.classList.add("is-disabled");
      button.disabled = true;
    } else {
      button.addEventListener("click", () => {
        setTeamSelection(team.team_key);
      });
    }
    els.teamList.appendChild(button);
  });

  updateTeamNote(season, teams, insightsAvailable);
  if (els.teamViewBadge && !state.currentTeamKey) {
    els.teamViewBadge.hidden = true;
  }
}

async function ensureTeamInsights(season) {
  const status = state.teamInsightsStatus[season];
  if (status === "loaded") {
    return state.teamInsightsBySeason[season];
  }
  if (status === "missing" || status === "loading") {
    return null;
  }

  state.teamInsightsStatus[season] = "loading";
  try {
    const response = await fetch(`data/insights_${season}_teams.json`);
    if (!response.ok) {
      throw new Error("Team insights not found.");
    }
    const data = await response.json();
    const teamsByKey = {};
    (data.teams || []).forEach((team) => {
      teamsByKey[team.team_key] = team;
    });
    state.teamInsightsBySeason[season] = {
      data,
      teams: data.teams || [],
      teamsByKey,
    };
    state.teamInsightsStatus[season] = "loaded";
    return state.teamInsightsBySeason[season];
  } catch (error) {
    state.teamInsightsBySeason[season] = null;
    state.teamInsightsStatus[season] = "missing";
    return null;
  }
}

function applyInsightsForSeason(season) {
  const seasonData = state.currentSeasonData;
  const teamKey = state.currentTeamKey;
  const teamData = teamKey
    ? state.teamInsightsBySeason[season]?.teamsByKey?.[teamKey]
    : null;

  if (teamData) {
    if (els.summarySection) {
      els.summarySection.hidden = true;
    }
    if (els.teamViewBadge) {
      els.teamViewBadge.textContent = `Team View • ${teamLabel(teamData)}`;
      els.teamViewBadge.hidden = false;
    }
    renderMissing(teamData.missing || []);
    renderInsights(teamData.insights || []);
    return;
  }

  if (els.summarySection) {
    els.summarySection.hidden = false;
  }
  if (els.teamViewBadge) {
    els.teamViewBadge.hidden = true;
  }
  renderMissing(seasonData?.missing || []);
  renderInsights(seasonData?.insights || []);
}

function setTeamSelection(teamKey) {
  const season = state.currentSeason;
  const teamInsights = state.teamInsightsBySeason[season];
  if (teamKey && !teamInsights?.teamsByKey?.[teamKey]) {
    return;
  }
  state.currentTeamKey = teamKey;
  renderTeamButtons(season);
  applyInsightsForSeason(season);
}

function renderThemeButtons() {
  els.themeButtons.textContent = "";
  state.themes.forEach((theme) => {
    const button = document.createElement("button");
    button.className = "theme-button";
    button.type = "button";
    button.dataset.theme = theme.id;

    const swatch = document.createElement("span");
    swatch.className = "theme-swatch";
    swatch.style.background = `linear-gradient(120deg, ${theme.swatch[0]}, ${theme.swatch[1]})`;

    const label = document.createElement("span");
    label.textContent = theme.name;

    button.appendChild(swatch);
    button.appendChild(label);

    button.addEventListener("click", () => {
      setTheme(theme.id);
    });

    if (document.body.dataset.theme === theme.id) {
      button.classList.add("is-active");
    }

    els.themeButtons.appendChild(button);
  });
}

function setTheme(themeId) {
  document.body.dataset.theme = themeId;
  window.localStorage.setItem("fi_theme", themeId);
  renderThemeButtons();
}

function updateHero(seasonData) {
  els.seasonPill.textContent = `Season ${seasonData.season}`;
  const leagueName = state.leagueBySeason[seasonData.season] || "League";
  els.leaguePill.textContent = leagueName;
  if (els.generatedFootnote) {
    els.generatedFootnote.textContent = `Data generated: ${seasonData.generated_at}`;
  }
}

async function loadSeason(season) {
  const response = await fetch(`data/insights_${season}.json`);
  const data = await response.json();
  state.currentSeason = season;
  state.currentSeasonData = data;
  state.currentTeamKey = null;
  renderSeasonButtons();
  updateHero(data);
  renderSummaryTable(state.summaryBySeason[season] || [], season);
  renderOverview(state.overviewBySeason[season], season);
  await ensureTeamInsights(season);
  renderTeamButtons(season);
  applyInsightsForSeason(season);
  const params = new URLSearchParams(window.location.search);
  params.set("season", season);
  window.history.replaceState({}, "", `${window.location.pathname}?${params}`);
}

async function init() {
  const savedTheme = window.localStorage.getItem("fi_theme");
  const defaultTheme = state.themes[0]?.id;
  setTheme(savedTheme || defaultTheme);
  renderThemeButtons();

  const indexRes = await fetch("data/insights_index.json");
  const index = await indexRes.json();
  state.seasons = [...(index.seasons || [])].sort(
    (a, b) => Number(a) - Number(b)
  );

  const leaguesRes = await fetch("data/leagues.json");
  const leagues = await leaguesRes.json();
  leagues.forEach((league) => {
    if (league.season) {
      state.leagueBySeason[league.season] = league.name;
      state.leagueKeyBySeason[league.season] = league.league_key;
      state.seasonByLeagueKey[league.league_key] = league.season;
    }
  });

  const teamsRes = await fetch("data/teams.json");
  const teams = await teamsRes.json();
  teams.forEach((team) => {
    const season = state.seasonByLeagueKey[team.league_key];
    if (!season) {
      return;
    }
    if (!state.teamsBySeason[season]) {
      state.teamsBySeason[season] = [];
    }
    state.teamsBySeason[season].push({
      team_key: team.team_key,
      team_name: team.name || team.team_name || "Unknown",
      manager_names: team.manager_names,
    });
  });

  const summaryRes = await fetch("data/league_summary.json");
  const summary = await summaryRes.json();
  summary.forEach((row) => {
    if (!row.season) {
      return;
    }
    if (!state.summaryBySeason[row.season]) {
      state.summaryBySeason[row.season] = [];
    }
    state.summaryBySeason[row.season].push(row);
  });

  const overviewRes = await fetch("data/league_overview.json");
  const overview = await overviewRes.json();
  overview.forEach((row) => {
    if (!row.season) {
      return;
    }
    state.overviewBySeason[row.season] = row;
    const placements = {};
    (row.final_placements || []).forEach((entry) => {
      placements[entry.team_key] = entry;
    });
    state.finalPlacementsBySeason[row.season] = placements;
  });

  const params = new URLSearchParams(window.location.search);
  const requested = params.get("season");
  const fallback = state.seasons[state.seasons.length - 1];
  const selected = state.seasons.includes(requested) ? requested : fallback;
  await loadSeason(selected);
}

init();
