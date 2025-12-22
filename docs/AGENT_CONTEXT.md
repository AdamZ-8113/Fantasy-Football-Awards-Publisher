# Fantasy Insights Agent Context

This document is a condensed onboarding guide for LLMs and new contributors. It captures project intent, architecture, commands, and how to safely iterate.

## Purpose
Fantasy Insights pulls Yahoo Fantasy Football data across seasons, computes awards/insights, and publishes a static “Wrapped” site. The data pipeline is offline and produces JSON for a static frontend in `site/`.

## High-level flow
1) Authenticate to Yahoo (OAuth 2.0 preferred).
2) Discover leagues and sync raw XML + normalized SQLite tables.
3) Backfill missing tables (drafts, modifiers, injuries, player stats).
4) Export site JSON datasets.
5) Generate league and team insights.
6) Publish `site/` to GitHub Pages.

## Key files and folders
- `scripts/`: ingestion, backfill, export, insights.
- `data/raw/`: raw Yahoo API responses (XML).
- `data/processed/`: normalized SQLite + processed JSON (ignored by git).
- `config/`: non-secret config. OAuth tokens stored at `config/oauth_tokens.json` (ignored).
- `site/`: static site (HTML/CSS/JS + data JSON).
- `docs/`: additional documentation.

## Required config
`.env` (never commit):
```
YAHOO_CONSUMER_KEY=...
YAHOO_CONSUMER_SECRET=...
YAHOO_OAUTH_REDIRECT_URI=https://localhost/callback
YAHOO_OAUTH_SCOPE=fspt-r
```

`config/config.toml` (safe to commit):
```
game_key = "nfl"
season_start = 2013
season_end = 2025
league_filter_mode = "filtered"
league_name_hint = ""
league_id_hint = ""
```

## OAuth bootstrap
Recommended (OAuth 2.0):
```
python scripts/oauth2_bootstrap.py
```
Fallback (OAuth 1.0a):
```
python scripts/oauth_bootstrap.py
```
Tokens are written to `config/oauth_tokens.json` (ignored by git).

## Core commands (standard pipeline)
```
python scripts/discover_leagues.py
python scripts/sync_all.py
python scripts/backfill_draft_results.py
python scripts/backfill_stat_modifiers.py
python scripts/backfill_roster_injuries.py
python scripts/backfill_player_stats.py
python scripts/export_site_data.py
python scripts/export_injury_reports.py
python scripts/generate_insights.py
python scripts/generate_team_insights.py
```

## Single-season workflow
Option A: limit by config (`season_start` and `season_end`) then run `discover_leagues` and `sync_all`.
Option B: sync a single league key:
```
python scripts/sync_all.py --only <league_key>
python scripts/generate_insights.py --season <year>
python scripts/generate_team_insights.py --season <year>
```

## Frontend notes
- `site/index.html`, `site/styles.css`, `site/app.js`.
- Data consumed from `site/data/*.json`.
- Team-specific view: team picker toggles team insights; when active, League Overview is hidden.

## Deployment (GitHub Pages)
Static site lives in `site/`, published via `gh-pages` branch using `git subtree`.
```
git subtree push --prefix site origin gh-pages
```

## Security constraints
- `.env`, `config/oauth_tokens.json`, SQLite DBs, and processed data are ignored by git.
- Never paste OAuth secrets into commits or docs.

## Common pitfalls
- Redirect URI must match Yahoo app settings exactly.
- If OAuth 1.0a returns 401, use OAuth 2.0 flow.
- After regenerating data, always rerun `export_site_data.py` and the insight generators.
