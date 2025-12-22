# Fantasy Insights

Fantasy Insights pulls multi-season Yahoo Fantasy Football data, calculates end-of-season awards, and publishes a static "Wrapped" style site.

## What this does
- Authenticates to the Yahoo Fantasy Sports API.
- Downloads league data (matchups, rosters, transactions, draft results, stats).
- Stores raw XML snapshots plus normalized SQLite tables.
- Generates season and team-specific insights JSON for the static site.
- Publishes everything in `site/` for GitHub Pages.

## Tech stack
- Python 3.11+ for ingestion, normalization, and analysis
- OAuth 2.0 (recommended) or OAuth 1.0a via `requests-oauthlib`
- XML parsing via `lxml`
- Storage in SQLite (raw + normalized tables)
- Static site in `site/` fed by precomputed JSON

## Repo layout
- `scripts/`: data ingestion and analysis scripts
- `data/raw/`: raw API responses (XML snapshots)
- `data/processed/`: normalized tables and aggregates
- `config/`: non-secret config files
- `site/`: static webpage assets and data
- `docs/`: setup, data model, and deployment docs

## Quick start
1) Create and activate a Python virtual environment.
   - `python -m venv .venv`
   - `./.venv/Scripts/Activate.ps1`
2) Install dependencies.
   - `pip install -r requirements.txt`
3) Copy config and environment templates.
   - `copy .env.example .env`
   - `copy config\config.example.toml config\config.toml`
4) Configure your Yahoo app and OAuth (see `docs/oauth_setup.md`).
5) Run the OAuth bootstrap:
   - Recommended: `python scripts/oauth2_bootstrap.py`
   - Fallback: `python scripts/oauth_bootstrap.py`

## Configuration
Environment variables in `.env`:
- `YAHOO_CONSUMER_KEY`: Yahoo app Client ID (consumer key)
- `YAHOO_CONSUMER_SECRET`: Yahoo app Client Secret
- `YAHOO_OAUTH_REDIRECT_URI`: Must match the Yahoo app Redirect URI
- `YAHOO_OAUTH_SCOPE`: Defaults to `fspt-r` (Fantasy Sports read)

Non-secret settings in `config/config.toml`:
- `game_key`: Typically `nfl`
- `season_start` / `season_end`: Season range to include
- `league_name_hint` / `league_id_hint`: Optional filters for league discovery
- `league_filter_mode`: `filtered` or `all`

## Data pipeline (standard run)
1) Discover leagues:
   - `python scripts/discover_leagues.py`
2) Sync league data into SQLite + raw XML:
   - `python scripts/sync_all.py`
3) Optional backfills:
   - `python scripts/backfill_draft_results.py`
   - `python scripts/backfill_stat_modifiers.py`
   - `python scripts/backfill_roster_injuries.py`
   - `python scripts/backfill_player_stats.py`
4) Export site datasets:
   - `python scripts/export_site_data.py`
   - `python scripts/export_injury_reports.py`
5) Generate awards:
   - `python scripts/generate_insights.py`
   - `python scripts/generate_team_insights.py`

## Script reference
- `scripts/oauth2_bootstrap.py`: OAuth 2.0 flow, writes `config/oauth_tokens.json`
- `scripts/oauth_bootstrap.py`: OAuth 1.0a flow (fallback)
- `scripts/discover_leagues.py`: Finds league keys across seasons
- `scripts/sync_all.py`: Pulls league data into SQLite and raw XML
- `scripts/backfill_draft_results.py`: Loads draft results into SQLite
- `scripts/backfill_stat_modifiers.py`: Loads scoring modifiers
- `scripts/backfill_team_stats.py`: Rebuilds team stats from raw XML
- `scripts/backfill_roster_injuries.py`: Backfills injury statuses
- `scripts/backfill_player_stats.py`: Backfills player stats from roster weeks
- `scripts/validate_counts.py`: Summarizes per-league row counts
- `scripts/export_site_data.py`: Builds `site/data/*` JSON
- `scripts/export_injury_reports.py`: Builds injury reports JSON
- `scripts/generate_insights.py`: Season-level awards JSON
- `scripts/generate_team_insights.py`: Team-specific awards JSON

## Generate data for a single season
Option A: limit discovery and sync via config.
1) Edit `config/config.toml`:
   - `season_start = 2024`
   - `season_end = 2024`
2) Run `python scripts/discover_leagues.py`
3) Run `python scripts/sync_all.py`

Option B: sync a known league key only.
1) Find the league key in `data/processed/leagues.json`.
2) Run `python scripts/sync_all.py --only <league_key>`

Insights for one season:
- `python scripts/generate_insights.py --season 2024`
- `python scripts/generate_team_insights.py --season 2024`

## Security and sensitive data
- `.env` is ignored by git.
- OAuth tokens live in `config/oauth_tokens.json` (ignored).
- Raw/processed data and SQLite files are ignored.

## Deployment
See `docs/DEPLOYMENT.md` for GitHub Pages instructions.

## Data refresh checklist
Use this when you want to refresh data and republish the site.
1) Update `config/config.toml` with the season range you want.
2) Refresh OAuth tokens if needed:
   - `python scripts/oauth2_bootstrap.py`
3) Discover leagues:
   - `python scripts/discover_leagues.py`
4) Sync league data:
   - `python scripts/sync_all.py`
5) Run backfills as needed:
   - `python scripts/backfill_draft_results.py`
   - `python scripts/backfill_stat_modifiers.py`
   - `python scripts/backfill_roster_injuries.py`
   - `python scripts/backfill_player_stats.py`
6) Export site datasets:
   - `python scripts/export_site_data.py`
   - `python scripts/export_injury_reports.py`
7) Generate insights:
   - `python scripts/generate_insights.py`
   - `python scripts/generate_team_insights.py`
8) Deploy updates (see `docs/DEPLOYMENT.md`):
   - `git add .`
   - `git commit -m "Refresh data"`
   - `git push`
   - `git subtree push --prefix site origin gh-pages`
