# Fantasy Insights

Goal: pull multi-season Yahoo Fantasy Football league data, analyze it, and publish insights on a simple static webpage.

## Tech Stack (decided)
- Python 3.11 for ingestion, normalization, and analysis
- OAuth 1.0a via `requests-oauthlib`
- XML parsing via `lxml`
- Storage in SQLite (raw + normalized tables)
- Static site in `site/` fed by precomputed JSON/CSV

## Repo Layout
- `scripts/`: data ingestion and analysis scripts
- `data/raw/`: raw API responses (XML or JSON snapshots)
- `data/processed/`: cleaned tables and aggregates
- `config/`: non-secret config files
- `site/`: static webpage assets
- `docs/`: design notes and data model docs

## Setup
1) Create a Python virtual environment.
   - `python -m venv .venv`
   - `./.venv/Scripts/Activate.ps1`
2) Install dependencies.
   - `pip install -r requirements.txt`
3) Copy config and environment templates.
   - `copy .env.example .env`
   - `copy config\config.example.toml config\config.toml`

## OAuth Notes
The Yahoo Fantasy Sports API uses OAuth 1.0a. The ingestion script will:
- open a URL for authorization
- capture the verifier via a local callback (default) or prompt for it
- store the access token/secret and session handle locally

If OAuth 1.0a fails with 401s, use the OAuth 2.0 fallback:
- `python scripts/oauth2_bootstrap.py`

## Scripts
- `python scripts/oauth_bootstrap.py` to create `config/oauth_tokens.json`
- `python scripts/discover_leagues.py` to list league keys by season
- `python scripts/sync_all.py` to pull all seasons into SQLite and raw XML
- `python scripts/export_site_data.py` to generate JSON for the site
- `python scripts/backfill_team_stats.py` to rebuild team_stats from raw XML
- `python scripts/validate_counts.py` to summarize per-league row counts
- `python scripts/backfill_player_stats.py` to fetch player stats from existing rosters
- `python scripts/generate_insights.py` to build season insight JSON files in `site/data/`
- `python scripts/backfill_draft_results.py` to pull draft results into SQLite
- `python scripts/backfill_stat_modifiers.py` to load scoring modifiers from raw settings
- `python scripts/backfill_roster_injuries.py` to backfill roster injury statuses
- `python scripts/export_injury_reports.py` to export injury roster/drop reports

## Next Implementation Steps
- Fetch league metadata by season
- Normalize league, team, matchup, roster, and transaction data
- Generate summary tables for the static site
