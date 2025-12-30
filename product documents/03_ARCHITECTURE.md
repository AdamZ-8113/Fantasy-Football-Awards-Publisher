# Architecture

## Overview
The system is a static-site pipeline:
1) Yahoo API ingestion and normalization into SQLite.
2) Insight generation into JSON files.
3) Static site renders JSON into UI.

## Components
1) Ingestion and storage
   - `scripts/yahoo_client.py` handles OAuth requests.
   - `scripts/sync_all.py` writes raw XML and normalized tables.
   - Raw XML in `data/raw/`.
   - SQLite in `data/processed/fantasy_insights.sqlite`.

2) Data enrichment and exports
   - Backfill scripts fill missing tables.
   - `scripts/export_site_data.py` creates `site/data/*` datasets.
   - `scripts/export_injury_reports.py` creates injury report JSON.

3) Insights
   - `scripts/generate_insights.py` builds season awards JSON.
   - `scripts/generate_team_insights.py` builds team awards JSON.

4) Frontend
   - `site/index.html`, `site/styles.css`, `site/app.js`
   - The app fetches `site/data/*.json` and renders.
   - Team-specific view hides the League Overview section.

## Data flow
Yahoo API -> raw XML -> SQLite -> export JSON -> render in static site

## Key storage paths
- Raw API: `data/raw/<season>/<league_key>/*.xml`
- SQLite: `data/processed/fantasy_insights.sqlite`
- Site JSON: `site/data/*.json`
