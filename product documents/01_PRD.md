# Product Requirements Document (PRD)

## Purpose
Create a "Wrapped" style site for a long-running Yahoo Fantasy Football league. Pull multi-season data, generate awards and insights, and publish a static site that updates when data is refreshed.

## Target users
- League managers who want year-by-year stories and awards
- League commissioners who want shareable recaps

## Goals
- Pull historical Yahoo Fantasy data (2012 to 2025 for this league).
- Compute awards across performance, heartbreak, draft/value, start/sit, player stats, and fun categories.
- Support team-specific insights (per team/manager) and a league-wide view.
- Publish as a static site via GitHub Pages with no paid hosting.

## Non-goals
- Real-time updates or live in-season scoring.
- Paid hosting or server-side rendering.
- Cross-league multi-tenant support (future option).

## Core features
- OAuth-based Yahoo API ingestion.
- Raw XML archival and normalized SQLite tables.
- Automated insight generation into `site/data/*.json`.
- Static site rendering with themes and season/team pickers.
- Team-specific insights view that hides the league overview panel.

## Key outputs
- `data/processed/fantasy_insights.sqlite` (local only)
- `data/raw/` XML snapshots (local only)
- `site/data/*.json` for the frontend
- Published site on GitHub Pages

## Constraints and assumptions
- Yahoo API has rate limits and requires OAuth.
- Redirect URI must exactly match Yahoo app settings.
- Static site only (no server).

## Success criteria
- 2012-2025 season data available in the site.
- 2025 appears in season picker and shows awards.
- Team picker toggles team-specific awards correctly.
- Site updated within minutes of publishing (after cache purge if using Cloudflare).
