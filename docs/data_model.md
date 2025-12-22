# Data Model

This project stores raw Yahoo Fantasy API XML on disk and normalized tables in SQLite.

## Storage Locations
- Raw XML: `data/raw/<season>/<league_key>/...`
- SQLite DB: `data/processed/fantasy_insights.sqlite`

## Tables

### raw_responses
Tracks every API response saved to disk.
- fetched_at, season, league_key, endpoint, params, http_status, file_path, body

### leagues
League metadata per season.
- league_key (PK), league_id, name, season, game_key

### league_settings
Settings for each league.
- league_key (PK), start_week, end_week, playoff_start_week, num_teams, scoring_type
- roster_positions (JSON), stat_categories (JSON), stat_modifiers (JSON)

### teams
Teams within a league.
- team_key (PK), league_key, team_id, name, url, manager_names

### standings
Final standings per team.
- league_key + team_key (PK), rank, wins, losses, ties, points_for, points_against

### matchups
Weekly matchups.
- league_key + week + matchup_id (PK), status, is_playoffs, is_consolation, winner_team_key

### matchup_teams
Team entries for each matchup.
- league_key + week + matchup_id + team_key (PK)
- points, projected_points, win_status

### rosters
Weekly roster membership.
- league_key + team_key + week + player_key (PK), position, status, injury_status, injury_note

### players
Player metadata.
- player_key (PK), player_id, name_full, position, editorial_team_abbr

### draft_results
Draft picks per league.
- league_key + round + pick + team_key + player_key (PK)
- team_key, player_key, round, pick, cost, is_keeper, is_autopick

### team_stats
Weekly team stats.
- league_key + team_key + week + stat_id (PK), value

### player_stats
Weekly player stats.
- league_key + player_key + week + stat_id (PK), value

### transactions
Transactions in a league.
- transaction_key (PK), league_key, type, status, timestamp

### transaction_players
Players involved in transactions.
- transaction_key + player_key + transaction_type + source_team_key + destination_team_key (PK)
- source_type, destination_type
