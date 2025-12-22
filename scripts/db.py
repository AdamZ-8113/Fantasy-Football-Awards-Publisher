import json
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "processed" / "fantasy_insights.sqlite"


def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _ensure_column(conn, table, column, definition):
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at TEXT NOT NULL,
            season TEXT,
            league_key TEXT,
            endpoint TEXT NOT NULL,
            params TEXT,
            http_status INTEGER,
            file_path TEXT,
            body TEXT
        );

        CREATE TABLE IF NOT EXISTS leagues (
            league_key TEXT PRIMARY KEY,
            league_id TEXT,
            name TEXT,
            season TEXT,
            game_key TEXT
        );

        CREATE TABLE IF NOT EXISTS league_settings (
            league_key TEXT PRIMARY KEY,
            start_week INTEGER,
            end_week INTEGER,
            playoff_start_week INTEGER,
            num_teams INTEGER,
            scoring_type TEXT,
            roster_positions TEXT,
            stat_categories TEXT,
            stat_modifiers TEXT
        );

        CREATE TABLE IF NOT EXISTS teams (
            team_key TEXT PRIMARY KEY,
            league_key TEXT,
            team_id TEXT,
            name TEXT,
            url TEXT,
            manager_names TEXT
        );

        CREATE TABLE IF NOT EXISTS standings (
            league_key TEXT,
            team_key TEXT,
            rank INTEGER,
            wins INTEGER,
            losses INTEGER,
            ties INTEGER,
            points_for REAL,
            points_against REAL,
            PRIMARY KEY (league_key, team_key)
        );

        CREATE TABLE IF NOT EXISTS matchups (
            league_key TEXT,
            week INTEGER,
            matchup_id TEXT,
            status TEXT,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            winner_team_key TEXT,
            PRIMARY KEY (league_key, week, matchup_id)
        );

        CREATE TABLE IF NOT EXISTS matchup_teams (
            league_key TEXT,
            week INTEGER,
            matchup_id TEXT,
            team_key TEXT,
            points REAL,
            projected_points REAL,
            win_status TEXT,
            PRIMARY KEY (league_key, week, matchup_id, team_key)
        );

        CREATE TABLE IF NOT EXISTS rosters (
            league_key TEXT,
            team_key TEXT,
            week INTEGER,
            player_key TEXT,
            position TEXT,
            status TEXT,
            injury_status TEXT,
            injury_note TEXT,
            PRIMARY KEY (league_key, team_key, week, player_key)
        );

        CREATE TABLE IF NOT EXISTS players (
            player_key TEXT PRIMARY KEY,
            player_id TEXT,
            name_full TEXT,
            position TEXT,
            editorial_team_abbr TEXT
        );

        CREATE TABLE IF NOT EXISTS draft_results (
            league_key TEXT,
            team_key TEXT,
            player_key TEXT,
            round INTEGER,
            pick INTEGER,
            cost REAL,
            is_keeper INTEGER,
            is_autopick INTEGER,
            PRIMARY KEY (league_key, round, pick, team_key, player_key)
        );

        CREATE TABLE IF NOT EXISTS team_stats (
            league_key TEXT,
            team_key TEXT,
            week INTEGER,
            stat_id TEXT,
            value TEXT,
            PRIMARY KEY (league_key, team_key, week, stat_id)
        );

        CREATE TABLE IF NOT EXISTS player_stats (
            league_key TEXT,
            player_key TEXT,
            week INTEGER,
            stat_id TEXT,
            value TEXT,
            PRIMARY KEY (league_key, player_key, week, stat_id)
        );

        CREATE TABLE IF NOT EXISTS transactions (
            transaction_key TEXT PRIMARY KEY,
            league_key TEXT,
            type TEXT,
            status TEXT,
            timestamp INTEGER
        );

        CREATE TABLE IF NOT EXISTS transaction_players (
            transaction_key TEXT,
            player_key TEXT,
            transaction_type TEXT,
            source_type TEXT,
            source_team_key TEXT,
            destination_type TEXT,
            destination_team_key TEXT,
            PRIMARY KEY (transaction_key, player_key, transaction_type, source_team_key, destination_team_key)
        );
        """
    )
    _ensure_column(conn, "league_settings", "stat_modifiers", "TEXT")
    _ensure_column(conn, "rosters", "status", "TEXT")
    _ensure_column(conn, "rosters", "injury_status", "TEXT")
    _ensure_column(conn, "rosters", "injury_note", "TEXT")
    conn.commit()


def upsert_many(conn, table, columns, rows):
    if not rows:
        return
    placeholders = ",".join("?" for _ in columns)
    cols = ",".join(columns)
    sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
    conn.executemany(sql, rows)
    conn.commit()


def insert_raw_response(conn, record):
    columns = (
        "fetched_at",
        "season",
        "league_key",
        "endpoint",
        "params",
        "http_status",
        "file_path",
        "body",
    )
    upsert_many(conn, "raw_responses", columns, [record])


def to_json(value):
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True)
