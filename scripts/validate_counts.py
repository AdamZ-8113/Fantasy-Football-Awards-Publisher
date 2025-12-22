import json
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "processed" / "fantasy_insights.sqlite"
LEAGUES_PATH = BASE_DIR / "data" / "processed" / "leagues.json"


def load_league_keys(conn):
    if LEAGUES_PATH.exists():
        data = json.loads(LEAGUES_PATH.read_text(encoding="utf-8"))
        return [(row.get("league_key"), row.get("season")) for row in data]
    rows = conn.execute("SELECT league_key, season FROM leagues").fetchall()
    return [(row[0], row[1]) for row in rows]


def count_by_league(conn, table):
    rows = conn.execute(f"SELECT league_key, COUNT(*) as count FROM {table} GROUP BY league_key").fetchall()
    return {row[0]: row[1] for row in rows}


def main():
    if not DB_PATH.exists():
        print(f"Missing database: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    league_keys = load_league_keys(conn)

    tables = ["teams", "standings", "matchups", "rosters", "team_stats", "transactions", "draft_results"]
    counts = {table: count_by_league(conn, table) for table in tables}

    for league_key, season in sorted(league_keys, key=lambda item: int(item[1] or 0)):
        season_label = season or "?"
        print(f"{season_label} {league_key}")
        for table in tables:
            value = counts[table].get(league_key, 0)
            print(f"  {table}: {value}")


if __name__ == "__main__":
    main()
