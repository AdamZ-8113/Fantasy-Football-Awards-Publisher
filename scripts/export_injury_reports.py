import argparse
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "processed" / "fantasy_insights.sqlite"
OUTPUT_PATH = BASE_DIR / "site" / "data" / "injury_reports.json"

IR_PLUS_STATUSES = {
    "IR",
    "O",
    "NA",
    "COVID-19",
    "PUP",
    "SUSP",
}


def normalize_status(value):
    return str(value or "").strip().upper()


def is_eligible_status(status):
    return normalize_status(status) in IR_PLUS_STATUSES


def load_team_map(conn):
    rows = conn.execute(
        "SELECT team_key, league_key, name, manager_names FROM teams"
    ).fetchall()
    return {
        row["team_key"]: {
            "team_name": row["name"],
            "manager_names": row["manager_names"],
            "league_key": row["league_key"],
        }
        for row in rows
    }


def load_player_map(conn):
    rows = conn.execute(
        "SELECT player_key, name_full, position FROM players"
    ).fetchall()
    return {
        row["player_key"]: {
            "player_name": row["name_full"],
            "player_position": row["position"],
        }
        for row in rows
    }


def load_end_weeks(conn):
    rows = conn.execute(
        "SELECT league_key, end_week FROM league_settings"
    ).fetchall()
    return {row["league_key"]: row["end_week"] for row in rows}


def main():
    parser = argparse.ArgumentParser(description="Export injury roster and drop reports.")
    parser.add_argument("--window-weeks", type=int, default=2, help="Weeks after injury to count a drop.")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Missing database: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    leagues = conn.execute(
        "SELECT league_key, season FROM leagues ORDER BY season"
    ).fetchall()
    team_map = load_team_map(conn)
    player_map = load_player_map(conn)
    end_weeks = load_end_weeks(conn)

    reports = []
    for league in leagues:
        league_key = league["league_key"]
        season = league["season"]

        roster_rows = conn.execute(
            """
            SELECT team_key, week, player_key, status, injury_status
            FROM rosters
            WHERE league_key = ?
            """,
            (league_key,),
        ).fetchall()

        team_player = defaultdict(lambda: {"weeks": set(), "injury_weeks": set(), "statuses": set()})
        for row in roster_rows:
            team_key = row["team_key"]
            player_key = row["player_key"]
            week = row["week"]
            if team_key is None or player_key is None or week is None:
                continue
            team_player[(team_key, player_key)]["weeks"].add(week)

            status = row["injury_status"] or row["status"]
            if is_eligible_status(status):
                team_player[(team_key, player_key)]["injury_weeks"].add(week)
                team_player[(team_key, player_key)]["statuses"].add(normalize_status(status))

        teams = defaultdict(lambda: {"injured_players": [], "injury_drops": []})
        end_week = end_weeks.get(league_key)

        for (team_key, player_key), data in team_player.items():
            if not data["weeks"]:
                continue
            weeks = sorted(data["weeks"])
            injury_weeks = sorted(data["injury_weeks"])
            if injury_weeks:
                player_info = player_map.get(player_key, {})
                teams[team_key]["injured_players"].append(
                    {
                        "player_key": player_key,
                        "player_name": player_info.get("player_name"),
                        "player_position": player_info.get("player_position"),
                        "injury_weeks": injury_weeks,
                        "statuses": sorted(data["statuses"]),
                    }
                )

            last_week = weeks[-1]
            if end_week and last_week >= end_week:
                continue
            if not injury_weeks:
                continue
            recent_injury = max(injury_weeks) >= last_week - args.window_weeks
            if recent_injury:
                player_info = player_map.get(player_key, {})
                teams[team_key]["injury_drops"].append(
                    {
                        "player_key": player_key,
                        "player_name": player_info.get("player_name"),
                        "player_position": player_info.get("player_position"),
                        "last_week": last_week,
                        "injury_weeks": injury_weeks,
                        "statuses": sorted(data["statuses"]),
                    }
                )

        report_teams = []
        for team_key, payload in teams.items():
            info = team_map.get(team_key, {})
            report_teams.append(
                {
                    "team_key": team_key,
                    "team_name": info.get("team_name"),
                    "manager_names": info.get("manager_names"),
                    "injured_players": sorted(
                        payload["injured_players"],
                        key=lambda item: len(item.get("injury_weeks", [])),
                        reverse=True,
                    ),
                    "injury_drops": payload["injury_drops"],
                }
            )

        reports.append(
            {
                "season": season,
                "league_key": league_key,
                "window_weeks": args.window_weeks,
                "eligible_statuses": sorted(IR_PLUS_STATUSES),
                "teams": report_teams,
            }
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "reports": reports,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
