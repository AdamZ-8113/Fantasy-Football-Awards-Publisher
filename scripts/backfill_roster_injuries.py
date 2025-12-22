import re
import sqlite3
from pathlib import Path

from db import connect_db, init_db, upsert_many
from parse_yahoo_xml import parse_roster
from yahoo_client import parse_xml

BASE_DIR = Path(__file__).resolve().parents[1]
ROSTER_ENDPOINT = re.compile(r"^/team/(?P<team_key>[^/]+)/roster;week=(?P<week>\d+)")


def _load_xml_bytes(row):
    file_path = row["file_path"]
    if file_path:
        path = Path(file_path)
        if path.exists():
            return path.read_bytes()
    body = row["body"]
    if body:
        return body.encode("utf-8", errors="replace")
    return None


def _load_roster_responses(conn):
    rows = conn.execute(
        """
        SELECT id, league_key, endpoint, file_path, body
        FROM raw_responses
        WHERE endpoint LIKE '/team/%/roster;week=%'
          AND http_status = 200
        """
    ).fetchall()
    return rows


def main():
    conn = connect_db()
    init_db(conn)

    rows = _load_roster_responses(conn)
    if not rows:
        print("No roster responses found in raw_responses.")
        return

    updated = 0
    for row in rows:
        endpoint = row["endpoint"]
        match = ROSTER_ENDPOINT.match(endpoint or "")
        if not match:
            continue
        week = int(match.group("week"))
        xml_bytes = _load_xml_bytes(row)
        if not xml_bytes:
            continue
        root = parse_xml(xml_bytes)
        roster_rows, _players = parse_roster(root, week)
        for item in roster_rows:
            item["league_key"] = row["league_key"]
        upsert_many(
            conn,
            "rosters",
            ("league_key", "team_key", "week", "player_key", "position", "status", "injury_status", "injury_note"),
            [
                (
                    item.get("league_key"),
                    item.get("team_key"),
                    item.get("week"),
                    item.get("player_key"),
                    item.get("position"),
                    item.get("status"),
                    item.get("injury_status"),
                    item.get("injury_note"),
                )
                for item in roster_rows
            ],
        )
        updated += 1

    print(f"Updated roster injury fields for {updated} roster responses.")


if __name__ == "__main__":
    main()
