import sqlite3
from pathlib import Path

from db import connect_db, init_db, upsert_many
from parse_yahoo_xml import parse_settings
from yahoo_client import parse_xml

BASE_DIR = Path(__file__).resolve().parents[1]


def _load_settings_xml(row):
    file_path = row["file_path"]
    if file_path:
        path = Path(file_path)
        if path.exists():
            return path.read_bytes()
    body = row["body"]
    if body:
        return body.encode("utf-8", errors="replace")
    return None


def _load_latest_settings_rows(conn):
    rows = conn.execute(
        """
        SELECT r.id, r.league_key, r.season, r.file_path, r.body
        FROM raw_responses r
        JOIN (
            SELECT league_key, MAX(id) as max_id
            FROM raw_responses
            WHERE endpoint LIKE '/league/%/settings'
              AND league_key IS NOT NULL
              AND http_status = 200
            GROUP BY league_key
        ) latest
          ON latest.max_id = r.id
        """
    ).fetchall()
    return rows


def main():
    conn = connect_db()
    init_db(conn)

    rows = _load_latest_settings_rows(conn)
    if not rows:
        print("No settings responses found in raw_responses.")
        return

    updates = []
    for row in rows:
        xml_bytes = _load_settings_xml(row)
        if not xml_bytes:
            continue
        root = parse_xml(xml_bytes)
        settings = parse_settings(root)
        if not settings:
            continue
        updates.append(
            (
                row["league_key"],
                settings.get("start_week"),
                settings.get("end_week"),
                settings.get("playoff_start_week"),
                settings.get("num_teams"),
                settings.get("scoring_type"),
                settings.get("roster_positions"),
                settings.get("stat_categories"),
                settings.get("stat_modifiers"),
            )
        )

    if not updates:
        print("No settings updates to apply.")
        return

    upsert_many(
        conn,
        "league_settings",
        (
            "league_key",
            "start_week",
            "end_week",
            "playoff_start_week",
            "num_teams",
            "scoring_type",
            "roster_positions",
            "stat_categories",
            "stat_modifiers",
        ),
        updates,
    )

    print(f"Updated league_settings for {len(updates)} leagues.")


if __name__ == "__main__":
    main()
