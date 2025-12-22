import re
from pathlib import Path

from db import connect_db, init_db, upsert_many
from parse_yahoo_xml import parse_team_stats
from yahoo_client import parse_xml

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

WEEK_PATTERN = re.compile(r"week_(\d+)")


def extract_week_from_path(path):
    match = WEEK_PATTERN.search(path.name)
    if match:
        return int(match.group(1))
    return None


def main():
    conn = connect_db()
    init_db(conn)

    files = sorted(RAW_DIR.rglob("*stats_type_week_week_*.xml"))
    if not files:
        print("No team stats files found.")
        return

    total_rows = 0
    for file_path in files:
        week = extract_week_from_path(file_path)
        if week is None:
            continue
        league_key = file_path.parent.name
        xml_bytes = file_path.read_bytes()
        root = parse_xml(xml_bytes)
        rows = parse_team_stats(root, week)
        for row in rows:
            row["league_key"] = league_key
        if rows:
            upsert_many(
                conn,
                "team_stats",
                ("league_key", "team_key", "week", "stat_id", "value"),
                [(r.get("league_key"), r.get("team_key"), r.get("week"), r.get("stat_id"), r.get("value")) for r in rows],
            )
            total_rows += len(rows)

    print(f"Backfilled team_stats rows: {total_rows}")


if __name__ == "__main__":
    main()
