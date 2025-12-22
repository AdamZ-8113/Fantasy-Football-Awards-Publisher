import argparse
import json
import time
from pathlib import Path

from db import connect_db, init_db, insert_raw_response, upsert_many
from parse_yahoo_xml import parse_draft_results
from raw_store import save_raw_xml
from yahoo_client import api_get_response, parse_xml

BASE_DIR = Path(__file__).resolve().parents[1]
STORE_RAW_BODY_IN_DB = True


class Counter:
    def __init__(self):
        self.value = 1

    def next(self):
        current = self.value
        self.value += 1
        return current


def fetch_xml(conn, counter, endpoint, season=None, league_key=None, allow_statuses=None):
    response = api_get_response(endpoint)
    body = response.content

    file_path = save_raw_xml(
        BASE_DIR,
        season or "unknown",
        league_key or "unknown",
        endpoint,
        None,
        body,
        counter.next(),
    )

    record = (
        time.strftime("%Y-%m-%d %H:%M:%S"),
        str(season) if season is not None else None,
        league_key,
        endpoint,
        None,
        response.status_code,
        file_path,
        body.decode("utf-8", errors="replace") if STORE_RAW_BODY_IN_DB else None,
    )
    insert_raw_response(conn, record)

    if allow_statuses is None:
        allow_statuses = {200}
    if response.status_code not in allow_statuses:
        raise RuntimeError(f"Request failed: {endpoint} ({response.status_code})")
    if response.status_code != 200:
        return None
    return body


def load_leagues(conn, season=None, league_key=None):
    rows = conn.execute(
        "SELECT league_key, season FROM leagues ORDER BY season"
    ).fetchall()
    leagues = [(row[0], row[1]) for row in rows]
    if season is not None:
        leagues = [l for l in leagues if str(l[1]) == str(season)]
    if league_key:
        leagues = [l for l in leagues if l[0] == league_key]
    return leagues


def draft_has_data(conn, league_key):
    cursor = conn.execute(
        "SELECT COUNT(*) FROM draft_results WHERE league_key = ?",
        (league_key,),
    )
    return cursor.fetchone()[0] > 0


def main():
    parser = argparse.ArgumentParser(description="Backfill Yahoo Fantasy draft results.")
    parser.add_argument("--season", help="Only backfill a single season.")
    parser.add_argument("--league", dest="league_key", help="Only backfill a single league_key.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip leagues that already have draft results.")
    args = parser.parse_args()

    conn = connect_db()
    init_db(conn)

    leagues = load_leagues(conn, season=args.season, league_key=args.league_key)
    if not leagues:
        print("No leagues available in the database to backfill.")
        return

    counter = Counter()
    for league_key, season in leagues:
        if args.skip_existing and draft_has_data(conn, league_key):
            print(f"Skipping {league_key} (season {season}) - draft results already present")
            continue

        print(f"Fetching draft results for {league_key} (season {season})")
        draft_xml = fetch_xml(
            conn,
            counter,
            f"/league/{league_key}/draftresults",
            season=season,
            league_key=league_key,
            allow_statuses={200, 404},
        )
        if draft_xml is None:
            continue
        draft_root = parse_xml(draft_xml)
        draft_results = parse_draft_results(draft_root)
        for row in draft_results:
            row["league_key"] = league_key
        upsert_many(
            conn,
            "draft_results",
            ("league_key", "team_key", "player_key", "round", "pick", "cost", "is_keeper", "is_autopick"),
            [
                (
                    row.get("league_key"),
                    row.get("team_key"),
                    row.get("player_key"),
                    row.get("round"),
                    row.get("pick"),
                    row.get("cost"),
                    row.get("is_keeper"),
                    row.get("is_autopick"),
                )
                for row in draft_results
            ],
        )

    print("Draft backfill complete.")


if __name__ == "__main__":
    main()
