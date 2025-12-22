import argparse
import json
import time
from pathlib import Path

from db import connect_db, init_db, insert_raw_response, upsert_many
from parse_yahoo_xml import parse_player_stats
from raw_store import save_raw_xml
from yahoo_client import api_get_response, parse_xml

BASE_DIR = Path(__file__).resolve().parents[1]

REQUEST_SLEEP_SECONDS = 0.25
PLAYER_BATCH_SIZE = 25
ALLOW_STATUSES = {200, 400, 404}
STORE_RAW_BODY_IN_DB = True


def _sleep():
    time.sleep(REQUEST_SLEEP_SECONDS)


def _batch(items, size):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def fetch_xml(conn, endpoint, params, season, league_key):
    response = api_get_response(endpoint, params=params)
    body = response.content

    file_path = save_raw_xml(
        BASE_DIR,
        season or "unknown",
        league_key or "unknown",
        endpoint,
        params,
        body,
        int(time.time() * 1000) % 1000000,
    )

    record = (
        time.strftime("%Y-%m-%d %H:%M:%S"),
        str(season) if season is not None else None,
        league_key,
        endpoint,
        json.dumps(params, ensure_ascii=True) if params else None,
        response.status_code,
        file_path,
        body.decode("utf-8", errors="replace") if STORE_RAW_BODY_IN_DB else None,
    )
    insert_raw_response(conn, record)

    if response.status_code not in ALLOW_STATUSES:
        raise RuntimeError(f"Request failed: {endpoint} ({response.status_code})")

    _sleep()
    if response.status_code != 200:
        return None
    return body


def dicts_to_rows(items, columns):
    return [tuple(item.get(col) for col in columns) for item in items]


def league_weeks(conn, league_key):
    rows = conn.execute(
        "SELECT DISTINCT week FROM rosters WHERE league_key = ? ORDER BY week",
        (league_key,),
    ).fetchall()
    return [row[0] for row in rows]


def league_player_keys(conn, league_key, week):
    rows = conn.execute(
        "SELECT DISTINCT player_key FROM rosters WHERE league_key = ? AND week = ?",
        (league_key, week),
    ).fetchall()
    return [row[0] for row in rows]


def has_player_stats(conn, league_key, week):
    row = conn.execute(
        "SELECT 1 FROM player_stats WHERE league_key = ? AND week = ? LIMIT 1",
        (league_key, week),
    ).fetchone()
    return row is not None


def main():
    parser = argparse.ArgumentParser(description="Backfill player stats from existing rosters.")
    parser.add_argument("--only", dest="only", help="Sync only the specified league_key.")
    parser.add_argument("--force", action="store_true", help="Re-fetch player stats even if they exist.")
    args = parser.parse_args()

    conn = connect_db()
    init_db(conn)

    league_rows = conn.execute("SELECT league_key, season FROM leagues ORDER BY season").fetchall()
    league_seasons = {row[0]: row[1] for row in league_rows}
    league_keys = [row[0] for row in league_rows]

    if args.only:
        league_keys = [key for key in league_keys if key == args.only]

    total_rows = 0
    for league_key in league_keys:
        season = league_seasons.get(league_key)
        weeks = league_weeks(conn, league_key)
        if not weeks:
            continue
        print(f"Backfilling player stats for {league_key} ({season})")

        for week in weeks:
            if not args.force and has_player_stats(conn, league_key, week):
                continue

            player_keys = league_player_keys(conn, league_key, week)
            if not player_keys:
                continue

            for batch in _batch(sorted(player_keys), PLAYER_BATCH_SIZE):
                batch_keys = ",".join(batch)
                endpoint = f"/league/{league_key}/players;player_keys={batch_keys}/stats;type=week;week={week}"
                xml_bytes = fetch_xml(conn, endpoint, None, season, league_key)
                if xml_bytes is None:
                    continue
                root = parse_xml(xml_bytes)
                stats_rows, players = parse_player_stats(root, week)
                for row in stats_rows:
                    row["league_key"] = league_key

                if stats_rows:
                    upsert_many(
                        conn,
                        "player_stats",
                        ("league_key", "player_key", "week", "stat_id", "value"),
                        dicts_to_rows(
                            stats_rows,
                            ("league_key", "player_key", "week", "stat_id", "value"),
                        ),
                    )
                    total_rows += len(stats_rows)

                if players:
                    upsert_many(
                        conn,
                        "players",
                        ("player_key", "player_id", "name_full", "position", "editorial_team_abbr"),
                        dicts_to_rows(
                            players,
                            ("player_key", "player_id", "name_full", "position", "editorial_team_abbr"),
                        ),
                    )

    print(f"Backfilled player_stats rows: {total_rows}")


if __name__ == "__main__":
    main()
