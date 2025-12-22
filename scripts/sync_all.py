import argparse
import json
import time
from pathlib import Path

from db import connect_db, init_db, insert_raw_response, upsert_many
from parse_yahoo_xml import (
    parse_games,
    parse_leagues,
    parse_league_meta,
    parse_settings,
    parse_teams,
    parse_standings,
    parse_matchups,
    parse_roster,
    parse_team_stats,
    parse_player_stats,
    parse_transactions,
    parse_draft_results,
)
from raw_store import save_raw_xml
from yahoo_client import api_get_response, load_config, parse_xml

BASE_DIR = Path(__file__).resolve().parents[1]

REQUEST_SLEEP_SECONDS = 0.25
TRANSACTION_PAGE_SIZE = 25
PLAYER_BATCH_SIZE = 25
STORE_RAW_BODY_IN_DB = True
DEFAULT_START_WEEK = 1
DEFAULT_END_WEEK = 17
FETCH_PLAYER_STATS = False
PROGRESS_PATH = BASE_DIR / "data" / "processed" / "sync_progress.json"


class SyncContext:
    def __init__(self):
        self.counter = 1

    def next_counter(self):
        value = self.counter
        self.counter += 1
        return value


def _sleep():
    time.sleep(REQUEST_SLEEP_SECONDS)


def fetch_xml(conn, ctx, endpoint, params=None, season=None, league_key=None, allow_statuses=None):
    response = api_get_response(endpoint, params=params)
    body = response.content

    file_path = save_raw_xml(
        BASE_DIR,
        season or "unknown",
        league_key or "unknown",
        endpoint,
        params,
        body,
        ctx.next_counter(),
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

    if allow_statuses is None:
        allow_statuses = {200}
    if response.status_code not in allow_statuses:
        raise RuntimeError(f"Request failed: {endpoint} ({response.status_code})")
    if response.status_code != 200:
        return None

    _sleep()
    return body


def dicts_to_rows(items, columns):
    rows = []
    for item in items:
        rows.append(tuple(item.get(col) for col in columns))
    return rows


def load_progress():
    if not PROGRESS_PATH.exists():
        return {}
    try:
        return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_progress(league):
    payload = {
        "last_league_key": league.get("league_key"),
        "season": league.get("season"),
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def league_has_data(conn, league_key):
    if not league_key:
        return False
    cursor = conn.execute(
        "SELECT COUNT(*) FROM matchups WHERE league_key = ?",
        (league_key,),
    )
    matchups = cursor.fetchone()[0]
    cursor = conn.execute(
        "SELECT COUNT(*) FROM teams WHERE league_key = ?",
        (league_key,),
    )
    teams = cursor.fetchone()[0]
    cursor = conn.execute(
        "SELECT COUNT(*) FROM standings WHERE league_key = ?",
        (league_key,),
    )
    standings = cursor.fetchone()[0]
    return matchups > 0 and teams > 0 and standings > 0


def discover_leagues(conn, ctx, config):
    game_code = str(config.get("game_key", "nfl"))
    season_start = int(config.get("season_start", 0))
    season_end = int(config.get("season_end", 9999))
    league_name_hint = str(config.get("league_name_hint", "")).strip().lower()
    league_id_hint = str(config.get("league_id_hint", "")).strip()

    games_xml = fetch_xml(conn, ctx, "/users;use_login=1/games", season="global")
    games_root = parse_xml(games_xml)
    games = parse_games(games_root)

    games = [
        g for g in games
        if g.get("code") == game_code
        and g.get("season").isdigit()
        and season_start <= int(g.get("season")) <= season_end
    ]

    leagues = []
    for game in games:
        game_key = game["game_key"]
        leagues_xml = fetch_xml(
            conn,
            ctx,
            f"/users;use_login=1/games;game_keys={game_key}/leagues",
            season=game.get("season"),
        )
        leagues_root = parse_xml(leagues_xml)
        parsed = parse_leagues(leagues_root)
        for league in parsed:
            league["game_key"] = game_key
            league["season"] = game.get("season")
        leagues.extend(parsed)

    if league_id_hint:
        leagues = [l for l in leagues if l.get("league_id") == league_id_hint]
    if league_name_hint:
        leagues = [l for l in leagues if league_name_hint in l.get("name", "").lower()]

    return leagues


def _week_range(settings):
    start_week = settings.get("start_week") or DEFAULT_START_WEEK
    end_week = settings.get("end_week") or DEFAULT_END_WEEK
    if end_week < start_week:
        end_week = start_week
    return range(start_week, end_week + 1)


def _batch(items, size):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def sync_league(conn, ctx, league):
    league_key = league["league_key"]
    season = league.get("season")

    league_xml = fetch_xml(conn, ctx, f"/league/{league_key}", season=season, league_key=league_key)
    league_root = parse_xml(league_xml)
    meta = parse_league_meta(league_root)
    if meta:
        meta["season"] = meta.get("season") or season
        meta["game_key"] = meta.get("game_key") or league.get("game_key")
        upsert_many(
            conn,
            "leagues",
            ("league_key", "league_id", "name", "season", "game_key"),
            [(meta.get("league_key"), meta.get("league_id"), meta.get("name"), meta.get("season"), meta.get("game_key"))],
        )

    settings_xml = fetch_xml(conn, ctx, f"/league/{league_key}/settings", season=season, league_key=league_key)
    settings_root = parse_xml(settings_xml)
    settings = parse_settings(settings_root)
    if settings:
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
            [(
                league_key,
                settings.get("start_week"),
                settings.get("end_week"),
                settings.get("playoff_start_week"),
                settings.get("num_teams"),
                settings.get("scoring_type"),
                settings.get("roster_positions"),
                settings.get("stat_categories"),
                settings.get("stat_modifiers"),
            )],
        )

    teams_xml = fetch_xml(conn, ctx, f"/league/{league_key}/teams", season=season, league_key=league_key)
    teams_root = parse_xml(teams_xml)
    teams = parse_teams(teams_root)
    for team in teams:
        team["league_key"] = league_key
    upsert_many(
        conn,
        "teams",
        ("team_key", "league_key", "team_id", "name", "url", "manager_names"),
        dicts_to_rows(teams, ("team_key", "league_key", "team_id", "name", "url", "manager_names")),
    )

    standings_xml = fetch_xml(conn, ctx, f"/league/{league_key}/standings", season=season, league_key=league_key)
    standings_root = parse_xml(standings_xml)
    standings = parse_standings(standings_root)
    for row in standings:
        row["league_key"] = league_key
    upsert_many(
        conn,
        "standings",
        ("league_key", "team_key", "rank", "wins", "losses", "ties", "points_for", "points_against"),
        dicts_to_rows(
            standings,
            ("league_key", "team_key", "rank", "wins", "losses", "ties", "points_for", "points_against"),
        ),
    )

    draft_xml = fetch_xml(
        conn,
        ctx,
        f"/league/{league_key}/draftresults",
        season=season,
        league_key=league_key,
        allow_statuses={200, 404},
    )
    if draft_xml is not None:
        draft_root = parse_xml(draft_xml)
        draft_results = parse_draft_results(draft_root)
        for row in draft_results:
            row["league_key"] = league_key
        upsert_many(
            conn,
            "draft_results",
            ("league_key", "team_key", "player_key", "round", "pick", "cost", "is_keeper", "is_autopick"),
            dicts_to_rows(
                draft_results,
                ("league_key", "team_key", "player_key", "round", "pick", "cost", "is_keeper", "is_autopick"),
            ),
        )

    for week in _week_range(settings):
        scoreboard_xml = fetch_xml(
            conn,
            ctx,
            f"/league/{league_key}/scoreboard;week={week}",
            season=season,
            league_key=league_key,
        )
        scoreboard_root = parse_xml(scoreboard_xml)
        matchups, matchup_teams = parse_matchups(scoreboard_root, week)
        for row in matchups:
            row["league_key"] = league_key
        for row in matchup_teams:
            row["league_key"] = league_key

        upsert_many(
            conn,
            "matchups",
            ("league_key", "week", "matchup_id", "status", "is_playoffs", "is_consolation", "winner_team_key"),
            dicts_to_rows(
                matchups,
                ("league_key", "week", "matchup_id", "status", "is_playoffs", "is_consolation", "winner_team_key"),
            ),
        )
        upsert_many(
            conn,
            "matchup_teams",
            ("league_key", "week", "matchup_id", "team_key", "points", "projected_points", "win_status"),
            dicts_to_rows(
                matchup_teams,
                ("league_key", "week", "matchup_id", "team_key", "points", "projected_points", "win_status"),
            ),
        )

    team_keys = [team["team_key"] for team in teams if team.get("team_key")]
    for week in _week_range(settings):
        week_player_keys = set()
        for team_key in team_keys:
            roster_xml = fetch_xml(
                conn,
                ctx,
                f"/team/{team_key}/roster;week={week}",
                season=season,
                league_key=league_key,
                allow_statuses={200, 400, 404},
            )
            if roster_xml is None:
                continue
            roster_root = parse_xml(roster_xml)
            roster_rows, roster_players = parse_roster(roster_root, week)
            for row in roster_rows:
                row["league_key"] = league_key
                week_player_keys.add(row["player_key"])
            upsert_many(
                conn,
                "rosters",
                ("league_key", "team_key", "week", "player_key", "position", "status", "injury_status", "injury_note"),
                dicts_to_rows(
                    roster_rows,
                    ("league_key", "team_key", "week", "player_key", "position", "status", "injury_status", "injury_note"),
                ),
            )
            upsert_many(
                conn,
                "players",
                ("player_key", "player_id", "name_full", "position", "editorial_team_abbr"),
                dicts_to_rows(
                    roster_players,
                    ("player_key", "player_id", "name_full", "position", "editorial_team_abbr"),
                ),
            )

            stats_xml = fetch_xml(
                conn,
                ctx,
                f"/team/{team_key}/stats;type=week;week={week}",
                season=season,
                league_key=league_key,
                allow_statuses={200, 400, 404},
            )
            if stats_xml is None:
                continue
            stats_root = parse_xml(stats_xml)
            team_stats = parse_team_stats(stats_root, week)
            for row in team_stats:
                row["league_key"] = league_key
            upsert_many(
                conn,
                "team_stats",
                ("league_key", "team_key", "week", "stat_id", "value"),
                dicts_to_rows(team_stats, ("league_key", "team_key", "week", "stat_id", "value")),
            )

        if FETCH_PLAYER_STATS and week_player_keys:
            player_keys = sorted(week_player_keys)
            for batch in _batch(player_keys, PLAYER_BATCH_SIZE):
                batch_keys = ",".join(batch)
                players_xml = fetch_xml(
                    conn,
                    ctx,
                    f"/league/{league_key}/players;player_keys={batch_keys}/stats;type=week;week={week}",
                    season=season,
                    league_key=league_key,
                    allow_statuses={200, 400, 404},
                )
                if players_xml is None:
                    continue
                players_root = parse_xml(players_xml)
                player_stats, players = parse_player_stats(players_root, week)
                for row in player_stats:
                    row["league_key"] = league_key
                upsert_many(
                    conn,
                    "player_stats",
                    ("league_key", "player_key", "week", "stat_id", "value"),
                    dicts_to_rows(player_stats, ("league_key", "player_key", "week", "stat_id", "value")),
                )
                upsert_many(
                    conn,
                    "players",
                    ("player_key", "player_id", "name_full", "position", "editorial_team_abbr"),
                    dicts_to_rows(
                        players,
                        ("player_key", "player_id", "name_full", "position", "editorial_team_abbr"),
                    ),
                )

    start = 0
    while True:
        transactions_xml = fetch_xml(
            conn,
            ctx,
            f"/league/{league_key}/transactions;start={start};count={TRANSACTION_PAGE_SIZE}",
            season=season,
            league_key=league_key,
        )
        transactions_root = parse_xml(transactions_xml)
        transactions, transaction_players, players = parse_transactions(transactions_root)

        if not transactions:
            break

        for txn in transactions:
            txn["league_key"] = league_key
        upsert_many(
            conn,
            "transactions",
            ("transaction_key", "league_key", "type", "status", "timestamp"),
            dicts_to_rows(transactions, ("transaction_key", "league_key", "type", "status", "timestamp")),
        )
        upsert_many(
            conn,
            "transaction_players",
            (
                "transaction_key",
                "player_key",
                "transaction_type",
                "source_type",
                "source_team_key",
                "destination_type",
                "destination_team_key",
            ),
            dicts_to_rows(
                transaction_players,
                (
                    "transaction_key",
                    "player_key",
                    "transaction_type",
                    "source_type",
                    "source_team_key",
                    "destination_type",
                    "destination_team_key",
                ),
            ),
        )
        upsert_many(
            conn,
            "players",
            ("player_key", "player_id", "name_full", "position", "editorial_team_abbr"),
            dicts_to_rows(players, ("player_key", "player_id", "name_full", "position", "editorial_team_abbr")),
        )

        if len(transactions) < TRANSACTION_PAGE_SIZE:
            break
        start += TRANSACTION_PAGE_SIZE


def main():
    parser = argparse.ArgumentParser(description="Sync all Yahoo Fantasy league data.")
    parser.add_argument("--start-after", dest="start_after", help="Skip leagues up to and including this league_key.")
    parser.add_argument("--start-at", dest="start_at", help="Start syncing from this league_key.")
    parser.add_argument("--only", dest="only", help="Sync only the specified league_key.")
    parser.add_argument("--resume", action="store_true", help="Resume from last completed league in sync_progress.json.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip leagues with existing matchup/team/standings data.")
    args = parser.parse_args()

    config = load_config()
    conn = connect_db()
    init_db(conn)

    ctx = SyncContext()
    leagues = discover_leagues(conn, ctx, config)
    if not leagues:
        print("No leagues found for the specified filters.")
        return

    leagues = sorted(
        leagues,
        key=lambda item: int(item.get("season") or 0),
    )

    if args.resume:
        progress = load_progress()
        if progress.get("last_league_key"):
            args.start_after = progress["last_league_key"]

    if args.only:
        leagues = [l for l in leagues if l.get("league_key") == args.only]
    elif args.start_at:
        found = False
        filtered = []
        for league in leagues:
            if league.get("league_key") == args.start_at:
                found = True
            if found:
                filtered.append(league)
        leagues = filtered
    elif args.start_after:
        filtered = []
        skip = True
        for league in leagues:
            if skip:
                if league.get("league_key") == args.start_after:
                    skip = False
                continue
            filtered.append(league)
        leagues = filtered

    for league in leagues:
        league_key = league.get("league_key")
        season = league.get("season")
        if args.skip_existing and league_has_data(conn, league_key):
            print(f"Skipping league {league_key} (season {season}) - data already present")
            continue

        print(f"Syncing league {league_key} (season {season})")
        sync_league(conn, ctx, league)
        save_progress(league)

    print("Sync complete.")


if __name__ == "__main__":
    main()
