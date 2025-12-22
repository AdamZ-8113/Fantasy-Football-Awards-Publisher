import argparse
import json
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "processed" / "fantasy_insights.sqlite"
OUTPUT_DIR = BASE_DIR / "site" / "data"

BENCH_POSITIONS = {"BN"}


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_leagues(conn):
    rows = conn.execute(
        "SELECT league_key, season FROM leagues ORDER BY season"
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def load_team_map(conn, league_key):
    rows = conn.execute(
        "SELECT team_key, name, manager_names FROM teams WHERE league_key = ?",
        (league_key,),
    ).fetchall()
    return {
        row[0]: {
            "team_key": row[0],
            "team_name": row[1],
            "manager_names": row[2],
        }
        for row in rows
    }


def load_standings(conn, league_key):
    rows = conn.execute(
        "SELECT team_key, rank, wins, losses, ties, points_for, points_against FROM standings WHERE league_key = ?",
        (league_key,),
    ).fetchall()
    return {
        row[0]: {
            "team_key": row[0],
            "rank": row[1],
            "wins": row[2],
            "losses": row[3],
            "ties": row[4],
            "points_for": to_float(row[5]),
            "points_against": to_float(row[6]),
        }
        for row in rows
    }


def load_draft_results(conn, league_key):
    rows = conn.execute(
        """
        SELECT team_key, player_key, round, pick, cost, is_keeper, is_autopick
        FROM draft_results
        WHERE league_key = ?
        """,
        (league_key,),
    ).fetchall()
    results = []
    for row in rows:
        results.append(
            {
                "team_key": row[0],
                "player_key": row[1],
                "round": row[2],
                "pick": row[3],
                "cost": to_float(row[4]),
                "is_keeper": row[5],
                "is_autopick": row[6],
            }
        )
    return results


def load_player_map(conn):
    rows = conn.execute(
        "SELECT player_key, name_full, position FROM players"
    ).fetchall()
    return {
        row[0]: {
            "player_name": row[1],
            "player_position": row[2],
        }
        for row in rows
    }


def load_league_settings(conn, league_key):
    try:
        row = conn.execute(
            "SELECT start_week, end_week, playoff_start_week, stat_categories, stat_modifiers FROM league_settings WHERE league_key = ?",
            (league_key,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = conn.execute(
            "SELECT start_week, end_week, playoff_start_week, stat_categories FROM league_settings WHERE league_key = ?",
            (league_key,),
        ).fetchone()
    if not row:
        return {}
    return {
        "start_week": row[0],
        "end_week": row[1],
        "playoff_start_week": row[2],
        "stat_categories": row[3],
        "stat_modifiers": row[4] if len(row) > 4 else None,
    }


def get_points_stat_id(settings_json):
    if not settings_json:
        return None
    try:
        stats = json.loads(settings_json)
    except json.JSONDecodeError:
        return None
    for stat in stats:
        name = str(stat.get("name", "")).lower()
        if "points" in name:
            return str(stat.get("stat_id"))
    return None


def parse_stat_modifiers(settings_json):
    if not settings_json:
        return {}
    try:
        modifiers = json.loads(settings_json)
    except json.JSONDecodeError:
        return {}
    parsed = {}
    for item in modifiers:
        stat_id = item.get("stat_id")
        if stat_id is None:
            continue
        value = to_float(item.get("value"))
        if value is None:
            continue
        parsed[str(stat_id)] = value
    return parsed


def load_matchups(conn, league_key):
    rows = conn.execute(
        """
        SELECT m.week, m.matchup_id, m.winner_team_key, m.is_playoffs, m.is_consolation,
               mt.team_key, mt.points, mt.projected_points, mt.win_status
        FROM matchups m
        JOIN matchup_teams mt
          ON m.league_key = mt.league_key
         AND m.week = mt.week
         AND m.matchup_id = mt.matchup_id
        WHERE m.league_key = ?
        """,
        (league_key,),
    ).fetchall()

    matchups = defaultdict(list)
    for row in rows:
        matchups[(row[0], row[1])].append(
            {
                "week": row[0],
                "matchup_id": row[1],
                "winner_team_key": row[2],
                "is_playoffs": row[3],
                "is_consolation": row[4],
                "team_key": row[5],
                "points": to_float(row[6]),
                "projected_points": to_float(row[7]),
                "win_status": row[8],
            }
        )
    return matchups


def build_weekly_points(matchups):
    weekly_points = defaultdict(list)
    weekly_projected = defaultdict(list)
    for (week, _mid), teams in matchups.items():
        for team in teams:
            if team["points"] is not None:
                weekly_points[team["team_key"]].append((week, team["points"]))
            if team["projected_points"] is not None:
                weekly_projected[team["team_key"]].append((week, team["projected_points"]))
    return weekly_points, weekly_projected


def build_records(matchups):
    records = defaultdict(lambda: {"wins": 0, "losses": 0, "ties": 0})
    points_against = defaultdict(float)
    margins = []

    for (week, matchup_id), teams in matchups.items():
        if len(teams) < 2:
            continue
        teams_sorted = sorted(teams, key=lambda t: t.get("points") or -9999, reverse=True)
        top = teams_sorted[0]
        bottom = teams_sorted[1]
        if top["points"] is None or bottom["points"] is None:
            continue

        margin = abs(top["points"] - bottom["points"])
        margins.append(margin)

        if top["points"] > bottom["points"]:
            records[top["team_key"]]["wins"] += 1
            records[bottom["team_key"]]["losses"] += 1
        elif top["points"] < bottom["points"]:
            records[bottom["team_key"]]["wins"] += 1
            records[top["team_key"]]["losses"] += 1
        else:
            records[top["team_key"]]["ties"] += 1
            records[bottom["team_key"]]["ties"] += 1

        points_against[top["team_key"]] += bottom["points"]
        points_against[bottom["team_key"]] += top["points"]

    return records, points_against, margins


def compute_roster_changes(conn, league_key):
    rows = conn.execute(
        "SELECT team_key, week, player_key FROM rosters WHERE league_key = ?",
        (league_key,),
    ).fetchall()

    team_weeks = defaultdict(lambda: defaultdict(set))
    for row in rows:
        team_weeks[row[0]][row[1]].add(row[2])

    changes = {}
    for team_key, weeks in team_weeks.items():
        total_changes = 0
        for week in sorted(weeks.keys()):
            if week - 1 in weeks:
                diff = weeks[week].symmetric_difference(weeks[week - 1])
                total_changes += len(diff)
        changes[team_key] = total_changes
    return changes


def compute_transactions(conn, league_key):
    rows = conn.execute(
        """
        SELECT tp.transaction_key, tp.player_key, tp.transaction_type, tp.source_team_key, tp.destination_team_key, t.type
        FROM transaction_players tp
        JOIN transactions t ON t.transaction_key = tp.transaction_key
        WHERE t.league_key = ?
        """,
        (league_key,),
    ).fetchall()

    waiver_counts = defaultdict(int)
    trade_counts = defaultdict(set)

    for row in rows:
        transaction_key = row[0]
        txn_type = str(row[2] or "").lower()
        source_team = row[3]
        dest_team = row[4]
        overall_type = str(row[5] or "").lower()

        if txn_type in {"add", "drop", "add/drop", "waiver"}:
            if source_team:
                waiver_counts[source_team] += 1
            if dest_team:
                waiver_counts[dest_team] += 1

        if overall_type == "trade" or "trade" in txn_type:
            for team_key in {source_team, dest_team}:
                if team_key:
                    trade_counts[team_key].add(transaction_key)

    trade_counts = {k: len(v) for k, v in trade_counts.items()}
    return waiver_counts, trade_counts


def load_player_points(conn, league_key, stat_id):
    if not stat_id:
        return {}
    rows = conn.execute(
        "SELECT player_key, week, value FROM player_stats WHERE league_key = ? AND stat_id = ?",
        (league_key, stat_id),
    ).fetchall()
    points = {}
    for row in rows:
        value = to_float(row[2])
        if value is None:
            continue
        points[(row[0], row[1])] = value
    return points


def load_player_fantasy_points(conn, league_key, stat_modifiers):
    if not stat_modifiers:
        return {}
    stat_ids = list(stat_modifiers.keys())
    placeholders = ",".join("?" for _ in stat_ids)
    rows = conn.execute(
        f"""
        SELECT player_key, week, stat_id, value
        FROM player_stats
        WHERE league_key = ?
          AND stat_id IN ({placeholders})
        """,
        (league_key, *stat_ids),
    ).fetchall()
    points = defaultdict(float)
    for row in rows:
        value = to_float(row[3])
        if value is None:
            continue
        modifier = stat_modifiers.get(row[2])
        if modifier is None:
            continue
        points[(row[0], row[1])] += value * modifier
    return dict(points)


def load_rosters(conn, league_key):
    try:
        rows = conn.execute(
            """
            SELECT r.team_key, r.week, r.player_key, r.position, r.status, r.injury_status, r.injury_note,
                   p.name_full, p.position
            FROM rosters r
            JOIN players p ON p.player_key = r.player_key
            WHERE r.league_key = ?
            """,
            (league_key,),
        ).fetchall()
        has_injury = True
    except sqlite3.OperationalError:
        rows = conn.execute(
            """
            SELECT r.team_key, r.week, r.player_key, r.position, p.name_full, p.position
            FROM rosters r
            JOIN players p ON p.player_key = r.player_key
            WHERE r.league_key = ?
            """,
            (league_key,),
        ).fetchall()
        has_injury = False

    roster_rows = []
    for row in rows:
        status = row[4] if has_injury else None
        injury_status = row[5] if has_injury else None
        injury_note = row[6] if has_injury else None
        name_index = 7 if has_injury else 4
        position_index = 8 if has_injury else 5
        roster_rows.append(
            {
                "team_key": row[0],
                "week": row[1],
                "player_key": row[2],
                "slot_position": row[3],
                "status": status,
                "injury_status": injury_status,
                "injury_note": injury_note,
                "player_name": row[name_index],
                "player_position": row[position_index],
            }
        )
    return roster_rows


def team_info(team_map, team_key):
    info = team_map.get(team_key, {})
    return {
        "team_key": team_key,
        "team_name": info.get("team_name"),
        "manager_names": info.get("manager_names"),
    }


def player_info(player_key, player_name, player_position):
    return {
        "player_key": player_key,
        "player_name": player_name,
        "player_position": player_position,
    }


def player_info_from_map(player_key, player_map):
    info = player_map.get(player_key, {})
    return {
        "player_key": player_key,
        "player_name": info.get("player_name"),
        "player_position": info.get("player_position"),
    }


def add_missing(missing, insight_id, reason):
    missing.append({"id": insight_id, "reason": reason})


def is_injured(status, injury_status):
    status_value = str(status or "").strip().upper()
    injury_value = str(injury_status or "").strip().upper()
    injury_set = {"IR", "O", "COVID-19", "PUP", "SUSP"}
    if injury_value in injury_set:
        return True
    if status_value in injury_set:
        return True
    return False


def compute_insights_for_league(conn, league_key, season):
    team_map = load_team_map(conn, league_key)
    standings = load_standings(conn, league_key)
    settings = load_league_settings(conn, league_key)
    stat_modifiers = parse_stat_modifiers(settings.get("stat_modifiers"))

    matchups = load_matchups(conn, league_key)
    weekly_points, weekly_projected = build_weekly_points(matchups)
    records, points_against, margins = build_records(matchups)
    roster_changes = compute_roster_changes(conn, league_key)
    waiver_counts, trade_counts = compute_transactions(conn, league_key)
    rosters = load_rosters(conn, league_key)
    player_points = load_player_fantasy_points(conn, league_key, stat_modifiers)
    draft_results = load_draft_results(conn, league_key)
    player_map = load_player_map(conn)
    playoff_start = settings.get("playoff_start_week")
    end_week = settings.get("end_week") or max(
        (w for plist in weekly_points.values() for w, _ in plist),
        default=0,
    )
    reg_weeks_count = (playoff_start - 1) if playoff_start else end_week
    if reg_weeks_count < 1:
        reg_weeks_count = end_week

    avg_points = {}
    if standings and reg_weeks_count:
        for team_key, row in standings.items():
            points_for = row.get("points_for")
            if points_for is None:
                continue
            avg_points[team_key] = points_for / reg_weeks_count
    if not avg_points:
        avg_points = {
            k: statistics.mean([p for _, p in v]) for k, v in weekly_points.items() if v
        }

    playoff_teams = set()
    playoff_weeks = set()
    playoff_matchups = []
    for (week, matchup_id), teams in matchups.items():
        if not teams:
            continue
        is_playoffs = teams[0].get("is_playoffs")
        is_consolation = teams[0].get("is_consolation")
        if is_playoffs == 1 and is_consolation != 1:
            playoff_weeks.add(week)
            playoff_matchups.append((week, matchup_id, teams))
            for team in teams:
                playoff_teams.add(team["team_key"])

    champion_team_key = None
    if playoff_matchups:
        final_week = max(playoff_weeks)
        final_matchups = [m for m in playoff_matchups if m[0] == final_week]
        candidates = []
        for week, matchup_id, teams in final_matchups:
            winner_key = teams[0].get("winner_team_key")
            if not winner_key:
                continue
            winner_points = None
            for team in teams:
                if team["team_key"] == winner_key:
                    winner_points = team["points"]
                    break
            candidates.append((winner_points or -1, winner_key))
        if candidates:
            champion_team_key = max(candidates, key=lambda item: item[0])[1]

    insights = []
    missing = []

    # Performance & Results
    top3_counts = defaultdict(int)
    weekly_scores = defaultdict(list)
    for (week, _mid), teams in matchups.items():
        if playoff_start and week >= playoff_start:
            continue
        for team in teams:
            if team.get("points") is not None:
                weekly_scores[week].append(team)
    for week, teams in weekly_scores.items():
        if playoff_start and week >= playoff_start:
            continue
        scored_sorted = sorted(teams, key=lambda t: t["points"], reverse=True)
        if not scored_sorted:
            continue
        threshold = scored_sorted[min(2, len(scored_sorted) - 1)]["points"]
        for team in scored_sorted:
            if team["points"] >= threshold:
                top3_counts[team["team_key"]] += 1

    if top3_counts:
        team_key = max(top3_counts, key=lambda k: top3_counts[k])
        team_row = standings.get(team_key, {}) if standings else {}
        insights.append(
            {
                "id": "league_champion_dna",
                "title": "League Champion DNA",
                "metric": {
                    "top3_weeks": top3_counts[team_key],
                    "wins": team_row.get("wins"),
                    "losses": team_row.get("losses"),
                    "avg_points": round(avg_points.get(team_key, 0), 2),
                },
                "team": team_info(team_map, team_key),
            }
        )

    if standings and avg_points:
        def win_pct(item):
            row = standings[item]
            games = row["wins"] + row["losses"] + row["ties"]
            return row["wins"] / games if games else 0

        best_pct = max(win_pct(k) for k in standings)
        contenders = [k for k in standings if win_pct(k) == best_pct]
        team_key = min(contenders, key=lambda k: avg_points.get(k, 9999))
        insights.append(
            {
                "id": "paper_tiger",
                "title": "Paper Tiger Award",
                "metric": {
                    "wins": standings[team_key]["wins"],
                    "losses": standings[team_key]["losses"],
                    "avg_points": round(avg_points.get(team_key, 0), 2),
                },
                "team": team_info(team_map, team_key),
            }
        )
    elif records and avg_points:
        def win_pct(item):
            rec = records[item]
            games = rec["wins"] + rec["losses"] + rec["ties"]
            return rec["wins"] / games if games else 0

        best_pct = max(win_pct(k) for k in records)
        contenders = [k for k in records if win_pct(k) == best_pct]
        team_key = min(contenders, key=lambda k: avg_points.get(k, 9999))
        insights.append(
            {
                "id": "paper_tiger",
                "title": "Paper Tiger Award",
                "metric": {
                    "wins": records[team_key]["wins"],
                    "losses": records[team_key]["losses"],
                    "avg_points": round(avg_points.get(team_key, 0), 2),
                },
                "team": team_info(team_map, team_key),
            }
        )

    if standings:
        team_key = max(standings, key=lambda k: standings[k].get("points_against") or 0)
        points_against_value = standings[team_key].get("points_against")
        insights.append(
            {
                "id": "unluckiest_manager",
                "title": "Unluckiest Manager",
                "metric": {"points_against": round(points_against_value or 0, 2)},
                "team": team_info(team_map, team_key),
            }
        )
    elif points_against:
        team_key = max(points_against, key=points_against.get)
        insights.append(
            {
                "id": "unluckiest_manager",
                "title": "Unluckiest Manager",
                "metric": {"points_against": round(points_against[team_key], 2)},
                "team": team_info(team_map, team_key),
            }
        )

    if avg_points:
        team_key = max(avg_points, key=avg_points.get)
        insights.append(
            {
                "id": "juggernaut",
                "title": "Juggernaut",
                "metric": {"avg_points": round(avg_points[team_key], 2)},
                "team": team_info(team_map, team_key),
            }
        )

        stdev = {
            k: statistics.pstdev([p for _, p in v]) for k, v in weekly_points.items() if len(v) > 1
        }
        if stdev:
            team_key = min(stdev, key=stdev.get)
            insights.append(
                {
                    "id": "consistent_king",
                    "title": "Consistent King",
                    "metric": {"std_dev": round(stdev[team_key], 2)},
                    "team": team_info(team_map, team_key),
                }
            )
            team_key = max(stdev, key=stdev.get)
            insights.append(
                {
                    "id": "boom_or_bust",
                    "title": "Boom or Bust",
                    "metric": {"std_dev": round(stdev[team_key], 2)},
                    "team": team_info(team_map, team_key),
                }
            )

    # Pain, Chaos & Heartbreak
    closest_loss = None
    highest_loss = None
    blowout = None
    loss_margins = defaultdict(list)

    for (week, matchup_id), teams in matchups.items():
        if playoff_start and week >= playoff_start:
            continue
        if len(teams) < 2:
            continue
        teams_sorted = sorted(teams, key=lambda t: t.get("points") or -9999, reverse=True)
        winner = teams_sorted[0]
        loser = teams_sorted[1]
        if winner["points"] is None or loser["points"] is None:
            continue
        margin = winner["points"] - loser["points"]

        if margin > 0:
            loss_margins[loser["team_key"]].append(margin)
            candidate = {
                "week": week,
                "matchup_id": matchup_id,
                "margin": round(margin, 2),
                "winner": team_info(team_map, winner["team_key"]),
                "loser": team_info(team_map, loser["team_key"]),
                "winner_points": round(winner["points"], 2),
                "loser_points": round(loser["points"], 2),
            }
            if closest_loss is None or margin < closest_loss["margin"]:
                closest_loss = candidate
            if highest_loss is None or loser["points"] > highest_loss["loser_points"]:
                highest_loss = candidate
            if blowout is None or margin > blowout["margin"]:
                blowout = candidate

    if closest_loss:
        insights.append({"id": "soul_crushing_loss", "title": "Soul-Crushing Loss", "metric": closest_loss})
    if highest_loss:
        insights.append({"id": "highest_score_loss", "title": "Highest Score in a Loss", "metric": highest_loss})
    if blowout:
        insights.append({"id": "blowout_victim", "title": "Blowout Victim", "metric": blowout})

    bridesmaid = {
        team_key: margins
        for team_key, margins in loss_margins.items()
        if len(margins) >= 3
    }
    if bridesmaid:
        team_key = min(bridesmaid, key=lambda k: statistics.mean(bridesmaid[k]))
        avg_margin = statistics.mean(bridesmaid[team_key])
        insights.append(
            {
                "id": "always_the_bridesmaid",
                "title": "Always the Bridesmaid",
                "metric": {
                    "avg_margin_loss": round(avg_margin, 2),
                    "losses": len(bridesmaid[team_key]),
                },
                "team": team_info(team_map, team_key),
            }
        )

    # Schedule Screwed Me
    schedule_delta = {}
    weekly_avg = defaultdict(list)
    for (week, _mid), teams in matchups.items():
        if playoff_start and week >= playoff_start:
            continue
        week_points = [t["points"] for t in teams if t.get("points") is not None]
        if not week_points:
            continue
        avg = statistics.mean(week_points)
        weekly_avg[week] = avg

    if standings:
        actual_wins = {k: v["wins"] for k, v in standings.items()}
    else:
        actual_wins = {k: v["wins"] for k, v in records.items()}
    hypothetical_wins = defaultdict(int)
    for team_key, points_list in weekly_points.items():
        for week, points in points_list:
            if week not in weekly_avg:
                continue
            if points >= weekly_avg[week]:
                hypothetical_wins[team_key] += 1

    for team_key in actual_wins:
        delta = hypothetical_wins.get(team_key, 0) - actual_wins.get(team_key, 0)
        schedule_delta[team_key] = delta

    if schedule_delta:
        team_key = max(schedule_delta, key=schedule_delta.get)
        if schedule_delta[team_key] > 0:
            insights.append(
                {
                    "id": "schedule_screwed_me",
                    "title": "The Schedule Screwed Me",
                    "metric": {
                        "actual_wins": actual_wins.get(team_key, 0),
                        "hypothetical_wins": hypothetical_wins.get(team_key, 0),
                        "delta": schedule_delta[team_key],
                        "hypothetical_rule": "Win if weekly score beats league average.",
                    },
                    "team": team_info(team_map, team_key),
                }
            )

    # Manager Tendencies
    if roster_changes:
        team_key = min(roster_changes, key=roster_changes.get)
        insights.append(
            {
                "id": "ride_or_die",
                "title": "Ride or Die",
                "metric": {"roster_changes": roster_changes[team_key]},
                "team": team_info(team_map, team_key),
            }
        )

        team_key = max(roster_changes, key=roster_changes.get)
        insights.append(
            {
                "id": "fantasy_sicko",
                "title": "Fantasy Sicko Award",
                "metric": {"roster_changes": roster_changes[team_key]},
                "team": team_info(team_map, team_key),
            }
        )

    if waiver_counts:
        team_key = max(waiver_counts, key=waiver_counts.get)
        insights.append(
            {
                "id": "waiver_wire_addict",
                "title": "Waiver Wire Addict",
                "metric": {"moves": waiver_counts[team_key]},
                "team": team_info(team_map, team_key),
            }
        )

    if trade_counts:
        team_key = max(trade_counts, key=trade_counts.get)
        insights.append(
            {
                "id": "trade_machine",
                "title": "Trade Machine",
                "metric": {"trades": trade_counts[team_key]},
                "team": team_info(team_map, team_key),
            }
        )

    if draft_results and rosters:
        final_week = settings.get("end_week") or max((row["week"] for row in rosters), default=None)
        final_rosters = defaultdict(set)
        for row in rosters:
            if final_week is None or row["week"] != final_week:
                continue
            final_rosters[row["team_key"]].add(row["player_key"])

        drafted_by_team = defaultdict(set)
        for row in draft_results:
            if row.get("team_key") and row.get("player_key"):
                drafted_by_team[row["team_key"]].add(row["player_key"])

        loyal_counts = {}
        for team_key, drafted_players in drafted_by_team.items():
            still_rostered = drafted_players.intersection(final_rosters.get(team_key, set()))
            loyal_counts[team_key] = (len(still_rostered), len(drafted_players))

        if loyal_counts:
            team_key = max(loyal_counts, key=lambda k: loyal_counts[k][0])
            still_rostered, drafted_total = loyal_counts[team_key]
            percent = (still_rostered / drafted_total) if drafted_total else 0
            insights.append(
                {
                    "id": "draft_loyalist",
                    "title": "Draft Loyalist",
                    "metric": {
                        "drafted_players": drafted_total,
                        "still_rostered": still_rostered,
                        "percent": round(percent * 100, 2),
                    },
                    "team": team_info(team_map, team_key),
                }
            )
            least_team_key = min(loyal_counts, key=lambda k: loyal_counts[k][0])
            least_still, least_total = loyal_counts[least_team_key]
            percent = (least_still / least_total) if least_total else 0
            insights.append(
                {
                    "id": "commitment_issues",
                    "title": "The Commitment Issues Trophy",
                    "metric": {
                        "drafted_players": least_total,
                        "still_rostered": least_still,
                        "percent": round(percent * 100, 2),
                    },
                    "team": team_info(team_map, least_team_key),
                }
            )
    else:
        add_missing(missing, "draft_loyalist", "Draft data not captured yet.")


    # Draft & Value
    draft_picks = [
        row for row in draft_results
        if row.get("player_key") and row.get("round") is not None and row.get("pick") is not None
    ]
    draft_pick_by_player = {}
    draft_rank_by_player = {}
    if draft_picks:
        for idx, row in enumerate(sorted(draft_picks, key=lambda r: (r["round"], r["pick"])), start=1):
            player_key = row["player_key"]
            if player_key in draft_rank_by_player:
                continue
            draft_rank_by_player[player_key] = idx
            draft_pick_by_player[player_key] = row

    if not draft_results or not draft_rank_by_player:
        add_missing(missing, "draft_steal", "Draft data not captured yet.")
        add_missing(missing, "draft_bust", "Draft data not captured yet.")
        add_missing(missing, "reached_and_regretted", "Draft data not captured yet.")
        add_missing(missing, "late_round_wizardry", "Draft data not captured yet.")
    elif not player_points:
        add_missing(missing, "draft_steal", "Player scoring modifiers missing.")
        add_missing(missing, "draft_bust", "Player scoring modifiers missing.")
        add_missing(missing, "reached_and_regretted", "Player scoring modifiers missing.")
        add_missing(missing, "late_round_wizardry", "Player scoring modifiers missing.")
    else:
        player_totals = defaultdict(float)
        for (player_key, _week), points in player_points.items():
            player_totals[player_key] += points

        season_rank = {
            player_key: idx + 1
            for idx, (player_key, _total) in enumerate(
                sorted(player_totals.items(), key=lambda item: item[1], reverse=True)
            )
        }

        deltas = []
        for player_key, draft_rank in draft_rank_by_player.items():
            if player_key not in season_rank:
                continue
            deltas.append(
                {
                    "player_key": player_key,
                    "draft_rank": draft_rank,
                    "season_rank": season_rank[player_key],
                    "delta": draft_rank - season_rank[player_key],
                }
            )

        if deltas:
            best = max(deltas, key=lambda item: item["delta"])
            player_key = best["player_key"]
            pick = draft_pick_by_player.get(player_key, {})
            insights.append(
                {
                    "id": "draft_steal",
                    "title": "Draft Steal of the Year",
                    "metric": {
                        "draft_rank": best["draft_rank"],
                        "season_rank": best["season_rank"],
                        "delta": best["delta"],
                        "season_points": round(player_totals.get(player_key, 0), 2),
                        "round": pick.get("round"),
                        "pick": pick.get("pick"),
                    },
                    "team": team_info(team_map, pick.get("team_key")),
                    "player": player_info_from_map(player_key, player_map),
                }
            )

            worst = min(deltas, key=lambda item: item["delta"])
            player_key = worst["player_key"]
            pick = draft_pick_by_player.get(player_key, {})
            insights.append(
                {
                    "id": "reached_and_regretted",
                    "title": "Reached and Regretted",
                    "metric": {
                        "draft_rank": worst["draft_rank"],
                        "season_rank": worst["season_rank"],
                        "delta": worst["delta"],
                        "season_points": round(player_totals.get(player_key, 0), 2),
                        "round": pick.get("round"),
                        "pick": pick.get("pick"),
                    },
                    "team": team_info(team_map, pick.get("team_key")),
                    "player": player_info_from_map(player_key, player_map),
                }
            )
        else:
            add_missing(missing, "draft_steal", "No overlapping draft + season stats.")
            add_missing(missing, "reached_and_regretted", "No overlapping draft + season stats.")

        first_round = [row for row in draft_picks if row.get("round") == 1]
        top_round = first_round
        if not top_round:
            top_round = draft_picks[: max(1, len(team_map))]

        if top_round:
            worst_pick = None
            for row in top_round:
                player_key = row.get("player_key")
                if not player_key:
                    continue
                points = player_totals.get(player_key)
                if points is None:
                    continue
                candidate = (points, player_key, row)
                if worst_pick is None or candidate[0] < worst_pick[0]:
                    worst_pick = candidate

            if worst_pick:
                points, player_key, row = worst_pick
                insights.append(
                    {
                        "id": "draft_bust",
                        "title": "Draft Bust Hall of Fame",
                        "metric": {
                            "round": row.get("round"),
                            "pick": row.get("pick"),
                            "season_points": round(points, 2),
                        },
                        "team": team_info(team_map, row.get("team_key")),
                        "player": player_info_from_map(player_key, player_map),
                    }
                )
            else:
                add_missing(missing, "draft_bust", "No draft picks with season points.")
        else:
            add_missing(missing, "draft_bust", "No draft picks to evaluate.")

        rounds = [row.get("round") for row in draft_picks if row.get("round") is not None]
        max_round = max(rounds) if rounds else None
        if max_round:
            late_round = 10 if max_round >= 10 else max(1, (max_round // 2) + 1)
            late_round_picks = [row for row in draft_picks if row.get("round") and row.get("round") >= late_round]
            best_pick = None
            for row in late_round_picks:
                player_key = row.get("player_key")
                if not player_key:
                    continue
                points = player_totals.get(player_key)
                if points is None:
                    continue
                candidate = (points, player_key, row)
                if best_pick is None or candidate[0] > best_pick[0]:
                    best_pick = candidate

            if best_pick:
                points, player_key, row = best_pick
                insights.append(
                    {
                        "id": "late_round_wizardry",
                        "title": "Late-Round Wizardry",
                        "metric": {
                            "round": row.get("round"),
                            "pick": row.get("pick"),
                            "season_points": round(points, 2),
                            "late_round_threshold": late_round,
                        },
                        "team": team_info(team_map, row.get("team_key")),
                        "player": player_info_from_map(player_key, player_map),
                    }
                )
            else:
                add_missing(missing, "late_round_wizardry", "No late-round picks with season points.")
        else:
            add_missing(missing, "late_round_wizardry", "No draft rounds to evaluate.")

    # Start/Sit Decisions
    if not player_points:
        add_missing(missing, "bench_war_crime", "Player scoring modifiers missing.")
        add_missing(missing, "set_and_forget", "Player scoring modifiers missing.")
        add_missing(missing, "overthinker", "Player scoring modifiers missing.")
    else:
        bench_scores = defaultdict(lambda: {"points": -1, "player": None})
        starter_min = defaultdict(lambda: {"points": None, "player": None})
        starts_by_player = defaultdict(int)
        start_points = defaultdict(list)
        bench_points_by_player_team = defaultdict(float)
        bench_weeks_by_player_team = defaultdict(int)

        for row in rosters:
            points = player_points.get((row["player_key"], row["week"]))
            if points is None:
                continue
            key = (row["team_key"], row["week"])
            if row["slot_position"] in BENCH_POSITIONS:
                bench_key = (row["player_key"], row["player_name"], row["player_position"], row["team_key"])
                bench_points_by_player_team[bench_key] += points
                bench_weeks_by_player_team[bench_key] += 1
                if points > bench_scores[key]["points"]:
                    bench_scores[key] = {"points": points, "player": row}
            else:
                if starter_min[key]["points"] is None or points < starter_min[key]["points"]:
                    starter_min[key] = {"points": points, "player": row}
                start_key = (row["team_key"], row["player_key"], row["player_name"], row["player_position"])
                starts_by_player[start_key] += 1
                start_points[start_key].append(points)

        if bench_scores:
            best = max(bench_scores.items(), key=lambda item: item[1]["points"])
            (team_key, week), data = best
            if data["player"]:
                insights.append(
                    {
                        "id": "bench_war_crime",
                        "title": "Bench War Crime",
                        "metric": {
                            "week": week,
                            "points": round(data["points"], 2),
                        },
                        "team": team_info(team_map, team_key),
                        "player": player_info(
                            data["player"]["player_key"],
                            data["player"]["player_name"],
                            data["player"]["player_position"],
                        ),
                    }
                )

        if starts_by_player:
            best_count = max(starts_by_player.values())
            contenders = [k for k, v in starts_by_player.items() if v == best_count]
            if len(contenders) > 1:
                best = max(
                    contenders,
                    key=lambda k: statistics.mean(start_points.get(k, [0])) if start_points.get(k) else 0,
                )
            else:
                best = contenders[0]
            team_key, player_key, player_name, player_position = best
            insights.append(
                {
                    "id": "set_and_forget",
                    "title": "Set-and-Forget Legend",
                    "metric": {
                        "starts": best_count,
                        "avg_weekly_score": round(
                            statistics.mean(start_points.get(best, [0])),
                            2,
                        ),
                    },
                    "team": team_info(team_map, team_key),
                    "player": player_info(player_key, player_name, player_position),
                }
            )

        # Overthinker
        overthinker_counts = defaultdict(int)
        for (week, matchup_id), teams in matchups.items():
            if len(teams) < 2:
                continue
            teams_sorted = sorted(teams, key=lambda t: t.get("points") or -9999, reverse=True)
            winner = teams_sorted[0]
            loser = teams_sorted[1]
            if winner["points"] is None or loser["points"] is None:
                continue
            margin = winner["points"] - loser["points"]
            key = (loser["team_key"], week)
            bench = bench_scores.get(key)
            starter = starter_min.get(key)
            if bench and starter and bench["points"] > starter["points"] and bench["points"] - starter["points"] > margin:
                overthinker_counts[loser["team_key"]] += 1

        if overthinker_counts:
            team_key = max(overthinker_counts, key=overthinker_counts.get)
            insights.append(
                {
                    "id": "overthinker",
                    "title": "Overthinker",
                    "metric": {"games": overthinker_counts[team_key]},
                    "team": team_info(team_map, team_key),
                }
            )

        if bench_points_by_player_team:
            best = max(bench_points_by_player_team.items(), key=lambda item: item[1])
            player_key, player_name, player_position, team_key = best[0]
            insights.append(
                {
                    "id": "why_dont_he_want_me",
                    "title": "Why Don't He Want Me, Man?",
                    "metric": {
                        "bench_points": round(best[1], 2),
                        "bench_weeks": bench_weeks_by_player_team.get(best[0], 0),
                    },
                    "player": player_info(player_key, player_name, player_position),
                    "team": team_info(team_map, team_key),
                }
            )

    # Player Stats
    if player_points and rosters:
        roster_counts = defaultdict(int)
        roster_teams = defaultdict(set)
        for row in rosters:
            key = (row["player_key"], row["player_name"], row["player_position"])
            roster_counts[key] += 1
            roster_teams[key].add(row["team_key"])

        favorite = max(
            roster_teams.items(),
            key=lambda item: (len(item[1]), roster_counts.get(item[0], 0)),
        )
        favorite_teams = [
            team_info(team_map, team_key)
            for team_key in sorted(roster_teams.get(favorite[0], []))
        ]
        insights.append(
            {
                "id": "favorite_player",
                "title": "Favorite Player",
                "metric": {
                    "unique_teams": len(favorite[1]),
                    "roster_appearances": roster_counts.get(favorite[0], 0),
                },
                "player": player_info(favorite[0][0], favorite[0][1], favorite[0][2]),
                "teams": favorite_teams,
            }
        )

        emotional_counts = defaultdict(int)
        emotional_points = defaultdict(list)
        for row in rosters:
            if row["slot_position"] in BENCH_POSITIONS:
                continue
            key = (row["team_key"], row["player_key"], row["player_name"], row["player_position"])
            emotional_counts[key] += 1
            points = player_points.get((row["player_key"], row["week"]))
            if points is not None:
                emotional_points[key].append(points)

        if emotional_counts:
            best_count = max(emotional_counts.values())
            contenders = [k for k, v in emotional_counts.items() if v == best_count]
            if len(contenders) > 1:
                best = max(
                    contenders,
                    key=lambda k: statistics.mean(emotional_points.get(k, [0])),
                )
            else:
                best = contenders[0]
            team_key, player_key, player_name, player_position = best
            insights.append(
                {
                    "id": "emotional_support",
                    "title": "Emotional Support Player",
                    "metric": {
                        "starts": best_count,
                        "avg_weekly_score": round(
                            statistics.mean(emotional_points.get(best, [0])),
                            2,
                        ),
                    },
                    "team": team_info(team_map, team_key),
                    "player": player_info(player_key, player_name, player_position),
                }
            )


    # Weekly & Seasonal Storylines
    if weekly_points:
        all_scores = []
        for team_key, points_list in weekly_points.items():
            for week, points in points_list:
                all_scores.append((points, team_key, week))
        all_scores_sorted = sorted(all_scores, key=lambda item: item[0])
        if all_scores_sorted:
            low = all_scores_sorted[0]
            high = all_scores_sorted[-1]
            insights.append(
                {
                    "id": "rock_bottom",
                    "title": "Rock Bottom",
                    "metric": {"week": low[2], "points": round(low[0], 2)},
                    "team": team_info(team_map, low[1]),
                }
            )
            insights.append(
                {
                    "id": "peak_week",
                    "title": "Peak Week",
                    "metric": {"week": high[2], "points": round(high[0], 2)},
                    "team": team_info(team_map, high[1]),
                }
            )

    if weekly_points:
        end_week = settings.get("end_week") or max((w for plist in weekly_points.values() for w, _ in plist), default=0)
        split_week = max(1, end_week // 2)
        improvements = {}
        for team_key, points_list in weekly_points.items():
            first = [p for w, p in points_list if w <= split_week]
            second = [p for w, p in points_list if w > split_week]
            if not first or not second:
                continue
            improvements[team_key] = statistics.mean(second) - statistics.mean(first)

        if improvements:
            team_key = max(improvements, key=improvements.get)
            insights.append(
                {
                    "id": "mid_season_glow_up",
                    "title": "Mid-Season Glow-Up",
                    "metric": {"points_diff": round(improvements[team_key], 2)},
                    "team": team_info(team_map, team_key),
                }
            )

            team_key = min(improvements, key=improvements.get)
            insights.append(
                {
                    "id": "late_season_collapse",
                    "title": "Late-Season Collapse",
                    "metric": {"points_diff": round(improvements[team_key], 2)},
                    "team": team_info(team_map, team_key),
                }
            )

    # Fun Awards
    if weekly_projected:
        week_projection = defaultdict(dict)
        for team_key, points_list in weekly_projected.items():
            for week, points in points_list:
                week_projection[(team_key, week)]["projected"] = points
        for team_key, points_list in weekly_points.items():
            for week, points in points_list:
                week_projection[(team_key, week)]["actual"] = points

        best = None
        for (team_key, week), values in week_projection.items():
            if playoff_start and week >= playoff_start:
                continue
            if "projected" not in values or "actual" not in values:
                continue
            diff = values["projected"] - values["actual"]
            candidate = {
                "team_key": team_key,
                "week": week,
                "projected": values["projected"],
                "actual": values["actual"],
                "difference": diff,
            }
            if best is None or diff > best["difference"]:
                best = candidate

        if best:
            insights.append(
                {
                    "id": "looked_better_on_paper",
                    "title": "It Looked Better on Paper",
                    "metric": {
                        "week": best["week"],
                        "projected_score": round(best["projected"], 2),
                        "actual_score": round(best["actual"], 2),
                        "difference": round(best["difference"], 2),
                    },
                    "team": team_info(team_map, best["team_key"]),
                }
            )

    if playoff_teams:
        end_week = settings.get("end_week") or max(
            (w for plist in weekly_points.values() for w, _ in plist),
            default=0,
        )
        split_week = max(1, end_week // 2)
        first_half = defaultdict(lambda: {"wins": 0, "losses": 0, "ties": 0})

        for (week, matchup_id), teams in matchups.items():
            if week > split_week or len(teams) < 2:
                continue
            teams_sorted = sorted(teams, key=lambda t: t.get("points") or -9999, reverse=True)
            top = teams_sorted[0]
            bottom = teams_sorted[1]
            if top["points"] is None or bottom["points"] is None:
                continue
            if top["points"] > bottom["points"]:
                first_half[top["team_key"]]["wins"] += 1
                first_half[bottom["team_key"]]["losses"] += 1
            elif top["points"] < bottom["points"]:
                first_half[bottom["team_key"]]["wins"] += 1
                first_half[top["team_key"]]["losses"] += 1
            else:
                first_half[top["team_key"]]["ties"] += 1
                first_half[bottom["team_key"]]["ties"] += 1

        playoff_first_half = {}
        for team_key in playoff_teams:
            record = first_half.get(team_key)
            if not record:
                continue
            games = record["wins"] + record["losses"] + record["ties"]
            playoff_first_half[team_key] = (record, (record["wins"] / games) if games else 0)

        if playoff_first_half:
            team_key = min(playoff_first_half, key=lambda k: playoff_first_half[k][1])
            record, win_pct = playoff_first_half[team_key]
            insights.append(
                {
                    "id": "trust_the_process",
                    "title": "Trust the Process Award",
                    "metric": {
                        "first_half_wins": record["wins"],
                        "first_half_losses": record["losses"],
                        "first_half_ties": record["ties"],
                        "first_half_win_pct": round(win_pct, 3),
                    },
                    "team": team_info(team_map, team_key),
                }
            )
    else:
        add_missing(missing, "trust_the_process", "Playoff data not captured yet.")


    if playoff_teams and standings:
        non_playoff = [
            team_key for team_key in standings.keys()
            if team_key not in playoff_teams
        ]
        if non_playoff:
            team_key = max(
                non_playoff,
                key=lambda k: standings.get(k, {}).get("points_for") or 0,
            )
            insights.append(
                {
                    "id": "well_get_em_next_year",
                    "title": "We'll Get 'Em Next Year",
                    "metric": {
                        "points_for": standings.get(team_key, {}).get("points_for"),
                        "rank": standings.get(team_key, {}).get("rank"),
                    },
                    "team": team_info(team_map, team_key),
                }
            )
    else:
        add_missing(missing, "well_get_em_next_year", "Playoff data not captured yet.")

    # League summary metrics
    league_summary = {}
    if weekly_points:
        total_points = sum(p for points_list in weekly_points.values() for _, p in points_list)
        league_summary["total_points"] = round(total_points, 2)
    league_summary["total_waiver_transactions"] = sum(waiver_counts.values()) if waiver_counts else 0
    league_summary["total_trades"] = sum(trade_counts.values()) if trade_counts else 0
    if margins:
        league_summary["average_margin_of_victory"] = round(statistics.mean(margins), 2)

    if playoff_teams and standings:
        playoff_points = [
            standings.get(team_key, {}).get("points_for")
            for team_key in playoff_teams
            if standings.get(team_key, {}).get("points_for") is not None
        ]
        if playoff_points:
            league_summary["playoff_cutoff_points"] = round(min(playoff_points), 2)

    if champion_team_key and draft_results:
        champion_picks = [
            row for row in draft_results
            if row.get("team_key") == champion_team_key and row.get("round") is not None
        ]
        if champion_picks:
            league_summary["champion_avg_draft_round"] = round(
                statistics.mean([row.get("round") for row in champion_picks]), 2
            )
            picks = [row.get("pick") for row in champion_picks if row.get("pick") is not None]
            if picks:
                league_summary["champion_avg_draft_pick"] = round(statistics.mean(picks), 2)

    if league_summary:
        insights.append(
            {
                "id": "league_summary",
                "title": "League Summary",
                "metric": league_summary,
            }
        )

    if "champion_avg_draft_round" not in league_summary:
        add_missing(missing, "draft_position_champion", "Draft data not captured yet.")
    if "playoff_cutoff_points" not in league_summary:
        add_missing(missing, "average_playoff_cutoff", "Playoff data not captured yet.")

    return {
        "season": season,
        "league_key": league_key,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "insights": insights,
        "missing": missing,
    }


def build_team_games(matchups, team_key, playoff_start):
    games = []
    for (week, matchup_id), teams in matchups.items():
        if playoff_start and week >= playoff_start:
            continue
        if len(teams) < 2:
            continue
        team_entry = next(
            (team for team in teams if team["team_key"] == team_key), None
        )
        if not team_entry:
            continue
        opponent = next(
            (team for team in teams if team["team_key"] != team_key), None
        )
        if not opponent:
            continue
        if team_entry["points"] is None or opponent["points"] is None:
            continue
        if team_entry["points"] > opponent["points"]:
            result = "win"
        elif team_entry["points"] < opponent["points"]:
            result = "loss"
        else:
            result = "tie"
        games.append(
            {
                "week": week,
                "matchup_id": matchup_id,
                "team_points": team_entry["points"],
                "opponent_points": opponent["points"],
                "opponent_key": opponent["team_key"],
                "margin": abs(team_entry["points"] - opponent["points"]),
                "result": result,
            }
        )
    return games


def main():
    parser = argparse.ArgumentParser(description="Generate season insights.")
    parser.add_argument("--league", dest="league_key", help="Generate for a single league key.")
    parser.add_argument("--season", dest="season", help="Generate for a single season.")
    parser.add_argument("--season-start", dest="season_start", help="Generate for seasons >= this year.")
    parser.add_argument("--season-end", dest="season_end", help="Generate for seasons <= this year.")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Missing database: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    leagues = load_leagues(conn)
    if args.season:
        leagues = [l for l in leagues if str(l[1]) == str(args.season)]
    if args.league_key:
        leagues = [l for l in leagues if l[0] == args.league_key]
    if args.season_start:
        leagues = [l for l in leagues if l[1] is not None and int(l[1]) >= int(args.season_start)]
    if args.season_end:
        leagues = [l for l in leagues if l[1] is not None and int(l[1]) <= int(args.season_end)]

    outputs = []
    for league_key, season in leagues:
        insights = compute_insights_for_league(conn, league_key, season)
        outputs.append(insights)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / f"insights_{season}.json"
        output_path.write_text(json.dumps(insights, indent=2), encoding="utf-8")
        print(f"Wrote {output_path}")

    index_path = OUTPUT_DIR / "insights_index.json"
    index_payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "seasons": [out["season"] for out in outputs],
    }
    index_path.write_text(json.dumps(index_payload, indent=2), encoding="utf-8")
    print(f"Wrote {index_path}")


if __name__ == "__main__":
    main()
