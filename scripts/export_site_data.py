import json
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "processed" / "fantasy_insights.sqlite"
SITE_DATA_DIR = BASE_DIR / "site" / "data"


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_mean(values):
    if not values:
        return None
    return statistics.mean(values)


def _safe_stdev(values):
    if len(values) < 2:
        return 0.0
    return statistics.pstdev(values)


def _team_payload(team_info, team_key):
    info = team_info.get(team_key, {})
    return {
        "team_key": team_key,
        "team_name": info.get("team_name"),
        "manager_names": info.get("manager_names"),
    }


def _win_pct(record):
    total = record["wins"] + record["losses"] + record["ties"]
    if total == 0:
        return 0.0
    return (record["wins"] + 0.5 * record["ties"]) / total


def _matchup_result(teams):
    if len(teams) != 2:
        return None
    a, b = teams[0], teams[1]
    status_a = (a.get("win_status") or "").lower()
    status_b = (b.get("win_status") or "").lower()
    if status_a == "tie" or status_b == "tie":
        return {"tie": True}
    if status_a == "win":
        return {"winner": a["team_key"], "loser": b["team_key"], "tie": False}
    if status_b == "win":
        return {"winner": b["team_key"], "loser": a["team_key"], "tie": False}
    points_a = a.get("points")
    points_b = b.get("points")
    if points_a is None or points_b is None:
        return None
    if points_a == points_b:
        return {"tie": True}
    if points_a > points_b:
        return {"winner": a["team_key"], "loser": b["team_key"], "tie": False}
    return {"winner": b["team_key"], "loser": a["team_key"], "tie": False}


def export_table(conn, table, output_path):
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    data = [dict(row) for row in rows]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def export_league_summary(conn, output_path):
    leagues = conn.execute("SELECT league_key, season FROM leagues").fetchall()
    league_seasons = {row["league_key"]: row["season"] for row in leagues}

    teams = conn.execute(
        "SELECT team_key, league_key, name, manager_names FROM teams"
    ).fetchall()
    team_info_by_league = defaultdict(dict)
    for row in teams:
        team_info_by_league[row["league_key"]][row["team_key"]] = {
            "team_name": row["name"],
            "manager_names": row["manager_names"],
        }

    transaction_rows = conn.execute(
        """
        SELECT t.league_key, tp.transaction_key, tp.transaction_type, tp.source_team_key, tp.destination_team_key
        FROM transaction_players tp
        JOIN transactions t ON t.transaction_key = tp.transaction_key
        """
    ).fetchall()

    waiver_counts = {}
    move_sets = {}
    for row in transaction_rows:
        league_key = row["league_key"]
        txn_key = row["transaction_key"]
        txn_type = (row["transaction_type"] or "").lower()
        teams = {row["source_team_key"], row["destination_team_key"]}
        teams.discard(None)
        for team_key in teams:
            move_sets.setdefault((league_key, team_key), set()).add(txn_key)
            if txn_type in {"add", "drop", "add/drop", "waiver"}:
                waiver_counts[(league_key, team_key)] = waiver_counts.get((league_key, team_key), 0) + 1

    matchups = conn.execute(
        "SELECT league_key, week, matchup_id, is_playoffs, is_consolation FROM matchups"
    ).fetchall()
    matchup_meta = defaultdict(dict)
    for row in matchups:
        matchup_meta[row["league_key"]][(row["week"], row["matchup_id"])] = {
            "is_playoffs": row["is_playoffs"],
            "is_consolation": row["is_consolation"],
        }

    matchup_teams = conn.execute(
        "SELECT league_key, week, matchup_id, team_key, points FROM matchup_teams"
    ).fetchall()
    matchup_teams_by_league = defaultdict(lambda: defaultdict(list))
    for row in matchup_teams:
        matchup_teams_by_league[row["league_key"]][(row["week"], row["matchup_id"])].append(
            {"team_key": row["team_key"], "points": _to_float(row["points"])}
        )

    summary_rows = []
    for league_key, season in league_seasons.items():
        team_info = team_info_by_league.get(league_key, {})
        records = {
            team_key: {
                "wins": 0,
                "losses": 0,
                "ties": 0,
                "points_for": 0.0,
                "points_against": 0.0,
            }
            for team_key in team_info
        }

        for (week, matchup_id), teams in matchup_teams_by_league.get(league_key, {}).items():
            meta = matchup_meta.get(league_key, {}).get((week, matchup_id), {})
            if meta.get("is_playoffs") == 1:
                continue
            if len(teams) != 2:
                continue
            team_a, team_b = teams
            points_a = team_a.get("points")
            points_b = team_b.get("points")
            if points_a is None or points_b is None:
                continue

            records.setdefault(team_a["team_key"], {"wins": 0, "losses": 0, "ties": 0, "points_for": 0.0, "points_against": 0.0})
            records.setdefault(team_b["team_key"], {"wins": 0, "losses": 0, "ties": 0, "points_for": 0.0, "points_against": 0.0})
            records[team_a["team_key"]]["points_for"] += points_a
            records[team_a["team_key"]]["points_against"] += points_b
            records[team_b["team_key"]]["points_for"] += points_b
            records[team_b["team_key"]]["points_against"] += points_a

            if points_a > points_b:
                records[team_a["team_key"]]["wins"] += 1
                records[team_b["team_key"]]["losses"] += 1
            elif points_b > points_a:
                records[team_b["team_key"]]["wins"] += 1
                records[team_a["team_key"]]["losses"] += 1
            else:
                records[team_a["team_key"]]["ties"] += 1
                records[team_b["team_key"]]["ties"] += 1

        def win_pct(record):
            games = record["wins"] + record["losses"] + record["ties"]
            if not games:
                return 0
            return (record["wins"] + 0.5 * record["ties"]) / games

        sorted_teams = sorted(
            records.items(),
            key=lambda item: (
                win_pct(item[1]),
                item[1]["points_for"],
                -item[1]["points_against"],
            ),
            reverse=True,
        )

        rank_lookup = {}
        for idx, (team_key, _record) in enumerate(sorted_teams, start=1):
            rank_lookup[team_key] = idx

        for team_key, record in records.items():
            info = team_info.get(team_key, {})
            summary_rows.append(
                {
                    "season": season,
                    "league_key": league_key,
                    "team_key": team_key,
                    "team_name": info.get("team_name"),
                    "manager_names": info.get("manager_names"),
                    "rank": rank_lookup.get(team_key),
                    "wins": record["wins"],
                    "losses": record["losses"],
                    "ties": record["ties"],
                    "points_for": round(record["points_for"], 2),
                    "points_against": round(record["points_against"], 2),
                    "waiver_moves": waiver_counts.get((league_key, team_key), 0),
                    "total_moves": len(move_sets.get((league_key, team_key), set())),
                }
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary_rows, indent=2), encoding="utf-8")


def export_league_overview(conn, output_path):
    leagues = conn.execute("SELECT league_key, season FROM leagues").fetchall()
    league_seasons = {row["league_key"]: row["season"] for row in leagues}

    teams = conn.execute(
        "SELECT team_key, league_key, name, manager_names FROM teams"
    ).fetchall()
    team_info_by_league = defaultdict(dict)
    for row in teams:
        team_info_by_league[row["league_key"]][row["team_key"]] = {
            "team_name": row["name"],
            "manager_names": row["manager_names"],
        }

    standings = conn.execute(
        "SELECT league_key, team_key, rank, wins, losses, ties, points_for, points_against FROM standings"
    ).fetchall()
    standings_by_league = defaultdict(dict)
    for row in standings:
        standings_by_league[row["league_key"]][row["team_key"]] = dict(row)

    matchups = conn.execute(
        "SELECT league_key, week, matchup_id, is_playoffs, is_consolation, winner_team_key FROM matchups"
    ).fetchall()
    matchup_flags = defaultdict(dict)
    for row in matchups:
        matchup_flags[row["league_key"]][(row["week"], row["matchup_id"])] = {
            "is_playoffs": row["is_playoffs"],
            "is_consolation": row["is_consolation"],
            "winner_team_key": row["winner_team_key"],
        }

    matchup_teams = conn.execute(
        "SELECT league_key, week, matchup_id, team_key, points, win_status FROM matchup_teams"
    ).fetchall()
    matchup_teams_by_league = defaultdict(lambda: defaultdict(list))
    for row in matchup_teams:
        league_key = row["league_key"]
        key = (row["week"], row["matchup_id"])
        matchup_teams_by_league[league_key][key].append(
            {
                "team_key": row["team_key"],
                "points": _to_float(row["points"]),
                "win_status": row["win_status"],
            }
        )

    transactions = conn.execute(
        "SELECT transaction_key, league_key, type, timestamp FROM transactions"
    ).fetchall()
    transactions_by_league = defaultdict(list)
    for row in transactions:
        transactions_by_league[row["league_key"]].append(dict(row))

    transaction_players = conn.execute(
        "SELECT transaction_key, source_team_key, destination_team_key FROM transaction_players"
    ).fetchall()
    transaction_team_map = defaultdict(set)
    for row in transaction_players:
        if row["source_team_key"]:
            transaction_team_map[row["transaction_key"]].add(row["source_team_key"])
        if row["destination_team_key"]:
            transaction_team_map[row["transaction_key"]].add(row["destination_team_key"])

    overview_rows = []
    for league_key, season in league_seasons.items():
        team_info = team_info_by_league.get(league_key, {})
        standings_rows = standings_by_league.get(league_key, {})
        matchup_map = matchup_teams_by_league.get(league_key, {})
        matchup_meta = matchup_flags.get(league_key, {})

        team_keys = set(team_info.keys()) or set(standings_rows.keys())
        if not team_keys:
            continue

        week_team_points = defaultdict(list)
        weekly_matchups = defaultdict(list)
        points_by_team = defaultdict(float)
        total_points = 0.0
        team_week_count = 0
        margins = []
        all_week_points = []
        playoff_team_keys = set()

        for (week, matchup_id), teams in matchup_map.items():
            meta = matchup_meta.get((week, matchup_id), {})
            is_playoffs = meta.get("is_playoffs", 0)
            is_consolation = meta.get("is_consolation", 0)
            if is_playoffs and not is_consolation:
                for team in teams:
                    playoff_team_keys.add(team["team_key"])
                continue
            weekly_matchups[week].append(teams)
            for team in teams:
                points = team.get("points")
                if points is None:
                    continue
                week_team_points[week].append((team["team_key"], points))
                points_by_team[team["team_key"]] += points
                total_points += points
                team_week_count += 1
                all_week_points.append(points)
            if len(teams) == 2:
                points_a = teams[0].get("points")
                points_b = teams[1].get("points")
                if points_a is not None and points_b is not None:
                    margins.append(abs(points_a - points_b))

        weekly_avg = []
        weekly_leaders = set()
        for week, items in week_team_points.items():
            points_list = [p for _, p in items]
            avg = _safe_mean(points_list)
            if avg is not None:
                weekly_avg.append({"week": week, "avg_points": avg})
            if points_list:
                max_points = max(points_list)
                for team_key, points in items:
                    if points == max_points:
                        weekly_leaders.add(team_key)

        median_wins = defaultdict(int)
        overall_median_score = (
            statistics.median(all_week_points) if all_week_points else None
        )
        for week, items in week_team_points.items():
            points_list = [p for _, p in items]
            if not points_list:
                continue
            week_median_score = statistics.median(points_list)
            for team_key, points in items:
                if points >= week_median_score:
                    median_wins[team_key] += 1

        actual_wins = {}
        for team_key in team_keys:
            row = standings_rows.get(team_key)
            actual_wins[team_key] = row["wins"] if row else 0

        median_leader_key = None
        if median_wins:
            median_leader_key = max(
                median_wins,
                key=lambda k: (median_wins[k], points_by_team.get(k, 0.0)),
            )

        gap_team_key = None
        gap_value = None
        for team_key, wins in actual_wins.items():
            gap = median_wins.get(team_key, 0) - wins
            if gap_value is None or gap > gap_value:
                gap_value = gap
                gap_team_key = team_key

        upset_games = 0
        total_games = 0
        record_map = {
            team_key: {"wins": 0, "losses": 0, "ties": 0} for team_key in team_keys
        }
        points_to_date = defaultdict(float)
        playoff_weeks_in_spot = defaultdict(int)

        playoff_count = len(playoff_team_keys)
        if playoff_count == 0:
            playoff_count = max(4, len(team_keys) // 2)

        for week in sorted(weekly_matchups.keys()):
            week_records = {
                team_key: dict(record) for team_key, record in record_map.items()
            }
            for teams in weekly_matchups[week]:
                result = _matchup_result(teams)
                if not result or result.get("tie"):
                    continue
                winner = result["winner"]
                loser = result["loser"]
                pct_winner = _win_pct(week_records[winner])
                pct_loser = _win_pct(week_records[loser])
                if pct_winner != pct_loser:
                    total_games += 1
                    if pct_winner < pct_loser:
                        upset_games += 1
                else:
                    total_games += 1

            for teams in weekly_matchups[week]:
                result = _matchup_result(teams)
                if not result:
                    continue
                if result.get("tie"):
                    for team in teams:
                        record_map[team["team_key"]]["ties"] += 1
                    continue
                record_map[result["winner"]]["wins"] += 1
                record_map[result["loser"]]["losses"] += 1

            for team_key, points in week_team_points.get(week, []):
                points_to_date[team_key] += points

            standings_snapshot = []
            for team_key in team_keys:
                record = record_map[team_key]
                standings_snapshot.append(
                    (
                        _win_pct(record),
                        points_to_date.get(team_key, 0.0),
                        team_key,
                    )
                )
            standings_snapshot.sort(key=lambda item: (item[0], item[1]), reverse=True)
            for _, _, team_key in standings_snapshot[:playoff_count]:
                playoff_weeks_in_spot[team_key] += 1

        playoff_cutoff = None
        first_out = None
        if standings_rows:
            if playoff_team_keys:
                playoff_ranks = [
                    standings_rows[team_key]
                    for team_key in playoff_team_keys
                    if team_key in standings_rows
                ]
                non_playoff_ranks = [
                    row
                    for team_key, row in standings_rows.items()
                    if team_key not in playoff_team_keys
                ]
            else:
                sorted_rows = sorted(
                    standings_rows.values(), key=lambda row: row["rank"] or 999
                )
                playoff_ranks = sorted_rows[:playoff_count]
                non_playoff_ranks = sorted_rows[playoff_count:]

            if playoff_ranks:
                playoff_cutoff = max(playoff_ranks, key=lambda row: row["rank"] or 0)
            if non_playoff_ranks:
                first_out = min(non_playoff_ranks, key=lambda row: row["rank"] or 999)

        points_gap = None
        if playoff_cutoff and first_out:
            points_gap = (
                _to_float(playoff_cutoff.get("points_for"))
                - _to_float(first_out.get("points_for"))
            )

        transaction_rows = transactions_by_league.get(league_key, [])
        total_transactions = len(transaction_rows)
        total_trades = sum(
            1
            for row in transaction_rows
            if (row.get("type") or "").lower() == "trade"
        )

        weekly_transactions = defaultdict(list)
        for row in transaction_rows:
            ts = row.get("timestamp")
            if not ts:
                continue
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            iso_year, iso_week, _ = dt.isocalendar()
            label = f"{iso_year}-W{iso_week:02d}"
            weekly_transactions[label].append(row["transaction_key"])

        busiest_week = None
        busiest_count = 0
        busiest_teams = 0
        for label, keys in weekly_transactions.items():
            if len(keys) > busiest_count:
                busiest_count = len(keys)
                busiest_week = label
                teams_in_week = set()
                for key in keys:
                    teams_in_week.update(transaction_team_map.get(key, set()))
                busiest_teams = len(teams_in_week)

        def matchup_winner(teams, winner_key):
            if winner_key:
                return winner_key
            if len(teams) == 2:
                points_a = teams[0].get("points")
                points_b = teams[1].get("points")
                if points_a is None or points_b is None:
                    return None
                if points_a == points_b:
                    return None
                return teams[0]["team_key"] if points_a > points_b else teams[1]["team_key"]
            return None

        def build_bracket_rounds(filter_fn):
            rounds = defaultdict(list)
            for (week, matchup_id), teams in matchup_map.items():
                meta = matchup_meta.get((week, matchup_id))
                if not meta or not filter_fn(meta):
                    continue
                if len(teams) < 2:
                    continue
                winner_key = matchup_winner(teams, meta.get("winner_team_key"))
                loser_key = None
                if winner_key:
                    loser_key = next(
                        (team["team_key"] for team in teams if team["team_key"] != winner_key),
                        None,
                    )
                matchup_entry = {
                    "matchup_id": matchup_id,
                    "winner_team_key": winner_key,
                    "loser_team_key": loser_key,
                    "teams": [],
                }
                for team in teams:
                    team_key = team["team_key"]
                    matchup_entry["teams"].append(
                        {
                            "team_key": team_key,
                            "team": _team_payload(team_info, team_key),
                            "points": team.get("points"),
                            "is_winner": team_key == winner_key,
                        }
                    )
                if winner_key:
                    matchup_entry["teams"].sort(
                        key=lambda item: item["team_key"] != winner_key
                    )
                rounds[week].append(matchup_entry)
            for week in rounds:
                rounds[week].sort(key=lambda item: item["matchup_id"])
            return [
                {"week": week, "matchups": rounds[week]}
                for week in sorted(rounds.keys())
            ]

        def compute_bracket_places(rounds, standings_rank):
            if isinstance(rounds, list):
                rounds = {entry["week"]: entry["matchups"] for entry in rounds}
            if not rounds:
                return {}, set()
            weeks = sorted(rounds.keys())
            final_week = weeks[-1]
            prev_week = weeks[-2] if len(weeks) > 1 else None
            prev_winners = set()
            prev_losers = set()
            if prev_week is not None:
                for matchup in rounds[prev_week]:
                    if matchup.get("winner_team_key"):
                        prev_winners.add(matchup["winner_team_key"])
                    if matchup.get("loser_team_key"):
                        prev_losers.add(matchup["loser_team_key"])

            placements = {}

            def assign(team_key, place):
                if team_key and team_key not in placements:
                    placements[team_key] = place

            champ_match = None
            third_match = None
            if rounds.get(final_week):
                if prev_winners:
                    for matchup in rounds[final_week]:
                        team_keys = {team["team_key"] for team in matchup["teams"]}
                        if team_keys and team_keys.issubset(prev_winners):
                            champ_match = matchup
                        elif prev_losers and team_keys.issubset(prev_losers):
                            third_match = matchup
                if champ_match is None and len(rounds[final_week]) == 1:
                    champ_match = rounds[final_week][0]

            if champ_match:
                assign(champ_match.get("winner_team_key"), 1)
                assign(champ_match.get("loser_team_key"), 2)
            if third_match:
                assign(third_match.get("winner_team_key"), 3)
                assign(third_match.get("loser_team_key"), 4)

            next_place = max(placements.values(), default=0) + 1
            for matchup in rounds.get(final_week, []):
                for team in matchup["teams"]:
                    if team["team_key"] not in placements:
                        assign(team["team_key"], next_place)
                        next_place += 1

            all_teams = {
                team["team_key"]
                for week in rounds
                for matchup in rounds[week]
                for team in matchup["teams"]
            }
            remaining = [team for team in all_teams if team not in placements]
            remaining.sort(key=lambda team: standings_rank.get(team, 999))
            for team_key in remaining:
                assign(team_key, next_place)
                next_place += 1

            return placements, all_teams

        def ordinal(value):
            if value is None:
                return None
            if 10 <= value % 100 <= 20:
                suffix = "th"
            else:
                suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
            return f"{value}{suffix}"

        playoff_rounds = build_bracket_rounds(
            lambda meta: meta.get("is_playoffs") == 1
            and meta.get("is_consolation") != 1
        )
        consolation_rounds = build_bracket_rounds(
            lambda meta: meta.get("is_playoffs") == 1
            and meta.get("is_consolation") == 1
        )

        standings_rank = {
            team_key: row.get("rank") or 999
            for team_key, row in standings_rows.items()
        }
        playoff_places_rel, playoff_teams = compute_bracket_places(
            playoff_rounds, standings_rank
        )
        consolation_places_rel, consolation_teams = compute_bracket_places(
            consolation_rounds, standings_rank
        )

        final_places = {}
        for team_key, place in playoff_places_rel.items():
            final_places[team_key] = place

        next_place = max(final_places.values(), default=0) + 1
        for team_key, place in consolation_places_rel.items():
            final_places[team_key] = next_place + place - 1
        if consolation_places_rel:
            next_place = max(final_places.values(), default=0) + 1

        remaining = [team for team in team_keys if team not in final_places]
        remaining.sort(key=lambda team: standings_rank.get(team, 999))
        for team_key in remaining:
            final_places[team_key] = next_place
            next_place += 1

        competitive_balance = {}
        if margins:
            median_margin = statistics.median(margins)
            close_threshold = 10
            close_games = sum(1 for margin in margins if margin <= close_threshold)
            competitive_balance = {
                "median_margin": median_margin,
                "close_games": close_games,
                "close_game_rate": close_games / len(margins) if margins else None,
                "close_threshold": close_threshold,
            }

        overview_rows.append(
            {
                "season": season,
                "league_key": league_key,
                "snapshot": {
                    "total_points": total_points if team_week_count else None,
                    "avg_weekly_points": total_points / team_week_count
                    if team_week_count
                    else None,
                    "avg_margin": _safe_mean(margins),
                    "closest_margin": min(margins) if margins else None,
                    "blowout_margin": max(margins) if margins else None,
                },
                "competitive_balance": competitive_balance,
                "median_record": {
                    "median_score": overall_median_score,
                    "leader": _team_payload(team_info, median_leader_key)
                    if median_leader_key
                    else None,
                    "leader_median_wins": median_wins.get(median_leader_key, 0)
                    if median_leader_key
                    else None,
                    "biggest_gap": gap_value,
                    "biggest_gap_team": _team_payload(team_info, gap_team_key)
                    if gap_team_key
                    else None,
                    "biggest_gap_median_wins": median_wins.get(gap_team_key, 0)
                    if gap_team_key
                    else None,
                    "biggest_gap_actual_wins": actual_wins.get(gap_team_key, 0)
                    if gap_team_key
                    else None,
                },
                "upset_rate": {
                    "upsets": upset_games,
                    "games": total_games,
                    "rate": upset_games / total_games if total_games else None,
                },
                "playoff_bubble": {
                    "playoff_teams": playoff_count,
                    "points_gap": points_gap,
                    "last_seed": {
                        **_team_payload(team_info, playoff_cutoff["team_key"]),
                        "rank": playoff_cutoff["rank"],
                        "points_for": playoff_cutoff["points_for"],
                    }
                    if playoff_cutoff
                    else None,
                    "first_out": {
                        **_team_payload(team_info, first_out["team_key"]),
                        "rank": first_out["rank"],
                        "points_for": first_out["points_for"],
                    }
                    if first_out
                    else None,
                    "weeks_in_spot": [
                        {
                            **_team_payload(team_info, team_key),
                            "weeks": weeks,
                        }
                        for team_key, weeks in sorted(
                            playoff_weeks_in_spot.items(),
                            key=lambda item: item[1],
                            reverse=True,
                        )
                    ],
                },
                "scoring_trend": sorted(
                    weekly_avg, key=lambda entry: entry["week"]
                ),
                "activity_pulse": {
                    "total_transactions": total_transactions,
                    "total_trades": total_trades,
                    "busiest_week": busiest_week,
                    "busiest_transactions": busiest_count or None,
                    "busiest_teams": busiest_teams or None,
                },
                "playoff_bracket": {
                    "rounds": playoff_rounds,
                },
                "consolation_bracket": {
                    "rounds": consolation_rounds,
                },
                "final_placements": [
                    {
                        "team_key": team_key,
                        "final_place": place,
                        "final_label": ordinal(place),
                    }
                    for team_key, place in sorted(final_places.items(), key=lambda item: item[1])
                ],
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(overview_rows, indent=2), encoding="utf-8")


def main():
    if not DB_PATH.exists():
        print(f"Missing database: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    exports = {
        "leagues": SITE_DATA_DIR / "leagues.json",
        "teams": SITE_DATA_DIR / "teams.json",
        "standings": SITE_DATA_DIR / "standings.json",
        "matchups": SITE_DATA_DIR / "matchups.json",
        "matchup_teams": SITE_DATA_DIR / "matchup_teams.json",
        "team_stats": SITE_DATA_DIR / "team_stats.json",
        "transactions": SITE_DATA_DIR / "transactions.json",
    }

    for table, path in exports.items():
        export_table(conn, table, path)
        print(f"Wrote {path}")

    summary_path = SITE_DATA_DIR / "league_summary.json"
    export_league_summary(conn, summary_path)
    print(f"Wrote {summary_path}")

    overview_path = SITE_DATA_DIR / "league_overview.json"
    export_league_overview(conn, overview_path)
    print(f"Wrote {overview_path}")


if __name__ == "__main__":
    main()
