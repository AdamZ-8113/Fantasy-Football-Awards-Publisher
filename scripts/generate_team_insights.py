import argparse
import json
import sqlite3
import statistics
import time
from collections import defaultdict

import generate_insights as gi


def compute_team_insights_for_league(conn, league_key, season):
    team_map = gi.load_team_map(conn, league_key)
    standings = gi.load_standings(conn, league_key)
    settings = gi.load_league_settings(conn, league_key)
    stat_modifiers = gi.parse_stat_modifiers(settings.get("stat_modifiers"))

    matchups = gi.load_matchups(conn, league_key)
    weekly_points, weekly_projected = gi.build_weekly_points(matchups)
    rosters = gi.load_rosters(conn, league_key)
    player_points = gi.load_player_fantasy_points(conn, league_key, stat_modifiers)
    draft_results = gi.load_draft_results(conn, league_key)
    player_map = gi.load_player_map(conn)
    playoff_start = settings.get("playoff_start_week")
    end_week = settings.get("end_week") or max(
        (w for plist in weekly_points.values() for w, _ in plist),
        default=0,
    )

    weekly_avg = {}
    for (week, _mid), teams in matchups.items():
        if playoff_start and week >= playoff_start:
            continue
        week_points = [t["points"] for t in teams if t.get("points") is not None]
        if week_points:
            weekly_avg[week] = statistics.mean(week_points)

    rosters_by_team = defaultdict(list)
    for row in rosters:
        rosters_by_team[row["team_key"]].append(row)

    team_games_map = {
        team_key: gi.build_team_games(matchups, team_key, playoff_start)
        for team_key in team_map
    }
    team_wins_map = {
        team_key: sum(1 for game in games if game["result"] == "win")
        for team_key, games in team_games_map.items()
    }
    team_points_for_map = {
        team_key: sum(game["team_points"] for game in games)
        for team_key, games in team_games_map.items()
    }

    player_totals = defaultdict(float)
    if player_points:
        for (player_key, _week), points in player_points.items():
            player_totals[player_key] += points

    season_rank = {
        player_key: idx + 1
        for idx, (player_key, _total) in enumerate(
            sorted(player_totals.items(), key=lambda item: item[1], reverse=True)
        )
    }

    draft_picks = [
        row
        for row in draft_results
        if row.get("player_key")
        and row.get("round") is not None
        and row.get("pick") is not None
    ]
    draft_rank_by_player = {}
    if draft_picks:
        for idx, row in enumerate(
            sorted(draft_picks, key=lambda r: (r["round"], r["pick"])),
            start=1,
        ):
            player_key = row["player_key"]
            if player_key in draft_rank_by_player:
                continue
            draft_rank_by_player[player_key] = idx

    rounds = [row.get("round") for row in draft_picks if row.get("round") is not None]
    max_round = max(rounds) if rounds else None
    late_round = None
    if max_round:
        late_round = 10 if max_round >= 10 else max(1, (max_round // 2) + 1)

    playoff_teams = set()
    for (_week, _mid), teams in matchups.items():
        if not teams:
            continue
        is_playoffs = teams[0].get("is_playoffs")
        is_consolation = teams[0].get("is_consolation")
        if is_playoffs == 1 and is_consolation != 1:
            for team in teams:
                playoff_teams.add(team["team_key"])

    team_payloads = []
    for team_key in sorted(team_map.keys()):
        insights = []
        missing = []

        team_games = team_games_map.get(team_key, [])
        team_losses = [game for game in team_games if game["result"] == "loss"]
        team_wins = team_wins_map.get(team_key, 0)

        # Pain, Chaos & Heartbreak
        if team_losses:
            closest = min(team_losses, key=lambda game: game["margin"])
            insights.append(
                {
                    "id": "soul_crushing_loss",
                    "title": "Soul-Crushing Loss",
                    "metric": {
                        "week": closest["week"],
                        "matchup_id": closest["matchup_id"],
                        "margin": round(closest["margin"], 2),
                        "winner": gi.team_info(team_map, closest["opponent_key"]),
                        "loser": gi.team_info(team_map, team_key),
                        "winner_points": round(closest["opponent_points"], 2),
                        "loser_points": round(closest["team_points"], 2),
                    },
                    "team": gi.team_info(team_map, team_key),
                }
            )

            highest_loss = max(team_losses, key=lambda game: game["team_points"])
            insights.append(
                {
                    "id": "highest_score_loss",
                    "title": "Highest Score in a Loss",
                    "metric": {
                        "week": highest_loss["week"],
                        "matchup_id": highest_loss["matchup_id"],
                        "margin": round(highest_loss["margin"], 2),
                        "winner": gi.team_info(team_map, highest_loss["opponent_key"]),
                        "loser": gi.team_info(team_map, team_key),
                        "winner_points": round(highest_loss["opponent_points"], 2),
                        "loser_points": round(highest_loss["team_points"], 2),
                    },
                    "team": gi.team_info(team_map, team_key),
                }
            )

            blowout = max(team_losses, key=lambda game: game["margin"])
            insights.append(
                {
                    "id": "blowout_victim",
                    "title": "Blowout Victim",
                    "metric": {
                        "week": blowout["week"],
                        "matchup_id": blowout["matchup_id"],
                        "margin": round(blowout["margin"], 2),
                        "winner": gi.team_info(team_map, blowout["opponent_key"]),
                        "loser": gi.team_info(team_map, team_key),
                        "winner_points": round(blowout["opponent_points"], 2),
                        "loser_points": round(blowout["team_points"], 2),
                    },
                    "team": gi.team_info(team_map, team_key),
                }
            )
        else:
            gi.add_missing(missing, "soul_crushing_loss", "No regular-season losses.")
            gi.add_missing(missing, "highest_score_loss", "No regular-season losses.")
            gi.add_missing(missing, "blowout_victim", "No regular-season losses.")

        if len(team_losses) >= 3:
            avg_margin = statistics.mean([game["margin"] for game in team_losses])
            insights.append(
                {
                    "id": "always_the_bridesmaid",
                    "title": "Always the Bridesmaid",
                    "metric": {
                        "avg_margin_loss": round(avg_margin, 2),
                        "losses": len(team_losses),
                    },
                    "team": gi.team_info(team_map, team_key),
                }
            )
        else:
            gi.add_missing(missing, "always_the_bridesmaid", "Fewer than 3 losses.")

        if weekly_avg and weekly_points.get(team_key):
            hypothetical_wins = 0
            for week, points in weekly_points.get(team_key, []):
                if week not in weekly_avg:
                    continue
                if points >= weekly_avg[week]:
                    hypothetical_wins += 1
            delta = hypothetical_wins - team_wins
            if delta > 0:
                insights.append(
                    {
                        "id": "schedule_screwed_me",
                        "title": "The Schedule Screwed Me",
                        "metric": {
                            "actual_wins": team_wins,
                            "hypothetical_wins": hypothetical_wins,
                            "delta": delta,
                            "hypothetical_rule": "Win if weekly score beats league average.",
                        },
                        "team": gi.team_info(team_map, team_key),
                    }
                )
            else:
                gi.add_missing(
                    missing, "schedule_screwed_me", "Schedule was neutral or favorable."
                )
        else:
            gi.add_missing(missing, "schedule_screwed_me", "Missing weekly averages.")
        # Draft & Value
        team_picks = [row for row in draft_picks if row.get("team_key") == team_key]
        if not team_picks:
            gi.add_missing(missing, "draft_steal", "No draft picks found.")
            gi.add_missing(missing, "draft_bust", "No draft picks found.")
            gi.add_missing(missing, "reached_and_regretted", "No draft picks found.")
            gi.add_missing(missing, "late_round_wizardry", "No draft picks found.")
        elif not player_points:
            gi.add_missing(missing, "draft_steal", "Player scoring modifiers missing.")
            gi.add_missing(missing, "draft_bust", "Player scoring modifiers missing.")
            gi.add_missing(
                missing, "reached_and_regretted", "Player scoring modifiers missing."
            )
            gi.add_missing(
                missing, "late_round_wizardry", "Player scoring modifiers missing."
            )
        else:
            deltas = []
            for row in team_picks:
                player_key = row.get("player_key")
                if player_key not in draft_rank_by_player or player_key not in season_rank:
                    continue
                deltas.append(
                    {
                        "player_key": player_key,
                        "draft_rank": draft_rank_by_player[player_key],
                        "season_rank": season_rank[player_key],
                        "delta": draft_rank_by_player[player_key]
                        - season_rank[player_key],
                        "round": row.get("round"),
                        "pick": row.get("pick"),
                    }
                )

            if deltas:
                best = max(deltas, key=lambda item: item["delta"])
                insights.append(
                    {
                        "id": "draft_steal",
                        "title": "Draft Steal of the Year",
                        "metric": {
                            "draft_rank": best["draft_rank"],
                            "season_rank": best["season_rank"],
                            "delta": best["delta"],
                            "season_points": round(
                                player_totals.get(best["player_key"], 0), 2
                            ),
                            "round": best["round"],
                            "pick": best["pick"],
                        },
                        "team": gi.team_info(team_map, team_key),
                        "player": gi.player_info_from_map(best["player_key"], player_map),
                    }
                )

                worst = min(deltas, key=lambda item: item["delta"])
                insights.append(
                    {
                        "id": "reached_and_regretted",
                        "title": "Reached and Regretted",
                        "metric": {
                            "draft_rank": worst["draft_rank"],
                            "season_rank": worst["season_rank"],
                            "delta": worst["delta"],
                            "season_points": round(
                                player_totals.get(worst["player_key"], 0), 2
                            ),
                            "round": worst["round"],
                            "pick": worst["pick"],
                        },
                        "team": gi.team_info(team_map, team_key),
                        "player": gi.player_info_from_map(
                            worst["player_key"], player_map
                        ),
                    }
                )
            else:
                gi.add_missing(
                    missing, "draft_steal", "No overlapping draft + season stats."
                )
                gi.add_missing(
                    missing, "reached_and_regretted", "No overlapping draft + season stats."
                )

            team_picks_sorted = sorted(team_picks, key=lambda r: (r["round"], r["pick"]))
            top_round = [row for row in team_picks_sorted if row.get("round") == 1]
            candidates = top_round or team_picks_sorted[
                : max(1, min(3, len(team_picks_sorted)))
            ]
            bust_pick = None
            for row in candidates:
                player_key = row.get("player_key")
                if player_key is None:
                    continue
                points = player_totals.get(player_key)
                if points is None:
                    continue
                candidate = (points, player_key, row)
                if bust_pick is None or candidate[0] < bust_pick[0]:
                    bust_pick = candidate

            if bust_pick:
                points, player_key, row = bust_pick
                insights.append(
                    {
                        "id": "draft_bust",
                        "title": "Draft Bust Hall of Fame",
                        "metric": {
                            "round": row.get("round"),
                            "pick": row.get("pick"),
                            "season_points": round(points, 2),
                        },
                        "team": gi.team_info(team_map, team_key),
                        "player": gi.player_info_from_map(player_key, player_map),
                    }
                )
            else:
                gi.add_missing(missing, "draft_bust", "No draft picks with season points.")

            if late_round:
                late_round_picks = [
                    row
                    for row in team_picks
                    if row.get("round") and row.get("round") >= late_round
                ]
                best_pick = None
                for row in late_round_picks:
                    player_key = row.get("player_key")
                    if player_key is None:
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
                            "team": gi.team_info(team_map, team_key),
                            "player": gi.player_info_from_map(player_key, player_map),
                        }
                    )
                else:
                    gi.add_missing(
                        missing,
                        "late_round_wizardry",
                        "No late-round picks with season points.",
                    )
            else:
                gi.add_missing(
                    missing, "late_round_wizardry", "No draft rounds to evaluate."
                )

        # Start/Sit Decisions + Player Stats
        if not player_points:
            gi.add_missing(missing, "bench_war_crime", "Player scoring modifiers missing.")
            gi.add_missing(missing, "set_and_forget", "Player scoring modifiers missing.")
            gi.add_missing(missing, "overthinker", "Player scoring modifiers missing.")
            gi.add_missing(missing, "why_dont_he_want_me", "Player scoring modifiers missing.")
            gi.add_missing(missing, "favorite_player", "Player scoring modifiers missing.")
            gi.add_missing(missing, "emotional_support", "Player scoring modifiers missing.")
        else:
            bench_scores = {}
            starter_min = {}
            starts_by_player = defaultdict(int)
            start_points = defaultdict(list)
            bench_points_by_player = defaultdict(float)
            bench_weeks_by_player = defaultdict(int)
            roster_counts = defaultdict(int)

            for row in rosters_by_team.get(team_key, []):
                roster_key = (
                    row["player_key"],
                    row["player_name"],
                    row["player_position"],
                )
                roster_counts[roster_key] += 1
                points = player_points.get((row["player_key"], row["week"]))
                if points is None:
                    continue
                if row["slot_position"] in gi.BENCH_POSITIONS:
                    bench_points_by_player[roster_key] += points
                    bench_weeks_by_player[roster_key] += 1
                    if row["week"] not in bench_scores or points > bench_scores[row["week"]]["points"]:
                        bench_scores[row["week"]] = {"points": points, "player": row}
                else:
                    if row["week"] not in starter_min or points < starter_min[row["week"]]["points"]:
                        starter_min[row["week"]] = {"points": points, "player": row}
                    start_key = (
                        row["player_key"],
                        row["player_name"],
                        row["player_position"],
                    )
                    starts_by_player[start_key] += 1
                    start_points[start_key].append(points)

            if bench_scores:
                week, data = max(bench_scores.items(), key=lambda item: item[1]["points"])
                if data["player"]:
                    insights.append(
                        {
                            "id": "bench_war_crime",
                            "title": "Bench War Crime",
                            "metric": {
                                "week": week,
                                "points": round(data["points"], 2),
                            },
                            "team": gi.team_info(team_map, team_key),
                            "player": gi.player_info(
                                data["player"]["player_key"],
                                data["player"]["player_name"],
                                data["player"]["player_position"],
                            ),
                        }
                    )
            else:
                gi.add_missing(missing, "bench_war_crime", "No bench scoring data.")

            if starts_by_player:
                best_count = max(starts_by_player.values())
                contenders = [k for k, v in starts_by_player.items() if v == best_count]
                if len(contenders) > 1:
                    best = max(
                        contenders,
                        key=lambda k: statistics.mean(start_points.get(k, [0]))
                        if start_points.get(k)
                        else 0,
                    )
                else:
                    best = contenders[0]
                player_key, player_name, player_position = best
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
                        "team": gi.team_info(team_map, team_key),
                        "player": gi.player_info(player_key, player_name, player_position),
                    }
                )
            else:
                gi.add_missing(missing, "set_and_forget", "No starter data.")

            overthinker_count = 0
            for game in team_games:
                if game["result"] != "loss":
                    continue
                week = game["week"]
                bench = bench_scores.get(week)
                starter = starter_min.get(week)
                if not bench or not starter:
                    continue
                if bench["points"] > starter["points"] and bench["points"] - starter["points"] > game["margin"]:
                    overthinker_count += 1

            if overthinker_count:
                insights.append(
                    {
                        "id": "overthinker",
                        "title": "Overthinker",
                        "metric": {"games": overthinker_count},
                        "team": gi.team_info(team_map, team_key),
                    }
                )
            else:
                gi.add_missing(missing, "overthinker", "No games lost due to start/sit choices.")

            if bench_points_by_player:
                best = max(bench_points_by_player.items(), key=lambda item: item[1])
                player_key, player_name, player_position = best[0]
                insights.append(
                    {
                        "id": "why_dont_he_want_me",
                        "title": "Why Don't He Want Me, Man?",
                        "metric": {
                            "bench_points": round(best[1], 2),
                            "bench_weeks": bench_weeks_by_player.get(best[0], 0),
                        },
                        "player": gi.player_info(player_key, player_name, player_position),
                        "team": gi.team_info(team_map, team_key),
                    }
                )
            else:
                gi.add_missing(missing, "why_dont_he_want_me", "No bench scoring totals.")

            if roster_counts:
                favorite = max(roster_counts.items(), key=lambda item: item[1])
                insights.append(
                    {
                        "id": "favorite_player",
                        "title": "Favorite Player",
                        "metric": {"roster_appearances": favorite[1]},
                        "player": gi.player_info(
                            favorite[0][0], favorite[0][1], favorite[0][2]
                        ),
                        "team": gi.team_info(team_map, team_key),
                    }
                )
            else:
                gi.add_missing(missing, "favorite_player", "No roster data available.")

            if starts_by_player:
                best_count = max(starts_by_player.values())
                contenders = [k for k, v in starts_by_player.items() if v == best_count]
                if len(contenders) > 1:
                    best = max(
                        contenders,
                        key=lambda k: statistics.mean(start_points.get(k, [0]))
                        if start_points.get(k)
                        else 0,
                    )
                else:
                    best = contenders[0]
                player_key, player_name, player_position = best
                insights.append(
                    {
                        "id": "emotional_support",
                        "title": "Emotional Support Player",
                        "metric": {
                            "starts": best_count,
                            "avg_weekly_score": round(
                                statistics.mean(start_points.get(best, [0])),
                                2,
                            ),
                        },
                        "team": gi.team_info(team_map, team_key),
                        "player": gi.player_info(player_key, player_name, player_position),
                    }
                )
            else:
                gi.add_missing(missing, "emotional_support", "No starter data.")
        # Weekly & Seasonal Storylines
        team_weekly = weekly_points.get(team_key, [])
        if team_weekly:
            high_week = max(team_weekly, key=lambda item: item[1])
            low_week = min(team_weekly, key=lambda item: item[1])
            insights.append(
                {
                    "id": "peak_week",
                    "title": "Peak Week",
                    "metric": {
                        "week": high_week[0],
                        "points": round(high_week[1], 2),
                    },
                    "team": gi.team_info(team_map, team_key),
                }
            )
            insights.append(
                {
                    "id": "rock_bottom",
                    "title": "Rock Bottom",
                    "metric": {
                        "week": low_week[0],
                        "points": round(low_week[1], 2),
                    },
                    "team": gi.team_info(team_map, team_key),
                }
            )
        else:
            gi.add_missing(missing, "peak_week", "No weekly scoring data.")
            gi.add_missing(missing, "rock_bottom", "No weekly scoring data.")

        if team_weekly:
            split_week = max(1, end_week // 2)
            first = [p for w, p in team_weekly if w <= split_week]
            second = [p for w, p in team_weekly if w > split_week]
            if first and second:
                diff = statistics.mean(second) - statistics.mean(first)
                if diff > 0:
                    insights.append(
                        {
                            "id": "mid_season_glow_up",
                            "title": "Mid-Season Glow-Up",
                            "metric": {"points_diff": round(diff, 2)},
                            "team": gi.team_info(team_map, team_key),
                        }
                    )
                else:
                    gi.add_missing(missing, "mid_season_glow_up", "No mid-season improvement.")
                if diff < 0:
                    insights.append(
                        {
                            "id": "late_season_collapse",
                            "title": "Late-Season Collapse",
                            "metric": {"points_diff": round(diff, 2)},
                            "team": gi.team_info(team_map, team_key),
                        }
                    )
                else:
                    gi.add_missing(missing, "late_season_collapse", "No late-season collapse.")
            else:
                gi.add_missing(missing, "mid_season_glow_up", "Not enough weekly data.")
                gi.add_missing(missing, "late_season_collapse", "Not enough weekly data.")

        # Fun Awards
        if weekly_projected.get(team_key):
            week_projection = {}
            for week, points in weekly_projected.get(team_key, []):
                week_projection.setdefault(week, {})["projected"] = points
            for week, points in weekly_points.get(team_key, []):
                week_projection.setdefault(week, {})["actual"] = points

            best = None
            for week, values in week_projection.items():
                if playoff_start and week >= playoff_start:
                    continue
                if "projected" not in values or "actual" not in values:
                    continue
                diff = values["projected"] - values["actual"]
                candidate = {
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
                        "team": gi.team_info(team_map, team_key),
                    }
                )
            else:
                gi.add_missing(missing, "looked_better_on_paper", "No projection deltas found.")
        else:
            gi.add_missing(missing, "looked_better_on_paper", "Missing projected scores.")

        if team_key in playoff_teams:
            split_week = max(1, end_week // 2)
            first_half = {"wins": 0, "losses": 0, "ties": 0}
            for game in team_games:
                if game["week"] > split_week:
                    continue
                if game["result"] == "win":
                    first_half["wins"] += 1
                elif game["result"] == "loss":
                    first_half["losses"] += 1
                else:
                    first_half["ties"] += 1
            games = first_half["wins"] + first_half["losses"] + first_half["ties"]
            win_pct = (first_half["wins"] / games) if games else 0
            if games and first_half["wins"] < first_half["losses"]:
                insights.append(
                    {
                        "id": "trust_the_process",
                        "title": "Trust the Process Award",
                        "metric": {
                            "first_half_wins": first_half["wins"],
                            "first_half_losses": first_half["losses"],
                            "first_half_ties": first_half["ties"],
                            "first_half_win_pct": round(win_pct, 3),
                        },
                        "team": gi.team_info(team_map, team_key),
                    }
                )
            else:
                gi.add_missing(missing, "trust_the_process", "First half was not below .500.")
        else:
            gi.add_missing(missing, "trust_the_process", "Did not make the playoffs.")

        if team_key not in playoff_teams:
            non_playoff = [key for key in team_map if key not in playoff_teams]
            if non_playoff:
                points_map = {key: team_points_for_map.get(key, 0) for key in non_playoff}
                if points_map and team_key == max(points_map, key=points_map.get):
                    insights.append(
                        {
                            "id": "well_get_em_next_year",
                            "title": "We'll Get 'Em Next Year",
                            "metric": {
                                "points_for": round(points_map.get(team_key, 0), 2),
                                "rank": standings.get(team_key, {}).get("rank"),
                            },
                            "team": gi.team_info(team_map, team_key),
                        }
                    )
                else:
                    gi.add_missing(missing, "well_get_em_next_year", "Not the top non-playoff scorer.")
            else:
                gi.add_missing(missing, "well_get_em_next_year", "No non-playoff teams found.")
        else:
            gi.add_missing(missing, "well_get_em_next_year", "Made the playoffs.")

        team_payloads.append(
            {
                "team_key": team_key,
                "team_name": team_map[team_key].get("team_name"),
                "manager_names": team_map[team_key].get("manager_names"),
                "insights": insights,
                "missing": missing,
            }
        )

    return {
        "season": season,
        "league_key": league_key,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "teams": team_payloads,
    }

def main():
    parser = argparse.ArgumentParser(description="Generate team-specific insights.")
    parser.add_argument("--league", dest="league_key", help="Generate for a single league key.")
    parser.add_argument("--season", dest="season", help="Generate for a single season.")
    parser.add_argument("--season-start", dest="season_start", help="Generate for seasons >= this year.")
    parser.add_argument("--season-end", dest="season_end", help="Generate for seasons <= this year.")
    args = parser.parse_args()

    if not gi.DB_PATH.exists():
        print(f"Missing database: {gi.DB_PATH}")
        return

    conn = sqlite3.connect(gi.DB_PATH)
    conn.row_factory = sqlite3.Row

    leagues = gi.load_leagues(conn)
    if args.season:
        leagues = [l for l in leagues if str(l[1]) == str(args.season)]
    if args.league_key:
        leagues = [l for l in leagues if l[0] == args.league_key]
    if args.season_start:
        leagues = [l for l in leagues if l[1] is not None and int(l[1]) >= int(args.season_start)]
    if args.season_end:
        leagues = [l for l in leagues if l[1] is not None and int(l[1]) <= int(args.season_end)]

    for league_key, season in leagues:
        payload = compute_team_insights_for_league(conn, league_key, season)
        gi.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = gi.OUTPUT_DIR / f"insights_{season}_teams.json"
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
