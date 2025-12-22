import json

from yahoo_client import find_child_text, strip_ns


def iter_elements(root, tag_name):
    for elem in root.iter():
        if strip_ns(elem.tag) == tag_name:
            yield elem


def find_child(elem, name):
    for child in list(elem):
        if strip_ns(child.tag) == name:
            return child
    return None


def find_descendant(elem, name):
    for child in elem.iter():
        if strip_ns(child.tag) == name:
            return child
    return None


def find_descendant_text(elem, name):
    if elem is None:
        return ""
    for child in elem.iter():
        if strip_ns(child.tag) == name:
            return (child.text or "").strip()
    return ""


def parse_games(root):
    games = []
    for elem in iter_elements(root, "game"):
        game_key = find_child_text(elem, "game_key")
        code = find_child_text(elem, "code")
        season = find_child_text(elem, "season")
        if game_key:
            games.append({"game_key": game_key, "code": code, "season": season})
    return _dedupe(games, "game_key")


def parse_leagues(root):
    leagues = []
    for elem in iter_elements(root, "league"):
        league_key = find_child_text(elem, "league_key")
        league_id = find_child_text(elem, "league_id")
        name = find_child_text(elem, "name")
        season = find_child_text(elem, "season")
        if league_key:
            leagues.append(
                {
                    "league_key": league_key,
                    "league_id": league_id,
                    "name": name,
                    "season": season,
                }
            )
    return _dedupe(leagues, "league_key")


def parse_league_meta(root):
    league = next(iter_elements(root, "league"), None)
    if league is None:
        return {}
    return {
        "league_key": find_child_text(league, "league_key"),
        "league_id": find_child_text(league, "league_id"),
        "name": find_child_text(league, "name"),
        "season": find_child_text(league, "season"),
        "game_key": find_child_text(league, "game_key"),
    }


def parse_settings(root):
    settings = {}
    settings_elem = find_descendant(root, "settings")
    if settings_elem is None:
        return settings

    settings["start_week"] = _to_int(find_descendant_text(settings_elem, "start_week"))
    settings["end_week"] = _to_int(find_descendant_text(settings_elem, "end_week"))
    settings["playoff_start_week"] = _to_int(find_descendant_text(settings_elem, "playoff_start_week"))
    settings["num_teams"] = _to_int(find_descendant_text(settings_elem, "num_teams"))
    settings["scoring_type"] = find_descendant_text(settings_elem, "scoring_type")

    roster_positions = []
    roster_positions_elem = find_descendant(settings_elem, "roster_positions")
    if roster_positions_elem is not None:
        for rp in iter_elements(roster_positions_elem, "roster_position"):
            roster_positions.append(
                {
                    "position": find_descendant_text(rp, "position"),
                    "count": _to_int(find_descendant_text(rp, "count")),
                }
            )

    stat_categories = []
    stat_categories_elem = find_descendant(settings_elem, "stat_categories")
    if stat_categories_elem is not None:
        for stat in iter_elements(stat_categories_elem, "stat"):
            stat_categories.append(
                {
                    "stat_id": find_descendant_text(stat, "stat_id"),
                    "name": find_descendant_text(stat, "name"),
                }
            )

    stat_modifiers = []
    stat_modifiers_elem = find_descendant(settings_elem, "stat_modifiers")
    if stat_modifiers_elem is not None:
        for stat in iter_elements(stat_modifiers_elem, "stat"):
            stat_id = find_descendant_text(stat, "stat_id")
            value = _to_float(find_descendant_text(stat, "value"))
            if stat_id:
                stat_modifiers.append({"stat_id": stat_id, "value": value})
        if not stat_modifiers:
            for modifier in iter_elements(stat_modifiers_elem, "stat_modifier"):
                stat_id = find_descendant_text(modifier, "stat_id")
                value = _to_float(find_descendant_text(modifier, "value"))
                if stat_id:
                    stat_modifiers.append({"stat_id": stat_id, "value": value})

    settings["roster_positions"] = json.dumps(roster_positions, ensure_ascii=True)
    settings["stat_categories"] = json.dumps(stat_categories, ensure_ascii=True)
    settings["stat_modifiers"] = json.dumps(stat_modifiers, ensure_ascii=True)
    return settings


def parse_teams(root):
    teams = []
    for team in iter_elements(root, "team"):
        team_key = find_child_text(team, "team_key")
        if not team_key:
            continue
        managers = []
        managers_elem = find_descendant(team, "managers")
        if managers_elem is not None:
            for manager in iter_elements(managers_elem, "manager"):
                name = find_descendant_text(manager, "nickname")
                if not name:
                    name = find_descendant_text(manager, "guid")
                if name:
                    managers.append(name)
        teams.append(
            {
                "team_key": team_key,
                "team_id": find_child_text(team, "team_id"),
                "name": find_child_text(team, "name"),
                "url": find_child_text(team, "url"),
                "manager_names": ", ".join(managers),
            }
        )
    return _dedupe(teams, "team_key")


def parse_standings(root):
    rows = []
    for team in iter_elements(root, "team"):
        team_key = find_child_text(team, "team_key")
        if not team_key:
            continue
        standings = find_descendant(team, "team_standings")
        if standings is None:
            continue
        outcomes = find_descendant(standings, "outcome_totals")
        rows.append(
            {
                "team_key": team_key,
                "rank": _to_int(find_descendant_text(standings, "rank")),
                "wins": _to_int(find_descendant_text(outcomes, "wins")),
                "losses": _to_int(find_descendant_text(outcomes, "losses")),
                "ties": _to_int(find_descendant_text(outcomes, "ties")),
                "points_for": _to_float(find_descendant_text(standings, "points_for")),
                "points_against": _to_float(find_descendant_text(standings, "points_against")),
            }
        )
    return rows


def parse_matchups(root, week):
    matchups = []
    matchup_teams = []

    for index, matchup in enumerate(iter_elements(root, "matchup"), start=1):
        matchup_id = find_child_text(matchup, "matchup_id") or str(index)
        status = find_child_text(matchup, "status")
        is_playoffs = _to_int(find_child_text(matchup, "is_playoffs"))
        is_consolation = _to_int(find_child_text(matchup, "is_consolation"))
        winner_team_key = find_child_text(matchup, "winner_team_key")

        if matchup_id:
            matchups.append(
                {
                    "week": week,
                    "matchup_id": matchup_id,
                    "status": status,
                    "is_playoffs": is_playoffs,
                    "is_consolation": is_consolation,
                    "winner_team_key": winner_team_key,
                }
            )

        for team in iter_elements(matchup, "team"):
            team_key = find_child_text(team, "team_key")
            if not team_key:
                continue
            points = _extract_points(team)
            projected_points = _extract_projected_points(team)
            win_status = find_child_text(team, "win_status")
            matchup_teams.append(
                {
                    "week": week,
                    "matchup_id": matchup_id,
                    "team_key": team_key,
                    "points": _to_float(points),
                    "projected_points": _to_float(projected_points),
                    "win_status": win_status,
                }
            )

    return matchups, matchup_teams


def parse_roster(root, week):
    roster_rows = []
    players = []

    for team in iter_elements(root, "team"):
        team_key = find_child_text(team, "team_key")
        if not team_key:
            continue
        roster = find_descendant(team, "roster")
        if roster is None:
            continue
        players_elem = find_descendant(roster, "players")
        if players_elem is None:
            continue

        for player in iter_elements(players_elem, "player"):
            player_key = find_child_text(player, "player_key")
            if not player_key:
                continue
            position = ""
            selected_position = find_descendant(player, "selected_position")
            if selected_position is not None:
                position = find_descendant_text(selected_position, "position")

            roster_rows.append(
                {
                    "team_key": team_key,
                    "week": week,
                    "player_key": player_key,
                    "position": position,
                    "status": find_descendant_text(player, "status"),
                    "injury_status": find_descendant_text(player, "injury_status"),
                    "injury_note": find_descendant_text(player, "injury_note"),
                }
            )

            players.append(_parse_player_core(player))

    return roster_rows, _dedupe(players, "player_key")


def parse_team_stats(root, week):
    rows = []
    for team in iter_elements(root, "team"):
        team_key = find_child_text(team, "team_key")
        if not team_key:
            continue
        stats_parent = find_descendant(team, "team_stats")
        if stats_parent is None:
            stats_parent = find_descendant(team, "stats")
        if stats_parent is not None:
            for stat in iter_elements(stats_parent, "stat"):
                stat_id = find_descendant_text(stat, "stat_id")
                value = find_descendant_text(stat, "value")
                if stat_id:
                    rows.append(
                        {
                            "team_key": team_key,
                            "week": week,
                            "stat_id": stat_id,
                            "value": value,
                        }
                    )
        points = _extract_points(team)
        projected_points = _extract_projected_points(team)
        if points:
            rows.append(
                {
                    "team_key": team_key,
                    "week": week,
                    "stat_id": "points",
                    "value": points,
                }
            )
        if projected_points:
            rows.append(
                {
                    "team_key": team_key,
                    "week": week,
                    "stat_id": "projected_points",
                    "value": projected_points,
                }
            )
    return rows


def parse_player_stats(root, week):
    rows = []
    players = []

    for player in iter_elements(root, "player"):
        player_key = find_child_text(player, "player_key")
        if not player_key:
            continue
        stats_parent = find_descendant(player, "player_stats")
        if stats_parent is None:
            stats_parent = find_descendant(player, "stats")
        if stats_parent is None:
            continue
        for stat in iter_elements(stats_parent, "stat"):
            stat_id = find_descendant_text(stat, "stat_id")
            value = find_descendant_text(stat, "value")
            if stat_id:
                rows.append(
                    {
                        "player_key": player_key,
                        "week": week,
                        "stat_id": stat_id,
                        "value": value,
                    }
                )
        players.append(_parse_player_core(player))

    return rows, _dedupe(players, "player_key")


def parse_transactions(root):
    transactions = []
    transaction_players = []
    players = []

    for txn in iter_elements(root, "transaction"):
        transaction_key = find_child_text(txn, "transaction_key")
        if not transaction_key:
            continue
        transactions.append(
            {
                "transaction_key": transaction_key,
                "type": find_child_text(txn, "type"),
                "status": find_child_text(txn, "status"),
                "timestamp": _to_int(find_child_text(txn, "timestamp")),
            }
        )

        players_elem = find_descendant(txn, "players")
        if players_elem is not None:
            for player in iter_elements(players_elem, "player"):
                player_key = find_child_text(player, "player_key")
                if not player_key:
                    continue
                txn_data = find_descendant(player, "transaction_data")
                transaction_players.append(
                    {
                        "transaction_key": transaction_key,
                        "player_key": player_key,
                        "transaction_type": find_descendant_text(txn_data, "type"),
                        "source_type": find_descendant_text(txn_data, "source_type"),
                        "source_team_key": find_descendant_text(txn_data, "source_team_key"),
                        "destination_type": find_descendant_text(txn_data, "destination_type"),
                        "destination_team_key": find_descendant_text(txn_data, "destination_team_key"),
                    }
                )
                players.append(_parse_player_core(player))

    return transactions, transaction_players, _dedupe(players, "player_key")


def parse_draft_results(root):
    results = []

    for draft_result in iter_elements(root, "draft_result"):
        keeper = find_child_text(draft_result, "keeper") or find_child_text(draft_result, "is_keeper")
        autopick = find_child_text(draft_result, "autopick") or find_child_text(draft_result, "is_autopick")
        results.append(
            {
                "round": _to_int(find_child_text(draft_result, "round")),
                "pick": _to_int(find_child_text(draft_result, "pick")),
                "team_key": find_child_text(draft_result, "team_key"),
                "player_key": find_child_text(draft_result, "player_key"),
                "cost": _to_float(find_child_text(draft_result, "cost")),
                "is_keeper": _to_int(keeper),
                "is_autopick": _to_int(autopick),
            }
        )

    return results


def _parse_player_core(player):
    name_full = ""
    name_elem = find_descendant(player, "name")
    if name_elem is not None:
        name_full = find_descendant_text(name_elem, "full")
    return {
        "player_key": find_child_text(player, "player_key"),
        "player_id": find_child_text(player, "player_id"),
        "name_full": name_full,
        "position": find_child_text(player, "display_position"),
        "editorial_team_abbr": find_child_text(player, "editorial_team_abbr"),
    }


def _extract_points(team):
    team_points = find_descendant(team, "team_points")
    if team_points is not None:
        value = find_descendant_text(team_points, "total")
        if value:
            return value
        value = find_descendant_text(team_points, "points")
        if value:
            return value
    return find_descendant_text(team, "points")


def _extract_projected_points(team):
    projected = find_descendant(team, "team_projected_points")
    if projected is not None:
        value = find_descendant_text(projected, "total")
        if value:
            return value
        value = find_descendant_text(projected, "points")
        if value:
            return value
    return find_descendant_text(team, "projected_points")


def _dedupe(items, key_name):
    seen = set()
    output = []
    for item in items:
        key = item.get(key_name)
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
