import json
from pathlib import Path

from yahoo_client import api_get, load_config, parse_xml
from parse_yahoo_xml import parse_games, parse_leagues

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "data" / "processed"


def main():
    config = load_config()
    game_code = str(config.get("game_key", "nfl"))
    season_start = int(config.get("season_start", 0))
    season_end = int(config.get("season_end", 9999))
    league_name_hint = str(config.get("league_name_hint", "")).strip().lower()
    league_id_hint = str(config.get("league_id_hint", "")).strip()
    league_filter_mode = str(config.get("league_filter_mode", "filtered")).strip().lower()

    games_xml = api_get("/users;use_login=1/games")
    games_root = parse_xml(games_xml)
    games = parse_games(games_root)

    games = [
        g for g in games
        if g.get("code") == game_code
        and g.get("season").isdigit()
        and season_start <= int(g.get("season")) <= season_end
    ]

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "games.json").write_text(json.dumps(games, indent=2), encoding="utf-8")

    leagues = []
    for game in games:
        game_key = game["game_key"]
        leagues_xml = api_get(f"/users;use_login=1/games;game_keys={game_key}/leagues")
        leagues_root = parse_xml(leagues_xml)
        parsed = parse_leagues(leagues_root)
        for league in parsed:
            league["game_key"] = game_key
            league["season"] = game.get("season")
        leagues.extend(parsed)

    all_leagues = list(leagues)

    if league_filter_mode != "all":
        if league_id_hint:
            leagues = [l for l in leagues if l.get("league_id") == league_id_hint]
        if league_name_hint:
            leagues = [l for l in leagues if league_name_hint in l.get("name", "").lower()]
    else:
        leagues = all_leagues

    (OUTPUT_DIR / "leagues.json").write_text(json.dumps(leagues, indent=2), encoding="utf-8")
    (OUTPUT_DIR / "leagues_all.json").write_text(json.dumps(all_leagues, indent=2), encoding="utf-8")

    print("Wrote:")
    print(f"- {OUTPUT_DIR / 'games.json'}")
    print(f"- {OUTPUT_DIR / 'leagues.json'}")
    print(f"- {OUTPUT_DIR / 'leagues_all.json'}")
    print(f"Leagues found: {len(leagues)}")


if __name__ == "__main__":
    main()
