import argparse
from pathlib import Path
import time
import requests
import pandas as pd

# Game types to include (Regular + Postseason + All-Star + extras commonly carrying officials)
INCLUDED_TYPES = {"R", "P", "F", "D", "L", "W", "A"}

# Completed game statuses
COMPLETED_DETAILED = {"Final", "Game Over", "Completed Early"}
COMPLETED_CODED = {"F", "O"}  # F=Final, O=Game Over


def fetch_season(year: int):
    start = f"{year}-01-01"
    end   = f"{year}-12-31"
    print(f"> fetching {year} season ({start}→{end}) umpires (filtered, with fallback)…")

    # Figure out where to write
    this_file    = Path(__file__).resolve()
    project_root = this_file.parents[2]
    ump_dir      = project_root / "data" / "full_season_umpires"
    ump_dir.mkdir(parents=True, exist_ok=True)

    # STEP 1: pull the schedule for the whole season (all game types),
    # then filter to completed games and the included types.
    schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={year}"
    response = requests.get(schedule_url, timeout=15)
    response.raise_for_status()
    schedule = response.json()

    # Build a list of filtered games we actually want to process
    filtered_games = []
    for day in schedule.get("dates", []):
        for game in day.get("games", []):
            game_type = game.get("gameType")
            if game_type not in INCLUDED_TYPES:
                continue

            status = game.get("status", {}) or {}
            detailed = status.get("detailedState")
            coded = status.get("codedGameState")

            if (detailed in COMPLETED_DETAILED) or (coded in COMPLETED_CODED):
                filtered_games.append(game)

    total_games = len(filtered_games)
    print(f"> filtered to {total_games} completed games across types {sorted(INCLUDED_TYPES)}")

    # URL templates
    gumbo_url_template = (
        "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        "?hydrate=officials"
    )
    boxscore_url_template = "https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"

    # STEP 2: fetch home-plate umpire for each filtered game
    records = []
    count = 0

    for game in filtered_games:
        game_pk   = game["gamePk"]
        game_type = game.get("gameType")

        home_plate_id = None
        home_plate_name = None

        # 2a) Try GUMBO feed/live (prefer liveData.boxscore.officials, then gameData.officials)
        try:
            r = requests.get(gumbo_url_template.format(game_pk=game_pk), timeout=12)
            r.raise_for_status()
            feed = r.json()

            officials = (
                feed.get("liveData", {}).get("boxscore", {}).get("officials", [])
                or feed.get("gameData", {}).get("officials", [])
            )

            if officials:
                for item in officials:
                    official_type = (item.get("officialType") or "").lower().replace(" ", "")
                    if official_type in ("homeplate", "hp"):
                        inner = item.get("official", item)  # nested or flat shape
                        home_plate_id = inner.get("id")
                        home_plate_name = inner.get("fullName")
                        break
        except requests.RequestException:
            # Keep going; we'll try the fallback below
            pass

        # 2b) Fallback: if still missing, query the v1 boxscore endpoint
        if home_plate_id is None and home_plate_name is None:
            try:
                bx = requests.get(boxscore_url_template.format(game_pk=game_pk), timeout=20)
                bx.raise_for_status()
                box = bx.json()

                officials = box.get("officials", []) or []
                if officials:
                    for item in officials:
                        official_type = (item.get("officialType") or "").lower().replace(" ", "")
                        if official_type in ("homeplate", "hp"):
                            inner = item.get("official", item)  # nested or flat shape
                            home_plate_id = inner.get("id")
                            home_plate_name = inner.get("fullName")
                            break
            except requests.RequestException:
                # leave as None; record and move on
                pass

        records.append(
            {
                "game_pk":                game_pk,
                "game_type":              game_type,
                "home_plate_umpire_id":   home_plate_id,
                "home_plate_umpire_name": home_plate_name,
            }
        )

        count += 1
        if count % 50 == 0 or count == total_games:
            print(f"    • processed {count}/{total_games} games")
        # time.sleep(0.1)  # polite rate-limit

    # STEP 3: save to Parquet
    df = pd.DataFrame(records)
    out_path = ump_dir / f"{year}_hp_umpires.parquet"
    df.to_parquet(out_path, index=False)
    print(f"> saved {len(df)} rows to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch home-plate umpires via GUMBO for a season (filtered + fallback)"
    )
    parser.add_argument(
        "--year", type=int, default=2024,
        help="Season year to fetch (e.g. 2024)"
    )
    args = parser.parse_args()
    fetch_season(args.year)
