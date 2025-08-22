import pandas as pd

def main():
    # 1) Path to parquet file
    path = "data/full_season_pitches/2023_pitches.parquet"

    # 2) Load into a pandas DataFrame:
    cols = ["game_pk", "game_date", "batter", "pitcher", "events", "description", "umpire", "plate_x", "plate_z", "game_type"]
    df = pd.read_parquet(path, columns=cols)

    print("Loaded DataFrame with shape:", df.shape)
    # e.g. “(731121, 118)” → 731 121 rows × 118 columns

    regular_season = df[df["game_type"] == "R"]

    # 3) Confirm it loaded:
    print("DataFrame with regular season pitches:", regular_season.shape)
    # e.g. “(731121, 118)” → 731 121 rows × 118 columns

    # Filter dataframe to only include rows where the batter id is the given id
    player_id = 592450  # aaron judge
    player_pitches = regular_season[regular_season["batter"] == player_id]
    print("Filter df to only include pitches with aaron judge batting:", player_pitches.shape)

    player_home_runs = player_pitches[player_pitches["events"] == "home_run"]
    print("Filtered to include pitches that the event is a home run", player_home_runs.shape)

    print("Number of rows:", player_home_runs.shape[0])

    print(player_home_runs)

if __name__ == "__main__":
    main()