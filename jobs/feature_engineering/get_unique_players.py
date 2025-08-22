import pandas as pd
from pathlib import Path

def get_unique_batters(year):
    """
    Get a DataFrame of all players in a single season who faced at least one pitch.
    Index is MLB player ID, column is player full name.
    
    Args:
        year (int): the year of the mlb season
    Returns:
        players_df (pandas dataframe): a two column dataframe of all the players that faced a pitch
            the first column is their MLB Player ID and the second column is their full name
        
    """


    # Get this script's directory, then go up to the project root
    root_dir = Path(__file__).resolve().parents[2]  # 2 levels up from jobs/feature_engineering
    path = root_dir / "data" / "full_season_pitches" / f"{year}_pitches.parquet"

    
    all_pitches_df = pd.read_parquet(path)
    
    # Confirm it loaded:
    print("Loaded DataFrame with shape:", all_pitches_df.shape)

    # filer pitches to only include regular season games where game_type is "R"
    print("Filtered DataFrame to only show regular season pitches")
    all_pitches_df = all_pitches_df[all_pitches_df["game_type"] == "R"]


    print("After filtering for regular season games:", all_pitches_df.shape)


    # List all column names:
    print(all_pitches_df.columns.tolist())

    # Count unique type of pitches:
    pitch_type_counts = all_pitches_df["pitch_type"].value_counts()
    print(f"Different types of pitches: {pitch_type_counts.shape[0]}")
    print(pitch_type_counts)


    game_pks_df = all_pitches_df["game_pk"].value_counts()
    print("game_pk dataframe with count of unique game_pks and the number of pitches in each game:")
    print("first 5 games:")
    print(game_pks_df.head(5))
    print(f"shape:", game_pks_df.shape)
    unique_game_pks = game_pks_df.shape[0]
    print(f"Unique Games: {unique_game_pks}")



    batter_pitch_counts = all_pitches_df['player_name'].value_counts()
    print(batter_pitch_counts.head(5))
    print(f"Unique batters: {batter_pitch_counts.shape[0]}")


    pitcher_pitch_counts = all_pitches_df['pitcher'].value_counts()
    print(pitcher_pitch_counts.head(5))
    print(f"Unique pitcher: {pitcher_pitch_counts.shape[0]}")

    
    pitch_events = all_pitches_df['events'].value_counts()
    print("Possible pitch events: ")
    print(pitch_events.head(25))
    print(f"Unique pitch events: {pitch_events.shape[0]}")
    

    # Example: Number of pitches per batter
    pitches_per_batter = (
        all_pitches_df.groupby('player_name')['pitch_type']
        .count()
        .sort_values(ascending=False)
    )
    print(pitches_per_batter.head())


    # Quick summary stats
    print(all_pitches_df['launch_speed'].describe())
    print(all_pitches_df['pitch_type'].describe())

    # Unique batters DataFrame
    unique_batters_df = (
        all_pitches_df[['batter', 'player_name']]
        .drop_duplicates()
        .set_index('batter')
        .sort_index()
    )

    return unique_batters_df



def main():
    """
    Print the dataframe of unique players and unique a
    """
    print("file ran")
    unique_batters = get_unique_batters(2022)
    print(unique_batters.head())
    print(f"Total Unique Batters: {unique_batters.shape}")

    

if __name__ == "__main__":
    main()