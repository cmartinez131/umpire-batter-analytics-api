import pandas as pd

def main():
    # 1) Point to your file path:
    path = "data/full_season_umpires/2022_hp_umpires.parquet"
    #path = "data/full_season_umpires/2023_hp_umpires.parquet"
    #path = "data/game_umpires/2022_game_hp_umpires.parquet"

    # 2) Load into a pandas DataFrame:
    df = pd.read_parquet(path)

    # 3) Confirm it loaded:
    print("Loaded DataFrame with shape:", df.shape)
    # e.g. “(1620, 4)” → ~1620 games × 4 columns

    # 4) List all column names:
    print("Columns:", df.columns.tolist())

    # 5) Print first 20 rows:
    print("\nFirst 30 rows:")
    print(df.head(30))

    print('\n')
    games_at_the_plate = df["home_plate_umpire_name"].value_counts()
    print(games_at_the_plate.head(30))

if __name__ == "__main__":
    main()
