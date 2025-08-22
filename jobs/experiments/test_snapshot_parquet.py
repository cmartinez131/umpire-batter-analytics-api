import pandas as pd

def test_snapshot_batch(year):
    # 1) Point to your file path:
    path = f"data/lookups/player_snapshots_{year}.parquet"
    #path = "data/full_season_umpires/2023_hp_umpires.parquet"
    #path = "data/game_umpires/2022_game_hp_umpires.parquet"

    # 2) Load into a pandas DataFrame:
    df = pd.read_parquet(path)

    # 3) Confirm it loaded:
    print("Loaded DataFrame with shape:", df.shape)
    # e.g. “(1620, 4)” → ~1620 games × 4 columns

    # 4) List all column names:
    print("Columns:", df.columns.tolist())

    # Filter rows
    df = df[['full_name', 'allstar_prior', 'hits_career_prior', 'ab_career_prior', 'mvps_prior', 'gold_gloves_prior', 'silver_sluggers_prior', 'war_career_prior']]

    # 5) Print first 20 rows:
    print("\nFirst 30 rows:")
    print(df.head(30))

    print('\n')
    

if __name__ == "__main__":
    test_snapshot_batch(2023)
