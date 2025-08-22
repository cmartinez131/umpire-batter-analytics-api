import pandas as pd

# 1) Point to your file path:
path = "data/full_season_pitches/2024_pitches.parquet"

# 2) Load into a pandas DataFrame:
df = pd.read_parquet(path)

# 3) Confirm it loaded:
print("Loaded DataFrame with shape:", df.shape)
# e.g. “(731121, 118)” → 731 121 rows × 118 columns

# List all column names:
print(df.columns.tolist())

counts = df["pitch_type"].value_counts()
print(counts)

umpires = df[["umpire","home_plate_umpire_id","home_plate_umpire_name"]]
print(umpires.head())