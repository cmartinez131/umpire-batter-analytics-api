import pandas as pd
from pybaseball import playerid_lookup, statcast

# Set the player's name and the season you want data for
player_first_name = 'shohei'
player_last_name = 'ohtani'
year = 2023

# 1. Look up the player's ID
# This returns a pandas DataFrame
player_info = playerid_lookup(player_last_name, player_first_name)

# Extract the MLBAM key (player ID)
# We use .iloc[0] assuming the first result is the correct player
mlbam_id = player_info['key_mlbam'].iloc[0]
print(f"Found Player ID for {player_first_name.title()} {player_last_name.title()}: {mlbam_id}")

# 2. Fetch all statcast data for the specified year
# Note: This can take a few minutes as it downloads a lot of data.
print(f"\nFetching Statcast data for the {year} season...")
start_date = f'{year}-03-01' # Start of spring training
end_date = f'{year}-11-01'   # End of the World Series
data = statcast(start_dt=start_date, end_dt=end_date)

# 3. Filter the data for the specific player
# Filter for pitches thrown by the player
pitches_thrown = data[data['pitcher'] == mlbam_id]

# Filter for pitches seen by the player as a batter
pitches_faced = data[data['batter'] == mlbam_id]

print(f"\nData retrieved successfully!")
print(f"Total pitches thrown by Ohtani in {year}: {len(pitches_thrown)}")
print(f"Total pitches faced by Ohtani in {year}: {len(pitches_faced)}")

# Display the first 5 rows of pitches thrown
print("\nSample of pitches thrown:")
print(pitches_thrown[['pitch_type', 'game_date', 'release_speed', 'description']].head())