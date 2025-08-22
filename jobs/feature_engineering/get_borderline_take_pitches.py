import pandas as pd
import numpy as np


def get_all_pitches(filePath):
    """
    Returns pandas dataframe of pitches
    """
    pitch_df = pd.read_parquet(filePath)
    return pitch_df

def filter_by_game_type(pitch_df, game_types):
    """
    Keep only rows whose game_type is in the provided list.

    Args:
        df (pandas DataFrame): input pitches dataframe
        game_types (list[str]): list of game types like ['R'] or ['R','W']

    Returns:
        filtered_by_game_type_df (pandas Dataframe of rows that only have the game type)
    """
    # Game type codes juist for reference
    GAME_TYPE_CODES = {
        "E": "Exhibition",
        "S": "Spring Training",
        "R": "Regular Season",
        "F": "Wild Card",
        "D": "Divisional Series",
        "L": "League Championship Series",
        "W": "World Series",
    }

    if "game_type" not in pitch_df.columns:
        raise KeyError("Missing required column: game_type")
    included_game_types = set(game_types)
    filtered_by_game_type_df = pitch_df[pitch_df['game_type'].isin(included_game_types)].copy()
    return filtered_by_game_type_df




# Maybe i could put all these helper function in a utils.py file and import it
def count_each_value(pitch_df, columnName):
    """
    Get the count of each pitch type
    """
    column_value_counts = pitch_df[columnName].value_counts()
    return column_value_counts

def get_unique_column_values(df, columnName):
    """
    Return a list of all possible 
    """
    unique_values = df[columnName].unique()
    return unique_values


def get_df_column_names(df):
    """
    Return a list of all the column names in a dataframe
    """
    column_names = df.columns.tolist()
    return column_names



def filter_taken_pitches(pitch_df):
    """
    Keep only TAKEN, CALLED pitches (no swings):
      - type in {'B','S'}
      - description in {'ball', 'blocked_ball', 'called_strike'}
    Also drop rows missing zone geometry needed downstream.
    """
    taken_pitches = pitch_df[
        (pitch_df["type"].isin(["B", "S"])) &
        (pitch_df["description"].isin(["ball", "blocked_ball", "called_strike"]))
    ].dropna(subset=["plate_x", "plate_z", "sz_top", "sz_bot"])

    return taken_pitches

def get_borderline_pitches(pitch_df, edge_margin_ft = 0.20, include_ball_diameter = True):
    """
    Return a pandas dataframe of only borderline pitches pitches.
    Batters have different strike zones based on their dimensions, so they must be used
        to calculate which pitches are borderline or not 

    Uses each pitch's own sz_top/sz_bot and plate_x/plate_z.
    A pitch is borderline if it's within edge_margin_ft of:
      - top or bottom edge (vertical) while horizontally inside the plate, or
      - side edge (inside/outside) while vertically between sz_bot and sz_top.

    Tunables (edit):
      edge_margin_ft: how close to the edge counts as 'borderline'
      include_ball_diameter: if True, expand plate half-width to include ball (~0.83 ft).
                             if False, use plate-only half-width (~0.7083 ft).
   

    Args:
        pitch_df (pandas dataFrame): dataframe of pitches in a season

        
    Returns:
        borderline_pitch_df (pandas dataFrame): 
    
    """

    # --- tunables ---
    # Tuning: change edge_margin_ft (0.15–0.25 is common) and toggle 
    # include_ball_diameter to switch between plate-only and plate+ball edge logic.
    
    


    # columns we need for borderline geometry
    need = ['sz_top', 'sz_bot', 'plate_x', 'plate_z']
    for c in need:
        if c not in pitch_df.columns:
            raise KeyError(f"Missing required column for borderline logic: {c}")
        
    # drop rows with missing geometry
    df = pitch_df.dropna(subset=need).copy()

    # choose plate half-width (ft)
    # plate-only half width ≈ 0.7083 ft (17"); plate+ball ≈ 0.83 ft (~10" each side)
    half_plate_ft = 0.83 if include_ball_diameter else 0.7083333333

    # vectorized series
    px = df['plate_x'].astype(float)
    pz = df['plate_z'].astype(float)
    top = df['sz_top'].astype(float)
    bot = df['sz_bot'].astype(float)

    # convenience masks
    inside_horiz = (np.abs(px) <= half_plate_ft)
    between_vert = (pz >= bot) & (pz <= top)



    # distance checks
    near_top = (np.abs(pz - top) <= edge_margin_ft) & inside_horiz
    near_bot = (np.abs(pz - bot) <= edge_margin_ft) & inside_horiz
    near_side = (np.abs(np.abs(px) - half_plate_ft) <= edge_margin_ft) & between_vert

    borderline_mask = near_top | near_bot | near_side

    # annotate reason (optional but handy)
    # if more than one edge qualifies, the first True in np.select will be used
    df['borderline_reason'] = np.select(
        [near_top, near_bot, near_side],
        ['near_top', 'near_bot', 'near_side'],
        default=''
    )

    # keep only borderline rows
    borderline_pitches = df[borderline_mask].copy()

    return borderline_pitches

def get_borderline_regular_season_pitches(file_path):
    """
    Filters a season of MLB pitches to only include pitches that are taken for a called strike/ball and are 'borderline'

    Args:
        file_path (string): a path to a parquet file full of pitch data for a season
    Returns:
        borderline_pitches_df (pandas DataFrane): a pandas dataframe where each row is a borderline strike/ball called pitch from the mlb regular season
    """

    # Read a parquet file to get all the pitches in a season including Spring Training, Regular Season, and Postseason
    full_season_pitches_df = get_all_pitches(file_path)
    print(f"Total number of pitches in {year} season including Spring Training, Regular Season, and Postseason: {full_season_pitches_df.shape[0]}")

    regular_season_game_types = ["R"]
    #post_season_game_types = ["F", "D", "L", "W"]
    # For regular season use ["R", "F", "D", "L", "W"]


    # Filter the pitches dataframe to only include regular season games
    regular_season_pitches_df = filter_by_game_type(full_season_pitches_df, regular_season_game_types)
    print(f"Total number of pitches after filtering for regular season games: {regular_season_pitches_df.shape[0]}")

    # Filter the pitches dataframe to only include pitches that were taken for a ball or a strike
    taken_pitche_df = filter_taken_pitches(regular_season_pitches_df)
    print(f"Total number of pitches taken for a called strike or ball in {year}: {taken_pitche_df.shape[0]}")


    # Filter pitches dataframe to only include pitches that are 'borderline' balls and strike
    borderline_pitch_df = get_borderline_pitches(taken_pitche_df)
    print("Number of taken pitches that can be considered borderline strike-ball:", borderline_pitch_df.shape[0])

    # print(borderline__pitch_df.head(10))
    return borderline_pitch_df


if __name__ == "__main__":

    year = 2023
    path = f"data/full_season_pitches/{year}_pitches.parquet"

    borderline_pitch_df = get_borderline_regular_season_pitches(path)
    print(borderline_pitch_df.head())
    print(borderline_pitch_df.columns.tolist())
    columns = [
        'sz_top', 'sz_bot', 'plate_x', 'plate_z', 'type', 'delta_run_exp',
        'borderline_reason',                 # from get_borderline_pitches
        'batter', 'pitcher', 'game_date', 'game_pk',
        # 'balls', 'strikes', 'zone', 'pitch_type', 'pitch_name',
        # 'inning', 'inning_topbot',
    ]


    filtered_df = borderline_pitch_df[columns]
    print(filtered_df.head())
    

    # delta_run_exp = Run Expectancy (after pitch) – Run Expectancy (before pitch)
    # measures how the expected number of runs in the remainder of the inning changes because of one pitch’s outcome
