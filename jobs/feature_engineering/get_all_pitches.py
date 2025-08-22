import argparse
from pathlib import Path
from pybaseball import statcast
import pandas as pd

def fetch_season(year: int):
    start = f"{year}-01-01"
    end = f"{year}-12-31"
    print(f"> fetching {year} season ({start}→{end}) including Spring training, Regular Season, and Playoffs…")

    # 1) Figure out where "data/" lives (two levels up from this file)
    this_file = Path(__file__).resolve()
    project_root = this_file.parent.parent.parent
    data_dir = project_root / "data" / "full_season_pitches"

    # 2) Make sure it exists
    data_dir.mkdir(exist_ok=True)

    # 3) Fetch and write
    df = statcast(start_dt=start, end_dt=end)
    out_path = data_dir / f"{year}_pitches.parquet"
    df.to_parquet(out_path)
    print(f"> saved {df.shape[0]} pitches to {out_path}")

if __name__ == "__main__":
    # Create command line tool to get pitches for a given year, or 2024 by default
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2024,
                   help="Which season year to fetch")
    args = p.parse_args()
    fetch_season(args.year)
