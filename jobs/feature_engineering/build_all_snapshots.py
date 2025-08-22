import argparse
import time
from pathlib import Path
import pandas as pd

from build_player_snapshot import build_player_snapshot


def build_all_snapshots(year):
    """
    Read data/full_season_pitches/{year}_pitches.parquet to get distinct batters,
    build season-start snapshots for each, and write:
      data/lookups/player_snapshots_{year}.parquet
    """
    this_file = Path(__file__).resolve()
    project_root = this_file.parents[2]

    pitches_path = project_root / "data" / "full_season_pitches" / f"{year}_pitches.parquet"
    lookups_dir = project_root / "data" / "lookups"
    lookups_dir.mkdir(parents=True, exist_ok=True)

    if not pitches_path.exists():
        raise FileNotFoundError(f"Missing parquet: {pitches_path}")

    print(f"> reading pitches: {pitches_path}")
    df = pd.read_parquet(pitches_path, columns=["batter"])

    # get unique batters
    batter_series = df["batter"].dropna()
    batter_series = batter_series.astype("int64")
    unique_batters = batter_series.unique().tolist()
    unique_batters.sort()

    total = len(unique_batters)
    print(f"> found {total} unique batters in {year}")

    snapshots = []
    processed = 0
    start_time = time.time()

    for idx, batter_id in enumerate(unique_batters, start=1):
        print(f"    → currently fetching snapshot {idx}/{total} for batter {batter_id} for {year} season")
        try:
            snap = build_player_snapshot(batter_id, year)
            # try to show the player's name if we have it
            player_name = None
            try:
                player_name = snap.iloc[0].get("full_name")
            except Exception:
                player_name = None
            if player_name:
                print(f"      ✓ Career snapshot prior to {year} season fetched for {player_name}")
            else:
                print(f"      ✓ fetched snapshot but couldn't fetch player name")
            snapshots.append(snap)
        except Exception as e:
            print(f"      ✗ failed for batter {batter_id}: {e}")
            continue

        processed += 1
        if processed % 200 == 0 or processed == total:
            elapsed = time.time() - start_time
            print(f"      • built {processed}/{total} snapshots in {elapsed:0.1f}s")

    if len(snapshots) == 0:
        raise RuntimeError("No snapshots were built.")

    out_df = pd.concat(snapshots, axis=0)
    out_path = lookups_dir / f"player_snapshots_{year}.parquet"
    out_df.to_parquet(out_path)
    print(f"> wrote {out_df.shape[0]} rows to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build all player snapshots for a season.")
    parser.add_argument("--year", type=int, default=2024, help="Season year (e.g., 2024)")
    args = parser.parse_args()
    build_all_snapshots(args.year)
