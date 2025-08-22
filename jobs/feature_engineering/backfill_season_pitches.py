import time
import pandas as pd
import argparse
from pathlib import Path

# Local imports from this same folder
from get_all_pitches import fetch_season as fetch_season_pitches
from get_all_hp_umpires import fetch_season as fetch_season_umpires


def stitch_umpire_info(year: int):
    """
    Read {year}_pitches.parquet and {year}_hp_umpires.parquet,
    merge by game_pk, fill legacy 'umpire' column, and write back.

    Note:
      - 'umpire' is deprecated in pybaseball's statcast; we fetch HP umpires
        from MLB StatsAPI and stitch them onto the pitches using game_pk.
    """
    this_file = Path(__file__).resolve()
    project_root = this_file.parents[2]

    pitch_path = project_root / "data" / "full_season_pitches" / f"{year}_pitches.parquet"
    umpire_path = project_root / "data" / "full_season_umpires" / f"{year}_hp_umpires.parquet"

    if not pitch_path.exists():
        raise FileNotFoundError(f"Missing pitches parquet: {pitch_path}")
    if not umpire_path.exists():
        raise FileNotFoundError(f"Missing hp umpire parquet: {umpire_path}")

    pitches = pd.read_parquet(pitch_path)
    umpires = pd.read_parquet(umpire_path)

    # Keep necessary columns, 1 row per game
    umpires = (
        umpires[["game_pk", "home_plate_umpire_id", "home_plate_umpire_name"]]
        .drop_duplicates(subset=["game_pk"])
        .copy()
    )

    # ensure types align
    pitches["game_pk"] = pitches["game_pk"].astype("int64")
    umpires["game_pk"] = umpires["game_pk"].astype("int64")
    umpires["home_plate_umpire_id"] = umpires["home_plate_umpire_id"].astype("Int64")

    # Merge pitches + HP umpire by game_pk
    merged_df = pitches.merge(umpires, on="game_pk", how="left")

    # Backfill legacy 'umpire' column with joined id
    if "umpire" in merged_df.columns:
        merged_df["umpire"] = merged_df["umpire"].astype("Int64")
        merged_df["umpire"] = merged_df["umpire"].fillna(merged_df["home_plate_umpire_id"])
    else:
        merged_df["umpire"] = merged_df["home_plate_umpire_id"].astype("Int64")

    # coverage print
    attached = int(merged_df["home_plate_umpire_id"].notna().sum())
    total = len(merged_df)
    print(f"> stitched hp umpires: {attached}/{total} rows ({attached/total:.1%})")

    merged_df.to_parquet(pitch_path, index=False)
    print(f"> updated {pitch_path.name} with umpire columns")
    print(merged_df[["game_pk", "home_plate_umpire_id", "umpire"]].dtypes)
    # Expected: int64, Int64, Int64


def run_year(year: int):
    """
    Full pipeline for a single season:
      1) fetch pitches → data/full_season_pitches/{year}_pitches.parquet
      2) fetch HP umpires → data/full_season_umpires/{year}_hp_umpires.parquet
      3) stitch umpire info onto pitches parquet
    """
    print(f"\n===== {year} =====")
    fetch_season_pitches(year)
    fetch_season_umpires(year)
    stitch_umpire_info(year)


def fetch_previous_seasons(start_year: int = 2016, end_year: int = 2024):
    """
    Loop across a range of seasons and run the full pipeline per year.
    Retries each failed year up to 3x with simple backoff.
    """
    errors = []
    successes = []

    for year in range(start_year, end_year + 1):
        print(f"\n==== {year} pitches ====")
        try:
            run_year(year)
            successes.append(year)
        except Exception as e:
            print(f"Error for year {year}: {e}")
            errors.append((year, str(e)))

    # Retry any failures (up to 3 times each)
    if errors:
        print("\n===== Retrying failed years =====")
        remaining = []
        for error_year, _ in errors:
            attempt = 1
            last_err = None
            while attempt <= 3:
                try:
                    time.sleep(2 * attempt)  # simple backoff
                    print(f"Retry {attempt} → {error_year}")
                    run_year(error_year)
                    print(f"Retry success: {error_year}")
                    successes.append(error_year)
                    break
                except Exception as e:
                    last_err = str(e)
                    print(f"Retry failed ({error_year}): {last_err}")
                    attempt += 1
            else:
                remaining.append((error_year, last_err))
        errors = remaining

    # Summary
    print("\n===== SUMMARY =====")
    if successes:
        unique_successes = sorted(set(successes))
        print(f"Years processed: {unique_successes}")
    else:
        print("No successful years processed.")

    if errors:
        errors = sorted(errors, key=lambda x: x[0])
        print(f"Failed to process {len(errors)} year(s):")
        for error_year, err in errors:
            print(f"- {error_year}: {err}")
    else:
        print("No failures.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch season pitches + HP umpires and stitch umpire IDs onto pitches."
    )
    parser.add_argument("--year", type=int, help="Single season year to process (e.g., 2024)")
    parser.add_argument("--start", type=int, help="Start year for multi-year backfill (e.g., 2018)")
    parser.add_argument("--end", type=int, help="End year for multi-year backfill (e.g., 2024)")
    args = parser.parse_args()

    if args.year is not None:
        run_year(int(args.year))
    else:
        if args.start is None or args.end is None:
            parser.error("Provide --year OR both --start and --end.")
        if args.start > args.end:
            raise SystemExit(f"--start ({args.start}) must be <= --end ({args.end}).")
        fetch_previous_seasons(int(args.start), int(args.end))
