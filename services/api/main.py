import os
from pathlib import Path
from functools import lru_cache

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

# --- project paths ---
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
# allow override at deploy time: DATA_ROOT=/data
DATA_ROOT = Path(os.getenv("DATA_ROOT", PROJECT_ROOT / "data")).resolve()

# --- import your helpers ---
from jobs.feature_engineering.calculate_veteran import compute_veteran_score_from_row
from jobs.feature_engineering.get_borderline_take_pitches import (
    filter_taken_pitches,
    get_borderline_pitches,
)

app = FastAPI(title="UBR API", version="0.1.0")


# ---------------------------
# Small helpers
# ---------------------------
def available_snapshot_years():
    """
    Look for data/lookups/player_snapshots_{YYYY}.parquet and return sorted years.
    """
    years = set()
    lookups = DATA_ROOT / "lookups"
    if lookups.exists():
        for p in lookups.glob("player_snapshots_*.parquet"):
            # e.g., player_snapshots_2024.parquet
            stem = p.stem  # player_snapshots_2024
            try:
                yr = int(stem.split("_")[-1])
                years.add(yr)
            except Exception:
                pass
    return sorted(years)


def latest_available_season(default_fallback=2024):
    yrs = available_snapshot_years()
    return yrs[-1] if yrs else default_fallback


# ---------------------------
# Data loaders (cached)
# ---------------------------
@lru_cache(maxsize=8)
def load_snapshots(year: int) -> pd.DataFrame:
    """
    Read data/lookups/player_snapshots_{year}.parquet.
    Ensure index is 'batter'.
    """
    path = DATA_ROOT / "lookups" / f"player_snapshots_{year}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing snapshots parquet: {path}")
    df = pd.read_parquet(path)
    if "batter" in df.columns:
        df = df.set_index("batter")
    return df


@lru_cache(maxsize=3)
def load_pitches(year: int) -> pd.DataFrame:
    """
    Read data/full_season_pitches/{year}_pitches.parquet and
    return only regular-season, taken/called pitches with geometry present.
    """
    path = DATA_ROOT / "full_season_pitches" / f"{year}_pitches.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing pitches parquet: {path}")
    # read minimally first to discover columns (cheap)
    head = pd.read_parquet(path, rows=1) if hasattr(pd, "read_parquet") else pd.read_parquet(path, nrows=1)
    cols = head.columns.tolist()

    needed = [
        "game_type", "type", "description",
        "plate_x", "plate_z", "sz_top", "sz_bot",
        "batter", "umpire", "delta_run_exp"
    ]
    use_cols = [c for c in needed if c in cols]
    df = pd.read_parquet(path, columns=use_cols)

    # regular season only
    df = df[df["game_type"] == "R"].copy()

    # taken/called only + geometry present
    df = filter_taken_pitches(df)
    return df


# ---------------------------
# Endpoints
# ---------------------------

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics/vp")
def get_vp_all(season: int = Query(None, ge=1900, le=2100)):
    """
    Return Veteran Presence (0–100) for all batters in a given season.
    If season omitted, uses the latest available snapshots parquet.
    """
    season = season or latest_available_season()

    try:
        snaps = load_snapshots(season)
    except FileNotFoundError as e:
        avail = available_snapshot_years()
        raise HTTPException(status_code=404, detail=f"{e}. Available seasons: {avail}")

    df = snaps.copy()
    df["vp"] = df.apply(compute_veteran_score_from_row, axis=1)
    return df.reset_index()[["batter", "full_name", "vp"]].to_dict(orient="records")


# VP by batter id
@app.get("/metrics/vp/{batter_id}")
def get_vp_by_id(batter_id: int, season: int = Query(None, ge=1900, le=2100)):
    """
    Return VP for a single batter id. Season optional (defaults to latest available).
    """
    season = season or latest_available_season()

    try:
        snaps = load_snapshots(season)
    except FileNotFoundError as e:
        avail = available_snapshot_years()
        raise HTTPException(status_code=404, detail=f"{e}. Available seasons: {avail}")

    if batter_id not in snaps.index:
        raise HTTPException(status_code=404, detail=f"batter {batter_id} not found in {season}")

    row = snaps.loc[batter_id]
    vp = compute_veteran_score_from_row(row)
    return {
        "batter_id": batter_id,
        "season": season,
        "full_name": row.get("full_name"),
        "vp": vp,
    }


@app.get("/metrics/ubr")
def get_ubr(batter_id: int, umpire_id: int, season: int = Query(..., ge=1900, le=2100)):
    """
    Compute simple UBR slices on the fly from stitched pitches:
    - restrict to borderline pitches
    - return called-strike rate, avg delta_run_exp, sample size
    """
    try:
        df = load_pitches(season)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    sub = df[(df["batter"] == batter_id) & (df["umpire"] == umpire_id)].copy()
    if sub.empty:
        return {
            "batter_id": batter_id,
            "umpire_id": umpire_id,
            "season": season,
            "samples": 0,
            "borderline_cs_rate": None,
            "delta_re_borderline": None,
        }

    borderline = get_borderline_pitches(sub)
    samples = int(borderline.shape[0])
    if samples == 0:
        return {
            "batter_id": batter_id,
            "umpire_id": umpire_id,
            "season": season,
            "samples": 0,
            "borderline_cs_rate": None,
            "delta_re_borderline": None,
        }

    cs_rate = float((borderline["description"] == "called_strike").mean())
    if "delta_run_exp" in borderline.columns:
        dre = float(borderline["delta_run_exp"].fillna(0).mean())
    else:
        dre = None

    return {
        "batter_id": batter_id,
        "umpire_id": umpire_id,
        "season": season,
        "samples": samples,
        "borderline_cs_rate": round(cs_rate, 3),
        "delta_re_borderline": None if dre is None else round(dre, 4),
    }
