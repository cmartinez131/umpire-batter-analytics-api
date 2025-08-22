"""
Compute a 'veteran_metric' (0–100) from season-start snapshots saved as:
  data/lookups/player_snapshot_{batter_id}_{season}.parquet

Signals used (all ..._prior through season-1):
- Years of service
- Career plate appearances (log-scaled)
- All-Star selections
- Career WAR (Baseball-Reference bWAR)
- Award points (MVP, Gold/Platinum Glove, Silver Slugger, Hank Aaron Award,
  All-MLB First/Second, HR Derby champion, AL/NL Rookie of the Year)

Hard-coded demo uses Aaron Judge (592450). Edit SEASONS as needed.
"""

from pathlib import Path
import numpy as np
import pandas as pd


# -----------------------
# Tunable scoring knobs
# -----------------------
WEIGHTS = {
    # Must sum to 1.0
    "tenure":      0.30,  # years of service
    "volume":      0.20,  # plate appearances (log)
    "allstar":     0.12,  # All-Star selections
    "performance": 0.18,  # WAR
    "awards":      0.20,  # composite awards
}

CAPS = {
    "years_service": 20.0,     # 20 yrs → cap
    "pa_log_k":      10000.0,  # ~10k PA ≈ near cap
    "allstar":       10.0,     # 10 ASG → cap
    "war":           60.0,     # 60 WAR → cap
    "award_points":  20.0,     # 20 points → cap
}

AWARD_POINTS = {
    "mvps_prior":               4.0,
    "hank_aaron_awards_prior":  2.5,
    "silver_sluggers_prior":    1.5,
    "gold_gloves_prior":        1.2,
    "platinum_gloves_prior":    1.7,
    "allmlb_first_team_prior":  1.5,
    "allmlb_second_team_prior": 1.0,
    "al_roty_prior":            1.2,
    "nl_roty_prior":            1.2,
    "hr_derby_titles_prior":    0.5,
}


# -----------------------
# Helpers (no leading underscores)
# -----------------------
def nz_float(x) -> float:
    """Non-negative float; missing/NaN → 0.0."""
    try:
        if x is None:
            return 0.0
        v = float(x)
        if np.isnan(v) or v < 0:
            return 0.0
        return v
    except Exception:
        return 0.0


def scale_min_cap(value: float, cap: float) -> float:
    """Linear scale to [0,1] with cap."""
    if cap <= 0:
        return 0.0
    v = max(0.0, min(value, cap))
    return v / cap


def scale_log_pa(pa: float, k: float) -> float:
    """log1p(PA)/log1p(k)."""
    if k <= 0:
        return 0.0
    return float(np.log1p(max(0.0, pa)) / np.log1p(k))


def award_points_from_row(row: pd.Series) -> float:
    total = 0.0
    for col, pts in AWARD_POINTS.items():
        total += pts * nz_float(row.get(col, 0))
    return total


# -----------------------
# Core metric
# -----------------------
def compute_veteran_score_from_row(row: pd.Series) -> float:
    """Return 0–100 veteran_metric on an objective scale."""
    svc = nz_float(row.get("years_service_prior", 0))
    pa  = nz_float(row.get("pa_career_prior", 0))
    asg = nz_float(row.get("allstar_prior", 0))
    war = nz_float(row.get("war_career_prior", 0))

    svc_s = scale_min_cap(svc, CAPS["years_service"])
    pa_s  = scale_log_pa(pa, CAPS["pa_log_k"])
    asg_s = scale_min_cap(asg, CAPS["allstar"])
    war_s = scale_min_cap(war, CAPS["war"])

    aw_pts = award_points_from_row(row)
    aw_s   = scale_min_cap(aw_pts, CAPS["award_points"])

    score01 = (
        WEIGHTS["tenure"]      * svc_s +
        WEIGHTS["volume"]      * pa_s  +
        WEIGHTS["allstar"]     * asg_s +
        WEIGHTS["performance"] * war_s +
        WEIGHTS["awards"]      * aw_s
    )
    score01 = min(max(score01, 0.0), 1.0)
    return float(round(100.0 * score01, 1))


# -----------------------
# I/O
# -----------------------
def load_snapshot(lookups: Path, batter_id: int, season: int) -> pd.DataFrame | None:
    """
    Load data/lookups/player_snapshot_{batter_id}_{season}.parquet
    and ensure index/season are set.
    """
    p = lookups / f"player_snapshot_{batter_id}_{season}.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    if "batter" in df.columns:
        df = df.set_index("batter")
    if "season" not in df.columns:
        df["season"] = season
    else:
        df["season"] = season
    return df


def load_snapshots_for_player(batter_id: int, seasons: list[int]) -> pd.DataFrame:
    """Concat one row per season; compute veteran_metric column."""
    # friendlier terminal printing
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.expand_frame_repr", False)

    root = Path(__file__).resolve().parents[2]
    lookups = root / "data" / "lookups"

    rows, missing = [], []
    for yr in seasons:
        df = load_snapshot(lookups, batter_id, yr)
        if df is None:
            missing.append(yr)
        else:
            rows.append(df)

    if missing:
        print("WARN: missing snapshots for seasons:", missing)
    if not rows:
        print("ERROR: no snapshots loaded. Aborting.")
        return pd.DataFrame()

    out = pd.concat(rows, axis=0)
    out["veteran_metric"] = out.apply(compute_veteran_score_from_row, axis=1)
    return out.sort_values("season").copy()


# -----------------------
# Script entry
# -----------------------
def main():
    batter_id = 592450       # Aaron Judge
    SEASONS   = [2025]       # edit this list to seasons you have locally

    df = load_snapshots_for_player(batter_id, SEASONS)
    if df.empty:
        return

    summary_cols = [
        "full_name", "season",
        "years_service_prior", "pa_career_prior", "war_career_prior",
        "allstar_prior", "mvps_prior",
        "gold_gloves_prior", "platinum_gloves_prior", "silver_sluggers_prior",
        "hank_aaron_awards_prior", "allmlb_first_team_prior", "allmlb_second_team_prior",
        "hr_derby_titles_prior", "al_roty_prior", "nl_roty_prior",
        "hr_career_prior", "avg_career_prior",
        "veteran_metric",
    ]
    existing_cols = [c for c in summary_cols if c in df.columns]
    summary = df[existing_cols].copy()

    print("\n===== VETERAN METRIC SUMMARY =====")
    print(summary.to_string(index=True))

    print("\n===== PER-SEASON VERTICAL VIEW =====")
    for yr in summary["season"].tolist():
        row = df[df["season"] == yr].iloc[0]
        print(f"\n--- {int(yr)} ---")
        print(row.to_frame(name="value"))

    print("\n===== TREND =====")
    trend = summary[["season", "veteran_metric"]].set_index("season")
    print(trend.to_string())


if __name__ == "__main__":
    main()
