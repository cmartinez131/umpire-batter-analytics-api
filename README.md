# MLB Umpire–Batter Analytics (UBR + VP)

> Quantifying whether reputation bends the strike zone — and serving it over an API.

This project is a small, reproducible analytics stack that (a) **scores hitter reputation** and (b) **measures how specific umpires call borderline takes** against specific batters. It’s built to run locally with plain Python, write everything to **Parquet**, and expose a thin **FastAPI** for quick queries and demos.

**What it does:**

1. **Veteran Presence (VP)** — A **0–100** résumé score computed from a batter’s history **through the prior season** (years of service, PA volume, bWAR, All-Star nods, major awards). Use it to stratify analyses or serve per-batter scores.
2. **Umpire–Batter Rapport (UBR)** — Empirical summaries on **borderline, taken** pitches for any *batter–umpire–season*:

   * Merges Statcast pitch-by-pitch with home-plate umpire IDs from MLB StatsAPI, stitched by `game_pk`.
   * Focuses on called/taken pitches within a narrow band around the zone edge.
   * Reports things like **borderline called-strike rate** and average **Δ run expectancy**.

**At a glance**

* **Data sources:** pybaseball Statcast + MLB StatsAPI (GUMBO/boxscore) for HP umpire IDs.
* **Storage:** Seasoned, columnar **Parquet** files under `data/` (no database required but may add later).
* **Reproducibility:** Deterministic build scripts; snapshots avoid future data leakage.
* **API:** Lightweight FastAPI app that reads Parquet and returns VP/UBR on demand.
* **Public endpoints:**

  * `GET /metrics/vp` → VP for **all** batters in a season (season optional; defaults to latest available).
  * `GET /metrics/vp/{batter_id}` → VP for a **single** batter (season optional; defaults to latest available).
* **Scope:** Works out of the box for **2016–2024 seasons** (extendable), single season or ranges.

Everything here is intentionally simple: **Python + Pandas** for transforms, **Parquet** for speed/portability, and a minimal **HTTP API** for sharing results.

---

## Table of Contents

* [What’s here](#whats-here)
* [Project structure](#project-structure)
* [Setup](#setup)
* [Build the data (step-by-step)](#build-the-data-step-by-step)
* [Command reference](#command-reference)
* [Run the API](#run-the-api)
* [API endpoints](#api-endpoints)
* [Veteran Presence (VP) formula](#veteran-presence-vp-formula)
* [Borderline pitch definition (UBR)](#borderline-pitch-definition-ubr)
* [Data layout (Parquet)](#data-layout-parquet)
* [Troubleshooting](#troubleshooting)
* [Requirements](#requirements)
* [License](#license)

---

## What’s here

* **ETL to Parquet**: fetch full-season Statcast, pull home-plate (HP) umpires from MLB StatsAPI (GUMBO/boxscore), stitch by `game_pk`.
* **Season-start snapshots** (one row per batter per season, **through prior season**): name, debut, PA, WAR, awards, etc.
* **VP metric**: composited from years, PA, ASG, WAR, and award points.
* **UBR slices**: filter to *taken* pitches near the strike-zone edge and summarize by batter–umpire.
* **FastAPI service**: read Parquet, compute VP and UBR on request.

---

## Project structure

```
.
├─ data/                         # generated Parquet (not committed)
│  ├─ full_season_pitches/{year}_pitches.parquet
│  ├─ full_season_umpires/{year}_hp_umpires.parquet
│  └─ lookups/player_snapshots_{year}.parquet
├─ jobs/
│  ├─ feature_engineering/
│  │  ├─ get_all_pitches.py
│  │  ├─ get_all_hp_umpires.py
│  │  ├─ backfill_season_pitches.py      # fetch pitches + HP umps + stitch (single year or range; default 2016–2024)
│  │  ├─ build_player_snapshot.py
│  │  ├─ build_all_snapshots.py
│  │  ├─ get_borderline_take_pitches.py
│  │  └─ calculate_veteran.py            # VP scoring helpers
│  └─ experiments/                       # scratch / prototypes
├─ services/
│  └─ api/
│     ├─ main.py                         # FastAPI app
│     └─ Dockerfile                      
├─ requirements.txt
└─ README.md
```

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

> I keep `data/` out of git. Add/keep `data/` in `.gitignore`.

---

## Build the data (step-by-step)

Pick a season (change the year as you like).

```bash
# 1) One-shot backfill for a single season (fetch pitches + HP umps + stitch onto pitches)
python jobs/feature_engineering/backfill_season_pitches.py --year 2024

# 2) Build batter snapshots (career THROUGH the PRIOR season only)
python jobs/feature_engineering/build_all_snapshots.py --year 2024
```

**Range runs (heavy):**

```bash
# Default full range (2016..2024)
python jobs/feature_engineering/backfill_season_pitches.py

# Custom range (inclusive)
python jobs/feature_engineering/backfill_season_pitches.py --start 2019 --end 2021

# Single year via range (same as --year 2024)
python jobs/feature_engineering/backfill_season_pitches.py --start 2024 --end 2024
```

> Heads up: Running many seasons can take hours (network + API latency + disk). Re-runs overwrite the same Parquet files for those years.

---

## Command reference

### Backfill (fetch + stitch) — recommended

```bash
# Single season
python jobs/feature_engineering/backfill_season_pitches.py --year 2024

# Default full range (2016..2024)
python jobs/feature_engineering/backfill_season_pitches.py

# Custom range (inclusive)
python jobs/feature_engineering/backfill_season_pitches.py --start 2019 --end 2021
```

* Writes/updates:

  * `data/full_season_pitches/{YEAR}_pitches.parquet`
  * `data/full_season_umpires/{YEAR}_hp_umpires.parquet`
  * Stitches `home_plate_umpire_id/name` and backfills legacy `umpire` in the pitches parquet.

### Pitches (fetch-only)

```bash
# Note: umpire fields will be null until you stitch
python jobs/feature_engineering/get_all_pitches.py --year 2024
```

### HP Umpires (fetch-only)

```bash
python jobs/feature_engineering/get_all_hp_umpires.py --year 2024
```

### Stitch only (advanced; use if you already fetched both inputs)

```bash
python -c "from jobs.feature_engineering.backfill_season_pitches import stitch_umpire_info; stitch_umpire_info(2024)"
```

### Snapshots for **all** batters in a season

```bash
python jobs/feature_engineering/build_all_snapshots.py --year 2024
```

* Writes `data/lookups/player_snapshots_2024.parquet`.

### Snapshot for **one** batter

Get a snapshot of the players's Resume at a specific point in their career. The result will be a parquet file with the player and the following information up to the input year (or present 2025) by default. The following information will be included in the result as a single row parquet file.

Columns:

* full\_name
* al\_allstar\_prior
* nl\_allstar\_prior
* allstar\_prior
* years\_service\_prior
* games\_played\_prior
* pa\_career\_prior
* hr\_career\_prior
* hits\_career\_prior
* ab\_career\_prior
* avg\_career\_prior
* al\_mvps\_prior
* nl\_mvps\_prior
* mvps\_prior
* gold\_gloves\_prior
* platinum\_gloves\_prior
* silver\_sluggers\_prior
* hank\_aaron\_awards\_prior
* allmlb\_first\_team\_prior
* allmlb\_second\_team\_prior
* hr\_derby\_titles\_prior
* al\_roty\_prior
* nl\_roty\_prior
* war\_career\_prior       (Baseball-Reference bWAR via pybaseball)
* key\_bbref              (via pybaseball)
* key\_fangraphs          (via pybaseball)
* season

```bash
python jobs/feature_engineering/build_player_snapshot.py --name "Shohei Ohtani" --season 2024
# or
python jobs/feature_engineering/build_player_snapshot.py --batter 592450 --season 2024
```

* Prints the single-row frame and (with `--save`) writes `data/lookups/player_snapshot_{mlbam}_{season}.parquet`.

### Batch-compute VP and attach to a snapshots parquet (optional CLI one-liner)

```bash
python - <<'PY'
import pandas as pd
from jobs.feature_engineering.calculate_veteran import compute_veteran_score_from_row
df = pd.read_parquet("data/lookups/player_snapshots_2024.parquet")
df["vp"] = df.apply(compute_veteran_score_from_row, axis=1)
print(df[["full_name","vp"]].head())
df.to_parquet("data/lookups/player_snapshots_2024_with_vp.parquet")
PY
```

---

## Run the API

The app reads Parquet from `DATA_ROOT` (defaults to `<repo>/data`).

```bash
# from repo root
uvicorn services.api.main:app --reload --port 8000

# or point at another data folder
DATA_ROOT=/abs/path/to/data uvicorn services.api.main:app --reload --port 8000

# docs
open http://127.0.0.1:8000/docs
```

---

## API endpoints

### Health

```
GET /health
→ {"status": "ok"}
```

### Veteran Presence (all batters)

```
GET /metrics/vp?season=YYYY
# season is optional; defaults to latest available snapshots (e.g., 2024)
```

Returns a list of `{batter, full_name, vp}` for everyone in that snapshots parquet.

### Veteran Presence (single batter by id)

```
GET /metrics/vp/{batter_id}?season=YYYY
# season is optional; defaults to latest available snapshots (e.g., 2024)
```

Example response:

```json
{
  "batter_id": 592450,
  "season": 2024,
  "full_name": "Aaron Judge",
  "vp": 86.5
}
```

### UBR slice (borderline, taken)

```
GET /metrics/ubr?batter_id=592450&umpire_id=4552&season=2024
```

Returns:

```json
{
  "batter_id": 592450,
  "umpire_id": 4552,
  "season": 2024,
  "samples": 37,
  "borderline_cs_rate": 0.432,
  "delta_re_borderline": 0.0183
}
```

---

## Veteran Presence (VP) formula

Defined in `jobs/feature_engineering/calculate_veteran.py`. Output is **0–100**.

**Weights (must sum to 1):**

* Tenure (years of service) — **0.30**
* Volume (log plate appearances) — **0.20**
* All-Star selections — **0.12**
* Performance (Baseball-Reference bWAR) — **0.18**
* Awards composite — **0.20**

**Caps (for scaling to \[0,1]):**

* years = 20
* PA log scale *k* = 10,000
* All-Star = 10
* WAR = 60
* award points = 20

**Award points (each count × weight):**

* MVP **4.0**
* Hank Aaron **2.5**
* Silver Slugger **1.5**
* Gold Glove **1.2**
* Platinum Glove **1.7**
* All-MLB First **1.5**
* All-MLB Second **1.0**
* ROY (AL/NL) **1.2**
* HR Derby champ **0.5**

Use it programmatically:

```python
from jobs.feature_engineering.calculate_veteran import compute_veteran_score_from_row
vp = compute_veteran_score_from_row(a_snapshot_row)
```

---

## Borderline pitch definition (UBR)

From `jobs/feature_engineering/get_borderline_take_pitches.py`.

* Only **taken/called** pitches:

  * `type` in `{ "B", "S" }`
  * `description` in `{ "ball", "blocked_ball", "called_strike" }`
* A pitch is **borderline** if it’s within **0.20 ft** of:

  * the **top** or **bottom** of that batter’s zone (`sz_top`, `sz_bot`) while horizontally inside the plate, **or**
  * the **left/right edge** of the plate (using half-plate ≈ **0.83 ft** to account for the ball diameter) while vertically *between* `sz_bot` and `sz_top`.

The API then summarizes borderline called-strike rate and mean `delta_run_exp`.

---

## Data layout (Parquet)

```
data/
├─ full_season_pitches/
│  └─ 2024_pitches.parquet
├─ full_season_umpires/
│  └─ 2024_hp_umpires.parquet
└─ lookups/
   ├─ player_snapshots_2024.parquet
   └─ player_snapshot_{batter}_{season}.parquet  # optional singles
```

**Pitches** should include at least:
`game_type, type, description, plate_x, plate_z, sz_top, sz_bot, batter, umpire, delta_run_exp, game_pk`

**Snapshots** include:
`full_name, years_service_prior, pa_career_prior, ab_career_prior, hits_career_prior, hr_career_prior, avg_career_prior, allstar_prior (+ AL/NL), mvps_prior (+ AL/NL), gold_gloves_prior, platinum_gloves_prior, silver_sluggers_prior, hank_aaron_awards_prior, allmlb_first_team_prior, allmlb_second_team_prior, hr_derby_titles_prior, al_roty_prior, nl_roty_prior, war_career_prior, season`

> Snapshots are always **through prior season** to avoid data leakage into the future.

---

## Troubleshooting

* **bWAR returns 0 with a parse error**
  `pybaseball.bwar_bat()` sometimes stumbles on a cached CSV (you’ll see a tokenizing error).
  Fix: blow away the cache:

  ```bash
  rm -rf ~/.pybaseball
  ```

  My code already falls back to `WAR=0.0` so pipelines won’t crash.

* **No umpire ids on pitches**
  Use `backfill_season_pitches` (recommended), or if you fetched separately: fetch pitches → fetch HP umpires → `stitch_umpire_info(year)`.

* **Large files**
  Parquet + column pruning keeps things fast. The API only reads the columns it needs.

---

## Requirements

```
pandas
numpy
pyarrow
requests
pybaseball
fastapi
uvicorn
```

---

## License

MIT
