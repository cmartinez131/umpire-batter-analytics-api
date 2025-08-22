"""
Build a season-start snapshot for one batter_id and season.
All stats/awards are computed THROUGH THE PRIOR SEASON to avoid future data leakage.

Columns:
- full_name
- al_allstar_prior
- nl_allstar_prior
- allstar_prior
- years_service_prior
- games_played_prior
- pa_career_prior
- hr_career_prior
- hits_career_prior
- ab_career_prior
- avg_career_prior
- al_mvps_prior
- nl_mvps_prior
- mvps_prior
- gold_gloves_prior
- platinum_gloves_prior
- silver_sluggers_prior
- hank_aaron_awards_prior
- allmlb_first_team_prior
- allmlb_second_team_prior
- hr_derby_titles_prior
- al_roty_prior
- nl_roty_prior
- war_career_prior       (Baseball-Reference bWAR via pybaseball)
- key_bbref              (via pybaseball)
- key_fangraphs          (via pybaseball)
- season

Usage:
  python jobs/feature_engineering/build_player_snapshot.py --batter 592450 --season 2024
  python jobs/feature_engineering/build_player_snapshot.py --name "Shohei Ohtani" --season 2024
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ID mapping + BR WAR
from pybaseball import playerid_reverse_lookup, bwar_bat, playerid_lookup

BASE = "https://statsapi.mlb.com/api/v1"
HEADERS = {"User-Agent": "UBR/1.0 (+https://example.com)"}


def get_json(url, params=None, timeout=30):
    """GET a JSON payload with basic error handling."""
    response = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_batter_id_by_name(full_name):
    """
    Resolve a player's MLBAM id from a full name using pybaseball.playerid_lookup.
    Accepts 'First Last' or 'Last, First'. Returns int id or None if not found.
    """
    if not isinstance(full_name, str) or not full_name.strip():
        return None

    s = full_name.strip()
    if "," in s:
        last, first = [x.strip() for x in s.split(",", 1)]
    else:
        parts = s.split()
        if len(parts) >= 2:
            first = " ".join(parts[:-1])
            last = parts[-1]
        else:
            # single token: try as last name, fuzzy lookup
            first = ""
            last = parts[0]

    try:
        df = playerid_lookup(last, first, fuzzy=True)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # keep rows with an MLBAM id
    if "key_mlbam" not in df.columns:
        return None
    df = df[df["key_mlbam"].notna()]
    if df.empty:
        return None

    # prefer the most recent MLB player if we have years
    pick = None
    if "mlb_played_last" in df.columns and df["mlb_played_last"].notna().any():
        pick = df[df["mlb_played_last"].notna()].sort_values("mlb_played_last", ascending=False).iloc[0]
    elif "mlb_played_first" in df.columns and df["mlb_played_first"].notna().any():
        pick = df[df["mlb_played_first"].notna()].sort_values("mlb_played_first", ascending=False).iloc[0]
    else:
        pick = df.iloc[0]

    try:
        return int(pick["key_mlbam"])
    except Exception:
        try:
            return int(float(pick["key_mlbam"]))
        except Exception:
            return None


def awards_counts_mlb(batter_id, season_year):
    """
    Count awards from MLB StatsAPI through (season_year - 1):
      - AL/NL All-Star selections (MLB only; no MiLB all-stars)
      - BBWAA MVPs split by league (AL/NL), regular-season only
      - Gold Gloves
      - Platinum Gloves
      - Silver Sluggers
      - Hank Aaron Award (AL or NL)
      - All-MLB First Team
      - All-MLB Second Team
      - Home Run Derby winner/champion
      - Jackie Robinson Rookie of the Year (AL/NL)
    Returns:
      (al_allstar_prior, nl_allstar_prior,
       al_mvps_prior, nl_mvps_prior,
       gold_gloves_prior, platinum_gloves_prior, silver_sluggers_prior,
       hank_aaron_awards_prior,
       allmlb_first_team_prior, allmlb_second_team_prior,
       hr_derby_titles_prior,
       al_roty_prior, nl_roty_prior)
    """
    awards = []

    # try primary endpoint
    try:
        data = get_json(f"{BASE}/people/{batter_id}/awards")
        awards = data.get("awards", []) or []
    except requests.RequestException:
        awards = []

    # fallback hydrate
    if not awards:
        try:
            data = get_json(f"{BASE}/people/{batter_id}", params={"hydrate": "awards"})
            people = data.get("people", [])
            if people:
                person_obj = people[0]
                awards = person_obj.get("awards", []) or []
        except requests.RequestException:
            awards = []

    def get_award_year(award):
        val = award.get("season")
        if val is None:
            val = award.get("year")
        if val is not None:
            try:
                return int(str(val)[:4])
            except Exception:
                pass

        date_str = award.get("date")
        if isinstance(date_str, str) and len(date_str) >= 4:
            try:
                return int(date_str[:4])
            except Exception:
                return None
        return None

    def get_award_text(award):
        # combine a few fields to be robust to variants/abbreviations
        award_block = award.get("award") or {}
        parts = []

        name_val = award_block.get("name")
        if name_val:
            parts.append(name_val)

        short_val = award_block.get("shortName")
        if short_val:
            parts.append(short_val)

        id_val = award_block.get("id")
        if id_val:
            parts.append(str(id_val))

        top_name = award.get("name")
        if top_name:
            parts.append(top_name)

        # also include some common top-level text fields if present
        for key in ("title", "awardName", "description", "notes"):
            v = award.get(key)
            if isinstance(v, str) and v:
                parts.append(v)

        # include any league hints
        league_obj = award.get("league") or {}
        if isinstance(league_obj, dict):
            v = league_obj.get("name")
            if isinstance(v, str) and v:
                parts.append(v)
            v = league_obj.get("abbreviation") or league_obj.get("abbrev")
            if isinstance(v, str) and v:
                parts.append(v)

        team_obj = award.get("team") or {}
        if isinstance(team_obj, dict):
            t_league = team_obj.get("league") or {}
            if isinstance(t_league, dict):
                v = t_league.get("name")
                if isinstance(v, str) and v:
                    parts.append(v)
                v = t_league.get("abbreviation") or t_league.get("abbrev")
                if isinstance(v, str) and v:
                    parts.append(v)

        text = " ".join(parts).lower()
        return text

    def count_if(predicate_func):
        c = 0
        for a in awards:
            yr = get_award_year(a)
            if yr is not None and yr >= season_year:
                continue  # only count prior seasons
            t = get_award_text(a)
            try:
                if predicate_func(t):
                    c += 1
            except Exception:
                continue
        return c
    
    # # Debug text to output the awards
    # # uncomment to see list of awards
    # for a in awards:
    #     yr = get_award_year(a)
    #     if yr is not None and yr >= season_year:
    #         continue
    #     # Build the same 'text' as above and print it
    #     parts = []
    #     aw = a.get("award")
    #     if isinstance(aw, dict):
    #         for key in ("id", "shortName", "name"):
    #             v = aw.get(key)
    #             if isinstance(v, str) and v: parts.append(v)
    #     for key in ("name", "title", "awardName", "description", "notes"):
    #         v = a.get(key)
    #         if isinstance(v, str) and v: parts.append(v)
    #     t = " ".join(parts).lower()
    #     print("DEBUG TEXT:", yr, "→", t[:140])

    # ---------- AL/NL ALL-STAR (MLB ONLY) ----------
    def count_al_nl_allstar():
        """
        Count MLB All-Star selections by league:
          - Look for 'al all-star'/'nl all-star' or 'american/national league all-star'
          - Exclude minors by requiring AL/NL context
          - Dedupe by (league, year)
        """
        al = 0
        nl = 0
        al_years = set()
        nl_years = set()

        for a in awards:
            yr = get_award_year(a)
            if yr is None or yr >= season_year:
                continue

            t = get_award_text(a)
            padded = f" {t} "

            # must be an All-Star (avoid ASG MVP confusion handled by MVP logic anyway)
            if ("all-star" not in t) and ("all star" not in t):
                continue

            # Require AL/NL major-league context (filters out "FSL/TEX/NWL mid-season all-star", etc.)
            al_hit = (" al all-star" in padded) or ("american league all-star" in t)
            nl_hit = (" nl all-star" in padded) or ("national league all-star" in t)

            # Dedupe by league-year
            if al_hit and yr not in al_years:
                al_years.add(yr)
                al += 1
                continue
            if nl_hit and yr not in nl_years:
                nl_years.add(yr)
                nl += 1
                continue

        return al, nl

    # ---------- MVP SPLIT (AL/NL) ----------
    def count_al_nl_mvp():
        """
        - Count AL/NL regular-season MVPs (BBWAA) by looking for 'al mvp'/'nl mvp'
          or 'american league mvp'/'national league mvp' in award text.
        - Exclude ASG/WS/LCS/DS MVPs and non-BBWAA (Players Choice, MLB.com, team MVP).
        - Dedupe by (league, year).
        """
        al = 0
        nl = 0
        al_years = set()
        nl_years = set()

        for a in awards:
            yr = get_award_year(a)
            if yr is None or yr >= season_year:
                continue

            # Build text
            t = get_award_text(a)
            padded = f" {t} "

            # Exclude non-regular-season / non-BBWAA variants
            block = (
                "all-star", "all star",
                "world series", "alcs", "nlcs", "division series",
                "players choice", "players' choice", "player of the year",
                "team mvp", "mlb.com", "mlb dot com"
            )
            if any(b in t for b in block):
                continue

            # Simple league-specific matches
            al_hit = (" al mvp" in padded) or ("american league mvp" in t)
            nl_hit = (" nl mvp" in padded) or ("national league mvp" in t)

            # Accept explicit award IDs if present
            aw = a.get("award") or {}
            award_id = aw.get("id") or ""
            if isinstance(award_id, str):
                award_id = award_id.strip().upper()
            else:
                award_id = ""
            if award_id == "ALMVP":
                al_hit = True
            elif award_id == "NLMVP":
                nl_hit = True

            if al_hit and yr not in al_years:
                al_years.add(yr)
                al += 1
                continue

            if nl_hit and yr not in nl_years:
                nl_years.add(yr)
                nl += 1
                continue

        return al, nl

    # ---------- SIMPLE TEXT MATCHERS FOR OTHER AWARDS ----------
    def is_gold_glove_text(t):
        return "gold glove" in t

    def is_platinum_glove_text(t):
        return "platinum glove" in t

    def is_silver_slugger_text(t):
        return "silver slugger" in t

    def is_hank_aaron_text(t):
        return "hank aaron award" in t

    def is_allmlb_first_text(t):
        return ("all-mlb first team" in t) or ("all mlb first team" in t) or ("first team all-mlb" in t)

    def is_allmlb_second_text(t):
        return ("all-mlb second team" in t) or ("all mlb second team" in t) or ("second team all-mlb" in t)

    def is_hr_derby_champ_text(t):
        return ("home run derby" in t) and ("winner" in t or "champion" in t or "champ" in t)

    def is_al_roty_text(t):
        padded = f" {t} "
        if "rookie of the year" not in t:
            return False
        if "mvp" in t:
            return False
        return ("american league" in t) or (" al " in padded) or ("jackie robinson al rookie of the year" in t)

    def is_nl_roty_text(t):
        padded = f" {t} "
        if "rookie of the year" not in t:
            return False
        if "mvp" in t:
            return False
        return ("national league" in t) or (" nl " in padded) or ("jackie robinson nl rookie of the year" in t)

    # ----- perform counts -----
    al_allstar_prior, nl_allstar_prior = count_al_nl_allstar()
    al_mvps_prior, nl_mvps_prior = count_al_nl_mvp()
    gold_gloves_prior = count_if(is_gold_glove_text)
    platinum_gloves_prior = count_if(is_platinum_glove_text)
    silver_sluggers_prior = count_if(is_silver_slugger_text)
    hank_aaron_awards_prior = count_if(is_hank_aaron_text)
    allmlb_first_team_prior = count_if(is_allmlb_first_text)
    allmlb_second_team_prior = count_if(is_allmlb_second_text)
    hr_derby_titles_prior = count_if(is_hr_derby_champ_text)
    al_roty_prior = count_if(is_al_roty_text)
    nl_roty_prior = count_if(is_nl_roty_text)

    return (
        al_allstar_prior, nl_allstar_prior,
        al_mvps_prior, nl_mvps_prior,
        gold_gloves_prior, platinum_gloves_prior, silver_sluggers_prior,
        hank_aaron_awards_prior,
        allmlb_first_team_prior, allmlb_second_team_prior,
        hr_derby_titles_prior,
        al_roty_prior, nl_roty_prior,
    )


def hitting_totals_mlb(batter_id, season_year):
    """
    Sum MLB regular-season hitting splits through (season_year - 1) for MLB only (sportId=1).
    Returns: full_name, debut_year, games_played, plate_appearances, at_bats, hits, home_runs
    """
    full_name = None
    debut_year = np.nan

    # name + debut
    try:
        person = get_json(f"{BASE}/people/{batter_id}", params={"hydrate": "mlbDebutDate"})
        people = person.get("people", [])
        if people:
            p = people[0]
            full_name = p.get("fullName")
            debut_iso = p.get("mlbDebutDate")
            if debut_iso:
                try:
                    debut_year = int(debut_iso[:4])
                except Exception:
                    debut_year = np.nan
    except requests.RequestException:
        pass

    games_played = 0
    plate_appearances = 0
    at_bats = 0
    hits = 0
    home_runs = 0

    # year-by-year MLB regular-season splits
    try:
        payload = get_json(
            f"{BASE}/people/{batter_id}/stats",
            params={
                "group": "hitting",
                "stats": "yearByYear",
                "sportId": 1,
                "gameType": "R",  # explicit: regular season
            },
        )
        stats = payload.get("stats", [])
        splits = []
        if stats:
            first = stats[0]
            if first:
                splits = first.get("splits", [])

        for s in splits:
            y = s.get("season")
            try:
                y = int(y)
            except Exception:
                continue

            if y >= season_year:
                continue  # prior seasons only

            st = s.get("stat", {}) or {}

            val = st.get("gamesPlayed", st.get("games"))
            if val is None:
                val = 0
            games_played += int(val)

            val = st.get("plateAppearances", st.get("pa"))
            if val is None:
                val = 0
            plate_appearances += int(val)

            val = st.get("atBats", st.get("ab"))
            if val is None:
                val = 0
            at_bats += int(val)

            val = st.get("hits", st.get("h"))
            if val is None:
                val = 0
            hits += int(val)

            val = st.get("homeRuns", st.get("hr"))
            if val is None:
                val = 0
            home_runs += int(val)

    except requests.RequestException:
        pass

    return full_name, debut_year, games_played, plate_appearances, at_bats, hits, home_runs


def map_player_ids(mlbam_id):
    """
    Map MLBAM -> Baseball-Reference and FanGraphs IDs using pybaseball.
    Returns: (key_bbref, key_fangraphs, mlb_played_first)
    """
    try:
        df = playerid_reverse_lookup([mlbam_id], key_type="mlbam")
        if df is None or df.empty:
            return None, None, None

        key_bbref = None
        if "key_bbref" in df.columns:
            key_bbref = df.get("key_bbref").iloc[0]

        key_fangraphs = None
        if "key_fangraphs" in df.columns:
            val = df.get("key_fangraphs").iloc[0]
            if pd.notna(val):
                try:
                    key_fangraphs = int(val)
                except Exception:
                    key_fangraphs = None

        mlb_played_first = None
        if "mlb_played_first" in df.columns:
            val = df.get("mlb_played_first").iloc[0]
            if pd.notna(val):
                try:
                    mlb_played_first = int(val)
                except Exception:
                    mlb_played_first = None

        return key_bbref, key_fangraphs, mlb_played_first

    except Exception:
        return None, None, None


def war_prior_bref(key_bbref, season_year):
    """
    Sum Baseball-Reference bWAR through (season_year - 1).
    Returns a float, or 0.0 if not available.
    """
    if not key_bbref or pd.isna(key_bbref):
        return 0.0

    try:
        df = bwar_bat()  # pybaseball downloads once and caches
        if "player_ID" not in df.columns:
            return 0.0

        sub = df[df["player_ID"] == key_bbref]
        if sub.empty:
            return 0.0

        # normal case
        if "WAR" in sub.columns:
            mask = sub["year_ID"] < season_year
            total = sub.loc[mask, "WAR"].sum()
            try:
                return float(total)
            except Exception:
                return 0.0

        # fallback: sum any columns that start with "WAR"
        war_cols = []
        for col in sub.columns:
            if isinstance(col, str) and col.lower().startswith("war"):
                war_cols.append(col)

        if war_cols:
            mask = sub["year_ID"] < season_year
            total = sub.loc[mask, war_cols].sum(axis=1).sum()
            try:
                return float(total)
            except Exception:
                return 0.0

        return 0.0

    except Exception as e:
        print(f"[WARN] bwar_bat() failed or changed schema: {e}. Using WAR=0.")
        return 0.0


def build_player_snapshot(batter_id, season_year):
    """
    Build a single-row DataFrame with season-start info for one player.
    Uses:
      - MLB StatsAPI for awards + counting stats + debut/name
      - pybaseball for ID mapping and Baseball-Reference WAR
    Everything is summed strictly before season_year.
    """
    # ensure output dir exists
    project_root = Path(__file__).resolve().parents[2]
    lookups_dir = project_root / "data" / "lookups"
    lookups_dir.mkdir(parents=True, exist_ok=True)

    # awards
    (
        al_allstar_prior, nl_allstar_prior,
        al_mvps_prior, nl_mvps_prior,
        gold_gloves_prior, platinum_gloves_prior, silver_sluggers_prior,
        hank_aaron_awards_prior,
        allmlb_first_team_prior, allmlb_second_team_prior,
        hr_derby_titles_prior,
        al_roty_prior, nl_roty_prior,
    ) = awards_counts_mlb(batter_id, season_year)

    # counting stats + debut/name
    (
        full_name_api,
        debut_year_api,
        games_played,
        plate_appearances,
        at_bats,
        hits,
        home_runs,
    ) = hitting_totals_mlb(batter_id, season_year)

    # external IDs (and a debut-year fallback)
    key_bbref, key_fangraphs, mlb_played_first = map_player_ids(batter_id)

    # choose best debut year
    if pd.notna(debut_year_api):
        debut_year = debut_year_api
    else:
        if mlb_played_first is not None:
            debut_year = mlb_played_first
        else:
            debut_year = np.nan

    # pick name (StatsAPI is usually fine)
    full_name = full_name_api

    # WAR through prior season
    war_career_prior = war_prior_bref(key_bbref, season_year)

    # AVG
    if at_bats > 0:
        avg_career_prior = hits / at_bats
    else:
        avg_career_prior = np.nan

    # rough years of service
    if pd.notna(debut_year):
        yrs = season_year - int(debut_year)
        if yrs < 0:
            yrs = 0
        if int(debut_year) == int(season_year):
            yrs = 0
        years_service_prior = int(yrs)
    else:
        if plate_appearances > 0:
            years_service_prior = 1
        else:
            years_service_prior = 0

    # totals derived from splits
    allstar_prior = int(al_allstar_prior) + int(nl_allstar_prior)
    mvps_prior = int(al_mvps_prior) + int(nl_mvps_prior)

    # assemble record
    row = {
        "full_name": full_name,
        "al_allstar_prior": int(al_allstar_prior),
        "nl_allstar_prior": int(nl_allstar_prior),
        "allstar_prior": int(allstar_prior),
        "years_service_prior": int(years_service_prior),
        "games_played_prior": int(games_played),
        "pa_career_prior": int(plate_appearances),
        "hr_career_prior": int(home_runs),
        "hits_career_prior": int(hits),
        "ab_career_prior": int(at_bats),
        "avg_career_prior": float(round(avg_career_prior, 3)) if pd.notna(avg_career_prior) else np.nan,
        "al_mvps_prior": int(al_mvps_prior),
        "nl_mvps_prior": int(nl_mvps_prior),
        "mvps_prior": int(mvps_prior),
        "gold_gloves_prior": int(gold_gloves_prior),
        "platinum_gloves_prior": int(platinum_gloves_prior),
        "silver_sluggers_prior": int(silver_sluggers_prior),
        "hank_aaron_awards_prior": int(hank_aaron_awards_prior),
        "allmlb_first_team_prior": int(allmlb_first_team_prior),
        "allmlb_second_team_prior": int(allmlb_second_team_prior),
        "hr_derby_titles_prior": int(hr_derby_titles_prior),
        "al_roty_prior": int(al_roty_prior),
        "nl_roty_prior": int(nl_roty_prior),
        "war_career_prior": float(round(war_career_prior, 2)),
    }

    snap = pd.DataFrame([row], index=[int(batter_id)])
    snap.index.name = "batter"
    return snap


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a season-start snapshot for one batter (StatsAPI + BR WAR via pybaseball)."
    )
    parser.add_argument("--batter", type=int, default=None, help="MLBAM batter id (e.g., 592450)")
    parser.add_argument("--name", type=str, default=None, help="Full player name, e.g., 'Shohei Ohtani' or 'Ohtani, Shohei'")
    parser.add_argument("--season", type=int, default=2024, help="Season year (e.g., 2024)")
    parser.add_argument("--save", action="store_true", help="Write Parquet to data/lookups/")
    args = parser.parse_args()

    # Resolve batter id: --name takes precedence over --batter if provided
    resolved_batter_id = None
    if args.name:
        resolved_batter_id = get_batter_id_by_name(args.name)
        if resolved_batter_id is None:
            print(f"[ERROR] Could not resolve name '{args.name}' to an MLBAM id.")
            raise SystemExit(1)
        print(f"> resolved '{args.name}' to MLBAM id {resolved_batter_id}")
    elif args.batter is not None:
        resolved_batter_id = int(args.batter)
    else:
        print("[ERROR] Provide either --name or --batter.")
        raise SystemExit(1)

    print("> building player snapshot…")
    df = build_player_snapshot(resolved_batter_id, args.season)

    print("Snapshot shape:", df.shape)
    print("Columns:", df.columns.tolist())
    print("\nSnapshot (head):")
    print(df.head(10))

    # Show key awards
    filtered_df_a = df[
        [
            'full_name', 'al_allstar_prior', 'nl_allstar_prior', 'allstar_prior', 
            'years_service_prior', 'games_played_prior', 'pa_career_prior', 'hr_career_prior',
            
        ]
    ]
    print("\nFiltered A(head):")
    print(filtered_df_a.head(10))

    filtered_df_b = df[
        [
            'hits_career_prior', 'ab_career_prior', 'avg_career_prior', 'al_mvps_prior', 
            'nl_mvps_prior', 'mvps_prior', 'gold_gloves_prior', 'platinum_gloves_prior',
            
        ]
    ]

    print("\nFiltered B(head):")
    print(filtered_df_b.head(10))

    filtered_df_c = df[
        [
            'silver_sluggers_prior', 'hank_aaron_awards_prior', 'allmlb_first_team_prior', 'allmlb_second_team_prior', 
            'hr_derby_titles_prior', 'al_roty_prior', 'nl_roty_prior', 'war_career_prior'
            
        ]
    ]

    print("\nFiltered C(head):")
    print(filtered_df_c.head(10))


    

    if args.save:
        project_root = Path(__file__).resolve().parents[2]
        out_path = project_root / "data" / "lookups" / f"player_snapshot_{resolved_batter_id}_{args.season}.parquet"
        df.to_parquet(out_path)
        print(f"> saved snapshot to: {out_path}")
