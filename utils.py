import requests

BASE = "https://statsapi.mlb.com/api/v1"

def search_people_by_name(name: str):
    r = requests.get(f"{BASE}/people/search", params={"names": name}, timeout=30)
    r.raise_for_status()
    return r.json().get("people", [])

def season_players(season: int):
    r = requests.get(f"{BASE}/sports/1/players", params={"season": season}, timeout=30)
    r.raise_for_status()
    return r.json().get("people", [])

def team_roster(team_id: int, season: int, roster_type="active"):
    r = requests.get(f"{BASE}/teams/{team_id}/roster", params={"season": season, "rosterType": roster_type}, timeout=15)
    r.raise_for_status()
    return r.json().get("roster", [])


def get_player_name_by_id():
    pass

if __name__ == "__main__":
    # Example: find Aaron Judge's ID via name search, then fall back to season list if needed
    candidates = search_people_by_name("Aaron Judge")
    print([(p["id"], p["fullName"], p.get("currentTeam", {}).get("name")) for p in candidates])
