# app/sources/espn.py

import requests
from datetime import datetime, timezone
from typing import List, Optional

from app.db_live import LiveGame


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ncaa-live-poller/1.0)"
}


def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def _status_from_competition(status_obj: dict) -> str:
    """
    ESPN status object is nested. We'll map to: PRE, LIVE, HALFTIME, FINAL
    """
    # common fields: type.state, type.completed, type.name/shortDetail, period, displayClock
    t = (status_obj or {}).get("type") or {}
    state = (t.get("state") or "").upper()
    completed = bool(t.get("completed"))

    # ESPN sometimes sets "STATUS_HALFTIME" as name or shortDetail.
    name = (t.get("name") or "").upper()
    short = (t.get("shortDetail") or "").upper()
    detail = (t.get("detail") or "").upper()

    if completed or state == "POST":
        return "FINAL"

    if "HALFTIME" in name or "HALFTIME" in short or "HALFTIME" in detail:
        return "HALFTIME"

    if state == "PRE":
        return "PRE"

    # IN covers "in progress"
    return "LIVE"


# beginning of the day, handles fething all games being played for that day
def fetch_scoreboard(scoreboard_url: str, date_yyyymmdd: str) -> List[LiveGame]:
    """
    date_yyyymmdd: e.g. 20241222
    """
    resp = requests.get(
        scoreboard_url,
        params={"dates": date_yyyymmdd, "groups": "50", "limit": "500"},
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    events = data.get("events") or []
    games: List[LiveGame] = []

    for e in events:
        game_id = str(e.get("id"))
        date_iso = e.get("date")  # ISO datetime

        start_time_utc = None
        if date_iso:
            # keep as ISO; ESPN provides timezone offset
            try:
                start_time_utc = datetime.fromisoformat(date_iso.replace("Z", "+00:00")).astimezone(timezone.utc).replace(microsecond=0).isoformat()
            except Exception:
                start_time_utc = date_iso

        competitions = e.get("competitions") or []
        if not competitions:
            continue

        comp = competitions[0]
        status = _status_from_competition(comp.get("status") or {})

        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            continue

        # ESPN gives "home"/"away" keys
        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home or not away:
            # fallback to order
            away, home = competitors[0], competitors[1]

        home_team = home.get("team") or {}
        away_team = away.get("team") or {}

        home_name = home_team.get("displayName") or home_team.get("shortDisplayName") or "Home"
        away_name = away_team.get("displayName") or away_team.get("shortDisplayName") or "Away"

        home_team_id = home_team.get("id")
        away_team_id = away_team.get("id")

        home_score = _safe_int(home.get("score"))
        away_score = _safe_int(away.get("score"))

        # date field for daily partitioning; caller owns timezone choice
        # We'll attach it later in poller (YYYY-MM-DD local).
        games.append(
            LiveGame(
                game_live_id=game_id,
                date="",  # filled in poller
                start_time_utc=start_time_utc,
                status=status,
                home_name=home_name,
                away_name=away_name,
                home_espn_team_id=str(home_team_id) if home_team_id is not None else None,
                away_espn_team_id=str(away_team_id) if away_team_id is not None else None,
                home_score=home_score,
                away_score=away_score,
            )
        )

    return games


# @ halftime functions -------------------------------------------------------
def fetch_game_summary(summary_url: str, event_id: str, headers: dict):
    resp = requests.get(
        summary_url,
        params={"event": event_id},
        headers=headers,
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()

def extract_first_half_team_stats(summary_json: dict) -> dict:
    """
    Returns:
    {
      "home": {...},
      "away": {...}
    }
    """
    out = {"home": {}, "away": {}}

    box = (summary_json or {}).get("boxscore") or {}
    teams = box.get("teams") or []

    def normalize(stats_list):
        m = {}
        for s in stats_list or []:
            key = (s.get("name") or s.get("label") or "").lower()
            val = s.get("displayValue") or s.get("value")
            if key:
                m[key] = val
        return m

    for t in teams:
        team_info = t.get("team") or {}
        side = (team_info.get("homeAway") or "").lower()
        if side not in ("home", "away"):
            continue

        stats = normalize(t.get("statistics"))
        out[side] = {
            "fg_pct": stats.get("fg%"),
            "fg3_pct": stats.get("3pt%"),
            "ft_att": stats.get("fta"),
            "turnovers": stats.get("turnovers"),
            "off_reb": stats.get("offReb"),
            "tot_reb": stats.get("rebounds"),
        }

    return out