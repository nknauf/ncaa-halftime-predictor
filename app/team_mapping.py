# app/team_mapping.py

# DEPRECATED FOR PHASE 1 MVP
# Replaced by team_mapping_static.py

import re
import sqlite3
from app.db_live import get_team_id_from_alias


def normalize_team_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


def _detect_team_name_column(conn: sqlite3.Connection):
    """
    Tries to discover which column in your `teams` table stores the readable name.
    Falls back to alias-only if none exist.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(teams);")
    cols = [r[1] for r in cur.fetchall()]  # (cid, name, type, notnull, dflt_value, pk)

    for candidate in ["name", "team_name", "school", "sportsref_name"]:
        if candidate in cols:
            return candidate
    return None


def resolve_team_id(conn: sqlite3.Connection, raw_name: str, alias_source: str = "espn"):
    """
    Resolution order:
      1) normalized alias in team_aliases (soucre-specific)
      2) (optional) match teams table by discovered name column
    """
    norm = normalize_team_name(raw_name)
    cur = conn.cursor()

    # Try to get id from alias table
    team_id = get_team_id_from_alias(conn, alias_source, norm)
    if team_id is not None:
        return team_id
    
    # Try exact normalized match, not a guess
    name_col = _detect_team_name_column(conn)
    if not name_col:
        return None
    cur.execute(
        f"""
        SELECT team_id, {name_col}
        FROM teams
        """
    )
    for team_id, team_name in cur.fetchall():
        if normalize_team_name(team_name) == norm:
            return team_id
    
    return None
