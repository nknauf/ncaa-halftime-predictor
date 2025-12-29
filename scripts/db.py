"""
db.py

Small helper utilities for interacting with the SQLite database.

Why this file exists:
- Centralizes SQLite connection logic
- Prevents duplicated code across scripts
- Makes future changes (e.g., switching DBs) easier
"""

import sqlite3
from pathlib import Path
from typing import Optional


DEFAULT_DB_PATH = Path("data/ncaa_mbb.db")


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Open a SQLite connection with foreign keys enabled.

    Important SQLite detail:
    Foreign key constraints are OFF by default and must be enabled
    for every new connection.
    """
    db_path = Path(db_path).expanduser().resolve()
    print("CONNECTING TO DB:", db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_or_create_season(conn: sqlite3.Connection, year: int) -> int:
    """
    Fetch a season_id for a given season year.
    If it doesn't exist, create it.

    Returns:
        season_id (int)
    """
    cursor = conn.cursor()

    cursor.execute(
        "SELECT season_id FROM seasons WHERE year = ?;",
        (year,)
    )
    row = cursor.fetchone()

    if row:
        return row[0]

    cursor.execute(
        "INSERT INTO seasons (year) VALUES (?);",
        (year,)
    )
    conn.commit()

    season_id = cursor.lastrowid
    if season_id is None:
        raise RuntimeError("Failed to get season_id after insert.")
    return season_id


def get_team_by_sportsref_id(
    conn: sqlite3.Connection,
    sportsref_id: str
) -> Optional[int]:
    """
    Return team_id if a team exists, otherwise None.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT team_id FROM teams WHERE sportsref_id = ?;",
        (sportsref_id,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def insert_team(
    conn: sqlite3.Connection,
    team_name: str,
    sportsref_id: str
) -> int:
    """
    Insert a team and return its team_id.

    Assumes the team does NOT already exist.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO teams (team_name, sportsref_id)
        VALUES (?, ?);
        """,
        (team_name, sportsref_id)
    )
    conn.commit()
    team_id = cursor.lastrowid
    if team_id is None:
        raise RuntimeError("Failed to get team_id after insert.")
    return team_id
