# app/db_live.py

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
import json
from team_mapping_static import get_sports_reference_name


@dataclass
class LiveGame:
    game_live_id: str
    date: str  # YYYY-MM-DD (local or UTC date you decide; be consistent)
    start_time_utc: Optional[str]  # ISO string

    status: str  # PRE, LIVE, HALFTIME, FINAL

    home_name: str
    away_name: str

    home_espn_team_id: Optional[str]
    away_espn_team_id: Optional[str]

    home_score: Optional[int]
    away_score: Optional[int]


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def ensure_daily_games_schema(conn: sqlite3.Connection):
    # NOTE: these tables are independent of your historical schema.
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS daily_games (
            game_live_id       TEXT PRIMARY KEY,
            date               TEXT NOT NULL,
            start_time_utc     TEXT,
            status             TEXT NOT NULL,

            home_name          TEXT NOT NULL,
            away_name          TEXT NOT NULL,

            home_espn_team_id  TEXT,
            away_espn_team_id  TEXT,

            home_score         INTEGER,
            away_score         INTEGER,

            last_seen_utc      TEXT NOT NULL
        );
        """
    )
    conn.commit()


def upsert_daily_game(conn: sqlite3.Connection, g: LiveGame):
    conn.execute(
        """
        INSERT INTO daily_games (
            game_live_id, date, start_time_utc, status,
            home_name, away_name,
            home_espn_team_id, away_espn_team_id,
            home_score, away_score,
            last_seen_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_live_id) DO UPDATE SET
            date = excluded.date,
            start_time_utc = excluded.start_time_utc,
            status = excluded.status,
            home_name = excluded.home_name,
            away_name = excluded.away_name,
            home_espn_team_id = excluded.home_espn_team_id,
            away_espn_team_id = excluded.away_espn_team_id,
            home_score = excluded.home_score,
            away_score = excluded.away_score,
            last_seen_utc = excluded.last_seen_utc;
        """,
        (
            g.game_live_id,
            g.date,
            g.start_time_utc,
            g.status,
            g.home_name,
            g.away_name,
            g.home_espn_team_id,
            g.away_espn_team_id,
            g.home_score,
            g.away_score,
            utc_now_iso(),
        ),
    )


def get_previous_status(conn: sqlite3.Connection, game_live_id: str) -> Optional[str]:
    row = conn.execute(
        "SELECT status FROM daily_games WHERE game_live_id = ?;",
        (game_live_id,),
    ).fetchone()
    return row[0] if row else None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# def insert_prediction(
#     conn: sqlite3.Connection,
#     game_live_id: str,
#     predicted_home_win_prob: float,
#     confidence: float,
#     created_at_utc: str,
#     explanation_json: dict,
# ) -> None:
#     conn.execute(
#         """
#         INSERT INTO predictions
#         (game_live_id, predicted_home_win_prob, confidence, created_at_utc, explanation_json)
#         VALUES (?, ?, ?, ?, ?)
#         """,
#         (
#             game_live_id,
#             predicted_home_win_prob,
#             confidence,
#             created_at_utc,
#             json.dumps(explanation_json),
#         ),
#     )

def upsert_season_game_from_live(
    conn: sqlite3.Connection,
    season_id: int,
    g: LiveGame,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
) -> int:
    """
    Returns season_games.game_id (PK).
    """
    now = utc_now_iso()

    conn.execute(
        """
        INSERT INTO season_games (
            season_id, game_live_id, game_date, start_time_utc,
            home_team_id, away_team_id,
            status, created_at_utc, updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_live_id) DO UPDATE SET
            game_date = excluded.game_date,
            start_time_utc = excluded.start_time_utc,
            status = excluded.status,
            updated_at_utc = excluded.updated_at_utc
        ;
        """,
        (
            season_id,
            g.game_live_id,
            g.date,
            g.start_time_utc,
            home_team_id,
            away_team_id,
            g.status,
            now,
            now,
        ),
    )
    conn.commit()

    row = conn.execute(
        "SELECT game_id FROM season_games WHERE game_live_id = ?;",
        (g.game_live_id,),
    ).fetchone()
    return int(row["game_id"])


def set_season_game_final(conn: sqlite3.Connection, game_live_id: str, home: int, away: int):
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE season_games
        SET home_final_score = ?, away_final_score = ?, status = 'FINAL', updated_at_utc = ?
        WHERE game_live_id = ?;
        """,
        (home, away, now, game_live_id),
    )
    conn.commit()



def get_team_id_from_alias(conn: sqlite3.Connection, alias_source: str, alias_name: str):
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM team_aliases WHERE alias_source = ? AND alias_name = ?", 
                (alias_source, alias_name,))
    row = cur.fetchone()
    return row[0] if row else None


def upsert_team_alias(
    conn: sqlite3.Connection,
    alias_source: str,
    alias_name: str,
    team_id: int,
    source: str = "manual",
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO team_aliases (alias_source, alias_name, team_id, mapping_source, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(alias_source, alias_name) DO UPDATE SET
            team_id = excluded.team_id,
            mapping_source = excluded.mapping_source,
            updated_at_utc = excluded.updated_at_utc
        """,
        (alias_source, alias_name, team_id, source, now, now),
    )
    conn.commit()


def get_or_create_season_id(conn: sqlite3.Connection, season_year: int) -> int:
    row = conn.execute("SELECT season_id FROM seasons WHERE year = ?;", (season_year,)).fetchone()
    if row:
        return int(row["season_id"])
    cur = conn.execute("INSERT INTO seasons (year) VALUES (?);", (season_year,))
    conn.commit()

    lastrowid = cur.lastrowid
    if lastrowid is None:
        # This should never happen for a normal INSERT, so fail loudly.
        raise RuntimeError("Expected lastrowid after INSERT into seasons, got None")
    
    return int(lastrowid)

def resolve_team_id_from_espn_name(
    conn: sqlite3.Connection,
    espn_display_name: str,
) -> int:
    """
    Resolve internal team_id using ESPN displayName → sportsref_id mapping.
    Raises if unmapped (intentional).
    """
    sportsref_id = get_sports_reference_name(espn_display_name)

    row = conn.execute(
        """
        SELECT team_id
        FROM teams
        WHERE sportsref_id = ?;
        """,
        (sportsref_id,),
    ).fetchone()

    if not row:
        raise RuntimeError(f"SportsRef team not found: {sportsref_id}")

    return int(row["team_id"])
