# app/db_live.py

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
import json


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
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def ensure_live_schema(conn: sqlite3.Connection):
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

        CREATE TABLE IF NOT EXISTS halftime_events (
            game_live_id       TEXT PRIMARY KEY,
            date               TEXT NOT NULL,
            season_year        INTEGER NOT NULL,

            home_halftime      INTEGER NOT NULL,
            away_halftime      INTEGER NOT NULL,

            -- NEW: canonical resolution results (nullable until mapped)
            home_team_id       INTEGER,
            away_team_id       INTEGER,
            alias_source       TEXT,

            -- NEW: raw halftime stats (JSON text)
            halftime_stats_json TEXT,

            captured_at_utc    TEXT NOT NULL
        );

        -- NEW: deterministic alias mapping
        CREATE TABLE IF NOT EXISTS team_aliases (
            alias_source       TEXT NOT NULL,
            alias_name         TEXT NOT NULL,    -- normalized
            team_id            INTEGER NOT NULL,
            mapping_source     TEXT DEFAULT 'manual',
            created_at_utc     TEXT NOT NULL,
            updated_at_utc     TEXT NOT NULL,
            PRIMARY KEY (alias_source, alias_name)
        );

        CREATE TABLE IF NOT EXISTS predictions (
            game_live_id                TEXT PRIMARY KEY,
            season_year                 INTEGER NOT NULL,

            predicted_home_win_prob     REAL NOT NULL,
            predicted_home_final_margin REAL,

            confidence                  REAL,
            explanation_json            TEXT,

            created_at_utc              TEXT NOT NULL
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


def halftime_event_exists(conn: sqlite3.Connection, game_live_id: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM halftime_events WHERE game_live_id = ?", (game_live_id,))
    return cur.fetchone() is not None


def insert_halftime_event(
    conn: sqlite3.Connection,
    game_live_id: str,
    home_halftime: int,
    away_halftime: int,
    season_year: int,
    captured_at_utc: str,
) -> None:
    conn.execute(
        """
        INSERT INTO halftime_events
        (game_live_id, home_halftime, away_halftime, season_year, captured_at_utc)
        VALUES (?, ?, ?, ?, ?)
        """,
        (game_live_id, home_halftime, away_halftime, season_year, captured_at_utc),
    )


def insert_prediction(
    conn: sqlite3.Connection,
    game_live_id: str,
    predicted_home_win_prob: float,
    confidence: float,
    created_at_utc: str,
    explanation_json: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO predictions
        (game_live_id, predicted_home_win_prob, confidence, created_at_utc, explanation_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            game_live_id,
            predicted_home_win_prob,
            confidence,
            created_at_utc,
            json.dumps(explanation_json),
        ),
    )

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
        INSERT INTO team_aliases (alias_source, alias_name, team_id, source, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(alias_source, alias_name) DO UPDATE SET
            team_id = excluded.team_id,
            source = excluded.source,
            updated_at_utc = excluded.updated_at_utc
        """,
        (alias_source, alias_name, team_id, source, now, now),
    )
    conn.commit()