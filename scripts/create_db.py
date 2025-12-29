"""
create_db.py

Creates or rebuilds the SQLite database schema for Phase 1
of the NCAA Men's Basketball Halftime Project.

This script is intentionally idempotent and reproducible:
- You can safely re-run it
- You can fully rebuild the database from scratch
"""

import argparse
import sqlite3
from pathlib import Path


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

-- =========================
-- Teams
-- =========================
CREATE TABLE IF NOT EXISTS teams (
    team_id       INTEGER PRIMARY KEY,
    team_name     TEXT NOT NULL,
    sportsref_id  TEXT NOT NULL UNIQUE
);

-- =========================
-- Seasons
-- =========================
CREATE TABLE IF NOT EXISTS seasons (
    season_id INTEGER PRIMARY KEY,
    year      INTEGER NOT NULL UNIQUE
);

-- =========================
-- Games
-- =========================
CREATE TABLE IF NOT EXISTS games (
    game_id           INTEGER PRIMARY KEY,
    season_id         INTEGER NOT NULL,
    date              TEXT NOT NULL,   -- ISO format: YYYY-MM-DD
    home_team_id      INTEGER NOT NULL,
    away_team_id      INTEGER NOT NULL,
    home_final_score  INTEGER,
    away_final_score  INTEGER,
    location          TEXT NOT NULL DEFAULT 'home',
    sportsref_box_id  TEXT UNIQUE,

    FOREIGN KEY (season_id)    REFERENCES seasons(season_id),
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id),

    UNIQUE (season_id, date, home_team_id, away_team_id)
);

-- =========================
-- Halftime Snapshot
-- =========================
CREATE TABLE IF NOT EXISTS halftime_stats (
    game_id              INTEGER PRIMARY KEY,
    home_first_half_pts  INTEGER,
    away_first_half_pts  INTEGER,

    FOREIGN KEY (game_id)
        REFERENCES games(game_id)
        ON DELETE CASCADE
);

-- =========================
-- Team Season Stats
-- =========================
CREATE TABLE IF NOT EXISTS team_season_stats (
    team_id           INTEGER NOT NULL,
    season_id         INTEGER NOT NULL,
    wins              INTEGER,
    losses            INTEGER,
    offensive_rating  REAL,
    defensive_rating  REAL,
    tempo             REAL,

    PRIMARY KEY (team_id, season_id),
    FOREIGN KEY (team_id)   REFERENCES teams(team_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id)
);

-- =========================
-- Indexes
-- =========================
CREATE INDEX IF NOT EXISTS idx_games_date
    ON games(date);

CREATE INDEX IF NOT EXISTS idx_games_season
    ON games(season_id);

CREATE INDEX IF NOT EXISTS idx_games_home
    ON games(home_team_id);

CREATE INDEX IF NOT EXISTS idx_games_away
    ON games(away_team_id);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    """
    Open a SQLite connection with foreign key enforcement enabled.

    Important SQLite detail:
    - Foreign keys are disabled by default
    - They must be enabled per connection
    """
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def rebuild_database(db_path: Path) -> None:
    """
    Delete the existing database file (if it exists).

    This guarantees a clean, reproducible rebuild.
    """
    if db_path.exists():
        db_path.unlink()


def create_tables(db_path: Path) -> None:
    """
    Create all database tables and indexes.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = connect(db_path)
    try:
        connection.executescript(SCHEMA_SQL)
        connection.commit()
    finally:
        connection.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create or rebuild the NCAA SQLite database schema"
    )

    parser.add_argument(
        "--db",
        type=str,
        default="data/ncaa_mbb.db",
        help="Path to SQLite database file"
    )

    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing database file before creating tables"
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    db_path = Path(args.db)

    if args.rebuild:
        rebuild_database(db_path)

    create_tables(db_path)

    print("Database schema created successfully.")
    print("Database path:", str(db_path.resolve()))


if __name__ == "__main__":
    main()