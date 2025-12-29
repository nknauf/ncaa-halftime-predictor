"""
build_halftime_view.py

Creates the halftime_state SQL VIEW and runs basic validation checks.

Why this script exists:
- Keeps schema logic out of ad-hoc sqlite shells
- Makes halftime state reproducible and version-controlled
- Acts as a gate before modeling
"""

import sqlite3
from pathlib import Path
import argparse


HALFTIME_VIEW_SQL = """
CREATE VIEW IF NOT EXISTS halftime_state AS
SELECT
    g.game_id,

    s.year AS season_year,
    g.date,

    g.home_team_id,
    g.away_team_id,

    h.home_first_half_pts,
    h.away_first_half_pts,

    (h.home_first_half_pts - h.away_first_half_pts) AS halftime_margin,

    g.home_final_score,
    g.away_final_score,

    (g.home_final_score - g.away_final_score) AS final_margin,

    CASE
        WHEN g.home_final_score > g.away_final_score THEN 1
        ELSE 0
    END AS home_won,

    CASE
        WHEN (h.home_first_half_pts - h.away_first_half_pts) > 0 THEN 1
        ELSE 0
    END AS home_led_at_halftime,

    g.location

FROM games g
JOIN halftime_stats h ON g.game_id = h.game_id
JOIN seasons s ON g.season_id = s.season_id
WHERE
    h.home_first_half_pts IS NOT NULL
    AND h.away_first_half_pts IS NOT NULL
    AND g.home_final_score IS NOT NULL
    AND g.away_final_score IS NOT NULL;
"""


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build halftime_state SQL view")
    parser.add_argument(
        "--db",
        type=str,
        default="data/ncaa_mbb.db",
        help="Path to SQLite database",
    )
    return parser.parse_args()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def build_view(conn: sqlite3.Connection):
    conn.executescript(HALFTIME_VIEW_SQL)
    conn.commit()


def run_validation(conn: sqlite3.Connection):
    cursor = conn.cursor()

    print("\nHalftime State Validation")
    print("-" * 40)

    cursor.execute("SELECT COUNT(*) FROM halftime_state;")
    total_rows = cursor.fetchone()[0]
    print(f"Total halftime_state rows: {total_rows}")

    cursor.execute("""
        SELECT
            MIN(home_first_half_pts),
            MAX(home_first_half_pts),
            MIN(away_first_half_pts),
            MAX(away_first_half_pts)
        FROM halftime_state;
    """)
    min_max = cursor.fetchone()
    print(
        f"Halftime score range | "
        f"Home: {min_max[0]}-{min_max[1]}, "
        f"Away: {min_max[2]}-{min_max[3]}"
    )

    cursor.execute("""
        SELECT
            MIN(halftime_margin),
            MAX(halftime_margin)
        FROM halftime_state;
    """)
    margins = cursor.fetchone()
    print(f"Halftime margin range: {margins[0]} to {margins[1]}")

    cursor.execute("""
        SELECT
            AVG(home_won),
            AVG(home_led_at_halftime)
        FROM halftime_state;
    """)
    home_win_rate, home_lead_rate = cursor.fetchone()
    print(f"Home win rate: {home_win_rate:.3f}")
    print(f"Home led at halftime: {home_lead_rate:.3f}")

    cursor.execute("""
        SELECT
            AVG(
                CASE
                    WHEN home_led_at_halftime = 1 AND home_won = 1 THEN 1
                    WHEN home_led_at_halftime = 0 AND home_won = 0 THEN 1
                    ELSE 0
                END
            )
        FROM halftime_state;
    """)
    leader_win_rate = cursor.fetchone()[0]
    print(f"Halftime leader win rate: {leader_win_rate:.3f}")
    print("-" * 40)

    # -----------------------------
    # Final score validation (NEW)
    # -----------------------------
    print("\nFinal Score Validation")
    print("-" * 40)

    cursor.execute("""
        SELECT COUNT(*)
        FROM halftime_state
        WHERE home_final_score IS NULL
           OR away_final_score IS NULL;
    """)
    missing_finals = cursor.fetchone()[0]
    print(f"Games with missing final scores: {missing_finals}")

    cursor.execute("""
        SELECT
            MIN(home_final_score),
            MAX(home_final_score),
            MIN(away_final_score),
            MAX(away_final_score)
        FROM halftime_state;
    """)
    finals = cursor.fetchone()
    print(
        f"Final score range | "
        f"Home: {finals[0]}-{finals[1]}, "
        f"Away: {finals[2]}-{finals[3]}"
    )

    cursor.execute("""
        SELECT
            MIN(final_margin),
            MAX(final_margin)
        FROM halftime_state;
    """)
    final_margins = cursor.fetchone()
    print(f"Final margin range: {final_margins[0]} to {final_margins[1]}")

    print("Validation complete.\n")


def main():
    args = parse_arguments()
    db_path = Path(args.db)

    print(f"Using database: {db_path.resolve()}")

    conn = connect(db_path)
    try:
        build_view(conn)
        run_validation(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()