"""
validate_season.py

Runs data quality and sanity checks for a given NCAA season.

Usage:
    python scripts/validate_season.py --season 2023
"""

import argparse
import sqlite3
from pathlib import Path


def parse_arguments():
    parser = argparse.ArgumentParser(description="Validate NCAA season data")
    parser.add_argument(
        "--season",
        type=int,
        required=True,
        help="Season ending year (e.g. 2023)"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="data/ncaa_mbb.db",
        help="Path to SQLite database"
    )
    return parser.parse_args()


def validate_season(season_year: int, db_path: Path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Resolve season_id
    cursor.execute(
        "SELECT season_id FROM seasons WHERE year = ?;",
        (season_year,)
    )
    row = cursor.fetchone()
    if row is None:
        print(f"Season {season_year} not found in database.")
        return

    season_id = row[0]
    print(f"\nValidating season {season_year} (season_id={season_id})")
    print("-" * 50)

    # Core counts
    cursor.execute(
        "SELECT COUNT(*) FROM games WHERE season_id = ?;",
        (season_id,)
    )
    games_count = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM halftime_stats h
        JOIN games g ON h.game_id = g.game_id
        WHERE g.season_id = ?;
        """,
        (season_id,)
    )
    halftime_count = cursor.fetchone()[0]

    print(f"Games collected: {games_count}")
    print(f"Halftime rows:   {halftime_count}")

    # Missing halftime data
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM halftime_stats h
        JOIN games g ON h.game_id = g.game_id
        WHERE g.season_id = ?
          AND (h.home_first_half_pts IS NULL
               OR h.away_first_half_pts IS NULL);
        """,
        (season_id,)
    )
    missing_halftime = cursor.fetchone()[0]

    pct_missing = (
        100.0 * missing_halftime / halftime_count
        if halftime_count > 0 else 0.0
    )

    print(f"Missing halftime rows: {missing_halftime} ({pct_missing:.2f}%)")

    # Duplicate game check
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT season_id, date, home_team_id, away_team_id, COUNT(*) AS cnt
            FROM games
            WHERE season_id = ?
            GROUP BY season_id, date, home_team_id, away_team_id
            HAVING cnt > 1
        );
        """,
        (season_id,)
    )
    dup_games = cursor.fetchone()[0]
    print(f"Duplicate games: {dup_games}")

    # Referential integrity
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM games g
        LEFT JOIN halftime_stats h ON g.game_id = h.game_id
        WHERE g.season_id = ?
          AND h.game_id IS NULL;
        """,
        (season_id,)
    )
    games_missing_halftime = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM halftime_stats h
        LEFT JOIN games g ON h.game_id = g.game_id
        WHERE g.game_id IS NULL;
        """
    )
    orphan_halftime = cursor.fetchone()[0]

    print(f"Games without halftime row: {games_missing_halftime}")
    print(f"Orphan halftime rows:       {orphan_halftime}")

    # 5) Halftime leader win rate
    cursor.execute(
        """
        SELECT
            AVG(
                CASE
                    WHEN h.home_first_half_pts > h.away_first_half_pts
                         AND g.home_final_score > g.away_final_score THEN 1
                    WHEN h.away_first_half_pts > h.home_first_half_pts
                         AND g.away_final_score > g.home_final_score THEN 1
                    ELSE 0
                END
            )
        FROM games g
        JOIN halftime_stats h ON g.game_id = h.game_id
        WHERE g.season_id = ?
          AND h.home_first_half_pts IS NOT NULL
          AND h.away_first_half_pts IS NOT NULL;
        """,
        (season_id,)
    )
    leader_win_rate = cursor.fetchone()[0]

    if leader_win_rate is not None:
        print(f"Halftime leader win rate: {leader_win_rate * 100:.2f}%")
    else:
        print("Halftime leader win rate: N/A")

    # 6) Score range sanity
    cursor.execute(
        """
        SELECT
            MIN(h.home_first_half_pts),
            MAX(h.home_first_half_pts),
            MIN(h.away_first_half_pts),
            MAX(h.away_first_half_pts)
        FROM halftime_stats h
        JOIN games g ON h.game_id = g.game_id
        WHERE g.season_id = ?
          AND h.home_first_half_pts IS NOT NULL;
        """,
        (season_id,)
    )
    mins_maxes = cursor.fetchone()

    print("Halftime score ranges:")
    print(f"  Home: {mins_maxes[0]} to {mins_maxes[1]}")
    print(f"  Away: {mins_maxes[2]} to {mins_maxes[3]}")

    conn.close()
    print("-" * 50)
    print("Validation complete.\n")


def main():
    args = parse_arguments()
    validate_season(args.season, Path(args.db))


if __name__ == "__main__":
    main()