"""
compute_baseline_probs.py

Computes baseline empirical win probabilities from halftime_state.

This is NOT a model.
It is a historical frequency lookup:
    halftime_margin -> P(home_win)
"""

import sqlite3
from pathlib import Path
import argparse


# Define halftime margin buckets
BUCKETS = [
    (-30, -21),
    (-20, -16),
    (-15, -11),
    (-10, -6),
    (-5, -1),
    (0, 0),
    (1, 5),
    (6, 10),
    (11, 15),
    (16, 20),
    (21, 30),
]


def parse_arguments():
    parser = argparse.ArgumentParser(description="Compute baseline halftime win probabilities")
    parser.add_argument(
        "--db",
        type=str,
        default="data/ncaa_mbb.db",
        help="Path to SQLite database",
    )
    return parser.parse_args()


def connect(db_path: Path):
    return sqlite3.connect(db_path)


def compute_bucket_stats(conn):
    cursor = conn.cursor()

    print("\nBaseline Halftime Win Probabilities")
    print("-" * 60)
    print(f"{'Margin Bucket':>15} | {'Games':>7} | {'Home Win %':>10}")
    print("-" * 60)

    for low, high in BUCKETS:
        cursor.execute(
            """
            SELECT
                COUNT(*),
                AVG(home_won)
            FROM halftime_state_capped
            WHERE halftime_margin BETWEEN ? AND ?;
            """,
            (low, high),
        )
        games, win_rate = cursor.fetchone()

        if games == 0 or win_rate is None:
            continue

        print(
            f"{f'{low} to {high}':>15} | "
            f"{games:>7} | "
            f"{win_rate * 100:>9.2f}%"
        )

    print("-" * 60)
    print("Done.\n")


def main():
    args = parse_arguments()
    db_path = Path(args.db)

    conn = connect(db_path)
    try:
        compute_bucket_stats(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
