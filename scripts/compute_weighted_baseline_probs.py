"""
compute_weighted_baseline_probs.py

Computes season-weighted baseline halftime win probabilities.
"""

import sqlite3
from pathlib import Path
import argparse


SEASON_WEIGHTS = {
    2024: 1.00,
    2023: 0.85,
    2022: 0.70,
}

BUCKETS = [
    (-20, -16),
    (-15, -11),
    (-10, -6),
    (-5, -1),
    (0, 0),
    (1, 5),
    (6, 10),
    (11, 15),
    (16, 20),
]


def parse_args():
    parser = argparse.ArgumentParser(description="Compute season-weighted halftime probabilities")
    parser.add_argument("--db", type=str, default="data/ncaa_mbb.db")
    return parser.parse_args()


def compute_weighted_probs(conn):
    cursor = conn.cursor()

    print("\nSeason-Weighted Halftime Win Probabilities")
    print("-" * 65)
    print(f"{'Margin':>10} | {'Weighted Games':>14} | {'Home Win %':>10}")
    print("-" * 65)

    for low, high in BUCKETS:
        weighted_wins = 0.0
        weighted_games = 0.0

        for season, weight in SEASON_WEIGHTS.items():
            cursor.execute(
                """
                SELECT COUNT(*), AVG(home_won)
                FROM halftime_state
                WHERE season_year = ?
                  AND halftime_margin BETWEEN ? AND ?;
                """,
                (season, low, high),
            )
            games, win_rate = cursor.fetchone()

            if games and win_rate is not None:
                weighted_games += games * weight
                weighted_wins += games * weight * win_rate

        if weighted_games == 0:
            continue

        prob = weighted_wins / weighted_games

        print(
            f"{f'{low} to {high}':>10} | "
            f"{weighted_games:>14.1f} | "
            f"{prob * 100:>9.2f}%"
        )

    print("-" * 65)


def main():
    args = parse_args()
    conn = sqlite3.connect(Path(args.db))
    try:
        compute_weighted_probs(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
