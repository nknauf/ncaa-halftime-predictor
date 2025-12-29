"""
smooth_baseline_probs.py

Applies shrinkage smoothing to baseline halftime probabilities.
"""

import sqlite3
from pathlib import Path
import argparse

PRIOR_PROB = 0.50  # explicit prior

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
    parser = argparse.ArgumentParser(description="Smooth baseline halftime probabilities")
    parser.add_argument("--db", type=str, default="data/ncaa_mbb.db")
    return parser.parse_args()


def smooth_probs(conn):
    cursor = conn.cursor()

    print("\nSmoothed Baseline Halftime Probabilities")
    print("-" * 85)
    print(
        f"{'Margin':>10} | "
        f"{'Games':>7} | "
        f"{'Raw %':>8} | "
        f"{'Smoothed %':>11} | "
        f"{'Weight':>7}"
    )
    print("-" * 85)

    for low, high in BUCKETS:

        # varied smoothing constant
        if abs(low) >= 15:
            k = 50
        elif abs(low) >= 10:
            k = 75
        else:
            k = 125

        cursor.execute(
            """
            SELECT COUNT(*), AVG(home_won)
            FROM halftime_state_capped
            WHERE halftime_margin BETWEEN ? AND ?;
            """,
            (low, high),
        )
        games, raw_prob = cursor.fetchone()

        if games == 0 or raw_prob is None:
            continue

        weight = games / (games + k)

        smoothed = (
            (games * raw_prob) +
            (k * PRIOR_PROB)
        ) / (games + k)

        # Defensive clamp
        smoothed = max(0.0, min(1.0, smoothed))

        print(
            f"{f'{low} to {high}':>10} | "
            f"{games:>7} | "
            f"{raw_prob * 100:>7.2f}% | "
            f"{smoothed * 100:>10.2f}% | "
            f"{weight:>6.2f}"
        )

    print("-" * 85)


def main():
    args = parse_args()
    conn = sqlite3.connect(Path(args.db))
    try:
        smooth_probs(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()