"""
calibrate_smoothed_probs.py

Evaluates calibration of smoothed halftime win probabilities
using a single season of data.
"""

import sqlite3
from pathlib import Path
import argparse


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

SMOOTHED_PROBS = {
    (-20, -16): 0.1331,
    (-15, -11): 0.1868,
    (-10, -6):  0.3065,
    (-5, -1):   0.4966,
    (0, 0):     0.5526,
    (1, 5):     0.6862,
    (6, 10):    0.8188,
    (11, 15):   0.9132,
    (16, 20):   0.9508,
}


def parse_args():
    parser = argparse.ArgumentParser(description="Calibrate smoothed halftime probabilities")
    parser.add_argument("--db", type=str, default="data/ncaa_mbb.db")
    parser.add_argument("--season", type=int, required=True)
    return parser.parse_args()


def calibrate(conn, season_year: int):
    cursor = conn.cursor()

    print("\nCalibration Report (Smoothed Probabilities)")
    print("-" * 75)
    print(f"{'Margin':>10} | {'Games':>7} | {'Pred %':>8} | {'Actual %':>9}")
    print("-" * 75)

    for low, high in BUCKETS:
        cursor.execute(
            """
            SELECT COUNT(*), AVG(home_won)
            FROM halftime_state_capped
            WHERE season_year = ?
              AND halftime_margin BETWEEN ? AND ?;
            """,
            (season_year, low, high),
        )

        games, actual = cursor.fetchone()
        if games == 0 or actual is None:
            continue

        predicted = SMOOTHED_PROBS[(low, high)]

        print(
            f"{f'{low} to {high}':>10} | "
            f"{games:>7} | "
            f"{predicted*100:>7.2f}% | "
            f"{actual*100:>8.2f}%"
        )

    print("-" * 75)


def main():
    args = parse_args()
    conn = sqlite3.connect(Path(args.db))
    try:
        calibrate(conn, args.season)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
