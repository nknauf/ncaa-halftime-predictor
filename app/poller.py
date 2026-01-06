# app/poller.py

import argparse
import time
from datetime import datetime
from pathlib import Path

from app.config import CONFIG
from app.db_live import connect, ensure_live_schema, get_previous_status, halftime_event_exists, upsert_daily_game
from app.handle_halftime import handle_halftime
from app.handle_final import handle_final
from app.sources.espn import fetch_scoreboard


def parse_args():
    p = argparse.ArgumentParser(description="Live poller: ESPN scoreboard -> daily_games -> halftime trigger")
    p.add_argument("--db", type=str, default=str(CONFIG.db_path))
    p.add_argument("--season", type=int, required=True, help="Season year metadata for halftime_events (e.g. 2025)")
    p.add_argument("--interval", type=int, default=CONFIG.poll_interval_seconds)
    p.add_argument("--date", type=str, default=None, help="YYYYMMDD (defaults to today)")
    return p.parse_args()


def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def today_yyyy_mm_dd() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def main():
    args = parse_args()
    db_path = Path(args.db)

    date_param = args.date or today_yyyymmdd()
    date_for_db = today_yyyy_mm_dd()

    conn = connect(db_path)
    try:
        ensure_live_schema(conn)    # makes SQLite tables for live games and predictions


        # Not deleteing daily games for the moment
        # conn.execute(
        #     "DELETE FROM daily_games WHERE date != ?;",
        #     (date_for_db,)
        # )
        # conn.commit()

        print(f"Using DB: {db_path.resolve()}")
        print(f"Polling ESPN for date={date_param} every {args.interval}s (season={args.season})")

        while True:
            try:
                games = fetch_scoreboard(CONFIG.espn_scoreboard_url, date_param)
            except Exception as e:
                print(f"[poller] fetch failed: {e}")
                time.sleep(args.interval)
                continue

            for g in games:

                # Fill date partition
                g.date = date_for_db

                prev_status = get_previous_status(conn, g.game_live_id)
                upsert_daily_game(conn, g)
                conn.commit()

                # Transition logic
                if g.status == "HALFTIME" and prev_status != "HALFTIME":
                    # Avoid duplicates even if we restart the poller
                    if halftime_event_exists(conn, g.game_live_id):
                        continue

                    # We only have halftime score if ESPN is providing current score at halftime
                    if g.home_score is None or g.away_score is None:
                        print(f"[HALFTIME] Missing scores, skipping: {g.away_name} @ {g.home_name} ({g.game_live_id})")
                        continue

                    handle_halftime(conn, g, args.season)

                if g.status == "FINAL" and prev_status != "FINAL":
                    handle_final(conn, g)

            time.sleep(args.interval)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
