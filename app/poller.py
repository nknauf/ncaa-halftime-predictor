# app/poller.py

import argparse
import time
from datetime import datetime
from pathlib import Path

from app.config import CONFIG
from app.db_live import (
    connect,
    ensure_daily_games_schema,
    get_previous_status,
    upsert_daily_game,
    upsert_season_game_from_live,
    get_or_create_season_id,
    resolve_team_id_from_espn_name
)
from app.handle_halftime import handle_halftime
from app.handle_final import handle_final
from app.sources.espn import fetch_scoreboard

from zoneinfo import ZoneInfo
from datetime import timedelta

ET = ZoneInfo("America/New_York")


def parse_args():
    p = argparse.ArgumentParser(description="Live poller: ESPN scoreboard -> daily_games -> halftime trigger")
    p.add_argument("--db", type=str, default=str(CONFIG.db_path))
    p.add_argument("--season", type=int, required=True, help="Season year metadata for halftime_events (e.g. 2025)")
    p.add_argument("--interval", type=int, default=CONFIG.poll_interval_seconds)
    p.add_argument("--date", type=str, default=None, help="YYYYMMDD (defaults to today)")
    return p.parse_args()


def current_sports_day_et():
    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    return sports_day_et(now_utc)


def scoreboard_date_param():
    return current_sports_day_et().replace("-", "")


def sports_day_et(now_utc: datetime) -> str:
    now_et = now_utc.astimezone(ET)
    # Sports day rolls over at 5am ET
    if now_et.hour < 5:
        sports_day = now_et.date() - timedelta(days=1)
    else:
        sports_day = now_et.date()

    return sports_day.isoformat()


def main():
    args = parse_args()
    db_path = Path(args.db)

    sports_day = args.date or current_sports_day_et()
    date_param = sports_day.replace("-", "")

    now_utc = datetime.now(tz=ZoneInfo("UTC"))

    sports_today = sports_day_et(now_utc)
    sports_yesterday = (datetime.fromisoformat(sports_today) - timedelta(days=1)).isoformat()

    conn = connect(db_path)
    season_id = get_or_create_season_id(conn, args.season)
    try:
        ensure_daily_games_schema(conn)

        # filters daily_games to only include today and yesterday basketball games
        conn.execute(
            """
            DELETE FROM daily_games
            WHERE date NOT IN (?, ?);
            """,
            (sports_today, sports_yesterday)
        )
        conn.commit()

        print(f"Using DB: {db_path.resolve()}")
        print(f"Polling ESPN for date={date_param} every {args.interval}s (season={args.season})")

        while True:

            new_sports_day = current_sports_day_et()
            if new_sports_day != sports_day:
                sports_day = new_sports_day
                date_param = sports_day.replace("-", "")
                print(f"[INFO] Sports day rolled over → {sports_day}")

            try:
                games = fetch_scoreboard(CONFIG.espn_scoreboard_url, date_param)
            except Exception as e:
                print(f"[poller] fetch failed: {e}")
                time.sleep(args.interval)
                continue

            for g in games:

                # Fill date partition
                g.date = sports_day

                prev_status = get_previous_status(conn, g.game_live_id)
                upsert_daily_game(conn, g)
                conn.commit()

                try:
                    home_team_id = resolve_team_id_from_espn_name(conn, g.home_name)
                    away_team_id = resolve_team_id_from_espn_name(conn, g.away_name)
                except KeyError as e:
                    print(f"[TEAM MAP MISSING] {e} — skipping game {g.game_live_id}")
                    continue

                game_pk = upsert_season_game_from_live (
                    conn=conn,
                    season_id=season_id,
                    g=g,
                    home_team_id=home_team_id,
                    away_team_id=away_team_id
                )

                # Transition logic
                if g.status == "HALFTIME" and prev_status != "HALFTIME":
        
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
