"""
scrape_games.py

Updates final game scores for an existing season by reading
team schedule pages on Sports-Reference.

Design principles:
- Idempotent
- No game insertion
- Schedule-based (no boxscores)
- One responsibility: final scores only
"""

import argparse
import time
import requests
from pathlib import Path
from bs4 import BeautifulSoup

from db import get_connection, get_or_create_season


BASE_URL = "https://www.sports-reference.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ncaa-halftime-bot/1.0)"
}


# --------------------------------------------------
# Args
# --------------------------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(description="Update final game scores")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--db", type=str, default="data/ncaa_mbb.db")
    return parser.parse_args()


# --------------------------------------------------
# Main logic
# --------------------------------------------------

def scrape_games(season_year: int, db_path: Path):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    season_id = get_or_create_season(conn, season_year)

    cursor.execute("SELECT team_id, sportsref_id FROM teams")
    teams = cursor.fetchall()

    print(f"Updating final scores for season {season_year}")
    print(f"Teams found: {len(teams)}")

    updated = 0
    skipped_existing = 0
    missing_games = 0

    for team_id, sportsref_id in teams:
        schedule_url = f"{BASE_URL}/cbb/schools/{sportsref_id}/{season_year}-schedule.html"
        print("Schedule:", schedule_url)

        try:
            resp = requests.get(schedule_url, headers=HEADERS, timeout=30)
        except requests.RequestException:
            continue

        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table", id="schedule")
        if not table:
            continue

        tbody = table.find("tbody")
        if not tbody:
            continue

        for row in tbody.find_all("tr"):
            if "thead" in (row.get("class") or []):
                continue

            # Resolve sportsref_box_id
            date_td = row.find("td", {"data-stat": "date_game"})
            if not date_td:
                continue

            link = date_td.find("a")
            if not link:
                continue

            box_href = link["href"]

            # Final score columns
            pts_td = row.find("td", {"data-stat": "pts"})
            opp_pts_td = row.find("td", {"data-stat": "opp_pts"})
            if not pts_td or not opp_pts_td:
                continue

            if not pts_td.text.isdigit() or not opp_pts_td.text.isdigit():
                continue

            team_pts = int(pts_td.text)
            opp_pts = int(opp_pts_td.text)

            # Determine home / away
            loc_td = row.find("td", {"data-stat": "game_location"})
            location = loc_td.text.strip() if loc_td else ""

            if location == "@":
                home_final = opp_pts
                away_final = team_pts
            else:
                home_final = team_pts
                away_final = opp_pts

            # Resolve game_id explicitly
            cursor.execute(
                """
                SELECT game_id, home_final_score, away_final_score
                FROM games
                WHERE season_id = ?
                  AND sportsref_box_id = ?
                """,
                (season_id, box_href),
            )
            game_row = cursor.fetchone()

            if game_row is None:
                missing_games += 1
                continue

            game_id, existing_home, existing_away = game_row

            # Skip if already has final scores
            if existing_home is not None and existing_away is not None:
                skipped_existing += 1
                continue

            # Update using internal key
            cursor.execute(
                """
                UPDATE games
                SET home_final_score = ?,
                    away_final_score = ?
                WHERE game_id = ?
                """,
                (home_final, away_final, game_id),
            )

            updated += 1

        conn.commit()
        time.sleep(0.5)

    conn.close()

    print("\nFinal score update summary")
    print("---------------------------")
    print(f"Updated games:         {updated}")
    print(f"Skipped (already set): {skipped_existing}")
    print(f"Missing DB rows:       {missing_games}")
    print("Finished updating final scores.")


def main():
    args = parse_arguments()
    scrape_games(args.season, Path(args.db))


if __name__ == "__main__":
    main()
