"""
scrape_games.py

Scrapes all games for a given season by iterating over team schedule pages,
then extracts halftime scores from box score pages.

Design principles:
- Reproducible
- Idempotent (safe to re-run)
- Minimal assumptions
"""

import argparse
import time
import requests
from pathlib import Path
from bs4 import BeautifulSoup, Comment

from db import get_connection, get_or_create_season

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ncaa-halftime-bot/1.0)"
}
BASE_URL = "https://www.sports-reference.com"

# -------------------------
# Rate limiting controls (NEW)
# -------------------------
SCHEDULE_SLEEP = 1.5        # seconds after each schedule page
BOX_SCORE_SLEEP = 2.5       # seconds after each boxscore
COOLDOWN_EVERY = 40         # cooldown after this many boxscores
COOLDOWN_SLEEP = 90         # seconds to cool down


def parse_arguments():
    parser = argparse.ArgumentParser(description="Scrape NCAA games + halftime stats")
    parser.add_argument("--season", type=int, required=True, help="Season ending year (e.g. 2023)")
    parser.add_argument("--db", type=str, default="data/ncaa_mbb.db")
    return parser.parse_args()


def extract_commented_table(soup, table_id):
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if table_id in comment:
            comment_soup = BeautifulSoup(comment, "lxml")
            table = comment_soup.find("table", id=table_id)
            if table:
                return table
    return None


def scrape_boxscore_scores(box_url: str):
    """
    Extract halftime score AND final scores from a box score page.
    """

    for attempt in range(3):  # NEW: retry loop
        try:
            resp = requests.get(box_url, headers=HEADERS, timeout=30)
        except requests.RequestException:
            time.sleep(5 * (attempt + 1))
            continue

        if resp.status_code == 429:
            wait = 10 * (2 ** attempt)
            print(f"429 on boxscore. Sleeping {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            return None, None, None, None

        break
    else:
        return None, None, None, None

    soup = BeautifulSoup(resp.text, "lxml")

    table = soup.find("table", id="line-score")
    if table is None:
        table = extract_commented_table(soup, "line-score")
    if table is None:
        return None, None, None, None

    tbody = table.find("tbody")
    if tbody is None:
        return None, None, None, None

    rows = []
    for tr in tbody.find_all("tr"):
        if "thead" not in (tr.get("class") or []):
            rows.append(tr)

    if len(rows) < 2:
        return None, None, None, None

    def get_int(td):
        if not td:
            return None
        txt = td.get_text(strip=True)
        return int(txt) if txt.isdigit() else None

    away_1h = get_int(rows[0].find("td", {"data-stat": "1"}))
    home_1h = get_int(rows[1].find("td", {"data-stat": "1"}))
    away_final = get_int(rows[0].find("td", {"data-stat": "T"}))
    home_final = get_int(rows[1].find("td", {"data-stat": "T"}))

    return home_1h, away_1h, home_final, away_final


def scrape_games(season_year: int, db_path: Path):
    conn = get_connection(db_path)
    cursor = conn.cursor()

    season_id = get_or_create_season(conn, season_year)

    cursor.execute("SELECT team_id, sportsref_id FROM teams")
    teams = cursor.fetchall()

    print(f"Scraping games for season {season_year}")
    print(f"Teams found: {len(teams)}")

    boxscores_scraped = 0  # NEW: progress counter

    for team_id, sportsref_id in teams:
        schedule_url = f"{BASE_URL}/cbb/schools/{sportsref_id}/{season_year}-schedule.html"
        print("Schedule:", schedule_url)

        try:
            resp = requests.get(schedule_url, headers=HEADERS, timeout=30)
        except requests.RequestException:
            time.sleep(5)
            continue

        if resp.status_code != 200:
            time.sleep(5)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table", id="schedule")
        if not table:
            time.sleep(SCHEDULE_SLEEP)
            continue

        rows = table.find("tbody").find_all("tr")

        for row in rows:
            if "thead" in (row.get("class") or []):
                continue

            date_td = row.find("td", {"data-stat": "date_game"})
            if not date_td:
                continue

            link = date_td.find("a")
            if not link:
                continue

            game_date = date_td.text.strip()
            box_href = link.get("href")
            box_url = BASE_URL + box_href

            loc_td = row.find("td", {"data-stat": "game_location"})
            location = "home"
            if loc_td and loc_td.text.strip() == "@":
                location = "away"
            elif loc_td and loc_td.text.strip().lower() == "n":
                location = "neutral"

            opp_td = row.find("td", {"data-stat": "opp_name"})
            opp_link = opp_td.find("a") if opp_td else None
            if not opp_link:
                continue

            opp_sportsref_id = opp_link.get("href").split("/")[3]

            cursor.execute(
                "SELECT team_id FROM teams WHERE sportsref_id = ?",
                (opp_sportsref_id,),
            )
            opp_row = cursor.fetchone()
            if not opp_row:
                continue

            opp_team_id = opp_row[0]

            if location == "away":
                home_team_id = opp_team_id
                away_team_id = team_id
            else:
                home_team_id = team_id
                away_team_id = opp_team_id

            try:
                cursor.execute(
                    """
                    INSERT INTO games
                    (season_id, date, home_team_id, away_team_id, location, sportsref_box_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (season_id, game_date, home_team_id, away_team_id, location, box_href),
                )
                game_id = cursor.lastrowid
            except Exception:
                cursor.execute(
                    "SELECT game_id FROM games WHERE sportsref_box_id = ?",
                    (box_href,),
                )
                row = cursor.fetchone()
                if not row:
                    continue
                game_id = row[0]

            home_1h, away_1h, home_final, away_final = scrape_boxscore_scores(box_url)

            cursor.execute(
                """
                INSERT INTO halftime_stats (game_id, home_first_half_pts, away_first_half_pts)
                VALUES (?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    home_first_half_pts = excluded.home_first_half_pts,
                    away_first_half_pts = excluded.away_first_half_pts
                """,
                (game_id, home_1h, away_1h),
            )

            cursor.execute(
                """
                UPDATE games
                SET home_final_score = ?,
                    away_final_score = ?
                WHERE game_id = ?
                """,
                (home_final, away_final, game_id),
            )

            boxscores_scraped += 1
            print(f"Boxscores scraped: {boxscores_scraped}")

            if boxscores_scraped % COOLDOWN_EVERY == 0:
                print(f"Cooling down for {COOLDOWN_SLEEP}s")
                time.sleep(COOLDOWN_SLEEP)

            time.sleep(BOX_SCORE_SLEEP)

        conn.commit()
        time.sleep(SCHEDULE_SLEEP)

    conn.close()
    print("Finished scraping games.")


def main():
    args = parse_arguments()
    scrape_games(args.season, Path(args.db))


if __name__ == "__main__":
    main()
