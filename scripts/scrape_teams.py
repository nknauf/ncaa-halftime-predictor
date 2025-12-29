"""
scrape_teams.py

Scrapes the list of NCAA Division I teams for a given season
from Sports-Reference and stores them in the database.
"""

import argparse
import time
import requests
from bs4 import BeautifulSoup, Comment
from pathlib import Path

from db import (
    get_connection, 
    get_or_create_season, 
    get_team_by_sportsref_id, 
    insert_team
)


BASE_URL = "https://www.sports-reference.com"


def parse_arguments():
    parser = argparse.ArgumentParser(description="Scrape NCAA teams for a season")
    parser.add_argument(
        "--season",
        type=int,
        required=True,
        help="Season ending year (e.g. 2023)"
    )
    parser.add_argument(
        "--db",
        type=str,
        default=str(Path("data") / "ncaa_mbb.db"),
        help="Path to SQLite database"
    )
    return parser.parse_args()


def scrape_teams(season_year: int, db_path: Path) -> None:
    url = f"{BASE_URL}/cbb/seasons/{season_year}-school-stats.html"
    print("Fetching:", url)

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ncaa-halftime-bot/1.0)"
    }

    time.sleep(5)
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 429:
        print("Rate limited. Try again later.")
        return
    response.raise_for_status()

    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    table = soup.find("table", id="basic_school_stats")
    if table is None:
        raise RuntimeError("Could not find school stats table")

    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    db_path = db_path.resolve()
    print("SCRAPER DB PATH:", repr(db_path))

    conn = get_connection(db_path)
    try:
        season_id = get_or_create_season(conn, season_year)
        print(f"Season ID: {season_id}")

        inserted = 0

        for row in rows:
            td = row.find("td", {"data-stat": "school_name"})
            if td is None:
                continue

            link = td.find("a")
            if link is None:
                continue

            team_name = link.text.strip()
            href = link.get("href")

            # Example: /cbb/schools/duke/2023.html → duke
            sportsref_id = href.split("/")[3]

            existing_team_id = get_team_by_sportsref_id(conn, sportsref_id)
            if existing_team_id:
                continue

            insert_team(conn, team_name, sportsref_id)
            inserted += 1


        total = len(rows)
        print(f"Processed {total} rows, inserted {inserted} new teams")

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM teams;")
        print("COUNT FROM SCRAPER DB:", cursor.fetchone()[0])

    finally:
        conn.close()


def main():
    args = parse_arguments()
    db_path = Path(args.db).expanduser().resolve()
    scrape_teams(args.season, db_path)


if __name__ == "__main__":
    main()
