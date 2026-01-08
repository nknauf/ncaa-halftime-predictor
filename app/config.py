# app/config.py
# acts as central place for all local variables


from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

@dataclass(frozen=True)
class Config:
    db_path: Path = Path("data/ncaa_mbb.db")

    # Polling interval for live scoreboard
    poll_interval_seconds: int = 60

    # ESPN endpoints (public JSON)
    # Scoreboard is all we need for halftime detection + current scores.
    espn_scoreboard_url: str = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"

    # Basic run settings
    # NOTE: your "season_year" here is just metadata for events.
    season_year: int = 2025  # set per run, can override via CLI later

    load_dotenv()


CONFIG = Config()
