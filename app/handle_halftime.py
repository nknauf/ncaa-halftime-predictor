# app/handle_halftime.py

import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

from app.db_live import LiveGame
from app.baseline_curve import lookup_baseline_prob
from app.team_mapping_static import get_sports_reference_name
from app.sources.espn import fetch_game_summary, extract_first_half_team_stats, HEADERS
from app.confidence_model import compute_confidence_with_stats
from app.messaging import (
    alert_config_from_env,
    notify_if_confident,
    build_halftime_message
)
from app.config import CONFIG

SHOULD_NOTIFY_THRESHOLD = 0.10  # only MEDIUM+


def handle_halftime(conn: sqlite3.Connection, game: LiveGame, season_year: int):
    """
    Handles a game that has JUST reached halftime.

    Assumptions:
    - season_games row already exists (created by poller)
    - season_id already resolved in poller
    """

    cursor = conn.cursor()
    game_live_id = game.game_live_id
    now_utc = datetime.now(timezone.utc).isoformat()

    cursor.execute(
        "SELECT season_id FROM seasons WHERE year = ?;",
        (season_year,)
    )
    row = cursor.fetchone()

    if not row:
        raise RuntimeError(f"Season {season_year} missing — poller should create it")

    season_id = row["season_id"]

    # Resolve season_games.game_id (CRITICAL)
    cursor.execute(
        """
        SELECT game_id
        FROM season_games
        WHERE game_live_id = ? AND season_id = ?;
        """,
        (game_live_id, season_id),
    )
    row = cursor.fetchone()

    if not row:
        raise RuntimeError(
            f"season_games row missing for {game_live_id} (poller bug)"
        )

    season_game_id = int(row["game_id"])


    # Resolve team IDs (canonical mapping)
    try:
        home_team_id = get_sports_reference_name(game.home_name)
        away_team_id = get_sports_reference_name(game.away_name)
    except KeyError:
        print("=================================================")
        print("[TEAM ALIAS MISSING]")
        print(f"{game.away_name} @ {game.home_name}")
        print("Add mapping to team_mapping_static.py")
        print("=================================================")
        return

    stats = None
    try:    
        summary = fetch_game_summary(
            CONFIG.espn_scoreboard_url,
            game.game_live_id,
            headers=HEADERS,
        )
        stats = extract_first_half_team_stats(summary)
    except Exception as e:
        print(f"[WARN] Could not fetch halftime stats: {e}")

    # Validate scores
    if game.home_score is None or game.away_score is None:
        return

    halftime_margin = game.home_score - game.away_score

    # Baseline probability lookup
    baseline_prob, baseline_weight = lookup_baseline_prob(halftime_margin)

    if stats is not None:
        confidence = compute_confidence_with_stats(
            p_baseline=baseline_prob,
            baseline_weight=baseline_weight,
            halftime_margin=halftime_margin,
            stats_home=stats["home"],
            stats_away=stats["away"]
        )
        source = "baseline+stats"
    else:
        confidence = abs(baseline_prob - 0.5) * baseline_weight
        source = "baseline_only"

    if confidence >= 0.20:
        bucket = "HIGH"
    elif confidence >= 0.10:
        bucket = "MEDIUM"
    else:
        bucket = "LOW"


    # create json explanation
    explanation = {
        "source": source,
        "halftime_margin": halftime_margin,
        "baseline_prob": baseline_prob,
        "stats_available": stats is not None,
        "home_team": game.home_name,
        "away_team": game.away_name,
    }

    cursor.execute(
        "SELECT 1 FROM predictions WHERE game_id = ?;",
        (season_game_id,)
    )
    if cursor.fetchone():
        return

    # Insert into predictions table
    cursor.execute(
        """
        INSERT INTO predictions (
            game_id,
            game_live_id,
            season_year,
            season_id,
            predicted_home_win_prob,
            predicted_home_final_margin,
            confidence,
            confidence_bucket,
            created_at_utc,
            explanation_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            season_game_id,
            game_live_id,
            season_year,
            season_id,
            baseline_prob,
            halftime_margin,
            confidence,
            bucket,
            now_utc,
            json.dumps(explanation),
        ),
    )

    conn.commit()


    # Notify if confident
    msg = build_halftime_message(
        away_name=game.away_name,
        home_name=game.home_name,
        away_score=game.away_score,
        home_score=game.home_score,
        p_home=baseline_prob,
        confidence_score=confidence,
        confidence_bucket=bucket,
        extra={"halftime_margin": halftime_margin},
    )

    notify_if_confident(
        conn=conn,
        alert_cfg=alert_config_from_env(),
        confidence_score=confidence,
        threshold=0.10,
        message=msg,
        metadata={
            "game_id": game_live_id,
            "season_year": season_year,
        },
    )
