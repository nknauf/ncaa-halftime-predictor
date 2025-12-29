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


def handle_halftime(conn: sqlite3.Connection, game: LiveGame, season_year: int):
    """
    Handles a game that has JUST reached halftime.

    game: LiveGame:
        {
            "game_live_id": str,
            "date": str,
            "season_year": int,
            "home_name": str,
            "away_name": str,
            "home_score": int,
            "away_score": int,
        }
    """

    cursor = conn.cursor()
    game_id = game.game_live_id
    now_iso = datetime.now(timezone.utc).isoformat()

    # Idempotency check
    cursor.execute(
        """
        SELECT 1
        FROM halftime_events
        WHERE game_live_id = ?;
        """,
        (game_id,),
    )
    if cursor.fetchone():
        return  # already processed


    # 2. Persist halftime event
    cursor.execute(
        """
        INSERT INTO halftime_events (
            game_live_id,
            date,
            season_year,
            home_halftime,
            away_halftime,
            captured_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        (
            game_id,
            game.date,
            season_year,
            game.home_score,
            game.away_score,
            now_iso,
        ),
    )
    conn.commit()

    try:
        home_team_id = get_sports_reference_name(game.home_name)
        away_team_id = get_sports_reference_name(game.away_name)
    except KeyError as e:
        print("============================================================")
        print("[STATIC ALIAS MISSING]")
        print(f"game_id: {game_id}")
        print(f"away ESPN name: {game.away_name}")
        print(f"home ESPN name: {game.home_name}")
        print("ACTION: add mapping to team_mapping_static.py")
        print("============================================================")

        cursor.execute(
            """
            UPDATE halftime_events
            SET home_team_id = ?, away_team_id = ?
            WHERE game_live_id = ?;
            """,
            (None, None, game_id),
        )
        conn.commit()
        return

    stats = None
    try:    
        summary = fetch_game_summary(
            CONFIG.espn_scoreboard_url,
            game.game_live_id,
            headers=HEADERS,
        )
        stats = extract_first_half_team_stats(summary)

        cursor.execute(
            """
            UPDATE halftime_events
            SET halftime_stats_json = ?
            WHERE game_live_id = ?;
            """,
            (json.dumps(stats), game_id),
        )
        conn.commit()

    except Exception as e:
        print(f"[WARN] Failed to fetch halftime stats for {game_id}: {e}")

    # Validate scores
    if game.home_score is not None and game.away_score is not None:
        halftime_margin = game.home_score - game.away_score
    else:
        return

    # Baseline probability lookup
    baseline_prob, baseline_weight = lookup_baseline_prob(halftime_margin)

    if stats is not None:
        margin_confidence_score = compute_confidence_with_stats(
            p_baseline=baseline_prob,
            baseline_weight=baseline_weight,
            halftime_margin=halftime_margin,
            stats_home=stats["home"],
            stats_away=stats["away"]
        )
        confidence_source = "baseline+stats"
    else: # Fallback: baseline-only confidence from halftime margin
        margin_confidence_score = abs(baseline_prob - 0.5) * baseline_weight
        confidence_source = "baseline_only"

    if margin_confidence_score >= 0.20:
        margin_confidence = "HIGH"
    elif margin_confidence_score >= 0.10:
        margin_confidence = "MEDIUM"
    else:
        margin_confidence = "LOW"

    SHOULD_NOTIFY_THRESHOLD = 0.10  # only MEDIUM+

    msg = build_halftime_message(
    away_name=game.away_name,
    home_name=game.home_name,
    away_score=game.away_score,
    home_score=game.home_score,
    p_home=baseline_prob,
    confidence_score=margin_confidence_score,
    confidence_bucket=margin_confidence,
    extra={
        "halftime_margin": halftime_margin,
        },
    )

    triggered = notify_if_confident(
        conn=conn,
        alert_cfg=alert_config_from_env(),
        confidence_score=margin_confidence_score,
        threshold=SHOULD_NOTIFY_THRESHOLD,
        message=msg,
        metadata={
            "game_id": game.game_live_id,
            "season_year": season_year,
        },
    )

    # create json explanation
    json_explanation = {
        "source": confidence_source,
        "halftime_margin": halftime_margin,
        "baseline_prob": baseline_prob,
        "stats_available": stats is not None,
        "home_team": game.home_name,
        "away_team": game.away_name,
    }

    # 6. Persist prediction
    cursor.execute(
        """
        INSERT INTO predictions (
            game_live_id,
            season_year,
            predicted_home_win_prob,
            predicted_home_final_margin,
            confidence,
            created_at_utc,
            explanation_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (
            game_id,
            season_year,
            baseline_prob,
            halftime_margin,  # placeholder until margin model
            margin_confidence_score,
            now_iso,
            json.dumps(json_explanation),
        ),
    )

    conn.commit()
