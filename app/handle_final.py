# app/handle_final.py

import sqlite3
from datetime import datetime, timezone
from app.db_live import set_season_game_final

def handle_final(conn: sqlite3.Connection, game):
    """
    Resolve a prediction once a game reaches FINAL.

   Responsibilities:
    1. Finalize season_games (scores + status)
    2. Resolve prediction correctness

    Idempotent: safe to call multiple times.
    """

    cursor = conn.cursor()
    game_live_id = game.game_live_id

    if game.home_score is None or game.away_score is None:
        print(f"[FINAL] Missing final scores for {game_live_id}")
        return

    # Finalize season_games (always safe)
    set_season_game_final(
        conn=conn,
        game_live_id=game_live_id,
        home=game.home_score,
        away=game.away_score,
    )

    # Ensure we even have a prediction to resolve
    row = cursor.execute(
        """
        SELECT
            predicted_home_win_prob,
            confidence,
            resolved_at_utc
        FROM predictions
        WHERE game_live_id = ?;
        """,
        (game_live_id,),
    ).fetchone()

    if not row:
        # No halftime prediction was made
        return

    predicted_prob, confidence_score, resolved_at = row

    # Already resolved
    if resolved_at is not None:
        return

    final_home = game.home_score
    final_away = game.away_score
    final_margin = final_home - final_away

    home_win = 1 if final_home > final_away else 0

    # ---------------------------------------------------------
    # 3. Was the prediction correct?
    # ---------------------------------------------------------
    predicted_home_win = 1 if predicted_prob >= 0.5 else 0
    prediction_correct = 1 if predicted_home_win == home_win else 0

    # ---------------------------------------------------------
    # 4. Confidence bucket (persisted for analytics)
    # ---------------------------------------------------------
    if confidence_score >= 0.20:
        confidence_bucket = "HIGH"
    elif confidence_score >= 0.10:
        confidence_bucket = "MEDIUM"
    else:
        confidence_bucket = "LOW"

    resolved_at_utc = datetime.now(timezone.utc).isoformat()

    # ---------------------------------------------------------
    # 5. Persist resolution
    # ---------------------------------------------------------
    cursor.execute(
        """
        UPDATE predictions
        SET
            final_home_score = ?,
            final_away_score = ?,
            final_margin = ?,
            home_win = ?,
            prediction_correct = ?,
            confidence_bucket = ?,
            resolved_at_utc = ?
        WHERE game_live_id = ?;
        """,
        (
            final_home,
            final_away,
            final_margin,
            home_win,
            prediction_correct,
            confidence_bucket,
            resolved_at_utc,
            game_live_id,
        ),
    )

    conn.commit()

    print(
        f"[FINAL RESOLVED] {game.away_name} @ {game.home_name} | "
        f"Final: {final_away}-{final_home} | "
        f"Correct: {bool(prediction_correct)}"
    )
