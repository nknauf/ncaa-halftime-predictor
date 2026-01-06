from fastapi import APIRouter
from app.db_live import connect
from app.config import CONFIG

from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta

ET = ZoneInfo("America/New_York")

router = APIRouter()

def sports_day_label(dt_utc):
    dt_et = dt_utc.astimezone(ET)   # transfers utc to et
    if dt_et.time() < time(6, 0):
        sports_day = dt_et.date() - timedelta(days=1)
    else:
        sports_day = dt_et.date()

    return sports_day.isoformat()

@router.get("/health")
def health():
    return {"status": "ok"}


def _wl_accuracy(conn, where_sql: str = "", params=()):
    """
    Helper to compute wins/losses/total/accuracy.
    prediction_correct:
      1 = win
      0 = loss
      NULL = pending/unresolved
    """
    sql = f"""
        SELECT
          COUNT(*) AS total,
          COALESCE(SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END), 0) AS wins,
          COALESCE(SUM(CASE WHEN prediction_correct = 0 THEN 1 ELSE 0 END), 0) AS losses,
          COALESCE(SUM(CASE WHEN prediction_correct IS NULL THEN 1 ELSE 0 END), 0) AS pending
        FROM predictions
        {where_sql}
    """
    row = conn.execute(sql, params).fetchone()
    total = int(row["total"])
    wins = int(row["wins"])
    losses = int(row["losses"])
    pending = int(row["pending"])

    decided = wins + losses
    accuracy = (wins / decided) if decided > 0 else None

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "decided": decided,
        "accuracy": accuracy,
    }


@router.get("/metrics/overall")
def metrics_overall():
    conn = connect(CONFIG.db_path)
    out = _wl_accuracy(conn)
    conn.close()
    return out


@router.get("/games/live")
def games_live():
    conn = connect(CONFIG.db_path)

    rows = conn.execute("""
        SELECT
            dg.game_live_id,
            dg.status,
            dg.home_name,
            dg.away_name,
            dg.home_score,
            dg.away_score,
            dg.start_time_utc,

            p.predicted_home_win_prob,
            p.confidence,
            p.confidence_bucket,
            p.prediction_correct
        FROM daily_games dg
        LEFT JOIN predictions p
            ON p.game_live_id = dg.game_live_id
        WHERE dg.status != 'FINAL'
        ORDER BY dg.last_seen_utc DESC;
    """).fetchall()

    conn.close()

    return [dict(r) for r in rows]


@router.get("/games/recent")
def games_recent():
    conn = connect(CONFIG.db_path)

    # 1️⃣ Find most recent resolved prediction
    row = conn.execute("""
        SELECT resolved_at_utc
        FROM predictions
        WHERE resolved_at_utc IS NOT NULL
        ORDER BY resolved_at_utc DESC
        LIMIT 1;
    """).fetchone()

    if not row:
        conn.close()
        return []

    resolved_utc = datetime.fromisoformat(row["resolved_at_utc"])

    sports_day = sports_day_label(resolved_utc)

    # 2️⃣ Fetch all FINAL games from that sports day
    rows = conn.execute("""
        SELECT
            g.date AS sports_day,
            dg.game_live_id,
            dg.home_name,
            dg.away_name,
            dg.home_score,
            dg.away_score,
            dg.status,

            p.predicted_home_win_prob,
            p.confidence,
            p.confidence_bucket,
            p.prediction_correct
        FROM daily_games dg
        JOIN predictions p
            ON p.game_live_id = dg.game_live_id
        JOIN games g
            ON g.game_live_id = dg.game_live_id
        WHERE dg.status = 'FINAL'
          AND g.date = ?
        ORDER BY dg.last_seen_utc DESC;
    """, (sports_day,)).fetchall()

    conn.close()

    return {
        "sports_day": sports_day,
        "games": [dict(r) for r in rows]
    }


@router.get("/games/season/{season_year}")
def games_by_season(season_year: int):
    conn = connect(CONFIG.db_path)

    # 1️⃣ Resolve season_id
    row = conn.execute(
        "SELECT season_id FROM seasons WHERE year = ?;",
        (season_year,)
    ).fetchone()

    if not row:
        conn.close()
        return {
            "season_year": season_year,
            "summary": {"wins": 0, "losses": 0, "pending": 0, "accuracy": None},
            "games": []
        }

    season_id = row["season_id"]

    # 2️⃣ Fetch games + predictions
    rows = conn.execute("""
        SELECT
            g.date,
            g.game_id,
            g.game_live_id,

            p.predicted_home_win_prob,
            p.confidence_bucket,
            p.prediction_correct,

            p.final_home_score,
            p.final_away_score
        FROM games g
        JOIN predictions p
            ON p.game_id = g.game_id
        WHERE g.season_id = ?
        ORDER BY g.date DESC;
    """, (season_id,)).fetchall()

    games = [dict(r) for r in rows]

    # 3️⃣ Compute record
    wins = sum(1 for g in games if g["prediction_correct"] == 1)
    losses = sum(1 for g in games if g["prediction_correct"] == 0)
    pending = sum(1 for g in games if g["prediction_correct"] is None)

    decided = wins + losses
    accuracy = (wins / decided) if decided > 0 else None

    conn.close()

    return {
        "season_year": season_year,
        "summary": {
            "wins": wins,
            "losses": losses,
            "pending": pending,
            "accuracy": accuracy
        },
        "games": games
    }


@router.get("/games/{game_id}")
def game_detail(game_id: int):
    conn = connect(CONFIG.db_path)

    row = conn.execute("""
        SELECT
            g.game_id,
            g.date,
            g.game_live_id,

            dg.home_name,
            dg.away_name,
            dg.home_score AS current_home_score,
            dg.away_score AS current_away_score,
            dg.status,

            p.predicted_home_win_prob,
            p.predicted_home_final_margin,
            p.confidence,
            p.confidence_bucket,
            p.prediction_correct,
            p.final_home_score,
            p.final_away_score,
            p.final_margin,
            p.created_at_utc,
            p.resolved_at_utc,
            p.explanation_json
        FROM games g
        JOIN predictions p ON p.game_id = g.game_id
        LEFT JOIN daily_games dg ON dg.game_live_id = g.game_live_id
        WHERE g.game_id = ?;
    """, (game_id,)).fetchone()

    conn.close()

    if not row:
        return {"error": "Game not found"}

    return dict(row)


@router.get("/metrics/confidence")
def metrics_by_confidence():
    conn = connect(CONFIG.db_path)

    rows = conn.execute("""
        SELECT
            confidence_bucket,
            COUNT(*) AS total,
            SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN prediction_correct = 0 THEN 1 ELSE 0 END) AS losses
        FROM predictions
        WHERE confidence_bucket IS NOT NULL
        GROUP BY confidence_bucket;
    """).fetchall()

    conn.close()
    return [dict(r) for r in rows]