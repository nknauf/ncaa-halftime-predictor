from fastapi import APIRouter
from app.db_live import connect
from app.config import CONFIG

router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/predictions/today")
def predictions_today():
    """
    Today's predictions (based on created_at_utc).
    Returns rows as dicts, including:
    - explanation_json (string)
    - prediction_correct (0/1/NULL)
    - final scores (nullable)
    """
    conn = connect(CONFIG.db_path)
    rows = conn.execute("""
        SELECT *
        FROM predictions
        WHERE date(created_at_utc) = date('now')
        ORDER BY created_at_utc DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


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


@router.get("/metrics/today")
def metrics_today():
    conn = connect(CONFIG.db_path)
    out = _wl_accuracy(conn, "WHERE date(created_at_utc) = date('now')")
    conn.close()
    return out


@router.get("/metrics/daily")
def metrics_daily():
    """
    Daily W/L and accuracy for all days (UTC).
    """
    conn = connect(CONFIG.db_path)
    rows = conn.execute("""
        SELECT
          date(created_at_utc) AS day,
          COUNT(*) AS total,
          COALESCE(SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END), 0) AS wins,
          COALESCE(SUM(CASE WHEN prediction_correct = 0 THEN 1 ELSE 0 END), 0) AS losses,
          COALESCE(SUM(CASE WHEN prediction_correct IS NULL THEN 1 ELSE 0 END), 0) AS pending
        FROM predictions
        GROUP BY date(created_at_utc)
        ORDER BY day DESC
    """).fetchall()
    conn.close()

    out = []
    for r in rows:
        decided = int(r["wins"]) + int(r["losses"])
        accuracy = (int(r["wins"]) / decided) if decided > 0 else None
        out.append({
            "day": r["day"],
            "total": int(r["total"]),
            "wins": int(r["wins"]),
            "losses": int(r["losses"]),
            "pending": int(r["pending"]),
            "decided": decided,
            "accuracy": accuracy,
        })
    return out
