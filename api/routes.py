from fastapi import APIRouter
from app.db_live import connect
from app.config import CONFIG

router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok"}

@router.get("/predictions/today")
def predictions_today():
    conn = connect(CONFIG.db_path)
    rows = conn.execute("""
        SELECT *
        FROM predictions
        WHERE date(game_date_utc) = date('now')
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]
