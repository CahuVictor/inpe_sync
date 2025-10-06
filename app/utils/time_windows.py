# app\utils\time_windows.py
from datetime import datetime, timedelta, timezone

def iso_date(d: datetime) -> str:
    """
    Converte datetime para 'YYYY-MM-DD' (UTC).
    """
    return d.strftime("%Y-%m-%d")

def window_from_last(last_date_str: str | None, days: int = 7):
    """
    Calcula janela [start, end] baseada no último registro (ou últimos `days`).

    - Se `last_date_str` existir, usa dia seguinte como início;
    - Caso contrário, usa `UTC now - days`.
    """
    # if last_date_str:
    #     start = datetime.fromisoformat(last_date_str[:10])  # yyyy-mm-dd
    # else:
    #     start = datetime.utcnow() - timedelta(days=days)
    # end = datetime.utcnow()
    # return iso_date(start), iso_date(end)
    today = datetime.now(timezone.utc).date()
    if last_date_str:
        try:
            # ISO "2025-01-03T17:09:00Z"
            dt = datetime.fromisoformat(last_date_str.replace("Z", "+00:00"))
            start = dt.date()
        except Exception:
            start = today - timedelta(days=days)
    else:
        start = today - timedelta(days=days)
    end = today
    return (start.isoformat(), end.isoformat())
