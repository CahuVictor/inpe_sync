from datetime import datetime, timedelta

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
    if last_date_str:
        start = datetime.fromisoformat(last_date_str[:10])  # yyyy-mm-dd
    else:
        start = datetime.utcnow() - timedelta(days=days)
    end = datetime.utcnow()
    return iso_date(start), iso_date(end)
