import pandas as pd
from sqlalchemy import text
from database.db_pool import get_engine

def list_fund_market(symbol: str, start_date: str, end_date: str, time_frame: str = '1d') -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT * FROM fund_market
        WHERE symbol = :symbol
        AND time >= :start_date
        AND time <= :end_date
        AND time_frame = :time_frame
        ORDER BY time ASC
    """
    params = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date,
        'time_frame': time_frame
    }
    df = pd.read_sql(text(query), engine, params=params)
    return df

def list_pct_chg(symbol:str, start_date: str, end_date: str, time_frame: str = '1d') -> pd.DataFrame:
    engine = get_engine()
    base = """
        SELECT time, symbol, pct_chg
        FROM fund_market
        WHERE symbol = :symbol
        AND time >= :start_date AND time <= :end_date
    """
    if time_frame:
        base += " AND time_frame = :time_frame"
    base += " ORDER BY time ASC"
    params = {'symbol': symbol, 'start_date': start_date, 'end_date': end_date}
    if time_frame:
        params['time_frame'] = time_frame
    return pd.read_sql(text(base), engine, params=params)
