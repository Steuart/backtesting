import pandas as pd
from sqlalchemy import create_engine, text
from common import config

def list_fund_market(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    db_url = config.DB_URL
    query = """
        SELECT * FROM fund_adj
        WHERE symbol = :symbol
        AND time >= :start_date
        AND time <= :end_date
    """
    engine = create_engine(db_url)
    params = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date
    }
    df = pd.read_sql(text(query), engine, params=params)
    return df

def get_latest_adj(symbol: str) -> float:
    db_url = config.DB_URL
    query = """
        SELECT fund_adj FROM fund_adj
        WHERE symbol = :symbol
        ORDER BY date DESC
        LIMIT 1
    """
    engine = create_engine(db_url)
    params = {'symbol': symbol}
    df = pd.read_sql(text(query), engine, params=params)
    return df.iloc[0]