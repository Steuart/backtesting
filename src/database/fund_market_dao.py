import pandas as pd
from sqlalchemy import create_engine, text
from common import config

def list_fund_market(symbol: str, start_date: str, end_date: str, time_frame: str = '1d') -> pd.DataFrame:
    db_url = config.DB_URL
    query = """
        SELECT * FROM fund_market
        WHERE symbol = :symbol
        AND time >= :start_date
        AND time <= :end_date
        AND time_frame = :time_frame
        ORDER BY time ASC
    """
    engine = create_engine(db_url)
    params = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date,
        'time_frame': time_frame
    }
    df = pd.read_sql(text(query), engine, params=params)
    return df