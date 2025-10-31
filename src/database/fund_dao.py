import pandas as pd
from sqlalchemy import create_engine,text
from common import config

def list_fund(start_date: str, end_date: str) -> pd.DataFrame:
    db_url = config.DB_URL
    query = """
        SELECT * FROM fund
        WHERE focus_on = 1
        AND time >= :start_date
        AND time <= :end_date
    """
    engine = create_engine(db_url)
    params = {
        'start_date': start_date,
        'end_date': end_date
    }
    df = pd.read_sql(text(query), engine, params=params)
    return df
