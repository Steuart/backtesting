import pandas as pd
from sqlalchemy import create_engine,text
from common import config

def list_fund(limit: int = 50) -> pd.DataFrame:
    db_url = config.DB_URL
    query = """
        SELECT * FROM fund order by found_date ASC limit :limit
    """
    engine = create_engine(db_url)
    result = pd.read_sql(text(query), engine, params = {"limit": limit})
    return result
