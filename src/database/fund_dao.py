import pandas as pd
from sqlalchemy import text
from database.db_pool import get_engine

def list_fund(limit: int = 50) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT * FROM fund order by found_date ASC limit :limit
    """
    result = pd.read_sql(text(query), engine, params = {"limit": limit})
    return result
