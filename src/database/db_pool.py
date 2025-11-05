from sqlalchemy import create_engine
from common import config

engine = None

def get_engine():
    global engine
    if engine is None:
        engine = create_engine(config.DB_URL)
    return engine