import psycopg2
from common import config
from typing import List


def list_fund_codes() -> List[str]:
    conn = psycopg2.connect(config.DB_URL)
    cursor = conn.cursor()
    sql = """
    SELECT DISTINCT symbol FROM fund_market
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in rows]


if __name__ == '__main__':
    fund_codes = list_fund_codes()
    for fund_code in fund_codes:
        print(fund_code)
