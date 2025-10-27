import psycopg2
from config import config
from typing import List


def list_fund_codes(self) -> List[str]:
    conn = psycopg2.connect(config.trader_conn_str)
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
    fund_dao = FundDao()
    fund_codes = fund_dao.list_fund_codes()
    print(fund_codes)
