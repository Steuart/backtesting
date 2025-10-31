from database import fund_market_dao

data = fund_market_dao.list_fund_market(
    symbol='159117.SZ',
    start_date='2025-01-01',
    end_date='2025-10-31',
    time_frame='1d'
)
print(data)