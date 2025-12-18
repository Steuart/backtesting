from collections import deque
import backtrader as bt
import pandas as pd
from database import fund_adj_dao
from database import fund_market_dao

class FundPandasData(bt.feeds.PandasData):
    lines = ('pct_chg','volume', 'interest')
    params = (
        ('pct_chg', 'pct_chg'),
        ('volume', 'volume'),
        ('interest', 'interest'),
    )

def load_data(sysbol: str, end: str, bars:int, time_frame:str) -> pd.DateFrame:
    fund_markets = fund_market_dao.list_by_limit(
        symbol=symbol,
        end_date=end,
        limit=bars,
        time_frame=time_frame
    )
    fund_markets = fund_markets.sort_values('time').set_index('time')
    fund_markets['open'] = fund_markets['open']
    fund_markets['high'] = fund_markets['high']
    fund_markets['low'] = fund_markets['low']
    fund_markets['close'] = fund_markets['close']
    fund_markets['volume'] = fund_markets['volume']
    fund_markets['pct_chg'] = fund_markets['pct_chg']
    fund_markets['interest'] = fund_markets['interest']
    return fund_markets

def load_data_with_adj(symbol: str, end: str, bars: int, time_frame: str, adjust_type: str = 'forward') -> pd.DataFrame:
    fund_markets = fund_market_dao.list_by_limit(
        symbol=symbol,
        end_date=end,
        limit=bars,
        time_frame=time_frame
    )
    fund_adjs = fund_adj_dao.list_by_limit(
        symbol=symbol,
        end_date=end,
        limit=bars
    )
    latest_adj = 1.0
    fund_adj_map = fund_adjs.set_index('time')['adj_factor'].to_dict()
    # 保证按时间排序并以时间为索引
    fund_markets = fund_markets.sort_values('time').set_index('time')
    fund_adj_map = {}
    factor = None
    adj_series = pd.Series(index=fund_markets.index, data=[fund_adj_map.get(ts) for ts in fund_markets.index]).ffill().infer_objects(copy=False)
    if adjust_type == 'forward':
        factor = adj_series.astype(float).fillna(1.0) / latest_adj
    elif adjust_type == 'backward':
        factor = adj_series.astype(float).fillna(1.0)
    fund_markets['open'] = fund_markets['open'] * factor
    fund_markets['high'] = fund_markets['high'] * factor
    fund_markets['low'] = fund_markets['low'] * factor
    fund_markets['close'] = fund_markets['close'] * factor
    return fund_markets