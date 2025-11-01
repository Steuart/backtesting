from collections import deque
import backtrader as bt
import pandas as pd
from database import fund_adj_dao
from database import fund_market_dao

def load_data(symbol: str, start: str, end: str, time_frame: str, adjust_type: str = 'forward') -> pd.DataFrame:
    fund_markets = fund_market_dao.list_fund_market(
        symbol=symbol,
        start_date=start,
        end_date=end,
        time_frame=time_frame
    )
    # fund_adjs = fund_adj_dao.list_fund_adj(
    #     symbol=symbol,
    #     start_date=start,
    #     end_date=end
    # )
    latest_adj = 1.0
    # fund_adj_map = fund_adjs.set_index('time')['adj_factor'].to_dict()
    # 保证按时间排序并以时间为索引
    fund_markets = fund_markets.sort_values('time').set_index('time')
    fund_adj_map = {}
    if adjust_type == 'forward':
        latest_adj = 1.0
        adj_series = pd.Series(index=fund_markets.index, data=[fund_adj_map.get(ts) for ts in fund_markets.index]).ffill().infer_objects(copy=False)
        factor = adj_series.astype(float).fillna(latest_adj) / latest_adj
        fund_markets['open'] = fund_markets['open'] * factor
        fund_markets['high'] = fund_markets['high'] * factor
        fund_markets['low'] = fund_markets['low'] * factor
        fund_markets['close'] = fund_markets['close'] * factor
    elif adjust_type == 'backward':
        adj_series = pd.Series(index=fund_markets.index, data=[fund_adj_map.get(ts) for ts in fund_markets.index]).bfill().infer_objects(copy=False)
        factor = adj_series.astype(float).fillna(1.0)
        fund_markets['open'] = fund_markets['open'] * factor
        fund_markets['high'] = fund_markets['high'] * factor
        fund_markets['low'] = fund_markets['low'] * factor
        fund_markets['close'] = fund_markets['close'] * factor
    print(f"load {len(fund_markets)} rows for {symbol}")
    return fund_markets
        