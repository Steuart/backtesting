from collections import deque
import backtrader as bt
import pandas as pd
from typing import Optional, Dict, Any
from database import fund_adj_dao
from database import fund_market_dao

class FundDataFeed(bt.feed.DataBase):
    params = (
        ('symbol', None),
        ('start', None),
        ('end', None),
        ('time_frame', None),
        ('adjust_type', None) # 调整类型，None 表示不调整，'forward' 表示前向调整，'backward' 表示后向调整
    )

    def __init__(self, **kwargs):
        super().__init__()
        self._rows: deque[pd.Series] = []
        self._resolved_cols: Dict[str, Optional[str]] = {}
        self.latest_adj: Optional[float] = None

    def start(self):
        super().start()
        # 检查参数
        if not self.p.symbol or not self.p.start or not self.p.end or not self.p.time_frame:
            raise ValueError("symbol, start, end, and time_frame are required parameters")
        
        # 查询数据
        self._rows = self.query_data()

        # 获取最新调整因子
        # self.latest_adj = fund_adj_dao.get_latest_adj(self.p.symbol)
        self.latest_adj = 1.0

    def _load(self):
        if not self._rows:
            return False

        row = self._rows.popleft()
        # 设置 datetime（必须是数值形式）
        self.lines.datetime[0] = bt.date2num(row.name)

        # 设置 OHLCV
        self.lines.open[0] = float(row['open'])
        self.lines.high[0] = float(row['high'])
        self.lines.low[0] = float(row['low'])
        self.lines.close[0] = float(row['close'])
        self.lines.volume[0] = float(row['vol'])
        self.lines.openinterest[0] = 0.0
        return True
    
    def query_data(self) -> deque[pd.Series]:
        fund_markets = fund_market_dao.list_fund_market(
            symbol=self.p.symbol,
            start_date=self.p.start,
            end_date=self.p.end,
            time_frame=self.p.time_frame
        )
        # fund_adjs = fund_adj_dao.list_fund_adj(
        #     symbol=self.p.symbol,
        #     start_date=self.p.start,
        #     end_date=self.p.end
        # )
        
        # fund_adj_map = fund_adjs.set_index('time')['adj_factor'].to_dict()
        # 保证按时间排序并以时间为索引
        fund_markets = fund_markets.sort_values('time').set_index('time')
        fund_adj_map = {}
        if self.p.adjust_type == 'forward':
            latest_adj = self.latest_adj if self.latest_adj is not None else 1.0
            adj_series = pd.Series(index=fund_markets.index, data=[fund_adj_map.get(ts) for ts in fund_markets.index]).ffill()
            factor = adj_series.fillna(latest_adj) / latest_adj
            fund_markets['open'] = fund_markets['open'] * factor
            fund_markets['high'] = fund_markets['high'] * factor
            fund_markets['low'] = fund_markets['low'] * factor
            fund_markets['close'] = fund_markets['close'] * factor
        elif self.p.adjust_type == 'backward':
            adj_series = pd.Series(index=fund_markets.index, data=[fund_adj_map.get(ts) for ts in fund_markets.index]).bfill()
            factor = adj_series.fillna(1.0)
            fund_markets['open'] = fund_markets['open'] * factor
            fund_markets['high'] = fund_markets['high'] * factor
            fund_markets['low'] = fund_markets['low'] * factor
            fund_markets['close'] = fund_markets['close'] * factor
        result = deque()
        for _, row in fund_markets.iterrows():
            result.append(row)
        print(f"load {len(result)} rows for {self.p.symbol}")
        return result

        