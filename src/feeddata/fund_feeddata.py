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
        ('time_frame', None)
        ('adjust_type', None) # 调整类型，None 表示不调整，'forward' 表示前向调整，'backward' 表示后向调整
    )

    def __init__(self, **kwargs):
        super().__init__()
        self._rows: deque[Dict[str, Any]] = []
        self._idx: int = 0
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
        self.latest_adj = fund_adj_dao.get_latest_adj(self.p.symbol)

    def _load(self):
        """
        Backtrader 数据加载回调：每次调用推进一根bar。
        返回 True 表示成功加载一条；返回 False 表示数据结束。
        """
        if self._idx >= len(self._rows):
            return False

        row = self._rows[self._idx]

        dt = pd.to_datetime(row[0]).to_pydatetime()
        # 设置 datetime（必须是数值形式）
        self.lines.datetime[0] = bt.date2num(dt)

        # 设置 OHLCV
        self.lines.open[0] = float(row[2])
        self.lines.high[0] = float(row[3])
        self.lines.low[0] = float(row[4])
        self.lines.close[0] = float(row[5])
        self.lines.volume[0] = float(row[9])
        self.lines.openinterest[0] = 0.0
        self._idx += 1
        return True
    
    def query_data(self):
        fund_markets = fund_market_dao.list_fund_market(
            symbol=self.p.symbol,
            start_date=self.p.start,
            end_date=self.p.end,
            time_frame=self.p.time_frame
        )


        