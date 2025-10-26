import backtrader as bt
import pandas as pd
from typing import Optional, Dict, Any
import config
import psycopg2

class FundDataFeed(bt.feed.DataBase):
    lines = ('open', 'high', 'low', 'close', 'volume', 'openinterest')
    params = (
        ('symbol', None),
        ('start', None),
        ('end', None),
        ('time_frame', None)
    )

    def __init__(self, **kwargs):
        super().__init__()
        # Backtrader 会把外部传入的参数绑定到 self.p 中，这里无需处理
        self._rows: list[Dict[str, Any]] = []
        self._idx: int = 0
        self._resolved_cols: Dict[str, Optional[str]] = {}

    def start(self):
        super().start()
        # 检查参数
        if not self.p.symbol or not self.p.start or not self.p.end or not self.p.time_frame:
            raise ValueError("symbol, start, end, and time_frame are required parameters")

        # 查询数据
        self._rows = self.query_data()

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
        """查询数据库数据"""
        conn = psycopg2.connect(config.trader_conn_str)
        cursor = conn.cursor()
        sql = """
        SELECT
            time,
            symbol,
            open,
            high,
            low,
            close,
            pre_close,
            change,
            pct_chg,
            vol,
            amount
        FROM
            fund_market
        WHERE
            symbol = %s
            AND time_frame = %s
            AND time >= %s
            AND time <= %s
        ORDER BY
            time ASC
        """
        cursor.execute(sql, (self.p.symbol, self.p.time_frame, self.p.start, self.p.end))
        rows = cursor.fetchall()
        conn.close()
        return rows