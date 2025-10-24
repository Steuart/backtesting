import backtrader as bt
import pandas as pd
import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any

# 列名自动推断的候选集合
CANDIDATE_TIME = ['time', 'timestamp', 'ts', 'date', 'dt']
CANDIDATE_CLOSE = ['close', 'price', 'nav', 'last']
CANDIDATE_OPEN = ['open']
CANDIDATE_HIGH = ['high']
CANDIDATE_LOW = ['low']
CANDIDATE_VOLUME = ['volume', 'vol', 'amount', 'turnover']
CANDIDATE_CODE = ['fund_code', 'code', 'symbol']


def _infer_cols(conn, table: str, schema: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    使用 information_schema 获取列名，自动推断关键列。
    """
    eff_schema = schema or 'public'
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (eff_schema, table)
        )
        cols = [r[0] for r in cur.fetchall()]

    def pick(candidates):
        for c in candidates:
            if c in cols:
                return c
        return None

    return {
        'time_col': pick(CANDIDATE_TIME),
        'close_col': pick(CANDIDATE_CLOSE),
        'open_col': pick(CANDIDATE_OPEN),
        'high_col': pick(CANDIDATE_HIGH),
        'low_col': pick(CANDIDATE_LOW),
        'volume_col': pick(CANDIDATE_VOLUME),
        'code_col': pick(CANDIDATE_CODE),
    }


class TimescaleFundData(bt.feed.DataBase):
    """
    自定义 Backtrader DataFeed：从 TimescaleDB(trader 库) 的 fund_market 表读取数据。

    - 通过 SQLAlchemy 连接数据库并查询数据；
    - 在 `start()` 阶段一次性取出数据并缓存在内存；
    - 在 `_load()` 中逐bar推送到 Backtrader 引擎。
    """

    lines = ('open', 'high', 'low', 'close', 'volume', 'openinterest')

    params = (
        ('conn_str', None),          # 连接串，例如 postgresql://user:pass@host:5432/trader
        ('fund_code', None),
        ('start', None),
        ('end', None),
        ('table', 'fund_market'),
        ('schema', None),
        ('time_col', None),
        ('open_col', None),
        ('high_col', None),
        ('low_col', None),
        ('close_col', None),
        ('volume_col', None),
        ('code_col', None),
    )

    def __init__(self, **kwargs):
        super().__init__()
        # Backtrader 会把外部传入的参数绑定到 self.p 中，这里无需处理
        self._rows: list[Dict[str, Any]] = []
        self._idx: int = 0
        self._resolved_cols: Dict[str, Optional[str]] = {}

    def start(self):
        super().start()
        if not self.p.conn_str:
            raise ValueError('conn_str 参数不能为空，例如 postgresql://user:pass@host:5432/trader')
        # 兼容此前的 SQLAlchemy 风格连接串，将 +psycopg2 去掉
        conn_str = str(self.p.conn_str)
        if conn_str.startswith('postgresql+psycopg2://'):
            conn_str = conn_str.replace('+psycopg2', '', 1)
        
        # 创建 psycopg2 连接
        conn = psycopg2.connect(conn_str)
        
        # 自动推断列名（若未显式提供）
        if not (self.p.time_col and self.p.close_col):
            inferred = _infer_cols(conn, self.p.table, self.p.schema)
        else:
            inferred = {}
        
        time_col = self.p.time_col or inferred.get('time_col') or 'time'
        close_col = self.p.close_col or inferred.get('close_col') or 'close'
        open_col = self.p.open_col or inferred.get('open_col')
        high_col = self.p.high_col or inferred.get('high_col')
        low_col = self.p.low_col or inferred.get('low_col')
        volume_col = self.p.volume_col or inferred.get('volume_col')
        code_col = self.p.code_col or inferred.get('code_col')
        
        self._resolved_cols = {
            'time_col': time_col,
            'open_col': open_col,
            'high_col': high_col,
            'low_col': low_col,
            'close_col': close_col,
            'volume_col': volume_col,
            'code_col': code_col,
        }
        
        # 组装 SELECT 子句（安全引用标识符）
        select_idents = [sql.Identifier(time_col)]
        if open_col:
            select_idents.append(sql.Identifier(open_col))
        if high_col:
            select_idents.append(sql.Identifier(high_col))
        if low_col:
            select_idents.append(sql.Identifier(low_col))
        select_idents.append(sql.Identifier(close_col))
        if volume_col:
            select_idents.append(sql.Identifier(volume_col))
        
        # 构建 WHERE 条件
        clauses = []
        params = []
        if self.p.fund_code and code_col:
            clauses.append(sql.SQL("{} = %s").format(sql.Identifier(code_col)))
            params.append(self.p.fund_code)
        if self.p.start:
            clauses.append(sql.SQL("{} >= %s").format(sql.Identifier(time_col)))
            params.append(self.p.start)
        if self.p.end:
            clauses.append(sql.SQL("{} <= %s").format(sql.Identifier(time_col)))
            params.append(self.p.end)
        
        where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("")
        table_sql = (
            sql.SQL("{}.{}").format(sql.Identifier(self.p.schema), sql.Identifier(self.p.table))
            if self.p.schema else sql.Identifier(self.p.table)
        )
        order_sql = sql.SQL(" ORDER BY {} ASC").format(sql.Identifier(time_col))
        query = sql.SQL("SELECT {cols} FROM {table}").format(
            cols=sql.SQL(', ').join(select_idents),
            table=table_sql,
        ) + where_sql + order_sql
        
        # 执行查询
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
        conn.close()
        
        # 转 DataFrame 并统一列名
        df = pd.DataFrame(rows, columns=colnames)
        rename_map = {time_col: 'Date', close_col: 'Close'}
        if open_col:
            rename_map[open_col] = 'Open'
        if high_col:
            rename_map[high_col] = 'High'
        if low_col:
            rename_map[low_col] = 'Low'
        if volume_col:
            rename_map[volume_col] = 'Volume'
        df = df.rename(columns=rename_map)
        
        # 补齐缺失列
        if 'Open' not in df.columns:
            df['Open'] = df['Close']
        if 'High' not in df.columns:
            df['High'] = df['Close']
        if 'Low' not in df.columns:
            df['Low'] = df['Close']
        if 'Volume' not in df.columns:
            df['Volume'] = 0
        
        # 规范类型与缓存
        df['Date'] = pd.to_datetime(df['Date'])
        self._rows = df.to_dict(orient='records')
        self._idx = 0

    def _load(self):
        """
        Backtrader 数据加载回调：每次调用推进一根bar。
        返回 True 表示成功加载一条；返回 False 表示数据结束。
        """
        if self._idx >= len(self._rows):
            return False

        row = self._rows[self._idx]
        self._idx += 1

        dt = pd.to_datetime(row['Date']).to_pydatetime()
        # 设置 datetime（必须是数值形式）
        self.lines.datetime[0] = bt.date2num(dt)

        # 设置 OHLCV
        self.lines.open[0] = float(row.get('Open', row['Close']))
        self.lines.high[0] = float(row.get('High', row['Close']))
        self.lines.low[0] = float(row.get('Low', row['Close']))
        self.lines.close[0] = float(row['Close'])
        self.lines.volume[0] = float(row.get('Volume', 0))
        self.lines.openinterest[0] = 0.0

        return True