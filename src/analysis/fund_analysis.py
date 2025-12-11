import pandas as pd
from sqlalchemy import text
from database.db_pool import get_engine
import matplotlib.dates as mdates
import mplfinance as mpf
import matplotlib.pyplot as plt

def load_funds_by_correlation(correlation_hash: str, limit: int = 10) -> pd.DataFrame:
    """读取 fund 表中指定 correlation 的基金代码列表。"""
    engine = get_engine()
    query = text(
        """
        SELECT symbol, name
        FROM fund
        WHERE correlation = :corr
        ORDER BY issue_date ASC
        LIMIT :limit
        """
    )
    try:
        df = pd.read_sql(query, engine, params={"corr": correlation_hash, "limit": limit})
        return df
    except Exception as e:
        print(f"查询基金时出错：{e}")
        return pd.DataFrame()


def load_market_ohlc(symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    """查询指定 symbol 在近一年的 OHLC 数据。"""
    engine = get_engine()
    query = text(
        """
        SELECT time, open, high, low, close, vol
        FROM fund_market
        WHERE symbol = :symbol
          AND time >= :start_date AND time <= :end_date
        ORDER BY time ASC
        """
    )
    df = pd.read_sql(query, engine, params={"symbol": symbol, "start_date": start_date, "end_date": end_date})
    if df.empty:
        return df
    # 统一为无时区的 datetime，避免绘图出现 1970 年刻度
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["time"] = df["time"].dt.tz_localize(None)
    df = df.set_index("time").sort_index()
    # 重命名为 mplfinance 规范列名
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "vol": "Volume",
    })
    return df[["Open", "High", "Low", "Close", "Volume"]]


def plot_group_candles(correlation_hash: str, limit: int = 10):
    """
    读取 fund 中 correlation 为指定值的基金，查询最近 1 年行情，
    并在一张图（一个 Figure）里用多个子图绘制各自的 K 线图。
    """
    funds = load_funds_by_correlation(correlation_hash, limit=limit)
    if funds.empty:
        print("未查询到符合条件的基金。")
        return

    end_date = pd.Timestamp.today().normalize()
    start_date = end_date - pd.DateOffset(years=1)

    # 逐个加载行情，过滤无数据的标的
    series = []
    for _, row in funds.iterrows():
        symbol = row["symbol"]
        ohlc = load_market_ohlc(symbol, start_date, end_date)
        if ohlc.empty or len(ohlc) < 10:
            print(f"{symbol} 近一年无足够行情数据，跳过")
            continue
        series.append((symbol, ohlc))

    if not series:
        print("所选基金近一年均无有效数据，无法绘制。")
        return

    # 对齐所有基金的日期索引（统一到共同的日历）
    all_dates = sorted({dt for _, df in series for dt in df.index})
    common_index = pd.DatetimeIndex(all_dates, name='time')
    aligned = []
    for code, ohlc in series:
        aligned_df = ohlc.reindex(common_index)
        aligned.append((code, aligned_df))
    series = aligned

    n = len(series)
    # 创建一个 Figure，包含 n 个竖向子图，共享 x 轴
    
    fig, axes = plt.subplots(nrows=n, ncols=1, figsize=(12, 2.8 * n), sharex=True)
    if n == 1:
        axes = [axes]

    for ax, (code, ohlc) in zip(axes, series):
        mpf.plot(ohlc, type="candle", ax=ax, volume=False, axtitle=code, style="charles")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # 固定调用示例：绘制 correlation 为指定值的 10 个基金
    plot_group_candles("e6ed7db3f417085d7e65b23310236125", limit=10)