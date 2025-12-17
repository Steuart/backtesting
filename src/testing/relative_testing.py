import backtrader as bt
from feeddata import fund_feeddata
from database import fund_dao
from observer.equity_observer import EquityObserver
from commission.fund_commission import FundCommission
import pandas as pd
from database.fund_market_dao import compute_volatility_dict, compute_correlation_matrix
from strategy.relative_strength_strategy import RelativeStrengthStrategy

def pivot_pct_chg(df: pd.DataFrame, symbols=None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df['time'] = pd.to_datetime(df['time'])
    if symbols is not None:
        df = df[df['symbol'].isin(list(symbols))]
    return df.pivot(index='time', columns='symbol', values='pct_chg').dropna(how='all')

def run_backtest():
    day_count=500
    end = pd.Timestamp.now()
    start = (end - pd.DateOffset(days=day_count)).strftime('%Y-%m-%d')
    funds = fund_dao.list_fund(10000)
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    for _,fund in funds.iterrows():
        code = fund['symbol']
        data = fund_feeddata.load_data(
                symbol=code,
                start=start,
                end=end.strftime('%Y-%m-%d'),
                time_frame='1d'
            )
        rows = len(data)
        if (rows<365):
            continue
        # 添加数据到Cerebro
        print(f"load {rows} rows for {code}")
        cerebro.adddata(data = fund_feeddata.FundPandasData(dataname=data), name=code)
    cerebro.addstrategy(RelativeStrengthStrategy)
    
    cerebro.broker.setcash(100000.0)
    cerebro.broker.set_checksubmit(True)
    cerebro.broker.set_shortcash(False)
    # 使用默认佣金规则应用到所有数据，不指定 name
    cerebro.broker.addcommissioninfo(FundCommission())
    
    cerebro.addobserver(EquityObserver)


    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    print(f'begin: {cerebro.broker.getvalue():.2f}')
    
    # 运行回测
    results = cerebro.run()
    
    print(f'end: {cerebro.broker.getvalue():.2f}')
    
    # 获取分析结果
    strat = results[0]
    
    print('\n=== result ===')
    print(f'sharpe ratio: {strat.analyzers.sharpe.get_analysis().get("sharperatio", "N/A")}')
    print(f'total return: {strat.analyzers.returns.get_analysis().get("rtot", 0):.2%}')
    # Backtrader DrawDown 返回的是百分比点值（0-100），不需要再乘以 100
    print(f'max drawdown: {strat.analyzers.drawdown.get_analysis().get("max", {}).get("drawdown", 0):.2f}%')
    
    for data in cerebro.datas:
        data.plotinfo.plot = False
    
    # 绘制结果图表
    try:
        print('\n正在生成图表...')
        cerebro.plot(style='line', barup='red', bardown='green')
        print('图表已保存到当前目录。')
    except Exception as e:
        print(f'无法生成图表: {e}')


if __name__ == '__main__':
    run_backtest()
