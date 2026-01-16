import backtrader as bt
from feeddata import fund_feeddata
from database import fund_dao
from observer.equity_observer import EquityObserver
from commission.fund_commission import FundCommission
import pandas as pd
from strategy.relative_strength_strategy import RelativeStrengthStrategy

def run_backtest():
    bars=500
    cash = 1000000.0
    end = pd.Timestamp.now()
    funds = fund_dao.list_fund(10000)
    cerebro = bt.Cerebro()
    
    for _,fund in funds.iterrows():
        code = fund['symbol']
        data = fund_feeddata.FundPandasData(name = code)
        data.load_data(symbol=code, end=end.strftime('%Y-%m-%d'), bars=bars, time_frame='1d')
        rows = data.datalength
        if (rows<bars):
            continue
        print(f"load {rows} rows for {code}")
        cerebro.adddata(data = data, name=code)
    cerebro.addstrategy(RelativeStrengthStrategy)
    cerebro.broker.setcash(cash)
    cerebro.broker.set_checksubmit(True)
    cerebro.broker.set_shortcash(False)
    cerebro.broker.addcommissioninfo(FundCommission())
    cerebro.addobserver(EquityObserver)


    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    print(f'begin: {cerebro.broker.getvalue():.2f}')
    
    results = cerebro.run()
    
    print(f'end: {cerebro.broker.getvalue():.2f}')
    
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
