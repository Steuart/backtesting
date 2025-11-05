import backtrader as bt
from feeddata import fund_feeddata
from database import fund_dao

# 导入策略和数据源
from strategy.relative_strength_strategy import RelativeStrengthStrategy    

def run_backtest():    
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    funds = fund_dao.list_fund(10000)
    for index,fund in funds.iterrows():
        code = fund['symbol']
        data = fund_feeddata.load_data(
                symbol=code,
                start='2023-05-01',
                end='2025-12-31',
                time_frame='1d'
            )
        rows = len(data)
        if (rows<30):
            continue
        # 添加数据到Cerebro
        # print(f"load {rows} rows for {code}")
        cerebro.adddata(data = bt.feeds.PandasData(dataname=data), name=code)
    cerebro.addstrategy(RelativeStrengthStrategy)
    
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    
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
    print(f'max drawdown: {strat.analyzers.drawdown.get_analysis().get("max", {}).get("drawdown", 0):.2%}')
    
    # 绘制结果图表
    # try:
    #     print('\n正在生成图表...')
    #     cerebro.plot(style='candlestick', barup='red', bardown='gray')
    #     print('图表已生成！')
    # except Exception as e:
    #     print(f'图表生成失败: {e}')
    #     print('可能需要安装matplotlib: pip install matplotlib')


if __name__ == '__main__':
    run_backtest()