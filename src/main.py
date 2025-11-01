import backtrader as bt
from feeddata import fund_feeddata
from database import fund_dao

# 导入策略和数据源
from strategy.relative_strength_strategy import RelativeStrengthStrategy    

def run_backtest():
    """运行回测"""
    print("开始运行Backtrader演示程序...")
    
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    funds = fund_dao.list_fund(500)
    for index,fund in funds.iterrows():
        data = fund_feeddata.load_data(
                symbol=fund['symbol'],  # 示例基金代码
                start='2023-05-01',
                end='2025-12-31',
                time_frame='1d'  # 日线数据
            )
        # 添加数据到Cerebro
        cerebro.adddata(data = bt.feeds.PandasData(dataname=data), name=fund['symbol'])
    # 添加策略
    cerebro.addstrategy(RelativeStrengthStrategy)
    
    # 设置初始资金
    cerebro.broker.setcash(100000.0)
    
    # 设置手续费
    cerebro.broker.setcommission(commission=0.001)  # 0.1%手续费
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    print(f'初始资金: {cerebro.broker.getvalue():.2f}')
    
    # 运行回测
    results = cerebro.run()
    
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
    
    # 获取分析结果
    strat = results[0]
    
    print('\n=== 回测结果分析 ===')
    print(f'夏普比率: {strat.analyzers.sharpe.get_analysis().get("sharperatio", "N/A")}')
    print(f'总收益率: {strat.analyzers.returns.get_analysis().get("rtot", 0):.2%}')
    print(f'最大回撤: {strat.analyzers.drawdown.get_analysis().get("max", {}).get("drawdown", 0):.2%}')
    
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