import backtrader as bt
import datetime
import pandas as pd
import numpy as np

# 导入策略和数据源
from strategy import SimpleMovingAverageStrategy
from feeddata.fund_feeddata import create_fund_dataframe

def run_backtest():
    """运行回测"""
    print("开始运行Backtrader演示程序...")
    
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(SimpleMovingAverageStrategy)
    
    # 使用FundDataFeed作为数据源
    # 注意：这里需要根据实际情况配置数据库连接参数
    try:
        # 创建基金数据源 DataFrame 并使用 PandasData
        df = create_fund_dataframe(
            symbol='508092.SH',
            start='2025-01-01',
            end='2025-12-31',
            time_frame='1d',
            adjust_type='forward',
        )
        data = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(data)
        
    except Exception as e:
        print(f"无法连接到数据库，使用示例数据: {e}")
        # 如果数据库连接失败，回退到示例数据
        df = create_sample_data()
        data = bt.feeds.PandasData(dataname=df)
        cerebro.adddata(data)
    
    # 设置初始资金
    cerebro.broker.setcash(10000.0)
    
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
    #     cerebro.plot(style='candlestick', barup='green', bardown='red')
    #     print('图表已生成！')
    # except Exception as e:
    #     print(f'图表生成失败: {e}')
    #     print('可能需要安装matplotlib: pip install matplotlib')


if __name__ == '__main__':
    run_backtest()