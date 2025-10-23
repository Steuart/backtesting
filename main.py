import backtrader as bt
import datetime
import pandas as pd


class SimpleMovingAverageStrategy(bt.Strategy):
    """
    简单移动平均线交叉策略
    当短期均线上穿长期均线时买入
    当短期均线下穿长期均线时卖出
    """
    
    params = (
        ('short_period', 10),  # 短期移动平均线周期
        ('long_period', 30),   # 长期移动平均线周期
        ('printlog', True),    # 是否打印交易日志
    )
    
    def __init__(self):
        # 计算移动平均线
        self.short_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.short_period
        )
        self.long_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.long_period
        )
        
        # 计算交叉信号
        self.crossover = bt.indicators.CrossOver(self.short_ma, self.long_ma)
        
        # 记录订单状态
        self.order = None
        
    def log(self, txt, dt=None):
        """日志记录函数"""
        if self.params.printlog:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()}: {txt}')
    
    def notify_order(self, order):
        """订单状态通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return
        
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行: 价格 {order.executed.price:.2f}, '
                        f'数量 {order.executed.size}, '
                        f'手续费 {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'卖出执行: 价格 {order.executed.price:.2f}, '
                        f'数量 {order.executed.size}, '
                        f'手续费 {order.executed.comm:.2f}')
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单取消/保证金不足/拒绝')
        
        self.order = None
    
    def notify_trade(self, trade):
        """交易通知"""
        if not trade.isclosed:
            return
        
        self.log(f'交易盈亏: 毛利润 {trade.pnl:.2f}, 净利润 {trade.pnlcomm:.2f}')
    
    def next(self):
        """策略主逻辑"""
        # 如果有未完成的订单，等待
        if self.order:
            return
        
        # 检查是否持仓
        if not self.position:
            # 没有持仓，检查买入信号
            if self.crossover[0] > 0:  # 短期均线上穿长期均线
                self.log(f'买入信号: 价格 {self.data.close[0]:.2f}')
                # 买入
                self.order = self.buy()
        
        else:
            # 有持仓，检查卖出信号
            if self.crossover[0] < 0:  # 短期均线下穿长期均线
                self.log(f'卖出信号: 价格 {self.data.close[0]:.2f}')
                # 卖出
                self.order = self.sell()


def create_sample_data():
    """创建示例数据"""
    # 生成示例价格数据
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    
    # 模拟股价走势
    import numpy as np
    np.random.seed(42)
    
    # 基础价格趋势
    base_price = 100
    trend = np.linspace(0, 20, len(dates))  # 上升趋势
    noise = np.random.normal(0, 2, len(dates))  # 随机噪声
    
    prices = base_price + trend + noise
    
    # 生成OHLC数据
    data = []
    for i, (date, price) in enumerate(zip(dates, prices)):
        high = price + abs(np.random.normal(0, 1))
        low = price - abs(np.random.normal(0, 1))
        open_price = prices[i-1] if i > 0 else price
        close_price = price
        volume = np.random.randint(1000, 10000)
        
        data.append({
            'Date': date,
            'Open': open_price,
            'High': max(open_price, high, close_price),
            'Low': min(open_price, low, close_price),
            'Close': close_price,
            'Volume': volume
        })
    
    df = pd.DataFrame(data)
    df.set_index('Date', inplace=True)
    return df


def run_backtest():
    """运行回测"""
    print("开始运行Backtrader演示程序...")
    
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(SimpleMovingAverageStrategy)
    
    # 创建示例数据
    df = create_sample_data()
    # 将数据转换为Backtrader格式
    data = bt.feeds.PandasData(dataname=df)
    
    # 添加数据到Cerebro
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
    try:
        print('\n正在生成图表...')
        cerebro.plot(style='candlestick', barup='green', bardown='red')
        print('图表已生成！')
    except Exception as e:
        print(f'图表生成失败: {e}')
        print('可能需要安装matplotlib: pip install matplotlib')


if __name__ == '__main__':
    run_backtest()