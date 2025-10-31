import backtrader as bt


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
                # 计算买入数量，使用98%的现金以避免保证金不足
                size = int(self.broker.getcash() * 0.98 / self.data.close[0])
                if size > 0:
                    self.order = self.buy(size=size)

        else:
            # 有持仓，检查卖出信号
            if self.crossover[0] < 0:  # 短期均线下穿长期均线
                self.log(f'卖出信号: 价格 {self.data.close[0]:.2f}')
                # 卖出所有持仓
                self.order = self.sell()
