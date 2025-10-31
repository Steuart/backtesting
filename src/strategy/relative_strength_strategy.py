import backtrader as bt
import numpy as np


class RelativeStrengthStrategy(bt.Strategy):
    """
    相对强弱轮动策略
    """
    params = (
        ('rebalance_period', 20),  # 调仓周期（例如，每月）
        ('momentum_period', 60),   # 动量计算周期（例如，每季度）
        ('num_top', 0.1),          # 选择排名前10%的基金
        ('printlog', True),
    )

    def __init__(self):
        self.stocks = self.datas
        self.order = None
        self.rebalance_dates = self.get_rebalance_dates()
        
        # 为每只基金计算动量
        self.momentums = [bt.indicators.ROC(d.close, period=self.p.momentum_period) for d in self.stocks]

    def get_rebalance_dates(self):
        """获取所有数据中可用的调仓日期"""
        dates = set()
        for data in self.stocks:
            dates.update([bt.num2date(line.datetime[0]) for line in data.lines.datetime.array])
        
        sorted_dates = sorted(list(dates))
        
        # 每隔 rebalance_period 天选择一个调仓日
        rebalance_dates = sorted_dates[::self.p.rebalance_period]
        return [d.date() for d in rebalance_dates]

    def next(self):
        """策略主逻辑"""
        current_date = self.datetime.date()
        
        # 检查今天是否是调仓日
        if current_date in self.rebalance_dates:
            self.rebalance()

    def rebalance(self):
        # 1. 计算所有基金的动量并排名
        ranks = sorted(
            self.stocks,
            key=lambda d: self.momentums[self.stocks.index(d)][0],
            reverse=True
        )
        
        num_top = int(len(self.stocks) * self.p.num_top)
        top_stocks = ranks[:num_top]

        # 2. 卖出不在前10%的持仓
        for d in self.stocks:
            if self.getposition(d).size > 0 and d not in top_stocks:
                self.log(f'卖出信号: {d._name}')
                self.order = self.sell(data=d)

        # 3. 买入新的前10%的基金
        # 计算每只基金的目标价值
        target_value = self.broker.getvalue() * (1 / num_top) * 0.98  # 留出2%的现金

        for d in top_stocks:
            if self.getposition(d).size == 0:
                size = int(target_value / d.close[0])
                if size > 0:
                    self.log(f'买入信号: {d._name}')
                    self.order = self.buy(data=d, size=size)

    def log(self, txt, dt=None):
        """日志记录函数"""
        if self.p.printlog:
            dt = dt or self.datetime.date()
            print(f'{dt.isoformat()}: {txt}')

    def notify_order(self, order):
        """订单状态通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行: {order.data._name}, 价格 {order.executed.price:.2f}, '
                         f'数量 {order.executed.size}, 手续费 {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'卖出执行: {order.data._name}, 价格 {order.executed.price:.2f}, '
                         f'数量 {order.executed.size}, 手续费 {order.executed.comm:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单问题: {order.getstatusname()}')

        self.order = None

    def notify_trade(self, trade):
        """交易通知"""
        if not trade.isclosed:
            return
        self.log(f'交易盈亏: 毛利润 {trade.pnl:.2f}, 净利润 {trade.pnlcomm:.2f}')