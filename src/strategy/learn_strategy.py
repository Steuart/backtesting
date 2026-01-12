from math import log
import backtrader as bt

class LearnStrategy(bt.Strategy):
    params = (
        ('fast', 50),
        ('slow', 200),
    )
    def __init__(self):
        self.log(f"type:{type(self.data.close[0])}")
        self.data_map = {}
        for _, data in enumerate(self.datas):
            self.data_map[data._name] = data

    
    def next(self):
        cash = self.broker.getcash()
        self.log(f'cash: {cash}')
        data1 = self.data_map['data1']
        data2 = self.data_map['data2']
        self.buy(data=data2, size=100)
        self.sell(data=data2, size=100)

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}')
    
    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f"TRADE CLOSED, Profit: {trade.pnl:.2f}")
        elif trade.isopen:
            self.log(f"TRADE OPENED, Size: {trade.size}")
        else:
            # 持仓中盈亏更新
            self.log(f"TRADE UPDATED, PnL: {trade.pnl:.2f}")

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')