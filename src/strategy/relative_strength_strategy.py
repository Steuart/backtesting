import backtrader as bt
import numpy as np


class RelativeStrengthStrategy(bt.Strategy):
    params = (
        ('rebalance_period', 20),
        ('momentum_period', 60),
        ('num_top', 10),
        ('printlog', True),
    )

    def __init__(self):
        self.broker.set_coc(True)
        self.stocks = self.datas
        self.order = None
        self.rebalance_dates = self.get_rebalance_dates()
        self.momentums = [bt.indicators.ROC(d.close, period=self.p.momentum_period) for d in self.stocks]

    def get_rebalance_dates(self):
        dates = set()
        for data in self.stocks:
            for line in data.lines.datetime.array:
                dates.add(bt.num2date(line))
        sorted_dates = sorted(list(dates))
        rebalance_dates = sorted_dates[::self.p.rebalance_period]
        return [d.date() for d in rebalance_dates]

    def next(self):
        current_date = self.datetime.date()
        if current_date in self.rebalance_dates:
            self.rebalance()

    def rebalance(self):
        ranks = sorted(
            self.stocks,
            key=lambda d: self.momentums[self.stocks.index(d)][0],
            reverse=True
        )
        top_stocks = ranks[:self.p.num_top]
        sell_size = 0
        for stock in self.stocks:
            if self.getposition(stock).size > 0 and stock not in top_stocks:
                self.log(f'Sell single: {stock._name}')
                self.order = self.sell(data=stock)
                sell_size +=1
        if sell_size == 0:
            return
        budget = self.broker.get_cash()/sell_size * 0.98
        for stock in top_stocks:
            if self.getposition(stock).size == 0:
                size = int(budget / stock.close[0])
                if size > 0:
                    self.log(f'Buy single: {stock._name}')
                    self.order = self.buy(data=stock, size=size)

    def log(self, txt, dt=None):
        if self.p.printlog:
            dt = dt or self.datetime.date()
            print(f'{dt.isoformat()}: {txt}')

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'Buy: {order.data._name}, Price {order.executed.price:.2f}, '
                         f'Amount {order.executed.size}, fee {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'Sell: {order.data._name}, Price {order.executed.price:.2f}, '
                         f'Amount {order.executed.size}, fee {order.executed.comm:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'Order problem: {order.getstatusname()}')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'Trade profit: Gross {trade.pnl:.2f}, Net {trade.pnlcomm:.2f}')
