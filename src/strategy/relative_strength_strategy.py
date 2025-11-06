import json
import backtrader as bt
import numpy as np


class RelativeStrengthStrategy(bt.Strategy):
    params = (
        ('rebalance_period', 10),
        ('num_top', 10),
        ('printlog', True),
    )

    def __init__(self):
        self.stocks = []
        self.top_stocks = []
        self.ordering_num = 0
        self.rebalance_dates = self.get_rebalance_dates()
        self.momentums = [bt.indicators.ROC(d.close, period=self.p.rebalance_period) for d in self.datas]

    def get_rebalance_dates(self):
        dates = set()
        for data in self.datas:
            for line in data.lines.datetime.array:
                dates.add(bt.num2date(line))
        sorted_dates = sorted(list(dates))
        rebalance_dates = sorted_dates[::self.p.rebalance_period]
        result = [d.date() for d in rebalance_dates]
        return result

    def next(self):
        if self.ordering_num > 0:
            return
        total_posizion = 0
        for stock in self.stocks:
            total_posizion += self.getposition(stock).size
        self.log(f"total_posizion:{total_posizion}")
        if total_posizion == 0:
            self.firstbuy()
        else:
            self.rebalance()

    def firstbuy(self):
        ranks = sorted(
                self.datas,
                key=lambda d: self.momentums[self.datas.index(d)][0],
                reverse=True
            )
        self.top_stocks = ranks[:self.p.num_top]
        budget = self.broker.get_cash()/len(self.top_stocks)*0.8
        self.log(f"firstbuy budget:{budget}")
        for stock in self.top_stocks:
            size = int(budget / stock.close[0])
            if size > 0:
                self.stocks.append(stock)
                self.buy(data=stock, size=size)
                self.ordering_num +=1

    def rebalance(self):
        current_date = self.datetime.date()
        if current_date in self.rebalance_dates:
            ranks = sorted(
                    self.datas,
                    key=lambda d: self.momentums[self.datas.index(d)][0],
                    reverse=True
                )
            self.top_stocks = ranks[:self.p.num_top]
            tmp_stocks = self.stocks
            for stock in self.stocks:
                if self.getposition(stock).size > 0 and stock not in self.top_stocks:
                    self.log(f'Sell single: {stock._name}')
                    self.ordering_num +=1
                    self.close(data=stock)
                    tmp_stocks.remove(stock)
            self.stocks = tmp_stocks
        
        if (self.stocks == self.top_stocks or self.ordering_num > 0):
            return
        budget = self.broker.get_cash()/(self.p.num_top-len(self.stocks)) * 0.8
        for stock in self.top_stocks:
            if self.getposition(stock).size == 0:
                size = int(budget / stock.close[0])
                if size > 0:
                    self.buy(data=stock, size=size)
                    self.ordering_num +=1

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
            self.log(f'Order problem: {order.p.data._name}, isBuy:{order.isbuy()}')
        self.ordering_num -= 1

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'Trade profit: Gross {trade.pnl:.2f}, Net {trade.pnlcomm:.2f}')
