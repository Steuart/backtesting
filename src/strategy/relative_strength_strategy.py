import backtrader as bt
import numpy as np


class RelativeStrengthStrategy(bt.Strategy):
    params = (
        ('rebalance_period', 10),
        ('num_top', 5),
        ('correlation_period', 60),
        ('vol_threshold', 2.0),
        ('corr_threshold', 0.8),
        ('printlog', True),
        ('stop_loss_pct', 2),
    )

    def __init__(self):
        self.holding_stocks = []
        self.for_buy = []
        self.for_sell = []
        self.rebalance_dates = self.data.datetime[::self.p.rebalance_period]
        self.data_map = {}
        self.atr = {}
        for data in self.datas:
            self.atr[data] = bt.indicators.ATR(data, period=self.p.rebalance_period)
            self.data_map[data._name] = data

    def next(self):
        # 执行订单
        if len(self.for_sell) > 0:
            self.sell_stocks()
        elif len(self.for_buy) > 0:
            self.buy_stocks()
        # 止损
        self.stop_loss()
        # 调仓
        current_date = self.data.datetime[0]
        if current_date in self.rebalance_dates or len(self.holding_stocks) == 0:
            top_stocks = self.top_stocks()
            self.for_buy = [x for x in top_stocks if x not in self.holding_stocks]
            self.for_sell = [x for x in self.holding_stocks if x not in top_stocks]

    def buy_stocks(self):
        if len(self.for_buy) == 0:
            return
        budget = self.broker.get_cash()/len(self.for_buy)*self.p.corr_threshold
        for stock_name in self.for_buy:
            stock = self.data_map[stock_name]
            size = int(budget / stock.close[0])
            if size > 0:
                self.buy(data=stock, size=size)

    def sell_stocks(self):
        if len(self.for_sell) == 0:
            return
        for stock_name in self.for_sell:
            stock = self.data_map[stock_name]
            self.close(data=stock)
    
    def stop_loss(self):
        if len(self.holding_stocks) == 0:
            return
        for stock_name in self.holding_stocks:
            stock = self.data_map[stock_name]
            atr = self.atr[stock][0]
            if stock.close[-1] - stock.close[0] >= self.p.stop_loss_pct * atr:
                self.close(data=stock)

    def correlation_groups(self, names, returns_map):
        if not names:
            return []
        matrix = np.vstack([returns_map[n] for n in names])
        corr = np.corrcoef(matrix)
        n = len(names)
        adj = {names[i]: set() for i in range(n)}
        for i in range(n):
            for j in range(i + 1, n):
                if corr[i, j] > self.p.corr_threshold:
                    adj[names[i]].add(names[j])
                    adj[names[j]].add(names[i])
        visited = set()
        groups = []
        for name in names:
            if name in visited:
                continue
            stack = [name]
            comp = set()
            while stack:
                s = stack.pop()
                if s in visited:
                    continue
                visited.add(s)
                comp.add(s)
                for nei in adj[s]:
                    if nei not in visited:
                        stack.append(nei)
            groups.append(comp)
        return groups

    def filter_candidates(self):
        eligible = []
        returns_map = {}
        for d in self.datas:
            if len(d) < self.p.correlation_period:
                continue

            rets = list(d.pct_chg.get(ago=0, size=self.p.correlation_period))
            if len(rets) < self.p.correlation_period:
                continue

            vol = float(self.atr[d][0])
            if vol <= self.p.vol_threshold:
                start_close = d.close[-self.p.correlation_period + 1]
                end_close = d.close[0]
                total_return = (end_close/start_close - 1.0) * 100.0 if start_close != 0 else 0.0
                
                eligible.append((d, vol, total_return))
                returns_map[d._name] = np.array(rets, dtype=float)
        return eligible, returns_map

    def top_stocks(self):
        eligible, returns_map = self.filter_candidates()
        if not eligible:
            return []
        names = [d._name for d, _, _ in eligible]
        groups = self.correlation_groups(names, returns_map)
        by_name = {d._name: (d, vol, ret) for d, vol, ret in eligible}
        winners = []
        for comp in groups:
            cand = sorted([by_name[n] for n in comp], key=lambda x: x[1])
            if cand:
                winners.append(cand[0])
        winners_sorted = sorted(winners, key=lambda x: x[2], reverse=True)
        top = winners_sorted[:self.p.num_top]
        return [d for d, _, _ in top]

    def log(self, txt, dt=None):
        if self.p.printlog:
            dt = dt or self.datetime.date()
            print(f'{dt.isoformat()}: {txt}')

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.for_buy.remove(order.data._name)
                self.holding_stocks.append(order.data._name)
                self.log(f'buy: {order.data._name}, price {order.executed.price:.2f}, '
                         f'size {order.executed.size}, comm {order.executed.comm:.2f}')
            elif order.issell():
                self.for_sell.remove(order.data._name)
                self.holding_stocks.remove(order.data._name)
                self.log(f'sell: {order.data._name}, price {order.executed.price:.2f}, '
                         f'size {order.executed.size}, comm {order.executed.comm:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            if order.isbuy():
                self.for_buy.remove(order.data._name)
            elif order.issell():
                self.for_sell.remove(order.data._name)
            self.log(f'order problem: {order.p.data._name}, isBuy:{order.isbuy()}')

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'trade profit: gross {trade.pnl:.2f}, net {trade.pnlcomm:.2f}')
