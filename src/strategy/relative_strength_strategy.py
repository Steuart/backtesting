import backtrader as bt
import numpy as np


class RelativeStrengthStrategy(bt.Strategy):
    params = (
        ('rebalance_period', 10),
        ('num_top', 5),
        ('lookback_days', 60),
        ('vol_threshold', 2.0),
        ('corr_threshold', 0.8),
        ('printlog', True),
    )

    def __init__(self):
        self.stocks = []
        self.top_stocks = []
        self.ordering_num = 0
        self.rebalance_dates = self.get_rebalance_dates()
        self.momentums = None

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
        if total_posizion == 0:
            self.firstbuy()
        else:
            self.rebalance()

    def firstbuy(self):
        self.top_stocks = self.select_candidates()
        budget = self.broker.get_cash()/len(self.top_stocks)*0.9
        self.log(f"首次购买，总预算：{self.broker.get_cash()}, 平均预算: {budget}")
        for stock in self.top_stocks:
            size = int(budget / stock.close[0])
            if size > 0:
                self.stocks.append(stock)
                self.buy(data=stock, size=size)
                self.ordering_num +=1

    def rebalance(self):
        current_date = self.datetime.date()
        if current_date in self.rebalance_dates:
            self.top_stocks = self.select_candidates()
            tmp_stocks = self.stocks
            for stock in self.stocks:
                if self.getposition(stock).size > 0 and stock not in self.top_stocks:
                    self.log(f'卖出信号: {stock._name}')
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
    
    def select_candidates(self):
        eligible = []
        returns_map = {}
        for d in self.datas:
            if len(d) < self.p.lookback_days:
                continue
            
            rets = list(d.pct_chg.get(ago=0, size=self.p.lookback_days))
            if len(rets) < self.p.lookback_days:
                continue
                
            vol = float(np.std(rets))
            if vol <= self.p.vol_threshold:
                # Calculate total return using close prices
                start_close = d.close[-self.p.lookback_days + 1]
                end_close = d.close[0]
                total_return = (end_close/start_close - 1.0) * 100.0 if start_close != 0 else 0.0
                
                eligible.append((d, vol, total_return))
                returns_map[d._name] = np.array(rets, dtype=float)
        if not eligible:
            return []
        names = [d._name for d, _, _ in eligible]
        matrix = np.vstack([returns_map[n] for n in names])
        corr = np.corrcoef(matrix)
        n = len(names)
        adj = {names[i]: set() for i in range(n)}
        for i in range(n):
            for j in range(i+1, n):
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
                self.log(f'购买: {order.data._name}, 价格 {order.executed.price:.2f}, '
                         f'数量 {order.executed.size}, 手续费 {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'卖出: {order.data._name}, 价格 {order.executed.price:.2f}, '
                         f'数量 {order.executed.size}, 手续费 {order.executed.comm:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单问题: {order.p.data._name}, isBuy:{order.isbuy()}')
        self.ordering_num -= 1

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'交易利润: 毛利润 {trade.pnl:.2f}, 净利润 {trade.pnlcomm:.2f}')
