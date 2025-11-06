import backtrader as bt
# 1. 自定义资金曲线观察者（只记录资产净值）
class EquityObserver(bt.Observer):
    lines = ('equity',)  # 定义要绘制的线
    plotinfo = dict(plot=True, subplot=False)  # 在主图显示

    def next(self):
        self.lines.equity[0] = self._owner.broker.getvalue()  # 当前资金