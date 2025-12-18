import backtrader as bt

class DummyInd(bt.Indicator):
    lines = ('dummy',)
    params = (
        ('value', 0.0),
    )
    def __init__(self):
        self.lines.dummy = self.p.value