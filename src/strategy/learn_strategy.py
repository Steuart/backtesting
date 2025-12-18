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
        self.log(f'data1 close: {self.data_map['data1'].close[0]}, data2 close: {self.data_map['data2'].close[0]}')

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')