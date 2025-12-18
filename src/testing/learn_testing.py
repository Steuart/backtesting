import backtrader as bt
from strategy.learn_strategy import LearnStrategy
import pandas as pd
import numpy as np

def main():
    data1 = pd.DataFrame(data = {
        'open': list(range(100)),
        'high': list(range(100)),
        'low': list(range(100)),
        'close': list(range(100)),
        'volume': list(range(100)),
    }, index = pd.date_range(start='2023-01-01', periods=100))
    data2 = pd.DataFrame(data = {
        'open': list(range(20)),
        'high': list(range(20)),
        'low': list(range(20)),
        'close': list(range(20)),
        'volume': list(range(20)),
    }, index = pd.date_range(start='2023-01-01', periods=20, freq='5D'))
    cerebro = bt.Cerebro()
    cerebro.adddata(data = bt.feeds.PandasData(dataname=data1), name='data1')
    cerebro.adddata(data = bt.feeds.PandasData(dataname=data2), name='data2')
    cerebro.addstrategy(LearnStrategy)
    cerebro.run()

if __name__ == '__main__':
    main()
