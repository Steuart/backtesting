import talib
import numpy as np
import pandas as pd

high = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
low = np.array([0.5, 1.0, 1.5, 2.0, 2.5])
close = np.array([1.2, 1.8, 2.4, 3.0, 3.6])
atr_14 = talib.ATR(high, low, close, timeperiod=3)
print(atr_14)
