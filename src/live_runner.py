
import sys
import os
import backtrader as bt
from datetime import datetime
import time

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feeddata.fund_live_feed import FundLiveData
from broker.sim_broker import SimBroker

class TestStrategy(bt.Strategy):
    def next(self):
        print(f'{self.data.datetime.datetime(0)} - Close: {self.data.close[0]}')
        
        # Simple Logic: Buy if close > open, Sell if close < open
        if not self.position:
            if self.data.close[0] > self.data.open[0]:
                self.buy(size=100)
        else:
            if self.data.close[0] < self.data.open[0]:
                self.sell(size=100)

    def notify_order(self, order):
        if order.status in [order.Completed]:
            print(f'Order Completed: {order.executed.price}')

def run_live():
    cerebro = bt.Cerebro()
    
    # Use SimBroker
    cerebro.broker = SimBroker()
    
    # Add Strategy
    cerebro.addstrategy(TestStrategy)
    
    # Create Live Data
    # Example symbol: '510300' (assuming it exists in DB)
    data = FundLiveData(
        symbol='510300',
        time_frame='1d',
        lookback=50,
        check_interval=10, # Check every 10s for demo
        live_start_date=None # Auto detect
    )
    
    cerebro.adddata(data)
    
    print("Starting Live Simulation...")
    # live=True makes cerebro wait for new data
    cerebro.run(live=True)

if __name__ == '__main__':
    try:
        run_live()
    except KeyboardInterrupt:
        print("Stopped by user")
