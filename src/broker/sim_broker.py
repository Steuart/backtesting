
import backtrader as bt

class SimBroker(bt.brokers.BackBroker):
    """
    Simulated Broker that behaves like BackBroker but logs orders 
    and could be extended to persist state for "Paper Trading".
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def submit(self, order):
        # In a real PaperTrading scenario, we would save this order to DB
        # so we can recover it if the process restarts.
        # For now, we just print it.
        action = 'BUY' if order.isbuy() else 'SELL'
        print(f"[SimBroker] Order Submitted: {action} {order.data._name} Size: {order.size} Price: {order.price or 'Market'}")
        
        return super().submit(order)
        
    # We can override other methods to simulate slippage/commission more specifically if needed
    # but BackBroker params usually handle that.
