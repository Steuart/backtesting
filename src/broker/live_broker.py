import backtrader as bt

class LiveBroker(bt.brokers.BackBroker):
    def __init__(self, **kwargs):
        super(LiveBroker, self).__init__(**kwargs)
    
    def submit(self, order):
        # 将订单发送到实盘交易所
        print("summit order")
        order.addinfo(real_order_id=1)
