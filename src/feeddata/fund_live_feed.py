
import backtrader as bt
import pandas as pd
import time
from datetime import datetime, timedelta
from database import fund_market_dao

class FundLiveData(bt.feed.DataBase):
    lines = ('pct_chg',)
    params = (
        ('symbol', None),
        ('time_frame', '1d'),
        ('check_interval', 60),  # Check DB every 60 seconds
        ('live_start_date', None), # Date to start "live" polling (or history end)
        ('lookback', 100), # Load initial history
    )

    def __init__(self):
        super().__init__()
        self.data_queue = []
        self.last_dt = None

    def start(self):
        super().start()
        # Load history
        end_date = self.p.live_start_date or datetime.now().strftime('%Y-%m-%d')
        
        # list_by_limit retrieves records <= end_date. 
        # We assume it orders by time ASC or we sort it.
        # Check fund_market_dao.list_by_limit implementation: it orders ASC and limits.
        # Wait, if it orders ASC and limits, it gets the OLDEST records. 
        # We usually want the NEWEST records up to end_date.
        # Let's assume we use list_fund_market with a calculated start date instead for safety.
        
        start_date_calc = (pd.to_datetime(end_date) - timedelta(days=self.p.lookback * 2)).strftime('%Y-%m-%d')
        
        hist_data = fund_market_dao.list_fund_market(
            symbol=self.p.symbol,
            start_date=start_date_calc,
            end_date=end_date,
            time_frame=self.p.time_frame
        )
        
        if not hist_data.empty:
            # Take last N rows
            hist_data = hist_data.sort_values('time').tail(self.p.lookback)
            for _, row in hist_data.iterrows():
                self.data_queue.append(row)
            
    def _load(self):
        # In live mode, we want to keep polling until we get data
        # We loop here to ensure we don't return False (which would stop Cerebro if not handled correctly)
        while not self.data_queue:
            self._fetch_new_data()
            if self.data_queue:
                break
            
            # If no data, wait
            time.sleep(self.p.check_interval)
            
        if self.data_queue:
            row = self.data_queue.pop(0)
            self._fill_lines(row)
            self.last_dt = row['time']
            return True
            
        return False # Should not be reached due to while loop

    def _fetch_new_data(self):
        # Determine start date for query
        if self.last_dt is None:
            # If no history was loaded, start from live_start_date or today
            if self.p.live_start_date:
                start_date = self.p.live_start_date
            else:
                start_date = datetime.now().strftime('%Y-%m-%d')
            
            # Use a default time if needed, but list_fund_market handles string dates
            last_ts = pd.to_datetime(start_date)
        else:
            # last_dt is a Timestamp or datetime or string
            last_ts = pd.to_datetime(self.last_dt)
            
            # Add a small buffer to avoid duplicate if strictly greater is needed, 
            # but SQL often uses >=. We want >.
            # Let's use start_date = last_ts + 1 second (if intraday) or 1 day (if daily)
            if self.p.time_frame == '1d':
                start_date = (last_ts + timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                start_date = (last_ts + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
            
        end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        try:
            new_data = fund_market_dao.list_fund_market(
                symbol=self.p.symbol,
                start_date=start_date,
                end_date=end_date,
                time_frame=self.p.time_frame
            )
        except Exception as e:
            print(f"Error fetching data: {e}")
            return
        
        if not new_data.empty:
            new_data = new_data.sort_values('time')
            for _, row in new_data.iterrows():
                row_dt = pd.to_datetime(row['time'])
                # Double check time > last_dt to be sure, if last_dt exists
                if self.last_dt is None or row_dt > last_ts:
                    self.data_queue.append(row)
        
    def _fill_lines(self, row):
        dt = row['time']
        if isinstance(dt, str):
            dt = pd.to_datetime(dt)
        self.lines.datetime[0] = bt.date2num(dt)
        self.lines.open[0] = row['open']
        self.lines.high[0] = row['high']
        self.lines.low[0] = row['low']
        self.lines.close[0] = row['close']
        self.lines.volume[0] = row['volume']
        self.lines.openinterest[0] = row.get('interest', 0)
        self.lines.pct_chg[0] = row.get('pct_chg', 0.0)

    def haslivedata(self):
        return True

    def islive(self):
        return True
