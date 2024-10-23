### This script generates intraday signals every minute based on the Concretum Intraday Momentum Strategy.
### It must run every minute when the market is open.

#First, the script checks what if today the market is open. (by taking into account weekends and holidays)
#If no, it stops
#If yes, it checks what time it is:
#If it is 9:30, it simply records the open bars of the session
#If it is later than 9:30 (and before 16:00), it proceeds with the trading strategy logic.

import requests
import pandas as pd
from   datetime import datetime, timedelta,time
import numpy as np
#import statsmodels.api as sm
from mining.utils import find_nearest_time, create_normalized_matrix
from research.historical_data import fetch_alpaca_data
from mining.concretum_strategy.config import market_open, market_close, symbol, rolling_window, band_mult


today = datetime.today()
start_date = today - timedelta(days=100)
end_date = today
start_date          =  start_date.strftime('%Y-%m-%d')
end_date            =  end_date.strftime('%Y-%m-%d')

intra_data = pd.read_csv(f'historical_data/{symbol}-{start_date}-{end_date}-1min.csv')
daily_data = pd.read_csv(f'historical_data/{symbol}-{start_date}-{end_date}-1d.csv')


last_intra_close = intra_data.close.iloc[-1]
last_daily_close = daily_data.close.iloc[-1]

# Check what day we are and what time it is 