import requests
import pandas as pd
from   datetime import datetime, timedelta,time
import numpy as np
#import statsmodels.api as sm
from mining.utils import find_nearest_time, create_normalized_matrix
from research.historical_data import fetch_alpaca_data


#### CHANGE OPEN TIME TO 09:30 !!!!!!!!!!!!!!!

API_KEY     = "AK1TXEM0NSG8T5O9DC8U"
API_SECRET = "hOFq2cQxKLNScoZ766Uo4PObpJk7Vgfd2xnH8DrV"


#1. Import 1 min SPY data from last 100 days

market_open         = time(9, 30)  # Market opens at 9:30 AM
market_close        = time(15, 59)  # Market closes just before 4:00 PM
symbol              = 'SPY'

today = datetime.today()
start_date = today - timedelta(days=100)
end_date = today #- timedelta(days=1)

start_date          =  start_date.strftime('%Y-%m-%d')
end_date            =  end_date.strftime('%Y-%m-%d')
rolling_window      =  90
band_mult           =  1


intra_data = fetch_alpaca_data(symbol, '1Min', start_date, end_date, market_open, market_close)
daily_data = fetch_alpaca_data(symbol, '1D', start_date, end_date, market_open, market_close)
intra_data.to_csv(f'historical_data/{symbol}-{start_date}-{end_date}-1min.csv')
daily_data.to_csv(f'historical_data/{symbol}-{start_date}-{end_date}-1d.csv')

last_intra_close = intra_data.close.iloc[-1]
last_daily_close = daily_data.close.iloc[-1]


#2. Fetch Open from today

open_time = '11:50'
    
data = pd.read_csv('historical_data/live_data.csv')
open_df = find_nearest_time(data,
    target_date = '2024-10-22',
    target_time = open_time,
    window_minutes =  10,
    timezone = 'America/New_York')


today_open = open_df.Open
print(today_open)

#3. Calculate indicators

#Calculate avg move from open over the last X days for each minute of the day

print(intra_data)
print(daily_data)

move_from_open = create_normalized_matrix(intra_data, daily_data, rolling_window)
avg_move_from_open = pd.DataFrame(move_from_open.mean(axis=1), columns=['avg_move_from_open'])
print(avg_move_from_open)

#4. Every day, this script updates by updating the last available day to compute the rolling window
#avg move from open

#5. Compare current move from open with historical avg move from open at that time

#now_time = time.now()

now_time = '14:30'

sigma = avg_move_from_open.loc[avg_move_from_open.index == now_time]

# get latest close bar and today's open bar

yesterday_close = last_daily_close

upper_bound = max(today_open, yesterday_close) * (1 + band_mult * sigma)
lower_bound = min(today_open, yesterday_close) * (1 - band_mult * sigma)

#Compute VWAP

#VWAP is re-initialized every day

#it is equal to sum(HLC * Volume) / sum(Volume)
#where HLC = (High + Low + Close) / 3
#so we need to record all the minutely H, L, C, V bars every day

today_date = '2024-10-24'

today_bars = pd.read_csv(f'live_data/{symbol}-{today_date}-1min.csv')

hlc = today_bars[['High', 'Low', 'Close']] / 3
volume = today_bars['Volume']
vwap = (hlc * volume).sum() / volume.sum()

# get current close

#6. Fetch latest bar

APCA_API_DATA_URL = 'https://data.alpaca.markets'

import alpaca_trade_api as alpacaapi
from alpaca_trade_api.rest import TimeFrame
api = alpacaapi.REST(API_KEY, API_SECRET, APCA_API_DATA_URL)
latest_bar = api.get_latest_bar("SPY")
latest_close = latest_bar.c
#today_open = 
bar_time = latest_bar.t

current_move_from_open = abs((latest_close / today_open) - 1)

#7. Compute signal

new_signal = 0

if current_move_from_open >= np.maximum(upper_bound, vwap):
    new_signal = 1
elif current_move_from_open <= np.minimum(lower_bound, vwap):
    new_signal = -1

#8. Sends new signal with all information. 

#9. Other script checks current position and pending signals. Takes position (or not) accordingly







