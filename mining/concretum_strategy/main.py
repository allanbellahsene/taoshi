from mining.concretum_strategy.config import (
    HIST_DATA_PATH,
    LIVE_DATA_PATH,
    SIGNALS_PATH,
    SIGNALS_PLOT_PATH
)

import os

symbols = ['SPY', 'QQQ']

for symbol in symbols:
    live_path = LIVE_DATA_PATH + f'{symbol}/'
    hist_path = HIST_DATA_PATH + f'{symbol}/'
    signal_path = SIGNALS_PATH + f'{symbol}/'
    plot_path = SIGNALS_PLOT_PATH + f'{symbol}/'

    if not os.path.exists(live_path):
        os.makedirs(live_path)
    
    if not os.path.exists(hist_path):
        os.makedirs(hist_path)


    if not os.path.exists(signal_path):
        os.makedirs(signal_path)
    
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
