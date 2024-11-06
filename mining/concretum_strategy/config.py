from   datetime import time

DIR_PATH = '/root/taoshi/mining/concretum_strategy/'
HIST_DATA_PATH = DIR_PATH + 'historical_data/'
LIVE_DATA_PATH = DIR_PATH + 'live_data/'
SIGNALS_PATH = DIR_PATH + 'signals_data/'
SIGNALS_PLOT_PATH = DIR_PATH + 'signals_plot/'


market_open         = time(9, 31)  # Market opens at 9:30 AM
market_close        = time(16, 0)  # Market closes just before 4:00 PM
symbol              = 'SPY'
rolling_window      =  14
band_mult           =  1.3
volatility_window   =  14
DAILY_VOL_TARGET    =  0.02
MAX_LEVERAGE        =  4
trade_frequency     =  30 #trade frequency, in minutes - can only trade at such minute of any trading hour. For example if equal to 30,
#can only trade at 10:00, 10:30, 11:00, 11:30, etc. if equal to 10, can only trade at 9:40, 9:50,...,15:50.

