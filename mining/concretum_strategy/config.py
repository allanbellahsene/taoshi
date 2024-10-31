from   datetime import time

DIR_PATH = '/root/taoshi/mining/concretum_strategy/'
HIST_DATA_PATH = DIR_PATH + 'historical_data/'
LIVE_DATA_PATH = DIR_PATH + 'live_data/'
SIGNALS_PATH = DIR_PATH + 'signals_data/'
SIGNALS_PLOT_PATH = DIR_PATH + 'signals_plot/'


API_KEY     = "AK1TXEM0NSG8T5O9DC8U"
API_SECRET = "hOFq2cQxKLNScoZ766Uo4PObpJk7Vgfd2xnH8DrV"


market_open         = time(9, 30)  # Market opens at 9:30 AM
market_close        = time(15, 59)  # Market closes just before 4:00 PM
symbol              = 'SPY'
rolling_window      =  90
band_mult           =  1

