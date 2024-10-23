# This script must run every day before the market open to download historical 
# intraday and daily bars from the last 100 days.
# It must ensure that we are indeed getting all available data until the most recent available data.

from   datetime import datetime, timedelta
from research.historical_data import fetch_alpaca_data
from mining.concretum_strategy.config import market_open, market_close, symbol
from mining.data_checks import check_data_freshness, validate_close_prices
from mining.utils import ensure_folder_exists

def fetch_and_save_historical_bars():

    today = datetime.today()
    start_date = today - timedelta(days=100)
    end_date = today

    start_date          =  start_date.strftime('%Y-%m-%d')
    end_date            =  end_date.strftime('%Y-%m-%d')


    intra_data = fetch_alpaca_data(symbol, '1Min', start_date, end_date, market_open, market_close)
    daily_data = fetch_alpaca_data(symbol, '1D', start_date, end_date, market_open, market_close)

    print(intra_data)
    print(daily_data)

    print('Checking freshness of intraday data...')
    check_data_freshness(intra_data)
    print('Checking freshness of daily data...')
    check_data_freshness(daily_data)
    print('Checking for outliers or nulls in close prices of intraday data...')
    is_valid, messages = validate_close_prices(intra_data)
    print(f'Is the data valid? {is_valid}')
    if not is_valid:
        print(messages)
    print('Checking for outliers or nulls in close prices of daily data...')
    is_valid, mess = validate_close_prices(daily_data)
    print(f'Is the data valid? {is_valid}')
    if not is_valid:
        print(mess)

    ensure_folder_exists('historical_data')

    intra_data.to_csv(f'historical_data/{symbol}-{start_date}-{end_date}-1min.csv')
    daily_data.to_csv(f'historical_data/{symbol}-{start_date}-{end_date}-1d.csv')

if __name__ == "__main__":
    fetch_and_save_historical_bars()





