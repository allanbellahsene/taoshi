from datetime import datetime, timedelta
import pandas as pd
import pytz
from typing import List, Tuple, Optional

class MarketCalendar:
    """Handles US market calendar including holidays"""
    
    @staticmethod
    def get_us_market_holidays(year: int) -> List[datetime]:
        """
        Get US market holidays for a given year.
        
        Parameters:
        year (int): The year to get holidays for
        
        Returns:
        List[datetime]: List of holiday dates
        """
        holidays = []
        
        # New Year's Day
        new_years = pd.Timestamp(f"{year}-01-01")
        if new_years.dayofweek in [5, 6]:  # Weekend
            new_years = pd.Timestamp(f"{year}-01-01") + pd.offsets.BusinessDay(1)
        holidays.append(new_years)
        
        # Martin Luther King Jr. Day (3rd Monday in January)
        mlk_day = pd.Timestamp(f"{year}-01-01") + pd.offsets.Week(weekday=0)  # First Monday
        mlk_day = mlk_day + pd.offsets.Week(2)  # Third Monday
        holidays.append(mlk_day)
        
        # Presidents Day (3rd Monday in February)
        presidents_day = pd.Timestamp(f"{year}-02-01") + pd.offsets.Week(weekday=0)  # First Monday
        presidents_day = presidents_day + pd.offsets.Week(2)  # Third Monday
        holidays.append(presidents_day)
        
        # Good Friday (requires easter calculation)
        easter = pd.Timestamp(f"{year}-04-15").normalize() - pd.Timedelta(days=2)  # Approximate
        holidays.append(easter)
        
        # Memorial Day (Last Monday in May)
        memorial_day = pd.Timestamp(f"{year}-05-31") + pd.offsets.Week(weekday=0) # Last Monday
        holidays.append(memorial_day)
        
        # Juneteenth National Independence Day
        juneteenth = pd.Timestamp(f"{year}-06-19")
        if juneteenth.dayofweek in [5, 6]:  # Weekend
            juneteenth = pd.Timestamp(f"{year}-06-19") + pd.offsets.BusinessDay(1)
        holidays.append(juneteenth)
        
        # Independence Day
        independence_day = pd.Timestamp(f"{year}-07-04")
        if independence_day.dayofweek in [5, 6]:  # Weekend
            independence_day = pd.Timestamp(f"{year}-07-04") + pd.offsets.BusinessDay(1)
        holidays.append(independence_day)
        
        # Labor Day (1st Monday in September)
        labor_day = pd.Timestamp(f"{year}-09-01") + pd.offsets.Week(weekday=0)  # First Monday
        holidays.append(labor_day)
        
        # Thanksgiving (4th Thursday in November)
        thanksgiving = pd.Timestamp(f"{year}-11-01") + pd.offsets.Week(weekday=3)  # First Thursday
        thanksgiving = thanksgiving + pd.offsets.Week(3)  # Fourth Thursday
        holidays.append(thanksgiving)
        
        # Christmas
        christmas = pd.Timestamp(f"{year}-12-25")
        if christmas.dayofweek in [5, 6]:  # Weekend
            christmas = pd.Timestamp(f"{year}-12-25") + pd.offsets.BusinessDay(1)
        holidays.append(christmas)
        
        return sorted(holidays)

def get_previous_trading_day(dt: datetime, market_calendar: MarketCalendar) -> datetime:
    """
    Get the previous trading day, accounting for weekends and holidays.
    
    Parameters:
    dt (datetime): The reference date
    market_calendar (MarketCalendar): Market calendar instance
    
    Returns:
    datetime: Previous trading day
    """
    prev_day = dt - timedelta(days=1)
    holidays = market_calendar.get_us_market_holidays(prev_day.year)
    
    while (prev_day.weekday() >= 5 or  # Weekend
           prev_day.date() in [h.date() for h in holidays]):  # Holiday
        prev_day = prev_day - timedelta(days=1)
        if prev_day.year != dt.year:
            holidays = market_calendar.get_us_market_holidays(prev_day.year)
    
    return prev_day

def validate_last_row_date(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validates if the last row of the DataFrame contains the most recent market close data.
    
    Parameters:
    df (pd.DataFrame): DataFrame with a 'caldt' column containing datetime strings
    
    Returns:
    tuple: (bool, str) - (is_valid, explanation message)
    """
    market_calendar = MarketCalendar()
    
    # Convert the last row's date to datetime
    last_date = pd.to_datetime(df.iloc[-1]['caldt'])
    
    # Convert to EST/EDT (US Market timezone)
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    
    # Handle timezone conversion
    if last_date.tzinfo is None:
        last_date = last_date.tz_localize(pytz.UTC).tz_convert(eastern)
    else:
        last_date = last_date.tz_convert(eastern)
    
    # Get expected latest data date
    market_close = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Get holidays for current year
    holidays = market_calendar.get_us_market_holidays(current_time.year)
    today_is_holiday = current_time.date() in [h.date() for h in holidays]
    
    if current_time.time() > market_close.time():
        # After market close
        if current_time.weekday() < 5 and not today_is_holiday:  # Business day
            expected_date = current_time.date()
        else:
            # Weekend or holiday - get last trading day
            expected_date = get_previous_trading_day(current_time, market_calendar).date()
    else:
        # Before market close - should have previous trading day's data
        expected_date = get_previous_trading_day(current_time, market_calendar).date()
    
    # Compare dates
    if last_date.date() < expected_date:
        days_behind = len(pd.bdate_range(last_date.date(), expected_date)) - 1
        return False, f"Data is stale by {days_behind} trading days. Latest data: {last_date.date()}, Expected: {expected_date}"
    elif last_date.date() > expected_date:
        return False, f"Last date ({last_date.date()}) is in the future. Expected: {expected_date}"
    
    return True, f"Data is up-to-date. Latest data: {last_date.date()}"

def check_data_freshness(df: pd.DataFrame) -> bool:
    """
    Wrapper function to check if the DataFrame's last row is up-to-date
    and print relevant information
    
    Parameters:
    df (pd.DataFrame): DataFrame with market data
    
    Returns:
    bool: True if data is up-to-date, False otherwise
    """
    try:
        # Print current market status
        eastern = pytz.timezone('US/Eastern')
        current_time = datetime.now(eastern)
        market_calendar = MarketCalendar()
        holidays = market_calendar.get_us_market_holidays(current_time.year)
        
        print("\n=== Market Calendar Status ===")
        print(f"Current time (NY): {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        if current_time.date() in [h.date() for h in holidays]:
            print("📅 Today is a market holiday")
        elif current_time.weekday() >= 5:
            print("📅 Market is closed (Weekend)")
        else:
            market_open = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
            if market_open <= current_time <= market_close:
                print("🔔 Market is open")
            else:
                print("🔔 Market is closed (Outside trading hours)")
        
        # Validate data freshness
        is_valid, message = validate_last_row_date(df)
        
        print("\n=== Data Freshness Status ===")
        if not is_valid:
            print(f"❌ Validation Failed: {message}")
            print(f"DataFrame contains data from {pd.to_datetime(df['caldt'].iloc[0]).date()} to {pd.to_datetime(df['caldt'].iloc[-1]).date()}")
            return False
            
        print(f"✅ Validation Passed: {message}")
        return True
        
    except Exception as e:
        print(f"Error validating data: {str(e)}")
        return False

# Example usage:
# df = your_dataframe
# is_fresh = check_data_freshness(df)

def validate_close_prices(df):
    """
    Validates the quality of close price data, checking for nulls,
    zeros, negative values, and potential outliers.
    
    Parameters:
    df (pd.DataFrame): DataFrame with 'close' column
    
    Returns:
    tuple: (bool, list) - (is_valid, list of validation messages)
    """
    messages = []
    is_valid = True
    
    # Check for null values
    null_count = df['close'].isnull().sum()
    if null_count > 0:
        is_valid = False
        null_dates = df[df['close'].isnull()]['caldt'].tolist()
        messages.append(f"Found {null_count} null values in close prices on dates: {null_dates}")
    
    # Check for zero or negative values
    zero_or_neg = df[df['close'] <= 0]
    if not zero_or_neg.empty:
        is_valid = False
        zero_dates = zero_or_neg['caldt'].tolist()
        messages.append(f"Found {len(zero_or_neg)} zero or negative close prices on dates: {zero_dates}")
    
    # Check for potential outliers using rolling statistics
    # Calculate rolling median and standard deviation
    rolling_median = df['close'].rolling(window=5, min_periods=1).median()
    rolling_std = df['close'].rolling(window=5, min_periods=1).std()
    
    # Define outliers as values more than 3 standard deviations from the rolling median
    outliers = df[abs(df['close'] - rolling_median) > 3 * rolling_std]
    if not outliers.empty:
        messages.append(f"Warning: Found {len(outliers)} potential outliers in close prices:")
        for _, row in outliers.iterrows():
            messages.append(f"Date: {row['caldt']}, Close: {row['close']}, Rolling Median: {rolling_median[row.name]:.2f}")
    
    # Check for unexpected gaps between values
    pct_changes = df['close'].pct_change().abs()
    large_changes = df[pct_changes > 0.1]  # Flag changes greater than 10%
    if not large_changes.empty:
        messages.append(f"Warning: Found {len(large_changes)} large price changes (>10%):")
        for _, row in large_changes.iterrows():
            messages.append(f"Date: {row['caldt']}, Change: {pct_changes[row.name]*100:.1f}%")
    
    return is_valid, messages