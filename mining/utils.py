import pandas as pd
import pytz
import os
import glob
from datetime import datetime


def find_nearest_time(
    df: pd.DataFrame,
    target_date: str,
    target_time: str,
    window_minutes: int = 5,
    timezone: str = 'America/New_York'
) -> pd.DataFrame:
    """
    Find row(s) in DataFrame closest to target date and time, within a specified window.
    
    Args:
        df: DataFrame with 'Datetime' column in format like '2024-10-22 11:59:00-04:00'
        target_date: Date string in 'YYYY-MM-DD' format
        target_time: Time string in 'HH:MM' format (24-hour)
        window_minutes: Minutes to search before and after target time (default: 5)
        timezone: Timezone name (default: 'America/New_York')
    
    Returns:
        DataFrame with the matching row(s), sorted by time difference from target
    """
    try:
        # Ensure Datetime is in datetime format
        df = df.copy()
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        
        # Create target datetime in NY timezone
        ny_tz = pytz.timezone(timezone)
        target_dt = pd.to_datetime(f"{target_date} {target_time}").tz_localize(ny_tz)
        
        # Calculate window boundaries
        window_start = target_dt - pd.Timedelta(minutes=window_minutes)
        window_end = target_dt + pd.Timedelta(minutes=window_minutes)
        
        # Filter rows within the date and time window
        mask = (
            (df['Datetime'] >= window_start) &
            (df['Datetime'] <= window_end)
        )
        filtered_df = df[mask]
        
        if filtered_df.empty:
            return pd.DataFrame()
        
        # Calculate time difference from target for each row
        filtered_df['time_diff'] = abs(filtered_df['Datetime'] - target_dt)
        
        # Sort by time difference and remove the time_diff column
        result = filtered_df.sort_values('time_diff').drop('time_diff', axis=1)
        
        return result.iloc[0]
    
    except Exception as e:
        print(f"Error finding nearest time: {str(e)}")
        return pd.DataFrame()
    
def create_normalized_matrix(intraday_df: pd.DataFrame, 
                             daily_df: pd.DataFrame,
                             rolling_window: int) -> pd.DataFrame:
    """
    Creates a matrix with times as rows and dates as columns, with values normalized by daily open.
    
    Args:
        intraday_df: DataFrame with minute-level data (columns: volume, open, high, low, close, caldt)
        daily_df: DataFrame with daily data (columns: volume, open, high, low, close, caldt)
        
    Returns:
        DataFrame with times as index and last 14 calendar dates as columns, values normalized by daily open
    """
    # Convert caldt to datetime for both dataframes
    intraday_df = intraday_df.copy()
    daily_df = daily_df.copy()
    
    intraday_df['datetime'] = pd.to_datetime(intraday_df['caldt'])
    daily_df['datetime'] = pd.to_datetime(daily_df['caldt'])
    
    # Extract date and time for intraday data
    intraday_df['date'] = intraday_df['datetime'].dt.date
    intraday_df['time'] = intraday_df['datetime'].dt.strftime('%H:%M')
    
    # Extract date for daily data
    daily_df['date'] = daily_df['datetime'].dt.date
    
    # Create a dictionary of daily opens for quick lookup
    daily_opens = daily_df.set_index('date')['open'].to_dict()

    print('daily open:')
    print(daily_df.loc[pd.to_datetime(daily_df['date']) >= '2024-10-02'])


    
    # Create the basic time-date matrix first
    pivot_df = intraday_df.pivot(
        index='time',
        columns='date',
        values='close'
    )

    print('Minute close:')

    pivot_df_debug = pivot_df.copy()

    
    # Get the last X dates
    last_X_dates = sorted(intraday_df['date'].unique())[-rolling_window:]
    pivot_df = pivot_df[last_X_dates]
    print(pivot_df)

    
    # Normalize each column by its daily open
    for date in last_X_dates:
        if date in daily_opens:
            pivot_df[date] = abs(pivot_df[date] / daily_opens[date] - 1)

    print(pivot_df)
    
    # Sort index by time
    pivot_df.sort_index(inplace=True)

    print(pivot_df)
    
    # Round to 4 decimal places for readability
    pivot_df = pivot_df.round(4)
    
    return pivot_df

from pathlib import Path
import logging

def ensure_folder_exists(folder_path: str) -> bool:
    """
    Check if a folder exists and create it if it doesn't.
    
    Args:
        folder_path (str): Path to the folder to check/create
        
    Returns:
        bool: True if folder exists or was created successfully, False on error
        
    Example:
        ensure_folder_exists("data/market_data")
        ensure_folder_exists("/home/user/logs")
    """
    try:
        # Convert string path to Path object
        path = Path(folder_path)
        
        # Create folder if it doesn't exist
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            print(f"Created folder: {folder_path}")
        else:
            print(f"Folder already exists: {folder_path}")
            
        return True
        
    except Exception as e:
        print(f"Error creating folder {folder_path}: {str(e)}")
        return False

def get_latest_csv(folder_path):
    """
    Get the CSV file with the most recent date from the specified folder.
    Files should follow the format: 'common_name-date.csv'
    
    Args:
        folder_path (str): Path to the folder containing CSV files
        
    Returns:
        str: Path to the most recent CSV file, or None if no CSV files found
    """
    # Get all CSV files in the folder
    pattern = os.path.join(folder_path, "*.csv")
    csv_files = glob.glob(pattern)
    
    if not csv_files:
        return None
    
    # Extract dates from filenames and pair with full paths
    def extract_date(filepath):
        filename = os.path.basename(filepath)
        date_str = filename.split('-')[1].replace('.csv', '')
        return datetime.strptime(date_str, '%Y%m%d')
    
    try:
        # Sort files by date and get the most recent
        latest_file = max(csv_files, key=extract_date)
        return latest_file
    except (ValueError, IndexError):
        return None