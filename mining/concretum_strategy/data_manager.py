import os
import logging
import shutil
import pandas as pd
from datetime import datetime
import alpaca_trade_api as alpacaapi
from dotenv import load_dotenv
from mining.concretum_strategy.config import (
    HIST_DATA_PATH,
    LIVE_DATA_PATH
)


# Load environment variables
load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

class DataManager:
    """Handles all data operations for the strategy"""
    
    def __init__(self, asset):
        self.asset = asset
        self.api_key = api_key
        self.api_secret = api_secret
        self.today = datetime.today()
        self.end_date = self.today.strftime('%Y-%m-%d')
        self.live_path = LIVE_DATA_PATH + f'{self.asset}/'
        self.hist_path = HIST_DATA_PATH + f'{self.asset}/'
        
    def get_live_data(self):
        """Fetch latest market data from Alpaca"""
        try:
            APCA_API_DATA_URL = 'https://data.alpaca.markets'
            api = alpacaapi.REST(self.api_key, self.api_secret, APCA_API_DATA_URL)
            latest_bar = api.get_latest_bar(self.asset)
            
            if latest_bar is None:
                raise ValueError("No data received from Alpaca")
                
            return {
                "symbol": [self.asset],
                "time": [latest_bar.t],
                "open": [latest_bar.o],
                "high": [latest_bar.h],
                "low": [latest_bar.l],
                "close": [latest_bar.c],
                "volume": [latest_bar.v]
            }
        except Exception as e:
            logging.error(f"Error fetching live data: {str(e)}")
            raise
            
    def save_open_data(self, data):
        """
        Save market opening data to a CSV file with backup functionality.
        
        Args:
            data: Market data to be saved, expected to contain 'time' column
            
        The function:
        1. Creates a backup of existing file if present
        2. Saves new data to CSV
        3. Removes backup if save is successful
        4. Restores from backup if save fails
        
        Raises:
            Exception: If saving fails and includes original error message
        """
        # Ensure the base directory exists
        if not os.path.exists(self.live_path):
            os.makedirs(self.live_path)
            
        # Generate filenames for main and backup files
        filename = f'{self.asset}-{self.end_date}-open_live-data.csv'
        filepath = os.path.join(self.live_path, filename)
        backup_filename = f'{filename}.backup'
        backup_filepath = os.path.join(self.live_path, backup_filename)
        
        print(f"Attempting to save data for {self.asset} to {filename}")
        
        # Clean up if filepath is incorrectly a directory
        if os.path.isdir(filepath):
            print(f"Removing incorrectly created directory: {filepath}")
            shutil.rmtree(filepath)
            logging.info(f"Removed directory: {filepath}")
            
        if os.path.isdir(backup_filepath):
            print(f"Removing incorrectly created backup directory: {backup_filepath}")
            shutil.rmtree(backup_filepath)
            logging.info(f"Removed directory: {backup_filepath}")
        
        try:
            # Create backup if original file exists
            if os.path.isfile(filepath):
                print(f"Creating backup of existing file: {backup_filename}")
                shutil.copy2(filepath, backup_filepath)
                logging.info(f"Backup created successfully: {backup_filename}")
            
            # Convert data to DataFrame and format time column
            print("Processing data for saving...")
            df = pd.DataFrame(data)
            df['time'] = pd.to_datetime(df['time'])
            
            # Save new data to CSV
            print(f"Saving {len(df)} rows of data to {filename}")
            df.to_csv(filepath, index=False)
            logging.info(f"Data successfully saved to {filename}")
            
            # Remove backup file if save was successful
            if os.path.exists(backup_filepath):
                print("Removing backup file after successful save")
                os.remove(backup_filepath)
                logging.info("Backup file removed successfully")
                
        except Exception as e:
            error_msg = f"Error saving data: {str(e)}"
            print(f"ERROR: {error_msg}")
            logging.error(error_msg)
            
            # Attempt to restore from backup if it exists
            if os.path.exists(backup_filepath):
                print("Attempting to restore from backup...")
                shutil.copy2(backup_filepath, filepath)
                logging.info("Successfully restored from backup")
                print("Restored from backup successfully")
            
            raise Exception(f"Failed to save data: {str(e)}")
            
    def load_historical_data(self):
        """Load historical data for calculations"""
        try:
            latest_file = self.find_latest_file(self.hist_path)
            intra_data = pd.read_csv(f'{self.hist_path}{latest_file}-1min.csv')
            daily_data = pd.read_csv(f'{self.hist_path}{latest_file}-1d.csv')
            return intra_data, daily_data
        except Exception as e:
            logging.error(f"Error loading historical data: {str(e)}")
            raise
            
    @staticmethod
    def find_latest_file(folder_path):
        """Find most recent data file"""
        latest_date = None
        latest_file = None
        
        for filename in os.listdir(folder_path):
            if not filename.endswith('.csv'):
                continue
                
            try:
                parts = filename.split('-')
                if len(parts) >= 6:
                    date_str = f"{parts[4]}-{parts[5]}-{parts[6][:2]}"
                    file_date = datetime.strptime(date_str, "%Y-%m-%d")
                    
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                        latest_file = filename
            except (ValueError, IndexError):
                continue


        print('LATEST FILE')
        print(latest_file)        
    
                
        return latest_file[:-7] if latest_file else None


if __name__ == "__main__":
    asset = 'QQQ'
    data_manager = DataManager(asset)
    live_data = data_manager.get_live_data()
    print('live data:')
    print(live_data)
    latest_historic_data = data_manager.load_historical_data()
    print('historical data:')
    print(latest_historic_data)
    print('saving live data...')
    data_manager.save_open_data(live_data)