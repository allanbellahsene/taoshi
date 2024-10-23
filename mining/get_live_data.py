import alpaca_trade_api as alpacaapi
import pandas as pd
from datetime import datetime
import pytz
import os
import logging
import time
from typing import Optional, Dict, Any, Callable
from functools import wraps

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spy_data_collection.log'),
        logging.StreamHandler()
    ]
)

def retry_with_delay(max_retries: int = 3, delay: float = 30):
    """
    Decorator that implements retry logic with fixed delay.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Fixed delay between retries in seconds
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for retry in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if retry == max_retries:
                        logging.error(f"Final retry attempt failed for {func.__name__}: {str(e)}")
                        raise
                    
                    logging.warning(f"Attempt {retry + 1}/{max_retries} failed for {func.__name__}: {str(e)}")
                    logging.info(f"Retrying in {delay} seconds...")
                    
                    time.sleep(delay)
            
            raise last_exception
        return wrapper
    return decorator

class DataCollector:
    def __init__(self, symbol: str, api_key: str, api_secret: str, api_base_url: str):
        """Initialize the data collector with API credentials."""
        self.api = alpacaapi.REST(api_key, api_secret, api_base_url)
        self.file_path = 'historical_data/live_data.csv'
        self.symbol = symbol

    @retry_with_delay(max_retries=3, delay=30)
    def get_bar_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest bar data from Alpaca API with retry logic.
        Returns None if all retries fail.
        """
        try:
            latest_bar = self.api.get_latest_bar(self.symbol)
            return {
                'Datetime': latest_bar.t,
                'Symbol': self.symbol,
                'Open': latest_bar.o,
                'High': latest_bar.h,
                'Low': latest_bar.l,
                'Close': latest_bar.c,
                'Volume': latest_bar.v
            }
        except Exception as e:
            logging.error(f"Error fetching bar data: {str(e)}")
            raise  # Re-raise for retry decorator to handle

    def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate the collected data.
        Returns True if data is valid, False otherwise.
        """
        required_fields = ['Datetime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        # Check if all fields exist and are not None
        if not all(field in data and data[field] is not None for field in required_fields):
            logging.error("Missing or None values in data")
            return False
            
        # Check if numeric fields are actually numeric
        numeric_fields = ['Open', 'High', 'Low', 'Close', 'Volume']
        try:
            for field in numeric_fields:
                float(data[field])
            return True
        except (ValueError, TypeError):
            logging.error("Invalid numeric values in data")
            return False

    @retry_with_delay(max_retries=3, delay=30)
    def save_data(self, data: Dict[str, Any]) -> bool:
        """
        Save the data to CSV file with retry logic.
        Returns True if successful, False otherwise.
        """
        try:
            # Convert data to DataFrame
            new_row = pd.DataFrame([data])
            
            # If file exists, append to it
            if os.path.exists(self.file_path):
                # Add file lock or semaphore here if needed for concurrent access
                existing_data = pd.read_csv(self.file_path)
                
                # Check for duplicate entries
                if not existing_data.empty:
                    if existing_data['Datetime'].astype(str).eq(str(data['Datetime'])).any():
                        logging.warning("Duplicate entry found, skipping...")
                        return False
                    
                updated_data = pd.concat([existing_data, new_row], ignore_index=True)
            else:
                updated_data = new_row
            
            # Save to CSV
            updated_data.to_csv(self.file_path, index=False)
            logging.info(f"Successfully saved data for {data['Datetime']}")
            return True
            
        except Exception as e:
            logging.error(f"Error saving data: {str(e)}")
            raise  # Re-raise for retry decorator to handle

    @retry_with_delay(max_retries=3, delay=30)
    def collect_and_save(self) -> None:
        """
        Main function to collect and save the data with retry logic.
        """
        try:
            # Get the data
            data = self.get_bar_data()
            
            # If data collection failed, exit
            if not data:
                raise Exception("Failed to collect data")
                
            # Validate the data
            if not self.validate_data(data):
                raise Exception("Data validation failed")
                
            # Save the data
            if not self.save_data(data):
                raise Exception("Failed to save data")
                
            logging.info("Data collection cycle completed successfully")
            
        except Exception as e:
            logging.error(f"Error in collect_and_save: {str(e)}")
            raise  # Re-raise for retry decorator to handle

def main():

    APCA_API_DATA_URL = 'https://data.alpaca.markets'
    API_KEY     = "AK1TXEM0NSG8T5O9DC8U"
    API_SECRET = "hOFq2cQxKLNScoZ766Uo4PObpJk7Vgfd2xnH8DrV"
    symbol = 'SPY'
    
    try:
        collector = DataCollector(symbol, API_KEY, API_SECRET, APCA_API_DATA_URL)
        collector.collect_and_save()
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")

if __name__ == "__main__":
    main()
