import os
import logging
import shutil
import pytz
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time
import alpaca_trade_api as alpacaapi
from dotenv import load_dotenv
from mining.utils import create_normalized_matrix
from mining.data_checks import MarketCalendar, is_market_open_today
from mining.concretum_strategy.config import (
    market_open,
    market_close, 
    symbol, 
    rolling_window, 
    band_mult,
    LIVE_DATA_PATH,
    HIST_DATA_PATH,
    SIGNALS_PATH
)
from mining.concretum_strategy.data_manager import DataManager
from mining.concretum_strategy.market_session import MarketSession

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('strategy.log'),
        logging.StreamHandler()
    ]
)

class SignalGenerator:
    """Handles signal generation logic"""
    
    def __init__(self):
        self.data_manager = DataManager()
        self.intra_data, self.daily_data = self.data_manager.load_historical_data()
        
    def handle_market_opening(self):
        """Handle market opening data collection"""
        try:
            current_bars = self.data_manager.get_live_data()
            self.data_manager.save_open_data(current_bars)
            logging.info("Successfully recorded market opening data")
            return None
        except Exception as e:
            logging.error(f"Error handling market opening: {str(e)}")
            raise
            
    def handle_regular_session(self):
        """Handle regular session signal generation"""
        try:
            # Load required data
            logging.info(f'End date: {self.data_manager.end_date}')
            print(f'End date: {self.data_manager.end_date}')
            open_data = pd.read_csv(f'{LIVE_DATA_PATH}{symbol}-{self.data_manager.end_date}-open_live-data.csv')
            current_bars = self.data_manager.get_live_data()
            
            # Calculate parameters
            current_time = datetime.now(pytz.timezone('America/New_York')).strftime('%H:%M')
            move_from_open = create_normalized_matrix(self.intra_data, self.daily_data, rolling_window)
            avg_move_from_open = pd.DataFrame(move_from_open.mean(axis=1), columns=['avg_move_from_open'])
            
            # Get key prices
            today_open = open_data.open[0]
            current_close = current_bars['close'][0]
            yesterday_close = self.daily_data.close.iloc[-1]
            sigma = avg_move_from_open.loc[current_time].values[0]
            
            # Validate inputs
            self.validate_inputs(today_open, yesterday_close, current_close, sigma)
            
            # Calculate bounds
            upper_bound = np.maximum(today_open, yesterday_close) * (1 + band_mult * sigma)
            lower_bound = np.minimum(today_open, yesterday_close) * (1 - band_mult * sigma)
            
            # Generate signal
            signal = self.generate_signal(current_close, upper_bound, lower_bound)
            
            # Record metadata
            metadata = self.record_signal_metadata(
                signal, today_open, current_close, yesterday_close,
                upper_bound, lower_bound, sigma
            )
            
            return signal, metadata
            
        except Exception as e:
            logging.error(f"Error in regular session handling: {str(e)}")
            raise
            
    @staticmethod
    def validate_inputs(today_open, yesterday_close, current_close, sigma):
        """Validate calculation inputs"""
        if any(pd.isna([today_open, yesterday_close, current_close, sigma])):
            raise ValueError("Input contains NaN values")
        if any(x <= 0 for x in [today_open, yesterday_close, current_close]):
            raise ValueError("Prices must be positive")
        if sigma < 0:
            raise ValueError("Sigma must be non-negative")
            
    @staticmethod
    def generate_signal(current_close, upper_bound, lower_bound):
        """Generate trading signal based on price levels"""
        if current_close > upper_bound:
            return 1
        elif current_close < lower_bound:
            return -1
        return 0
        
    def record_signal_metadata(self, signal, today_open, current_close, 
                             yesterday_close, upper_bound, lower_bound, sigma):
        """Record signal metadata for logging and analysis"""
        metadata = {
            'timestamp': datetime.now(pytz.timezone('America/New_York')),
            'today_open': today_open,
            'current_close': current_close,
            'yesterday_close': yesterday_close,
            'upper_bound': upper_bound,
            'lower_bound': lower_bound,
            'sigma': sigma,
            'signal': signal
        }
        
        # Log signal
        logging.info(f"""
        Signal Generated:
        Time: {metadata['timestamp']}
        Signal: {signal}
        Current Price: {current_close}
        Bounds: [{lower_bound}, {upper_bound}]
        Sigma: {sigma}
        """)
        
        # Save to history
        try:
            signal_history = pd.DataFrame([metadata])
            signal_history.to_csv(
                f'{SIGNALS_PATH}signal_history_{self.data_manager.end_date}.csv',
                mode='a',
                header=not os.path.exists(f'{SIGNALS_PATH}signal_history_{self.data_manager.end_date}.csv'),
                index=False
            )
        except Exception as e:
            logging.warning(f"Could not save signal history: {str(e)}")
            
        return metadata

if __name__ == "__main__":
    signal_generator = SignalGenerator()
    print(signal_generator.handle_market_opening())
    #print(signal_generator.handle_regular_session())

    today_open = 100
    yesterday_close = 98
    current_close = 102
    sigma = 0.001

    upper_bound = np.maximum(today_open, yesterday_close) * (1 + band_mult * sigma)
    lower_bound = np.minimum(today_open, yesterday_close) * (1 - band_mult * sigma)


    #print(signal_generator.validate_inputs(today_open, yesterday_close, current_close, sigma))
    #signal = signal_generator.generate_signal(current_close, upper_bound, lower_bound)
    #signal_generator.record_signal_metadata(signal, today_open, current_close, 
    #                         yesterday_close, upper_bound, lower_bound, sigma)

