import os
import logging
import pytz
import numpy as np
import pandas as pd
from datetime import datetime
from mining.utils import create_normalized_matrix
from mining.concretum_strategy.config import (
    symbol, 
    rolling_window, 
    band_mult,
    LIVE_DATA_PATH,
    SIGNALS_PATH
)
from mining.concretum_strategy.data_manager import DataManager
from mining.send_signals_to_miner import map_signal_data, send_signals
from mining.concretum_strategy.position_sizing import PositionSizing

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
        self.position_sizing = PositionSizing()
        self.intra_data, self.daily_data = self.data_manager.load_historical_data()
        self.current_position = 0
        self.current_vol = self.position_sizing.calculate_daily_vol(self.daily_data)

    def calculate_vwap(self, data):
        """Calculate VWAP based on current day's data"""
        data['pv'] = data['avg_price'] * data['volume']
        cumulative_pv = data['pv'].sum()
        cumulative_volume = data['volume'].sum()
        return cumulative_pv / cumulative_volume if cumulative_volume > 0 else 0
        
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

            try:
                cumulative_data = pd.read_csv(f'{SIGNALS_PATH}signal_history_{self.data_manager.end_date}.csv')
                vwap = self.calculate_vwap(cumulative_data)
            except Exception as e:
                logging.error(f"Error calculating VWAP: {str(e)}")
                vwap = None
            
            # Calculate parameters
            current_time = datetime.now(pytz.timezone('America/New_York')).strftime('%H:%M')
            move_from_open = create_normalized_matrix(self.intra_data, self.daily_data, rolling_window)
            avg_move_from_open = pd.DataFrame(move_from_open.mean(axis=1), columns=['avg_move_from_open'])
            
            # Get key prices
            today_open = open_data.open[0]
            current_close = current_bars['close'][0]
            low = current_bars['low'][0]
            high = current_bars['high'][0]
            avg_price = (current_close + high + low) / 3
            volume = current_bars['volume'][0]
            yesterday_close = self.daily_data.close.iloc[-1]
            sigma = avg_move_from_open.loc[current_time].values[0]
            
            # Validate inputs
            self.validate_inputs(today_open, yesterday_close, current_close, sigma)
            
            # Calculate bounds
            upper_bound = np.maximum(today_open, yesterday_close) * (1 + band_mult * sigma)
            lower_bound = np.minimum(today_open, yesterday_close) * (1 - band_mult * sigma)
            
            # Generate signal
            signal, pos_size = self.generate_signal(current_close, upper_bound, lower_bound, vwap)
            
            # Record metadata
            metadata = self.record_signal_metadata(
                signal, pos_size, today_open, current_close, yesterday_close,
                upper_bound, lower_bound, sigma, volume, vwap, avg_price
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
            
    def generate_signal(self, current_close, upper_bound, lower_bound, vwap):
        """Generate trading signal based on price levels"""

        signal = self.current_position
        position_size = 0

        #Entry signals when no position


        if self.current_position == 0:
            if current_close > upper_bound:
                signal = 1

            elif current_close < lower_bound:
                signal = -1

        #Exit signals when in position 
        elif vwap is None:
            #Exit without VWAP
            if self.current_position == 1 and current_close < upper_bound:
                signal = 0
            elif self.current_position == -1 and current_close > lower_bound:
                signal = 0
        else:
            #Exit with VWAP
            if self.current_position == 1 and current_close < np.maximum(vwap, upper_bound):
                signal = 0
            elif self.current_position == -1 and current_close > np.minimum(vwap, lower_bound):
                signal = 0
        
        if self.current_position != signal:
            position_size = position_sizing.calculate_size(signal, self.current_vol)
            signal_type = 'entry' if self.current_position == 0 else 'exit'
            signal_data = map_signal_data(symbol, position_size, signal_type, signal)
            signal_json = send_signals(signal_data)
            logging.info(signal_json)
            self.current_position = signal
        
        return signal, position_size
        
    def record_signal_metadata(self, signal, pos_size, today_open, current_close, 
                             yesterday_close, upper_bound, lower_bound, sigma, volume, vwap, avg_price):
        """Record signal metadata for logging and analysis"""
        metadata = {
            'timestamp': datetime.now(pytz.timezone('America/New_York')),
            'today_open': today_open,
            'current_close': current_close,
            'yesterday_close': yesterday_close,
            'upper_bound': upper_bound,
            'lower_bound': lower_bound,
            'sigma': sigma,
            'signal': signal,
            'position_size': pos_size,
            'volume': volume,
            'vwap': vwap,
            'avg_price': avg_price
        }
        
        # Log signal
        logging.info(f"""
        Signal Generated:
        Time: {metadata['timestamp']}
        Signal: {signal}
        Position Size: {pos_size}
        Current Price: {current_close}
        Bounds: [{lower_bound}, {upper_bound}]
        Sigma: {sigma}
        VWAP: {vwap}
        Average Price: {avg_price}
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

