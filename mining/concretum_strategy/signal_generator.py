import os
import logging
import pytz
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from mining.utils import create_normalized_matrix
from mining.concretum_strategy.config import (
    rolling_window, 
    band_mult,
    LIVE_DATA_PATH,
    SIGNALS_PATH,
    trade_frequency  # Add this to your config file
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
    
    def __init__(self, asset):
        self.data_manager = DataManager(asset)
        self.position_sizing = PositionSizing()
        self.intra_data, self.daily_data = self.data_manager.load_historical_data()
        #logging.info(f'Intra data: {self.intra_data}')
        #logging.info(f'Intra data columns: {self.intra_data.columns}')
        self.current_position = 0
        self.current_vol = self.position_sizing.calculate_daily_vol(self.daily_data)
        self.position_size = self.position_sizing.calculate_size(self.current_vol)
        self.last_signal_time = None
        self.pending_signal = None
        self.asset = asset
        self.live_path = LIVE_DATA_PATH + f'{self.asset}/'
        self.signal_path = SIGNALS_PATH + f'{self.asset}/'
        
        try:
            signal_data = pd.read_csv(f'{SIGNALS_PATH}signal_history_{self.data_manager.end_date}.csv')
            self.current_position = signal_data['signal'].iloc[-1]
            logging.info(f'Current position: {self.current_position}')
        except:
            self.current_position = 0

    def is_trading_allowed(self):
            """Check if trading is allowed based on trade frequency"""
            current_time = datetime.now(pytz.timezone('America/New_York'))
            
            # This will be true at XX:00, XX:30 for trade_frequency=30
            # or at XX:00, XX:10, XX:20, XX:30, XX:40, XX:50 for trade_frequency=10

            logging.info(f'{current_time.minute % trade_frequency} minutes from allowed trades.')
            return current_time.minute % trade_frequency == 0

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
    
    def get_trade_inputs(self):
        try:
            # Load required data
            #logging.info(f'End date: {self.data_manager.end_date}')
            #print(f'End date: {self.data_manager.end_date}')
            filename = f'{self.asset}-{self.data_manager.end_date}-open_live-data.csv'
            filepath = os.path.join(self.live_path, filename)
            open_data = pd.read_csv(filepath)
            current_bars = self.data_manager.get_live_data()

            try:
                cumulative_data = pd.read_csv(f'{self.signal_path}signal_history_{self.data_manager.end_date}.csv')
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
            #upper_bound = 573
            lower_bound = np.minimum(today_open, yesterday_close) * (1 - band_mult * sigma)

            return today_open, current_close, yesterday_close, upper_bound, lower_bound, sigma, volume, vwap, avg_price
        except Exception as e:
            logging.error(f"Error in fetching trade inputs: {str(e)}")
            raise


            
    def handle_regular_session(self):
        """Handle regular session signal generation"""
        try:
            today_open, current_close, yesterday_close, upper_bound, lower_bound, sigma, volume, vwap, avg_price = self.get_trade_inputs()
            # Generate signal
            computed_signal = self.generate_signal(current_close, upper_bound, lower_bound, vwap, execute_trades=self.is_trading_allowed())
            
            # Record metadata
            metadata = self.record_signal_metadata(
                computed_signal, today_open, current_close, yesterday_close,
                upper_bound, lower_bound, sigma, volume, vwap, avg_price
            )
            
            return computed_signal, metadata
            
        except Exception as e:
            logging.error(f"Error in regular session handling: {str(e)}")
            raise
    
    def handle_market_closing(self):
        if self.current_position != 0:
            signal_data = map_signal_data(self.asset, self.current_position, 
            self.position_size, signal_type='exit', signal_direction=-self.current_position)
            signal_json = send_signals(signal_data)
            logging.info('NEW SIGNAL GENERATED')
            logging.info('EXITING POSITION ON EOD')
            logging.info(signal_json)
            self.current_position = 0
            today_open, current_close, yesterday_close, upper_bound, lower_bound, sigma, volume, vwap, avg_price = self.get_trade_inputs()
            metadata = self.record_signal_metadata(
                self.current_position, today_open, current_close, yesterday_close,
                upper_bound, lower_bound, sigma, volume, vwap, avg_price
            )
            
    @staticmethod
    def validate_inputs(today_open, yesterday_close, current_close, sigma):
        """Validate calculation inputs"""
        if any(pd.isna([today_open, yesterday_close, current_close, sigma])):
            raise ValueError("Input contains NaN values")
        if any(x <= 0 for x in [today_open, yesterday_close, current_close]):
            raise ValueError("Prices must be positive")
        if sigma < 0:
            raise ValueError("Sigma must be non-negative")
        if not all(isinstance(x, (int, float, np.float64)) for x in [today_open, yesterday_close, current_close, sigma]):
            raise TypeError("All inputs must be numerical values")
            
    def generate_signal(self, current_close, upper_bound, lower_bound, vwap, execute_trades=False):
        """Generate trading signal based on price levels"""
        signal = self.current_position

        # Compute the desired signal regardless of trade frequency
        if self.current_position == 0:
            if current_close > upper_bound:
                signal = 1
            elif current_close < lower_bound:
                signal = -1
        elif vwap is None:
            if self.current_position == 1 and current_close < upper_bound:
                signal = 0
            elif self.current_position == -1 and current_close > lower_bound:
                signal = 0
        else:
            if self.current_position == 1 and current_close < np.maximum(vwap, upper_bound):
                signal = 0
            elif self.current_position == -1 and current_close > np.minimum(vwap, lower_bound):
                signal = 0
        
        # Only execute trades if we're at a valid trading interval
        if execute_trades and self.current_position != signal:
            signal_type = 'entry' if self.current_position == 0 else 'exit'
            signal_data = map_signal_data(self.asset, self.current_position, self.position_size, signal_type, signal)
            signal_json = send_signals(signal_data)
            logging.info(f'NEW SIGNAL GENERATED for {self.asset}')
            logging.info(f'Trading window: {datetime.now(pytz.timezone("America/New_York")).strftime("%H:%M")}')
            logging.info(signal_json)
            self.current_position = signal
            self.last_signal_time = datetime.now(pytz.timezone('America/New_York'))
        elif not execute_trades and self.current_position != signal:
            logging.info(f'Signal computed but not executed due to trade frequency restriction. Time: {datetime.now(pytz.timezone("America/New_York")).strftime("%H:%M")}')
        
        return self.current_position
        
    def record_signal_metadata(self, signal, today_open, current_close, 
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
            'position_size': self.position_size,
            'volume': volume,
            'vwap': vwap,
            'avg_price': avg_price,
            'trade_allowed': self.is_trading_allowed()
        }
        
        logging.info(f"""
        Symbol: {self.asset}
        Time: {metadata['timestamp']}
        Signal: {signal}
        Position Size: {self.position_size}
        Current Price: {current_close}
        Bounds: [{lower_bound}, {upper_bound}]
        Sigma: {sigma}
        VWAP: {vwap}
        Average Price: {avg_price}
        Trade Allowed: {self.is_trading_allowed()}
        """)
        
        try:
            signal_history = pd.DataFrame([metadata])
            signal_history.to_csv(
                f'{self.signal_path}signal_history_{self.data_manager.end_date}.csv',
                mode='a',
                header=not os.path.exists(f'{self.signal_path}signal_history_{self.data_manager.end_date}.csv'),
                index=False
            )
        except Exception as e:
            logging.warning(f"Could not save signal history: {str(e)}")
            
        return metadata

if __name__ == "__main__":
    signal_generator = SignalGenerator('SPY')
    print(signal_generator.handle_market_opening())
    #print(signal_generator.handle_regular_session())

    today_open = 100
    yesterday_close = 98
    current_close = 102
    sigma = 0.001

    upper_bound = np.maximum(today_open, yesterday_close) * (1 + band_mult * sigma)
    lower_bound = np.minimum(today_open, yesterday_close) * (1 - band_mult * sigma)

