# Every day, fetch daily data from last X days
# Compute daily volatility as unbiased standard deviation of returns

import numpy as np
from mining.concretum_strategy.config import (
    volatility_window,
    MAX_LEVERAGE,
    DAILY_VOL_TARGET
)

class PositionSizing:
    """Handles signal generation logic"""
    
    def __init__(self):
        self.vol_window = volatility_window
        self.max_leverage = MAX_LEVERAGE
        self.vol_target = DAILY_VOL_TARGET
    
    def calculate_daily_vol(self, daily_data):
        if len(daily_data) < self.vol_window:
            raise ValueError(f'Length of daily data ({len(daily_data)}) is smaller than length of volatility window ({self.vol_window})')
        
        data = daily_data.iloc[-self.vol_window:]

        returns = data['close'].pct_change()

        vol = np.std(returns)

        return vol

    def calculate_size(self, vol_estimate):
        return np.minimum(self.max_leverage, self.vol_target / vol_estimate)

if __name__ == "__main__":
    symbol = 'QQQ'
    from mining.concretum_strategy.data_manager import DataManager
    data_manager = DataManager(symbol)
    _, daily_data = data_manager.load_historical_data()
    position_sizing = PositionSizing()
    vol = position_sizing.calculate_daily_vol(daily_data)
    signal = -1
    print(position_sizing.calculate_size(vol))
    print(vol)

