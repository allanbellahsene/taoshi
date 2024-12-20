import pandas as pd
import matplotlib.pyplot as plt
from mining.concretum_strategy.config import SIGNALS_PATH, SIGNALS_PLOT_PATH
from mining.utils import get_most_recent_signal_file

#### ADD LOGIC TO FORCE EXIT IF STILL SIGNAL ON LAST MINUTE OF DAY

class SignalValidator:
    def __init__(self, df, asset):
        """
        Initialize the validator with a DataFrame containing trading signals.
        
        Parameters:
        df: pandas DataFrame with columns:
            - timestamp
            - current_close
            - upper_bound
            - lower_bound
            - signal
        """
        self.df = df.copy()
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.asset = asset
        self.plot_path = SIGNALS_PLOT_PATH + f'{symbol}/'
    
    ### CHECK NUMBER OF SIGNALS GENERATED DURING THE DAY: LONG AND SHORTS
    ### CHECK THAT SIGNALS WERE CORRECTLY GENERATED
    ### PLOT SIGNALS ALONG WITH INDICATORS AND SAVE PLOT

    def count_signals(self):
        long_signals_count = len(self.df.loc[self.df['signal'] == 1])
        short_signals_count = len(self.df.loc[self.df['signal'] == -1])

        long_entry = len(self.df.loc[(self.df['signal'].shift() != 1) & (self.df['signal'] == 1)])
        long_exit = len(self.df.loc[(self.df['signal'].shift() == 1) & (self.df['signal'] != 1)])

        short_entry = len(self.df.loc[(self.df['signal'].shift() != -1) & (self.df['signal'] == -1)])
        short_exit = len(self.df.loc[(self.df['signal'].shift() == -1) & (self.df['signal'] != -1)])

        data = {"Long_Signals_Count": [long_signals_count], "Short_Signals_Count": [short_signals_count],
        "Long_Entry_Count": [long_entry], "Long_Exit_Count": [long_exit], 
        "Short_Entry_Count": [short_entry], "Short_Exit_Count": [short_exit]}

        return data
    
    def check_signals_validity(self):

        long_signals_check = len(self.df.loc[self.df['signal'] == 1]) == len(self.df.loc[self.df['current_close'] > self.df['upper_bound']])
        short_signals_check = len(self.df.loc[self.df['signal'] == -1]) == len(self.df.loc[self.df['current_close'] < self.df['lower_bound']])

        print(f'Long Signals Check Passed: {long_signals_check}')
        print(f'Short Signals Check Passed: {short_signals_check}')


    def plot_signals(self, plot_title):
        """Plot trading signals with different colors for long and short"""
        plt.figure(figsize=(15, 8))
        
        # Plot price and bounds
        plt.plot(self.df['timestamp'], self.df['current_close'], label='Price', color='blue')
        plt.plot(self.df['timestamp'], self.df['upper_bound'], label='Upper Bound', color='gray', linestyle='--', alpha=0.7)
        plt.plot(self.df['timestamp'], self.df['lower_bound'], label='Lower Bound', color='gray', linestyle='--', alpha=0.7)
        plt.plot(self.df['timestamp'], self.df['vwap'], label='VWAP', color='pink')
    
        
        # Long entries (0 to 1)
        long_entries = self.df.loc[(self.df['signal'].shift() != 1) & (self.df['signal'] == 1)]
        plt.scatter(long_entries['timestamp'], long_entries['current_close'], 
                   color='green', marker='^', s=100, label='Long Entry')
        
        # Long exits (1 to 0)
        long_exits = self.df.loc[(self.df['signal'].shift() == 1) & (self.df['signal'] != 1)]
        plt.scatter(long_exits['timestamp'], long_exits['current_close'], 
                   color='orange', marker='v', s=100, label='Long Exit')
        
        # Short entries (0 to -1)
        short_entries = self.df.loc[(self.df['signal'].shift() != -1) & (self.df['signal'] == -1)]
        plt.scatter(short_entries['timestamp'], short_entries['current_close'], 
                   color='red', marker='v', s=100, label='Short Entry')
        
        # Short exits (-1 to 0)
        short_exits = self.df.loc[(self.df['signal'].shift() == -1) & (self.df['signal'] != -1)]
        plt.scatter(short_exits['timestamp'], short_exits['current_close'], 
                   color='lime', marker='^', s=100, label='Short Exit')
        
        plt.title(f'{self.asset} Trading Signals with Price Bounds', fontsize=12)
        plt.xlabel('Time')
        plt.ylabel('Price')
        plt.grid(True, alpha=0.3)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{self.plot_path}{plot_title}.png')
        return plt

def run_signal_analysis(asset, filename, plot_title):
    """
    Main function to run all signal analysis.
    """
    # Load data
    df = pd.read_csv(filename)
    
    # Initialize validator
    validator = SignalValidator(df, asset)
    
    # Run checks
    nb_signals = validator.count_signals()
    validity_results = validator.check_signals_validity()
    
    # Plot signals
    plot = validator.plot_signals(plot_title)
    plot.show()
    
    return validator, nb_signals, validity_results

if __name__ == "__main__":
    # Replace with your CSV file path
    symbol = 'SPY'
    signal_path = SIGNALS_PATH + f'{symbol}/'
    filename = get_most_recent_signal_file(signal_path)
    filename = f'{signal_path}{filename}'
    print(filename)
    plot_title = 'signals_plot' + filename[-14:-4]
    validator, nb_signals, validity_results = run_signal_analysis(symbol, filename, plot_title)
    print(validity_results)
    print(nb_signals)