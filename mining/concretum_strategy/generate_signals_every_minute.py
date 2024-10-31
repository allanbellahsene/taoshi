"""
Concretum Intraday Momentum Strategy
Generates intraday signals every minute during market hours.

The script:
1. Validates if market is open
2. Records opening data at market open (9:30 ET)
3. Generates trading signals during regular session (9:31-16:00 ET)
"""
######## TO DO: 
# - ADD VWAP
# - ADD LOGIC TO ONLY TRIGGER SIGNALS AT 00 or 30 every hour

import os
import logging
from dotenv import load_dotenv
from mining.data_checks import MarketCalendar, is_market_open_today
from mining.concretum_strategy.market_session import MarketSession
from mining.concretum_strategy.signal_generator import SignalGenerator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('strategy.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

def main():
    """Main execution function"""
    try:
        # Initialize components
        calendar = MarketCalendar()
        signal_generator = SignalGenerator()
        
        # Check if market is open
        if not is_market_open_today(calendar):
            logging.info('Market is closed today.')
            return None
            
        # Get market session status
        market_status = MarketSession.get_session_status()
        
        if market_status == "CLOSED":
            logging.info('Outside trading hours')
            return None
            
        # Handle appropriate session
        if market_status == "OPENING":
            return signal_generator.handle_market_opening()
        elif market_status == "REGULAR":
            return signal_generator.handle_regular_session()
        else:
            logging.error(f'{market_status} is not an accepted market status. Market status can be "OPENING", "REGULAR" or "CLOSED".')
            
    except Exception as e:
        logging.error(f"Critical error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()








