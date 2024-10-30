from datetime import datetime, timedelta, time
import pytz

class MarketSession:
    """Handles market session logic and validation"""
    
    @staticmethod
    def get_session_status():
        """Get current market session status"""
        ny_time = datetime.now(pytz.timezone('America/New_York'))
        
        if not MarketSession.is_valid_trading_time(ny_time):
            return "CLOSED"
        elif time(9, 30) <= ny_time.time() <= time(9, 31):
            return "OPENING"
        elif ny_time.time() == time(16, 0):
            return "CLOSING"
        else:
            return "REGULAR"
            
    @staticmethod
    def is_valid_trading_time(current_time):
        """Validate if current time is within trading hours"""
        ny_tz = pytz.timezone('America/New_York')
        market_open = time(9, 30)
        market_close = time(16, 0)
        
        if not isinstance(current_time, datetime):
            raise ValueError("current_time must be a datetime object")
            
        ny_time = current_time.astimezone(ny_tz)
        current_time = ny_time.time()

        print(f'Current NY time is: {current_time}')
        
        return market_open <= current_time <= market_close

if __name__ == "__main__":
    market_session = MarketSession()
    print(market_session.get_session_status())