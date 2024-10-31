from datetime import datetime, timedelta, time
import pytz

class MarketSession:
    """Handles market session logic and validation"""
    
    @staticmethod
    def get_session_status():
        """Get current market session status"""
        ny_time = datetime.now(pytz.timezone('America/New_York')).time()

        if ny_time < time(9, 31) or ny_time > time(16, 0):
            return "CLOSED"
        
        else:
            if ny_time < time(9, 32):
                return "OPENING"
            elif ny_time >= time(15, 59):
                return "CLOSING"
            else:
                return "REGULAR"
        

if __name__ == "__main__":
    market_session = MarketSession()
    print(market_session.get_session_status())