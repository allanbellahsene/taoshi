import bittensor as bt
import argparse
from template.protocol import GetDashData
from datetime import datetime

def timestamp_to_datetime(ms):
    """Convert timestamp in milliseconds to readable datetime"""
    return datetime.fromtimestamp(ms/1000).strftime('%Y/%m/%d %H:%M:%S')

def get_current_positions(config) -> list:
    """
    Query current open positions using the dashboard endpoint.
    
    Args:
        config: Bittensor config object with wallet and network settings
    
    Returns:
        list: List of open positions
    """
    # Setup wallet and subtensor
    wallet = bt.wallet(config=config)
    subtensor = bt.subtensor(config=config)
    
    # Setup metagraph
    metagraph = subtensor.metagraph(netuid=config.netuid)
    metagraph.sync()
    
    # Create dendrite for network calls
    dendrite = bt.dendrite(wallet=wallet)
    
    # Create GetDashData request
    request = GetDashData()
    
    # Query validators
    if config.subtensor.network == "test":
        validators = metagraph.axons
    else:
        validators = [n.axon_info for n in metagraph.neurons 
                     if n.hotkey == "5FFApaS75bv5pJHfAp2FVLBj9ZaXuFDjEypsaBNc1wCfe52v"]
    
    print(f"Querying validator...")
    
    # Query validator
    responses = dendrite.query(validators, request, deserialize=True, timeout=12)
    
    # Process responses
    open_positions = []
    for validator, response in zip(validators, responses):
        if response.successfully_processed and response.data:
            dash_data = response.data
            
            # Extract positions from the correct path in the response
            positions_data = dash_data.get('positions', {})
            for miner_data in positions_data.values():
                miner_positions = miner_data.get('positions', [])
                
                # Filter for open positions only
                open_positions.extend([pos for pos in miner_positions if not pos.get('is_closed_position', True)])
                
    # Display open positions
    if open_positions:
        print("\nCurrent Open Positions:")
        for pos in open_positions:
            trade_pair = pos['trade_pair'][0]  # First element is the trade pair ID
            position_type = pos['position_type']
            net_leverage = pos['net_leverage']
            entry_price = pos['average_entry_price']
            open_time = timestamp_to_datetime(pos['open_ms'])
            current_return = pos['current_return']
            
            print(f"\nTrade Pair: {trade_pair}")
            print(f"Position Type: {position_type}")
            print(f"Net Leverage: {net_leverage}")
            print(f"Entry Price: {entry_price}")
            print(f"Open Time: {open_time}")
            print(f"Current Return: {current_return}")
            
            print("Orders:")
            for order in pos['orders']:
                print(f"  - Type: {order['order_type']}")
                print(f"    Leverage: {order['leverage']}")
                print(f"    Price: {order['price']}")
                print(f"    Time: {timestamp_to_datetime(order['processed_ms'])}")
    
    return open_positions

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--netuid", type=int, default=116, help="The chain subnet uid (default=116)")
    
    # Add bittensor specific arguments
    bt.subtensor.add_args(parser)
    bt.logging.add_args(parser)
    bt.wallet.add_args(parser)
    
    # Parse config
    config = bt.config(parser)
    
    # Enable logging
    #bt.logging(config=config)
    
    positions = get_current_positions(config)
    
    if not positions:
        print("\nNo open positions found.")
    else:
        print(f"\nFound total of {len(positions)} open positions.")
        print(positions)