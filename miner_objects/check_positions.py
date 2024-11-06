import bittensor as bt
from typing import Optional

def setup_wallet(name: str = "default") -> bt.wallet:
    """Setup and return a bittensor wallet."""
    wallet = bt.wallet(name=name)
    return wallet

def setup_subtensor() -> bt.subtensor:
    """Setup and return a subtensor connection."""
    subtensor = bt.subtensor()
    return subtensor

def print_positions(wallet_name: Optional[str] = None):
    """Print all current positions for the specified wallet."""
    try:
        # Setup wallet and subtensor
        wallet = setup_wallet(wallet_name) if wallet_name else setup_wallet()
        subtensor = setup_subtensor()
        
        # Setup metagraph (assuming mainnet, subnet 1)
        metagraph = bt.metagraph(netuid=1, subtensor=subtensor)
        metagraph.sync()
        
        class Config:
            class subtensor:
                network = "main"
            STAKE_MIN = 100
            AXON_NO_IP = "0.0.0.0"
            HIGH_V_TRUST_THRESHOLD = 0.5

        # Initialize position inspector
        from position_inspector import PositionInspector
        inspector = PositionInspector(wallet, metagraph, Config)
        
        # Get validators and query positions
        validators = inspector.get_possible_validators()
        positions = inspector.get_positions_with_retry(validators)
        
        if not positions:
            print("No positions found.")
            return
            
        # Print positions in a readable format
        print("\n=== Current Positions ===\n")
        for i, position in enumerate(positions, 1):
            print(f"Position {i}:")
            print(f"  Symbol: {position.get('symbol', 'N/A')}")
            print(f"  Net Leverage: {position.get('net_leverage', 'N/A')}")
            
            if 'orders' in position:
                print("  Orders:")
                for j, order in enumerate(position['orders'], 1):
                    print(f"    Order {j}:")
                    print(f"      Side: {order.get('side', 'N/A')}")
                    print(f"      Size: {order.get('size', 'N/A')}")
                    print(f"      Price: {order.get('price', 'N/A')}")
            print()

    except Exception as e:
        bt.logging.error(f"Error printing positions: {e}")
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Print current positions from validators")
    parser.add_argument("--wallet", type=str, help="Wallet name to use", default=None)
    
    args = parser.parse_args()
    print_positions(args.wallet)