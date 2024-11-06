import sys

import requests
import json

from vali_objects.enums.order_type_enum import OrderType
from vali_config import TradePair, TradePairCategory


import json
import os

def load_api_key(config_path='mining/miner_secrets.json'):
    """
    Load API key from a JSON configuration file.

    Args:
        config_path (str): Path to the configuration JSON file.

    Returns:
        str: The API key.
    """
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
            api_key = config.get('api_key')
            if not api_key:
                raise KeyError("API key not found in the configuration file.")
            return api_key
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
    except json.JSONDecodeError:
        raise ValueError(f"Configuration file '{config_path}' contains invalid JSON.")
    except KeyError as e:
        raise KeyError(str(e))

# Usage
api_key = load_api_key()



class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, TradePair) or isinstance(obj, OrderType):
            return obj.__json__()  # Use the to_dict method to serialize TradePair

        if isinstance(obj, TradePairCategory):
            # Return the value of the Enum member, which is a string
            return obj.value

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

def send_signals(data):
    # Set the default URL endpoint
    default_base_url = 'http://127.0.0.1:5000'

    # Check if the URL argument is provided
    if len(sys.argv) == 2:
        # Extract the URL from the command line argument
        base_url = sys.argv[1]
    else:
        # Use the default URL if no argument is provided
        base_url = default_base_url

    print("base URL endpoint:", base_url)

    url = f'{base_url}/api/receive-signal'

    # Convert the Python dictionary to JSON format
    json_data = json.dumps(data, cls=CustomEncoder)
    print(json_data)
    # Set the headers to specify that the content is in JSON format
    headers = {
        'Content-Type': 'application/json',
    }

    # Make the POST request with JSON data
    response = requests.post(url, data=json_data, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        print("POST request was successful.")
        print("Response:")
        print(response.json())  # Print the response data
        return response.json()
    else:
        print(response.__dict__)
        print("POST request failed with status code:", response.status_code)


def map_signal_data(symbol, current_position, position_size, signal_type, signal_direction):
    """
    If signal_type is 'entry', signal_direction is the relevant object. Trade is taken at the same direction of signal_direction.
    If signal_type is 'exit', current_position is the relevant object. Trade is taken at the opposite direction of current_position.
    """
    if symbol == 'SPY':
        trade_pair = TradePair.SPX
    elif symbol == 'BTCUSDT':
        trade_pair = TradePair.BTCUSD
    elif symbol == 'QQQ':
        trade_pair = TradePair.NDX
    else:
        raise ValueError(f'For now, symbol can only be SPY, QQQ or BTCUSDT')
    
    if signal_type == 'entry':
        position_size = signal_direction * position_size
        if signal_direction == 1:
            order_type = OrderType.LONG
        elif signal_direction == -1:
            order_type = OrderType.SHORT
        elif signal_direction == 0:
            return
        else:
            raise ValueError(f'Signal can only be 0, 1 or -1')
    
    elif signal_type == 'exit':
        position_size = -current_position * position_size
        order_type = OrderType.FLAT
    
    else:
        raise ValueError(f'Signal type can only be entry or exit')
    
    data = {
        'trade_pair': trade_pair,
        'order_type': order_type,
        'leverage': position_size,
        'api_key': api_key
    }

    return data

if __name__ == "__main__":
    # Define the JSON data to be sent in the request
    symbol = 'SPY'
    position_size = 0.5
    signal_type = 'entry'
    signal_direction = -1
    
    data = map_signal_data(symbol, position_size, signal_type, signal_direction)

    print(data)

    send_signals(data)